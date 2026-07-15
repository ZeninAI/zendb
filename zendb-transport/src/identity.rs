//! Concrete local device identity and signing implementation.

use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use bincode::{Decode, Encode};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use zendb_types::{DeviceId, DeviceKeyPhase, DeviceKeyRing, DevicePublicKey, Hlc, SignatureBytes};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Encode, Decode, Zeroize, ZeroizeOnDrop)]
struct DeviceSecret([u8; 32]);

impl DeviceSecret {
    fn generate() -> io::Result<Self> {
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self(bytes))
    }

    fn signing_key(&self) -> SigningKey {
        SigningKey::from_bytes(&self.0)
    }
}

#[derive(Encode, Decode)]
struct ProfileState {
    generation: u64,
    device_id: DeviceId,
    primary_secret: DeviceSecret,
    staged_secret: Option<DeviceSecret>,
    presence_seq: u64,
    next_origin_seq: u64,
    last_hlc: Hlc,
}

/// One persisted, non-clonable local credential profile. All sequence
/// allocations are persisted before being returned so a restart cannot reuse
/// a signed presence or shared-event sequence.
pub struct DeviceProfile {
    base_path: PathBuf,
    state: Mutex<ProfileState>,
}

impl DeviceProfile {
    pub fn create(base_path: &Path) -> io::Result<Self> {
        Self::create_with_device_id(base_path, DeviceId::generate()?)
    }

    pub fn create_with_device_id(base_path: &Path, device_id: DeviceId) -> io::Result<Self> {
        if load_best_slot(base_path)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "device profile already exists",
            ));
        }
        let state = ProfileState {
            generation: 1,
            device_id,
            primary_secret: DeviceSecret::generate()?,
            staged_secret: None,
            presence_seq: 0,
            next_origin_seq: 1,
            last_hlc: Hlc::ZERO,
        };
        persist_slot(base_path, &state)?;
        Ok(Self {
            base_path: base_path.to_path_buf(),
            state: Mutex::new(state),
        })
    }

    pub fn open(base_path: &Path) -> io::Result<Self> {
        let state = load_best_slot(base_path)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "device profile does not exist")
        })?;
        Ok(Self {
            base_path: base_path.to_path_buf(),
            state: Mutex::new(state),
        })
    }

    pub fn open_or_create(base_path: &Path) -> io::Result<Self> {
        match Self::open(base_path) {
            Ok(profile) => Ok(profile),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Self::create(base_path),
            Err(error) => Err(error),
        }
    }

    pub fn device_id(&self) -> DeviceId {
        self.state
            .lock()
            .expect("device profile poisoned")
            .device_id
    }

    pub fn primary_public_key(&self) -> DevicePublicKey {
        let state = self.state.lock().expect("device profile poisoned");
        public_key(&state.primary_secret)
    }

    pub fn initial_key_ring(&self) -> DeviceKeyRing {
        DeviceKeyRing {
            primary_key: self.primary_public_key(),
            secondary_key: None,
            primary_from_seq: 1,
            phase: DeviceKeyPhase::Stable,
        }
    }

    pub fn sign_primary(&self, message: &[u8]) -> SignatureBytes {
        let state = self.state.lock().expect("device profile poisoned");
        SignatureBytes(
            state
                .primary_secret
                .signing_key()
                .sign(message)
                .to_bytes()
                .to_vec(),
        )
    }

    pub fn sign_staged(&self, message: &[u8]) -> io::Result<SignatureBytes> {
        let state = self.state.lock().expect("device profile poisoned");
        let secret = state
            .staged_secret
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no staged device key"))?;
        Ok(SignatureBytes(
            secret.signing_key().sign(message).to_bytes().to_vec(),
        ))
    }

    pub fn stage_rotation(&self, current: &DeviceKeyRing) -> io::Result<DeviceKeyRing> {
        if current.phase != DeviceKeyPhase::Stable {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device key rotation is already staged",
            ));
        }
        let mut state = self.state.lock().expect("device profile poisoned");
        if current.primary_key != public_key(&state.primary_secret) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local primary key does not match the Workspace Device record",
            ));
        }
        state.staged_secret = Some(DeviceSecret::generate()?);
        let candidate = public_key(state.staged_secret.as_ref().expect("staged above"));
        persist_next(&self.base_path, &mut state)?;
        current
            .stage(candidate)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "key ring cannot be staged"))
    }

    /// Promote after the caller has durably published a promotion event signed
    /// by `sign_staged` and confirmed the staging event's stable frontier.
    pub fn promote_rotation(
        &self,
        staged: &DeviceKeyRing,
        promotion_origin_seq: u64,
    ) -> io::Result<DeviceKeyRing> {
        let promoted = staged
            .promote(promotion_origin_seq)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "key ring is not staged"))?;
        let mut state = self.state.lock().expect("device profile poisoned");
        let candidate = state
            .staged_secret
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no staged private key"))?;
        if promoted.primary_key != public_key(&candidate) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged private key does not match promoted public key",
            ));
        }
        state.primary_secret = candidate;
        persist_next(&self.base_path, &mut state)?;
        Ok(promoted)
    }

    /// Reconcile local private-key slots with the replicated key ring after a
    /// crash between a durable promotion event and the local profile update.
    pub fn reconcile_key_ring(&self, ring: &DeviceKeyRing) -> io::Result<()> {
        let mut state = self.state.lock().expect("device profile poisoned");
        if public_key(&state.primary_secret) == ring.primary_key {
            return Ok(());
        }
        let Some(staged) = state.staged_secret.as_ref() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Workspace key ring has no matching local private key",
            ));
        };
        if public_key(staged) != ring.primary_key
            || ring.secondary_key != Some(public_key(&state.primary_secret))
            || ring.phase != DeviceKeyPhase::Stable
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Workspace key ring conflicts with local private-key slots",
            ));
        }
        let promoted = state
            .staged_secret
            .take()
            .expect("checked staged private key above");
        state.primary_secret = promoted;
        persist_next(&self.base_path, &mut state)
    }

    pub fn allocate_presence_seq(&self) -> io::Result<u64> {
        let mut state = self.state.lock().expect("device profile poisoned");
        state.presence_seq = state.presence_seq.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "presence sequence exhausted")
        })?;
        let sequence = state.presence_seq;
        persist_next(&self.base_path, &mut state)?;
        Ok(sequence)
    }

    /// Return the sequence to use for the next shared journal append.
    ///
    /// The value is not consumed until [`commit_origin_seq`](Self::commit_origin_seq)
    /// is called after the journal append is durable. Callers must serialize
    /// this pair; `Workspace` does so with its shared mutation lock.
    pub fn next_origin_seq(&self) -> u64 {
        self.state
            .lock()
            .expect("device profile poisoned")
            .next_origin_seq
    }

    /// Persist successful use of `sequence` after its journal record is durable.
    pub fn commit_origin_seq(&self, sequence: u64) -> io::Result<()> {
        let mut state = self.state.lock().expect("device profile poisoned");
        if state.next_origin_seq != sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "origin sequence was not the next allocatable value",
            ));
        }
        state.next_origin_seq = sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "origin sequence exhausted")
        })?;
        persist_next(&self.base_path, &mut state)
    }

    /// Repair the profile from durable journal history after a crash between
    /// appending an event and checkpointing the next sequence.
    pub fn advance_origin_seq_past(&self, durable_sequence: u64) -> io::Result<()> {
        let required = durable_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "origin sequence exhausted")
        })?;
        let mut state = self.state.lock().expect("device profile poisoned");
        if state.next_origin_seq >= required {
            return Ok(());
        }
        state.next_origin_seq = required;
        persist_next(&self.base_path, &mut state)
    }

    /// Allocate a monotonically increasing local HLC and persist it before use.
    pub fn next_hlc(&self, physical_ms: u64) -> io::Result<Hlc> {
        let mut state = self.state.lock().expect("device profile poisoned");
        let previous_physical = state.last_hlc.physical_ms();
        let physical = physical_ms.max(previous_physical);
        let logical = if physical == previous_physical {
            state.last_hlc.logical().checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "HLC logical counter exhausted")
            })?
        } else {
            0
        };
        let hlc = Hlc::with_device_id(physical, logical, state.device_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "physical clock exceeds HLC range",
            )
        })?;
        state.last_hlc = hlc;
        persist_next(&self.base_path, &mut state)?;
        Ok(hlc)
    }

    /// Incorporate a received HLC before the next local event allocation.
    pub fn observe_hlc(&self, remote: Hlc, physical_ms: u64) -> io::Result<Hlc> {
        let mut state = self.state.lock().expect("device profile poisoned");
        let local = state.last_hlc;
        let physical = physical_ms
            .max(local.physical_ms())
            .max(remote.physical_ms());
        let logical = if physical == local.physical_ms() && physical == remote.physical_ms() {
            local.logical().max(remote.logical()).checked_add(1)
        } else if physical == local.physical_ms() {
            local.logical().checked_add(1)
        } else if physical == remote.physical_ms() {
            remote.logical().checked_add(1)
        } else {
            Some(0)
        }
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "HLC logical counter exhausted")
        })?;
        let hlc = Hlc::with_device_id(physical, logical, state.device_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "physical clock exceeds HLC range",
            )
        })?;
        state.last_hlc = hlc;
        persist_next(&self.base_path, &mut state)?;
        Ok(hlc)
    }

    pub fn verify(public_key: DevicePublicKey, message: &[u8], signature: &SignatureBytes) -> bool {
        let Ok(key) = VerifyingKey::from_bytes(&public_key.0) else {
            return false;
        };
        let Ok(signature) = ed25519_dalek::Signature::from_slice(&signature.0) else {
            return false;
        };
        key.verify(message, &signature).is_ok()
    }
}

fn public_key(secret: &DeviceSecret) -> DevicePublicKey {
    DevicePublicKey(secret.signing_key().verifying_key().to_bytes())
}

fn slot_path(base: &Path, generation: u64) -> PathBuf {
    let suffix = if generation % 2 == 0 { "a" } else { "b" };
    PathBuf::from(format!("{}.{}", base.display(), suffix))
}

fn persist_next(base: &Path, state: &mut ProfileState) -> io::Result<()> {
    state.generation = state.generation.checked_add(1).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "profile generation exhausted")
    })?;
    persist_slot(base, state)
}

fn persist_slot(base: &Path, state: &ProfileState) -> io::Result<()> {
    if let Some(parent) = base.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = bincode::encode_to_vec(state, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut file = File::create(slot_path(base, state.generation))?;
    file.write_all(&bytes)?;
    file.sync_all()
}

fn load_best_slot(base: &Path) -> io::Result<Option<ProfileState>> {
    let mut best: Option<ProfileState> = None;
    for generation_parity in [2, 1] {
        let path = slot_path(base, generation_parity);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let Ok((state, consumed)) =
            bincode::decode_from_slice::<ProfileState, _>(&bytes, bincode::config::standard())
        else {
            continue;
        };
        if consumed == bytes.len()
            && best
                .as_ref()
                .is_none_or(|current| state.generation > current.generation)
        {
            best = Some(state);
        }
    }
    Ok(best)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "zendb-profile-{name}-{}",
            DeviceId::generate().unwrap()
        ))
    }

    #[test]
    fn profile_reopens_without_reusing_sequences() {
        let path = temp_path("sequence");
        let profile = DeviceProfile::create(&path).unwrap();
        let device_id = profile.device_id();
        assert_eq!(profile.next_origin_seq(), 1);
        profile.commit_origin_seq(1).unwrap();
        assert_eq!(profile.allocate_presence_seq().unwrap(), 1);
        drop(profile);

        let reopened = DeviceProfile::open(&path).unwrap();
        assert_eq!(reopened.device_id(), device_id);
        assert_eq!(reopened.next_origin_seq(), 2);
        assert_eq!(reopened.allocate_presence_seq().unwrap(), 2);
    }

    #[test]
    fn signatures_verify_and_reject_modified_messages() {
        let path = temp_path("signature");
        let profile = DeviceProfile::create(&path).unwrap();
        let signature = profile.sign_primary(b"message");
        assert!(DeviceProfile::verify(
            profile.primary_public_key(),
            b"message",
            &signature
        ));
        assert!(!DeviceProfile::verify(
            profile.primary_public_key(),
            b"modified",
            &signature
        ));
    }
}
