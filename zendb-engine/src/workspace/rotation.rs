//! Stable-DeviceId signing-key rotation state machine.

use std::{io, sync::Arc};

use zendb_types::{DeviceKeyPhase, EventIdentity};

use super::{now_ms, system, Workspace};

impl Workspace {
    /// Stage a fresh secondary public key using the current primary key.
    pub fn stage_local_key_rotation(self: &Arc<Self>) -> io::Result<EventIdentity> {
        let device = self.device(self.device_id())?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local Device is not admitted",
            )
        })?;
        if device.key_ring.phase != DeviceKeyPhase::Stable {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a key rotation is already staged",
            ));
        }
        if device.key_ring.secondary_key.is_some()
            && self.stable_frontier()?.applied_through(&self.device_id())
                < device.key_ring.primary_from_seq
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "the previous promotion is not stable on every admitted Device",
            ));
        }
        let staged = self.device_profile.stage_rotation(&device.key_ring)?;
        let at = self.device_profile.next_hlc(now_ms())?;
        let value = system::key_ring_value(&staged, at)?;
        self.commit_shared_event(system::replace_field_event(
            at,
            self.device_id(),
            "key_ring",
            value,
        ))
    }

    /// Promote a staged key only after every admitted Device checkpoint covers
    /// the staging event. The promotion marker itself is signed by the staged
    /// private key and atomically swaps the public key slots.
    pub fn promote_local_key_rotation(
        self: &Arc<Self>,
        staging_event: EventIdentity,
    ) -> io::Result<EventIdentity> {
        if staging_event.origin_device_id != self.device_id()
            || self.stable_frontier()?.applied_through(&self.device_id()) < staging_event.origin_seq
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "key staging event is not stable on every admitted Device",
            ));
        }
        let device = self.device(self.device_id())?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local Device is not admitted",
            )
        })?;
        if device.key_ring.phase != DeviceKeyPhase::Staged {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Device key ring has no staged key",
            ));
        }
        let promotion_sequence = self.device_profile.next_origin_seq();
        let promoted = device
            .key_ring
            .promote(promotion_sequence)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid staged key ring"))?;
        let at = self.device_profile.next_hlc(now_ms())?;
        let value = system::key_ring_value(&promoted, at)?;
        let identity = self.commit_shared_event_with_staged_key(system::replace_field_event(
            at,
            self.device_id(),
            "key_ring",
            value,
        ))?;
        if identity.origin_seq != promotion_sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "promotion sequence changed while rotation was serialized",
            ));
        }
        self.device_profile
            .promote_rotation(&device.key_ring, identity.origin_seq)?;
        Ok(identity)
    }
}
