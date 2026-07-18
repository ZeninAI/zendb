//! Concrete client-side cluster lifecycle, reconnect, and LAN discovery.

use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use bincode::{Decode, Encode};
use parking_lot::{Mutex, RwLock};
use zendb_transport::{LanDiscoverySocket, TcpLinkListener};
use zendb_types::{DeviceId, DevicePublicKey, SignatureBytes, WorkspaceId};

use super::{now_ms, Workspace};

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// TCP address for incoming replication and bootstrap sessions.
    pub listen_address: SocketAddr,
    /// Address placed in LAN announcements. Required when listening on an
    /// unspecified address and discovery is enabled.
    pub advertised_address: Option<SocketAddr>,
    /// Initial peers retried by the periodic anti-entropy loop.
    pub seed_peers: BTreeSet<SocketAddr>,
    /// Heartbeat, sync, and LAN announcement cadence. Values below 100 ms are
    /// clamped to protect the local runtime.
    pub sync_interval: Duration,
    /// Optional UDP socket used to receive LAN announcements.
    pub discovery_bind: Option<SocketAddr>,
    /// UDP unicast or broadcast destinations for signed announcements.
    pub discovery_targets: Vec<SocketAddr>,
    /// Maximum age accepted for a signed LAN announcement.
    pub announcement_ttl: Duration,
    /// Multiplier applied to a peer's advertised idle interval before it is
    /// classified Offline locally.
    pub presence_grace_multiplier: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0:0".parse().expect("valid address"),
            advertised_address: None,
            seed_peers: BTreeSet::new(),
            sync_interval: Duration::from_secs(30),
            discovery_bind: None,
            discovery_targets: Vec::new(),
            announcement_ttl: Duration::from_secs(90),
            presence_grace_multiplier: 3,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
struct LanAnnouncement {
    workspace_id: WorkspaceId,
    device_id: DeviceId,
    public_key: DevicePublicKey,
    endpoint: SocketAddr,
    emitted_at_ms: u64,
    expires_at_ms: u64,
    signature: SignatureBytes,
}

/// Running networking owned by one client-side Workspace. It is concrete
/// because there is only one cluster lifecycle implementation; applications
/// configure endpoints and discovery targets rather than implementing a trait.
pub struct ClusterRuntime {
    workspace: Arc<Workspace>,
    local_address: SocketAddr,
    peers: Arc<RwLock<BTreeSet<SocketAddr>>>,
    stopping: Arc<AtomicBool>,
    errors: Arc<Mutex<Vec<String>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Workspace {
    /// Start the concrete listener, discovery, reconnect, and presence loops.
    pub fn start_cluster(self: &Arc<Self>, config: ClusterConfig) -> io::Result<ClusterRuntime> {
        ClusterRuntime::start(Arc::clone(self), config)
    }
}

impl ClusterRuntime {
    fn start(workspace: Arc<Workspace>, config: ClusterConfig) -> io::Result<Self> {
        let listener = TcpLinkListener::bind(config.listen_address)?;
        listener.set_nonblocking(true)?;
        let local_address = listener.local_addr()?;
        let advertised_address = config.advertised_address.unwrap_or(local_address);
        if config.discovery_bind.is_some() && advertised_address.ip().is_unspecified() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "LAN discovery requires a routable advertised_address",
            ));
        }
        let sync_interval = config.sync_interval.max(Duration::from_millis(100));
        workspace.presence_idle_ms.store(
            sync_interval.as_millis().max(1).min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        workspace
            .presence_grace_multiplier
            .store(config.presence_grace_multiplier.max(1), Ordering::Relaxed);

        let peers = Arc::new(RwLock::new(config.seed_peers));
        let stopping = Arc::new(AtomicBool::new(false));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let mut threads = Vec::new();

        threads.push(spawn_listener(
            Arc::clone(&workspace),
            listener,
            Arc::clone(&stopping),
            Arc::clone(&errors),
        ));
        threads.push(spawn_sync_loop(
            Arc::clone(&workspace),
            Arc::clone(&peers),
            Arc::clone(&stopping),
            local_address,
            sync_interval,
            Arc::clone(&errors),
        ));
        if let Some(discovery_bind) = config.discovery_bind {
            let socket = LanDiscoverySocket::bind(discovery_bind)?;
            threads.push(spawn_discovery_loop(
                Arc::clone(&workspace),
                socket,
                Arc::clone(&peers),
                Arc::clone(&stopping),
                advertised_address,
                config.discovery_targets,
                sync_interval,
                config.announcement_ttl,
            ));
        }

        Ok(Self {
            workspace,
            local_address,
            peers,
            stopping,
            errors,
            threads: Mutex::new(threads),
        })
    }

    /// Return the actual bound TCP address.
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Add a peer endpoint to the reconnect set.
    pub fn add_peer(&self, address: SocketAddr) -> bool {
        address != self.local_address && self.peers.write().insert(address)
    }

    /// Remove a peer endpoint from the reconnect set.
    pub fn remove_peer(&self, address: &SocketAddr) -> bool {
        self.peers.write().remove(address)
    }

    /// Return a snapshot of configured and discovered peer endpoints.
    pub fn peers(&self) -> Vec<SocketAddr> {
        self.peers.read().iter().copied().collect()
    }

    /// Return bounded background diagnostics collected by the runtime.
    pub fn errors(&self) -> Vec<String> {
        self.errors.lock().clone()
    }

    /// Stop networking, join all heartbeat-producing sessions, then send a
    /// best-effort signed departure to known peers.
    pub fn shutdown(&self) {
        if self.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        for thread in self.threads.lock().drain(..) {
            let _ = thread.join();
        }
        // Stop every source of local heartbeats before publishing departure,
        // otherwise an in-flight sync could make this device appear live again.
        for peer in self.peers() {
            let _ = self.workspace.send_departure_tcp(peer);
        }
    }
}

impl Drop for ClusterRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn spawn_listener(
    workspace: Arc<Workspace>,
    listener: TcpLinkListener,
    stopping: Arc<AtomicBool>,
    errors: Arc<Mutex<Vec<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut sessions: Vec<JoinHandle<()>> = Vec::new();
        while !stopping.load(Ordering::Relaxed) {
            let mut index = 0;
            while index < sessions.len() {
                if sessions[index].is_finished() {
                    let session = sessions.swap_remove(index);
                    let _ = session.join();
                } else {
                    index += 1;
                }
            }
            match listener.accept() {
                Ok((link, _)) => {
                    let workspace = Arc::clone(&workspace);
                    let errors = Arc::clone(&errors);
                    sessions.push(thread::spawn(move || {
                        if let Err(error) = workspace.serve_tcp(link) {
                            record_error(&errors, format!("inbound session: {error}"));
                        }
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        }
        // The listener owns accepted sessions so ClusterRuntime shutdown can
        // stop every heartbeat producer before it emits a departure notice.
        for session in sessions {
            let _ = session.join();
        }
    })
}

fn spawn_sync_loop(
    workspace: Arc<Workspace>,
    peers: Arc<RwLock<BTreeSet<SocketAddr>>>,
    stopping: Arc<AtomicBool>,
    local_address: SocketAddr,
    interval: Duration,
    errors: Arc<Mutex<Vec<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let interval = interval.max(Duration::from_millis(100));
        while !stopping.load(Ordering::Relaxed) {
            let started = Instant::now();
            let endpoints: Vec<_> = peers.read().iter().copied().collect();
            for endpoint in endpoints {
                if stopping.load(Ordering::Relaxed) {
                    break;
                }
                if endpoint != local_address {
                    match workspace.sync_tcp(endpoint) {
                        Ok(_) => {
                            if let Err(error) = workspace.checkpoint_local_frontier() {
                                record_error(&errors, format!("frontier checkpoint: {error}"));
                            }
                        }
                        Err(error) => {
                            record_error(&errors, format!("outbound session {endpoint}: {error}"))
                        }
                    }
                }
            }
            sleep_until_or_stopping(&stopping, interval.saturating_sub(started.elapsed()));
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_discovery_loop(
    workspace: Arc<Workspace>,
    socket: LanDiscoverySocket,
    peers: Arc<RwLock<BTreeSet<SocketAddr>>>,
    stopping: Arc<AtomicBool>,
    advertised_address: SocketAddr,
    targets: Vec<SocketAddr>,
    announce_interval: Duration,
    ttl: Duration,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut last_announcement = Instant::now() - announce_interval;
        let mut buffer = vec![0; 64 * 1024];
        while !stopping.load(Ordering::Relaxed) {
            if last_announcement.elapsed() >= announce_interval {
                if let Ok(bytes) = make_announcement(&workspace, advertised_address, ttl) {
                    for target in &targets {
                        let _ = socket.send_to(&bytes, target);
                    }
                }
                last_announcement = Instant::now();
            }
            loop {
                match socket.receive_from(&mut buffer) {
                    Ok((length, _)) => {
                        if let Ok(endpoint) =
                            verify_announcement(&workspace, &buffer[..length], ttl)
                        {
                            if endpoint != advertised_address {
                                peers.write().insert(endpoint);
                            }
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => break,
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
    })
}

fn make_announcement(
    workspace: &Workspace,
    endpoint: SocketAddr,
    ttl: Duration,
) -> io::Result<Vec<u8>> {
    let emitted_at_ms = now_ms();
    let ttl_ms = ttl.as_millis().min(u64::MAX as u128) as u64;
    let expires_at_ms = emitted_at_ms.saturating_add(ttl_ms);
    let public_key = workspace.device_profile().primary_public_key();
    let signing = announcement_signing_bytes(
        workspace.workspace_id(),
        workspace.device_id(),
        public_key,
        endpoint,
        emitted_at_ms,
        expires_at_ms,
    )?;
    let announcement = LanAnnouncement {
        workspace_id: workspace.workspace_id().clone(),
        device_id: workspace.device_id(),
        public_key,
        endpoint,
        emitted_at_ms,
        expires_at_ms,
        signature: workspace.device_profile().sign_primary(&signing),
    };
    bincode::encode_to_vec(announcement, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))
}

fn verify_announcement(
    workspace: &Workspace,
    bytes: &[u8],
    maximum_ttl: Duration,
) -> io::Result<SocketAddr> {
    let (announcement, consumed): (LanAnnouncement, usize) =
        bincode::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let now = now_ms();
    let maximum_ttl_ms = maximum_ttl.as_millis().min(u64::MAX as u128) as u64;
    if consumed != bytes.len()
        || announcement.workspace_id != *workspace.workspace_id()
        || announcement.device_id == workspace.device_id()
        || announcement.emitted_at_ms > announcement.expires_at_ms
        || announcement.expires_at_ms < now
        || announcement
            .expires_at_ms
            .saturating_sub(announcement.emitted_at_ms)
            > maximum_ttl_ms
        || announcement.emitted_at_ms > now.saturating_add(60_000)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "LAN announcement scope or expiry is invalid",
        ));
    }
    let device = workspace.device(announcement.device_id)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "announcing Device is not admitted",
        )
    })?;
    // Presence and discovery use only the current primary key, never a retired
    // historic key.
    if device.key_ring.primary_key != announcement.public_key {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "LAN announcement key is not the Device primary",
        ));
    }
    let signing = announcement_signing_bytes(
        &announcement.workspace_id,
        announcement.device_id,
        announcement.public_key,
        announcement.endpoint,
        announcement.emitted_at_ms,
        announcement.expires_at_ms,
    )?;
    if !zendb_transport::DeviceProfile::verify(
        announcement.public_key,
        &signing,
        &announcement.signature,
    ) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "LAN announcement signature is invalid",
        ));
    }
    Ok(announcement.endpoint)
}

fn announcement_signing_bytes(
    workspace_id: &WorkspaceId,
    device_id: DeviceId,
    public_key: DevicePublicKey,
    endpoint: SocketAddr,
    emitted_at_ms: u64,
    expires_at_ms: u64,
) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(
        (
            b"zendb-lan-announcement-v1".as_slice(),
            workspace_id,
            device_id,
            public_key,
            endpoint,
            emitted_at_ms,
            expires_at_ms,
        ),
        bincode::config::standard(),
    )
    .map_err(|error| io::Error::other(error.to_string()))
}

fn sleep_until_or_stopping(stopping: &AtomicBool, duration: Duration) {
    let deadline = Instant::now() + duration;
    while !stopping.load(Ordering::Relaxed) && Instant::now() < deadline {
        thread::sleep(
            Duration::from_millis(20).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn record_error(errors: &Mutex<Vec<String>>, error: String) {
    let mut errors = errors.lock();
    if errors.len() == 100 {
        errors.remove(0);
    }
    errors.push(error);
}
