use std::{io, time::Duration};

use crate::ConnectionHint;

/// Reliable ordered frame carrier below the ZenDB authenticated session.
///
/// Carrier implementations own fragmentation, stream framing, and platform
/// I/O. Membership and peer authentication remain above this boundary.
pub trait FramedLink: Send {
    fn send_frame(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn receive_frame(&mut self, max_bytes: usize) -> io::Result<Vec<u8>>;
    fn remote_endpoint(&self) -> io::Result<ConnectionHint>;
    fn set_timeouts(&mut self, timeout: Option<Duration>) -> io::Result<()>;
    fn close(&mut self) -> io::Result<()>;
}
