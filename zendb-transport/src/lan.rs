use std::{io, net::SocketAddr, net::UdpSocket};

/// Concrete LAN datagram socket used only for untrusted peer announcements.
pub struct LanDiscoverySocket {
    socket: UdpSocket,
}

impl LanDiscoverySocket {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(address)?;
        socket.set_nonblocking(true)?;
        socket.set_broadcast(true)?;
        Ok(Self { socket })
    }

    pub fn send_to(&self, bytes: &[u8], target: &SocketAddr) -> io::Result<usize> {
        self.socket.send_to(bytes, target)
    }

    pub fn receive_from(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(bytes)
    }
}
