use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    time::Duration,
};

use crate::{ConnectionHint, FramedLink};

pub struct TcpLink {
    stream: TcpStream,
}

impl TcpLink {
    pub fn connect(address: SocketAddr) -> io::Result<Self> {
        Self::from_stream(TcpStream::connect(address)?)
    }

    pub fn from_stream(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }
}

impl FramedLink for TcpLink {
    fn send_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        let length = u32::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame is too large"))?;
        self.stream.write_all(&length.to_be_bytes())?;
        self.stream.write_all(bytes)?;
        self.stream.flush()
    }

    fn receive_frame(&mut self, max_bytes: usize) -> io::Result<Vec<u8>> {
        let mut length = [0; 4];
        self.stream.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length) as usize;
        if length > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incoming frame exceeds configured limit",
            ));
        }
        let mut bytes = vec![0; length];
        self.stream.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn remote_endpoint(&self) -> io::Result<ConnectionHint> {
        Ok(ConnectionHint::Tcp(self.peer_addr()?))
    }

    fn set_timeouts(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)?;
        self.stream.set_write_timeout(timeout)
    }

    fn close(&mut self) -> io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Both)
    }
}

pub struct TcpLinkListener {
    listener: TcpListener,
}

impl TcpLinkListener {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(address)?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.listener.set_nonblocking(nonblocking)
    }

    pub fn accept(&self) -> io::Result<(TcpLink, SocketAddr)> {
        let (stream, address) = self.listener.accept()?;
        stream.set_nonblocking(false)?;
        Ok((TcpLink::from_stream(stream)?, address))
    }
}
