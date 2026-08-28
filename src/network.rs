use std::net::SocketAddrV4;

use crate::discovery::discover::Discovery;
use crate::discovery::discover::Mode;
use crate::error::Res;

use async_channel::Sender;
use async_channel::Receiver;
use async_channel::bounded;
use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use bytes::Bytes;

/// The server manages the TcpListener for foreign clients accepting
/// and produces a Node upon successful connection
/// Also tracks individual nodes
pub struct Server {

}

/// The point of contact to a foreign server/client
/// Unified interface, the goal of which is to obfuscate who is the server, and who is the client
pub struct Node {
    send: Sender<Bytes>,
    recv: Receiver<Bytes>,
    send_task: tokio::task::JoinHandle<Res<()>>,
    recv_task: tokio::task::JoinHandle<Res<()>>
}

impl Node {

    /// The older node will be the server. If the two nodes were created at the same time
    /// The lower value of the bitwise interpretation (u32) of the ipv4 address will be the server
    /// Idiomatically, this function does nothing if this side will be the server
    pub async fn connect(discovery: Discovery) -> Res<Option<Node>> {

        // Decide who will be the server / client
        match Discovery::decide_server(&discovery) {
            Mode::Server => return Ok(None),
            Mode::Client => {
                // Form a connection to the discovery
                let socket = TcpStream::connect(SocketAddrV4::new(discovery.ip, discovery.port)).await?;


                Ok(())
            }
        }

    }

    async fn send_task(mut write_half: OwnedWriteHalf, queue: Receiver<Bytes>) -> Res<()> {
        while let Ok(bytes) = queue.recv().await {

            // Write length header into queue
            let length: u32 = bytes.len() as u32;
            let header: Bytes = Bytes::copy_from_slice(&length.to_be_bytes());
            write_half.write_all(&header).await?;

            // Write payload into queue
            write_half.write_all(&bytes).await?;
        }

        Ok(())
    }

    async fn recv_task(mut read_half: OwnedReadHalf, output: Sender<Bytes>) -> Res<()> {
        let mut size_buffer = [0u8; 4];
        while let Ok(_) = read_half.read_exact(&mut size_buffer).await {

            // Process size header
            let size: usize = u32::from_be_bytes(size_buffer) as usize;

            // Buffer the bytes
            let mut bytes = BytesMut::with_capacity(size);
            read_half.read_exact(&mut bytes).await?;

            // Send bytes object out into the world
            output.send(bytes.freeze()).await?;
        }

        Ok(())
    }
}
