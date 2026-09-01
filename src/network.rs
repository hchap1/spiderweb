use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::sync::Arc;

use crate::discovery::discover::Discovery;
use crate::discovery::discover::Mode;
use crate::discovery::register::register;
use crate::error::Res;

use async_channel::Sender;
use async_channel::Receiver;
use async_channel::bounded;
use bytes::BytesMut;
use mdns_sd::ServiceEvent;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::sync::Mutex;
use bytes::Bytes;
use tokio::task::JoinHandle;

/// The server manages the TcpListener for foreign clients accepting
/// and produces a Node upon successful connection
/// Also tracks individual nodes
pub struct Server {
    nodes: Arc<Mutex<HashMap<String, String>>>,
    acquisition_task: JoinHandle<Res<()>>
}

impl Server {

    /// This function is responsible for receiving new mDNS discoveries AND foreign clients
    /// it is also responsible for maintaining concurrency limit
    async fn acquisition_manager(
        application_name: &'static str, port: u16, nickname: Option<String>,
        concurrency_limit: usize, channel_size: usize,
        nodes: Arc<Mutex<HashMap<String, String>>>
    ) -> Res<()> {

        let server = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port)).await?;
        let advertiser = register(application_name, port, nickname).await?;
        let semaphore = Arc::new(Semaphore::new(concurrency_limit));
        let mdns_event_receiver = advertiser.get_event_stream()?;
        let mut queue: Vec<(String, Node)> = Vec::new();

        while let Ok(event) = mdns_event_receiver.recv().await {
            let discovery = match event {
                ServiceEvent::ServiceResolved(resolved_service) => Discovery::from_resolved_service(resolved_service),
                _ => continue
            };

            let discovery = match discovery {
                Ok(v) => v,
                Err(e) => {
                    println!("[SPIDERWEB] Failed to parse discovery. {e:?}");
                    continue;
                }
            };

            let mode = match advertiser.discovery.decide_server(&discovery).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[SPIDERWEB] Failed to decide server. {e:?}");
                    continue;
                }
            };

            let sempahore_clone = semaphore.clone();
            match mode {
                Mode::Client => tokio::task::spawn(async {
                    let permit = sempahore_clone.acquire_owned().await?;
                    Node::connect()
                })
            }

            // TODO node connection logic then tokio select this against incoming TCP connection
            // TODO integrate all into semaphore by wrapping in Arc and spawning tasks waiting on permit
        }


        Ok(())
    }
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

    /// Connect to a foreign node. This should only be run if it has already been determined that this end is the client.
    pub async fn connect(discovery: Discovery, channel_size: usize) -> Res<Option<Node>> {
        // Form a connection to the discovery
        let socket = TcpStream::connect(SocketAddrV4::new(discovery.ip, discovery.port)).await?;
        Node::build(socket, channel_size).await.map(Some)
    }

    pub async fn build(stream: TcpStream, channel_size: usize) -> Res<Self> {

        // Build Node
        let (send, queue) = bounded(channel_size);
        let (output, recv) = bounded(channel_size);

        // Start processing the Node
        let (read_half, write_half) = stream.into_split();
        let send_task = tokio::task::spawn(Self::send_task(write_half, queue));
        let recv_task = tokio::task::spawn(Self::recv_task(read_half, output));

        Ok(
            Node {
                send,
                recv,
                send_task,
                recv_task
            }
        )
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
