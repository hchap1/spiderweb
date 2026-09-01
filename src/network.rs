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
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::Mutex;
use bytes::Bytes;
use tokio::task::JoinHandle;

/// The server manages the TcpListener for foreign clients accepting
/// and produces a Node upon successful connection
/// Also tracks individual nodes
pub struct Server {
    nodes: Arc<Mutex<HashMap<String, Node>>>,

    pub incoming: Receiver <(Bytes, String)>,
    pub outgoing: Sender   <(Bytes, Vec<String>)>,

    // Outgoing messages
    send_task: JoinHandle<Res<()>>,
    acquisition_task: JoinHandle<Res<()>>,
}

impl Server {

    pub fn build(
        application_name: &'static str, port: u16, nickname: Option<String>,
        concurrency_limit: usize, channel_size: usize
    ) -> Self {

        // Create channels for owned tasks
        let (incoming_sender, incoming) = bounded(channel_size);
        let (outgoing, outgoing_receiver) = bounded(channel_size);
        let nodes = Arc::new(Mutex::new(HashMap::new()));

        let send_task = tokio::task::spawn(Self::send(outgoing_receiver, nodes.clone()));
        let acquisition_task = tokio::task::spawn(
            Self::acquisition_manager(application_name, port, nickname, concurrency_limit, channel_size, nodes.clone(), incoming_sender)
        );

        Self {
            nodes,
            incoming,
            outgoing,
            send_task,
            acquisition_task
        }
    }

    pub async fn get_foreign_identifiers(&self) -> Vec<String> {
        let hashmap = self.nodes.lock().await;
        hashmap.keys().cloned().collect()
    }

    /// Distribute a packet to a selection of identifiers
    /// If the Vec<String> is empty, then the packet will be sent to all nodes.
    async fn send(outgoing_receiver: Receiver<(Bytes, Vec<String>)>, nodes: Arc<Mutex<HashMap<String, Node>>>) -> Res<()> {
        while let Ok((packet, identifiers)) = outgoing_receiver.recv().await {
            let hashmap = nodes.lock().await;
            if identifiers.is_empty() {
                for node in hashmap.values() {
                    let _ = node.send.send(packet.clone()).await;
                }
            } else {
                for identifier in identifiers {
                    if let Some(node) = hashmap.get(&identifier) {
                        let _ = node.send.send(packet.clone()).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// This function is responsible for receiving new mDNS discoveries AND foreign clients
    /// it is also responsible for maintaining concurrency limit
    async fn acquisition_manager(
        application_name: &'static str, port: u16, nickname: Option<String>,
        concurrency_limit: usize, channel_size: usize,
        nodes: Arc<Mutex<HashMap<String, Node>>>, incoming_sender: Sender<(Bytes, String)>
    ) -> Res<()> {

        let server = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port)).await?;
        let advertiser = register(application_name, port, nickname).await?;
        let semaphore = Arc::new(Semaphore::new(concurrency_limit));
        let mdns_event_receiver = advertiser.get_event_stream()?;

        loop {
            tokio::select! {

                // Handle an mDNS resolution
                maybe_event = mdns_event_receiver.recv() => {
                    let event = match maybe_event {
                        Ok(event) => event,
                        Err(e) => {
                            eprintln!("[SPIDERWEB] Failed to retrieve mDNS event: {e:?}");
                            continue;
                        }
                    };

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

                    let hashmap_clone = nodes.clone();
                    let discovery_clone = advertiser.discovery.clone();
                    let semaphore_clone = semaphore.clone();
                    let incoming_sender_clone = incoming_sender.clone();
                    
                    match mode {
                        Mode::Client => tokio::task::spawn(async move {
                            let permit = semaphore_clone.acquire_owned().await?;
                            let identifier = discovery.get_identifier();
                            match Node::connect(discovery_clone, discovery, channel_size, permit, incoming_sender_clone).await {
                                Ok(node) => {
                                    let mut hashmap = hashmap_clone.lock().await;
                                    let _ = hashmap.insert(identifier, node);

                                    // TODO remove debug print
                                    println!("[SPIDERWEB INFO] Successfully connected to foreign Node");
                                },
                                Err(e) => {
                                    eprintln!("[SPIDERWEB] Failed to connect to foreign node as a client. {e:?}");
                                }
                            }
                            Ok::<(), crate::error::Error>(())
                        }),
                        Mode::Server => {
                            eprintln!("[SPIDERWEB] Discovered node that should connect.");
                            continue;
                        }
                    };
                }

                // Handle an incoming TCP connection (likely from a foreign client)
                // First, must wait for a packet containing the serialised discovery information
                maybe_request = server.accept() => {
                    let (socket, _) = match maybe_request {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[SPIDERWEB] Failed to accept connection request. {e:?}");
                            continue;
                        }
                    };

                    let my_identifier = advertiser.discovery.get_identifier();
                    let semaphore_clone = semaphore.clone();
                    let hashmap_clone = nodes.clone();
                    let incoming_sender_clone = incoming_sender.clone();

                    tokio::task::spawn(async move {

                        // Take out a permit to enforce concurrency limit
                        let permit = semaphore_clone.acquire_owned().await?;
                        let (node, first_packet_bytes) = Node::build_and_listen(my_identifier, socket, channel_size, permit, incoming_sender_clone).await?;

                        // This should contain serialised discovery information
                        let discovery = match Discovery::from_bytes(first_packet_bytes) {
                            Ok(discovery) => discovery,
                            error => {
                                eprintln!("[SPIDERWEB] Refusing client due to malformed discovery header.");
                                return error.map(|_| ());
                            }
                        };

                        let mut hashmap = hashmap_clone.lock().await;
                        let _ = hashmap.insert(discovery.get_identifier(), node);

                        // TODO remove debug print
                        println!("[SPIDERWEB DEBUG] Successfully accepted and parsed foreign Node");

                        Ok::<(), crate::error::Error>(())
                    });
                }
            }
        }
    }
}

/// The point of contact to a foreign server/client
/// Unified interface, the goal of which is to obfuscate who is the server, and who is the client
pub struct Node {
    send: Sender<Bytes>,
    task: tokio::task::JoinHandle<Res<()>>
}

impl Node {

    /// Connect to a foreign node. This should only be run if it has already been determined that this end is the client.
    pub async fn connect(me: Discovery, discovery: Discovery, channel_size: usize, permit: OwnedSemaphorePermit, incoming_sender: Sender<(Bytes, String)>) -> Res<Node> {
        // Form a connection to the discovery
        let socket = TcpStream::connect(SocketAddrV4::new(discovery.ip, discovery.port)).await?;
        let (node, _) = Node::build(me.get_identifier(), socket, channel_size, permit, incoming_sender, false).await?;
        node.send.send(me.to_bytes()?).await?;
        Ok(node)
    }

    pub async fn build(
        identifier: String,
        stream: TcpStream,
        channel_size: usize,
        permit: OwnedSemaphorePermit,
        incoming_sender: Sender<(Bytes, String)>,
        receive_one: bool
    ) -> Res<(Self, Option<Bytes>)> {

        // Build Node
        let (send, queue) = bounded(channel_size);

        // Start processing the Node
        let (mut read_half, write_half) = stream.into_split();

        let first_packet = match receive_one {
            true => {
                let mut size_buffer: [u8; 4] = [0u8; 4];
                read_half.read_exact(&mut size_buffer).await?;
                let size = u32::from_be_bytes(size_buffer);
                let mut packet = BytesMut::with_capacity(size as usize);
                read_half.read_exact(&mut packet).await?;
                Ok::<_, crate::error::Error>(packet.freeze())
            }.ok(),
            false => None
        };

        let send_task = tokio::task::spawn(Self::send_task(write_half, queue));
        let recv_task = tokio::task::spawn(Self::recv_task(read_half, incoming_sender, identifier));
        let task = tokio::task::spawn(Self::node_heartbeat(permit, send_task, recv_task));

        Ok(
            (
                Node {
                    send,
                    task
                },
                first_packet
            )
        )
    }
    
    async fn build_and_listen(
        identifier: String, stream: TcpStream, channel_size: usize, permit: OwnedSemaphorePermit, incoming_sender: Sender<(Bytes, String)>
    ) -> Res<(Node, Bytes)> {
        let (node, first_packet) = Node::build(identifier, stream, channel_size, permit, incoming_sender, true).await?;
        match first_packet {
            Some(first_packet) => Ok((node, first_packet)),
            None => Err(crate::error::Error::NoFirstPacket)
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

    async fn recv_task(mut read_half: OwnedReadHalf, output: Sender<(Bytes, String)>, identifier: String) -> Res<()> {
        let mut size_buffer = [0u8; 4];
        while let Ok(_) = read_half.read_exact(&mut size_buffer).await {

            // Process size header
            let size: usize = u32::from_be_bytes(size_buffer) as usize;

            // Buffer the bytes
            let mut bytes = BytesMut::with_capacity(size);
            read_half.read_exact(&mut bytes).await?;

            // Send bytes object out into the world
            output.send((bytes.freeze(), identifier.clone())).await?;
        }

        Ok(())
    }

    /// Hold a permit from the semaphore and both tasks.
    /// Enforces connection concurrency limit
    async fn node_heartbeat(
        _permit: OwnedSemaphorePermit,
        send_task: JoinHandle<Res<()>>,
        recv_task: JoinHandle<Res<()>>
    ) -> Res<()> {
        send_task.await?;
        recv_task.await?;
        Ok(())
    }
}
