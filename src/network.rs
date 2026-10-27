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

/// Log for the Server
/// Returned at the end
#[derive(Clone)]
pub struct Log {
    buffer: Vec<String>,
    ptr: usize
}

impl Log {
    pub fn new() -> Self {
        Self {
            buffer: vec![String::new(); 1000],
            ptr: 0
        }
    }

    pub fn log(&mut self, string: String) {
        if let Some(line) = self.buffer.get_mut(self.ptr) {
            *line = string;
        }
    }
}

/// Wraps log so it can be Send
pub struct LogContainer {
    log: Arc<Mutex<Log>>
}

impl LogContainer {
    pub fn new() -> Self {
        Self {
            log: Arc::new(Mutex::new(Log::new()))
        }
    }

    pub async fn log(&self, string: String) {
        let mut log = self.log.lock().await;
        log.log(string)
    }
}

/// The server manages the TcpListener for foreign clients accepting
/// and produces a Node upon successful connection
/// Also tracks individual nodes
pub struct Server {
    nodes: Arc<Mutex<HashMap<String, Node>>>,

    pub incoming: Receiver <(Bytes, String)>,
    pub outgoing: Sender   <(Bytes, Vec<String>)>,

    // Outgoing messages
    _send_task: Option<JoinHandle<Res<()>>>,
    _acquisition_task: Option<JoinHandle<Res<Log>>>,

    // Trigger graceful shutdown
    _killswitch: Option<tokio::sync::oneshot::Sender<()>>,
    _incoming_sender_part: Sender<(Bytes, String)>,
}

impl Server {

    pub fn build(
        application_name: &'static str, port: u16, nickname: Option<String>,
        concurrency_limit: usize, channel_size: usize
    ) -> Self {
        // Create channels for owned tasks
        let (incoming_sender, incoming) = bounded(channel_size);
        let (outgoing, outgoing_receiver) = bounded(channel_size);
        let _incoming_sender_part = incoming_sender.clone();
        let nodes = Arc::new(Mutex::new(HashMap::new()));

        // Shutdown channels
        let (killswitch, killswitch_receiver) = tokio::sync::oneshot::channel();
        let (send_killswitch, send_killswitch_receiver) = tokio::sync::oneshot::channel();

        let _send_task = tokio::task::spawn(Self::send(outgoing_receiver, nodes.clone(), send_killswitch_receiver));
        let _acquisition_task = tokio::task::spawn(
            Self::acquisition_manager(application_name, port, nickname, concurrency_limit, channel_size, nodes.clone(), incoming_sender, killswitch_receiver, send_killswitch)
        );

        Self {
            nodes,
            incoming,
            outgoing,
            _send_task: Some(_send_task),
            _acquisition_task: Some(_acquisition_task),
            _killswitch: Some(killswitch),
            _incoming_sender_part
        }
    }

    pub async fn graceful_shutdown(&mut self) -> Res<Log> {
        match (self._killswitch.take(), self._send_task.take(), self._acquisition_task.take()) {
            (Some(k), Some(s), Some(a)) => {
                self._incoming_sender_part.close();
                let _ = k.send(());
                let _ = s.await?;
                a.await?
            },

            _ => Err(crate::error::Error::ServerShutddownAlready)
        }
    }

    pub async fn get_foreign_identifiers(&self) -> Vec<String> {
        let hashmap = self.nodes.lock().await;
        hashmap.keys().cloned().collect()
    }

    /// Distribute a packet to a selection of identifiers
    /// If the Vec<String> is empty, then the packet will be sent to all nodes.
    async fn send(
        outgoing_receiver: Receiver<(Bytes, Vec<String>)>,
        nodes: Arc<Mutex<HashMap<String, Node>>>,
        mut killswitch_receiver: tokio::sync::oneshot::Receiver<()>
    ) -> Res<()> {

        loop {
            tokio::select! {
                maybe_incoming = outgoing_receiver.recv() => {

                    let (packet, identifiers) = match maybe_incoming {
                        Ok(incoming) => incoming,
                        Err(_) => continue
                    };

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

                _ = &mut killswitch_receiver => {
                    break;
                }
            }
        }

        Ok(())
    }

    /// This function is responsible for receiving new mDNS discoveries AND foreign clients
    /// it is also responsible for maintaining concurrency limit
    #[allow(clippy::too_many_arguments)]
    async fn acquisition_manager(
        application_name: &'static str, port: u16, nickname: Option<String>,
        concurrency_limit: usize, channel_size: usize,
        nodes: Arc<Mutex<HashMap<String, Node>>>, incoming_sender: Sender<(Bytes, String)>,
        mut killswitch_receiver: tokio::sync::oneshot::Receiver<()>,
        send_killswitch: tokio::sync::oneshot::Sender<()>
    ) -> Res<Log> {

        let server = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port)).await?;
        let advertiser = register(application_name, port, nickname).await?;
        let semaphore = Arc::new(Semaphore::new(concurrency_limit));
        let mdns_event_receiver = advertiser.get_event_stream()?;

        // Sender/Receiver pair for deleting nodes
        let (request_delete, garbage_collector) = bounded(channel_size);

        // Logging
        let log = Arc::new(LogContainer::new());

        loop {
            tokio::select! {

                // Handle an mDNS resolution
                maybe_event = mdns_event_receiver.recv() => {
                    let event = match maybe_event {
                        Ok(event) => event,
                        Err(e) => {
                            log.log(format!("[SPIDERWEB] Failed to retrieve mDNS event: {e:?}")).await;
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
                            log.log(format!("[SPIDERWEB] Failed to parse discovery. {e:?}")).await;
                            continue;
                        }
                    };

                    // Make sure that the discovery isn't already an active connection
                    let exists = {
                        if discovery.get_identifier() == advertiser.discovery.get_identifier() {
                            true
                        } else {
                            let hashmap = nodes.lock().await;
                            if let Some(node) = hashmap.get(&discovery.get_identifier()) {
                                !node.task.is_finished()
                            } else {
                                false
                            }
                        }
                    };

                    if exists {
                        log.log(String::from("[SPIDERWEB] Found existing or self connection, skipping")).await;
                        continue;
                    }

                    let mode = match advertiser.discovery.decide_server(&discovery).await {
                        Ok(v) => v,
                        Err(e) => {
                            log.log(format!("[SPIDERWEB] Failed to decide server. {e:?}")).await;
                            continue;
                        }
                    };

                    log.log(format!("[SPIDERWEB] Decided mode for new connection: {mode:?}")).await;

                    let hashmap_clone = nodes.clone();
                    let discovery_clone = advertiser.discovery.clone();
                    let semaphore_clone = semaphore.clone();
                    let incoming_sender_clone = incoming_sender.clone();

                    let request_delete_clone = request_delete.clone();

                    let log_clone = log.clone();
                    
                    match mode {
                        Mode::Client => tokio::task::spawn(async move {
                            let permit = semaphore_clone.acquire_owned().await?;
                            let identifier = discovery.get_identifier();
                            match Node::connect(
                                discovery_clone, discovery, channel_size, permit, incoming_sender_clone, request_delete_clone
                            ).await {
                                Ok(node) => {
                                    let mut hashmap = hashmap_clone.lock().await;
                                    let _ = hashmap.insert(identifier, node);
                                    log_clone.log(String::from("[SPIDERWEB INFO] Successfully connected to foreign Node")).await;
                                },
                                Err(e) => {
                                    log_clone.log(format!("[SPIDERWEB] Failed to connect to foreign node as a client. {e:?}")).await;
                                }
                            }
                            Ok::<(), crate::error::Error>(())
                        }),
                        Mode::Server => {
                            log.log(String::from("[SPIDERWEB] Discovered node that should connect.")).await;
                            continue;
                        }
                    };
                }

                // Handle an incoming TCP connection (likely from a foreign client)
                // First, must wait for a packet containing the serialised discovery information
                maybe_request = server.accept() => {

                    log.log(String::from("[SPIDERWEB] Foreign-node attempted connection.")).await;

                    let (socket, _) = match maybe_request {
                        Ok(v) => v,
                        Err(e) => {
                            log.log(format!("[SPIDERWEB] Failed to accept connection request. {e:?}")).await;
                            continue;
                        }
                    };

                    let semaphore_clone = semaphore.clone();
                    let hashmap_clone = nodes.clone();
                    let incoming_sender_clone = incoming_sender.clone();
                    let request_delete_clone = request_delete.clone();

                    tokio::task::spawn(async move {

                        // Take out a permit to enforce concurrency limit
                        let permit = semaphore_clone.acquire_owned().await?;
                        let (node, discovery) = Node::build_and_listen(
                            socket, channel_size, permit, incoming_sender_clone, request_delete_clone
                        ).await?;

                        let mut hashmap = hashmap_clone.lock().await;
                        let _ = hashmap.insert(discovery.get_identifier(), node);

                        Ok::<(), crate::error::Error>(())
                    });
                }

                maybe_request_delete = garbage_collector.recv() => {
                    match maybe_request_delete {

                        // The node by this identifier needs to be terminated
                        Ok(identifier) => {
                            let mut nodes = nodes.lock().await;
                            _ = nodes.remove(&identifier);
                        },

                        // Something went wrong
                        Err(e) => {
                            log.log(format!("[SPIDERWEB] Failed to receive deletion request: {e:?}")).await;
                        }
                    }
                }

                _ = &mut killswitch_receiver => {
                    let mut nodes = nodes.lock().await;
                    nodes.clear();
                    _ = send_killswitch.send(());
                    break;
                }
            }
        }

        let log = log.log.lock().await;
        let log_clone = log.clone();

        Ok(log_clone)

    }
}

/// Client already knows the server it connects to
/// Server needs to wait for the Client to identify itself
#[derive(Clone, Debug)]
pub enum IdentificationMethod {
    Known(Discovery), // the discovery this node is connecting to
    Listen,           // listen for the node to send its identity packet
}

/// The point of contact to a foreign server/client
/// Unified interface, the goal of which is to obfuscate who is the server, and who is the client
pub struct Node {
    send: Sender<Bytes>,
    task: tokio::task::JoinHandle<Res<()>>
}

impl Node {

    /// Connect to a foreign node. This should only be run if it has already been determined that this end is the client.
    pub async fn connect(
        me: Discovery,
        discovery: Discovery,
        channel_size: usize,
        permit: OwnedSemaphorePermit,
        incoming_sender: Sender<(Bytes, String)>,
        request_delete: Sender<String>,
    ) -> Res<Node> {
        // Form a connection to the discovery
        let mut socket = TcpStream::connect(SocketAddrV4::new(discovery.ip, discovery.port)).await?;

        // Send an initial packet with details of self
        let bytes = me.to_bytes()?;
        let size_header = bytes.len() as u32;
        socket.write_all(&size_header.to_be_bytes()).await?;
        socket.write_all(&bytes).await?;

        let (node, _) = Node::build(socket, channel_size, permit, incoming_sender, IdentificationMethod::Known(discovery), request_delete).await?;
        Ok(node)
    }

    pub async fn build(
        stream: TcpStream,
        channel_size: usize,
        permit: OwnedSemaphorePermit,
        incoming_sender: Sender<(Bytes, String)>,
        identification_method: IdentificationMethod,
        request_delete: Sender<String>,
    ) -> Res<(Self, Discovery)> {

        // Build Node
        let (send, queue) = bounded(channel_size);

        // Start processing the Node
        let (mut read_half, write_half) = stream.into_split();

        let discovery = match identification_method {
            IdentificationMethod::Listen => {
                let mut size_buffer: [u8; 4] = [0u8; 4];
                read_half.read_exact(&mut size_buffer).await?;
                let size = u32::from_be_bytes(size_buffer) as usize;
                let mut bytes = BytesMut::with_capacity(size);
                // Buffer the bytes

                while bytes.len() < size {
                    let n = read_half.read_buf(&mut bytes).await?;

                    // EOF
                    if n == 0 {
                        return Err(crate::error::Error::EOF)
                    }
                }

                let bytes = bytes.freeze();

                // This should contain serialised discovery information
                Discovery::from_bytes(bytes)
            },

            IdentificationMethod::Known(discovery) => Ok(discovery)
        }?;

        let (killswitch, terminator) = tokio::sync::oneshot::channel();

        let send_task = tokio::task::spawn(Self::send_task(write_half, queue, terminator));
        let recv_task = tokio::task::spawn(Self::recv_task(read_half, incoming_sender, discovery.get_identifier()));
        let task = tokio::task::spawn(Self::node_heartbeat(permit, discovery.get_identifier(), send_task, recv_task, killswitch, request_delete));

        Ok(
            (
                Node {
                    send,
                    task
                },
                discovery
            )
        )
    }
    
    async fn build_and_listen(
        stream: TcpStream,
        channel_size: usize,
        permit: OwnedSemaphorePermit,
        incoming_sender: Sender<(Bytes, String)>,
        request_delete: Sender<String>,
    ) -> Res<(Node, Discovery)> {
        Node::build(stream, channel_size, permit, incoming_sender, IdentificationMethod::Listen, request_delete).await
    }

    async fn send_task(mut write_half: OwnedWriteHalf, queue: Receiver<Bytes>, mut terminate: tokio::sync::oneshot::Receiver<()>) -> Res<()> {

        loop {
        // while let Ok(bytes) = queue.recv().await {
        
            tokio::select! {
                    
                // Write length header into queue
                maybe_bytes = queue.recv() => {

                    let bytes = match maybe_bytes {
                        Ok(bytes) => bytes,
                        Err(_) => break
                    };

                    let length: u32 = bytes.len() as u32;
                    let header: Bytes = Bytes::copy_from_slice(&length.to_be_bytes());
                    write_half.write_all(&header).await?;

                    // Write payload into queue
                    write_half.write_all(&bytes).await?;
                },

                _ = &mut terminate => {
                    break
                }
            }
        }

        Ok(())
    }

    async fn recv_task(mut read_half: OwnedReadHalf, output: Sender<(Bytes, String)>, identifier: String) -> Res<()> {
        let mut size_buffer = [0u8; 4];
        while read_half.read_exact(&mut size_buffer).await.is_ok() {

            // Process size header
            let size: usize = u32::from_be_bytes(size_buffer) as usize;

            // Buffer the bytes
            let mut bytes = BytesMut::with_capacity(size);

            while bytes.len() < size {
                let n = read_half.read_buf(&mut bytes).await?;

                // EOF
                if n == 0 {
                    return Err(crate::error::Error::EOF)
                }
            }

            // Send bytes object out into the world
            output.send((bytes.freeze(), identifier.clone())).await?;
        }

        Ok(())
    }

    /// Hold a permit from the semaphore and both tasks.
    /// Enforces connection concurrency limit
    async fn node_heartbeat(
        _permit: OwnedSemaphorePermit,
        identifier: String,
        send_task: JoinHandle<Res<()>>,
        recv_task: JoinHandle<Res<()>>,
        killswitch: tokio::sync::oneshot::Sender<()>,
        request_delete: Sender<String>,
    ) -> Res<()> {
        let _ = recv_task.await?;
        let _ = killswitch.send(());
        let _ = send_task.await?;
        request_delete.send(identifier).await?;
        Ok(())
    }
}
