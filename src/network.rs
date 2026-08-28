use std::net::SocketAddrV4;

use tokio::sync::OnceCell;
use tokio::sync::Notify;

/// The server manages the TcpListener for foreign clients accepting
/// and produces a Node upon successful connection
/// Also tracks individual nodes
pub struct Server {

}

/// The point of contact to a foreign server/client
/// Unified interface, the goal of which is to obfuscate who is the server, and who is the client
pub struct Node {

}

impl Node {

    /// The older node will be the server. If the two nodes were created at the same time
    /// The lower value of the bitwise interpretation (u32) of the ipv4 address will be the server
    pub async fn connect(address: SocketAddrV4) {

    }
}
