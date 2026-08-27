use std::net::SocketAddrV4;

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
