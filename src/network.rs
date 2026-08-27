use std::net::SocketAddrV4;


/// The point of contact to a foreign server/client
/// Unified interface, the goal of which is to obfuscate who is the server, and who is the client
pub struct Node {

}

impl Node {

    /// Connects to another node. The server will be client with the lower value of the function
    /// which is the sum of the 4 byte IP. To resolve collisions, the OLDER node will be the server
    pub async fn connect(address: SocketAddrV4) {

    }
}
