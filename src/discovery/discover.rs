use std::net::Ipv4Addr;
use crate::error::Res;
use crate::error::Error;

pub struct Discovery {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub instantiation_timestamp: u64,
    pub nickname: Option<String>
}

pub enum Mode {
    Server,
    Client
}

impl Discovery {

    /// The older node will be the server. If the two nodes were created at the same time
    /// The lower value of the bitwise interpretation (u32) of the ipv4 address will be the server
    pub async fn decide_server(self: &Discovery, other: &Discovery) -> Res<Mode> {
        if self.instantiation_timestamp < other.instantiation_timestamp {
            Ok(Mode::Server)
        } else if other.instantiation_timestamp < self.instantiation_timestamp {
            Ok(Mode::Client)
        } else {
            // Same age, use IP comparison instead
            if self.ip.to_bits() < other.ip.to_bits() {
                Ok(Mode::Server)
            } else if other.ip.to_bits() < self.ip.to_bits() {
                Ok(Mode::Client)
            } else {
                Err(Error::ServerCMPCollision)
            }
        }
    }
}
