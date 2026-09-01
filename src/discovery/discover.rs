use std::net::Ipv4Addr;
use bytes::Bytes;
use mdns_sd::ResolvedService;

use crate::error::Res;
use crate::error::Error;

#[derive(Debug, Clone, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[rkyv()]
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

    /// Load from a resolved service
    #[allow(clippy::boxed_local)]
    pub fn from_resolved_service(resolved_service: Box<ResolvedService>) -> Res<Discovery> {
        let ipv4 = resolved_service.get_addresses_v4().into_iter().next().ok_or(Error::ResolutionWithoutAddress)?;
        let port = resolved_service.port;
        let instantiation_timestamp = resolved_service
            .txt_properties.get_property_val_str("instantiation_timestamp")
            .ok_or(Error::MissingProperty)?
            .parse::<u64>()
            .map_err(|_| Error::MissingProperty)?;

        let nickname = resolved_service
            .txt_properties.get_property_val_str("nickname")
            .map(String::from);

        Ok(Discovery {
            ip: ipv4,
            port,
            instantiation_timestamp,
            nickname
        })
    }

    /// The older node will be the server. If the two nodes were created at the same time
    /// The lower value of the bitwise interpretation (u32) of the ipv4 address will be the server
    pub async fn decide_server(self: &Discovery, other: &Discovery) -> Res<Mode> {
        println!("Comparing: {self:?}, {other:?}");
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

    pub fn get_identifier(&self) -> String {
        format!("{}-{}-{}", self.ip, self.port, self.instantiation_timestamp)
    }

    pub fn to_bytes(&self) -> Res<Bytes> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)?;
        Ok(Bytes::from_owner(bytes))
    }

    pub fn from_bytes(bytes: Bytes) -> Res<Self> {
        let archived = rkyv::access::<ArchivedDiscovery, rkyv::rancor::Error>(&bytes)?;
        let original = rkyv::deserialize::<Self, rkyv::rancor::Error>(archived)?;
        Ok(original)
    }
}
