use bytes::Bytes;
use thiserror::Error;

use crate::discovery::register::Advertiser;

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[error("MDNS error: {:?}", .0)]
    MdnsError(#[from] mdns_sd::Error),

    #[error("OnceCell init failed: {:?}", .0)]
    TokioOnceCellError(#[from] tokio::sync::SetError<Advertiser>),

    #[error("Tokio join error: {:?}", .0)]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("Local ip address error: {:?}", .0)]
    LocalIpAddressError(#[from] local_ip_address::Error),

    #[error("SystemTimeError: {:?}", .0)]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("Collision on deciding server")]
    ServerCMPCollision,

    #[error("Do not support ipv6")]
    DoNotSupportIPV6,

    #[error("IO error (STD): {:?}", .0)]
    StdIoError(#[from] std::io::Error),

    #[error("AsyncChannel Send Error")]
    SendError,

    #[error("AsyncChannel Recv Error: {:?}", .0)]
    RecvError(#[from] async_channel::RecvError),

    #[error("mDNS resolution succeeded but no address was found")]
    ResolutionWithoutAddress,

    #[error("Missing mDNS property")]
    MissingProperty,

    #[error("Unable to acquire a permit from semaphore")]
    TokioAcquireError(#[from] tokio::sync::AcquireError),

    #[error("Failed to serialise: {:?}", .0)]
    RancorError(#[from] rkyv::rancor::Error),

    #[error("Failed to receive first packet from client")]
    NoFirstPacket,
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_value: async_channel::SendError<T>) -> Self {
        Self::SendError
    }
}

pub type Res<T> = Result<T, Error>;
