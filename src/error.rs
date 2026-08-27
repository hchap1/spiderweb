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
}

pub type Res<T> = Result<T, Error>;
