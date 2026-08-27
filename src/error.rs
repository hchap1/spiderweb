use thiserror::Error;

use crate::discovery::register::Advertiser;

#[derive(Debug, Error)]
pub enum Error {
    #[error("MDNS error: {:?}", .0)]
    MdnsError(#[from] mdns_sd::Error),

    #[error("OnceCell init failed: {:?}", .0)]
    TokioOnceCellError(#[from] tokio::sync::SetError<Advertiser>),
}

pub type Res<T> = Result<T, Error>;
