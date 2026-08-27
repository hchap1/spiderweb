use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("MDNS error: {:?}", .0)]
    MdnsError(#[from] mdns_sd::Error)
}

pub type Res<T> = Result<T, Error>;
