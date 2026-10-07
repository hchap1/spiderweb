#![allow(clippy::result_large_err)]

mod discovery;
mod network;
mod error;

use crate::discovery::register::register;
use crate::error::Res;

#[tokio::main]
async fn main() -> Res<()> {
    let advertiser = register("mdnstest", 19000_u16, None).await?;
    let receiver = advertiser.get_event_stream()?;

    while let Ok(event) = receiver.recv().await {
        println!("EVENT: {event:?}");
    }

    Ok(())
}
