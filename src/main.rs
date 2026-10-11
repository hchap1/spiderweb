#![allow(clippy::result_large_err)]

mod discovery;
mod network;
mod error;

use crate::network::Server;
use crate::error::Res;

#[tokio::main]
async fn main() -> Res<()> {

    let server = Server::build("spiderweb_test", 12345, Some(String::from("PC")), 10, 100);
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    while server.get_foreign_identifiers().await.is_empty() {
        println!("Waiting for a client...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    let clients = server.get_foreign_identifiers().await;
    println!("Received clients:");
    for client in &clients {
        println!("{}", client);
    }

    for client in clients {

    }

    Ok(())
}
