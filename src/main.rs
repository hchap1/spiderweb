#![allow(clippy::result_large_err)]

mod discovery;
mod network;
mod error;

use network::Server;

#[tokio::main]
async fn main() {
    let server = Server::build("spiderweb", 1234, None, 5, 1024);
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    
    for i in server.get_foreign_identifiers().await {
        println!("Found: {i}");
    }
}
