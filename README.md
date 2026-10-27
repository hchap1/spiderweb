# spiderweb-async

[![Crates.io](https://img.shields.io/crates/v/spiderweb-async.svg)](https://crates.io/crates/spiderweb-async)
[![Documentation](https://docs.rs/spiderweb-async/badge.svg)](https://docs.rs/spiderweb-async)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A minimal, async Rust crate for peer-to-peer connections on a local network. Nodes find each other automatically via **mDNS** and exchange length-framed binary messages over **TCP** — no central server, no manual IP configuration, no signaling service.

Point every instance of your application at the same `application_name` and they'll discover each other, agree on who dials whom, and start exchanging bytes.

## Features

- **Zero-configuration discovery** — peers advertise and browse each other via mDNS (`_<application_name>._tcp.local.`), so there's nothing to configure beyond a shared name.
- **Deterministic connection roles** — when two peers discover each other, they independently agree on who connects out and who accepts, with no handshake race or coordination needed.
- **Length-prefixed framing** — every message on the wire is a 4-byte big-endian length header followed by the payload, so arbitrary `Bytes` payloads are read and written cleanly.
- **Concurrency-limited** — a semaphore caps the number of simultaneously active peer connections.
- **Backpressure-aware channels** — incoming and outgoing traffic flow through bounded `async-channel` queues, sized by you.
- **Graceful shutdown** — tear down mDNS advertising and every active connection cleanly, and get a diagnostic log back.

## How it works

1. **Advertise & browse.** On startup, a `Server` registers an mDNS service for `application_name` carrying its instantiation timestamp and an optional nickname, and starts browsing for other instances of the same service.
2. **Elect server/client.** When a peer is discovered, both sides run the same deterministic rule to decide the TCP connection's direction: the older instance (by instantiation timestamp) becomes the **server** (waits to be dialed); on a timestamp tie, the lower IPv4 address wins. This means both peers reach the same decision independently, without talking to each other first.
3. **Handshake.** The connecting side immediately sends a serialized `Discovery` record (its IP, port, timestamp, and nickname) over the socket, so the accepting side learns who just connected.
4. **Exchange messages.** From then on, each connection is a `Node`: a dedicated read task and write task shuttle length-framed `Bytes` messages in and out, identified by a stable string key derived from the peer's discovery info (`ip-port-timestamp`).
5. **Garbage collection.** If a connection's read side closes, the node tears itself down, releases its concurrency permit, and removes itself from the active peer set.

## Installation

```sh
cargo add spiderweb-async
```

## Quick start

```rust
use bytes::Bytes;
use spiderweb_async::network::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // application_name, port, nickname, concurrency_limit, channel_size
    let mut server = Server::build("my-app", 9000, Some("my-nickname".into()), 8, 32);

    // Broadcast a message to every currently connected peer.
    server.outgoing.send((Bytes::from("hello, mesh"), vec![])).await?;

    // React to incoming messages from any peer.
    while let Ok((bytes, from)) = server.incoming.recv().await {
        println!("received {} bytes from {from}", bytes.len());
    }

    Ok(())
}
```

Sending to a specific peer, instead of broadcasting:

```rust
let peers = server.get_foreign_identifiers().await;
if let Some(peer) = peers.first() {
    server.outgoing.send((Bytes::from("just for you"), vec![peer.clone()])).await?;
}
```

Shutting down cleanly:

```rust
let log = server.graceful_shutdown().await?;
```

## API overview

`Server::build(application_name, port, nickname, concurrency_limit, channel_size)` spins up mDNS advertising/discovery and the TCP listener in the background, and returns a handle with:

| Member | Type | Purpose |
|---|---|---|
| `incoming` | `Receiver<(Bytes, String)>` | Messages received from peers, tagged with the sending peer's identifier. |
| `outgoing` | `Sender<(Bytes, Vec<String>)>` | Messages to send. An empty identifier list broadcasts to every connected peer; a non-empty list targets specific peers. |
| `get_foreign_identifiers()` | `async fn -> Vec<String>` | Snapshot of currently connected peer identifiers. |
| `graceful_shutdown()` | `async fn -> Res<Log>` | Stops advertising, closes every connection, and returns the server's internal log. |

All fallible operations return `spiderweb_async::error::Res<T>` (an alias for `Result<T, Error>`), where `Error` wraps the underlying mDNS, I/O, channel, and (de)serialization failures via [`thiserror`](https://docs.rs/thiserror).

## Notes & limitations

- Discovery relies on mDNS, so peers must be reachable on the same local network/broadcast domain; it is not designed for WAN use.
- Only IPv4 is supported — an IPv6-only host will fail discovery setup.
- Payloads are opaque `Bytes`; this crate handles framing and transport only — define and (de)serialize your own message format on top (the crate itself uses [`rkyv`](https://docs.rs/rkyv) internally just for the discovery handshake).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
