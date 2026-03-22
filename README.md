# centrifuge-client

[![Crates.io](https://img.shields.io/crates/v/centrifuge-client.svg)](https://crates.io/crates/centrifuge-client)
[![docs.rs](https://docs.rs/centrifuge-client/badge.svg)](https://docs.rs/centrifuge-client)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Rust client SDK for [Centrifugo](https://github.com/centrifugal/centrifugo) server and [Centrifuge](https://github.com/centrifugal/centrifuge) library.

> This SDK behaves according to the [Centrifuge client SDK specification](https://centrifugal.dev/docs/transports/client_api). It's recommended to read that document first as it covers common behavior — client and subscription state transitions, options, and methods.

The features implemented by this SDK can be found in the [SDK feature matrix](https://centrifugal.dev/docs/transports/client_sdk#sdk-feature-matrix).

> `centrifuge-client` is compatible with [Centrifugo](https://github.com/centrifugal/centrifugo) server v6, v5, and v4, and [Centrifuge](https://github.com/centrifugal/centrifuge) >= 0.25.0.

* [Install](#install)
* [Quick start](#quick-start)
* [Client API](#client-api)
* [Subscription API](#subscription-api)
* [Authentication](#authentication)
* [Server-side subscriptions](#server-side-subscriptions)
* [Command batching](#command-batching)
* [Protobuf support](#protobuf-support)
* [Feature flags](#feature-flags)
* [Custom transport](#custom-transport)
* [SDK specification compliance](#sdk-specification-compliance)
* [Run tests](#run-tests)

## Install

```toml
[dependencies]
centrifuge-client = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Quick start

```rust
use centrifuge_client::{Client, ClientConfig, SubEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientConfig::new("ws://localhost:8000/connection/websocket"));

    let (sub, mut events) = client.subscribe("news").await?;
    client.connect().await?;

    while let Some(event) = events.recv().await {
        match event {
            SubEvent::Publication(pub_data) => println!("{:?}", pub_data.data),
            SubEvent::Subscribed(ctx) => println!("subscribed to {}", ctx.channel),
            _ => {}
        }
    }

    Ok(())
}
```

## Client API

**Methods:** `connect()`, `disconnect()`, `close()`, `publish(channel, data)`, `send(data)`, `rpc(method, data)`, `history(channel, opts)`, `presence(channel)`, `presence_stats(channel)`, `set_token(token)`, `set_data(data)`, `start_batching()`, `stop_batching()`, `events()`, `state()`, `subscriptions()`

**Events:** `Connected`, `Connecting`, `Disconnected`, `Error`, `Message`, `ServerSubscribed`, `ServerSubscribing`, `ServerUnsubscribed`, `ServerPublication`, `ServerJoin`, `ServerLeave`

**Options:** `token`, `get_token`, `data`, `get_data`, `name`, `version`, `protocol_type`, `timeout`, `min_reconnect_delay`, `max_reconnect_delay`, `max_server_ping_delay`, `header`

## Subscription API

**Methods:** `subscribe()`, `unsubscribe()`, `publish(data)`, `history(opts)`, `presence()`, `presence_stats()`, `events()`, `channel()`

**Events:** `Subscribed`, `Subscribing`, `Unsubscribed`, `Publication`, `Join`, `Leave`, `Error`

**Options:** `token`, `get_token`, `data`, `get_data`, `recoverable`, `delta`, `join_leave`, `since`

## Authentication

JWT tokens for connection authentication. Tokens can be set statically or refreshed via async callback:

```rust
use centrifuge_client::{ClientConfig, get_token_fn};

let config = ClientConfig::new("ws://localhost:8000/connection/websocket")
    .token("initial-jwt-token")
    .get_token(get_token_fn(|| async {
        // Called when the token expires. Return Err(Unauthorized) to disconnect.
        Ok("refreshed-jwt-token".to_string())
    }));
```

Subscription tokens work the same way via `SubscriptionConfig::token()` and `SubscriptionConfig::get_token()`.

## Server-side subscriptions

Server-side subscriptions are managed by the server and delivered to the client on connect. Listen via client events:

```rust
use centrifuge_client::ClientEvent;

let mut events = client.events()?;
while let Some(event) = events.recv().await {
    match event {
        ClientEvent::ServerSubscribed(ctx) => println!("server sub: {}", ctx.channel),
        ClientEvent::ServerPublication(ctx) => println!("data on {}", ctx.channel),
        _ => {}
    }
}
```

## Command batching

Batch multiple commands into a single WebSocket frame to reduce round-trips:

```rust
client.start_batching();
sub1.subscribe().await?;
sub2.subscribe().await?;
sub3.subscribe().await?;
client.stop_batching();  // All three subscribes sent as one frame
```

## Protobuf support

Both JSON (default) and Protobuf encodings are supported. Select at configuration time:

```rust
use centrifuge_client::{ClientConfig, ProtocolType};

let config = ClientConfig::new("ws://localhost:8000/connection/websocket")
    .protocol_type(ProtocolType::Protobuf);
```

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `native-tls` | Yes | TLS via the platform's native library (OpenSSL / Schannel / Secure Transport) |
| `rustls` | No | TLS via rustls (pure Rust, uses Mozilla certificate bundle) |

The features are mutually exclusive. To use rustls:

```toml
[dependencies]
centrifuge-client = { version = "0.1", default-features = false, features = ["rustls"] }
```

## Custom transport

The WebSocket transport can be replaced by implementing the `Transport` trait:

```rust
use centrifuge_client::transport::{Transport, TransportConn, TransportError, BoxFuture};

struct MyTransport;

impl Transport for MyTransport {
    fn connect(&self) -> BoxFuture<'_, Result<TransportConn, TransportError>> {
        Box::pin(async { todo!() })
    }
}

let client = Client::new_with_transport(config, Box::new(MyTransport));
```

## SDK specification compliance

This SDK implements 139/139 requirements from the [Centrifuge Client SDK specification](https://centrifugal.dev/docs/transports/client_api). See [SDK_COMPLIANCE.md](SDK_COMPLIANCE.md) for the full mapping.

## Run tests

Unit and actor tests (no Docker needed):

```
cargo test --lib --features native-tls
cargo test --test actor_commands --test actor_connection --test actor_server_subs --test actor_state --test actor_subscriptions --test actor_token --features native-tls
```

Integration tests (requires Docker):

```
cargo test --test integration_test --test integration_advanced_test --features native-tls
```

## License

MIT
