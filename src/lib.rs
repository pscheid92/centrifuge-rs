//! # centrifuge-client
//!
//! Rust client SDK for the [Centrifuge](https://centrifugal.dev/) real-time messaging protocol.
//!
//! Connect to [Centrifugo](https://github.com/centrifugal/centrifugo) or any Centrifuge-based
//! server over WebSocket, subscribe to channels, and receive publications in real time.
//!
//! ## Quick Start
//!
//! ```no_run
//! use centrifuge_client::{Client, ClientConfig, SubEvent};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::new(ClientConfig::new("ws://localhost:8000/connection/websocket"));
//!
//! let (sub, mut events) = client.subscribe("chat").await?;
//! client.connect().await?;
//!
//! while let Some(event) = events.recv().await {
//!     match event {
//!         SubEvent::Publication(pub_data) => println!("{} bytes", pub_data.data.len()),
//!         SubEvent::Subscribed(ctx) => println!("subscribed to {}", ctx.channel),
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The main entry points are [`Client`] and [`Subscription`]. See [`ClientConfig`] and
//! [`SubscriptionConfig`] for configuration options.

#[cfg(all(feature = "native-tls", feature = "rustls"))]
compile_error!("Features `native-tls` and `rustls` are mutually exclusive. Enable only one.");

#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
compile_error!("Either the `native-tls` or `rustls` feature must be enabled for TLS support.");

pub(crate) mod backoff;
pub mod client;
pub(crate) mod codec;
pub(crate) mod codes;
pub mod config;
pub(crate) mod delta;
pub mod errors;
pub(crate) mod protocol;
pub mod subscription;
pub mod transport;

// Core types
pub use client::Client;
pub use errors::CentrifugeError;
pub use subscription::Subscription;

// Configuration
pub use config::{
    ClientConfig, DeltaType, ProtocolType, SubscriptionConfig, get_data_fn, get_sub_data_fn, get_sub_token_fn,
    get_token_fn,
};

// Event types
pub use protocol::types::{
    ClientEvent,
    // Data types
    ClientInfo,
    ClientState,
    // Client event contexts
    ConnectedContext,
    ConnectingContext,
    DisconnectedContext,
    ErrorContext,
    // Operation results
    HistoryOptions,
    HistoryResult,
    // Legacy contexts (for direct construction)
    JoinContext,
    LeaveContext,
    MessageContext,
    PresenceResult,
    PresenceStatsResult,
    Publication,
    PublicationContext,
    RpcResult,
    ServerError,
    ServerJoinContext,
    ServerLeaveContext,
    ServerPublicationContext,
    // Server-side subscription event contexts
    ServerSubscribedContext,
    ServerSubscribingContext,
    ServerUnsubscribedContext,
    StreamPosition,
    SubEvent,
    // Subscription event contexts
    SubscribedContext,
    SubscribingContext,
    SubscriptionState,
    UnsubscribedContext,
};

// Protocol
pub use protocol::proto::FilterNode;

// Compile-time assertions: Client and Subscription must be Send + Sync
// for safe use across threads in async applications.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Client>();
    assert_send_sync::<Subscription>();
};
