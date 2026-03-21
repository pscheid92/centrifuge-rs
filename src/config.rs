use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::errors::CentrifugeError;
use crate::events::{ClientEventHandlers, SubscriptionEventHandlers};
use crate::protocol::types::StreamPosition;

/// Protocol encoding format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolType {
    #[default]
    Json,
    Protobuf,
}

/// Async callback for obtaining/refreshing a connection token.
pub type GetTokenFn =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String, CentrifugeError>> + Send>> + Send + Sync>;

/// Async callback for obtaining/refreshing a subscription token.
pub type GetSubscriptionTokenFn = Box<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<String, CentrifugeError>> + Send>>
        + Send
        + Sync,
>;

/// Configuration for creating a Client.
pub struct ClientConfig {
    pub url: String,
    pub protocol_type: ProtocolType,
    pub token: String,
    pub get_token: Option<GetTokenFn>,
    pub data: Vec<u8>,
    pub name: String,
    pub version: String,
    pub min_reconnect_delay: Duration,
    pub max_reconnect_delay: Duration,
    pub timeout: Duration,
    pub max_server_ping_delay: Duration,
    pub events: ClientEventHandlers,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            protocol_type: ProtocolType::Json,
            token: String::new(),
            get_token: None,
            data: Vec::new(),
            name: "rs".into(),
            version: String::new(),
            min_reconnect_delay: Duration::from_millis(500),
            max_reconnect_delay: Duration::from_secs(20),
            timeout: Duration::from_secs(5),
            max_server_ping_delay: Duration::from_secs(10),
            events: ClientEventHandlers::default(),
        }
    }
}

impl ClientConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn protocol_type(mut self, pt: ProtocolType) -> Self {
        self.protocol_type = pt;
        self
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }

    pub fn get_token(mut self, f: GetTokenFn) -> Self {
        self.get_token = Some(f);
        self
    }

    pub fn data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn min_reconnect_delay(mut self, d: Duration) -> Self {
        self.min_reconnect_delay = d;
        self
    }

    pub fn max_reconnect_delay(mut self, d: Duration) -> Self {
        self.max_reconnect_delay = d;
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    pub fn max_server_ping_delay(mut self, d: Duration) -> Self {
        self.max_server_ping_delay = d;
        self
    }

    pub fn events(mut self, events: ClientEventHandlers) -> Self {
        self.events = events;
        self
    }
}

/// Configuration for creating a Subscription.
pub struct SubscriptionConfig {
    pub token: String,
    pub get_token: Option<GetSubscriptionTokenFn>,
    pub data: Vec<u8>,
    pub positioned: bool,
    pub recoverable: bool,
    pub join_leave: bool,
    pub min_resubscribe_delay: Duration,
    pub max_resubscribe_delay: Duration,
    pub since: Option<StreamPosition>,
    pub events: SubscriptionEventHandlers,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            token: String::new(),
            get_token: None,
            data: Vec::new(),
            positioned: false,
            recoverable: false,
            join_leave: false,
            min_resubscribe_delay: Duration::from_millis(500),
            max_resubscribe_delay: Duration::from_secs(20),
            since: None,
            events: SubscriptionEventHandlers::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_builder() {
        let config = ClientConfig::new("ws://localhost:8000")
            .protocol_type(ProtocolType::Protobuf)
            .token("jwt-token")
            .data(b"hello".to_vec())
            .name("myapp")
            .version("1.0.0")
            .min_reconnect_delay(Duration::from_secs(1))
            .max_reconnect_delay(Duration::from_secs(30))
            .timeout(Duration::from_secs(10))
            .max_server_ping_delay(Duration::from_secs(5));

        assert_eq!(config.url, "ws://localhost:8000");
        assert_eq!(config.protocol_type, ProtocolType::Protobuf);
        assert_eq!(config.token, "jwt-token");
        assert_eq!(config.data, b"hello");
        assert_eq!(config.name, "myapp");
        assert_eq!(config.version, "1.0.0");
        assert_eq!(config.min_reconnect_delay, Duration::from_secs(1));
        assert_eq!(config.max_reconnect_delay, Duration::from_secs(30));
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.max_server_ping_delay, Duration::from_secs(5));
    }

    #[test]
    fn test_client_config_defaults() {
        let config = ClientConfig::default();
        assert_eq!(config.name, "rs");
        assert_eq!(config.min_reconnect_delay, Duration::from_millis(500));
        assert_eq!(config.max_reconnect_delay, Duration::from_secs(20));
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.max_server_ping_delay, Duration::from_secs(10));
        assert!(config.token.is_empty());
        assert!(config.get_token.is_none());
    }

    #[test]
    fn test_subscription_config_defaults() {
        let config = SubscriptionConfig::default();
        assert!(config.token.is_empty());
        assert!(config.get_token.is_none());
        assert!(!config.positioned);
        assert!(!config.recoverable);
        assert!(!config.join_leave);
        assert!(config.since.is_none());
    }

    #[test]
    fn test_protocol_type_default() {
        assert_eq!(ProtocolType::default(), ProtocolType::Json);
    }
}
