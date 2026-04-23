use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::errors::CentrifugeError;
use crate::protocol::types::StreamPosition;

/// A boxed, `Send`-able future — shorthand for callback return types.
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Protocol encoding format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolType {
    #[default]
    Json,
    Protobuf,
}

// ---------------------------------------------------------------------------
// Callback types and helpers
// ---------------------------------------------------------------------------

/// Async callback for obtaining/refreshing a connection token.
///
/// Held behind `Arc` so `ClientConfig`/`SubscriptionConfig` can be `Clone`.
pub type GetTokenFn = Arc<dyn Fn() -> BoxFuture<Result<String, CentrifugeError>> + Send + Sync>;

/// Async callback for obtaining/refreshing a subscription token.
pub type GetSubscriptionTokenFn = Arc<dyn Fn(String) -> BoxFuture<Result<String, CentrifugeError>> + Send + Sync>;

/// Async callback for obtaining fresh connection data on each connect attempt.
pub type GetDataFn = Arc<dyn Fn() -> BoxFuture<Result<Vec<u8>, CentrifugeError>> + Send + Sync>;

/// Async callback for obtaining fresh subscription data on each subscribe attempt.
pub type GetSubDataFn = Arc<dyn Fn(String) -> BoxFuture<Result<Vec<u8>, CentrifugeError>> + Send + Sync>;

/// Helper to create a [`GetTokenFn`] without double-boxing boilerplate.
///
/// ```ignore
/// let config = ClientConfig::new(url)
///     .get_token(get_token_fn(|| async {
///         Ok(fetch_token().await?)
///     }));
/// ```
pub fn get_token_fn<F, Fut>(f: F) -> GetTokenFn
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, CentrifugeError>> + Send + 'static,
{
    Arc::new(move || Box::pin(f()))
}

/// Helper to create a [`GetSubscriptionTokenFn`] without double-boxing boilerplate.
pub fn get_sub_token_fn<F, Fut>(f: F) -> GetSubscriptionTokenFn
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, CentrifugeError>> + Send + 'static,
{
    Arc::new(move |channel| Box::pin(f(channel)))
}

/// Helper to create a [`GetDataFn`] without double-boxing boilerplate.
pub fn get_data_fn<F, Fut>(f: F) -> GetDataFn
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u8>, CentrifugeError>> + Send + 'static,
{
    Arc::new(move || Box::pin(f()))
}

/// Helper to create a [`GetSubDataFn`] without double-boxing boilerplate.
pub fn get_sub_data_fn<F, Fut>(f: F) -> GetSubDataFn
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u8>, CentrifugeError>> + Send + 'static,
{
    Arc::new(move |channel| Box::pin(f(channel)))
}

/// Configuration for creating a Client.
#[derive(Clone)]
pub struct ClientConfig {
    pub url: String,
    pub protocol_type: ProtocolType,
    pub token: String,
    pub get_token: Option<GetTokenFn>,
    pub data: Vec<u8>,
    pub get_data: Option<GetDataFn>,
    pub name: String,
    pub version: String,
    pub min_reconnect_delay: Duration,
    pub max_reconnect_delay: Duration,
    pub timeout: Duration,
    pub max_server_ping_delay: Duration,
    pub headers: HashMap<String, String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            protocol_type: ProtocolType::Json,
            token: String::new(),
            get_token: None,
            data: Vec::new(),
            get_data: None,
            name: "rs".into(),
            version: String::new(),
            min_reconnect_delay: Duration::from_millis(500),
            max_reconnect_delay: Duration::from_secs(20),
            timeout: Duration::from_secs(5),
            max_server_ping_delay: Duration::from_secs(10),
            headers: HashMap::new(),
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

    /// Validate the configuration. Called automatically via `debug_assert!`
    /// in `Client::new`, but exposed for callers who want to proactively
    /// surface misconfiguration in release builds too.
    pub fn validate(&self) -> Result<(), CentrifugeError> {
        if self.url.is_empty() {
            return Err(CentrifugeError::BadConfiguration("url must not be empty".into()));
        }
        if self.min_reconnect_delay > self.max_reconnect_delay {
            return Err(CentrifugeError::BadConfiguration(
                "min_reconnect_delay must be <= max_reconnect_delay".into(),
            ));
        }
        if self.timeout.is_zero() {
            return Err(CentrifugeError::BadConfiguration("timeout must be > 0".into()));
        }
        Ok(())
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

    pub fn get_data(mut self, f: GetDataFn) -> Self {
        self.get_data = Some(f);
        self
    }

    /// Set client name for analytics (max 16 characters, truncated if longer).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        let mut name = name.into();
        name.truncate(16);
        self.name = name;
        self
    }

    /// Set client version for analytics (max 64 characters, truncated if longer).
    pub fn version(mut self, version: impl Into<String>) -> Self {
        let mut version = version.into();
        version.truncate(64);
        self.version = version;
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

    /// Add a custom header to the WebSocket upgrade request.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// Delta compression type for subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaType {
    #[default]
    None,
    Fossil,
}

/// Configuration for creating a Subscription.
#[derive(Clone)]
pub struct SubscriptionConfig {
    pub token: String,
    pub get_token: Option<GetSubscriptionTokenFn>,
    pub data: Vec<u8>,
    pub get_data: Option<GetSubDataFn>,
    pub positioned: bool,
    pub recoverable: bool,
    pub join_leave: bool,
    pub delta: DeltaType,
    pub tags_filter: Option<crate::protocol::proto::FilterNode>,
    pub min_resubscribe_delay: Duration,
    pub max_resubscribe_delay: Duration,
    pub since: Option<StreamPosition>,
}

impl SubscriptionConfig {
    /// Validate the configuration. Called automatically via `debug_assert!`
    /// in `Client::new_subscription`.
    pub fn validate(&self) -> Result<(), CentrifugeError> {
        if self.min_resubscribe_delay > self.max_resubscribe_delay {
            return Err(CentrifugeError::BadConfiguration(
                "min_resubscribe_delay must be <= max_resubscribe_delay".into(),
            ));
        }
        Ok(())
    }

    pub fn recoverable(mut self) -> Self {
        self.recoverable = true;
        self
    }

    pub fn delta(mut self, delta: DeltaType) -> Self {
        self.delta = delta;
        self
    }

    pub fn join_leave(mut self) -> Self {
        self.join_leave = true;
        self
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }

    pub fn get_token(mut self, f: GetSubscriptionTokenFn) -> Self {
        self.get_token = Some(f);
        self
    }

    pub fn since(mut self, pos: StreamPosition) -> Self {
        self.since = Some(pos);
        self
    }

    pub fn data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    pub fn get_data(mut self, f: GetSubDataFn) -> Self {
        self.get_data = Some(f);
        self
    }
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            token: String::new(),
            get_token: None,
            data: Vec::new(),
            get_data: None,
            positioned: false,
            recoverable: false,
            join_leave: false,
            delta: DeltaType::None,
            tags_filter: None,
            min_resubscribe_delay: Duration::from_millis(500),
            max_resubscribe_delay: Duration::from_secs(20),
            since: None,
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

    #[test]
    fn test_client_config_get_token() {
        let config = ClientConfig::new("ws://test").get_token(get_token_fn(|| async { Ok("token".into()) }));
        assert!(config.get_token.is_some());
    }

    #[test]
    fn test_client_config_get_data() {
        let config = ClientConfig::new("ws://test").get_data(get_data_fn(|| async { Ok(b"data".to_vec()) }));
        assert!(config.get_data.is_some());
    }

    #[test]
    fn test_client_name_truncation() {
        let config = ClientConfig::new("ws://test").name("this-name-is-way-too-long-for-the-limit");
        assert_eq!(config.name.len(), 16);
        assert_eq!(config.name, "this-name-is-way");
    }

    #[test]
    fn test_client_version_truncation() {
        let long_version = "v".repeat(100);
        let config = ClientConfig::new("ws://test").version(long_version);
        assert_eq!(config.version.len(), 64);
    }

    #[test]
    fn test_subscription_config_builders() {
        let config = SubscriptionConfig::default()
            .recoverable()
            .delta(DeltaType::Fossil)
            .join_leave()
            .token("sub-token")
            .data(b"sub-data".to_vec())
            .since(StreamPosition {
                offset: 10,
                epoch: "e1".into(),
            });

        assert!(config.recoverable);
        assert_eq!(config.delta, DeltaType::Fossil);
        assert!(config.join_leave);
        assert_eq!(config.token, "sub-token");
        assert_eq!(config.data, b"sub-data");
        let since = config.since.unwrap();
        assert_eq!(since.offset, 10);
        assert_eq!(since.epoch, "e1");
    }

    #[test]
    fn test_subscription_config_get_token() {
        let config = SubscriptionConfig::default().get_token(get_sub_token_fn(|_ch| async { Ok("t".into()) }));
        assert!(config.get_token.is_some());
    }

    #[test]
    fn test_subscription_config_get_data() {
        let config = SubscriptionConfig::default().get_data(get_sub_data_fn(|_ch| async { Ok(b"d".to_vec()) }));
        assert!(config.get_data.is_some());
    }

    #[test]
    fn test_delta_type_default() {
        assert_eq!(DeltaType::default(), DeltaType::None);
    }

    #[test]
    fn test_client_config_clone_preserves_callbacks() {
        let config = ClientConfig::new("ws://test")
            .get_token(get_token_fn(|| async { Ok("t".into()) }))
            .get_data(get_data_fn(|| async { Ok(b"d".to_vec()) }));
        let cloned = config.clone();
        assert!(cloned.get_token.is_some());
        assert!(cloned.get_data.is_some());
        assert_eq!(cloned.url, "ws://test");
    }

    #[test]
    fn test_client_config_validate_rejects_empty_url() {
        let cfg = ClientConfig::default();
        assert!(matches!(cfg.validate(), Err(CentrifugeError::BadConfiguration(_))));
    }

    #[test]
    fn test_client_config_validate_rejects_min_gt_max_delay() {
        let cfg = ClientConfig::new("ws://test")
            .min_reconnect_delay(Duration::from_secs(30))
            .max_reconnect_delay(Duration::from_secs(5));
        assert!(matches!(cfg.validate(), Err(CentrifugeError::BadConfiguration(_))));
    }

    #[test]
    fn test_client_config_validate_rejects_zero_timeout() {
        let cfg = ClientConfig::new("ws://test").timeout(Duration::ZERO);
        assert!(matches!(cfg.validate(), Err(CentrifugeError::BadConfiguration(_))));
    }

    #[test]
    fn test_client_config_validate_accepts_good_config() {
        assert!(ClientConfig::new("ws://localhost").validate().is_ok());
    }

    #[test]
    fn test_subscription_config_clone_preserves_callbacks() {
        let config = SubscriptionConfig::default()
            .get_token(get_sub_token_fn(|_| async { Ok("t".into()) }))
            .get_data(get_sub_data_fn(|_| async { Ok(b"d".to_vec()) }));
        let cloned = config.clone();
        assert!(cloned.get_token.is_some());
        assert!(cloned.get_data.is_some());
    }
}
