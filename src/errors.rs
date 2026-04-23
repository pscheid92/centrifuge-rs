use crate::protocol::types::ServerError;

#[derive(Debug, thiserror::Error)]
pub enum CentrifugeError {
    #[error("operation timed out")]
    Timeout,
    #[error("client disconnected")]
    ClientDisconnected,
    #[error("client closed")]
    ClientClosed,
    #[error("subscription unsubscribed")]
    SubscriptionUnsubscribed,
    #[error("subscription to this channel already exists")]
    DuplicateSubscription,
    #[error("unauthorized")]
    Unauthorized,
    #[error("transport error: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("bad configuration: {0}")]
    BadConfiguration(String),
    #[error("server error {}: {}", .0.code, .0.message)]
    Server(ServerError),
}

pub type Result<T> = std::result::Result<T, CentrifugeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[rustfmt::skip]
    fn test_display_all_variants() {
        assert_eq!(CentrifugeError::Timeout.to_string(), "operation timed out");
        assert_eq!(CentrifugeError::ClientDisconnected.to_string(), "client disconnected");
        assert_eq!(CentrifugeError::ClientClosed.to_string(), "client closed");
        assert_eq!(CentrifugeError::SubscriptionUnsubscribed.to_string(), "subscription unsubscribed");
        assert_eq!(CentrifugeError::DuplicateSubscription.to_string(), "subscription to this channel already exists");
        assert_eq!(CentrifugeError::Unauthorized.to_string(), "unauthorized");
        assert_eq!(CentrifugeError::Transport("conn reset".into()).to_string(), "transport error: conn reset");
        assert_eq!(CentrifugeError::Protocol("bad frame".into()).to_string(), "protocol error: bad frame");
        assert_eq!(CentrifugeError::BadConfiguration("missing url".into()).to_string(), "bad configuration: missing url");
        assert_eq!(CentrifugeError::Server(ServerError { code: 100, message: "internal".into(), temporary: false }).to_string(), "server error 100: internal");
    }

    #[test]
    fn test_error_trait() {
        let err: Box<dyn std::error::Error> = Box::new(CentrifugeError::Timeout);
        assert_eq!(err.to_string(), "operation timed out");
    }

    // Transport errors must expose the inner error via std::error::Error::source
    // so downstream code can walk the chain (e.g. anyhow, eyre).
    #[test]
    fn test_transport_preserves_source() {
        use std::error::Error as _;
        let inner: Box<dyn std::error::Error + Send + Sync> = "connection refused".into();
        let err = CentrifugeError::Transport(inner);
        let src = err.source().expect("Transport must expose source");
        assert_eq!(src.to_string(), "connection refused");
        // Display still shows the inner message for convenience.
        assert_eq!(err.to_string(), "transport error: connection refused");
    }
}
