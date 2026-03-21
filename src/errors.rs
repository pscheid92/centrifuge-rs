use std::fmt;

use crate::protocol::types::ServerError;

#[derive(Debug)]
pub enum CentrifugeError {
    Timeout,
    ClientDisconnected,
    ClientClosed,
    SubscriptionUnsubscribed,
    DuplicateSubscription,
    Unauthorized,
    Transport(String),
    Protocol(String),
    BadConfiguration(String),
    Server(ServerError),
}

impl fmt::Display for CentrifugeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CentrifugeError::Timeout => write!(f, "operation timed out"),
            CentrifugeError::ClientDisconnected => write!(f, "client disconnected"),
            CentrifugeError::ClientClosed => write!(f, "client closed"),
            CentrifugeError::SubscriptionUnsubscribed => write!(f, "subscription unsubscribed"),
            CentrifugeError::DuplicateSubscription => {
                write!(f, "subscription to this channel already exists")
            }
            CentrifugeError::Unauthorized => write!(f, "unauthorized"),
            CentrifugeError::Transport(msg) => write!(f, "transport error: {msg}"),
            CentrifugeError::Protocol(msg) => write!(f, "protocol error: {msg}"),
            CentrifugeError::BadConfiguration(msg) => write!(f, "bad configuration: {msg}"),
            CentrifugeError::Server(err) => {
                write!(f, "server error {}: {}", err.code, err.message)
            }
        }
    }
}

impl std::error::Error for CentrifugeError {}

pub type Result<T> = std::result::Result<T, CentrifugeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
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
        assert_eq!(
            CentrifugeError::Server(ServerError { code: 100, message: "internal".into(), temporary: false }).to_string(),
            "server error 100: internal"
        );
    }

    #[test]
    fn test_error_trait() {
        let err: Box<dyn std::error::Error> = Box::new(CentrifugeError::Timeout);
        assert_eq!(err.to_string(), "operation timed out");
    }
}
