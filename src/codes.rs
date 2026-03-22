/// Client-side disconnect codes (used when client moves to Disconnected state).
pub mod disconnect {
    pub const DISCONNECT_CALLED: u32 = 0;
    pub const UNAUTHORIZED: u32 = 1;
    pub const BAD_PROTOCOL: u32 = 2;
    pub const MESSAGE_SIZE_LIMIT: u32 = 3;
}

/// Client-side connecting codes (used when client moves to Connecting state).
pub mod connecting {
    pub const CONNECT_CALLED: u32 = 0;
    pub const TRANSPORT_CLOSED: u32 = 1;
    pub const NO_PING: u32 = 2;
    pub const SUBSCRIBE_TIMEOUT: u32 = 3;
    pub const UNSUBSCRIBE_ERROR: u32 = 4;
}

/// Client-side subscribing codes (used when subscription moves to Subscribing state).
pub mod subscribing {
    pub const SUBSCRIBE_CALLED: u32 = 0;
    pub const TRANSPORT_CLOSED: u32 = 1;
}

/// Client-side unsubscribed codes (used when subscription moves to Unsubscribed state).
pub mod unsubscribed {
    pub const UNSUBSCRIBE_CALLED: u32 = 0;
    pub const UNAUTHORIZED: u32 = 1;
    pub const CLIENT_CLOSED: u32 = 2;
}

/// Returns true if the server disconnect code indicates the client should reconnect.
/// Reconnect ranges: 3000-3499, 4000-4499.
/// Terminal ranges: 3500-3999, 4500-4999.
pub fn should_reconnect_on_disconnect(code: u32) -> bool {
    !(3500..5000).contains(&code) || (4000..4500).contains(&code)
}

/// Returns true if the server unsubscribe code indicates the client should resubscribe.
/// Resubscribe: >= 2500. Terminal: < 2500.
pub fn should_resubscribe_on_unsubscribe(code: u32) -> bool {
    code >= 2500
}

/// Server error codes (codes 100+ returned by the server in error replies).
pub mod server_error {
    /// Internal error (marks the boundary between client-side [0-99] and server-side [100+]).
    pub const INTERNAL_ERROR: u32 = 100;

    /// Already subscribed — tolerated as success when retrying after timeout.
    pub const ALREADY_SUBSCRIBED: u32 = 105;

    /// Token expired — triggers token refresh.
    pub const TOKEN_EXPIRED: u32 = 109;

    /// Returns true if the server error is temporary (should retry).
    pub fn is_temporary(code: u32, temporary: bool) -> bool {
        temporary || code < INTERNAL_ERROR || code == TOKEN_EXPIRED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_reconnect_on_disconnect() {
        // Reconnectable ranges
        assert!(should_reconnect_on_disconnect(3000));
        assert!(should_reconnect_on_disconnect(3001));
        assert!(should_reconnect_on_disconnect(3499));
        assert!(should_reconnect_on_disconnect(4000));
        assert!(should_reconnect_on_disconnect(4499));
        assert!(should_reconnect_on_disconnect(5000)); // >= 5000 reconnectable
        assert!(should_reconnect_on_disconnect(9999));

        // Terminal ranges
        assert!(!should_reconnect_on_disconnect(3500));
        assert!(!should_reconnect_on_disconnect(3999));
        assert!(!should_reconnect_on_disconnect(4500));
        assert!(!should_reconnect_on_disconnect(4999));
    }

    #[test]
    fn test_should_resubscribe_on_unsubscribe() {
        assert!(should_resubscribe_on_unsubscribe(2500));
        assert!(should_resubscribe_on_unsubscribe(2999));
        assert!(!should_resubscribe_on_unsubscribe(2000));
        assert!(!should_resubscribe_on_unsubscribe(2499));
    }

    #[test]
    fn test_is_temporary_error() {
        assert!(server_error::is_temporary(50, false)); // code < 100
        assert!(server_error::is_temporary(109, false)); // token expired
        assert!(server_error::is_temporary(500, true)); // explicit temporary flag
        assert!(!server_error::is_temporary(100, false)); // internal error, not temporary
        assert!(!server_error::is_temporary(200, false)); // permanent error
    }
}
