pub mod proto {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/centrifugal.centrifuge.protocol.rs"));
}

pub mod types {
    use std::collections::HashMap;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ClientState {
        Disconnected,
        Connecting,
        Connected,
        Closed,
    }

    impl ClientState {
        pub(crate) fn as_u8(self) -> u8 {
            match self {
                Self::Disconnected => 0,
                Self::Connecting => 1,
                Self::Connected => 2,
                Self::Closed => 3,
            }
        }

        pub(crate) fn from_u8(v: u8) -> Self {
            match v {
                1 => Self::Connecting,
                2 => Self::Connected,
                3 => Self::Closed,
                _ => Self::Disconnected,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SubscriptionState {
        Unsubscribed,
        Subscribing,
        Subscribed,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct StreamPosition {
        pub offset: u64,
        pub epoch: String,
    }

    #[derive(Debug, Clone)]
    pub struct Publication {
        pub data: Vec<u8>,
        pub info: Option<ClientInfo>,
        pub offset: u64,
        pub tags: HashMap<String, String>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct ClientInfo {
        pub client: String,
        pub user: String,
        pub conn_info: Vec<u8>,
        pub chan_info: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    pub struct ServerError {
        pub code: u32,
        pub message: String,
        pub temporary: bool,
    }

    #[derive(Debug, Clone, Default)]
    pub struct HistoryOptions {
        pub limit: i32,
        pub since: Option<StreamPosition>,
        pub reverse: bool,
    }

    impl HistoryOptions {
        pub fn with_limit(limit: i32) -> Self {
            Self {
                limit,
                ..Default::default()
            }
        }

        pub fn since(mut self, pos: StreamPosition) -> Self {
            self.since = Some(pos);
            self
        }

        pub fn reverse(mut self) -> Self {
            self.reverse = true;
            self
        }
    }

    #[derive(Debug, Clone)]
    pub struct HistoryResult {
        pub publications: Vec<Publication>,
        pub offset: u64,
        pub epoch: String,
    }

    #[derive(Debug, Clone)]
    pub struct PresenceResult {
        pub presence: HashMap<String, ClientInfo>,
    }

    #[derive(Debug, Clone)]
    pub struct PresenceStatsResult {
        pub num_clients: u32,
        pub num_users: u32,
    }

    #[derive(Debug, Clone)]
    pub struct RpcResult {
        pub data: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    pub struct ConnectedContext {
        pub client_id: String,
        pub version: String,
        pub data: Vec<u8>,
        pub session: String,
        pub node: String,
    }

    #[derive(Debug, Clone)]
    pub struct ConnectingContext {
        pub code: u32,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub struct DisconnectedContext {
        pub code: u32,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub struct SubscribedContext {
        pub channel: String,
        pub recoverable: bool,
        pub positioned: bool,
        pub stream_position: Option<StreamPosition>,
        pub was_recovering: bool,
        pub recovered: bool,
        pub has_recovered_publications: bool,
        pub data: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    pub struct SubscribingContext {
        pub code: u32,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub struct UnsubscribedContext {
        pub code: u32,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub struct PublicationContext {
        pub channel: String,
        pub publication: Publication,
    }

    #[derive(Debug, Clone)]
    pub struct JoinContext {
        pub channel: String,
        pub info: ClientInfo,
    }

    #[derive(Debug, Clone)]
    pub struct LeaveContext {
        pub channel: String,
        pub info: ClientInfo,
    }

    #[derive(Debug, Clone)]
    pub struct ErrorContext {
        pub error: String,
    }

    #[derive(Debug, Clone)]
    pub struct MessageContext {
        pub data: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    pub struct ServerSubscribedContext {
        pub channel: String,
        pub recoverable: bool,
        pub positioned: bool,
        pub stream_position: Option<StreamPosition>,
        pub was_recovering: bool,
        pub recovered: bool,
        pub has_recovered_publications: bool,
        pub data: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    pub struct ServerSubscribingContext {
        pub channel: String,
        pub code: u32,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub struct ServerUnsubscribedContext {
        pub channel: String,
        pub code: u32,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub struct ServerPublicationContext {
        pub channel: String,
        pub publication: Publication,
    }

    #[derive(Debug, Clone)]
    pub struct ServerJoinContext {
        pub channel: String,
        pub info: ClientInfo,
    }

    #[derive(Debug, Clone)]
    pub struct ServerLeaveContext {
        pub channel: String,
        pub info: ClientInfo,
    }

    // Conversion helpers from proto types

    impl From<&super::proto::ClientInfo> for ClientInfo {
        fn from(info: &super::proto::ClientInfo) -> Self {
            ClientInfo {
                client: info.client.clone(),
                user: info.user.clone(),
                conn_info: info.conn_info.clone(),
                chan_info: info.chan_info.clone(),
            }
        }
    }

    impl From<&super::proto::Publication> for Publication {
        fn from(pub_msg: &super::proto::Publication) -> Self {
            Publication {
                data: pub_msg.data.clone(),
                info: pub_msg.info.as_ref().map(ClientInfo::from),
                offset: pub_msg.offset,
                tags: pub_msg.tags.clone(),
            }
        }
    }

    impl From<&super::proto::Error> for ServerError {
        fn from(err: &super::proto::Error) -> Self {
            ServerError {
                code: err.code,
                message: err.message.clone(),
                temporary: err.temporary,
            }
        }
    }

    // -----------------------------------------------------------------
    // Event enums for stream-based API
    // -----------------------------------------------------------------

    /// Events emitted by a client-side subscription.
    #[derive(Debug, Clone)]
    pub enum SubEvent {
        Subscribing(SubscribingContext),
        Subscribed(SubscribedContext),
        Unsubscribed(UnsubscribedContext),
        Publication(Publication),
        Join(ClientInfo),
        Leave(ClientInfo),
        Error(ErrorContext),
    }

    /// Events emitted by the client (connection lifecycle + server-side subscriptions).
    #[derive(Debug, Clone)]
    pub enum ClientEvent {
        Connecting(ConnectingContext),
        Connected(ConnectedContext),
        Disconnected(DisconnectedContext),
        Error(ErrorContext),
        Message(MessageContext),
        ServerSubscribed(ServerSubscribedContext),
        ServerSubscribing(ServerSubscribingContext),
        ServerUnsubscribed(ServerUnsubscribedContext),
        ServerPublication(ServerPublicationContext),
        ServerJoin(ServerJoinContext),
        ServerLeave(ServerLeaveContext),
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_history_options_builder() {
            let opts = HistoryOptions::with_limit(50);
            assert_eq!(opts.limit, 50);
            assert!(opts.since.is_none());
            assert!(!opts.reverse);

            let opts = HistoryOptions::with_limit(10)
                .since(StreamPosition {
                    offset: 42,
                    epoch: "e1".into(),
                })
                .reverse();
            assert_eq!(opts.limit, 10);
            assert!(opts.reverse);
            let since = opts.since.unwrap();
            assert_eq!(since.offset, 42);
            assert_eq!(since.epoch, "e1");
        }

        #[test]
        fn test_history_options_default() {
            let opts = HistoryOptions::default();
            assert_eq!(opts.limit, 0);
            assert!(opts.since.is_none());
            assert!(!opts.reverse);
        }

        #[test]
        fn test_client_info_from_proto() {
            let proto_info = super::super::proto::ClientInfo {
                client: "c1".into(),
                user: "u1".into(),
                conn_info: b"conn".to_vec(),
                chan_info: b"chan".to_vec(),
            };
            let info = ClientInfo::from(&proto_info);
            assert_eq!(info.client, "c1");
            assert_eq!(info.user, "u1");
            assert_eq!(info.conn_info, b"conn");
            assert_eq!(info.chan_info, b"chan");
        }

        #[test]
        fn test_publication_from_proto() {
            let proto_pub = super::super::proto::Publication {
                data: b"hello".to_vec(),
                offset: 5,
                tags: [("k".into(), "v".into())].into(),
                info: Some(super::super::proto::ClientInfo {
                    client: "c1".into(),
                    user: "u1".into(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let pub_msg = Publication::from(&proto_pub);
            assert_eq!(pub_msg.data, b"hello");
            assert_eq!(pub_msg.offset, 5);
            assert_eq!(pub_msg.tags["k"], "v");
            assert!(pub_msg.info.is_some());
            assert_eq!(pub_msg.info.unwrap().client, "c1");
        }

        #[test]
        fn test_publication_from_proto_no_info() {
            let proto_pub = super::super::proto::Publication {
                data: b"data".to_vec(),
                ..Default::default()
            };
            let pub_msg = Publication::from(&proto_pub);
            assert!(pub_msg.info.is_none());
            assert_eq!(pub_msg.offset, 0);
            assert!(pub_msg.tags.is_empty());
        }

        #[test]
        fn test_server_error_from_proto() {
            let proto_err = super::super::proto::Error {
                code: 100,
                message: "internal".into(),
                temporary: true,
            };
            let err = ServerError::from(&proto_err);
            assert_eq!(err.code, 100);
            assert_eq!(err.message, "internal");
            assert!(err.temporary);
        }

        #[test]
        fn test_stream_position_default() {
            let pos = StreamPosition::default();
            assert_eq!(pos.offset, 0);
            assert!(pos.epoch.is_empty());
        }
    }
}
