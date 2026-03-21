pub mod proto {
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

    #[derive(Debug, Clone)]
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

    #[derive(Debug, Clone)]
    #[derive(Default)]
    pub struct HistoryOptions {
        pub limit: i32,
        pub since: Option<StreamPosition>,
        pub reverse: bool,
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
}
