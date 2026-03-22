use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::debug;

use crate::codec::{self, Codec};
use crate::config::{ClientConfig, SubscriptionConfig};
use crate::errors::{CentrifugeError, Result};
use crate::protocol::{proto, types::*};
use crate::subscription::SubState;
use crate::transport::{Transport, TransportFrame, TransportSink};

/// Commands sent from Client handle to the connection actor.
pub(crate) enum ActorCommand {
    Connect {
        reply: oneshot::Sender<Result<()>>,
    },
    Disconnect {
        reply: oneshot::Sender<Result<()>>,
    },
    Close {
        reply: oneshot::Sender<Result<()>>,
    },
    NewSubscription {
        channel: String,
        config: Box<SubscriptionConfig>,
        reply: oneshot::Sender<Result<()>>,
    },
    GetSubscription {
        channel: String,
        reply: oneshot::Sender<bool>,
    },
    ListSubscriptions {
        reply: oneshot::Sender<Vec<(String, SubscriptionState)>>,
    },
    RemoveSubscription {
        channel: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        channel: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        channel: String,
        reply: oneshot::Sender<Result<()>>,
    },
    SetClientEventChannel {
        tx: mpsc::Sender<ClientEvent>,
    },
    SetSubEventChannel {
        channel: String,
        tx: mpsc::Sender<SubEvent>,
    },
    /// Generic protocol request: send a command and return the raw reply.
    /// Used for publish, history, presence, presence_stats, rpc, unsubscribe.
    SendRequest {
        cmd: Box<proto::Command>,
        reply: oneshot::Sender<Result<proto::Reply>>,
    },
    Send {
        data: Vec<u8>,
    },
    SetToken {
        token: String,
    },
    SetData {
        data: Vec<u8>,
    },
    StartBatching,
    StopBatching,
    // Internal
    Resubscribe {
        channel: String,
    },
    RefreshToken,
    RefreshSubToken {
        channel: String,
    },
    RequestTimeout {
        id: u32,
    },
}

/// Pending request awaiting a server reply.
pub(super) enum PendingRequest {
    Subscribe {
        channel: String,
        sender: oneshot::Sender<Result<()>>,
    },
    /// Generic request — returns raw proto::Reply to the caller.
    Request(oneshot::Sender<Result<proto::Reply>>),
    Refresh(oneshot::Sender<Result<()>>),
    SubRefresh {
        channel: String,
        sender: oneshot::Sender<Result<()>>,
    },
}

impl PendingRequest {
    /// Fail this pending request. Returns the channel name if this was a Subscribe.
    pub(super) fn fail(self, make_err: impl FnOnce() -> CentrifugeError) -> Option<String> {
        match self {
            PendingRequest::Subscribe { channel, sender } => {
                let _ = sender.send(Err(make_err()));
                Some(channel)
            }
            PendingRequest::Request(tx) => {
                let _ = tx.send(Err(make_err()));
                None
            }
            PendingRequest::Refresh(tx) => {
                let _ = tx.send(Err(make_err()));
                None
            }
            PendingRequest::SubRefresh { sender, .. } => {
                let _ = sender.send(Err(make_err()));
                None
            }
        }
    }
}

/// Server-side subscription state.
pub(super) struct ServerSubState {
    pub(super) recoverable: bool,
    pub(super) offset: u64,
    pub(super) epoch: String,
}

/// Server ping/pong state.
pub(super) struct PingState {
    pub(super) interval: Duration,
    pub(super) send_pong: bool,
    pub(super) last_data_received: Instant,
}

/// Connection token refresh state.
pub(super) struct TokenState {
    pub(super) expires: bool,
    pub(super) ttl: u32,
    pub(super) refresh_required: bool,
}

/// Command batching state.
pub(super) struct BatchState {
    pub(super) active: bool,
    pub(super) queue: Vec<Vec<u8>>,
}

pub(crate) struct ConnectionActor {
    pub(super) config: ClientConfig,
    pub(super) cmd_rx: mpsc::Receiver<ActorCommand>,
    pub(super) cmd_tx: mpsc::Sender<ActorCommand>,
    pub(super) codec: Box<dyn Codec>,
    pub(super) state: ClientState,
    pub(super) client_id: String,

    pub(super) transport: Box<dyn Transport>,
    pub(super) sink: Option<Box<dyn TransportSink>>,
    pub(super) stream: Option<Pin<Box<dyn tokio_stream::Stream<Item = TransportFrame> + Send>>>,

    pub(super) next_id: AtomicU32,
    pub(super) pending: HashMap<u32, PendingRequest>,

    pub(super) subs: HashMap<String, SubState>,
    pub(super) server_subs: HashMap<String, ServerSubState>,

    pub(super) ping: PingState,
    pub(super) token: TokenState,
    pub(super) batch: BatchState,

    pub(super) reconnect_attempts: u32,
    pub(super) connect_requested: bool,
    pub(super) connect_waiters: Vec<oneshot::Sender<Result<()>>>,

    /// Channel for streaming client events (connection lifecycle + server subs).
    pub(super) client_event_tx: Option<mpsc::Sender<ClientEvent>>,

    /// Shared atomic state for sync state() access.
    pub(super) shared_state: Arc<AtomicU8>,
}

impl ConnectionActor {
    pub fn new(
        config: ClientConfig,
        cmd_rx: mpsc::Receiver<ActorCommand>,
        cmd_tx: mpsc::Sender<ActorCommand>,
        transport: Box<dyn Transport>,
        shared_state: Arc<AtomicU8>,
    ) -> Self {
        let codec = codec::new_codec(config.protocol_type);
        Self {
            config,
            cmd_rx,
            cmd_tx,
            codec,
            state: ClientState::Disconnected,
            client_id: String::new(),
            transport,
            sink: None,
            stream: None,
            next_id: AtomicU32::new(1),
            pending: HashMap::new(),
            subs: HashMap::new(),
            server_subs: HashMap::new(),
            ping: PingState {
                interval: Duration::from_secs(25),
                send_pong: false,
                last_data_received: Instant::now(),
            },
            token: TokenState {
                expires: false,
                ttl: 0,
                refresh_required: false,
            },
            batch: BatchState {
                active: false,
                queue: Vec::new(),
            },
            reconnect_attempts: 0,
            connect_requested: false,
            connect_waiters: Vec::new(),
            client_event_tx: None,
            shared_state,
        }
    }

    /// Send a client event to the event stream (if one exists).
    pub(super) fn emit_client_event(&self, event: ClientEvent) {
        if let Some(ref tx) = self.client_event_tx {
            let _ = tx.try_send(event);
        }
    }

    /// Emit an error event with a formatted message.
    pub(super) fn emit_error(&self, msg: impl Into<String>) {
        self.emit_client_event(ClientEvent::Error(ErrorContext { error: msg.into() }));
    }

    /// Update the shared atomic state (for sync state() access).
    pub(super) fn update_shared_state(&self) {
        self.shared_state.store(self.state.as_u8(), Ordering::Relaxed);
    }

    pub(super) fn next_cmd_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Main actor loop.
    pub async fn run(mut self) {
        loop {
            match self.state {
                ClientState::Disconnected => match self.cmd_rx.recv().await {
                    Some(cmd) => self.handle_command(cmd).await,
                    None => break,
                },
                ClientState::Connecting => {
                    self.do_connect_cycle().await;
                }
                ClientState::Connected => {
                    self.do_connected_loop().await;
                }
                ClientState::Closed => break,
            }
        }
        debug!("connection actor shut down");
    }
}
