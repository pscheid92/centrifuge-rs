use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use std::pin::Pin;

use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant};
use tracing::{debug, trace, warn};

use crate::backoff;
use crate::codec::{self, Codec};
use crate::codes;
use crate::config::{ClientConfig, SubscriptionConfig};
use crate::errors::{CentrifugeError, Result};
use crate::protocol::{proto, types::*};
use crate::subscription::SubState;
use crate::transport::{self, Transport, TransportFrame, TransportSink};

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
    Publish {
        channel: String,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    History {
        channel: String,
        opts: HistoryOptions,
        reply: oneshot::Sender<Result<HistoryResult>>,
    },
    Presence {
        channel: String,
        reply: oneshot::Sender<Result<PresenceResult>>,
    },
    PresenceStats {
        channel: String,
        reply: oneshot::Sender<Result<PresenceStatsResult>>,
    },
    Rpc {
        method: String,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<RpcResult>>,
    },
    Send {
        data: Vec<u8>,
    },
    // Internal commands
    Resubscribe {
        channel: String,
    },
    RefreshToken,
    RefreshSubToken {
        channel: String,
    },
}

/// Pending request awaiting a server reply.
#[allow(dead_code)]
enum PendingRequest {
    Connect(oneshot::Sender<Result<()>>),
    Subscribe {
        channel: String,
        sender: oneshot::Sender<Result<()>>,
    },
    Unsubscribe(oneshot::Sender<Result<()>>),
    Publish(oneshot::Sender<Result<()>>),
    History(oneshot::Sender<Result<HistoryResult>>),
    Presence(oneshot::Sender<Result<PresenceResult>>),
    PresenceStats(oneshot::Sender<Result<PresenceStatsResult>>),
    Rpc(oneshot::Sender<Result<RpcResult>>),
    Refresh(oneshot::Sender<Result<()>>),
    SubRefresh {
        channel: String,
        sender: oneshot::Sender<Result<()>>,
    },
}

/// Server-side subscription state.
#[allow(dead_code)]
struct ServerSubState {
    recoverable: bool,
    positioned: bool,
    offset: u64,
    epoch: String,
}

pub(crate) struct ConnectionActor {
    config: ClientConfig,
    cmd_rx: mpsc::Receiver<ActorCommand>,
    cmd_tx: mpsc::Sender<ActorCommand>,
    codec: Box<dyn Codec>,
    state: ClientState,
    client_id: String,

    // Transport
    transport: Box<dyn Transport>,
    sink: Option<Box<dyn TransportSink>>,
    stream: Option<Pin<Box<dyn futures_util::Stream<Item = TransportFrame> + Send>>>,

    // Command ID tracking
    next_id: AtomicU32,
    pending: HashMap<u32, PendingRequest>,

    // Subscriptions
    subs: HashMap<String, SubState>,
    server_subs: HashMap<String, ServerSubState>,

    // Ping/pong
    ping_interval: Duration,
    send_pong: bool,
    last_data_received: Instant,

    // Token refresh
    token_expires: bool,
    token_ttl: u32,
    refresh_required: bool,

    // Reconnect
    reconnect_attempts: u32,
    connect_requested: bool,

    // Pending connect reply senders
    connect_waiters: Vec<oneshot::Sender<Result<()>>>,
}

impl ConnectionActor {
    pub fn new(
        config: ClientConfig,
        cmd_rx: mpsc::Receiver<ActorCommand>,
        cmd_tx: mpsc::Sender<ActorCommand>,
        transport: Box<dyn Transport>,
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
            ping_interval: Duration::from_secs(25),
            send_pong: false,
            last_data_received: Instant::now(),
            token_expires: false,
            token_ttl: 0,
            refresh_required: false,
            reconnect_attempts: 0,
            connect_requested: false,
            connect_waiters: Vec::new(),
        }
    }

    fn next_cmd_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Main actor loop.
    pub async fn run(mut self) {
        loop {
            match self.state {
                ClientState::Disconnected => {
                    // Wait for commands only
                    match self.cmd_rx.recv().await {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => break, // All handles dropped
                    }
                }
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

    // -----------------------------------------------------------------------
    // State transitions
    // -----------------------------------------------------------------------

    fn move_to_connecting(&mut self, code: u32, reason: &str) {
        debug!(code, reason, "moving to connecting");
        self.state = ClientState::Connecting;
        if let Some(ref cb) = self.config.events.on_connecting {
            cb(ConnectingContext {
                code,
                reason: reason.to_string(),
            });
        }
    }

    fn move_to_connected(&mut self, result: &proto::ConnectResult) {
        self.state = ClientState::Connected;
        self.client_id = result.client.clone();
        self.reconnect_attempts = 0;

        // Extract ping/pong settings
        if result.ping > 0 {
            self.ping_interval = Duration::from_secs(result.ping as u64);
        }
        self.send_pong = result.pong;
        self.last_data_received = Instant::now();

        // Token expiration
        self.token_expires = result.expires;
        self.token_ttl = result.ttl;

        debug!(
            client_id = %self.client_id,
            ping = ?self.ping_interval,
            pong = self.send_pong,
            "connected"
        );

        if let Some(ref cb) = self.config.events.on_connected {
            cb(ConnectedContext {
                client_id: result.client.clone(),
                version: result.version.clone(),
                data: result.data.clone(),
                session: result.session.clone(),
                node: result.node.clone(),
            });
        }
    }

    fn move_to_disconnected(&mut self, code: u32, reason: &str) {
        debug!(code, reason, "moving to disconnected");
        let was_connected = self.state == ClientState::Connected;
        self.state = ClientState::Disconnected;
        self.client_id.clear();
        self.connect_requested = false;

        // Close transport
        self.sink = None;
        self.stream = None;

        // Fail all pending requests
        self.fail_all_pending(CentrifugeError::ClientDisconnected);

        // Notify connect waiters
        for w in self.connect_waiters.drain(..) {
            let _ = w.send(Err(CentrifugeError::ClientDisconnected));
        }

        // Move subscriptions to unsubscribed if we were connected
        if was_connected {
            for sub in self.subs.values_mut() {
                if sub.state != SubscriptionState::Unsubscribed {
                    sub.state = SubscriptionState::Unsubscribed;
                    if let Some(ref cb) = sub.config.events.on_unsubscribed {
                        cb(UnsubscribedContext {
                            code: codes::unsubscribed::CLIENT_CLOSED,
                            reason: reason.to_string(),
                        });
                    }
                }
            }
        }

        // Emit server-side unsubscribed for all
        for (channel, _) in self.server_subs.drain() {
            if let Some(ref cb) = self.config.events.on_server_unsubscribed {
                cb(ServerUnsubscribedContext {
                    channel,
                    code,
                    reason: reason.to_string(),
                });
            }
        }

        if let Some(ref cb) = self.config.events.on_disconnected {
            cb(DisconnectedContext {
                code,
                reason: reason.to_string(),
            });
        }
    }

    fn move_to_closed(&mut self) {
        debug!("moving to closed");
        self.state = ClientState::Closed;
        self.sink = None;
        self.stream = None;
        self.fail_all_pending(CentrifugeError::ClientClosed);
        for w in self.connect_waiters.drain(..) {
            let _ = w.send(Err(CentrifugeError::ClientClosed));
        }
        // Move all subs to unsubscribed
        for sub in self.subs.values_mut() {
            if sub.state != SubscriptionState::Unsubscribed {
                sub.state = SubscriptionState::Unsubscribed;
                if let Some(ref cb) = sub.config.events.on_unsubscribed {
                    cb(UnsubscribedContext {
                        code: codes::unsubscribed::CLIENT_CLOSED,
                        reason: "client closed".into(),
                    });
                }
            }
        }
    }

    fn fail_all_pending(&mut self, err: CentrifugeError) {
        for (_, req) in self.pending.drain() {
            match req {
                PendingRequest::Connect(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::Subscribe { sender, .. } => {
                    let _ = sender.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::Unsubscribe(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::Publish(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::History(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::Presence(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::PresenceStats(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::Rpc(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::Refresh(tx) => {
                    let _ = tx.send(Err(CentrifugeError::ClientDisconnected));
                }
                PendingRequest::SubRefresh { sender, .. } => {
                    let _ = sender.send(Err(CentrifugeError::ClientDisconnected));
                }
            }
        }
        // Suppress unused variable warning — err describes the reason for failure
        let _ = err;
    }

    // -----------------------------------------------------------------------
    // Connect cycle (Connecting state)
    // -----------------------------------------------------------------------

    async fn do_connect_cycle(&mut self) {
        loop {
            if self.state != ClientState::Connecting {
                return;
            }

            // Backoff delay
            if self.reconnect_attempts > 0 {
                let delay = backoff::next_delay(
                    self.reconnect_attempts.saturating_sub(1),
                    self.config.min_reconnect_delay,
                    self.config.max_reconnect_delay,
                );
                debug!(attempt = self.reconnect_attempts, delay = ?delay, "reconnect backoff");

                // Wait for delay, but also accept commands (e.g., disconnect, close)
                let sleep = time::sleep(delay);
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        _ = &mut sleep => break,
                        cmd = self.cmd_rx.recv() => {
                            match cmd {
                                Some(cmd) => {
                                    self.handle_command(cmd).await;
                                    if self.state != ClientState::Connecting {
                                        return;
                                    }
                                }
                                None => {
                                    self.move_to_closed();
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            self.reconnect_attempts += 1;

            // Optionally refresh token
            if self.refresh_required || (self.config.token.is_empty() && self.config.get_token.is_some()) {
                match self.do_token_refresh().await {
                    Ok(()) => {
                        self.refresh_required = false;
                    }
                    Err(CentrifugeError::Unauthorized) => {
                        self.move_to_disconnected(
                            codes::disconnect::UNAUTHORIZED,
                            "unauthorized",
                        );
                        return;
                    }
                    Err(e) => {
                        if let Some(ref cb) = self.config.events.on_error {
                            cb(ErrorContext {
                                error: format!("token refresh: {e}"),
                            });
                        }
                        continue; // retry with backoff
                    }
                }
            }

            // Attempt transport connection
            match self.transport.connect().await {
                Ok(conn) => {
                    self.sink = Some(conn.sink);
                    self.stream = Some(conn.stream);
                }
                Err(e) => {
                    if let Some(ref cb) = self.config.events.on_error {
                        cb(ErrorContext {
                            error: format!("transport: {e}"),
                        });
                    }
                    continue; // retry with backoff
                }
            }

            // Send connect command
            match self.do_handshake().await {
                Ok(result) => {
                    self.move_to_connected(&result);
                    self.process_server_subs(&result);
                    self.schedule_token_refresh();
                    self.resubscribe_all().await;

                    // Notify connect waiters
                    for w in self.connect_waiters.drain(..) {
                        let _ = w.send(Ok(()));
                    }
                    return;
                }
                Err(e) => {
                    self.sink = None;
                    self.stream = None;
                    if let Some(ref cb) = self.config.events.on_error {
                        cb(ErrorContext {
                            error: format!("handshake: {e}"),
                        });
                    }
                    // Check if it's a token expired error
                    if let CentrifugeError::Server(ref err) = e
                        && err.code == codes::TOKEN_EXPIRED {
                            self.refresh_required = true;
                        }
                    continue; // retry with backoff
                }
            }
        }
    }

    async fn do_token_refresh(&mut self) -> Result<()> {
        if let Some(ref get_token) = self.config.get_token {
            let token = get_token().await?;
            if token.is_empty() {
                return Err(CentrifugeError::Unauthorized);
            }
            self.config.token = token;
            Ok(())
        } else {
            Ok(())
        }
    }

    async fn do_handshake(&mut self) -> Result<proto::ConnectResult> {
        let id = self.next_cmd_id();
        let mut subs_map = HashMap::new();

        // Include recovery info for server-side subs
        for (channel, sub) in &self.server_subs {
            if sub.recoverable {
                subs_map.insert(
                    channel.clone(),
                    proto::SubscribeRequest {
                        recover: true,
                        offset: sub.offset,
                        epoch: sub.epoch.clone(),
                        ..Default::default()
                    },
                );
            }
        }

        let cmd = proto::Command {
            id,
            connect: Some(proto::ConnectRequest {
                token: self.config.token.clone(),
                data: self.config.data.clone(),
                subs: subs_map,
                name: self.config.name.clone(),
                version: self.config.version.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };

        self.send_command(&cmd).await?;

        // Wait for reply with timeout
        let reply = self.read_reply_with_timeout(id).await?;

        if let Some(err) = reply.error {
            return Err(CentrifugeError::Server(ServerError::from(&err)));
        }

        reply
            .connect
            .ok_or_else(|| CentrifugeError::Protocol("missing connect result".into()))
    }

    // -----------------------------------------------------------------------
    // Connected loop
    // -----------------------------------------------------------------------

    async fn do_connected_loop(&mut self) {
        let ping_timeout = self.ping_interval + self.config.max_server_ping_delay;
        self.last_data_received = Instant::now();

        loop {
            if self.state != ClientState::Connected {
                return;
            }

            let deadline = self.last_data_received + ping_timeout;

            tokio::select! {
                // Read from transport stream
                frame = async {
                    if let Some(ref mut stream) = self.stream {
                        stream.next().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    match frame {
                        Some(TransportFrame::Data(data)) => {
                            self.last_data_received = Instant::now();
                            self.handle_transport_data(&data).await;
                        }
                        Some(TransportFrame::Close(info)) => {
                            self.on_transport_close(info).await;
                            return;
                        }
                        None => {
                            self.on_transport_close(None).await;
                            return;
                        }
                    }
                }
                // Read commands from client handles
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => {
                            self.move_to_closed();
                            return;
                        }
                    }
                }
                // Ping timeout
                _ = time::sleep_until(deadline) => {
                    if Instant::now() >= deadline {
                        debug!("ping timeout");
                        self.on_transport_close(Some(transport::DisconnectInfo {
                            code: codes::connecting::NO_PING,
                            reason: "no ping".into(),
                            reconnect: true,
                        })).await;
                        return;
                    }
                }
            }
        }
    }

    async fn handle_transport_data(&mut self, data: &[u8]) {
        let replies = match self.codec.decode_replies(data) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to decode replies");
                return;
            }
        };

        for reply in replies {
            self.dispatch_reply(reply).await;
        }
    }

    async fn dispatch_reply(&mut self, reply: proto::Reply) {
        // Server ping: reply with no id and no push
        if reply.id == 0 && reply.push.is_none() {
            trace!("received server ping");
            if self.send_pong {
                let pong = proto::Command {
                    id: 0,
                    ..Default::default()
                };
                if let Err(e) = self.send_command(&pong).await {
                    debug!(error = %e, "failed to send pong");
                }
            }
            return;
        }

        // Push message (async, no matching command)
        if reply.id == 0 {
            if let Some(push) = reply.push {
                self.handle_push(push).await;
            }
            return;
        }

        // Reply to a command
        if let Some(req) = self.pending.remove(&reply.id) {
            self.resolve_pending(req, reply).await;
        } else {
            debug!(id = reply.id, "received reply for unknown request");
        }
    }

    async fn resolve_pending(&mut self, req: PendingRequest, reply: proto::Reply) {
        match req {
            PendingRequest::Connect(tx) => {
                // Connect replies are handled inline during handshake
                // This shouldn't happen, but handle gracefully
                let _ = tx.send(Ok(()));
            }
            PendingRequest::Subscribe { channel, sender } => {
                if let Some(err) = reply.error {
                    let server_err = ServerError::from(&err);
                    self.handle_subscribe_error(&channel, server_err.clone());
                    let _ = sender.send(Err(CentrifugeError::Server(server_err)));
                } else if let Some(result) = reply.subscribe {
                    self.handle_subscribe_success(&channel, &result);
                    let _ = sender.send(Ok(()));
                } else {
                    let _ = sender.send(Err(CentrifugeError::Protocol(
                        "missing subscribe result".into(),
                    )));
                }
            }
            PendingRequest::Unsubscribe(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else {
                    let _ = tx.send(Ok(()));
                }
            }
            PendingRequest::Publish(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else {
                    let _ = tx.send(Ok(()));
                }
            }
            PendingRequest::History(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else if let Some(result) = reply.history {
                    let _ = tx.send(Ok(HistoryResult {
                        publications: result.publications.iter().map(Publication::from).collect(),
                        offset: result.offset,
                        epoch: result.epoch,
                    }));
                } else {
                    let _ = tx.send(Err(CentrifugeError::Protocol(
                        "missing history result".into(),
                    )));
                }
            }
            PendingRequest::Presence(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else if let Some(result) = reply.presence {
                    let _ = tx.send(Ok(PresenceResult {
                        presence: result
                            .presence
                            .iter()
                            .map(|(k, v)| (k.clone(), ClientInfo::from(v)))
                            .collect(),
                    }));
                } else {
                    let _ = tx.send(Err(CentrifugeError::Protocol(
                        "missing presence result".into(),
                    )));
                }
            }
            PendingRequest::PresenceStats(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else if let Some(result) = reply.presence_stats {
                    let _ = tx.send(Ok(PresenceStatsResult {
                        num_clients: result.num_clients,
                        num_users: result.num_users,
                    }));
                } else {
                    let _ = tx.send(Err(CentrifugeError::Protocol(
                        "missing presence_stats result".into(),
                    )));
                }
            }
            PendingRequest::Rpc(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else if let Some(result) = reply.rpc {
                    let _ = tx.send(Ok(RpcResult { data: result.data }));
                } else {
                    let _ = tx.send(Err(CentrifugeError::Protocol(
                        "missing rpc result".into(),
                    )));
                }
            }
            PendingRequest::Refresh(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else if let Some(result) = reply.refresh {
                    self.token_expires = result.expires;
                    self.token_ttl = result.ttl;
                    self.schedule_token_refresh();
                    let _ = tx.send(Ok(()));
                } else {
                    let _ = tx.send(Ok(()));
                }
            }
            PendingRequest::SubRefresh { channel, sender } => {
                if let Some(err) = reply.error {
                    let _ = sender.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else if let Some(result) = reply.sub_refresh {
                    if self.subs.contains_key(&channel) && result.expires {
                        self.schedule_sub_token_refresh(&channel, result.ttl);
                    }
                    let _ = sender.send(Ok(()));
                } else {
                    let _ = sender.send(Ok(()));
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Push handling
    // -----------------------------------------------------------------------

    async fn handle_push(&mut self, push: proto::Push) {
        let channel = push.channel.clone();

        if let Some(pub_msg) = push.r#pub {
            self.handle_publication(&channel, &pub_msg);
        } else if let Some(join) = push.join {
            self.handle_join(&channel, &join);
        } else if let Some(leave) = push.leave {
            self.handle_leave(&channel, &leave);
        } else if let Some(unsub) = push.unsubscribe {
            self.handle_server_unsubscribe(&channel, unsub.code, &unsub.reason);
        } else if let Some(sub) = push.subscribe {
            self.handle_server_subscribe(&channel, &sub);
        } else if let Some(disconnect) = push.disconnect {
            self.handle_disconnect_push(disconnect).await;
        } else if let Some(message) = push.message
            && let Some(ref cb) = self.config.events.on_message {
                cb(MessageContext {
                    data: message.data,
                });
            }
    }

    fn handle_publication(&mut self, channel: &str, pub_msg: &proto::Publication) {
        let publication = Publication::from(pub_msg);

        // Check client-side subscriptions first
        if let Some(sub) = self.subs.get_mut(channel) {
            if pub_msg.offset > 0 {
                sub.offset = pub_msg.offset;
            }
            if let Some(ref cb) = sub.config.events.on_publication {
                cb(PublicationContext {
                    channel: channel.to_string(),
                    publication: publication.clone(),
                });
            }
            return;
        }

        // Check server-side subscriptions
        if let Some(server_sub) = self.server_subs.get_mut(channel) {
            if pub_msg.offset > 0 {
                server_sub.offset = pub_msg.offset;
            }
            if let Some(ref cb) = self.config.events.on_server_publication {
                cb(ServerPublicationContext {
                    channel: channel.to_string(),
                    publication,
                });
            }
        }
    }

    fn handle_join(&mut self, channel: &str, join: &proto::Join) {
        let info = join.info.as_ref().map(ClientInfo::from).unwrap_or(ClientInfo {
            client: String::new(),
            user: String::new(),
            conn_info: Vec::new(),
            chan_info: Vec::new(),
        });

        if let Some(sub) = self.subs.get(channel) {
            if let Some(ref cb) = sub.config.events.on_join {
                cb(JoinContext {
                    channel: channel.to_string(),
                    info: info.clone(),
                });
            }
            return;
        }

        if self.server_subs.contains_key(channel)
            && let Some(ref cb) = self.config.events.on_server_join {
                cb(ServerJoinContext {
                    channel: channel.to_string(),
                    info,
                });
            }
    }

    fn handle_leave(&mut self, channel: &str, leave: &proto::Leave) {
        let info = leave.info.as_ref().map(ClientInfo::from).unwrap_or(ClientInfo {
            client: String::new(),
            user: String::new(),
            conn_info: Vec::new(),
            chan_info: Vec::new(),
        });

        if let Some(sub) = self.subs.get(channel) {
            if let Some(ref cb) = sub.config.events.on_leave {
                cb(LeaveContext {
                    channel: channel.to_string(),
                    info: info.clone(),
                });
            }
            return;
        }

        if self.server_subs.contains_key(channel)
            && let Some(ref cb) = self.config.events.on_server_leave {
                cb(ServerLeaveContext {
                    channel: channel.to_string(),
                    info,
                });
            }
    }

    fn handle_server_unsubscribe(&mut self, channel: &str, code: u32, reason: &str) {
        // Client-side subscription
        if let Some(sub) = self.subs.get_mut(channel) {
            if codes::should_resubscribe_on_unsubscribe(code) {
                // Move to subscribing and schedule resubscribe
                sub.state = SubscriptionState::Subscribing;
                if let Some(ref cb) = sub.config.events.on_subscribing {
                    cb(SubscribingContext {
                        code,
                        reason: reason.to_string(),
                    });
                }
                let channel = channel.to_string();
                let tx = self.cmd_tx.clone();
                tokio::spawn(async move {
                    let _ = tx.send(ActorCommand::Resubscribe { channel }).await;
                });
            } else {
                sub.state = SubscriptionState::Unsubscribed;
                sub.resubscribe_attempts = 0;
                if let Some(ref cb) = sub.config.events.on_unsubscribed {
                    cb(UnsubscribedContext {
                        code,
                        reason: reason.to_string(),
                    });
                }
            }
            return;
        }

        // Server-side subscription
        if self.server_subs.remove(channel).is_some()
            && let Some(ref cb) = self.config.events.on_server_unsubscribed {
                cb(ServerUnsubscribedContext {
                    channel: channel.to_string(),
                    code,
                    reason: reason.to_string(),
                });
            }
    }

    fn handle_server_subscribe(&mut self, channel: &str, sub: &proto::Subscribe) {
        self.server_subs.insert(
            channel.to_string(),
            ServerSubState {
                recoverable: sub.recoverable,
                positioned: sub.positioned,
                offset: sub.offset,
                epoch: sub.epoch.clone(),
            },
        );

        if let Some(ref cb) = self.config.events.on_server_subscribed {
            cb(ServerSubscribedContext {
                channel: channel.to_string(),
                recoverable: sub.recoverable,
                positioned: sub.positioned,
                stream_position: if sub.positioned || sub.recoverable {
                    Some(StreamPosition {
                        offset: sub.offset,
                        epoch: sub.epoch.clone(),
                    })
                } else {
                    None
                },
                was_recovering: false,
                recovered: false,
                data: sub.data.clone(),
            });
        }
    }

    async fn handle_disconnect_push(&mut self, disconnect: proto::Disconnect) {
        let reconnect = disconnect.reconnect
            || codes::should_reconnect_on_disconnect(disconnect.code);
        if reconnect {
            self.on_transport_close(Some(transport::DisconnectInfo {
                code: disconnect.code,
                reason: disconnect.reason,
                reconnect: true,
            }))
            .await;
        } else {
            self.move_to_disconnected(disconnect.code, &disconnect.reason);
        }
    }

    // -----------------------------------------------------------------------
    // Subscription management
    // -----------------------------------------------------------------------

    fn handle_subscribe_success(&mut self, channel: &str, result: &proto::SubscribeResult) {
        if let Some(sub) = self.subs.get_mut(channel) {
            let was_recovering = sub.recover && (sub.offset > 0 || !sub.epoch.is_empty());

            sub.state = SubscriptionState::Subscribed;
            sub.resubscribe_attempts = 0;

            if result.recoverable {
                sub.recover = true;
                sub.offset = result.offset;
                sub.epoch = result.epoch.clone();
            }
            if result.positioned {
                sub.offset = result.offset;
                sub.epoch = result.epoch.clone();
            }

            if let Some(ref cb) = sub.config.events.on_subscribed {
                cb(SubscribedContext {
                    channel: channel.to_string(),
                    recoverable: result.recoverable,
                    positioned: result.positioned,
                    stream_position: if result.positioned || result.recoverable {
                        Some(StreamPosition {
                            offset: result.offset,
                            epoch: result.epoch.clone(),
                        })
                    } else {
                        None
                    },
                    was_recovering,
                    recovered: result.recovered,
                    data: result.data.clone(),
                });
            }

            // Deliver recovered publications
            for pub_msg in &result.publications {
                if pub_msg.offset > 0 {
                    sub.offset = pub_msg.offset;
                }
                if let Some(ref cb) = sub.config.events.on_publication {
                    cb(PublicationContext {
                        channel: channel.to_string(),
                        publication: Publication::from(pub_msg),
                    });
                }
            }

            // Schedule sub token refresh if needed
            if result.expires && result.ttl > 0 {
                self.schedule_sub_token_refresh(channel, result.ttl);
            }
        }
    }

    fn handle_subscribe_error(&mut self, channel: &str, err: ServerError) {
        if let Some(sub) = self.subs.get_mut(channel) {
            if codes::is_temporary_error(err.code, err.temporary) {
                // Temporary error - schedule resubscribe
                if let Some(ref cb) = sub.config.events.on_error {
                    cb(ErrorContext {
                        error: format!("subscribe error: {}: {}", err.code, err.message),
                    });
                }
                if err.code == codes::TOKEN_EXPIRED {
                    sub.token.clear();
                }
                let attempts = sub.resubscribe_attempts;
                sub.resubscribe_attempts += 1;
                let delay = backoff::next_delay(
                    attempts,
                    sub.config.min_resubscribe_delay,
                    sub.config.max_resubscribe_delay,
                );
                let channel = channel.to_string();
                let tx = self.cmd_tx.clone();
                tokio::spawn(async move {
                    time::sleep(delay).await;
                    let _ = tx.send(ActorCommand::Resubscribe { channel }).await;
                });
            } else {
                // Permanent error - unsubscribe
                sub.state = SubscriptionState::Unsubscribed;
                sub.resubscribe_attempts = 0;
                if let Some(ref cb) = sub.config.events.on_unsubscribed {
                    cb(UnsubscribedContext {
                        code: err.code,
                        reason: err.message,
                    });
                }
            }
        }
    }

    async fn resubscribe_all(&mut self) {
        let channels: Vec<String> = self
            .subs
            .iter()
            .filter(|(_, s)| s.state == SubscriptionState::Subscribing)
            .map(|(ch, _)| ch.clone())
            .collect();

        for channel in channels {
            self.do_subscribe(&channel).await;
        }
    }

    async fn do_subscribe(&mut self, channel: &str) {
        let sub = match self.subs.get_mut(channel) {
            Some(s) => s,
            None => return,
        };

        if sub.state == SubscriptionState::Unsubscribed {
            return;
        }

        // Token refresh if needed
        if sub.token.is_empty()
            && let Some(get_token) = sub.config.get_token.as_ref()
        {
            match get_token(channel.to_string()).await {
                Ok(token) => {
                    if token.is_empty() {
                        sub.state = SubscriptionState::Unsubscribed;
                        sub.resubscribe_attempts = 0;
                        if let Some(ref cb) = sub.config.events.on_unsubscribed {
                            cb(UnsubscribedContext {
                                code: codes::unsubscribed::UNAUTHORIZED,
                                reason: "unauthorized".into(),
                            });
                        }
                        return;
                    }
                    sub.token = token;
                }
                Err(CentrifugeError::Unauthorized) => {
                    sub.state = SubscriptionState::Unsubscribed;
                    sub.resubscribe_attempts = 0;
                    if let Some(ref cb) = sub.config.events.on_unsubscribed {
                        cb(UnsubscribedContext {
                            code: codes::unsubscribed::UNAUTHORIZED,
                            reason: "unauthorized".into(),
                        });
                    }
                    return;
                }
                Err(e) => {
                    if let Some(ref cb) = sub.config.events.on_error {
                        cb(ErrorContext {
                            error: format!("subscription token: {e}"),
                        });
                    }
                    // Schedule retry
                    let attempts = sub.resubscribe_attempts;
                    sub.resubscribe_attempts += 1;
                    let delay = backoff::next_delay(
                        attempts,
                        sub.config.min_resubscribe_delay,
                        sub.config.max_resubscribe_delay,
                    );
                    let ch = channel.to_string();
                    let tx = self.cmd_tx.clone();
                    tokio::spawn(async move {
                        time::sleep(delay).await;
                        let _ = tx.send(ActorCommand::Resubscribe { channel: ch }).await;
                    });
                    return;
                }
            }
        }

        let sub = self.subs.get(channel).unwrap();

        let id = self.next_cmd_id();
        let cmd = proto::Command {
            id,
            subscribe: Some(proto::SubscribeRequest {
                channel: channel.to_string(),
                token: sub.token.clone(),
                recover: sub.recover && (sub.offset > 0 || !sub.epoch.is_empty()),
                epoch: sub.epoch.clone(),
                offset: sub.offset,
                data: sub.config.data.clone(),
                positioned: sub.config.positioned,
                recoverable: sub.config.recoverable,
                join_leave: sub.config.join_leave,
                ..Default::default()
            }),
            ..Default::default()
        };

        // Create a oneshot for internal tracking (not user-facing)
        let (tx, _rx) = oneshot::channel();
        self.pending.insert(
            id,
            PendingRequest::Subscribe {
                channel: channel.to_string(),
                sender: tx,
            },
        );

        if let Err(e) = self.send_command(&cmd).await {
            self.pending.remove(&id);
            debug!(error = %e, channel, "failed to send subscribe");
        }
    }

    // -----------------------------------------------------------------------
    // Server-side subscriptions
    // -----------------------------------------------------------------------

    fn process_server_subs(&mut self, result: &proto::ConnectResult) {
        let old_channels: Vec<String> = self.server_subs.keys().cloned().collect();

        for (channel, sub_result) in &result.subs {
            let was_recovering = self.server_subs.contains_key(channel);

            self.server_subs.insert(
                channel.clone(),
                ServerSubState {
                    recoverable: sub_result.recoverable,
                    positioned: sub_result.positioned,
                    offset: sub_result.offset,
                    epoch: sub_result.epoch.clone(),
                },
            );

            if let Some(ref cb) = self.config.events.on_server_subscribed {
                cb(ServerSubscribedContext {
                    channel: channel.clone(),
                    recoverable: sub_result.recoverable,
                    positioned: sub_result.positioned,
                    stream_position: if sub_result.positioned || sub_result.recoverable {
                        Some(StreamPosition {
                            offset: sub_result.offset,
                            epoch: sub_result.epoch.clone(),
                        })
                    } else {
                        None
                    },
                    was_recovering,
                    recovered: sub_result.recovered,
                    data: sub_result.data.clone(),
                });
            }

            // Deliver recovered publications
            for pub_msg in &sub_result.publications {
                if let Some(ss) = self.server_subs.get_mut(channel)
                    && pub_msg.offset > 0 {
                        ss.offset = pub_msg.offset;
                    }
                if let Some(ref cb) = self.config.events.on_server_publication {
                    cb(ServerPublicationContext {
                        channel: channel.clone(),
                        publication: Publication::from(pub_msg),
                    });
                }
            }
        }

        // Detect disappeared server-side subs
        for channel in old_channels {
            if !result.subs.contains_key(&channel) {
                self.server_subs.remove(&channel);
                if let Some(ref cb) = self.config.events.on_server_unsubscribed {
                    cb(ServerUnsubscribedContext {
                        channel,
                        code: 0,
                        reason: "subscription not found after reconnect".into(),
                    });
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Token refresh scheduling
    // -----------------------------------------------------------------------

    fn schedule_token_refresh(&self) {
        if !self.token_expires || self.token_ttl == 0 {
            return;
        }
        let delay = Duration::from_secs(self.token_ttl as u64);
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            time::sleep(delay).await;
            let _ = tx.send(ActorCommand::RefreshToken).await;
        });
    }

    fn schedule_sub_token_refresh(&self, channel: &str, ttl: u32) {
        if ttl == 0 {
            return;
        }
        let delay = Duration::from_secs(ttl as u64);
        let channel = channel.to_string();
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            time::sleep(delay).await;
            let _ = tx.send(ActorCommand::RefreshSubToken { channel }).await;
        });
    }

    async fn do_refresh_token(&mut self) {
        if self.state != ClientState::Connected {
            return;
        }

        match self.do_token_refresh().await {
            Ok(()) => {
                // Send refresh command
                let id = self.next_cmd_id();
                let cmd = proto::Command {
                    id,
                    refresh: Some(proto::RefreshRequest {
                        token: self.config.token.clone(),
                    }),
                    ..Default::default()
                };
                let (tx, _) = oneshot::channel();
                self.pending.insert(id, PendingRequest::Refresh(tx));
                if let Err(e) = self.send_command(&cmd).await {
                    self.pending.remove(&id);
                    debug!(error = %e, "failed to send refresh");
                    // Schedule retry
                    self.schedule_token_refresh_retry();
                }
            }
            Err(CentrifugeError::Unauthorized) => {
                self.move_to_disconnected(codes::disconnect::UNAUTHORIZED, "unauthorized");
            }
            Err(e) => {
                if let Some(ref cb) = self.config.events.on_error {
                    cb(ErrorContext {
                        error: format!("token refresh: {e}"),
                    });
                }
                self.schedule_token_refresh_retry();
            }
        }
    }

    fn schedule_token_refresh_retry(&self) {
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_secs(10)).await;
            let _ = tx.send(ActorCommand::RefreshToken).await;
        });
    }

    async fn do_refresh_sub_token(&mut self, channel: String) {
        if self.state != ClientState::Connected {
            return;
        }

        let sub = match self.subs.get_mut(&channel) {
            Some(s) if s.state == SubscriptionState::Subscribed => s,
            _ => return,
        };

        if let Some(ref get_token) = sub.config.get_token {
            match get_token(channel.clone()).await {
                Ok(token) => {
                    if token.is_empty() {
                        if let Some(sub) = self.subs.get_mut(&channel) {
                            sub.state = SubscriptionState::Unsubscribed;
                            if let Some(ref cb) = sub.config.events.on_unsubscribed {
                                cb(UnsubscribedContext {
                                    code: codes::unsubscribed::UNAUTHORIZED,
                                    reason: "unauthorized".into(),
                                });
                            }
                        }
                        return;
                    }

                    if let Some(sub) = self.subs.get_mut(&channel) {
                        sub.token = token.clone();
                    }

                    let id = self.next_cmd_id();
                    let cmd = proto::Command {
                        id,
                        sub_refresh: Some(proto::SubRefreshRequest {
                            channel: channel.clone(),
                            token,
                        }),
                        ..Default::default()
                    };
                    let (tx, _) = oneshot::channel();
                    self.pending.insert(
                        id,
                        PendingRequest::SubRefresh {
                            channel: channel.clone(),
                            sender: tx,
                        },
                    );
                    if let Err(e) = self.send_command(&cmd).await {
                        self.pending.remove(&id);
                        debug!(error = %e, "failed to send sub_refresh");
                    }
                }
                Err(CentrifugeError::Unauthorized) => {
                    if let Some(sub) = self.subs.get_mut(&channel) {
                        sub.state = SubscriptionState::Unsubscribed;
                        if let Some(ref cb) = sub.config.events.on_unsubscribed {
                            cb(UnsubscribedContext {
                                code: codes::unsubscribed::UNAUTHORIZED,
                                reason: "unauthorized".into(),
                            });
                        }
                    }
                }
                Err(e) => {
                    if let Some(sub) = self.subs.get(&channel)
                        && let Some(ref cb) = sub.config.events.on_error {
                            cb(ErrorContext {
                                error: format!("subscription token refresh: {e}"),
                            });
                        }
                    // Schedule retry
                    self.schedule_sub_token_refresh(&channel, 10);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Transport close handling
    // -----------------------------------------------------------------------

    async fn on_transport_close(&mut self, info: Option<transport::DisconnectInfo>) {
        let info = info.unwrap_or(transport::DisconnectInfo {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason: "transport closed".into(),
            reconnect: true,
        });

        self.sink = None;
        self.stream = None;
        self.fail_all_pending(CentrifugeError::ClientDisconnected);

        if info.reconnect && self.connect_requested {
            // Move subs to subscribing
            for sub in self.subs.values_mut() {
                if sub.state == SubscriptionState::Subscribed {
                    sub.state = SubscriptionState::Subscribing;
                    sub.resubscribe_attempts = 0;
                    if let Some(ref cb) = sub.config.events.on_subscribing {
                        cb(SubscribingContext {
                            code: codes::subscribing::TRANSPORT_CLOSED,
                            reason: info.reason.clone(),
                        });
                    }
                }
            }

            // Emit server subscribing
            for channel in self.server_subs.keys() {
                if let Some(ref cb) = self.config.events.on_server_subscribing {
                    cb(ServerSubscribingContext {
                        channel: channel.clone(),
                        code: codes::subscribing::TRANSPORT_CLOSED,
                        reason: info.reason.clone(),
                    });
                }
            }

            self.move_to_connecting(info.code, &info.reason);
        } else {
            self.move_to_disconnected(info.code, &info.reason);
        }
    }

    // -----------------------------------------------------------------------
    // Command handling
    // -----------------------------------------------------------------------

    async fn handle_command(&mut self, cmd: ActorCommand) {
        match cmd {
            ActorCommand::Connect { reply } => {
                match self.state {
                    ClientState::Disconnected => {
                        self.connect_requested = true;
                        self.connect_waiters.push(reply);
                        self.move_to_connecting(
                            codes::connecting::CONNECT_CALLED,
                            "connect called",
                        );
                    }
                    ClientState::Connecting => {
                        self.connect_waiters.push(reply);
                    }
                    ClientState::Connected => {
                        let _ = reply.send(Ok(()));
                    }
                    ClientState::Closed => {
                        let _ = reply.send(Err(CentrifugeError::ClientClosed));
                    }
                }
            }
            ActorCommand::Disconnect { reply } => {
                self.move_to_disconnected(
                    codes::disconnect::DISCONNECT_CALLED,
                    "disconnect called",
                );
                let _ = reply.send(Ok(()));
            }
            ActorCommand::Close { reply } => {
                self.move_to_closed();
                let _ = reply.send(Ok(()));
            }
            ActorCommand::NewSubscription {
                channel,
                config,
                reply,
            } => {
                if let std::collections::hash_map::Entry::Vacant(e) = self.subs.entry(channel) {
                    e.insert(SubState::new(*config));
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err(CentrifugeError::DuplicateSubscription));
                }
            }
            ActorCommand::GetSubscription { channel, reply } => {
                let _ = reply.send(self.subs.contains_key(&channel));
            }
            ActorCommand::RemoveSubscription { channel, reply } => {
                if let Some(mut sub) = self.subs.remove(&channel) {
                    if sub.state != SubscriptionState::Unsubscribed
                        && self.state == ClientState::Connected
                    {
                        // Send unsubscribe command
                        let id = self.next_cmd_id();
                        let cmd = proto::Command {
                            id,
                            unsubscribe: Some(proto::UnsubscribeRequest {
                                channel: channel.clone(),
                            }),
                            ..Default::default()
                        };
                        let _ = self.send_command(&cmd).await;
                    }
                    sub.state = SubscriptionState::Unsubscribed;
                }
                let _ = reply.send(Ok(()));
            }
            ActorCommand::Subscribe { channel, reply } => {
                if let Some(sub) = self.subs.get_mut(&channel) {
                    if sub.state == SubscriptionState::Subscribed {
                        let _ = reply.send(Ok(()));
                        return;
                    }
                    sub.state = SubscriptionState::Subscribing;
                    sub.resubscribe_attempts = 0;
                    if let Some(ref cb) = sub.config.events.on_subscribing {
                        cb(SubscribingContext {
                            code: codes::subscribing::SUBSCRIBE_CALLED,
                            reason: "subscribe called".into(),
                        });
                    }
                    if self.state == ClientState::Connected {
                        // Register user reply
                        let id = self.next_cmd_id();
                        // We need to do subscribe inline
                        let sub = self.subs.get(&channel).unwrap();
                        let cmd = proto::Command {
                            id,
                            subscribe: Some(proto::SubscribeRequest {
                                channel: channel.clone(),
                                token: sub.token.clone(),
                                recover: sub.recover && (sub.offset > 0 || !sub.epoch.is_empty()),
                                epoch: sub.epoch.clone(),
                                offset: sub.offset,
                                data: sub.config.data.clone(),
                                positioned: sub.config.positioned,
                                recoverable: sub.config.recoverable,
                                join_leave: sub.config.join_leave,
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                        self.pending
                            .insert(id, PendingRequest::Subscribe { channel, sender: reply });
                        if let Err(e) = self.send_command(&cmd).await
                            && let Some(req) = self.pending.remove(&id)
                                && let PendingRequest::Subscribe { sender, .. } = req {
                                    let _ = sender.send(Err(CentrifugeError::Transport(e.to_string())));
                                }
                    } else {
                        // Will subscribe when connected
                        self.connect_waiters.push(reply);
                    }
                } else {
                    let _ = reply.send(Err(CentrifugeError::SubscriptionUnsubscribed));
                }
            }
            ActorCommand::Unsubscribe { channel, reply } => {
                if let Some(sub) = self.subs.get_mut(&channel) {
                    let was_subscribed = sub.state == SubscriptionState::Subscribed;
                    sub.state = SubscriptionState::Unsubscribed;
                    sub.resubscribe_attempts = 0;

                    if let Some(ref cb) = sub.config.events.on_unsubscribed {
                        cb(UnsubscribedContext {
                            code: codes::unsubscribed::UNSUBSCRIBE_CALLED,
                            reason: "unsubscribe called".into(),
                        });
                    }

                    if was_subscribed && self.state == ClientState::Connected {
                        let id = self.next_cmd_id();
                        let cmd = proto::Command {
                            id,
                            unsubscribe: Some(proto::UnsubscribeRequest {
                                channel: channel.clone(),
                            }),
                            ..Default::default()
                        };
                        self.pending.insert(id, PendingRequest::Unsubscribe(reply));
                        if let Err(_) = self.send_command(&cmd).await
                            && let Some(PendingRequest::Unsubscribe(tx)) =
                                self.pending.remove(&id)
                            {
                                let _ = tx.send(Ok(())); // Best effort
                            }
                    } else {
                        let _ = reply.send(Ok(()));
                    }
                } else {
                    let _ = reply.send(Ok(()));
                }
            }
            ActorCommand::Publish {
                channel,
                data,
                reply,
            } => {
                if self.state != ClientState::Connected {
                    let _ = reply.send(Err(CentrifugeError::ClientDisconnected));
                    return;
                }
                let id = self.next_cmd_id();
                let cmd = proto::Command {
                    id,
                    publish: Some(proto::PublishRequest { channel, data }),
                    ..Default::default()
                };
                self.pending.insert(id, PendingRequest::Publish(reply));
                if let Err(e) = self.send_command(&cmd).await
                    && let Some(PendingRequest::Publish(tx)) = self.pending.remove(&id) {
                        let _ = tx.send(Err(CentrifugeError::Transport(e.to_string())));
                    }
            }
            ActorCommand::History {
                channel,
                opts,
                reply,
            } => {
                if self.state != ClientState::Connected {
                    let _ = reply.send(Err(CentrifugeError::ClientDisconnected));
                    return;
                }
                let id = self.next_cmd_id();
                let cmd = proto::Command {
                    id,
                    history: Some(proto::HistoryRequest {
                        channel,
                        limit: opts.limit,
                        since: opts.since.map(|s| proto::StreamPosition {
                            offset: s.offset,
                            epoch: s.epoch,
                        }),
                        reverse: opts.reverse,
                    }),
                    ..Default::default()
                };
                self.pending.insert(id, PendingRequest::History(reply));
                if let Err(e) = self.send_command(&cmd).await
                    && let Some(PendingRequest::History(tx)) = self.pending.remove(&id) {
                        let _ = tx.send(Err(CentrifugeError::Transport(e.to_string())));
                    }
            }
            ActorCommand::Presence { channel, reply } => {
                if self.state != ClientState::Connected {
                    let _ = reply.send(Err(CentrifugeError::ClientDisconnected));
                    return;
                }
                let id = self.next_cmd_id();
                let cmd = proto::Command {
                    id,
                    presence: Some(proto::PresenceRequest { channel }),
                    ..Default::default()
                };
                self.pending.insert(id, PendingRequest::Presence(reply));
                if let Err(e) = self.send_command(&cmd).await
                    && let Some(PendingRequest::Presence(tx)) = self.pending.remove(&id) {
                        let _ = tx.send(Err(CentrifugeError::Transport(e.to_string())));
                    }
            }
            ActorCommand::PresenceStats { channel, reply } => {
                if self.state != ClientState::Connected {
                    let _ = reply.send(Err(CentrifugeError::ClientDisconnected));
                    return;
                }
                let id = self.next_cmd_id();
                let cmd = proto::Command {
                    id,
                    presence_stats: Some(proto::PresenceStatsRequest { channel }),
                    ..Default::default()
                };
                self.pending
                    .insert(id, PendingRequest::PresenceStats(reply));
                if let Err(e) = self.send_command(&cmd).await
                    && let Some(PendingRequest::PresenceStats(tx)) = self.pending.remove(&id) {
                        let _ = tx.send(Err(CentrifugeError::Transport(e.to_string())));
                    }
            }
            ActorCommand::Rpc {
                method,
                data,
                reply,
            } => {
                if self.state != ClientState::Connected {
                    let _ = reply.send(Err(CentrifugeError::ClientDisconnected));
                    return;
                }
                let id = self.next_cmd_id();
                let cmd = proto::Command {
                    id,
                    rpc: Some(proto::RpcRequest { data, method }),
                    ..Default::default()
                };
                self.pending.insert(id, PendingRequest::Rpc(reply));
                if let Err(e) = self.send_command(&cmd).await
                    && let Some(PendingRequest::Rpc(tx)) = self.pending.remove(&id) {
                        let _ = tx.send(Err(CentrifugeError::Transport(e.to_string())));
                    }
            }
            ActorCommand::Send { data } => {
                if self.state == ClientState::Connected {
                    let cmd = proto::Command {
                        id: 0,
                        send: Some(proto::SendRequest { data }),
                        ..Default::default()
                    };
                    let _ = self.send_command(&cmd).await;
                }
            }
            ActorCommand::Resubscribe { channel } => {
                if self.state == ClientState::Connected {
                    self.do_subscribe(&channel).await;
                }
            }
            ActorCommand::RefreshToken => {
                self.do_refresh_token().await;
            }
            ActorCommand::RefreshSubToken { channel } => {
                self.do_refresh_sub_token(channel).await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Transport helpers
    // -----------------------------------------------------------------------

    async fn send_command(&mut self, cmd: &proto::Command) -> Result<()> {
        let data = self.codec.encode_commands(std::slice::from_ref(cmd))?;
        if let Some(ref mut sink) = self.sink {
            sink.send_data(data)
                .await
                .map_err(CentrifugeError::Transport)?;
            Ok(())
        } else {
            Err(CentrifugeError::ClientDisconnected)
        }
    }

    async fn read_reply_with_timeout(&mut self, expected_id: u32) -> Result<proto::Reply> {
        let timeout = self.config.timeout;
        let deadline = time::sleep(timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                frame = async {
                    if let Some(ref mut stream) = self.stream {
                        stream.next().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    match frame {
                        Some(TransportFrame::Data(data)) => {
                            let replies = self.codec.decode_replies(&data)?;
                            for reply in replies {
                                if reply.id == expected_id {
                                    return Ok(reply);
                                }
                                self.dispatch_reply(reply).await;
                            }
                        }
                        Some(TransportFrame::Close(_)) | None => {
                            return Err(CentrifugeError::ClientDisconnected);
                        }
                    }
                }
                _ = &mut deadline => {
                    return Err(CentrifugeError::Timeout);
                }
            }
        }
    }
}
