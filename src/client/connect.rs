use std::collections::HashMap;

use tokio::time::{self, Instant};
use tokio_stream::StreamExt;
use tracing::{debug, trace, warn};

use super::actor::{ConnectionActor, PendingRequest};
use crate::backoff;
use crate::codes;
use crate::config::DeltaType;
use crate::errors::{CentrifugeError, Result};
use crate::protocol::{proto, types::*};
use crate::transport::{self, TransportFrame};

impl ConnectionActor {
    pub(super) async fn do_connect_cycle(&mut self) {
        loop {
            if self.state != ClientState::Connecting {
                return;
            }

            if self.reconnect_attempts > 0 && !self.wait_backoff().await {
                return; // State changed during backoff (closed or disconnected).
            }
            self.reconnect_attempts += 1;

            if !self.refresh_token_if_needed().await {
                continue; // Token refresh failed, retry after backoff.
            }

            if !self.open_transport().await {
                continue; // Transport connect failed, retry after backoff.
            }

            match self.do_handshake().await {
                Ok(result) => {
                    self.on_handshake_success(result).await;
                    return;
                }
                Err(e) => {
                    self.on_handshake_failure(e);
                    if self.state != ClientState::Connecting {
                        return; // Permanent error caused terminal disconnect.
                    }
                    continue;
                }
            }
        }
    }

    /// Wait for backoff delay, processing commands while waiting.
    /// Returns false if the state changed (caller should return).
    async fn wait_backoff(&mut self) -> bool {
        let delay = backoff::next_delay(
            self.reconnect_attempts.saturating_sub(1),
            self.config.min_reconnect_delay,
            self.config.max_reconnect_delay,
        );
        debug!(attempt = self.reconnect_attempts, delay = ?delay, "reconnect backoff");

        let sleep = time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => return true,
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(cmd) => {
                        self.handle_command(cmd).await;
                        if self.state != ClientState::Connecting {
                            return false;
                        }
                    }
                    None => {
                        self.move_to_closed();
                        return false;
                    }
                },
            }
        }
    }

    /// Refresh the connection token if needed.
    /// Returns false if refresh failed (caller should continue to retry).
    async fn refresh_token_if_needed(&mut self) -> bool {
        let needs_refresh =
            self.token.refresh_required || (self.config.token.is_empty() && self.config.get_token.is_some());

        if !needs_refresh {
            return true;
        }

        match self.do_token_refresh().await {
            Ok(()) => {
                self.token.refresh_required = false;
                true
            }
            Err(CentrifugeError::Unauthorized) => {
                self.move_to_disconnected(codes::disconnect::UNAUTHORIZED, "unauthorized");
                true // State changed to Disconnected; outer loop will return.
            }
            Err(e) => {
                self.emit_error(format!("token refresh: {e}"));
                false
            }
        }
    }

    /// Open the transport connection.
    /// Returns false if connect failed (caller should continue to retry).
    async fn open_transport(&mut self) -> bool {
        match self.transport.connect().await {
            Ok(conn) => {
                self.sink = Some(conn.sink);
                self.stream = Some(conn.stream);
                true
            }
            Err(e) => {
                self.emit_error(format!("transport: {e}"));
                false
            }
        }
    }

    async fn on_handshake_success(&mut self, result: proto::ConnectResult) {
        self.move_to_connected(&result);
        self.process_server_subs(&result);
        self.schedule_token_refresh();
        self.resubscribe_all().await;
        for w in self.connect_waiters.drain(..) {
            let _ = w.send(Ok(()));
        }
    }

    fn on_handshake_failure(&mut self, err: CentrifugeError) {
        self.sink = None;
        self.stream = None;
        self.emit_error(format!("handshake: {err}"));

        if let CentrifugeError::Server(ref server_err) = err {
            if server_err.code == codes::server_error::TOKEN_EXPIRED {
                self.token.refresh_required = true;
            } else if !codes::server_error::is_temporary(server_err.code, server_err.temporary) {
                self.move_to_disconnected(server_err.code, &server_err.message);
            }
        }
    }

    pub(super) async fn do_token_refresh(&mut self) -> Result<()> {
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

    pub(super) async fn do_handshake(&mut self) -> Result<proto::ConnectResult> {
        // Refresh connection data if a callback is provided.
        if let Some(ref get_data) = self.config.get_data {
            match get_data().await {
                Ok(data) => self.config.data = data,
                Err(e) => {
                    self.emit_error(format!("get_data: {e}"));
                    // Continue with existing data on error.
                }
            }
        }

        let id = self.next_cmd_id();
        let mut subs_map = HashMap::new();

        // Server-side subs: include recoverable ones so the server can replay
        // missed publications as part of ConnectResult.subs.
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

        // Client-side subs in Subscribing state: batch them into the Connect
        // command so the server resolves them in ConnectResult.subs, saving N
        // separate Subscribe round-trips on reconnect. Matches JS's
        // startBatching + _sendSubscribeCommands + stopBatching pattern
        // around the connect reply (centrifuge.ts:1507-1509).
        //
        // Skip subs whose token or data callback still needs to run — let
        // do_subscribe handle them via the regular path, since the callback
        // is async and we can't block the handshake on it here.
        for (channel, sub) in &self.subs {
            if sub.state != SubscriptionState::Subscribing {
                continue;
            }
            let needs_token = sub.token.is_empty() && sub.config.get_token.is_some();
            if needs_token || sub.config.get_data.is_some() {
                continue;
            }
            subs_map.insert(
                channel.clone(),
                proto::SubscribeRequest {
                    channel: channel.clone(),
                    token: sub.token.clone(),
                    recover: sub.recover && (sub.offset > 0 || !sub.epoch.is_empty()),
                    epoch: sub.epoch.clone(),
                    offset: sub.offset,
                    data: sub.config.data.clone(),
                    positioned: sub.config.positioned,
                    recoverable: sub.config.recoverable,
                    join_leave: sub.config.join_leave,
                    delta: match sub.config.delta {
                        DeltaType::Fossil => "fossil".into(),
                        DeltaType::None => String::new(),
                    },
                    tf: sub.config.tags_filter.clone(),
                    ..Default::default()
                },
            );
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
        let reply = self.read_reply_with_timeout(id).await?;

        if let Some(err) = reply.error {
            return Err(CentrifugeError::Server(ServerError::from(&err)));
        }

        reply
            .connect
            .ok_or_else(|| CentrifugeError::Protocol("missing connect result".into()))
    }

    pub(super) async fn do_connected_loop(&mut self) {
        let ping_timeout = self.ping.interval + self.config.max_server_ping_delay;
        self.ping.last_data_received = Instant::now();

        loop {
            if self.state != ClientState::Connected {
                return;
            }

            let deadline = self.ping.last_data_received + ping_timeout;

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
                            self.ping.last_data_received = Instant::now();
                            self.handle_transport_data(&data).await;
                        }
                        Some(TransportFrame::Close(info)) => {
                            self.on_transport_close(info);
                            return;
                        }
                        None => {
                            self.on_transport_close(None);
                            return;
                        }
                    }
                }
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => {
                            self.move_to_closed();
                            return;
                        }
                    }
                }
                _ = time::sleep_until(deadline) => {
                    // sleep_until doesn't return early, so we're past the deadline.
                    debug!("ping timeout");
                    self.on_transport_close(Some(transport::DisconnectInfo {
                        code: codes::connecting::NO_PING,
                        reason: "no ping".into(),
                        reconnect: true,
                    }));
                    return;
                }
            }
        }
    }

    pub(super) async fn handle_transport_data(&mut self, data: &[u8]) {
        let replies = match self.codec.decode_replies(data) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to decode replies, disconnecting");
                self.on_transport_close(Some(transport::DisconnectInfo {
                    code: codes::disconnect::BAD_PROTOCOL,
                    reason: format!("decode error: {e}"),
                    reconnect: false,
                }));
                return;
            }
        };

        for reply in replies {
            self.dispatch_reply(reply).await;
        }
    }

    pub(super) async fn dispatch_reply(&mut self, reply: proto::Reply) {
        if reply.id == 0 && reply.push.is_none() {
            trace!("received server ping");
            if self.ping.send_pong {
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

        if reply.id == 0 {
            if let Some(push) = reply.push {
                self.handle_push(push);
            }
            return;
        }

        if let Some(req) = self.pending.remove(&reply.id) {
            self.resolve_pending(req, reply);
        } else {
            debug!(id = reply.id, "received reply for unknown request");
        }
    }

    pub(super) fn resolve_pending(&mut self, req: PendingRequest, reply: proto::Reply) {
        match req {
            PendingRequest::Subscribe { channel, sender } => {
                if let Some(ref err) = reply.error
                    && err.code == codes::server_error::ALREADY_SUBSCRIBED
                {
                    // Server says we're already subscribed — tolerate as success.
                    // This can happen when retrying a subscribe after a timeout.
                    let result = reply.subscribe.unwrap_or_default();
                    self.handle_subscribe_success(&channel, &result);
                    let _ = sender.send(Ok(()));
                } else if let Some(err) = reply.error {
                    let server_err = ServerError::from(&err);
                    self.handle_subscribe_error(&channel, server_err.clone());
                    let _ = sender.send(Err(CentrifugeError::Server(server_err)));
                } else if let Some(result) = reply.subscribe {
                    self.handle_subscribe_success(&channel, &result);
                    let _ = sender.send(Ok(()));
                } else {
                    let _ = sender.send(Err(CentrifugeError::Protocol("missing subscribe result".into())));
                }
            }
            PendingRequest::Request(tx) => {
                // Generic request — return the raw reply for the caller to interpret
                let _ = tx.send(Ok(reply));
            }
            PendingRequest::Refresh(tx) => {
                if let Some(err) = reply.error {
                    let _ = tx.send(Err(CentrifugeError::Server(ServerError::from(&err))));
                } else if let Some(result) = reply.refresh {
                    self.token.expires = result.expires;
                    self.token.ttl = result.ttl;
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
}
