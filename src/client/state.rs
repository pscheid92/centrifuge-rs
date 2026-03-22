use tracing::debug;

use super::actor::ConnectionActor;
use crate::codes;
use crate::errors::CentrifugeError;
use crate::protocol::{proto, types::*};

impl ConnectionActor {
    pub(super) fn move_to_connecting(&mut self, code: u32, reason: &str) {
        debug!(code, reason, "moving to connecting");
        self.state = ClientState::Connecting;
        self.update_shared_state();
        self.emit_client_event(ClientEvent::Connecting(ConnectingContext {
            code,
            reason: reason.to_string(),
        }));
    }

    pub(super) fn move_to_connected(&mut self, result: &proto::ConnectResult) {
        self.state = ClientState::Connected;
        self.client_id = result.client.clone();
        self.reconnect_attempts = 0;

        if result.ping > 0 {
            self.ping.interval = std::time::Duration::from_secs(result.ping as u64);
        }
        self.ping.send_pong = result.pong;
        self.ping.last_data_received = tokio::time::Instant::now();

        self.token.expires = result.expires;
        self.token.ttl = result.ttl;

        debug!(
            client_id = %self.client_id,
            ping = ?self.ping.interval,
            pong = self.ping.send_pong,
            "connected"
        );

        self.update_shared_state();
        self.emit_client_event(ClientEvent::Connected(ConnectedContext {
            client_id: result.client.clone(),
            version: result.version.clone(),
            data: result.data.clone(),
            session: result.session.clone(),
            node: result.node.clone(),
        }));
    }

    pub(super) fn move_to_disconnected(&mut self, code: u32, reason: &str) {
        debug!(code, reason, "moving to disconnected");
        let was_connected = self.state == ClientState::Connected;
        self.state = ClientState::Disconnected;
        self.update_shared_state();
        self.client_id.clear();
        self.connect_requested = false;

        self.sink = None;
        self.stream = None;
        self.fail_all_pending();

        for w in self.connect_waiters.drain(..) {
            let _ = w.send(Err(CentrifugeError::ClientDisconnected));
        }

        if was_connected {
            self.unsubscribe_all_subs(
                || CentrifugeError::ClientDisconnected,
                codes::unsubscribed::CLIENT_CLOSED,
                reason,
            );
        }

        let drained_server_subs: Vec<String> = self.server_subs.drain().map(|(ch, _)| ch).collect();
        for channel in drained_server_subs {
            self.emit_client_event(ClientEvent::ServerUnsubscribed(ServerUnsubscribedContext {
                channel,
                code,
                reason: reason.to_string(),
            }));
        }

        self.emit_client_event(ClientEvent::Disconnected(DisconnectedContext {
            code,
            reason: reason.to_string(),
        }));
    }

    pub(super) fn move_to_closed(&mut self) {
        debug!("moving to closed");
        self.state = ClientState::Closed;
        self.update_shared_state();
        self.sink = None;
        self.stream = None;
        self.fail_all_pending();
        for w in self.connect_waiters.drain(..) {
            let _ = w.send(Err(CentrifugeError::ClientClosed));
        }
        self.unsubscribe_all_subs(
            || CentrifugeError::ClientClosed,
            codes::unsubscribed::CLIENT_CLOSED,
            "client closed",
        );
    }

    pub(super) fn unsubscribe_unauthorized(&mut self, channel: &str) {
        if let Some(sub) = self.subs.get_mut(channel) {
            sub.state = SubscriptionState::Unsubscribed;
            sub.resubscribe_attempts = 0;
            for waiter in sub.subscribe_waiters.drain(..) {
                let _ = waiter.send(Err(CentrifugeError::Unauthorized));
            }
            sub.emit(SubEvent::Unsubscribed(UnsubscribedContext {
                code: codes::unsubscribed::UNAUTHORIZED,
                reason: "unauthorized".into(),
            }));
        }
    }

    fn unsubscribe_all_subs(&mut self, make_err: impl Fn() -> CentrifugeError, code: u32, reason: &str) {
        for sub in self.subs.values_mut() {
            if sub.state != SubscriptionState::Unsubscribed {
                sub.state = SubscriptionState::Unsubscribed;
                for waiter in sub.subscribe_waiters.drain(..) {
                    let _ = waiter.send(Err(make_err()));
                }
                sub.emit(SubEvent::Unsubscribed(UnsubscribedContext {
                    code,
                    reason: reason.to_string(),
                }));
            }
        }
    }

    pub(super) fn fail_all_pending(&mut self) {
        for (_, req) in self.pending.drain() {
            req.fail(|| CentrifugeError::ClientDisconnected);
        }
    }
}
