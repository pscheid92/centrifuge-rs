use tracing::debug;

use super::actor::{ActorCommand, ConnectionActor, ServerSubState};
use crate::codes;
use crate::protocol::{proto, types::*};
use crate::transport;

impl ConnectionActor {
    pub(super) async fn handle_push(&mut self, push: proto::Push) {
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
        } else if let Some(message) = push.message {
            self.emit_client_event(ClientEvent::Message(MessageContext { data: message.data }));
        }
    }

    fn handle_publication(&mut self, channel: &str, pub_msg: &proto::Publication) {
        if let Some(sub) = self.subs.get_mut(channel) {
            if pub_msg.offset > 0 {
                sub.offset = pub_msg.offset;
            }
            let data = sub.apply_delta(&pub_msg.data, pub_msg.delta);
            let mut publication = Publication::from(pub_msg);
            publication.data = data;
            sub.emit(SubEvent::Publication(publication));
            return;
        }

        if let Some(server_sub) = self.server_subs.get_mut(channel) {
            if pub_msg.offset > 0 {
                server_sub.offset = pub_msg.offset;
            }
            let publication = Publication::from(pub_msg);
            self.emit_client_event(ClientEvent::ServerPublication(ServerPublicationContext {
                channel: channel.to_string(),
                publication,
            }));
            return;
        }

        debug!(channel = %channel, "received publication for unknown channel");
    }

    fn handle_join(&mut self, channel: &str, join: &proto::Join) {
        let info = join.info.as_ref().map(ClientInfo::from).unwrap_or_default();

        if let Some(sub) = self.subs.get(channel) {
            sub.emit(SubEvent::Join(info));
            return;
        }

        if self.server_subs.contains_key(channel) {
            self.emit_client_event(ClientEvent::ServerJoin(ServerJoinContext {
                channel: channel.to_string(),
                info,
            }));
        } else {
            debug!(channel = %channel, "received join for unknown channel");
        }
    }

    fn handle_leave(&mut self, channel: &str, leave: &proto::Leave) {
        let info = leave.info.as_ref().map(ClientInfo::from).unwrap_or_default();

        if let Some(sub) = self.subs.get(channel) {
            sub.emit(SubEvent::Leave(info));
            return;
        }

        if self.server_subs.contains_key(channel) {
            self.emit_client_event(ClientEvent::ServerLeave(ServerLeaveContext {
                channel: channel.to_string(),
                info,
            }));
        } else {
            debug!(channel = %channel, "received leave for unknown channel");
        }
    }

    fn handle_server_unsubscribe(&mut self, channel: &str, code: u32, reason: &str) {
        if let Some(sub) = self.subs.get_mut(channel) {
            if codes::should_resubscribe_on_unsubscribe(code) {
                sub.state = SubscriptionState::Subscribing;
                sub.emit(SubEvent::Subscribing(SubscribingContext {
                    code,
                    reason: reason.to_string(),
                }));
                let channel = channel.to_string();
                let tx = self.cmd_tx.clone();
                tokio::spawn(async move {
                    let _ = tx.send(ActorCommand::Resubscribe { channel }).await;
                });
            } else {
                sub.state = SubscriptionState::Unsubscribed;
                sub.resubscribe_attempts = 0;
                sub.emit(SubEvent::Unsubscribed(UnsubscribedContext {
                    code,
                    reason: reason.to_string(),
                }));
            }
            return;
        }

        if self.server_subs.remove(channel).is_some() {
            self.emit_client_event(ClientEvent::ServerUnsubscribed(ServerUnsubscribedContext {
                channel: channel.to_string(),
                code,
                reason: reason.to_string(),
            }));
        }
    }

    fn handle_server_subscribe(&mut self, channel: &str, sub: &proto::Subscribe) {
        self.server_subs.insert(
            channel.to_string(),
            ServerSubState {
                recoverable: sub.recoverable,
                offset: sub.offset,
                epoch: sub.epoch.clone(),
            },
        );

        self.emit_client_event(ClientEvent::ServerSubscribed(ServerSubscribedContext {
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
            has_recovered_publications: false,
            data: sub.data.clone(),
        }));
    }

    async fn handle_disconnect_push(&mut self, disconnect: proto::Disconnect) {
        // Reconnect decision is defined purely by code ranges per the Centrifuge SDK
        // spec (see additionals/client_sdk.md — "Disconnect codes"). The proto's
        // `reconnect` field is ignored for push disconnects, matching the Go and JS
        // reference SDKs (client.go:897-904, centrifuge.ts:1738-1745).
        let reconnect = codes::should_reconnect_on_disconnect(disconnect.code);
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
}
