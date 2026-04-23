use std::collections::hash_map::Entry;

use tokio::sync::oneshot;

use super::actor::{ActorCommand, ConnectionActor, PendingRequest};
use crate::codes;
use crate::errors::{CentrifugeError, Result};
use crate::protocol::{proto, types::*};
use crate::subscription::SubState;

impl ConnectionActor {
    pub(super) async fn handle_command(&mut self, cmd: ActorCommand) {
        match cmd {
            ActorCommand::Connect { reply } => match self.state {
                ClientState::Disconnected => {
                    self.connect_requested = true;
                    self.connect_waiters.push(reply);
                    self.move_to_connecting(codes::connecting::CONNECT_CALLED, "connect called");
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
            },
            ActorCommand::Disconnect { reply } => {
                self.move_to_disconnected(codes::disconnect::DISCONNECT_CALLED, "disconnect called");
                let _ = reply.send(Ok(()));
            }
            ActorCommand::Close { reply } => {
                self.move_to_closed();
                let _ = reply.send(Ok(()));
            }
            ActorCommand::NewSubscription { channel, config, reply } => {
                if let Entry::Vacant(e) = self.subs.entry(channel) {
                    e.insert(SubState::new(*config));
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err(CentrifugeError::DuplicateSubscription));
                }
            }
            ActorCommand::GetSubscription { channel, reply } => {
                let _ = reply.send(self.subs.contains_key(&channel));
            }
            ActorCommand::ListSubscriptions { reply } => {
                let list = self.subs.iter().map(|(ch, s)| (ch.clone(), s.state)).collect();
                let _ = reply.send(list);
            }
            ActorCommand::SetClientEventChannel { tx } => {
                self.client_event_tx = Some(tx);
            }
            ActorCommand::SetSubEventChannel { channel, tx } => {
                if let Some(sub) = self.subs.get_mut(&channel) {
                    sub.event_tx = Some(tx);
                }
            }
            ActorCommand::RemoveSubscription { channel, reply } => {
                self.handle_remove_subscription(channel, reply).await;
            }
            ActorCommand::Subscribe { channel, reply } => {
                self.handle_subscribe(channel, reply).await;
            }
            ActorCommand::Unsubscribe { channel, reply } => {
                self.handle_unsubscribe(channel, reply).await;
            }
            ActorCommand::SendRequest { cmd, reply } => {
                self.send_request_command(*cmd, PendingRequest::Request(reply)).await;
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
            ActorCommand::SetToken { token } => {
                self.config.token = token;
            }
            ActorCommand::SetData { data } => {
                self.config.data = data;
            }
            ActorCommand::StartBatching => {
                self.batch.active = true;
            }
            ActorCommand::StopBatching => {
                self.flush_batch().await;
            }
            ActorCommand::RefreshToken => {
                self.do_refresh_token().await;
            }
            ActorCommand::RefreshSubToken { channel } => {
                self.do_refresh_sub_token(channel).await;
            }
            ActorCommand::RequestTimeout { id } => {
                if let Some(req) = self.pending.remove(&id)
                    && let Some(channel) = req.fail(|| CentrifugeError::Timeout)
                    && let Some(sub) = self.subs.get_mut(&channel)
                {
                    for w in sub.subscribe_waiters.drain(..) {
                        let _ = w.send(Err(CentrifugeError::Timeout));
                    }
                    // Subscribe timeout likely means a broken connection.
                    // Disconnect and reconnect, matching Go/JS client behavior.
                    self.on_transport_close(Some(crate::transport::DisconnectInfo {
                        code: codes::connecting::SUBSCRIBE_TIMEOUT,
                        reason: "subscribe timeout".into(),
                        reconnect: true,
                    }))
                    .await;
                }
            }
        }
    }

    async fn handle_subscribe(&mut self, channel: String, reply: oneshot::Sender<Result<()>>) {
        let Some(sub) = self.subs.get_mut(&channel) else {
            let _ = reply.send(Err(CentrifugeError::SubscriptionUnsubscribed));
            return;
        };

        if sub.state == SubscriptionState::Subscribed {
            let _ = reply.send(Ok(()));
            return;
        }

        // Already in-flight — attach to the existing attempt so both callers
        // resolve together, and do not re-emit Subscribing or reset the
        // backoff counter mid-backoff. Matches Go (subscription.go:401-407)
        // and JS (subscription.ts:319-322), which both early-return from
        // Subscribing.
        if sub.state == SubscriptionState::Subscribing {
            sub.subscribe_waiters.push(reply);
            return;
        }

        sub.state = SubscriptionState::Subscribing;
        sub.resubscribe_attempts = 0;
        sub.emit(SubEvent::Subscribing(SubscribingContext {
            code: codes::subscribing::SUBSCRIBE_CALLED,
            reason: "subscribe called".into(),
        }));

        if self.state != ClientState::Connected {
            if let Some(sub) = self.subs.get_mut(&channel) {
                sub.subscribe_waiters.push(reply);
            }
            return;
        }

        let id = self.next_cmd_id();
        let Some(cmd) = self.build_subscribe_command(id, &channel) else {
            let _ = reply.send(Err(CentrifugeError::SubscriptionUnsubscribed));
            return;
        };

        self.pending
            .insert(id, PendingRequest::Subscribe { channel, sender: reply });
        self.schedule_request_timeout(id);
        if let Err(e) = self.send_command(&cmd).await
            && let Some(req) = self.pending.remove(&id)
            && let PendingRequest::Subscribe { sender, .. } = req
        {
            let _ = sender.send(Err(e));
        }
    }

    async fn handle_unsubscribe(&mut self, channel: String, reply: oneshot::Sender<Result<()>>) {
        let Some(sub) = self.subs.get_mut(&channel) else {
            let _ = reply.send(Ok(()));
            return;
        };

        let was_subscribed = sub.state == SubscriptionState::Subscribed;
        sub.state = SubscriptionState::Unsubscribed;
        sub.resubscribe_attempts = 0;
        sub.emit(SubEvent::Unsubscribed(UnsubscribedContext {
            code: codes::unsubscribed::UNSUBSCRIBE_CALLED,
            reason: "unsubscribe called".into(),
        }));

        if was_subscribed && self.state == ClientState::Connected {
            let id = self.next_cmd_id();
            let cmd = proto::Command {
                id,
                unsubscribe: Some(proto::UnsubscribeRequest {
                    channel: channel.clone(),
                }),
                ..Default::default()
            };
            let (unsub_tx, _) = oneshot::channel();
            self.pending.insert(id, PendingRequest::Request(unsub_tx));
            self.schedule_request_timeout(id);
            if self.send_command(&cmd).await.is_err() {
                let _ = reply.send(Ok(()));
                self.on_transport_close(Some(crate::transport::DisconnectInfo {
                    code: codes::connecting::UNSUBSCRIBE_ERROR,
                    reason: "unsubscribe error".into(),
                    reconnect: true,
                }))
                .await;
                return;
            }
        }

        let _ = reply.send(Ok(()));
    }

    async fn handle_remove_subscription(&mut self, channel: String, reply: oneshot::Sender<Result<()>>) {
        let Some(mut sub) = self.subs.remove(&channel) else {
            let _ = reply.send(Ok(()));
            return;
        };

        // Spec (client_sdk.md:218): "Subscription is automatically unsubscribed
        // before being removed." Matches JS SDK (centrifuge.ts:191-200). Skipped
        // when already Unsubscribed so consumers don't see a duplicate event.
        let was_subscribed = sub.state == SubscriptionState::Subscribed;
        if sub.state != SubscriptionState::Unsubscribed {
            sub.state = SubscriptionState::Unsubscribed;
            sub.resubscribe_attempts = 0;
            for waiter in sub.subscribe_waiters.drain(..) {
                let _ = waiter.send(Err(CentrifugeError::SubscriptionUnsubscribed));
            }
            sub.emit(SubEvent::Unsubscribed(UnsubscribedContext {
                code: codes::unsubscribed::UNSUBSCRIBE_CALLED,
                reason: "unsubscribe called".into(),
            }));
        }

        // Wire unsubscribe is only meaningful while the server believes we're
        // subscribed — i.e. the sub was in the Subscribed state. Mirrors the
        // `was_subscribed && Connected` gate in handle_unsubscribe.
        if was_subscribed && self.state == ClientState::Connected {
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

        let _ = reply.send(Ok(()));
    }

    /// Helper for simple request-response commands (publish, history, presence, etc.)
    async fn send_request_command(&mut self, mut cmd: proto::Command, pending: PendingRequest) {
        if self.state != ClientState::Connected {
            pending.fail(|| CentrifugeError::ClientDisconnected);
            return;
        }
        let id = self.next_cmd_id();
        cmd.id = id;
        self.pending.insert(id, pending);
        self.schedule_request_timeout(id);
        if let Err(e) = self.send_command(&cmd).await
            && let Some(req) = self.pending.remove(&id)
        {
            req.fail(move || e);
        }
    }
}
