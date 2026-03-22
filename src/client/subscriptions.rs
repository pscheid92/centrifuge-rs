use tokio::sync::oneshot;
use tokio::time;
use tracing::debug;

use super::actor::{ActorCommand, ConnectionActor, PendingRequest};
use crate::backoff;
use crate::codes;
use crate::config::DeltaType;
use crate::errors::CentrifugeError;
use crate::protocol::{proto, types::*};

impl ConnectionActor {
    /// Build a subscribe command for the given channel.
    /// Returns None if the subscription doesn't exist.
    pub(super) fn build_subscribe_command(&self, id: u32, channel: &str) -> Option<proto::Command> {
        let sub = self.subs.get(channel)?;
        Some(proto::Command {
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
                delta: match sub.config.delta {
                    DeltaType::Fossil => "fossil".into(),
                    DeltaType::None => String::new(),
                },
                tf: sub.config.tags_filter.clone(),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    pub(super) fn handle_subscribe_success(&mut self, channel: &str, result: &proto::SubscribeResult) {
        let Some(sub) = self.subs.get_mut(channel) else { return };
        let was_recovering = sub.recover && (sub.offset > 0 || !sub.epoch.is_empty());

        sub.state = SubscriptionState::Subscribed;
        sub.resubscribe_attempts = 0;
        sub.delta_negotiated = result.delta;

        if result.recoverable {
            sub.recover = true;
            sub.offset = result.offset;
            sub.epoch = result.epoch.clone();
        }
        if result.positioned {
            sub.offset = result.offset;
            sub.epoch = result.epoch.clone();
        }

        sub.emit(SubEvent::Subscribed(SubscribedContext {
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
            has_recovered_publications: !result.publications.is_empty(),
            data: result.data.clone(),
        }));

        for pub_msg in &result.publications {
            if pub_msg.offset > 0 {
                sub.offset = pub_msg.offset;
            }
            let data = sub.apply_delta(&pub_msg.data, pub_msg.delta);
            let mut publication = Publication::from(pub_msg);
            publication.data = data;
            sub.emit(SubEvent::Publication(publication));
        }

        for waiter in sub.subscribe_waiters.drain(..) {
            let _ = waiter.send(Ok(()));
        }

        if result.expires && result.ttl > 0 {
            self.schedule_sub_token_refresh(channel, result.ttl);
        }
    }

    pub(super) fn handle_subscribe_error(&mut self, channel: &str, err: ServerError) {
        let Some(sub) = self.subs.get_mut(channel) else { return };
        if codes::server_error::is_temporary(err.code, err.temporary) {
            sub.emit(SubEvent::Error(ErrorContext {
                error: format!("subscribe error: {}: {}", err.code, err.message),
            }));
            if err.code == codes::server_error::TOKEN_EXPIRED {
                sub.token.clear();
            }
            sub.resubscribe_attempts += 1;
            self.schedule_resubscribe(channel);
        } else {
            sub.state = SubscriptionState::Unsubscribed;
            sub.resubscribe_attempts = 0;
            for waiter in sub.subscribe_waiters.drain(..) {
                let _ = waiter.send(Err(CentrifugeError::Server(ServerError {
                    code: err.code,
                    message: err.message.clone(),
                    temporary: err.temporary,
                })));
            }
            sub.emit(SubEvent::Unsubscribed(UnsubscribedContext {
                code: err.code,
                reason: err.message,
            }));
        }
    }

    /// Schedule a resubscribe attempt after exponential backoff.
    fn schedule_resubscribe(&self, channel: &str) {
        let Some(sub) = self.subs.get(channel) else { return };
        let delay = backoff::next_delay(
            sub.resubscribe_attempts.saturating_sub(1),
            sub.config.min_resubscribe_delay,
            sub.config.max_resubscribe_delay,
        );
        let channel = channel.to_string();
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            time::sleep(delay).await;
            let _ = tx.send(ActorCommand::Resubscribe { channel }).await;
        });
    }

    pub(super) async fn resubscribe_all(&mut self) {
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

    pub(super) async fn do_subscribe(&mut self, channel: &str) {
        let Some(sub) = self.subs.get(channel) else { return };
        if sub.state == SubscriptionState::Unsubscribed {
            return;
        }

        let needs_token = sub.token.is_empty() && sub.config.get_token.is_some();
        if needs_token {
            let Some(sub) = self.subs.get(channel) else { return };
            let Some(get_token) = sub.config.get_token.as_ref() else {
                return;
            };
            let result = get_token(channel.to_string()).await;
            match result {
                Ok(token) if token.is_empty() => {
                    self.unsubscribe_unauthorized(channel);
                    return;
                }
                Ok(token) => {
                    let Some(sub) = self.subs.get_mut(channel) else { return };
                    sub.token = token;
                }
                Err(CentrifugeError::Unauthorized) => {
                    self.unsubscribe_unauthorized(channel);
                    return;
                }
                Err(e) => {
                    let Some(sub) = self.subs.get_mut(channel) else { return };
                    sub.emit(SubEvent::Error(ErrorContext {
                        error: format!("subscription token: {e}"),
                    }));
                    sub.resubscribe_attempts += 1;
                    self.schedule_resubscribe(channel);
                    return;
                }
            }
        }

        // Refresh subscription data if a callback is provided.
        let has_get_data = self.subs.get(channel).is_some_and(|s| s.config.get_data.is_some());
        if has_get_data {
            let Some(sub) = self.subs.get(channel) else { return };
            let Some(get_data) = sub.config.get_data.as_ref() else {
                return;
            };
            match get_data(channel.to_string()).await {
                Ok(data) => {
                    let Some(sub) = self.subs.get_mut(channel) else { return };
                    sub.config.data = data;
                }
                Err(e) => {
                    let Some(sub) = self.subs.get(channel) else { return };
                    sub.emit(SubEvent::Error(ErrorContext {
                        error: format!("get_data: {e}"),
                    }));
                    // Continue with existing data on error.
                }
            }
        }

        let id = self.next_cmd_id();
        let Some(cmd) = self.build_subscribe_command(id, channel) else {
            return;
        };

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
}
