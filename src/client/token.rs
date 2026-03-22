use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time;
use tracing::debug;

use super::actor::{ActorCommand, ConnectionActor, PendingRequest};
use crate::codes;
use crate::errors::CentrifugeError;
use crate::protocol::{proto, types::*};

/// Calculate refresh delay: 90% of TTL to refresh before expiration.
fn refresh_delay(ttl: u32) -> Duration {
    Duration::from_millis((ttl as u64) * 900)
}

impl ConnectionActor {
    pub(super) fn schedule_token_refresh(&self) {
        if !self.token.expires || self.token.ttl == 0 {
            return;
        }
        let delay = refresh_delay(self.token.ttl);
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            time::sleep(delay).await;
            let _ = tx.send(ActorCommand::RefreshToken).await;
        });
    }

    pub(super) fn schedule_sub_token_refresh(&self, channel: &str, ttl: u32) {
        if ttl == 0 {
            return;
        }
        let delay = refresh_delay(ttl);
        let channel = channel.to_string();
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            time::sleep(delay).await;
            let _ = tx.send(ActorCommand::RefreshSubToken { channel }).await;
        });
    }

    pub(super) async fn do_refresh_token(&mut self) {
        if self.state != ClientState::Connected {
            return;
        }

        match self.do_token_refresh().await {
            Ok(()) => {
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
                    self.schedule_token_refresh_retry();
                }
            }
            Err(CentrifugeError::Unauthorized) => {
                self.move_to_disconnected(codes::disconnect::UNAUTHORIZED, "unauthorized");
            }
            Err(e) => {
                self.emit_error(format!("token refresh: {e}"));
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

    pub(super) async fn do_refresh_sub_token(&mut self, channel: String) {
        if self.state != ClientState::Connected {
            return;
        }

        let Some(sub) = self.subs.get(&channel) else { return };
        if sub.state != SubscriptionState::Subscribed {
            return;
        }
        let Some(get_token) = sub.config.get_token.as_ref() else {
            return;
        };
        let result = get_token(channel.clone()).await;
        match result {
            Ok(token) => {
                if token.is_empty() {
                    self.unsubscribe_unauthorized(&channel);
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
                self.unsubscribe_unauthorized(&channel);
            }
            Err(e) => {
                if let Some(sub) = self.subs.get(&channel) {
                    sub.emit(SubEvent::Error(ErrorContext {
                        error: format!("subscription token refresh: {e}"),
                    }));
                }
                self.schedule_sub_token_refresh(&channel, 10);
            }
        }
    }
}
