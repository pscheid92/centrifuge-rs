use tokio::sync::oneshot;

use crate::config::SubscriptionConfig;
use crate::errors::{CentrifugeError, Result};
use crate::protocol::types::*;

/// Handle to a client-side subscription. Cheaply cloneable.
#[derive(Clone)]
pub struct Subscription {
    pub(crate) channel: String,
    pub(crate) cmd_tx: tokio::sync::mpsc::Sender<crate::client::actor::ActorCommand>,
}

impl Subscription {
    /// Returns the channel name.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Start subscribing to the channel.
    pub async fn subscribe(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(crate::client::actor::ActorCommand::Subscribe {
                channel: self.channel.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Unsubscribe from the channel.
    pub async fn unsubscribe(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(crate::client::actor::ActorCommand::Unsubscribe {
                channel: self.channel.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Publish data to the subscription channel.
    pub async fn publish(&self, data: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(crate::client::actor::ActorCommand::Publish {
                channel: self.channel.clone(),
                data,
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Request channel history.
    pub async fn history(&self, opts: HistoryOptions) -> Result<HistoryResult> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(crate::client::actor::ActorCommand::History {
                channel: self.channel.clone(),
                opts,
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Request channel presence.
    pub async fn presence(&self) -> Result<PresenceResult> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(crate::client::actor::ActorCommand::Presence {
                channel: self.channel.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Request channel presence stats.
    pub async fn presence_stats(&self) -> Result<PresenceStatsResult> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(crate::client::actor::ActorCommand::PresenceStats {
                channel: self.channel.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }
}


/// Internal subscription state held by the actor.
pub(crate) struct SubState {
    pub config: SubscriptionConfig,
    pub state: SubscriptionState,
    pub offset: u64,
    pub epoch: String,
    pub recover: bool,
    pub resubscribe_attempts: u32,
    pub token: String,
}

impl SubState {
    pub fn new(config: SubscriptionConfig) -> Self {
        let token = config.token.clone();
        let recover = config.since.is_some() || config.recoverable;
        let offset = config.since.as_ref().map(|s| s.offset).unwrap_or(0);
        let epoch = config
            .since
            .as_ref()
            .map(|s| s.epoch.clone())
            .unwrap_or_default();
        Self {
            config,
            state: SubscriptionState::Unsubscribed,
            offset,
            epoch,
            recover,
            resubscribe_attempts: 0,
            token,
        }
    }
}
