pub(crate) mod actor;

use tokio::sync::{mpsc, oneshot};

use crate::config::{ClientConfig, SubscriptionConfig};
use crate::errors::{CentrifugeError, Result};
use crate::protocol::types::*;
use crate::subscription::Subscription;

/// Handle to a Centrifuge client. Cheaply cloneable.
#[derive(Clone)]
pub struct Client {
    cmd_tx: mpsc::Sender<actor::ActorCommand>,
}

impl Client {
    /// Creates a new Client and spawns the background connection actor.
    pub fn new(config: ClientConfig) -> Self {
        let transport = Box::new(crate::transport::WsTransport::new(
            config.url.clone(),
            config.protocol_type,
        ));
        Self::new_with_transport(config, transport)
    }

    /// Creates a new Client with a custom transport (useful for testing).
    pub fn new_with_transport(
        config: ClientConfig,
        transport: Box<dyn crate::transport::Transport>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let actor = actor::ConnectionActor::new(config, cmd_rx, cmd_tx.clone(), transport);
        tokio::spawn(actor.run());
        Client { cmd_tx }
    }

    /// Initiate connection to the server.
    pub async fn connect(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::Connect { reply: tx })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Disconnect from the server. No reconnection will be attempted.
    pub async fn disconnect(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::Disconnect { reply: tx })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Close the client permanently and release all resources.
    pub async fn close(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::Close { reply: tx })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Create and register a new subscription. Returns error if channel is already subscribed.
    pub async fn new_subscription(
        &self,
        channel: impl Into<String>,
        config: SubscriptionConfig,
    ) -> Result<Subscription> {
        let channel = channel.into();
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::NewSubscription {
                channel: channel.clone(),
                config: Box::new(config),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)??;
        Ok(Subscription {
            channel,
            cmd_tx: self.cmd_tx.clone(),
        })
    }

    /// Get an existing subscription by channel name.
    pub async fn get_subscription(&self, channel: &str) -> Result<Option<Subscription>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::GetSubscription {
                channel: channel.to_string(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        let exists = rx.await.map_err(|_| CentrifugeError::ClientClosed)?;
        if exists {
            Ok(Some(Subscription {
                channel: channel.to_string(),
                cmd_tx: self.cmd_tx.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Remove a subscription from the registry.
    pub async fn remove_subscription(&self, sub: &Subscription) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::RemoveSubscription {
                channel: sub.channel.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Send an RPC request.
    pub async fn rpc(&self, method: impl Into<String>, data: Vec<u8>) -> Result<RpcResult> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::Rpc {
                method: method.into(),
                data,
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Send an async message (fire-and-forget).
    pub async fn send(&self, data: Vec<u8>) -> Result<()> {
        self.cmd_tx
            .send(actor::ActorCommand::Send { data })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        Ok(())
    }

    /// Publish to a channel (for server-side subscriptions).
    pub async fn publish(&self, channel: impl Into<String>, data: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::Publish {
                channel: channel.into(),
                data,
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Get history for a channel (for server-side subscriptions).
    pub async fn history(
        &self,
        channel: impl Into<String>,
        opts: HistoryOptions,
    ) -> Result<HistoryResult> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::History {
                channel: channel.into(),
                opts,
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Get presence for a channel (for server-side subscriptions).
    pub async fn presence(&self, channel: impl Into<String>) -> Result<PresenceResult> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::Presence {
                channel: channel.into(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }

    /// Get presence stats for a channel (for server-side subscriptions).
    pub async fn presence_stats(
        &self,
        channel: impl Into<String>,
    ) -> Result<PresenceStatsResult> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(actor::ActorCommand::PresenceStats {
                channel: channel.into(),
                reply: tx,
            })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        rx.await.map_err(|_| CentrifugeError::ClientClosed)?
    }
}
