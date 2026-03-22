pub(crate) mod actor;
mod commands;
mod connect;
mod push;
mod server_subs;
mod state;
mod subscriptions;
mod token;
mod transport_io;

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::{mpsc, oneshot};

use crate::config::{ClientConfig, SubscriptionConfig};
use crate::errors::{CentrifugeError, Result};
use crate::protocol::{proto, types::*};
use crate::subscription::Subscription;

/// Send a command to the actor and await the reply.
pub(crate) async fn request<T>(
    cmd_tx: &mpsc::Sender<actor::ActorCommand>,
    make_cmd: impl FnOnce(oneshot::Sender<Result<T>>) -> actor::ActorCommand,
) -> Result<T> {
    let (tx, rx) = oneshot::channel();
    cmd_tx
        .send(make_cmd(tx))
        .await
        .map_err(|_| CentrifugeError::ClientClosed)?;
    rx.await.map_err(|_| CentrifugeError::ClientClosed)?
}

/// Handle to a Centrifuge client. Cheaply cloneable.
#[derive(Clone)]
pub struct Client {
    cmd_tx: mpsc::Sender<actor::ActorCommand>,
    shared_state: Arc<AtomicU8>,
}

impl Client {
    /// Creates a new Client and spawns the background connection actor.
    pub fn new(config: ClientConfig) -> Self {
        let transport = Box::new(crate::transport::WsTransport::new(
            config.url.clone(),
            config.protocol_type,
            config.headers.clone(),
        ));
        Self::new_with_transport(config, transport)
    }

    /// Creates a new Client with a custom transport (useful for testing).
    pub fn new_with_transport(config: ClientConfig, transport: Box<dyn crate::transport::Transport>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let shared_state = Arc::new(AtomicU8::new(0));
        let actor = actor::ConnectionActor::new(config, cmd_rx, cmd_tx.clone(), transport, shared_state.clone());
        tokio::spawn(actor.run());
        Client { cmd_tx, shared_state }
    }

    /// Query current connection state. Sync — no actor roundtrip needed.
    pub fn state(&self) -> ClientState {
        ClientState::from_u8(self.shared_state.load(Ordering::Relaxed))
    }

    /// Get a stream of client events (connection lifecycle + server-side subscriptions).
    pub fn events(&self) -> Result<mpsc::Receiver<ClientEvent>> {
        let (tx, rx) = mpsc::channel(256);
        self.cmd_tx
            .try_send(actor::ActorCommand::SetClientEventChannel { tx })
            .map_err(|_| CentrifugeError::ClientClosed)?;
        Ok(rx)
    }

    pub async fn subscriptions(&self) -> Vec<(String, SubscriptionState)> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(actor::ActorCommand::ListSubscriptions { reply: tx })
            .await
            .is_err()
        {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn connect(&self) -> Result<()> {
        request(&self.cmd_tx, |reply| actor::ActorCommand::Connect { reply }).await
    }

    pub async fn disconnect(&self) -> Result<()> {
        request(&self.cmd_tx, |reply| actor::ActorCommand::Disconnect { reply }).await
    }

    pub async fn close(&self) -> Result<()> {
        request(&self.cmd_tx, |reply| actor::ActorCommand::Close { reply }).await
    }

    /// Create a subscription with default config. Set events via builder methods
    /// on the returned Subscription before calling subscribe().
    pub async fn new_subscription_default(&self, channel: impl Into<String>) -> Result<Subscription> {
        self.new_subscription(channel, SubscriptionConfig::default()).await
    }

    /// Create, register, and subscribe in one call. Returns the subscription
    /// handle and an event stream. Idiomatic Rust alternative to the callback API.
    ///
    /// ```ignore
    /// let (sub, mut events) = client.subscribe("chat").await?;
    /// while let Some(event) = events.recv().await {
    ///     match event {
    ///         SubEvent::Publication(pub) => println!("{:?}", pub.data),
    ///         SubEvent::Unsubscribed(_) => break,
    ///         _ => {}
    ///     }
    /// }
    /// ```
    pub async fn subscribe(&self, channel: impl Into<String>) -> Result<(Subscription, mpsc::Receiver<SubEvent>)> {
        let sub = self.new_subscription_default(channel).await?;
        let events = sub.events()?;
        sub.subscribe_inner().await?;
        Ok((sub, events))
    }

    /// Create a subscription with full config.
    pub async fn new_subscription(
        &self,
        channel: impl Into<String>,
        config: SubscriptionConfig,
    ) -> Result<Subscription> {
        let channel = channel.into();
        request(&self.cmd_tx, |reply| actor::ActorCommand::NewSubscription {
            channel: channel.clone(),
            config: Box::new(config),
            reply,
        })
        .await?;
        Ok(Subscription {
            channel,
            cmd_tx: self.cmd_tx.clone(),
        })
    }

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
        Ok(exists.then(|| Subscription {
            channel: channel.to_string(),
            cmd_tx: self.cmd_tx.clone(),
        }))
    }

    pub async fn remove_subscription(&self, sub: &Subscription) -> Result<()> {
        request(&self.cmd_tx, |reply| actor::ActorCommand::RemoveSubscription {
            channel: sub.channel.clone(),
            reply,
        })
        .await
    }

    pub async fn rpc(&self, method: impl Into<String>, data: Vec<u8>) -> Result<RpcResult> {
        let cmd = proto::Command {
            rpc: Some(proto::RpcRequest {
                method: method.into(),
                data,
            }),
            ..Default::default()
        };
        let result = self.send_proto_extract(cmd, |r| r.rpc, "rpc").await?;
        Ok(RpcResult { data: result.data })
    }

    /// Send a protocol command and return the raw reply.
    async fn send_proto(&self, cmd: proto::Command) -> Result<proto::Reply> {
        request(&self.cmd_tx, |reply| actor::ActorCommand::SendRequest {
            cmd: Box::new(cmd),
            reply,
        })
        .await
    }

    /// Send a protocol command, check for errors, return Ok(()) on success.
    async fn send_proto_ok(&self, cmd: proto::Command) -> Result<()> {
        self.send_proto_extract(cmd, |_| Some(()), "").await
    }

    /// Send a protocol command, check for server error, extract a result field.
    async fn send_proto_extract<T>(
        &self,
        cmd: proto::Command,
        extract: impl FnOnce(proto::Reply) -> Option<T>,
        name: &str,
    ) -> Result<T> {
        let reply = self.send_proto(cmd).await?;
        if let Some(err) = reply.error {
            return Err(CentrifugeError::Server(ServerError::from(&err)));
        }
        extract(reply).ok_or_else(|| CentrifugeError::Protocol(format!("missing {name} result")))
    }

    /// Update the connection token for the next reconnect attempt.
    pub fn set_token(&self, token: impl Into<String>) {
        let _ = self
            .cmd_tx
            .try_send(actor::ActorCommand::SetToken { token: token.into() });
    }

    /// Update the connection data for the next reconnect attempt.
    pub fn set_data(&self, data: Vec<u8>) {
        let _ = self.cmd_tx.try_send(actor::ActorCommand::SetData { data });
    }

    /// Start batching commands. Commands sent between `start_batching` and
    /// `stop_batching` are queued and sent as a single WebSocket frame.
    pub fn start_batching(&self) {
        let _ = self.cmd_tx.try_send(actor::ActorCommand::StartBatching);
    }

    /// Stop batching and send all queued commands as a single frame.
    pub fn stop_batching(&self) {
        let _ = self.cmd_tx.try_send(actor::ActorCommand::StopBatching);
    }

    pub async fn send(&self, data: Vec<u8>) -> Result<()> {
        self.cmd_tx
            .send(actor::ActorCommand::Send { data })
            .await
            .map_err(|_| CentrifugeError::ClientClosed)?;
        Ok(())
    }

    pub async fn publish(&self, channel: impl Into<String>, data: Vec<u8>) -> Result<()> {
        self.send_proto_ok(proto::Command {
            publish: Some(proto::PublishRequest {
                channel: channel.into(),
                data,
            }),
            ..Default::default()
        })
        .await
    }

    pub async fn history(&self, channel: impl Into<String>, opts: HistoryOptions) -> Result<HistoryResult> {
        let cmd = proto::Command {
            history: Some(proto::HistoryRequest {
                channel: channel.into(),
                limit: opts.limit,
                since: opts.since.map(|s| proto::StreamPosition {
                    offset: s.offset,
                    epoch: s.epoch,
                }),
                reverse: opts.reverse,
            }),
            ..Default::default()
        };
        let result = self.send_proto_extract(cmd, |r| r.history, "history").await?;
        Ok(HistoryResult {
            publications: result.publications.iter().map(Publication::from).collect(),
            offset: result.offset,
            epoch: result.epoch,
        })
    }

    pub async fn presence(&self, channel: impl Into<String>) -> Result<PresenceResult> {
        let cmd = proto::Command {
            presence: Some(proto::PresenceRequest {
                channel: channel.into(),
            }),
            ..Default::default()
        };
        let result = self.send_proto_extract(cmd, |r| r.presence, "presence").await?;
        Ok(PresenceResult {
            presence: result
                .presence
                .iter()
                .map(|(k, v)| (k.clone(), ClientInfo::from(v)))
                .collect(),
        })
    }

    pub async fn presence_stats(&self, channel: impl Into<String>) -> Result<PresenceStatsResult> {
        let cmd = proto::Command {
            presence_stats: Some(proto::PresenceStatsRequest {
                channel: channel.into(),
            }),
            ..Default::default()
        };
        let result = self
            .send_proto_extract(cmd, |r| r.presence_stats, "presence_stats")
            .await?;
        Ok(PresenceStatsResult {
            num_clients: result.num_clients,
            num_users: result.num_users,
        })
    }
}
