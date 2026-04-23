use tokio::sync::{mpsc, oneshot};

use crate::client::actor::ActorCommand;
use crate::client::request;
use crate::config::{ProtocolType, SubscriptionConfig};
use crate::errors::{CentrifugeError, Result};
use crate::protocol::{proto, types::*};

/// Handle to a client-side subscription. Cheaply cloneable.
///
/// ```ignore
/// let sub = client.new_subscription("chat", Default::default()).await?;
/// let mut events = sub.events()?;
/// sub.subscribe().await?;
/// ```
#[derive(Clone)]
pub struct Subscription {
    pub(crate) channel: String,
    pub(crate) cmd_tx: mpsc::Sender<ActorCommand>,
}

impl Subscription {
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Get a stream of subscription events (publications, state changes, errors).
    pub fn events(&self) -> Result<mpsc::Receiver<SubEvent>> {
        let (tx, rx) = mpsc::channel(256);
        self.cmd_tx
            .try_send(ActorCommand::SetSubEventChannel {
                channel: self.channel.clone(),
                tx,
            })
            .map_err(|_| CentrifugeError::ClientClosed)?;
        Ok(rx)
    }

    // --- Protocol methods ---

    pub async fn subscribe(&self) -> Result<()> {
        self.subscribe_inner().await
    }

    pub(crate) async fn subscribe_inner(&self) -> Result<()> {
        request(&self.cmd_tx, |reply| ActorCommand::Subscribe {
            channel: self.channel.clone(),
            reply,
        })
        .await
    }

    pub async fn unsubscribe(&self) -> Result<()> {
        request(&self.cmd_tx, |reply| ActorCommand::Unsubscribe {
            channel: self.channel.clone(),
            reply,
        })
        .await
    }

    pub async fn publish(&self, data: Vec<u8>) -> Result<()> {
        self.send_proto_ok(proto::Command {
            publish: Some(proto::PublishRequest {
                channel: self.channel.clone(),
                data,
            }),
            ..Default::default()
        })
        .await
    }

    pub async fn history(&self, opts: HistoryOptions) -> Result<HistoryResult> {
        let cmd = proto::Command {
            history: Some(proto::HistoryRequest {
                channel: self.channel.clone(),
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

    pub async fn presence(&self) -> Result<PresenceResult> {
        let cmd = proto::Command {
            presence: Some(proto::PresenceRequest {
                channel: self.channel.clone(),
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

    pub async fn presence_stats(&self) -> Result<PresenceStatsResult> {
        let cmd = proto::Command {
            presence_stats: Some(proto::PresenceStatsRequest {
                channel: self.channel.clone(),
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

    async fn send_proto(&self, cmd: proto::Command) -> Result<proto::Reply> {
        request(&self.cmd_tx, |reply| ActorCommand::SendRequest {
            cmd: Box::new(cmd),
            reply,
        })
        .await
    }

    async fn send_proto_ok(&self, cmd: proto::Command) -> Result<()> {
        self.send_proto_extract(cmd, |_| Some(()), "").await
    }

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
}

/// If the data is a JSON-encoded string (starts/ends with `"`), extract
/// the inner string content as bytes. This handles the case where
/// embedded_json deserialization wraps delta bytes in JSON string encoding.
fn unwrap_json_string(data: &[u8]) -> Vec<u8> {
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data)
        && let Some(s) = value.as_str()
    {
        return s.as_bytes().to_vec();
    }
    data.to_vec()
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
    pub subscribe_waiters: Vec<oneshot::Sender<Result<()>>>,
    pub event_tx: Option<mpsc::Sender<SubEvent>>,
    pub delta_negotiated: bool,
    pub prev_data: Vec<u8>,
}

impl SubState {
    /// Emit a subscription event (no-op if no event receiver is set).
    pub fn emit(&self, event: SubEvent) {
        if let Some(ref tx) = self.event_tx {
            let _ = tx.try_send(event);
        }
    }

    pub fn new(config: SubscriptionConfig) -> Self {
        let token = config.token.clone();
        let recover = config.since.is_some() || config.recoverable;
        let offset = config.since.as_ref().map_or(0, |s| s.offset);
        let epoch = config.since.as_ref().map(|s| s.epoch.clone()).unwrap_or_default();
        Self {
            config,
            state: SubscriptionState::Unsubscribed,
            offset,
            epoch,
            recover,
            resubscribe_attempts: 0,
            token,
            subscribe_waiters: Vec::new(),
            event_tx: None,
            delta_negotiated: false,
            prev_data: Vec::new(),
        }
    }

    /// Apply delta if negotiated, returning the full data for the publication.
    ///
    /// In JSON mode, `embedded_json` deserialization re-wraps byte fields as a
    /// JSON string; we unwrap those before fossil apply or storing as
    /// `prev_data`. In Protobuf mode, `pub.data` is already raw bytes and must
    /// be used as-is (unwrapping would corrupt payloads that happen to parse as
    /// a JSON string literal). Matches the Go SDK split at `subscription.go:546-586`
    /// and the JS split between `json.ts` and `protobuf.codec.ts`.
    pub fn apply_delta(&mut self, pub_data: &[u8], is_delta: bool, protocol_type: ProtocolType) -> Vec<u8> {
        if !self.delta_negotiated {
            return pub_data.to_vec();
        }
        let raw_data = match protocol_type {
            ProtocolType::Json => unwrap_json_string(pub_data),
            ProtocolType::Protobuf => pub_data.to_vec(),
        };
        if is_delta {
            match crate::delta::apply_delta(&self.prev_data, &raw_data) {
                Ok(full_data) => {
                    self.prev_data.clone_from(&full_data);
                    full_data
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to apply fossil delta, using raw data");
                    raw_data
                }
            }
        } else {
            self.prev_data.clone_from(&raw_data);
            raw_data
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn negotiated_sub_state() -> SubState {
        let mut s = SubState::new(SubscriptionConfig::default());
        s.delta_negotiated = true;
        s
    }

    #[test]
    fn apply_delta_noop_when_not_negotiated() {
        let mut s = SubState::new(SubscriptionConfig::default());
        let data = b"\"hello\"";
        let result = s.apply_delta(data, false, ProtocolType::Protobuf);
        assert_eq!(result, data);
        let result = s.apply_delta(data, false, ProtocolType::Json);
        assert_eq!(result, data);
        assert!(
            s.prev_data.is_empty(),
            "prev_data must not change when delta is not negotiated"
        );
    }

    // Protobuf raw bytes that happen to form a valid JSON string literal must
    // be preserved verbatim — unwrapping would corrupt the payload. This path
    // is what the Go SDK handles at subscription.go:574-584 (the `else` branch
    // for non-JSON protocol).
    #[test]
    fn apply_delta_protobuf_does_not_unwrap_json_looking_bytes() {
        let mut s = negotiated_sub_state();
        let data = b"\"hello\"";
        let result = s.apply_delta(data, false, ProtocolType::Protobuf);
        assert_eq!(result, data, "protobuf bytes must not be JSON-unwrapped");
        assert_eq!(s.prev_data, data, "prev_data must store raw bytes verbatim");
    }

    // JSON mode: embedded_json re-encodes byte fields as JSON strings, so delta
    // apply and prev_data must operate on the unwrapped form. Matches Go
    // subscription.go:550-572.
    #[test]
    fn apply_delta_json_unwraps_json_string_encoded_data() {
        let mut s = negotiated_sub_state();
        let data = b"\"hello\"";
        let result = s.apply_delta(data, false, ProtocolType::Json);
        assert_eq!(result, b"hello", "json: must unwrap JSON string wrapping");
        assert_eq!(s.prev_data, b"hello");
    }

    // If the JSON codec hands us bytes that aren't a JSON string (e.g. an
    // object), `unwrap_json_string` is a no-op by design.
    #[test]
    fn apply_delta_json_leaves_non_string_json_alone() {
        let mut s = negotiated_sub_state();
        let data = br#"{"k":"v"}"#;
        let result = s.apply_delta(data, false, ProtocolType::Json);
        assert_eq!(result, data);
        assert_eq!(s.prev_data, data);
    }
}
