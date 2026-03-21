use prost::Message;

use crate::errors::CentrifugeError;
use crate::protocol::proto;

/// Trait for encoding commands and decoding replies.
pub trait Codec: Send + Sync {
    fn encode_commands(&self, commands: &[proto::Command]) -> Result<Vec<u8>, CentrifugeError>;
    fn decode_replies(&self, data: &[u8]) -> Result<Vec<proto::Reply>, CentrifugeError>;
}

/// JSON codec: newline-delimited JSON encoding/decoding.
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode_commands(&self, commands: &[proto::Command]) -> Result<Vec<u8>, CentrifugeError> {
        let mut parts = Vec::with_capacity(commands.len());
        for cmd in commands {
            let json = serde_json::to_string(&CommandJson::from_proto(cmd))
                .map_err(|e| CentrifugeError::Protocol(format!("json encode: {e}")))?;
            parts.push(json);
        }
        Ok(parts.join("\n").into_bytes())
    }

    fn decode_replies(&self, data: &[u8]) -> Result<Vec<proto::Reply>, CentrifugeError> {
        let text =
            std::str::from_utf8(data).map_err(|e| CentrifugeError::Protocol(e.to_string()))?;
        let mut replies = Vec::new();
        for line in text.split('\n') {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let reply: ReplyJson = serde_json::from_str(line)
                .map_err(|e| CentrifugeError::Protocol(format!("json decode: {e}")))?;
            replies.push(reply.to_proto());
        }
        Ok(replies)
    }
}

/// Protobuf codec: varint-length-delimited encoding/decoding.
pub struct ProtobufCodec;

impl Codec for ProtobufCodec {
    fn encode_commands(&self, commands: &[proto::Command]) -> Result<Vec<u8>, CentrifugeError> {
        let mut buf = Vec::new();
        for cmd in commands {
            let encoded = cmd.encode_to_vec();
            prost::encoding::encode_varint(encoded.len() as u64, &mut buf);
            buf.extend_from_slice(&encoded);
        }
        Ok(buf)
    }

    fn decode_replies(&self, data: &[u8]) -> Result<Vec<proto::Reply>, CentrifugeError> {
        let mut replies = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let (len, bytes_read) = decode_varint(&data[offset..])
                .map_err(|e| CentrifugeError::Protocol(format!("varint decode: {e}")))?;
            offset += bytes_read;
            if offset + len > data.len() {
                return Err(CentrifugeError::Protocol(
                    "protobuf frame exceeds data length".into(),
                ));
            }
            let reply = proto::Reply::decode(&data[offset..offset + len])
                .map_err(|e| CentrifugeError::Protocol(format!("protobuf decode: {e}")))?;
            replies.push(reply);
            offset += len;
        }
        Ok(replies)
    }
}

fn decode_varint(buf: &[u8]) -> Result<(usize, usize), String> {
    let mut value: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in buf.iter().enumerate() {
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value as usize, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint too long".into());
        }
    }
    Err("unexpected end of varint".into())
}

/// Creates a codec for the given protocol type.
pub fn new_codec(protocol_type: crate::config::ProtocolType) -> Box<dyn Codec> {
    match protocol_type {
        crate::config::ProtocolType::Json => Box::new(JsonCodec),
        crate::config::ProtocolType::Protobuf => Box::new(ProtobufCodec),
    }
}

// ---------------------------------------------------------------------------
// JSON serialization layer
// ---------------------------------------------------------------------------
// The Centrifuge JSON protocol uses the same field names as protobuf but with
// JSON semantics: `data` fields are embedded JSON (not base64 bytes), and
// only non-default fields are serialized. We use intermediate serde structs
// rather than deriving on the prost types directly.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Default)]
struct CommandJson {
    #[serde(skip_serializing_if = "is_zero_u32")]
    id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    connect: Option<ConnectRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscribe: Option<SubscribeRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsubscribe: Option<UnsubscribeRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    publish: Option<PublishRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence: Option<PresenceRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence_stats: Option<PresenceStatsRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<HistoryRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ping: Option<PingRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    send: Option<SendRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpc: Option<RpcRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh: Option<RefreshRequestJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub_refresh: Option<SubRefreshRequestJson>,
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

#[derive(Serialize, Deserialize, Default)]
struct ConnectRequestJson {
    #[serde(skip_serializing_if = "String::is_empty", default)]
    token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    subs: HashMap<String, SubscribeRequestJson>,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    version: String,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct SubscribeRequestJson {
    #[serde(skip_serializing_if = "String::is_empty", default)]
    channel: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    token: String,
    #[serde(skip_serializing_if = "is_false", default)]
    recover: bool,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    epoch: String,
    #[serde(skip_serializing_if = "is_zero_u64", default)]
    offset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "is_false", default)]
    positioned: bool,
    #[serde(skip_serializing_if = "is_false", default)]
    recoverable: bool,
    #[serde(skip_serializing_if = "is_false", default)]
    join_leave: bool,
}

fn is_false(v: &bool) -> bool {
    !*v
}
fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

#[derive(Serialize, Deserialize, Default)]
struct UnsubscribeRequestJson {
    channel: String,
}

#[derive(Serialize, Deserialize, Default)]
struct PublishRequestJson {
    channel: String,
    data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Default)]
struct PresenceRequestJson {
    channel: String,
}

#[derive(Serialize, Deserialize, Default)]
struct PresenceStatsRequestJson {
    channel: String,
}

#[derive(Serialize, Deserialize, Default)]
struct HistoryRequestJson {
    channel: String,
    #[serde(skip_serializing_if = "is_zero_i32", default)]
    limit: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    since: Option<StreamPositionJson>,
    #[serde(skip_serializing_if = "is_false", default)]
    reverse: bool,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

#[derive(Serialize, Deserialize, Default)]
struct StreamPositionJson {
    offset: u64,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    epoch: String,
}

#[derive(Serialize, Deserialize, Default)]
struct PingRequestJson {}

#[derive(Serialize, Deserialize, Default)]
struct SendRequestJson {
    data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Default)]
struct RpcRequestJson {
    data: serde_json::Value,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    method: String,
}

#[derive(Serialize, Deserialize, Default)]
struct RefreshRequestJson {
    token: String,
}

#[derive(Serialize, Deserialize, Default)]
struct SubRefreshRequestJson {
    channel: String,
    token: String,
}

// --- Reply JSON types ---

#[derive(Deserialize, Default)]
struct ReplyJson {
    #[serde(default)]
    id: u32,
    #[serde(default)]
    error: Option<ErrorJson>,
    #[serde(default)]
    push: Option<PushJson>,
    #[serde(default)]
    connect: Option<ConnectResultJson>,
    #[serde(default)]
    subscribe: Option<SubscribeResultJson>,
    #[serde(default)]
    unsubscribe: Option<serde_json::Value>,
    #[serde(default)]
    publish: Option<serde_json::Value>,
    #[serde(default)]
    presence: Option<PresenceResultJson>,
    #[serde(default)]
    presence_stats: Option<PresenceStatsResultJson>,
    #[serde(default)]
    history: Option<HistoryResultJson>,
    #[serde(default)]
    ping: Option<serde_json::Value>,
    #[serde(default)]
    rpc: Option<RpcResultJson>,
    #[serde(default)]
    refresh: Option<RefreshResultJson>,
    #[serde(default)]
    sub_refresh: Option<SubRefreshResultJson>,
}

#[derive(Deserialize, Default)]
struct ErrorJson {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    message: String,
    #[serde(default)]
    temporary: bool,
}

#[derive(Deserialize, Default)]
struct PushJson {
    #[serde(default)]
    channel: String,
    #[serde(default, rename = "pub")]
    publication: Option<PublicationJson>,
    #[serde(default)]
    join: Option<JoinJson>,
    #[serde(default)]
    leave: Option<LeaveJson>,
    #[serde(default)]
    unsubscribe: Option<UnsubscribePushJson>,
    #[serde(default)]
    message: Option<MessageJson>,
    #[serde(default)]
    subscribe: Option<SubscribePushJson>,
    #[serde(default)]
    connect: Option<ConnectPushJson>,
    #[serde(default)]
    disconnect: Option<DisconnectPushJson>,
}

#[derive(Deserialize, Default, Clone)]
struct PublicationJson {
    #[serde(default)]
    data: serde_json::Value,
    #[serde(default)]
    info: Option<ClientInfoJson>,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    tags: HashMap<String, String>,
}

#[derive(Deserialize, Default, Clone)]
struct ClientInfoJson {
    #[serde(default)]
    user: String,
    #[serde(default)]
    client: String,
    #[serde(default)]
    conn_info: Option<serde_json::Value>,
    #[serde(default)]
    chan_info: Option<serde_json::Value>,
}

#[derive(Deserialize, Default)]
struct JoinJson {
    info: ClientInfoJson,
}

#[derive(Deserialize, Default)]
struct LeaveJson {
    info: ClientInfoJson,
}

#[derive(Deserialize, Default)]
struct UnsubscribePushJson {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    reason: String,
}

#[derive(Deserialize, Default)]
struct MessageJson {
    #[serde(default)]
    data: serde_json::Value,
}

#[derive(Deserialize, Default)]
struct SubscribePushJson {
    #[serde(default)]
    recoverable: bool,
    #[serde(default)]
    epoch: String,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    positioned: bool,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

#[derive(Deserialize, Default)]
struct ConnectPushJson {
    #[serde(default)]
    client: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    subs: HashMap<String, SubscribeResultJson>,
    #[serde(default)]
    expires: bool,
    #[serde(default)]
    ttl: u32,
    #[serde(default)]
    ping: u32,
    #[serde(default)]
    pong: bool,
    #[serde(default)]
    session: String,
    #[serde(default)]
    node: String,
}

#[derive(Deserialize, Default)]
struct DisconnectPushJson {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    reconnect: bool,
}

#[derive(Deserialize, Default, Clone)]
struct ConnectResultJson {
    #[serde(default)]
    client: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    expires: bool,
    #[serde(default)]
    ttl: u32,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    subs: HashMap<String, SubscribeResultJson>,
    #[serde(default)]
    ping: u32,
    #[serde(default)]
    pong: bool,
    #[serde(default)]
    session: String,
    #[serde(default)]
    node: String,
}

#[derive(Deserialize, Default, Clone)]
struct SubscribeResultJson {
    #[serde(default)]
    expires: bool,
    #[serde(default)]
    ttl: u32,
    #[serde(default)]
    recoverable: bool,
    #[serde(default)]
    epoch: String,
    #[serde(default)]
    publications: Vec<PublicationJson>,
    #[serde(default)]
    recovered: bool,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    positioned: bool,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    was_recovering: bool,
}

#[derive(Deserialize, Default)]
struct PresenceResultJson {
    #[serde(default)]
    presence: HashMap<String, ClientInfoJson>,
}

#[derive(Deserialize, Default)]
struct PresenceStatsResultJson {
    #[serde(default)]
    num_clients: u32,
    #[serde(default)]
    num_users: u32,
}

#[derive(Deserialize, Default)]
struct HistoryResultJson {
    #[serde(default)]
    publications: Vec<PublicationJson>,
    #[serde(default)]
    epoch: String,
    #[serde(default)]
    offset: u64,
}

#[derive(Deserialize, Default)]
struct RpcResultJson {
    #[serde(default)]
    data: serde_json::Value,
}

#[derive(Deserialize, Default)]
struct RefreshResultJson {
    #[serde(default)]
    client: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    expires: bool,
    #[serde(default)]
    ttl: u32,
}

#[derive(Deserialize, Default)]
struct SubRefreshResultJson {
    #[serde(default)]
    expires: bool,
    #[serde(default)]
    ttl: u32,
}

// ---------------------------------------------------------------------------
// Conversion: CommandJson <-> proto::Command
// ---------------------------------------------------------------------------

impl CommandJson {
    fn from_proto(cmd: &proto::Command) -> Self {
        let mut json = CommandJson {
            id: cmd.id,
            ..Default::default()
        };
        if let Some(req) = &cmd.connect {
            json.connect = Some(ConnectRequestJson {
                token: req.token.clone(),
                data: json_value_from_bytes(&req.data),
                subs: req
                    .subs
                    .iter()
                    .map(|(k, v)| (k.clone(), SubscribeRequestJson::from_proto_req(v)))
                    .collect(),
                name: req.name.clone(),
                version: req.version.clone(),
            });
        }
        if let Some(req) = &cmd.subscribe {
            json.subscribe = Some(SubscribeRequestJson::from_proto_req(req));
        }
        if let Some(req) = &cmd.unsubscribe {
            json.unsubscribe = Some(UnsubscribeRequestJson {
                channel: req.channel.clone(),
            });
        }
        if let Some(req) = &cmd.publish {
            json.publish = Some(PublishRequestJson {
                channel: req.channel.clone(),
                data: json_value_from_bytes(&req.data).unwrap_or(serde_json::Value::Null),
            });
        }
        if let Some(req) = &cmd.presence {
            json.presence = Some(PresenceRequestJson {
                channel: req.channel.clone(),
            });
        }
        if let Some(req) = &cmd.presence_stats {
            json.presence_stats = Some(PresenceStatsRequestJson {
                channel: req.channel.clone(),
            });
        }
        if let Some(req) = &cmd.history {
            json.history = Some(HistoryRequestJson {
                channel: req.channel.clone(),
                limit: req.limit,
                since: req.since.as_ref().map(|s| StreamPositionJson {
                    offset: s.offset,
                    epoch: s.epoch.clone(),
                }),
                reverse: req.reverse,
            });
        }
        if cmd.ping.is_some() {
            json.ping = Some(PingRequestJson {});
        }
        if let Some(req) = &cmd.send {
            json.send = Some(SendRequestJson {
                data: json_value_from_bytes(&req.data).unwrap_or(serde_json::Value::Null),
            });
        }
        if let Some(req) = &cmd.rpc {
            json.rpc = Some(RpcRequestJson {
                data: json_value_from_bytes(&req.data).unwrap_or(serde_json::Value::Null),
                method: req.method.clone(),
            });
        }
        if let Some(req) = &cmd.refresh {
            json.refresh = Some(RefreshRequestJson {
                token: req.token.clone(),
            });
        }
        if let Some(req) = &cmd.sub_refresh {
            json.sub_refresh = Some(SubRefreshRequestJson {
                channel: req.channel.clone(),
                token: req.token.clone(),
            });
        }
        json
    }
}

impl SubscribeRequestJson {
    fn from_proto_req(req: &proto::SubscribeRequest) -> Self {
        Self {
            channel: req.channel.clone(),
            token: req.token.clone(),
            recover: req.recover,
            epoch: req.epoch.clone(),
            offset: req.offset,
            data: json_value_from_bytes(&req.data),
            positioned: req.positioned,
            recoverable: req.recoverable,
            join_leave: req.join_leave,
        }
    }
}

impl ReplyJson {
    fn to_proto(&self) -> proto::Reply {
        let mut reply = proto::Reply {
            id: self.id,
            ..Default::default()
        };
        if let Some(err) = &self.error {
            reply.error = Some(proto::Error {
                code: err.code,
                message: err.message.clone(),
                temporary: err.temporary,
            });
        }
        if let Some(push) = &self.push {
            reply.push = Some(push.to_proto());
        }
        if let Some(c) = &self.connect {
            reply.connect = Some(c.to_proto());
        }
        if let Some(s) = &self.subscribe {
            reply.subscribe = Some(s.to_proto());
        }
        if self.unsubscribe.is_some() {
            reply.unsubscribe = Some(proto::UnsubscribeResult {});
        }
        if self.publish.is_some() {
            reply.publish = Some(proto::PublishResult {});
        }
        if let Some(p) = &self.presence {
            reply.presence = Some(p.to_proto());
        }
        if let Some(ps) = &self.presence_stats {
            reply.presence_stats = Some(proto::PresenceStatsResult {
                num_clients: ps.num_clients,
                num_users: ps.num_users,
            });
        }
        if let Some(h) = &self.history {
            reply.history = Some(h.to_proto());
        }
        if self.ping.is_some() {
            reply.ping = Some(proto::PingResult {});
        }
        if let Some(r) = &self.rpc {
            reply.rpc = Some(proto::RpcResult {
                data: json_value_to_bytes(&r.data),
            });
        }
        if let Some(r) = &self.refresh {
            reply.refresh = Some(proto::RefreshResult {
                client: r.client.clone(),
                version: r.version.clone(),
                expires: r.expires,
                ttl: r.ttl,
            });
        }
        if let Some(sr) = &self.sub_refresh {
            reply.sub_refresh = Some(proto::SubRefreshResult {
                expires: sr.expires,
                ttl: sr.ttl,
            });
        }
        reply
    }
}

impl PushJson {
    fn to_proto(&self) -> proto::Push {
        let mut push = proto::Push {
            channel: self.channel.clone(),
            ..Default::default()
        };
        if let Some(p) = &self.publication {
            push.r#pub = Some(p.to_proto());
        }
        if let Some(j) = &self.join {
            push.join = Some(proto::Join {
                info: Some(j.info.to_proto()),
            });
        }
        if let Some(l) = &self.leave {
            push.leave = Some(proto::Leave {
                info: Some(l.info.to_proto()),
            });
        }
        if let Some(u) = &self.unsubscribe {
            push.unsubscribe = Some(proto::Unsubscribe {
                code: u.code,
                reason: u.reason.clone(),
            });
        }
        if let Some(m) = &self.message {
            push.message = Some(proto::Message {
                data: json_value_to_bytes(&m.data),
            });
        }
        if let Some(s) = &self.subscribe {
            push.subscribe = Some(proto::Subscribe {
                recoverable: s.recoverable,
                epoch: s.epoch.clone(),
                offset: s.offset,
                positioned: s.positioned,
                data: s
                    .data
                    .as_ref()
                    .map(json_value_to_bytes)
                    .unwrap_or_default(),
            });
        }
        if let Some(c) = &self.connect {
            push.connect = Some(proto::Connect {
                client: c.client.clone(),
                version: c.version.clone(),
                data: c
                    .data
                    .as_ref()
                    .map(json_value_to_bytes)
                    .unwrap_or_default(),
                subs: c
                    .subs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_proto()))
                    .collect(),
                expires: c.expires,
                ttl: c.ttl,
                ping: c.ping,
                pong: c.pong,
                session: c.session.clone(),
                node: c.node.clone(),
                time: 0,
            });
        }
        if let Some(d) = &self.disconnect {
            push.disconnect = Some(proto::Disconnect {
                code: d.code,
                reason: d.reason.clone(),
                reconnect: d.reconnect,
            });
        }
        push
    }
}

impl ConnectResultJson {
    fn to_proto(&self) -> proto::ConnectResult {
        proto::ConnectResult {
            client: self.client.clone(),
            version: self.version.clone(),
            expires: self.expires,
            ttl: self.ttl,
            data: self
                .data
                .as_ref()
                .map(json_value_to_bytes)
                .unwrap_or_default(),
            subs: self
                .subs
                .iter()
                .map(|(k, v)| (k.clone(), v.to_proto()))
                .collect(),
            ping: self.ping,
            pong: self.pong,
            session: self.session.clone(),
            node: self.node.clone(),
            time: 0,
        }
    }
}

impl SubscribeResultJson {
    fn to_proto(&self) -> proto::SubscribeResult {
        proto::SubscribeResult {
            expires: self.expires,
            ttl: self.ttl,
            recoverable: self.recoverable,
            epoch: self.epoch.clone(),
            publications: self.publications.iter().map(|p| p.to_proto()).collect(),
            recovered: self.recovered,
            offset: self.offset,
            positioned: self.positioned,
            data: self
                .data
                .as_ref()
                .map(json_value_to_bytes)
                .unwrap_or_default(),
            was_recovering: self.was_recovering,
            delta: false,
        }
    }
}

impl PublicationJson {
    fn to_proto(&self) -> proto::Publication {
        proto::Publication {
            data: json_value_to_bytes(&self.data),
            info: self.info.as_ref().map(|i| i.to_proto()),
            offset: self.offset,
            tags: self.tags.clone(),
            delta: false,
            time: 0,
            channel: String::new(),
        }
    }
}

impl ClientInfoJson {
    fn to_proto(&self) -> proto::ClientInfo {
        proto::ClientInfo {
            user: self.user.clone(),
            client: self.client.clone(),
            conn_info: self
                .conn_info
                .as_ref()
                .map(json_value_to_bytes)
                .unwrap_or_default(),
            chan_info: self
                .chan_info
                .as_ref()
                .map(json_value_to_bytes)
                .unwrap_or_default(),
        }
    }
}

impl PresenceResultJson {
    fn to_proto(&self) -> proto::PresenceResult {
        proto::PresenceResult {
            presence: self
                .presence
                .iter()
                .map(|(k, v)| (k.clone(), v.to_proto()))
                .collect(),
        }
    }
}

impl HistoryResultJson {
    fn to_proto(&self) -> proto::HistoryResult {
        proto::HistoryResult {
            publications: self.publications.iter().map(|p| p.to_proto()).collect(),
            epoch: self.epoch.clone(),
            offset: self.offset,
        }
    }
}

/// Convert bytes to a JSON value (for JSON protocol, data fields are embedded JSON).
fn json_value_from_bytes(data: &[u8]) -> Option<serde_json::Value> {
    if data.is_empty() {
        return None;
    }
    serde_json::from_slice(data).ok()
}

/// Convert a JSON value back to bytes.
fn json_value_to_bytes(value: &serde_json::Value) -> Vec<u8> {
    if value.is_null() {
        return Vec::new();
    }
    serde_json::to_vec(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_encode_single_connect_command() {
        let codec = JsonCodec;
        let cmd = proto::Command {
            id: 1,
            connect: Some(proto::ConnectRequest {
                token: "test-token".into(),
                name: "rs".into(),
                version: "0.1.0".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let encoded = codec.encode_commands(&[cmd]).unwrap();
        let text = std::str::from_utf8(&encoded).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["connect"]["token"], "test-token");
        assert_eq!(parsed["connect"]["name"], "rs");
    }

    #[test]
    fn test_json_encode_batch_commands() {
        let codec = JsonCodec;
        let cmds = vec![
            proto::Command {
                id: 1,
                subscribe: Some(proto::SubscribeRequest {
                    channel: "ch1".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            proto::Command {
                id: 2,
                subscribe: Some(proto::SubscribeRequest {
                    channel: "ch2".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];
        let encoded = codec.encode_commands(&cmds).unwrap();
        let text = std::str::from_utf8(&encoded).unwrap();
        let lines: Vec<&str> = text.split('\n').collect();
        assert_eq!(lines.len(), 2);
        let p1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let p2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(p1["subscribe"]["channel"], "ch1");
        assert_eq!(p2["subscribe"]["channel"], "ch2");
    }

    #[test]
    fn test_json_decode_reply() {
        let codec = JsonCodec;
        let data = br#"{"id":1,"connect":{"client":"abc","version":"1.0","ping":25,"pong":true}}"#;
        let replies = codec.decode_replies(data).unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].id, 1);
        let connect = replies[0].connect.as_ref().unwrap();
        assert_eq!(connect.client, "abc");
        assert_eq!(connect.ping, 25);
        assert!(connect.pong);
    }

    #[test]
    fn test_json_decode_batched_replies() {
        let codec = JsonCodec;
        let data = b"{\"id\":1,\"subscribe\":{}}\n{\"id\":2,\"subscribe\":{}}";
        let replies = codec.decode_replies(data).unwrap();
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].id, 1);
        assert_eq!(replies[1].id, 2);
    }

    #[test]
    fn test_json_decode_push_publication() {
        let codec = JsonCodec;
        let data = br#"{"push":{"channel":"test","pub":{"data":{"msg":"hello"},"offset":42}}}"#;
        let replies = codec.decode_replies(data).unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].id, 0);
        let push = replies[0].push.as_ref().unwrap();
        assert_eq!(push.channel, "test");
        let pub_msg = push.r#pub.as_ref().unwrap();
        assert_eq!(pub_msg.offset, 42);
        let data_str = std::str::from_utf8(&pub_msg.data).unwrap();
        assert!(data_str.contains("hello"));
    }

    #[test]
    fn test_json_decode_empty_reply_ping() {
        let codec = JsonCodec;
        let data = b"{}";
        let replies = codec.decode_replies(data).unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].id, 0);
        assert!(replies[0].push.is_none());
    }

    #[test]
    fn test_json_decode_error_reply() {
        let codec = JsonCodec;
        let data = br#"{"id":1,"error":{"code":100,"message":"internal error","temporary":true}}"#;
        let replies = codec.decode_replies(data).unwrap();
        let err = replies[0].error.as_ref().unwrap();
        assert_eq!(err.code, 100);
        assert!(err.temporary);
    }

    #[test]
    fn test_json_skips_empty_lines() {
        let codec = JsonCodec;
        let data = b"{\"id\":1,\"subscribe\":{}}\n\n{\"id\":2,\"subscribe\":{}}\n";
        let replies = codec.decode_replies(data).unwrap();
        assert_eq!(replies.len(), 2);
    }

    #[test]
    fn test_protobuf_roundtrip_single_command() {
        let codec = ProtobufCodec;
        let cmd = proto::Command {
            id: 1,
            connect: Some(proto::ConnectRequest {
                token: "test-token".into(),
                name: "rs".into(),
                version: "0.1.0".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let encoded = codec.encode_commands(&[cmd.clone()]).unwrap();
        // Decode as replies won't work since Command != Reply, but we can verify encoding
        // by manually decoding
        let (len, bytes_read) = decode_varint(&encoded).unwrap();
        assert_eq!(bytes_read + len, encoded.len());
        let decoded = proto::Command::decode(&encoded[bytes_read..bytes_read + len]).unwrap();
        assert_eq!(decoded.id, 1);
        assert_eq!(
            decoded.connect.as_ref().unwrap().token,
            cmd.connect.as_ref().unwrap().token
        );
    }

    #[test]
    fn test_protobuf_roundtrip_reply() {
        let codec = ProtobufCodec;
        // Create a reply, encode it manually, then decode via codec
        let reply = proto::Reply {
            id: 1,
            connect: Some(proto::ConnectResult {
                client: "abc".into(),
                version: "1.0".into(),
                ping: 25,
                pong: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let reply_bytes = reply.encode_to_vec();
        let mut frame = Vec::new();
        prost::encoding::encode_varint(reply_bytes.len() as u64, &mut frame);
        frame.extend_from_slice(&reply_bytes);

        let decoded = codec.decode_replies(&frame).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].id, 1);
        assert_eq!(decoded[0].connect.as_ref().unwrap().client, "abc");
    }

    #[test]
    fn test_protobuf_batch_replies() {
        let codec = ProtobufCodec;
        let replies = vec![
            proto::Reply {
                id: 1,
                subscribe: Some(proto::SubscribeResult::default()),
                ..Default::default()
            },
            proto::Reply {
                id: 2,
                subscribe: Some(proto::SubscribeResult::default()),
                ..Default::default()
            },
        ];
        let mut frame = Vec::new();
        for r in &replies {
            let bytes = r.encode_to_vec();
            prost::encoding::encode_varint(bytes.len() as u64, &mut frame);
            frame.extend_from_slice(&bytes);
        }

        let decoded = codec.decode_replies(&frame).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].id, 1);
        assert_eq!(decoded[1].id, 2);
    }

    #[test]
    fn test_protobuf_empty_data() {
        let codec = ProtobufCodec;
        let decoded = codec.decode_replies(&[]).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_varint_decode() {
        // 300 encoded as varint: 0xAC 0x02
        let (val, len) = decode_varint(&[0xAC, 0x02]).unwrap();
        assert_eq!(val, 300);
        assert_eq!(len, 2);

        // 1 encoded as varint: 0x01
        let (val, len) = decode_varint(&[0x01]).unwrap();
        assert_eq!(val, 1);
        assert_eq!(len, 1);
    }
}
