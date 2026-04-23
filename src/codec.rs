use bytes::Buf;
use prost::Message;

use crate::config::ProtocolType;
use crate::errors::CentrifugeError;
use crate::protocol::proto;

/// Trait for encoding commands and decoding replies.
pub trait Codec: Send + Sync {
    fn encode_commands(&self, commands: &[proto::Command]) -> Result<Vec<u8>, CentrifugeError>;
    fn decode_replies(&self, data: &[u8]) -> Result<Vec<proto::Reply>, CentrifugeError>;
}

/// Custom serde module for `bytes` fields that should be serialized as
/// embedded JSON objects (not base64 strings). This is required because the
/// Centrifuge JSON protocol treats `data` fields as raw JSON payloads.
pub mod embedded_json {
    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(data: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if data.is_empty() {
            serializer.serialize_none()
        } else {
            let value: serde_json::Value = serde_json::from_slice(data).map_err(serde::ser::Error::custom)?;
            value.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_null() {
            Ok(Vec::new())
        } else {
            serde_json::to_vec(&value).map_err(serde::de::Error::custom)
        }
    }
}

/// Helper for skip_serializing_if on u32 fields.
pub fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

/// JSON codec: newline-delimited JSON encoding/decoding.
///
/// Uses serde derives on prost-generated types directly. The `embedded_json`
/// module handles the Centrifuge protocol's non-standard treatment of `bytes`
/// fields as embedded JSON objects.
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode_commands(&self, commands: &[proto::Command]) -> Result<Vec<u8>, CentrifugeError> {
        let mut parts = Vec::with_capacity(commands.len());
        for cmd in commands {
            let json =
                serde_json::to_string(cmd).map_err(|e| CentrifugeError::Protocol(format!("json encode: {e}")))?;
            parts.push(json);
        }
        Ok(parts.join("\n").into_bytes())
    }

    fn decode_replies(&self, data: &[u8]) -> Result<Vec<proto::Reply>, CentrifugeError> {
        let text = std::str::from_utf8(data).map_err(|e| CentrifugeError::Protocol(e.to_string()))?;
        let mut replies = Vec::new();
        for line in text.split('\n') {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let reply: proto::Reply =
                serde_json::from_str(line).map_err(|e| CentrifugeError::Protocol(format!("json decode: {e}")))?;
            replies.push(reply);
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
        let mut buf = data;
        while buf.has_remaining() {
            let len = prost::encoding::decode_varint(&mut buf)
                .map_err(|e| CentrifugeError::Protocol(format!("varint decode: {e}")))?;
            let len = usize::try_from(len).map_err(|_| CentrifugeError::Protocol("frame length overflow".into()))?;
            if len > buf.remaining() {
                return Err(CentrifugeError::Protocol("protobuf frame exceeds data length".into()));
            }
            let reply = proto::Reply::decode(&buf[..len])
                .map_err(|e| CentrifugeError::Protocol(format!("protobuf decode: {e}")))?;
            replies.push(reply);
            buf.advance(len);
        }
        Ok(replies)
    }
}

/// Creates a codec for the given protocol type.
pub(crate) fn new_codec(protocol_type: ProtocolType) -> Box<dyn Codec> {
    match protocol_type {
        ProtocolType::Json => Box::new(JsonCodec),
        ProtocolType::Protobuf => Box::new(ProtobufCodec),
    }
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
    fn test_json_embedded_data_encode() {
        let cmd = proto::Command {
            id: 1,
            publish: Some(proto::PublishRequest {
                channel: "ch".into(),
                data: br#"{"msg":"hello"}"#.to_vec(),
            }),
            ..Default::default()
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        // data must be embedded JSON, not base64
        assert_eq!(parsed["publish"]["data"]["msg"], "hello");
    }

    // Empty byte fields must be omitted from the JSON output — matches
    // proto3 JSON conventions and the external Go encoder used by the Go SDK
    // (protocol.NewJSONCommandEncoder). Previously emitted `"data": null`,
    // which is not what Centrifugo's other client SDKs produce for an empty
    // payload.
    #[test]
    fn test_json_empty_data_field_is_omitted() {
        let cmd = proto::Command {
            id: 1,
            publish: Some(proto::PublishRequest {
                channel: "ch".into(),
                data: Vec::new(),
            }),
            ..Default::default()
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            parsed["publish"].get("data").is_none(),
            "empty PublishRequest.data must be omitted from JSON, got: {json}"
        );
        assert_eq!(parsed["publish"]["channel"], "ch");
    }

    #[test]
    fn test_json_empty_connect_data_is_omitted() {
        let cmd = proto::Command {
            id: 1,
            connect: Some(proto::ConnectRequest {
                token: "t".into(),
                name: "rs".into(),
                data: Vec::new(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            parsed["connect"].get("data").is_none(),
            "empty ConnectRequest.data must be omitted from JSON, got: {json}"
        );
    }

    #[test]
    fn test_json_embedded_data_decode() {
        let json = r#"{"push":{"channel":"ch","pub":{"data":{"msg":"world"}}}}"#;
        let reply: proto::Reply = serde_json::from_str(json).unwrap();
        let data = &reply.push.unwrap().r#pub.unwrap().data;
        let parsed: serde_json::Value = serde_json::from_slice(data).unwrap();
        assert_eq!(parsed["msg"], "world");
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
        let encoded = codec.encode_commands(std::slice::from_ref(&cmd)).unwrap();
        let mut buf = &encoded[..];
        let len = prost::encoding::decode_varint(&mut buf).unwrap() as usize;
        assert_eq!(buf.len(), len);
        let decoded = proto::Command::decode(&buf[..len]).unwrap();
        assert_eq!(decoded.id, 1);
        assert_eq!(
            decoded.connect.as_ref().unwrap().token,
            cmd.connect.as_ref().unwrap().token
        );
    }

    #[test]
    fn test_protobuf_roundtrip_reply() {
        let codec = ProtobufCodec;
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
    }

    #[test]
    fn test_protobuf_empty_data() {
        let codec = ProtobufCodec;
        assert!(codec.decode_replies(&[]).unwrap().is_empty());
    }

    #[test]
    fn test_varint_decode() {
        let mut buf: &[u8] = &[0xAC, 0x02];
        let val = prost::encoding::decode_varint(&mut buf).unwrap();
        assert_eq!(val, 300);
        assert_eq!(buf.len(), 0); // fully consumed

        let mut buf: &[u8] = &[0x01];
        let val = prost::encoding::decode_varint(&mut buf).unwrap();
        assert_eq!(val, 1);
        assert_eq!(buf.len(), 0);
    }

    // ---------------------------------------------------------------
    // Protobuf codec error handling
    // ---------------------------------------------------------------

    #[test]
    fn test_protobuf_truncated_varint() {
        let codec = ProtobufCodec;
        // 0x80 means "more bytes follow" but there are none
        let result = codec.decode_replies(&[0x80]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("varint"), "error should mention varint: {msg}");
    }

    #[test]
    fn test_protobuf_varint_too_long() {
        let codec = ProtobufCodec;
        // 10 bytes of continuation (shift would exceed 64 bits)
        let data = [0x80; 10];
        let result = codec.decode_replies(&data);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("varint"), "error should mention varint: {msg}");
    }

    #[test]
    fn test_protobuf_frame_exceeds_data_length() {
        let codec = ProtobufCodec;
        // Varint says 100 bytes follow, but only 2 bytes of data available
        let data = [100, 0x0A, 0x0B];
        let result = codec.decode_replies(&data);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("exceeds"), "error should mention exceeds: {msg}");
    }

    #[test]
    fn test_protobuf_garbage_data_in_valid_frame() {
        let codec = ProtobufCodec;
        // Varint says 5 bytes follow, then 5 bytes of garbage
        let data = [5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let result = codec.decode_replies(&data);
        // prost should fail to decode this as a Reply
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("protobuf decode"),
            "error should mention protobuf decode: {msg}"
        );
    }

    #[test]
    fn test_protobuf_partial_frame_after_valid() {
        let codec = ProtobufCodec;
        // First: a valid empty Reply (varint=0, no bytes)
        // Second: truncated frame (varint says 50, only 1 byte follows)
        let data = [0, 50, 0x0A];
        let result = codec.decode_replies(&data);
        assert!(result.is_err(), "should fail on the truncated second frame");
    }

    #[test]
    fn test_protobuf_zero_length_frame() {
        let codec = ProtobufCodec;
        // Varint = 0 means a zero-byte protobuf message (valid empty Reply)
        let result = codec.decode_replies(&[0]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }
}
