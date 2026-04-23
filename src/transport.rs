use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use tracing::warn;

use crate::codes;
use crate::config::ProtocolType;

// ---------------------------------------------------------------------------
// Transport trait abstraction
// ---------------------------------------------------------------------------

/// A boxed, `Send`-able future with a lifetime — shorthand for trait return types.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A transport error.
pub type TransportError = Box<dyn std::error::Error + Send + Sync>;

/// A frame received from the transport.
pub enum TransportFrame {
    Data(Vec<u8>),
    Close(Option<DisconnectInfo>),
}

/// Write half of a transport connection.
pub trait TransportSink: Send {
    fn send_data(&mut self, data: Vec<u8>) -> BoxFuture<'_, Result<(), TransportError>>;
    fn close(&mut self) -> BoxFuture<'_, Result<(), TransportError>>;
}

/// A connected transport: sink + stream.
pub struct TransportConn {
    pub sink: Box<dyn TransportSink>,
    pub stream: Pin<Box<dyn Stream<Item = TransportFrame> + Send>>,
}

/// Factory for creating transport connections. Must support multiple `connect()`
/// calls (the actor reconnects).
pub trait Transport: Send + Sync {
    fn connect(&self) -> BoxFuture<'_, Result<TransportConn, TransportError>>;
}

// ---------------------------------------------------------------------------
// Disconnect info
// ---------------------------------------------------------------------------

/// Parsed disconnect info.
#[derive(Debug, Clone)]
pub struct DisconnectInfo {
    pub code: u32,
    pub reason: String,
    pub reconnect: bool,
}

fn map_close_code(code: u16, reason: String) -> DisconnectInfo {
    match code {
        1000 | 1001 => DisconnectInfo {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason,
            reconnect: true,
        },
        1009 => DisconnectInfo {
            code: codes::disconnect::MESSAGE_SIZE_LIMIT,
            reason: "message size limit".into(),
            reconnect: false,
        },
        c if c >= 3000 => DisconnectInfo {
            code: u32::from(c),
            reason,
            reconnect: codes::should_reconnect_on_disconnect(u32::from(c)),
        },
        _ => DisconnectInfo {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason: format!("transport closed with code {code}"),
            reconnect: true,
        },
    }
}

fn parse_close(code: u16, reason: &str) -> DisconnectInfo {
    // Try JSON disconnect advice first
    if let Ok(advice) = serde_json::from_str::<CloseAdvice>(reason) {
        return DisconnectInfo {
            code: advice.code,
            reason: advice.reason,
            reconnect: advice.reconnect,
        };
    }
    map_close_code(code, reason.to_string())
}

#[derive(serde::Deserialize)]
struct CloseAdvice {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    reconnect: bool,
}

// ---------------------------------------------------------------------------
// WebSocket transport (tokio-tungstenite)
// ---------------------------------------------------------------------------

/// WebSocket transport for Centrifuge using tokio-tungstenite.
pub struct WsTransport {
    url: String,
    protocol_type: ProtocolType,
    headers: HashMap<String, String>,
}

impl WsTransport {
    pub fn new(url: String, protocol_type: ProtocolType, headers: HashMap<String, String>) -> Self {
        Self {
            url,
            protocol_type,
            headers,
        }
    }
}

impl Transport for WsTransport {
    fn connect(&self) -> BoxFuture<'_, Result<TransportConn, TransportError>> {
        Box::pin(async move {
            let is_binary = self.protocol_type == ProtocolType::Protobuf;

            let mut request = self
                .url
                .as_str()
                .into_client_request()
                .map_err(|e| Box::new(e) as TransportError)?;

            if self.protocol_type == ProtocolType::Protobuf {
                request
                    .headers_mut()
                    .insert("Sec-WebSocket-Protocol", "centrifuge-protobuf".parse().unwrap());
            }

            for (key, value) in &self.headers {
                match (key.parse::<HeaderName>(), value.parse::<HeaderValue>()) {
                    (Ok(name), Ok(val)) => {
                        request.headers_mut().insert(name, val);
                    }
                    _ => {
                        warn!(key = %key, "skipping invalid header");
                    }
                }
            }

            let (ws_stream, _response) = tokio_tungstenite::connect_async(request)
                .await
                .map_err(|e| Box::new(e) as TransportError)?;

            let (frame_tx, frame_rx) = mpsc::channel::<TransportFrame>(256);
            let (write_tx, write_rx) = mpsc::channel::<Vec<u8>>(256);

            spawn_ws_task(ws_stream, frame_tx, write_rx, is_binary);

            Ok(TransportConn {
                sink: Box::new(ChannelSink { tx: Some(write_tx) }),
                stream: Box::pin(ChannelStream { rx: frame_rx }),
            })
        })
    }
}

/// Spawn a single task that bridges between a tungstenite WebSocketStream
/// and our channel-based TransportConn interface.
fn spawn_ws_task<S>(
    ws_stream: tokio_tungstenite::WebSocketStream<S>,
    frame_tx: mpsc::Sender<TransportFrame>,
    mut write_rx: mpsc::Receiver<Vec<u8>>,
    is_binary: bool,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let (mut sink, mut stream) = ws_stream.split();

        // Spawn a dedicated writer task so reads and writes proceed independently.
        let write_task = tokio::spawn(async move {
            while let Some(data) = write_rx.recv().await {
                let msg = if is_binary {
                    Message::Binary(data.into())
                } else {
                    // serde_json always emits valid UTF-8, so from_utf8 can't
                    // fail here. _lossy would silently replace bytes with
                    // U+FFFD on a codec bug, corrupting the frame.
                    let text = String::from_utf8(data).expect("json codec must emit valid UTF-8");
                    Message::Text(text.into())
                };
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        // Reader loop in the current task.
        while let Some(result) = stream.next().await {
            match result {
                Ok(Message::Text(t)) => {
                    if frame_tx
                        .send(TransportFrame::Data(t.as_bytes().to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(Message::Binary(b)) => {
                    if frame_tx.send(TransportFrame::Data(b.to_vec())).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(reason)) => {
                    let info = reason.map(|r| parse_close(u16::from(r.code), &r.reason));
                    let _ = frame_tx.send(TransportFrame::Close(info)).await;
                    break;
                }
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => {}
                Err(_) => {
                    let _ = frame_tx.send(TransportFrame::Close(None)).await;
                    break;
                }
            }
        }

        write_task.abort();
    });
}

struct ChannelSink {
    tx: Option<mpsc::Sender<Vec<u8>>>,
}

impl TransportSink for ChannelSink {
    fn send_data(&mut self, data: Vec<u8>) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            match self.tx {
                Some(ref tx) => tx.send(data).await.map_err(|e| Box::new(e) as TransportError),
                None => Err("transport closed".into()),
            }
        })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), TransportError>> {
        self.tx.take();
        Box::pin(async { Ok(()) })
    }
}

struct ChannelStream {
    rx: mpsc::Receiver<TransportFrame>,
}

impl Stream for ChannelStream {
    type Item = TransportFrame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_close_with_json_advice() {
        let info = parse_close(1000, r#"{"code":3001,"reason":"maintenance","reconnect":true}"#);
        assert_eq!(info.code, 3001);
        assert_eq!(info.reason, "maintenance");
        assert!(info.reconnect);
    }

    #[test]
    fn test_parse_close_terminal_code() {
        let info = parse_close(3500, "banned");
        assert_eq!(info.code, 3500);
        assert!(!info.reconnect);
    }

    #[test]
    fn test_parse_close_reconnectable_code() {
        let info = parse_close(3001, "restart");
        assert!(info.reconnect);
    }

    #[test]
    fn test_parse_close_message_too_big() {
        let info = parse_close(1009, "");
        assert_eq!(info.code, codes::disconnect::MESSAGE_SIZE_LIMIT);
        assert!(!info.reconnect);
    }

    #[test]
    fn test_parse_close_default() {
        let info = parse_close(1006, "");
        assert_eq!(info.code, codes::connecting::TRANSPORT_CLOSED);
        assert!(info.reconnect);
    }
}
