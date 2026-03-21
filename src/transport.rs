use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, Stream, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::codes;
use crate::config::ProtocolType;

// ---------------------------------------------------------------------------
// Transport trait abstraction
// ---------------------------------------------------------------------------

/// A frame received from the transport.
pub enum TransportFrame {
    Data(Vec<u8>),
    Close(Option<DisconnectInfo>),
}

/// Write half of a transport connection.
pub trait TransportSink: Send {
    fn send_data(
        &mut self,
        data: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>>;

    fn close(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>>;
}

/// A connected transport: sink + stream.
pub struct TransportConn {
    pub sink: Box<dyn TransportSink>,
    pub stream: Pin<Box<dyn Stream<Item = TransportFrame> + Send>>,
}

/// Factory for creating transport connections. Must support multiple `connect()`
/// calls (the actor reconnects).
pub trait Transport: Send + Sync {
    fn connect(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<TransportConn, String>> + Send + '_>>;
}

// ---------------------------------------------------------------------------
// WebSocket transport implementation
// ---------------------------------------------------------------------------

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

/// WebSocket transport for Centrifuge.
pub struct WsTransport {
    url: String,
    protocol_type: ProtocolType,
}

impl WsTransport {
    pub fn new(url: String, protocol_type: ProtocolType) -> Self {
        Self { url, protocol_type }
    }
}

impl Transport for WsTransport {
    fn connect(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<TransportConn, String>> + Send + '_>>
    {
        Box::pin(async move {
            let mut request = self
                .url
                .as_str()
                .into_client_request()
                .map_err(|e| format!("invalid URL: {e}"))?;

            if self.protocol_type == ProtocolType::Protobuf {
                request.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_static("centrifuge-protobuf"),
                );
            }

            let (ws_stream, _response) = tokio_tungstenite::connect_async(request)
                .await
                .map_err(|e| format!("websocket connect: {e}"))?;

            let (writer, reader) = ws_stream.split();

            let sink = Box::new(WsSink {
                writer,
                protocol_type: self.protocol_type,
            });
            let stream: Pin<Box<dyn Stream<Item = TransportFrame> + Send>> =
                Box::pin(WsStreamAdapter { reader });

            Ok(TransportConn { sink, stream })
        })
    }
}

/// WebSocket sink wrapper.
struct WsSink {
    writer: WsWriter,
    protocol_type: ProtocolType,
}

impl TransportSink for WsSink {
    fn send_data(
        &mut self,
        data: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            let msg = match self.protocol_type {
                ProtocolType::Json => {
                    let text = String::from_utf8(data).map_err(|e| e.to_string())?;
                    Message::Text(text.into())
                }
                ProtocolType::Protobuf => Message::Binary(data.into()),
            };
            self.writer
                .send(msg)
                .await
                .map_err(|e| format!("ws send: {e}"))
        })
    }

    fn close(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            self.writer
                .close()
                .await
                .map_err(|e| format!("ws close: {e}"))
        })
    }
}

/// WebSocket reader adapter that converts `Message` to `TransportFrame`.
struct WsStreamAdapter {
    reader: WsReader,
}

impl Stream for WsStreamAdapter {
    type Item = TransportFrame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.reader).poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => match msg {
                Message::Text(text) => {
                    Poll::Ready(Some(TransportFrame::Data(text.as_bytes().to_vec())))
                }
                Message::Binary(bin) => Poll::Ready(Some(TransportFrame::Data(bin.to_vec()))),
                Message::Close(frame) => {
                    let info = parse_close_frame(frame);
                    Poll::Ready(Some(TransportFrame::Close(Some(info))))
                }
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                    // Skip control frames, poll again
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            },
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(TransportFrame::Close(None))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// Disconnect info and close frame parsing (unchanged)
// ---------------------------------------------------------------------------

/// Parsed disconnect info extracted from a WebSocket close frame.
#[derive(Debug, Clone)]
pub struct DisconnectInfo {
    pub code: u32,
    pub reason: String,
    pub reconnect: bool,
}

/// Extracts disconnect information from a WebSocket close frame.
pub fn parse_close_frame(frame: Option<CloseFrame>) -> DisconnectInfo {
    match frame {
        Some(close) => {
            let ws_code = close.code.into();
            if let Ok(advice) = serde_json::from_str::<CloseAdvice>(&close.reason) {
                return DisconnectInfo {
                    code: advice.code,
                    reason: advice.reason,
                    reconnect: advice.reconnect,
                };
            }
            map_ws_close_code(ws_code, close.reason.to_string())
        }
        None => DisconnectInfo {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason: "transport closed".into(),
            reconnect: true,
        },
    }
}

fn map_ws_close_code(ws_code: u16, reason: String) -> DisconnectInfo {
    match ws_code {
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
        code if code >= 3000 => {
            let reconnect = codes::should_reconnect_on_disconnect(code as u32);
            DisconnectInfo {
                code: code as u32,
                reason,
                reconnect,
            }
        }
        _ => DisconnectInfo {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason: format!("transport closed with code {ws_code}"),
            reconnect: true,
        },
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    #[test]
    fn test_parse_close_frame_with_json_advice() {
        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: r#"{"code":3001,"reason":"maintenance","reconnect":true}"#.into(),
        };
        let info = parse_close_frame(Some(frame));
        assert_eq!(info.code, 3001);
        assert_eq!(info.reason, "maintenance");
        assert!(info.reconnect);
    }

    #[test]
    fn test_parse_close_frame_terminal_code() {
        let frame = CloseFrame {
            code: CloseCode::from(3500),
            reason: "banned".into(),
        };
        let info = parse_close_frame(Some(frame));
        assert_eq!(info.code, 3500);
        assert!(!info.reconnect);
    }

    #[test]
    fn test_parse_close_frame_reconnectable_code() {
        let frame = CloseFrame {
            code: CloseCode::from(3001),
            reason: "restart".into(),
        };
        let info = parse_close_frame(Some(frame));
        assert!(info.reconnect);
    }

    #[test]
    fn test_parse_close_frame_message_too_big() {
        let frame = CloseFrame {
            code: CloseCode::Size,
            reason: "".into(),
        };
        let info = parse_close_frame(Some(frame));
        assert_eq!(info.code, codes::disconnect::MESSAGE_SIZE_LIMIT);
        assert!(!info.reconnect);
    }

    #[test]
    fn test_parse_close_frame_none() {
        let info = parse_close_frame(None);
        assert_eq!(info.code, codes::connecting::TRANSPORT_CLOSED);
        assert!(info.reconnect);
    }
}
