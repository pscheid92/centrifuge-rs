//! Shared mock transport and test helpers for actor-level tests.
#![allow(unused)]

use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;
use tokio_stream::Stream;

use centrifuge_client::Client;
use centrifuge_client::config::ClientConfig;
use centrifuge_client::transport::{
    BoxFuture, Transport, TransportConn, TransportError, TransportFrame, TransportSink,
};

// ---------------------------------------------------------------------------
// Mock transport
// ---------------------------------------------------------------------------

pub struct MockConnection {
    pub incoming_tx: mpsc::Sender<TransportFrame>,
    pub outgoing_rx: mpsc::Receiver<Vec<u8>>,
}

#[allow(clippy::type_complexity)]
pub struct MockTransport {
    pub connections: Mutex<Vec<(mpsc::Receiver<TransportFrame>, mpsc::Sender<Vec<u8>>)>>,
    pub connect_errors: Mutex<Vec<String>>,
    pub connect_count: AtomicU32,
}

impl MockTransport {
    pub fn new() -> (Arc<Self>, MockConnection) {
        let (in_tx, in_rx) = mpsc::channel(256);
        let (out_tx, out_rx) = mpsc::channel(256);
        let transport = Arc::new(Self {
            connections: Mutex::new(vec![(in_rx, out_tx)]),
            connect_errors: Mutex::new(vec![]),
            connect_count: AtomicU32::new(0),
        });
        (
            transport,
            MockConnection {
                incoming_tx: in_tx,
                outgoing_rx: out_rx,
            },
        )
    }

    pub fn add_connection(&self) -> MockConnection {
        let (in_tx, in_rx) = mpsc::channel(256);
        let (out_tx, out_rx) = mpsc::channel(256);
        self.connections.lock().unwrap().push((in_rx, out_tx));
        MockConnection {
            incoming_tx: in_tx,
            outgoing_rx: out_rx,
        }
    }

    pub fn connect_count(&self) -> u32 {
        self.connect_count.load(Ordering::Relaxed)
    }
}

impl Transport for MockTransport {
    fn connect(&self) -> BoxFuture<'_, Result<TransportConn, TransportError>> {
        self.connect_count.fetch_add(1, Ordering::Relaxed);
        {
            let mut errors = self.connect_errors.lock().unwrap();
            if !errors.is_empty() {
                let err = errors.remove(0);
                return Box::pin(async move { Err(err.into()) });
            }
        }
        let pair = {
            let mut conns = self.connections.lock().unwrap();
            if conns.is_empty() {
                return Box::pin(async { Err("no more mock connections".into()) });
            }
            conns.remove(0)
        };
        Box::pin(async move {
            let (rx, tx) = pair;
            Ok(TransportConn {
                sink: Box::new(MockSink { tx }),
                stream: Box::pin(MockStream { rx }),
            })
        })
    }
}

struct MockSink {
    tx: mpsc::Sender<Vec<u8>>,
}
impl TransportSink for MockSink {
    fn send_data(&mut self, data: Vec<u8>) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move { self.tx.send(data).await.map_err(|e| Box::new(e) as TransportError) })
    }
    fn close(&mut self) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async { Ok(()) })
    }
}

struct MockStream {
    rx: mpsc::Receiver<TransportFrame>,
}
impl Stream for MockStream {
    type Item = TransportFrame;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

pub struct ArcTransport(pub Arc<MockTransport>);
impl Transport for ArcTransport {
    fn connect(&self) -> BoxFuture<'_, Result<TransportConn, TransportError>> {
        self.0.connect()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn default_config() -> ClientConfig {
    ClientConfig {
        timeout: Duration::from_secs(2),
        min_reconnect_delay: Duration::from_millis(10),
        max_reconnect_delay: Duration::from_millis(50),
        ..ClientConfig::new("ws://test")
    }
}

pub fn encode_reply(reply: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(reply).unwrap()
}

pub async fn read_command(conn: &mut MockConnection) -> serde_json::Value {
    let data = time::timeout(Duration::from_secs(2), conn.outgoing_rx.recv())
        .await
        .expect("timeout waiting for command")
        .expect("channel closed");
    serde_json::from_slice(&data).unwrap()
}

/// Perform a full connect handshake: read the connect command and feed a result.
/// Returns the command ID.
pub async fn do_connect(conn: &mut MockConnection) -> u32 {
    let cmd = read_command(conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert!(cmd.get("connect").is_some(), "expected connect command, got: {cmd}");
    let reply = serde_json::json!({
        "id": id,
        "connect": {"client": "test-client-id", "version": "1.0.0", "ping": 25, "pong": true}
    });
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    id
}

/// Helper: connect the client (spawning the connect in a task so it doesn't block).
pub async fn connect_client(client: &Client, conn: &mut MockConnection) {
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(conn).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;
}

/// Subscribe a subscription and complete the handshake.
pub async fn subscribe_sub(sub: &centrifuge_client::Subscription, conn: &mut MockConnection) {
    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert!(cmd.get("subscribe").is_some());
    let reply = serde_json::json!({"id": id, "subscribe": {}});
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;
}

pub fn make_client(config: ClientConfig) -> (Client, MockConnection, Arc<MockTransport>) {
    let (transport, conn) = MockTransport::new();
    let t = transport.clone();
    let client = Client::new_with_transport(config, Box::new(ArcTransport(transport)));
    (client, conn, t)
}
