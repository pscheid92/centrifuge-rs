/// Comprehensive tests for the centrifuge client using a mock transport.
///
/// Tests are organized by category:
/// A. Connection Lifecycle
/// B. Transport Reconnection
/// C. Server Ping/Pong
/// D. Request/Reply Commands
/// E. Client-Side Subscriptions
/// F. Server-Side Subscriptions
/// G. Push Message Handling
/// H. Edge Cases
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::Stream;
use tokio::sync::mpsc;
use tokio::time;

use centrifuge::config::{ClientConfig, SubscriptionConfig};
use centrifuge::events::{ClientEventHandlers, SubscriptionEventHandlers};
use centrifuge::transport::{DisconnectInfo, Transport, TransportConn, TransportFrame, TransportSink};
use centrifuge::{Client, CentrifugeError};

// ---------------------------------------------------------------------------
// Mock transport
// ---------------------------------------------------------------------------

struct MockConnection {
    incoming_tx: mpsc::Sender<TransportFrame>,
    outgoing_rx: mpsc::Receiver<Vec<u8>>,
}

struct MockTransport {
    connections: Mutex<Vec<(mpsc::Receiver<TransportFrame>, mpsc::Sender<Vec<u8>>)>>,
    connect_errors: Mutex<Vec<String>>,
    connect_count: AtomicU32,
}

impl MockTransport {
    fn new() -> (Arc<Self>, MockConnection) {
        let (in_tx, in_rx) = mpsc::channel(256);
        let (out_tx, out_rx) = mpsc::channel(256);
        let transport = Arc::new(Self {
            connections: Mutex::new(vec![(in_rx, out_tx)]),
            connect_errors: Mutex::new(vec![]),
            connect_count: AtomicU32::new(0),
        });
        (transport, MockConnection { incoming_tx: in_tx, outgoing_rx: out_rx })
    }

    fn add_connection(&self) -> MockConnection {
        let (in_tx, in_rx) = mpsc::channel(256);
        let (out_tx, out_rx) = mpsc::channel(256);
        self.connections.lock().unwrap().push((in_rx, out_tx));
        MockConnection { incoming_tx: in_tx, outgoing_rx: out_rx }
    }

    fn queue_connect_error(&self, err: &str) {
        self.connect_errors.lock().unwrap().push(err.to_string());
    }

    fn connect_count(&self) -> u32 {
        self.connect_count.load(Ordering::Relaxed)
    }
}

impl Transport for MockTransport {
    fn connect(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<TransportConn, String>> + Send + '_>> {
        self.connect_count.fetch_add(1, Ordering::Relaxed);
        {
            let mut errors = self.connect_errors.lock().unwrap();
            if !errors.is_empty() {
                let err = errors.remove(0);
                return Box::pin(async move { Err(err) });
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

struct MockSink { tx: mpsc::Sender<Vec<u8>> }
impl TransportSink for MockSink {
    fn send_data(&mut self, data: Vec<u8>) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move { self.tx.send(data).await.map_err(|e| e.to_string()) })
    }
    fn close(&mut self) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

struct MockStream { rx: mpsc::Receiver<TransportFrame> }
impl Stream for MockStream {
    type Item = TransportFrame;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

struct ArcTransport(Arc<MockTransport>);
impl Transport for ArcTransport {
    fn connect(&self) -> Pin<Box<dyn std::future::Future<Output = Result<TransportConn, String>> + Send + '_>> {
        self.0.connect()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn default_config() -> ClientConfig {
    ClientConfig {
        timeout: Duration::from_secs(2),
        min_reconnect_delay: Duration::from_millis(10),
        max_reconnect_delay: Duration::from_millis(50),
        ..ClientConfig::new("ws://test")
    }
}

fn encode_reply(reply: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(reply).unwrap()
}

async fn read_command(conn: &mut MockConnection) -> serde_json::Value {
    let data = time::timeout(Duration::from_secs(2), conn.outgoing_rx.recv())
        .await.expect("timeout waiting for command").expect("channel closed");
    serde_json::from_slice(&data).unwrap()
}

/// Perform a full connect handshake: read the connect command and feed a result.
/// Returns the command ID.
async fn do_connect(conn: &mut MockConnection) -> u32 {
    let cmd = read_command(conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert!(cmd.get("connect").is_some(), "expected connect command, got: {cmd}");
    let reply = serde_json::json!({
        "id": id,
        "connect": {"client": "test-client-id", "version": "1.0.0", "ping": 25, "pong": true}
    });
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&reply))).await.unwrap();
    id
}

/// Helper: connect the client (spawning the connect in a task so it doesn't block).
async fn connect_client(client: &Client, conn: &mut MockConnection) {
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(conn).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;
}

/// Subscribe a subscription and complete the handshake.
async fn subscribe_sub(sub: &centrifuge::Subscription, conn: &mut MockConnection) {
    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert!(cmd.get("subscribe").is_some());
    let reply = serde_json::json!({"id": id, "subscribe": {}});
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&reply))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;
}

fn make_client(config: ClientConfig) -> (Client, MockConnection, Arc<MockTransport>) {
    let (transport, conn) = MockTransport::new();
    let t = transport.clone();
    let client = Client::new_with_transport(config, Box::new(ArcTransport(transport)));
    (client, conn, t)
}


// =========================================================================
// A. Connection Lifecycle
// =========================================================================

#[tokio::test]
async fn connect_happy_path() {
    let connected = Arc::new(Mutex::new(false));
    let c = connected.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default().on_connected(move |_| { *c.lock().unwrap() = true; }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    connect_client(&client, &mut conn).await;
    assert!(*connected.lock().unwrap());
}

#[tokio::test]
async fn connect_when_already_connected() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;
    // Second connect should succeed immediately (already connected)
    client.connect().await.unwrap();
}

#[tokio::test]
async fn connect_when_closed() {
    let (client, _conn, _) = make_client(default_config());
    client.close().await.unwrap();
    let result = client.connect().await;
    assert!(matches!(result, Err(CentrifugeError::ClientClosed)));
}

#[tokio::test]
async fn disconnect_from_connected() {
    let disconnected = Arc::new(Mutex::new(false));
    let d = disconnected.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default().on_disconnected(move |_| { *d.lock().unwrap() = true; }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    connect_client(&client, &mut conn).await;
    client.disconnect().await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
    assert!(*disconnected.lock().unwrap());
}

#[tokio::test]
async fn close_shuts_down_cleanly() {
    let (client, _conn, _) = make_client(default_config());
    client.close().await.unwrap();
    assert!(matches!(client.connect().await, Err(CentrifugeError::ClientClosed)));
}

#[tokio::test]
async fn connect_transport_failure_retries() {
    let transport = Arc::new(MockTransport {
        connections: Mutex::new(vec![]),
        connect_errors: Mutex::new(vec!["refused".into()]),
        connect_count: AtomicU32::new(0),
    });
    let mut good_conn = transport.add_connection();
    let t = transport.clone();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut good_conn).await;
    task.await.unwrap().unwrap();
    assert!(t.connect_count() >= 2);
}

#[tokio::test]
async fn connect_server_error_retries() {
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });

    // First attempt: read command, return error
    let cmd = read_command(&mut MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx }).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "error": {"code": 100, "message": "internal error", "temporary": true}
    })))).await.unwrap();

    // Second attempt should succeed
    do_connect(&mut conn2).await;
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn connect_token_expired_triggers_refresh() {
    let refreshed = Arc::new(Mutex::new(false));
    let r = refreshed.clone();
    let config = ClientConfig {
        get_token: Some(Box::new(move || {
            let r = r.clone();
            Box::pin(async move { *r.lock().unwrap() = true; Ok("new-token".into()) })
        })),
        ..default_config()
    };
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(config, Box::new(ArcTransport(transport)));
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });

    // First: return token expired
    let cmd = read_command(&mut MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx }).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "error": {"code": 109, "message": "token expired"}
    })))).await.unwrap();

    // Second: succeed
    do_connect(&mut conn2).await;
    task.await.unwrap().unwrap();
    assert!(*refreshed.lock().unwrap());
}

#[tokio::test]
async fn connect_unauthorized_token_disconnects() {
    let disconnected = Arc::new(Mutex::new(false));
    let d = disconnected.clone();
    let config = ClientConfig {
        get_token: Some(Box::new(|| Box::pin(async { Err(CentrifugeError::Unauthorized) }))),
        token: String::new(),
        events: ClientEventHandlers::default().on_disconnected(move |_| { *d.lock().unwrap() = true; }),
        ..default_config()
    };
    let (client, _conn, _) = make_client(config);
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    // Should fail with disconnected
    let result = task.await.unwrap();
    assert!(result.is_err());
    time::sleep(Duration::from_millis(50)).await;
    assert!(*disconnected.lock().unwrap());
}

// =========================================================================
// B. Transport Reconnection
// =========================================================================

#[tokio::test]
async fn reconnect_on_transport_close() {
    let connecting_count = Arc::new(AtomicU32::new(0));
    let cc = connecting_count.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default().on_connecting(move |_| { cc.fetch_add(1, Ordering::Relaxed); }),
        ..default_config()
    };
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(config, Box::new(ArcTransport(transport)));

    // Connect
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx }).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Close with reconnect
    conn.incoming_tx.send(TransportFrame::Close(Some(DisconnectInfo {
        code: 3001, reason: "restart".into(), reconnect: true,
    }))).await.unwrap();

    // Should reconnect
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;
    assert!(connecting_count.load(Ordering::Relaxed) >= 2);
}

#[tokio::test]
async fn no_reconnect_on_terminal_close() {
    let disconnected = Arc::new(Mutex::new(false));
    let d = disconnected.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default().on_disconnected(move |ctx| {
            if ctx.code == 3500 { *d.lock().unwrap() = true; }
        }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    connect_client(&client, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Close(Some(DisconnectInfo {
        code: 3500, reason: "banned".into(), reconnect: false,
    }))).await.unwrap();

    time::sleep(Duration::from_millis(100)).await;
    assert!(*disconnected.lock().unwrap());
}

#[tokio::test]
async fn reconnect_on_stream_end() {
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx }).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    drop(conn.incoming_tx); // End the stream
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;
}

// =========================================================================
// C. Server Ping/Pong
// =========================================================================

#[tokio::test]
async fn server_ping_sends_pong() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    // Server ping (empty reply)
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({})))).await.unwrap();

    // Read pong
    let pong = read_command(&mut conn).await;
    assert!(pong.get("connect").is_none());
    assert!(pong.get("subscribe").is_none());
}

#[tokio::test]
async fn server_ping_no_pong_when_disabled() {
    let (client, mut conn, _) = make_client(default_config());
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });

    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let reply = serde_json::json!({"id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": false}});
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&reply))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Server ping
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({})))).await.unwrap();

    // Should NOT get a pong back
    let result = time::timeout(Duration::from_millis(200), conn.outgoing_rx.recv()).await;
    assert!(result.is_err(), "should not receive pong when pong=false");
}

#[tokio::test]
async fn ping_timeout_triggers_reconnect() {
    let config = ClientConfig {
        max_server_ping_delay: Duration::from_millis(50),
        ..default_config()
    };
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(config, Box::new(ArcTransport(transport)));

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx }).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    // Short ping interval (1s) + 50ms delay = 1.05s timeout
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 1, "pong": true}
    })))).await.unwrap();
    task.await.unwrap().unwrap();

    // Wait for ping timeout
    time::sleep(Duration::from_millis(1200)).await;
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;
}

// =========================================================================
// D. Request/Reply Commands
// =========================================================================

#[tokio::test]
async fn publish_success() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.publish("test", b"hello".to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["publish"]["channel"], "test");
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "publish": {}})))).await.unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publish_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.publish("test", b"hello".to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "error": {"code": 103, "message": "permission denied"}
    })))).await.unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

#[tokio::test]
async fn publish_when_disconnected() {
    let (client, _conn, _) = make_client(default_config());
    assert!(matches!(client.publish("test", b"hi".to_vec()).await, Err(CentrifugeError::ClientDisconnected)));
}

#[tokio::test]
async fn history_success() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.history("ch", centrifuge::HistoryOptions { limit: 10, ..Default::default() }).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "history": {"publications": [{"data": {"msg": "hi"}, "offset": 1}], "epoch": "abc", "offset": 1}
    })))).await.unwrap();
    let result = task.await.unwrap().unwrap();
    assert_eq!(result.publications.len(), 1);
    assert_eq!(result.epoch, "abc");
}

#[tokio::test]
async fn presence_success() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.presence("ch").await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "presence": {"presence": {"c1": {"user": "u1", "client": "c1"}}}
    })))).await.unwrap();
    let result = task.await.unwrap().unwrap();
    assert!(result.presence.contains_key("c1"));
}

#[tokio::test]
async fn presence_stats_success() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.presence_stats("ch").await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "presence_stats": {"num_clients": 5, "num_users": 3}
    })))).await.unwrap();
    let result = task.await.unwrap().unwrap();
    assert_eq!(result.num_clients, 5);
    assert_eq!(result.num_users, 3);
}

#[tokio::test]
async fn rpc_success() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.rpc("get_settings", b"{}".to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["rpc"]["method"], "get_settings");
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "rpc": {"data": {"result": "ok"}}
    })))).await.unwrap();
    let result = task.await.unwrap().unwrap();
    assert!(!result.data.is_empty());
}

#[tokio::test]
async fn send_fire_and_forget() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    client.send(b"{\"action\":\"ping\"}".to_vec()).await.unwrap();
    let cmd = read_command(&mut conn).await;
    assert!(cmd.get("send").is_some());
}

#[tokio::test]
async fn concurrent_pending_requests() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c1 = client.clone();
    let c2 = client.clone();
    let t1 = tokio::spawn(async move { c1.rpc("m1", b"{}".to_vec()).await });
    let t2 = tokio::spawn(async move { c2.rpc("m2", b"{}".to_vec()).await });

    let cmd1 = read_command(&mut conn).await;
    let cmd2 = read_command(&mut conn).await;
    let id1 = cmd1["id"].as_u64().unwrap() as u32;
    let id2 = cmd2["id"].as_u64().unwrap() as u32;

    // Reply in reverse order
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id2, "rpc": {"data": {"r": 2}}})))).await.unwrap();
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id1, "rpc": {"data": {"r": 1}}})))).await.unwrap();

    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();
}

// =========================================================================
// E. Client-Side Subscriptions
// =========================================================================

#[tokio::test]
async fn new_subscription_and_subscribe() {
    let subscribed = Arc::new(Mutex::new(false));
    let s = subscribed.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_subscribed(move |_| { *s.lock().unwrap() = true; }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;
    assert!(*subscribed.lock().unwrap());
}

#[tokio::test]
async fn duplicate_subscription_error() {
    let (client, _conn, _) = make_client(default_config());
    client.new_subscription("ch", SubscriptionConfig::default()).await.unwrap();
    assert!(matches!(client.new_subscription("ch", SubscriptionConfig::default()).await, Err(CentrifugeError::DuplicateSubscription)));
}

#[tokio::test]
async fn subscribe_with_recovery() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        recoverable: true,
        since: Some(centrifuge::StreamPosition { offset: 42, epoch: "epoch1".into() }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert!(cmd["subscribe"]["recover"].as_bool().unwrap());
    assert_eq!(cmd["subscribe"]["offset"], 42);
    assert_eq!(cmd["subscribe"]["epoch"], "epoch1");

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "subscribe": {
            "recoverable": true, "recovered": true, "was_recovering": true,
            "offset": 45, "epoch": "epoch1",
            "publications": [{"data": {"msg": "m1"}, "offset": 43}, {"data": {"msg": "m2"}, "offset": 45}]
        }
    })))).await.unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsubscribe_sends_command() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig::default()).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.unsubscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert!(cmd.get("unsubscribe").is_some());
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "unsubscribe": {}})))).await.unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn subscribe_permanent_error_unsubscribes() {
    let unsubscribed = Arc::new(Mutex::new(false));
    let u = unsubscribed.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_unsubscribed(move |_| { *u.lock().unwrap() = true; }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "error": {"code": 103, "message": "permission denied", "temporary": false}
    })))).await.unwrap();
    assert!(task.await.unwrap().is_err());
    time::sleep(Duration::from_millis(50)).await;
    assert!(*unsubscribed.lock().unwrap());
}

#[tokio::test]
async fn get_subscription_exists_and_not_found() {
    let (client, _conn, _) = make_client(default_config());
    client.new_subscription("ch1", SubscriptionConfig::default()).await.unwrap();
    assert!(client.get_subscription("ch1").await.unwrap().is_some());
    assert!(client.get_subscription("ch2").await.unwrap().is_none());
}

#[tokio::test]
async fn remove_subscription_while_subscribed() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig::default()).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    client.remove_subscription(&sub).await.unwrap();
    let cmd = read_command(&mut conn).await;
    assert!(cmd.get("unsubscribe").is_some());
    assert!(client.get_subscription("ch").await.unwrap().is_none());
}

// =========================================================================
// F. Server-Side Subscriptions
// =========================================================================

#[tokio::test]
async fn server_subscribe_on_connect() {
    let channels = Arc::new(Mutex::new(Vec::new()));
    let ch = channels.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default().on_server_subscribed(move |ctx| { ch.lock().unwrap().push(ctx.channel.clone()); }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let reply = serde_json::json!({
        "id": id, "connect": {
            "client": "test", "version": "1.0", "ping": 25, "pong": true,
            "subs": {"notif": {"recoverable": true, "offset": 10, "epoch": "e1"}, "updates": {}}
        }
    });
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&reply))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    let subs = channels.lock().unwrap();
    assert!(subs.contains(&"notif".to_string()));
    assert!(subs.contains(&"updates".to_string()));
}

#[tokio::test]
async fn server_sub_disappears_on_reconnect() {
    let unsubs = Arc::new(Mutex::new(Vec::new()));
    let u = unsubs.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default()
            .on_server_subscribed(|_| {})
            .on_server_unsubscribed(move |ctx| { u.lock().unwrap().push(ctx.channel.clone()); }),
        ..default_config()
    };
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(config, Box::new(ArcTransport(transport)));

    // First connect with server sub
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx }).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true,
            "subs": {"notif": {"recoverable": true, "offset": 5, "epoch": "e1"}}
        }
    })))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // Trigger reconnect
    conn.incoming_tx.send(TransportFrame::Close(Some(DisconnectInfo { code: 3001, reason: "restart".into(), reconnect: true }))).await.unwrap();

    // Second connect without server sub
    let cmd2 = read_command(&mut conn2).await;
    let id2 = cmd2["id"].as_u64().unwrap() as u32;
    // Verify recovery info was sent
    assert!(cmd2["connect"]["subs"]["notif"]["recover"].as_bool().unwrap_or(false));
    conn2.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id2, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {}}
    })))).await.unwrap();

    time::sleep(Duration::from_millis(100)).await;
    assert!(unsubs.lock().unwrap().contains(&"notif".to_string()));
}

// =========================================================================
// G. Push Message Handling
// =========================================================================

#[tokio::test]
async fn publication_to_client_sub() {
    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let r = received.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_publication(move |ctx| { r.lock().unwrap().push(ctx.publication.data.clone()); }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"channel": "ch", "pub": {"data": {"msg": "hello"}, "offset": 1}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert_eq!(received.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn publication_to_server_sub() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let r = received.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default()
            .on_server_subscribed(|_| {})
            .on_server_publication(move |ctx| { r.lock().unwrap().push(ctx.channel.clone()); }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"updates": {}}}
    })))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"channel": "updates", "pub": {"data": {"msg": "world"}, "offset": 1}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert_eq!(received.lock().unwrap()[0], "updates");
}

#[tokio::test]
async fn join_and_leave_pushes() {
    let joins = Arc::new(Mutex::new(Vec::new()));
    let leaves = Arc::new(Mutex::new(Vec::new()));
    let j = joins.clone();
    let l = leaves.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default()
            .on_join(move |ctx| { j.lock().unwrap().push(ctx.info.client.clone()); })
            .on_leave(move |ctx| { l.lock().unwrap().push(ctx.info.client.clone()); }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"push": {"channel": "ch", "join": {"info": {"client": "c1", "user": "u1"}}}})))).await.unwrap();
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"push": {"channel": "ch", "leave": {"info": {"client": "c2", "user": "u2"}}}})))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert_eq!(joins.lock().unwrap().len(), 1);
    assert_eq!(leaves.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn disconnect_push_reconnectable() {
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx }).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"disconnect": {"code": 3001, "reason": "restart", "reconnect": true}}
    })))).await.unwrap();
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn disconnect_push_terminal() {
    let disconnected = Arc::new(Mutex::new(false));
    let d = disconnected.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default().on_disconnected(move |_| { *d.lock().unwrap() = true; }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    connect_client(&client, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"disconnect": {"code": 3500, "reason": "banned", "reconnect": false}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(*disconnected.lock().unwrap());
}

#[tokio::test]
async fn message_push() {
    let received = Arc::new(Mutex::new(false));
    let r = received.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default().on_message(move |_| { *r.lock().unwrap() = true; }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    connect_client(&client, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"message": {"data": {"alert": "hello"}}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(*received.lock().unwrap());
}

#[tokio::test]
async fn server_unsubscribe_resubscribable() {
    let subscribing_count = Arc::new(AtomicU32::new(0));
    let sc = subscribing_count.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_subscribing(move |_| { sc.fetch_add(1, Ordering::Relaxed); }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    // Server unsubscribe with code >= 2500 → resubscribe
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"channel": "ch", "unsubscribe": {"code": 2500, "reason": "temporary"}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(200)).await;
    assert!(subscribing_count.load(Ordering::Relaxed) >= 2);
}

#[tokio::test]
async fn server_unsubscribe_terminal() {
    let unsubscribed = Arc::new(Mutex::new(false));
    let u = unsubscribed.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_unsubscribed(move |_| { *u.lock().unwrap() = true; }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"channel": "ch", "unsubscribe": {"code": 2000, "reason": "revoked"}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(*unsubscribed.lock().unwrap());
}

// =========================================================================
// H. Reconnect + Resubscribe
// =========================================================================

#[tokio::test]
async fn reconnect_resubscribes_active_subscriptions() {
    let subscribed_count = Arc::new(AtomicU32::new(0));
    let sc = subscribed_count.clone();
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_subscribed(move |_| { sc.fetch_add(1, Ordering::Relaxed); }),
        ..Default::default()
    }).await.unwrap();

    // Connect
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let mut first = MockConnection { incoming_tx: conn.incoming_tx.clone(), outgoing_rx: conn.outgoing_rx };
    do_connect(&mut first).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Subscribe
    subscribe_sub(&sub, &mut first).await;
    assert_eq!(subscribed_count.load(Ordering::Relaxed), 1);

    // Trigger reconnect
    conn.incoming_tx.send(TransportFrame::Close(Some(DisconnectInfo { code: 3001, reason: "restart".into(), reconnect: true }))).await.unwrap();

    // Second connect
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;

    // Read and reply to resubscribe
    let cmd = read_command(&mut conn2).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["subscribe"]["channel"], "ch");
    conn2.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "subscribe": {}})))).await.unwrap();

    time::sleep(Duration::from_millis(100)).await;
    assert_eq!(subscribed_count.load(Ordering::Relaxed), 2);
}

// =========================================================================
// I. Edge Cases
// =========================================================================

#[tokio::test]
async fn reply_for_unknown_id() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": 99999, "publish": {}})))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    // Should not panic, client still functional
    client.send(b"test".to_vec()).await.unwrap();
}

#[tokio::test]
async fn malformed_data_from_transport() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(b"not json {{{{".to_vec())).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    client.send(b"test".to_vec()).await.unwrap();
}

#[tokio::test]
async fn multiple_replies_in_single_frame() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c1 = client.clone();
    let c2 = client.clone();
    let t1 = tokio::spawn(async move { c1.rpc("m1", b"{}".to_vec()).await });
    let t2 = tokio::spawn(async move { c2.rpc("m2", b"{}".to_vec()).await });

    let cmd1 = read_command(&mut conn).await;
    let cmd2 = read_command(&mut conn).await;
    let id1 = cmd1["id"].as_u64().unwrap() as u32;
    let id2 = cmd2["id"].as_u64().unwrap() as u32;

    // Both replies in one frame
    let combined = format!(
        "{{\"id\":{id1},\"rpc\":{{\"data\":{{\"r\":1}}}}}}\n{{\"id\":{id2},\"rpc\":{{\"data\":{{\"r\":2}}}}}}"
    );
    conn.incoming_tx.send(TransportFrame::Data(combined.into_bytes())).await.unwrap();

    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();
}

// =========================================================================
// J. Subscribe before connect (queued subscriptions)
// =========================================================================

#[tokio::test]
async fn subscribe_before_connect_auto_subscribes() {
    let subscribed = Arc::new(Mutex::new(false));
    let s = subscribed.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_subscribed(move |_| { *s.lock().unwrap() = true; }),
        ..Default::default()
    }).await.unwrap();

    // Subscribe BEFORE connecting
    let sub2 = sub.clone();
    let sub_task = tokio::spawn(async move { sub2.subscribe().await });

    // Now connect
    let c = client.clone();
    let conn_task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut conn).await;
    conn_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // Read the auto-sent subscribe command
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["subscribe"]["channel"], "ch");
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "subscribe": {}})))).await.unwrap();

    sub_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;
    assert!(*subscribed.lock().unwrap());
}

// =========================================================================
// K. Temporary subscribe error with backoff retry
// =========================================================================

#[tokio::test]
async fn subscribe_temporary_error_retries() {
    let error_count = Arc::new(AtomicU32::new(0));
    let ec = error_count.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_error(move |_| { ec.fetch_add(1, Ordering::Relaxed); }),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;

    let sub2 = sub.clone();
    let _sub_task = tokio::spawn(async move { sub2.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "error": {"code": 50, "message": "try again", "temporary": true}
    })))).await.unwrap();

    // Wait for resubscribe attempt
    time::sleep(Duration::from_millis(200)).await;
    let cmd2 = read_command(&mut conn).await;
    assert_eq!(cmd2["subscribe"]["channel"], "ch");
    assert!(error_count.load(Ordering::Relaxed) >= 1);
}

// =========================================================================
// L. Token expired (109) on subscribe
// =========================================================================

#[tokio::test]
async fn subscribe_token_expired_refreshes_and_retries() {
    let token_called = Arc::new(AtomicU32::new(0));
    let tc = token_called.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        token: "old-token".into(),
        get_token: Some(Box::new(move |_channel| {
            let tc = tc.clone();
            Box::pin(async move { tc.fetch_add(1, Ordering::Relaxed); Ok("new-token".to_string()) })
        })),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;

    let sub2 = sub.clone();
    let _sub_task = tokio::spawn(async move { sub2.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["subscribe"]["token"], "old-token");
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "error": {"code": 109, "message": "token expired"}
    })))).await.unwrap();

    time::sleep(Duration::from_millis(200)).await;
    let cmd2 = read_command(&mut conn).await;
    assert_eq!(cmd2["subscribe"]["token"], "new-token");
    assert!(token_called.load(Ordering::Relaxed) >= 1);
}

// =========================================================================
// M. Subscription handle methods
// =========================================================================

#[tokio::test]
async fn subscription_publish() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig::default()).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.publish(b"hello".to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["publish"]["channel"], "ch");
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "publish": {}})))).await.unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn subscription_history() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig::default()).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.history(centrifuge::HistoryOptions { limit: 5, ..Default::default() }).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "history": {"publications": [], "epoch": "e1", "offset": 0}
    })))).await.unwrap();
    assert_eq!(task.await.unwrap().unwrap().epoch, "e1");
}

#[tokio::test]
async fn subscription_presence() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig::default()).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.presence().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "presence": {"presence": {"c1": {"user": "u1", "client": "c1"}}}
    })))).await.unwrap();
    assert!(task.await.unwrap().unwrap().presence.contains_key("c1"));
}

#[tokio::test]
async fn subscription_presence_stats() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig::default()).await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.presence_stats().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "presence_stats": {"num_clients": 3, "num_users": 2}
    })))).await.unwrap();
    let r = task.await.unwrap().unwrap();
    assert_eq!(r.num_clients, 3);
    assert_eq!(r.num_users, 2);
}

// =========================================================================
// N. Disconnect with active subs and server subs
// =========================================================================

#[tokio::test]
async fn disconnect_unsubscribes_active_and_server_subs() {
    let sub_unsub = Arc::new(Mutex::new(false));
    let su = sub_unsub.clone();
    let server_unsub = Arc::new(Mutex::new(Vec::new()));
    let ssu = server_unsub.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default()
            .on_server_subscribed(|_| {})
            .on_server_unsubscribed(move |ctx| { ssu.lock().unwrap().push(ctx.channel.clone()); }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    let sub = client.new_subscription("ch", SubscriptionConfig {
        events: SubscriptionEventHandlers::default().on_unsubscribed(move |_| { *su.lock().unwrap() = true; }),
        ..Default::default()
    }).await.unwrap();

    // Connect with server sub
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"notif": {"recoverable": true}}}
    })))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;
    subscribe_sub(&sub, &mut conn).await;

    client.disconnect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(*sub_unsub.lock().unwrap());
    assert!(server_unsub.lock().unwrap().contains(&"notif".to_string()));
}

// =========================================================================
// O. Close while connecting
// =========================================================================

#[tokio::test]
async fn close_while_connecting_fails_waiters() {
    let transport = Arc::new(MockTransport {
        connections: Mutex::new(vec![]),
        connect_errors: Mutex::new(vec!["refused".into()]),
        connect_count: AtomicU32::new(0),
    });
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();
    assert!(task.await.unwrap().is_err());
}

// =========================================================================
// P. Server join/leave on server-side subscriptions
// =========================================================================

#[tokio::test]
async fn server_sub_join_and_leave() {
    let joins = Arc::new(Mutex::new(Vec::new()));
    let leaves = Arc::new(Mutex::new(Vec::new()));
    let j = joins.clone();
    let l = leaves.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default()
            .on_server_subscribed(|_| {})
            .on_server_join(move |ctx| { j.lock().unwrap().push(ctx.channel.clone()); })
            .on_server_leave(move |ctx| { l.lock().unwrap().push(ctx.channel.clone()); }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"notif": {}}}
    })))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"push": {"channel": "notif", "join": {"info": {"client": "c1", "user": "u1"}}}})))).await.unwrap();
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"push": {"channel": "notif", "leave": {"info": {"client": "c2", "user": "u2"}}}})))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert_eq!(joins.lock().unwrap().len(), 1);
    assert_eq!(leaves.lock().unwrap().len(), 1);
}

// =========================================================================
// Q. Server-side unsubscribe and subscribe pushes
// =========================================================================

#[tokio::test]
async fn server_sub_unsubscribe_push() {
    let unsubs = Arc::new(Mutex::new(Vec::new()));
    let u = unsubs.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default()
            .on_server_subscribed(|_| {})
            .on_server_unsubscribed(move |ctx| { u.lock().unwrap().push(ctx.channel.clone()); }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"notif": {}}}
    })))).await.unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"channel": "notif", "unsubscribe": {"code": 2000, "reason": "removed"}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(unsubs.lock().unwrap().contains(&"notif".to_string()));
}

#[tokio::test]
async fn server_subscribe_push_mid_connection() {
    let subs = Arc::new(Mutex::new(Vec::new()));
    let s = subs.clone();
    let config = ClientConfig {
        events: ClientEventHandlers::default()
            .on_server_subscribed(move |ctx| { s.lock().unwrap().push(ctx.channel.clone()); }),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    connect_client(&client, &mut conn).await;

    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "push": {"channel": "alerts", "subscribe": {"recoverable": true, "offset": 1, "epoch": "e1"}}
    })))).await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(subs.lock().unwrap().contains(&"alerts".to_string()));
}

// =========================================================================
// R. Error replies for various operations
// =========================================================================

#[tokio::test]
async fn rpc_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.rpc("bad", b"{}".to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "error": {"code": 100, "message": "internal"}})))).await.unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

#[tokio::test]
async fn history_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.history("ch", centrifuge::HistoryOptions::default()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "error": {"code": 103, "message": "denied"}})))).await.unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

// =========================================================================
// S. Connection token refresh during connected state
// =========================================================================

#[tokio::test]
async fn connection_token_refresh_during_connected() {
    let refresh_called = Arc::new(AtomicU32::new(0));
    let rc = refresh_called.clone();
    let config = ClientConfig {
        get_token: Some(Box::new(move || {
            let rc = rc.clone();
            Box::pin(async move { rc.fetch_add(1, Ordering::Relaxed); Ok("refreshed".to_string()) })
        })),
        token: "initial".into(),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "expires": true, "ttl": 1}
    })))).await.unwrap();
    task.await.unwrap().unwrap();

    // Wait for refresh timer (1s)
    time::sleep(Duration::from_millis(1200)).await;
    let cmd = read_command(&mut conn).await;
    assert!(cmd.get("refresh").is_some());
    assert_eq!(cmd["refresh"]["token"], "refreshed");
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "refresh": {"expires": false}})))).await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
    assert!(refresh_called.load(Ordering::Relaxed) >= 1);
}

// =========================================================================
// T. Subscription token refresh during subscribed state
// =========================================================================

#[tokio::test]
async fn subscription_token_refresh_during_subscribed() {
    let tc = Arc::new(AtomicU32::new(0));
    let tc2 = tc.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription("ch", SubscriptionConfig {
        token: "sub-tok".into(),
        get_token: Some(Box::new(move |_| { let tc = tc2.clone(); Box::pin(async move { tc.fetch_add(1, Ordering::Relaxed); Ok("new-sub-tok".into()) }) })),
        ..Default::default()
    }).await.unwrap();
    connect_client(&client, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "subscribe": {"expires": true, "ttl": 1}})))).await.unwrap();
    task.await.unwrap().unwrap();

    time::sleep(Duration::from_millis(1200)).await;
    let cmd = read_command(&mut conn).await;
    assert!(cmd.get("sub_refresh").is_some());
    assert_eq!(cmd["sub_refresh"]["channel"], "ch");
    assert_eq!(cmd["sub_refresh"]["token"], "new-sub-tok");
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({"id": id, "sub_refresh": {"expires": false}})))).await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
    assert!(tc.load(Ordering::Relaxed) >= 1);
}
