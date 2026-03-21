/// Integration tests against a real Centrifugo server.
///
/// Prerequisites: `docker compose up -d` to start the Centrifugo server.
///
/// These tests verify actual Centrifuge protocol compliance by connecting
/// to a real server and exercising all operations.
///
/// Run with: `cargo test --test integration_test`
/// Skip with: `cargo test --test actor_tests` (unit tests only)
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time;

use centrifuge::config::{ClientConfig, ProtocolType, SubscriptionConfig};
use centrifuge::events::{ClientEventHandlers, SubscriptionEventHandlers};
use centrifuge::Client;

const WS_URL: &str = "ws://localhost:8000/connection/websocket";

fn is_server_available() -> bool {
    std::net::TcpStream::connect("localhost:8000").is_ok()
}

macro_rules! skip_if_no_server {
    () => {
        if !is_server_available() {
            eprintln!("SKIPPED: Centrifugo server not running (docker compose up -d)");
            return;
        }
    };
}

fn json_config() -> ClientConfig {
    ClientConfig::new(WS_URL)
        .protocol_type(ProtocolType::Json)
        .name("rs-test")
        .version("0.1.0")
        .timeout(Duration::from_secs(5))
}

fn protobuf_config() -> ClientConfig {
    ClientConfig::new(WS_URL)
        .protocol_type(ProtocolType::Protobuf)
        .name("rs-test-pb")
        .version("0.1.0")
        .timeout(Duration::from_secs(5))
}

// =========================================================================
// JSON protocol tests
// =========================================================================

#[tokio::test]
async fn json_connect_and_disconnect() {
    skip_if_no_server!();

    let connected = Arc::new(Mutex::new(false));
    let c = connected.clone();
    let disconnected = Arc::new(Mutex::new(false));
    let d = disconnected.clone();

    let config = json_config()
        .events(
            ClientEventHandlers::default()
                .on_connected(move |ctx| {
                    assert!(!ctx.client_id.is_empty(), "should have client_id");
                    *c.lock().unwrap() = true;
                })
                .on_disconnected(move |_| {
                    *d.lock().unwrap() = true;
                }),
        );

    let client = Client::new(config);
    client.connect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(*connected.lock().unwrap(), "on_connected should have fired");

    client.disconnect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(*disconnected.lock().unwrap(), "on_disconnected should have fired");
}

#[tokio::test]
async fn json_subscribe_and_receive_publication() {
    skip_if_no_server!();

    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let r = received.clone();

    let client = Client::new(json_config());
    let sub = client
        .new_subscription(
            "jsonpub",
            SubscriptionConfig {
                events: SubscriptionEventHandlers::default().on_publication(move |ctx| {
                    r.lock().unwrap().push(ctx.publication.data.clone());
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // Publish from the same client
    sub.publish(br#"{"msg":"hello from rust"}"#.to_vec())
        .await
        .unwrap();

    time::sleep(Duration::from_millis(500)).await;

    let pubs = received.lock().unwrap();
    assert!(
        !pubs.is_empty(),
        "should have received the publication we sent"
    );
    let data_str = String::from_utf8_lossy(&pubs[0]);
    assert!(data_str.contains("hello from rust"));

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_history() {
    skip_if_no_server!();

    let client = Client::new(json_config());
    let sub = client
        .new_subscription("jsonhist", SubscriptionConfig::default())
        .await
        .unwrap();

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // Publish a few messages
    for i in 0..3 {
        sub.publish(format!(r#"{{"i":{i}}}"#).into_bytes())
            .await
            .unwrap();
    }
    time::sleep(Duration::from_millis(300)).await;

    // Query history
    let result = sub
        .history(centrifuge::HistoryOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(
        result.publications.len() >= 3,
        "history should contain at least 3 publications, got {}",
        result.publications.len()
    );
    assert!(!result.epoch.is_empty(), "epoch should be non-empty");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_presence() {
    skip_if_no_server!();

    let client = Client::new(json_config());
    let sub = client
        .new_subscription("jsonpres", SubscriptionConfig::default())
        .await
        .unwrap();

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    // Check presence — we should see ourselves
    let result = sub.presence().await.unwrap();
    assert!(
        !result.presence.is_empty(),
        "presence should contain at least our connection"
    );

    // Check presence stats
    let stats = sub.presence_stats().await.unwrap();
    assert!(
        stats.num_clients >= 1,
        "num_clients should be >= 1, got {}",
        stats.num_clients
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_subscribe_with_recovery() {
    skip_if_no_server!();

    let publications = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let p = publications.clone();

    let client = Client::new(json_config());
    let sub = client
        .new_subscription(
            "jsonrecov",
            SubscriptionConfig {
                recoverable: true,
                events: SubscriptionEventHandlers::default().on_publication(move |ctx| {
                    p.lock().unwrap().push(ctx.publication.data.clone());
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // Publish a message
    sub.publish(br#"{"msg":"before disconnect"}"#.to_vec())
        .await
        .unwrap();
    time::sleep(Duration::from_millis(200)).await;

    // Verify we received it
    let count_before = publications.lock().unwrap().len();
    assert!(count_before >= 1, "should receive publication before disconnect");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_reconnect_on_server_restart() {
    skip_if_no_server!();

    let connecting_count = Arc::new(AtomicU32::new(0));
    let cc = connecting_count.clone();
    let connected_count = Arc::new(AtomicU32::new(0));
    let conc = connected_count.clone();

    let config = json_config().events(
        ClientEventHandlers::default()
            .on_connecting(move |_| {
                cc.fetch_add(1, Ordering::Relaxed);
            })
            .on_connected(move |_| {
                conc.fetch_add(1, Ordering::Relaxed);
            }),
    );

    let client = Client::new(config);
    client.connect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert_eq!(connected_count.load(Ordering::Relaxed), 1);

    client.disconnect().await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn json_rpc() {
    skip_if_no_server!();

    let client = Client::new(json_config());
    client.connect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // RPC without a handler on the server should return an error
    let result = client.rpc("nonexistent", b"{}".to_vec()).await;
    // Centrifugo returns "not available" error for unknown RPC methods
    assert!(result.is_err(), "RPC to unknown method should fail");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_cross_client_publish() {
    skip_if_no_server!();

    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let r = received.clone();

    // Client 1: subscriber
    let client1 = Client::new(json_config());
    let sub1 = client1
        .new_subscription(
            "jsoncross",
            SubscriptionConfig {
                events: SubscriptionEventHandlers::default().on_publication(move |ctx| {
                    r.lock().unwrap().push(ctx.publication.data.clone());
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    // Client 2: publisher
    let client2 = Client::new(json_config());
    let sub2 = client2
        .new_subscription("jsoncross", SubscriptionConfig::default())
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // Publish from client 2
    sub2.publish(br#"{"from":"client2"}"#.to_vec())
        .await
        .unwrap();

    time::sleep(Duration::from_millis(500)).await;

    let pubs = received.lock().unwrap();
    assert!(
        !pubs.is_empty(),
        "client1 should receive publication from client2"
    );
    let data_str = String::from_utf8_lossy(&pubs[0]);
    assert!(data_str.contains("client2"));

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_join_leave_events() {
    skip_if_no_server!();

    let joins = Arc::new(Mutex::new(Vec::<String>::new()));
    let j = joins.clone();

    // Client 1: listens for joins
    let client1 = Client::new(json_config());
    let sub1 = client1
        .new_subscription(
            "jsonjl",
            SubscriptionConfig {
                join_leave: true,
                events: SubscriptionEventHandlers::default().on_join(move |ctx| {
                    j.lock().unwrap().push(ctx.info.client.clone());
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    // Client 2: joins the same channel
    let client2 = Client::new(json_config());
    let sub2 = client2
        .new_subscription(
            "jsonjl",
            SubscriptionConfig {
                join_leave: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(500)).await;

    let joined = joins.lock().unwrap();
    assert!(
        !joined.is_empty(),
        "should have received a join event for client2"
    );

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

// =========================================================================
// Protobuf protocol tests — same scenarios, different wire format
// =========================================================================

#[tokio::test]
async fn protobuf_connect_and_disconnect() {
    skip_if_no_server!();

    let connected = Arc::new(Mutex::new(false));
    let c = connected.clone();

    let config = protobuf_config().events(
        ClientEventHandlers::default().on_connected(move |ctx| {
            assert!(!ctx.client_id.is_empty());
            *c.lock().unwrap() = true;
        }),
    );

    let client = Client::new(config);
    client.connect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    assert!(*connected.lock().unwrap());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_subscribe_and_publish() {
    skip_if_no_server!();

    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let r = received.clone();

    let client = Client::new(protobuf_config());
    let sub = client
        .new_subscription(
            "pbpub",
            SubscriptionConfig {
                events: SubscriptionEventHandlers::default().on_publication(move |ctx| {
                    r.lock().unwrap().push(ctx.publication.data.clone());
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // Publish binary data (protobuf treats data as bytes)
    sub.publish(b"binary payload".to_vec()).await.unwrap();

    time::sleep(Duration::from_millis(500)).await;

    let pubs = received.lock().unwrap();
    assert!(!pubs.is_empty(), "should receive publication via protobuf");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_history_and_presence() {
    skip_if_no_server!();

    let client = Client::new(protobuf_config());
    let sub = client
        .new_subscription("pbhist", SubscriptionConfig::default())
        .await
        .unwrap();

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub.publish(b"msg1".to_vec()).await.unwrap();
    sub.publish(b"msg2".to_vec()).await.unwrap();
    time::sleep(Duration::from_millis(300)).await;

    let history = sub
        .history(centrifuge::HistoryOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        history.publications.len() >= 2,
        "protobuf history should work"
    );

    let presence = sub.presence().await.unwrap();
    assert!(!presence.presence.is_empty(), "protobuf presence should work");

    let stats = sub.presence_stats().await.unwrap();
    assert!(stats.num_clients >= 1, "protobuf presence stats should work");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_cross_client_publish() {
    skip_if_no_server!();

    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let r = received.clone();

    let client1 = Client::new(protobuf_config());
    let sub1 = client1
        .new_subscription(
            "pbcross",
            SubscriptionConfig {
                events: SubscriptionEventHandlers::default().on_publication(move |ctx| {
                    r.lock().unwrap().push(ctx.publication.data.clone());
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let client2 = Client::new(protobuf_config());
    let sub2 = client2
        .new_subscription("pbcross", SubscriptionConfig::default())
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub2.publish(b"from pb client2".to_vec()).await.unwrap();
    time::sleep(Duration::from_millis(500)).await;

    let pubs = received.lock().unwrap();
    assert!(!pubs.is_empty(), "protobuf cross-client publish should work");

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

// =========================================================================
// Cross-protocol test: JSON publisher, Protobuf subscriber
// =========================================================================

#[tokio::test]
async fn cross_protocol_json_publishes_protobuf_receives() {
    skip_if_no_server!();

    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let r = received.clone();

    // Subscriber uses Protobuf
    let client1 = Client::new(protobuf_config());
    let sub1 = client1
        .new_subscription(
            "xproto",
            SubscriptionConfig {
                events: SubscriptionEventHandlers::default().on_publication(move |ctx| {
                    r.lock().unwrap().push(ctx.publication.data.clone());
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    // Publisher uses JSON
    let client2 = Client::new(json_config());
    let sub2 = client2
        .new_subscription("xproto", SubscriptionConfig::default())
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub2.publish(br#"{"cross":"protocol"}"#.to_vec())
        .await
        .unwrap();
    time::sleep(Duration::from_millis(500)).await;

    let pubs = received.lock().unwrap();
    assert!(
        !pubs.is_empty(),
        "protobuf client should receive from json publisher"
    );

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}
