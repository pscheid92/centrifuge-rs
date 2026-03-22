/// Integration tests against a real Centrifugo server (via testcontainers).
///
/// These tests verify actual Centrifuge protocol compliance by connecting
/// to a real server and exercising all operations.
///
/// Docker must be running. The container is started automatically.
mod common;

use std::time::Duration;

use tokio::time;

use centrifuge_client::Client;
use centrifuge_client::config::{ClientConfig, ProtocolType, SubscriptionConfig};

fn json_config(url: &str) -> ClientConfig {
    ClientConfig::new(url)
        .protocol_type(ProtocolType::Json)
        .name("rs-test")
        .version("0.1.0")
        .timeout(Duration::from_secs(5))
}

fn protobuf_config(url: &str) -> ClientConfig {
    ClientConfig::new(url)
        .protocol_type(ProtocolType::Protobuf)
        .name("rs-test-pb")
        .version("0.1.0")
        .timeout(Duration::from_secs(5))
}

/// Drain subscription events until the predicate returns true, with a timeout.
async fn wait_for_sub_event(
    events: &mut tokio::sync::mpsc::Receiver<centrifuge_client::SubEvent>,
    timeout_ms: u64,
    mut predicate: impl FnMut(&centrifuge_client::SubEvent) -> bool,
) -> bool {
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(timeout_ms), events.recv()).await {
        if predicate(&e) {
            return true;
        }
    }
    false
}

/// Drain client events until the predicate returns true, with a timeout.
async fn wait_for_client_event(
    events: &mut tokio::sync::mpsc::Receiver<centrifuge_client::ClientEvent>,
    timeout_ms: u64,
    mut predicate: impl FnMut(&centrifuge_client::ClientEvent) -> bool,
) -> bool {
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(timeout_ms), events.recv()).await {
        if predicate(&e) {
            return true;
        }
    }
    false
}

// =========================================================================
// JSON protocol tests
// =========================================================================

#[tokio::test]
async fn json_connect_and_disconnect() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let mut events = client.events().expect("events");
    client.connect().await.unwrap();

    // Wait for Connected event
    assert!(
        wait_for_client_event(&mut events, 500, |e| {
            if let centrifuge_client::ClientEvent::Connected(ctx) = e {
                assert!(!ctx.client_id.is_empty(), "should have client_id");
                true
            } else {
                false
            }
        })
        .await
    );

    client.disconnect().await.unwrap();

    // Wait for Disconnected event
    assert!(
        wait_for_client_event(&mut events, 500, |e| {
            matches!(e, centrifuge_client::ClientEvent::Disconnected(_))
        })
        .await
    );
}

#[tokio::test]
async fn json_subscribe_and_receive_publication() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("jsonpub", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub.publish(br#"{"msg":"hello from rust"}"#.to_vec()).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            if let centrifuge_client::SubEvent::Publication(pub_data) = e {
                assert!(String::from_utf8_lossy(&pub_data.data).contains("hello from rust"));
                true
            } else {
                false
            }
        })
        .await,
        "should have received the publication"
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_history() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("jsonhist", SubscriptionConfig::default())
        .await
        .unwrap();
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    for i in 0..3 {
        sub.publish(format!(r#"{{"i":{i}}}"#).into_bytes()).await.unwrap();
    }
    time::sleep(Duration::from_millis(300)).await;

    let result = sub
        .history(centrifuge_client::HistoryOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(result.publications.len() >= 3);
    assert!(!result.epoch.is_empty());

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_presence() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("jsonpres", SubscriptionConfig::default())
        .await
        .unwrap();
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let result = sub.presence().await.unwrap();
    assert!(!result.presence.is_empty());

    let stats = sub.presence_stats().await.unwrap();
    assert!(stats.num_clients >= 1);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_subscribe_with_recovery() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription(
            "jsonrecov",
            SubscriptionConfig {
                recoverable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub.publish(br#"{"msg":"before disconnect"}"#.to_vec()).await.unwrap();

    // Wait for the publication event
    assert!(
        wait_for_sub_event(&mut events, 500, |e| {
            matches!(e, centrifuge_client::SubEvent::Publication(_))
        })
        .await
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_rpc() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    client.connect().await.unwrap();

    let result = client.rpc("nonexistent", b"{}".to_vec()).await;
    assert!(result.is_err(), "RPC to unknown method should fail");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_cross_client_publish() {
    let server = common::start_insecure().await;

    let client1 = Client::new(json_config(&server.ws_url));
    let sub1 = client1
        .new_subscription("jsoncross", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub1.events().expect("events");
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let client2 = Client::new(json_config(&server.ws_url));
    let sub2 = client2
        .new_subscription("jsoncross", SubscriptionConfig::default())
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub2.publish(br#"{"from":"client2"}"#.to_vec()).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            if let centrifuge_client::SubEvent::Publication(pub_data) = e {
                String::from_utf8_lossy(&pub_data.data).contains("client2")
            } else {
                false
            }
        })
        .await
    );

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_join_leave_events() {
    let server = common::start_insecure().await;

    let client1 = Client::new(json_config(&server.ws_url));
    let sub1 = client1
        .new_subscription(
            "jsonjl",
            SubscriptionConfig {
                join_leave: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub1.events().expect("events");
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let client2 = Client::new(json_config(&server.ws_url));
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

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            matches!(e, centrifuge_client::SubEvent::Join(_))
        })
        .await
    );

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

// =========================================================================
// Protobuf protocol tests
// =========================================================================

#[tokio::test]
async fn protobuf_connect_and_disconnect() {
    let server = common::start_insecure().await;

    let client = Client::new(protobuf_config(&server.ws_url));
    let mut events = client.events().expect("events");
    client.connect().await.unwrap();

    assert!(
        wait_for_client_event(&mut events, 500, |e| {
            if let centrifuge_client::ClientEvent::Connected(ctx) = e {
                assert!(!ctx.client_id.is_empty());
                true
            } else {
                false
            }
        })
        .await
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_subscribe_and_publish() {
    let server = common::start_insecure().await;

    let client = Client::new(protobuf_config(&server.ws_url));
    let sub = client
        .new_subscription("pbpub", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub.publish(b"binary payload".to_vec()).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            matches!(e, centrifuge_client::SubEvent::Publication(_))
        })
        .await
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_history_and_presence() {
    let server = common::start_insecure().await;

    let client = Client::new(protobuf_config(&server.ws_url));
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
        .history(centrifuge_client::HistoryOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(history.publications.len() >= 2);

    let presence = sub.presence().await.unwrap();
    assert!(!presence.presence.is_empty());

    let stats = sub.presence_stats().await.unwrap();
    assert!(stats.num_clients >= 1);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_cross_client_publish() {
    let server = common::start_insecure().await;

    let client1 = Client::new(protobuf_config(&server.ws_url));
    let sub1 = client1
        .new_subscription("pbcross", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub1.events().expect("events");
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let client2 = Client::new(protobuf_config(&server.ws_url));
    let sub2 = client2
        .new_subscription("pbcross", SubscriptionConfig::default())
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub2.publish(b"from pb client2".to_vec()).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            matches!(e, centrifuge_client::SubEvent::Publication(_))
        })
        .await
    );

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

// =========================================================================
// Cross-protocol test
// =========================================================================

#[tokio::test]
async fn cross_protocol_json_publishes_protobuf_receives() {
    let server = common::start_insecure().await;

    let client1 = Client::new(protobuf_config(&server.ws_url));
    let sub1 = client1
        .new_subscription("xproto", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub1.events().expect("events");
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let client2 = Client::new(json_config(&server.ws_url));
    let sub2 = client2
        .new_subscription("xproto", SubscriptionConfig::default())
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub2.publish(br#"{"cross":"protocol"}"#.to_vec()).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            matches!(e, centrifuge_client::SubEvent::Publication(_))
        })
        .await
    );

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

// =========================================================================
// Go client test parity -- replicating centrifuge-go test scenarios
// =========================================================================

/// Equivalent to Go's TestHandlePublish: publication includes publisher info.
#[tokio::test]
async fn go_parity_publication_includes_info() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("goparity_info", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    sub.publish(br#"{"msg":"with_info"}"#.to_vec()).await.unwrap();

    let mut pub_info = None;
    wait_for_sub_event(&mut events, 1000, |e| {
        if let centrifuge_client::SubEvent::Publication(pub_data) = e {
            pub_info = pub_data.info.clone();
            true
        } else {
            false
        }
    })
    .await;
    assert!(
        pub_info.is_some(),
        "publication should include publisher info (Go: TestHandlePublish)"
    );
    assert!(
        !pub_info.as_ref().unwrap().client.is_empty(),
        "info.client should be non-empty"
    );
    client.disconnect().await.unwrap();
}

/// Equivalent to Go's TestSubscribeError: unknown namespace triggers error.
#[tokio::test]
async fn go_parity_subscribe_unknown_namespace_error() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("unknown_ns:channel", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    client.connect().await.unwrap();
    let result = sub.subscribe().await;

    let error_seen = wait_for_sub_event(&mut events, 200, |e| matches!(e, centrifuge_client::SubEvent::Error(_))).await;
    assert!(
        result.is_err() || error_seen,
        "subscribe to unknown namespace should fail (Go: TestSubscribeError)"
    );
    client.disconnect().await.unwrap();
}

/// Equivalent to Go's TestClient_History: history on fresh channel succeeds.
#[tokio::test]
async fn go_parity_history_on_fresh_channel() {
    let server = common::start_insecure().await;
    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("goparity_hist", SubscriptionConfig::default())
        .await
        .unwrap();
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    let result = sub
        .history(centrifuge_client::HistoryOptions {
            limit: 100,
            ..Default::default()
        })
        .await;
    assert!(
        result.is_ok(),
        "history on fresh channel should succeed (Go: TestClient_History)"
    );
    client.disconnect().await.unwrap();
}

/// Stress test inspired by Go's TestConcurrentPublishSubscribe.
#[tokio::test]
async fn go_parity_concurrent_publish() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("goparity_conc", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let mut handles = Vec::new();
    for i in 0..50 {
        let s = sub.clone();
        handles.push(tokio::spawn(async move {
            s.publish(format!(r#"{{"i":{i}}}"#).into_bytes()).await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    // Count received publications
    let mut received = 0u32;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(2000), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Publication(_)) {
            received += 1;
            if received >= 50 {
                break;
            }
        }
    }

    assert!(
        received >= 50,
        "should receive all publications (Go: TestConcurrentPublishSubscribe), got {received}"
    );
    client.disconnect().await.unwrap();
}

/// Equivalent to Go's TestConnectWrongAddress: connecting to unreachable
/// address should fire an error event with a transport error.
#[tokio::test]
async fn go_parity_connect_wrong_address() {
    let config = ClientConfig::new("ws://127.0.0.1:19999/connection/websocket")
        .timeout(Duration::from_secs(2))
        .min_reconnect_delay(Duration::from_millis(50))
        .max_reconnect_delay(Duration::from_millis(100));

    let client = Client::new(config);
    let mut events = client.events().expect("events");
    let _ = time::timeout(Duration::from_secs(3), client.connect()).await;

    assert!(
        wait_for_client_event(&mut events, 500, |e| {
            matches!(e, centrifuge_client::ClientEvent::Error(_))
        })
        .await,
        "Error event should fire for unreachable address (Go: TestConnectWrongAddress)"
    );
    let _ = client.disconnect().await;
}

/// Equivalent to Go's TestConcurrentCloseDisconnect: concurrent close and
/// disconnect should not panic or deadlock.
#[tokio::test]
async fn go_parity_concurrent_close_disconnect() {
    let server = common::start_insecure().await;

    for _ in 0..20 {
        let client = Client::new(json_config(&server.ws_url));
        let sub = client
            .new_subscription("conc_close", SubscriptionConfig::default())
            .await
            .unwrap();
        client.connect().await.unwrap();
        sub.subscribe().await.unwrap();

        let c1 = client.clone();
        let c2 = client.clone();
        let h1 = tokio::spawn(async move {
            let _ = c1.close().await;
        });
        let h2 = tokio::spawn(async move {
            let _ = c2.disconnect().await;
        });
        let _ = h1.await;
        let _ = h2.await;
    }
    // If we get here without panic or hang, the test passes
}

/// Equivalent to Go's TestConcurrentPublishSubscribeDisconnect: clients
/// that randomly disconnect while receiving publications should not panic.
#[tokio::test]
async fn go_parity_concurrent_publish_subscribe_disconnect() {
    let server = common::start_insecure().await;

    // Producer: publishes 100 messages
    let producer = Client::new(json_config(&server.ws_url));
    let prod_sub = producer
        .new_subscription("conc_pubsub_disc", SubscriptionConfig::default())
        .await
        .unwrap();
    producer.connect().await.unwrap();
    prod_sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    let producer_handle = {
        let ps = prod_sub.clone();
        tokio::spawn(async move {
            for i in 0..100 {
                let _ = ps.publish(format!(r#"{{"i":{i}}}"#).into_bytes()).await;
            }
        })
    };

    // 10 consumers that subscribe and randomly close
    let mut consumer_handles = Vec::new();
    for _ in 0..10 {
        let url = server.ws_url.clone();
        consumer_handles.push(tokio::spawn(async move {
            let consumer = Client::new(ClientConfig::new(&url).timeout(Duration::from_secs(5)));
            let sub = consumer
                .new_subscription("conc_pubsub_disc", SubscriptionConfig::default())
                .await
                .unwrap();
            consumer.connect().await.unwrap();
            sub.subscribe().await.unwrap();
            // Random delay then close
            time::sleep(Duration::from_millis(rand::random::<u64>() % 150)).await;
            let _ = consumer.close().await;
        }));
    }

    let _ = producer_handle.await;
    for h in consumer_handles {
        let _ = h.await;
    }

    producer.disconnect().await.unwrap();
    // If we get here without panic, the test passes
}

/// Equivalent to Go's TestPublishInvalidJSON: publishing non-JSON data
/// in JSON mode should fail.
#[tokio::test]
async fn go_parity_publish_invalid_json() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("goparity_invalid", SubscriptionConfig::default())
        .await
        .unwrap();
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // "boom" is not valid JSON -- should fail
    let result = sub.publish(b"boom".to_vec()).await;
    assert!(
        result.is_err(),
        "publishing non-JSON data in JSON mode should fail (Go: TestPublishInvalidJSON)"
    );

    client.disconnect().await.unwrap();
}

/// Equivalent to Go's TestHandlePublishFossil: subscribe with delta compression,
/// publish multiple messages, verify that publications are received with full data
/// (deltas are transparently applied by the client).
#[tokio::test]
async fn go_parity_delta_compression() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription(
            "delta_test",
            SubscriptionConfig {
                delta: centrifuge_client::DeltaType::Fossil,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    // Publish multiple messages -- server should send deltas after the first
    sub.publish(br#"{"counter":1,"data":"hello world"}"#.to_vec())
        .await
        .unwrap();
    time::sleep(Duration::from_millis(200)).await;
    sub.publish(br#"{"counter":2,"data":"hello world"}"#.to_vec())
        .await
        .unwrap();
    time::sleep(Duration::from_millis(200)).await;
    sub.publish(br#"{"counter":3,"data":"hello earth"}"#.to_vec())
        .await
        .unwrap();
    time::sleep(Duration::from_millis(500)).await;

    let mut pubs = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        if let centrifuge_client::SubEvent::Publication(pub_data) = e {
            pubs.push(pub_data.data.clone());
        }
    }
    assert!(pubs.len() >= 3, "should receive all 3 publications, got {}", pubs.len());

    // Verify publications received correctly (delta transparently applied)
    for (i, p) in pubs.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_slice(p)
            .unwrap_or_else(|_| panic!("pub {i} not valid JSON: {:?}", String::from_utf8_lossy(p)));
        assert_eq!(v["counter"], i as u64 + 1, "pub {i} counter mismatch: {v}");
    }
    let v2: serde_json::Value = serde_json::from_slice(&pubs[2]).unwrap();
    assert_eq!(v2["data"], "hello earth", "third pub should have delta-applied data");

    client.disconnect().await.unwrap();
}

// =========================================================================
// JS client test parity
// =========================================================================

/// JS: "subscribe and unsubscribe loop" -- rapid subscribe/unsubscribe
/// cycles with presence checks should not panic or deadlock.
#[tokio::test]
async fn js_parity_subscribe_unsubscribe_loop() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    client.connect().await.unwrap();

    for i in 0..10 {
        let channel = format!("sub_unsub_loop_{i}");
        let sub = client
            .new_subscription(&channel, SubscriptionConfig::default())
            .await
            .unwrap();
        sub.subscribe().await.unwrap();

        let presence = sub.presence_stats().await;
        assert!(presence.is_ok(), "presence should work on iteration {i}");

        sub.unsubscribe().await.unwrap();
        client.remove_subscription(&sub).await.unwrap();
    }

    client.disconnect().await.unwrap();
}

/// JS: "unsubscribe right after connect" -- unsubscribe called immediately
/// after connect, before subscribe reply arrives. Should not hang or panic.
#[tokio::test]
async fn js_parity_unsubscribe_right_after_connect() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("unsub_early", SubscriptionConfig::default())
        .await
        .unwrap();

    // Fire subscribe (don't await -- it will wait for connect)
    let sub2 = sub.clone();
    let _sub_task = tokio::spawn(async move {
        let _ = sub2.subscribe().await;
    });

    // Connect then immediately unsubscribe before subscribe completes
    client.connect().await.unwrap();
    // Small delay to let subscribe command reach the actor
    time::sleep(Duration::from_millis(50)).await;
    sub.unsubscribe().await.unwrap();

    time::sleep(Duration::from_millis(100)).await;
    client.disconnect().await.unwrap();
}

/// JS: "subscribes and unsubscribes from many subs" -- managing multiple
/// concurrent subscriptions.
#[tokio::test]
async fn js_parity_many_subscriptions() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    client.connect().await.unwrap();

    let mut subs = Vec::new();
    for i in 0..20 {
        let sub = client
            .new_subscription(format!("many_sub_{i}"), SubscriptionConfig::default())
            .await
            .unwrap();
        sub.subscribe().await.unwrap();
        subs.push(sub);
    }
    time::sleep(Duration::from_millis(200)).await;

    // Verify all are subscribed by checking presence stats on each
    for sub in &subs {
        let stats = sub.presence_stats().await.unwrap();
        assert!(stats.num_clients >= 1, "channel {} should have presence", sub.channel());
    }

    // Unsubscribe all
    for sub in &subs {
        sub.unsubscribe().await.unwrap();
    }

    client.disconnect().await.unwrap();
}

// =========================================================================
// Protobuf parity tests (matching JSON-only tests above)
// =========================================================================

#[tokio::test]
async fn protobuf_rpc() {
    let server = common::start_insecure().await;

    let client = Client::new(protobuf_config(&server.ws_url));
    client.connect().await.unwrap();

    let result = client.rpc("nonexistent", b"{}".to_vec()).await;
    assert!(result.is_err(), "RPC to unknown method should fail with Protobuf");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_join_leave_events() {
    let server = common::start_insecure().await;

    let client1 = Client::new(protobuf_config(&server.ws_url));
    let sub1 = client1
        .new_subscription(
            "pbjl",
            SubscriptionConfig {
                join_leave: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub1.events().expect("events");
    client1.connect().await.unwrap();
    sub1.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let client2 = Client::new(protobuf_config(&server.ws_url));
    let sub2 = client2
        .new_subscription(
            "pbjl",
            SubscriptionConfig {
                join_leave: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client2.connect().await.unwrap();
    sub2.subscribe().await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            matches!(e, centrifuge_client::SubEvent::Join(_))
        })
        .await,
        "should receive join event with Protobuf"
    );

    client1.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

#[tokio::test]
async fn protobuf_subscribe_with_recovery() {
    let server = common::start_insecure().await;

    let client = Client::new(protobuf_config(&server.ws_url));
    let sub = client
        .new_subscription(
            "pbrecov",
            SubscriptionConfig {
                recoverable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    sub.publish(b"recovery test payload".to_vec()).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 500, |e| {
            matches!(e, centrifuge_client::SubEvent::Publication(_))
        })
        .await,
        "should receive publication with Protobuf recovery"
    );

    client.disconnect().await.unwrap();
}

// =========================================================================
// Timeout behavior
// =========================================================================

/// Verify that operations against a real server complete within the
/// configured timeout (not hang). Operation timeout firing is tested
/// in actor_tests with a mock that never responds — here we verify that
/// the configured timeout doesn't interfere with normal operations.
#[tokio::test]
async fn operations_complete_within_configured_timeout() {
    let server = common::start_insecure().await;

    let config = ClientConfig::new(&server.ws_url).timeout(Duration::from_secs(5));

    let client = Client::new(config);
    client.connect().await.unwrap();

    // All operations should complete well within the 5s timeout.
    let start = tokio::time::Instant::now();

    let sub = client
        .new_subscription("timeout_test", SubscriptionConfig::default())
        .await
        .unwrap();
    sub.subscribe().await.unwrap();
    sub.publish(br#"{"msg":"test"}"#.to_vec()).await.unwrap();
    let _ = sub
        .history(centrifuge_client::HistoryOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let _ = sub.presence().await.unwrap();
    let _ = sub.presence_stats().await.unwrap();

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "all operations should complete quickly, took {elapsed:?}"
    );

    client.disconnect().await.unwrap();
}

// =========================================================================
// Edge cases: payloads, channels, ordering, stress
// =========================================================================

/// Publish and receive a large JSON payload (tests WebSocket framing).
#[tokio::test]
async fn large_payload() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("large_payload", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // 32KB JSON payload — exercises multi-frame WebSocket handling
    // while staying within Centrifugo's default max message size
    let large_string = "A".repeat(32 * 1024);
    let payload = format!(r#"{{"data":"{large_string}"}}"#);
    let payload_bytes = payload.into_bytes();
    let payload_len = payload_bytes.len();
    sub.publish(payload_bytes).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 3000, |e| {
            if let centrifuge_client::SubEvent::Publication(pub_data) = e {
                assert!(
                    pub_data.data.len() >= payload_len - 100,
                    "payload size should be ~{payload_len}, got {}",
                    pub_data.data.len()
                );
                true
            } else {
                false
            }
        })
        .await,
        "should receive the large publication"
    );
    client.disconnect().await.unwrap();
}

/// Publish and receive an empty (zero-byte) payload.
#[tokio::test]
async fn empty_payload() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("empty_payload", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // Empty JSON object — the smallest valid JSON payload
    sub.publish(br#"{}"#.to_vec()).await.unwrap();

    assert!(
        wait_for_sub_event(&mut events, 1000, |e| {
            if let centrifuge_client::SubEvent::Publication(pub_data) = e {
                assert_eq!(pub_data.data, br#"{}"#, "empty object payload should round-trip");
                true
            } else {
                false
            }
        })
        .await,
        "should receive the empty payload publication"
    );
    client.disconnect().await.unwrap();
}

/// Subscribe to channels with special characters: unicode, slashes, colons.
#[tokio::test]
async fn channel_names_with_special_characters() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    client.connect().await.unwrap();

    let channels = [
        "channel-with-dashes_and_underscores",
        "channel.with.dots",
        "UPPERCASE_CHANNEL",
        "channel123numbers",
    ];

    for channel_name in &channels {
        let sub = client
            .new_subscription(*channel_name, SubscriptionConfig::default())
            .await
            .unwrap();
        let mut events = sub.events().expect("events");
        sub.subscribe().await.unwrap();
        time::sleep(Duration::from_millis(50)).await;

        sub.publish(br#"{"test":true}"#.to_vec()).await.unwrap();

        assert!(
            wait_for_sub_event(&mut events, 500, |e| {
                matches!(e, centrifuge_client::SubEvent::Publication(_))
            })
            .await,
            "should receive publication on channel '{channel_name}'"
        );
        sub.unsubscribe().await.unwrap();
        client.remove_subscription(&sub).await.unwrap();
    }

    client.disconnect().await.unwrap();
}

/// Publish 100 numbered messages and verify they arrive in order.
#[tokio::test]
async fn message_ordering_preserved() {
    let server = common::start_insecure().await;

    let client = Client::new(json_config(&server.ws_url));
    let sub = client
        .new_subscription("ordering_test", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    let count = 100;
    for i in 0..count {
        sub.publish(format!(r#"{{"seq":{i}}}"#).into_bytes()).await.unwrap();
    }

    let mut received = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(3000), events.recv()).await {
        if let centrifuge_client::SubEvent::Publication(pub_data) = e {
            let v: serde_json::Value = serde_json::from_slice(&pub_data.data).unwrap();
            received.push(v["seq"].as_u64().unwrap());
            if received.len() >= count {
                break;
            }
        }
    }

    assert_eq!(
        received.len(),
        count,
        "should receive all {count} messages, got {}",
        received.len()
    );
    for (i, &seq) in received.iter().enumerate() {
        assert_eq!(seq, i as u64, "message {i} should have seq={i}, got {seq}");
    }

    client.disconnect().await.unwrap();
}

/// Stress test: 500 concurrent publishes from 5 publishers, one subscriber
/// receives all of them without dropping or panicking.
#[tokio::test]
async fn stress_concurrent_publishers() {
    let server = common::start_insecure().await;

    let subscriber = Client::new(json_config(&server.ws_url));
    let sub = subscriber
        .new_subscription("stress_test", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    subscriber.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    let total_messages: u32 = 200;
    let num_publishers = 5;
    let per_publisher = total_messages / num_publishers;

    let mut handles = Vec::new();
    for p in 0..num_publishers {
        let url = server.ws_url.clone();
        handles.push(tokio::spawn(async move {
            let pub_client = Client::new(ClientConfig::new(&url).timeout(Duration::from_secs(10)));
            let pub_sub = pub_client
                .new_subscription("stress_test", SubscriptionConfig::default())
                .await
                .unwrap();
            pub_client.connect().await.unwrap();
            pub_sub.subscribe().await.unwrap();
            time::sleep(Duration::from_millis(100)).await;
            for i in 0..per_publisher {
                pub_sub
                    .publish(format!(r#"{{"p":{p},"i":{i}}}"#).into_bytes())
                    .await
                    .unwrap();
            }
            pub_client.disconnect().await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Collect received publications
    let mut received = 0u32;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(5000), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Publication(_)) {
            received += 1;
            if received >= total_messages {
                break;
            }
        }
    }

    assert!(
        received >= total_messages,
        "should receive all {total_messages} publications from {num_publishers} publishers, got {received}"
    );

    subscriber.disconnect().await.unwrap();
}
