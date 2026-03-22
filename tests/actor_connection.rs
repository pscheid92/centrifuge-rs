mod actor_helpers;
use actor_helpers::*;

use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time;

use centrifuge_client::config::ClientConfig;
use centrifuge_client::transport::{DisconnectInfo, TransportFrame};
use centrifuge_client::{CentrifugeError, Client};

// =========================================================================
// A. Connection Lifecycle
// =========================================================================

#[tokio::test]
async fn connect_happy_path() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;
    // Drain Connecting event
    let e1 = time::timeout(Duration::from_millis(200), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(e1, centrifuge_client::ClientEvent::Connecting(_)));
    // Connected event
    let e2 = time::timeout(Duration::from_millis(200), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(e2, centrifuge_client::ClientEvent::Connected(_)));
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
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;
    // Drain Connecting + Connected
    let _ = time::timeout(Duration::from_millis(200), events.recv()).await;
    let _ = time::timeout(Duration::from_millis(200), events.recv()).await;

    client.disconnect().await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
    let e = time::timeout(Duration::from_millis(200), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(e, centrifuge_client::ClientEvent::Disconnected(_)));
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
    let cmd = read_command(&mut MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    })
    .await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 100, "message": "internal error", "temporary": true}
        }))))
        .await
        .unwrap();

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
            Box::pin(async move {
                *r.lock().unwrap() = true;
                Ok("new-token".into())
            })
        })),
        ..default_config()
    };
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(config, Box::new(ArcTransport(transport)));
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });

    // First: return token expired
    let cmd = read_command(&mut MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    })
    .await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 109, "message": "token expired"}
        }))))
        .await
        .unwrap();

    // Second: succeed
    do_connect(&mut conn2).await;
    task.await.unwrap().unwrap();
    assert!(*refreshed.lock().unwrap());
}

#[tokio::test]
async fn connect_unauthorized_token_disconnects() {
    let config = ClientConfig {
        get_token: Some(Box::new(|| Box::pin(async { Err(CentrifugeError::Unauthorized) }))),
        token: String::new(),
        ..default_config()
    };
    let (client, _conn, _) = make_client(config);
    let mut events = client.events().expect("events");
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    // Should fail with disconnected
    let result = task.await.unwrap();
    assert!(result.is_err());
    time::sleep(Duration::from_millis(50)).await;
    // Drain events to find Disconnected
    let mut found_disconnected = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Disconnected(_)) {
            found_disconnected = true;
            break;
        }
    }
    assert!(found_disconnected);
}

// =========================================================================
// B. Transport Reconnection
// =========================================================================

#[tokio::test]
async fn reconnect_on_transport_close() {
    let (transport, conn_inner) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));
    let mut events = client.events().expect("events");

    // Connect
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut MockConnection {
        incoming_tx: conn_inner.incoming_tx.clone(),
        outgoing_rx: conn_inner.outgoing_rx,
    })
    .await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Close with reconnect
    conn_inner
        .incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    // Should reconnect
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;

    // Count Connecting events
    let mut connecting_count = 0u32;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Connecting(_)) {
            connecting_count += 1;
        }
    }
    assert!(connecting_count >= 2);
}

#[tokio::test]
async fn no_reconnect_on_terminal_close() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3500,
            reason: "banned".into(),
            reconnect: false,
        })))
        .await
        .unwrap();

    time::sleep(Duration::from_millis(100)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::Disconnected(ctx) = e
            && ctx.code == 3500
        {
            found = true;
            break;
        }
    }
    assert!(found);
}

#[tokio::test]
async fn reconnect_on_stream_end() {
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    })
    .await;
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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({}))))
        .await
        .unwrap();

    // Read pong — should be an empty command (or have all-null fields)
    let pong = read_command(&mut conn).await;
    // In proto serde mode, null fields are present but null
    let is_empty = pong.get("connect").is_none() || pong["connect"].is_null();
    assert!(is_empty, "pong should not have a connect request, got: {pong}");
}

#[tokio::test]
async fn server_ping_no_pong_when_disabled() {
    let (client, mut conn, _) = make_client(default_config());
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });

    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let reply =
        serde_json::json!({"id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": false}});
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Server ping
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({}))))
        .await
        .unwrap();

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
    let cmd = read_command(&mut MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    })
    .await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    // Short ping interval (1s) + 50ms delay = 1.05s timeout
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "connect": {"client": "test", "version": "1.0", "ping": 1, "pong": true}
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();

    // Wait for ping timeout
    time::sleep(Duration::from_millis(1200)).await;
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;
}

// =========================================================================
// H. Reconnect + Resubscribe
// =========================================================================

#[tokio::test]
async fn reconnect_resubscribes_active_subscriptions() {
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    let sub = client
        .new_subscription("ch", centrifuge_client::config::SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    // Connect
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let mut first = MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    };
    do_connect(&mut first).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Subscribe
    subscribe_sub(&sub, &mut first).await;

    // Trigger reconnect
    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    // Second connect
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;

    // Read and reply to resubscribe
    let cmd = read_command(&mut conn2).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["subscribe"]["channel"], "ch");
    conn2
        .incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {}}),
        )))
        .await
        .unwrap();

    time::sleep(Duration::from_millis(100)).await;
    let mut subscribed_count = 0u32;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Subscribed(_)) {
            subscribed_count += 1;
        }
    }
    assert_eq!(subscribed_count, 2);
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
// Bug regression: connection-related
// =========================================================================

/// Bug #4: When transport closes while a subscribe is in-flight (sent but not
/// yet confirmed), subscribe_waiters must be failed. Previously only
/// Subscribed subs were transitioned, leaving Subscribing subs' waiters hanging.
#[tokio::test]
async fn bug4_transport_close_while_subscribe_inflight() {
    let (transport, conn) = MockTransport::new();
    let _conn2 = transport.add_connection(); // for reconnect, but we don't care

    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));
    let sub = client
        .new_subscription("ch", centrifuge_client::config::SubscriptionConfig::default())
        .await
        .unwrap();

    // Connect
    let c = client.clone();
    let conn_task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    })
    .await;
    conn_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Start subscribe but DON'T reply — leave it in-flight
    let s = sub.clone();
    let sub_task = tokio::spawn(async move { s.subscribe().await });
    time::sleep(Duration::from_millis(50)).await;

    // Now kill the transport — subscribe is still in-flight
    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    // The subscribe future MUST NOT hang — it should fail with an error
    let result = time::timeout(Duration::from_secs(2), sub_task)
        .await
        .expect("subscribe task should not hang after transport close")
        .expect("subscribe task should not panic");

    assert!(
        result.is_err(),
        "subscribe should fail when transport closes mid-flight, got: {result:?}"
    );
}

/// Bug #5: Subscribing subs (not yet confirmed) must also have their
/// subscribe_waiters failed on disconnect (not just Subscribed subs).
#[tokio::test]
async fn bug5_disconnect_fails_subscribing_sub_waiters() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", centrifuge_client::config::SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;

    // Start subscribe but DON'T reply
    let s = sub.clone();
    let sub_task = tokio::spawn(async move { s.subscribe().await });
    time::sleep(Duration::from_millis(50)).await;

    // Disconnect while subscribe is in-flight
    client.disconnect().await.unwrap();

    // The subscribe future must resolve with an error, not hang
    let result = time::timeout(Duration::from_secs(2), sub_task)
        .await
        .expect("subscribe should not hang after disconnect")
        .expect("should not panic");

    assert!(result.is_err(), "subscribe should fail on disconnect, got: {result:?}");
}

// =========================================================================
// Coverage: connect while already connecting
// =========================================================================

/// commands.rs:19-20 -- connect while already connecting adds to waiters
#[tokio::test]
async fn connect_while_connecting_queues_waiter() {
    let transport = Arc::new(MockTransport {
        connections: Mutex::new(vec![]),
        connect_errors: Mutex::new(vec!["refused".into()]),
        connect_count: AtomicU32::new(0),
    });
    let mut conn = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    // First connect enters Connecting state
    let c1 = client.clone();
    let t1 = tokio::spawn(async move { c1.connect().await });
    // Second connect while still connecting -- should queue
    let c2 = client.clone();
    let t2 = tokio::spawn(async move { c2.connect().await });

    // Complete connection
    time::sleep(Duration::from_millis(100)).await;
    do_connect(&mut conn).await;

    // Both should resolve successfully
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();
}

// =========================================================================
// Coverage: drop all handles during backoff
// =========================================================================

/// connect.rs:43-44 -- all handles dropped during backoff closes actor
#[tokio::test]
async fn drop_all_handles_during_backoff_closes() {
    let transport = Arc::new(MockTransport {
        connections: Mutex::new(vec![]),
        connect_errors: Mutex::new(vec!["refused".into(), "refused".into(), "refused".into()]),
        connect_count: AtomicU32::new(0),
    });
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));
    let c = client.clone();
    let _task = tokio::spawn(async move { c.connect().await });
    time::sleep(Duration::from_millis(50)).await;

    // Drop all handles -- actor should close
    drop(client);
    time::sleep(Duration::from_millis(100)).await;
    // If we get here without hanging, the actor shut down
}

// =========================================================================
// getData callback on connect
// =========================================================================

#[tokio::test]
async fn get_data_callback_invoked_on_connect() {
    let data_called = Arc::new(Mutex::new(false));
    let dc = data_called.clone();

    let config = ClientConfig {
        get_data: Some(centrifuge_client::get_data_fn(move || {
            let dc = dc.clone();
            async move {
                *dc.lock().unwrap() = true;
                Ok(br#"{"fresh":"data"}"#.to_vec())
            }
        })),
        ..default_config()
    };

    let (client, mut conn, _) = make_client(config);

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });

    // Read connect command — should have fresh data from callback
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let connect_data = &cmd["connect"]["data"];
    assert_eq!(
        connect_data["fresh"], "data",
        "connect should use data from get_data callback"
    );

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "connect": {"client": "c1", "version": "1.0.0", "ping": 25, "pong": true}
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();

    assert!(
        *data_called.lock().unwrap(),
        "get_data callback should have been called"
    );
    client.disconnect().await.unwrap();
}

// =========================================================================
// Permanent server error during connect → terminal disconnect
// =========================================================================

#[tokio::test]
async fn connect_permanent_server_error_disconnects() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });

    // Return a permanent (non-temporary) server error
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 103, "message": "bad request", "temporary": false}
        }))))
        .await
        .unwrap();

    // Should fail (not retry forever)
    let result = task.await.unwrap();
    assert!(result.is_err());

    // Should get Disconnected event (not Connecting for retry)
    time::sleep(Duration::from_millis(50)).await;
    let mut disconnected = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Disconnected(_)) {
            disconnected = true;
            break;
        }
    }
    assert!(disconnected, "permanent server error should cause terminal disconnect");
}
