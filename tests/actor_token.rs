mod actor_helpers;
use actor_helpers::*;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time;

use centrifuge_client::CentrifugeError;
use centrifuge_client::config::{ClientConfig, SubscriptionConfig};
use centrifuge_client::transport::TransportFrame;

// =========================================================================
// S. Connection token refresh during connected state
// =========================================================================

#[tokio::test]
async fn connection_token_refresh_during_connected() {
    let refresh_called = Arc::new(AtomicU32::new(0));
    let rc = refresh_called.clone();
    let config = ClientConfig {
        get_token: Some(Arc::new(move || {
            let rc = rc.clone();
            Box::pin(async move {
                rc.fetch_add(1, Ordering::Relaxed);
                Ok("refreshed".to_string())
            })
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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "refresh": {"expires": false}}),
        )))
        .await
        .unwrap();
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
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                token: "sub-tok".into(),
                get_token: Some(Arc::new(move |_| {
                    let tc = tc2.clone();
                    Box::pin(async move {
                        tc.fetch_add(1, Ordering::Relaxed);
                        Ok("new-sub-tok".into())
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {"expires": true, "ttl": 1}}),
        )))
        .await
        .unwrap();
    task.await.unwrap().unwrap();

    time::sleep(Duration::from_millis(1200)).await;
    let cmd = read_command(&mut conn).await;
    assert!(cmd.get("sub_refresh").is_some());
    assert_eq!(cmd["sub_refresh"]["channel"], "ch");
    assert_eq!(cmd["sub_refresh"]["token"], "new-sub-tok");
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "sub_refresh": {"expires": false}}),
        )))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(50)).await;
    assert!(tc.load(Ordering::Relaxed) >= 1);
}

// =========================================================================
// Coverage: token refresh error paths
// =========================================================================

/// Connection token refresh returns Unauthorized during connected state -> disconnect
#[tokio::test]
async fn token_refresh_unauthorized_during_connected_disconnects() {
    let config = ClientConfig {
        token: "initial".into(),
        get_token: Some(Arc::new(|| {
            // Always returns unauthorized -- called only during refresh (not connect, since token is set)
            Box::pin(async { Err(CentrifugeError::Unauthorized) })
        })),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    let mut events = client.events().expect("events");

    // Connect with expires=true, ttl=1
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "expires": true, "ttl": 1}
    })))).await.unwrap();
    task.await.unwrap().unwrap();

    // Wait for refresh (at 90% of 1s = 0.9s)
    time::sleep(Duration::from_millis(1200)).await;

    // The unauthorized refresh should cause disconnect
    let mut found_disconnected = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Disconnected(_)) {
            found_disconnected = true;
            break;
        }
    }
    assert!(found_disconnected);
}

/// Connection token refresh returns generic error -> retries
#[tokio::test]
async fn token_refresh_generic_error_retries() {
    let config = ClientConfig {
        token: "initial".into(),
        get_token: Some(Arc::new(|| {
            Box::pin(async { Err(CentrifugeError::Transport("network down".into())) })
        })),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "expires": true, "ttl": 1}
    })))).await.unwrap();
    task.await.unwrap().unwrap();

    time::sleep(Duration::from_millis(1200)).await;
    let mut found_error = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Error(_)) {
            found_error = true;
            break;
        }
    }
    assert!(found_error, "Error event should fire on token refresh failure");
}

// =========================================================================
// Sub token refresh error paths
// =========================================================================

/// Sub token refresh returns empty string -> unsubscribe
#[tokio::test]
async fn sub_token_refresh_empty_unsubscribes() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                token: "sub-tok".into(),
                get_token: Some(Arc::new(|_| {
                    Box::pin(async { Ok(String::new()) }) // always empty -> unauthorized
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;

    // Subscribe with expires=true, ttl=1
    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {"expires": true, "ttl": 1}}),
        )))
        .await
        .unwrap();
    task.await.unwrap().unwrap();

    time::sleep(Duration::from_millis(1200)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Unsubscribed(_)) {
            found = true;
            break;
        }
    }
    assert!(found, "should unsubscribe when sub token refresh returns empty");
}

/// Sub token refresh returns Unauthorized -> unsubscribe
#[tokio::test]
async fn sub_token_refresh_unauthorized_unsubscribes() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                token: "sub-tok".into(),
                get_token: Some(Arc::new(|_| Box::pin(async { Err(CentrifugeError::Unauthorized) }))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {"expires": true, "ttl": 1}}),
        )))
        .await
        .unwrap();
    task.await.unwrap().unwrap();

    time::sleep(Duration::from_millis(1200)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Unsubscribed(_)) {
            found = true;
            break;
        }
    }
    assert!(found);
}

/// Sub token refresh returns generic error -> on_error + retry
#[tokio::test]
async fn sub_token_refresh_error_retries() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                token: "sub-tok".into(),
                get_token: Some(Arc::new(|_| {
                    Box::pin(async { Err(CentrifugeError::Transport("fail".into())) })
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {"expires": true, "ttl": 1}}),
        )))
        .await
        .unwrap();
    task.await.unwrap().unwrap();

    time::sleep(Duration::from_millis(1200)).await;
    let mut found_error = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Error(_)) {
            found_error = true;
            break;
        }
    }
    assert!(found_error, "Error event should fire on sub token refresh failure");
}

// =========================================================================
// Bug regression: token refresh timing
// =========================================================================

/// Bug #3: Token refresh should happen BEFORE the TTL expires, not at
/// exactly the TTL. We should schedule at ~90% of TTL.
#[tokio::test]
async fn bug3_token_refresh_before_ttl_expires() {
    let refresh_time = Arc::new(Mutex::new(None::<std::time::Instant>));
    let rt = refresh_time.clone();

    let config = ClientConfig {
        get_token: Some(Arc::new(move || {
            let rt = rt.clone();
            Box::pin(async move {
                *rt.lock().unwrap() = Some(std::time::Instant::now());
                Ok("refreshed".to_string())
            })
        })),
        token: "initial".into(),
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);

    // Connect with expires=true, ttl=2 (2 seconds)
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let connect_time = std::time::Instant::now();
    conn.incoming_tx.send(TransportFrame::Data(encode_reply(&serde_json::json!({
        "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "expires": true, "ttl": 2}
    })))).await.unwrap();
    task.await.unwrap().unwrap();

    // Wait for the refresh command (should arrive BEFORE 2s)
    let cmd = time::timeout(Duration::from_secs(3), read_command(&mut conn))
        .await
        .expect("should get refresh command");
    assert!(cmd.get("refresh").is_some());

    let refresh_at = refresh_time.lock().unwrap().unwrap();
    let elapsed = refresh_at.duration_since(connect_time);

    // The refresh should happen before the full TTL (2s).
    // With 90% scheduling, it should be around 1.8s.
    assert!(
        elapsed < Duration::from_secs(2),
        "token refresh should happen BEFORE TTL expires, but happened at {elapsed:?}"
    );
    // Should be at least 50% into the TTL (not immediately)
    assert!(
        elapsed > Duration::from_millis(500),
        "token refresh should not happen immediately, happened at {elapsed:?}"
    );

    client.disconnect().await.unwrap();
}

// =========================================================================
// Edge case: concurrent connection + subscription token refresh
// =========================================================================

#[tokio::test]
async fn concurrent_connection_and_subscription_token_refresh() {
    use centrifuge_client::config::{get_sub_token_fn, get_token_fn};

    let conn_refresh_count = Arc::new(AtomicU32::new(0));
    let sub_refresh_count = Arc::new(AtomicU32::new(0));

    let crc = conn_refresh_count.clone();
    let src = sub_refresh_count.clone();

    let config = ClientConfig {
        timeout: Duration::from_secs(2),
        min_reconnect_delay: Duration::from_millis(10),
        max_reconnect_delay: Duration::from_millis(50),
        get_token: Some(get_token_fn(move || {
            let crc = crc.clone();
            async move {
                crc.fetch_add(1, Ordering::Relaxed);
                Ok("refreshed-conn-token".into())
            }
        })),
        ..ClientConfig::new("ws://test")
    };

    let (client, mut conn, _transport) = make_client(config);

    // Connect with token_expires + short ttl so the actor schedules a refresh
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let reply = serde_json::json!({
        "id": id,
        "connect": {
            "client": "test-client", "version": "1.0.0",
            "ping": 25, "pong": true,
            "expires": true, "ttl": 1
        }
    });
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Subscribe with get_token callback and short ttl
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig::default().get_token(get_sub_token_fn(move |_channel| {
                let src = src.clone();
                async move {
                    src.fetch_add(1, Ordering::Relaxed);
                    Ok("refreshed-sub-token".into())
                }
            })),
        )
        .await
        .unwrap();

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let reply = serde_json::json!({"id": id, "subscribe": {"expires": true, "ttl": 1}});
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    task.await.unwrap().unwrap();

    // Wait for both refresh timers to fire (ttl=1 → 900ms delay)
    time::sleep(Duration::from_millis(1100)).await;

    // Connection token refresh: actor calls get_token then sends refresh command
    let cmd = read_command(&mut conn).await;
    assert!(
        cmd.get("refresh").is_some() || cmd.get("sub_refresh").is_some(),
        "expected refresh or sub_refresh command, got: {cmd}"
    );
    let id = cmd["id"].as_u64().unwrap() as u32;
    if cmd.get("refresh").is_some() {
        conn.incoming_tx
            .send(TransportFrame::Data(encode_reply(&serde_json::json!(
                {"id": id, "refresh": {"expires": true, "ttl": 300}}
            ))))
            .await
            .unwrap();
    } else {
        conn.incoming_tx
            .send(TransportFrame::Data(encode_reply(&serde_json::json!(
                {"id": id, "sub_refresh": {"expires": true, "ttl": 300}}
            ))))
            .await
            .unwrap();
    }

    // Second refresh command
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    if cmd.get("refresh").is_some() {
        conn.incoming_tx
            .send(TransportFrame::Data(encode_reply(&serde_json::json!(
                {"id": id, "refresh": {"expires": true, "ttl": 300}}
            ))))
            .await
            .unwrap();
    } else {
        conn.incoming_tx
            .send(TransportFrame::Data(encode_reply(&serde_json::json!(
                {"id": id, "sub_refresh": {"expires": true, "ttl": 300}}
            ))))
            .await
            .unwrap();
    }

    // Both callbacks should have been invoked
    assert!(
        conn_refresh_count.load(Ordering::Relaxed) >= 1,
        "connection token refresh should fire"
    );
    assert!(
        sub_refresh_count.load(Ordering::Relaxed) >= 1,
        "subscription token refresh should fire"
    );

    client.disconnect().await.unwrap();
}
