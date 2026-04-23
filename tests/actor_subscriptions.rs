mod actor_helpers;
use actor_helpers::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::time;

use centrifuge_client::config::SubscriptionConfig;
use centrifuge_client::transport::{DisconnectInfo, TransportFrame};
use centrifuge_client::{CentrifugeError, Client};

// =========================================================================
// E. Client-Side Subscriptions
// =========================================================================

#[tokio::test]
async fn new_subscription_and_subscribe() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;
    let e = time::timeout(Duration::from_millis(200), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(e, centrifuge_client::SubEvent::Subscribing(_))
            || matches!(e, centrifuge_client::SubEvent::Subscribed(_))
    );
}

#[tokio::test]
async fn duplicate_subscription_error() {
    let (client, _conn, _) = make_client(default_config());
    client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    assert!(matches!(
        client.new_subscription("ch", SubscriptionConfig::default()).await,
        Err(CentrifugeError::DuplicateSubscription)
    ));
}

#[tokio::test]
async fn subscribe_with_recovery() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                recoverable: true,
                since: Some(centrifuge_client::StreamPosition {
                    offset: 42,
                    epoch: "epoch1".into(),
                }),
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
    assert!(cmd["subscribe"]["recover"].as_bool().unwrap());
    assert_eq!(cmd["subscribe"]["offset"], 42);
    assert_eq!(cmd["subscribe"]["epoch"], "epoch1");

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "subscribe": {
                "recoverable": true, "recovered": true, "was_recovering": true,
                "offset": 45, "epoch": "epoch1",
                "publications": [{"data": {"msg": "m1"}, "offset": 43}, {"data": {"msg": "m2"}, "offset": 45}]
            }
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsubscribe_sends_command() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.unsubscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert!(cmd.get("unsubscribe").is_some());
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "unsubscribe": {}}),
        )))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn subscribe_permanent_error_unsubscribes() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 103, "message": "permission denied", "temporary": false}
        }))))
        .await
        .unwrap();
    assert!(task.await.unwrap().is_err());
    time::sleep(Duration::from_millis(50)).await;
    let mut found_unsubscribed = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Unsubscribed(_)) {
            found_unsubscribed = true;
            break;
        }
    }
    assert!(found_unsubscribed);
}

#[tokio::test]
async fn get_subscription_exists_and_not_found() {
    let (client, _conn, _) = make_client(default_config());
    client
        .new_subscription("ch1", SubscriptionConfig::default())
        .await
        .unwrap();
    assert!(client.get_subscription("ch1").await.unwrap().is_some());
    assert!(client.get_subscription("ch2").await.unwrap().is_none());
}

#[tokio::test]
async fn remove_subscription_while_subscribed() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    client.remove_subscription(&sub).await.unwrap();
    let cmd = read_command(&mut conn).await;
    assert!(cmd.get("unsubscribe").is_some());
    assert!(client.get_subscription("ch").await.unwrap().is_none());
}

// Spec (client_sdk.md:218): "Subscription is automatically unsubscribed before
// being removed." Matches JS SDK centrifuge.ts:191-200 which calls
// sub.unsubscribe() (emitting Unsubscribed) before removing from the registry.
#[tokio::test]
async fn remove_subscription_while_subscribed_emits_unsubscribed() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    // Drain Subscribing + Subscribed from the subscribe flow.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(50);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(drain_deadline) => break,
            _ = events.recv() => {}
        }
    }

    client.remove_subscription(&sub).await.unwrap();
    let cmd = read_command(&mut conn).await;
    assert!(cmd.get("unsubscribe").is_some(), "wire unsubscribe must still be sent");

    let evt = time::timeout(Duration::from_millis(100), events.recv())
        .await
        .expect("Unsubscribed event must arrive before the sub's event channel is dropped")
        .expect("sub event channel must still be readable");
    match evt {
        centrifuge_client::SubEvent::Unsubscribed(ctx) => {
            assert_eq!(ctx.code, 0, "expected UNSUBSCRIBE_CALLED code (0)");
        }
        other => panic!("expected SubEvent::Unsubscribed, got {other:?}"),
    }

    assert!(client.get_subscription("ch").await.unwrap().is_none());
}

// If the sub is already Unsubscribed (user called unsubscribe() first), remove
// must not emit a second Unsubscribed event — matches JS's `if (state !==
// Unsubscribed) sub.unsubscribe()` guard.
#[tokio::test]
async fn remove_subscription_when_already_unsubscribed_is_silent() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    // Explicit unsubscribe — this emits one Unsubscribed event.
    sub.unsubscribe().await.unwrap();
    // Drain the wire unsubscribe command from the outgoing stream.
    let _ = read_command(&mut conn).await;

    // Drain all events from the unsubscribe (and the earlier subscribe flow).
    let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(50);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(drain_deadline) => break,
            _ = events.recv() => {}
        }
    }

    client.remove_subscription(&sub).await.unwrap();

    // No further Unsubscribed event — the sub was already in the Unsubscribed
    // state when remove was called.
    let evt = time::timeout(Duration::from_millis(50), events.recv()).await;
    match evt {
        Err(_) => {}                  // timeout, no event — expected
        Ok(None) => {}                // channel closed (sub dropped) — also fine
        Ok(Some(e)) => panic!("unexpected event after remove of already-unsubscribed sub: {e:?}"),
    }
    assert!(client.get_subscription("ch").await.unwrap().is_none());
}

// =========================================================================
// J. Subscribe before connect (queued subscriptions)
// =========================================================================

#[tokio::test]
async fn subscribe_before_connect_auto_subscribes() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {}}),
        )))
        .await
        .unwrap();

    sub_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;
    let mut found_subscribed = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Subscribed(_)) {
            found_subscribed = true;
            break;
        }
    }
    assert!(found_subscribed);
}

// =========================================================================
// K. Temporary subscribe error with backoff retry
// =========================================================================

#[tokio::test]
async fn subscribe_temporary_error_retries() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;

    let sub2 = sub.clone();
    let _sub_task = tokio::spawn(async move { sub2.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 50, "message": "try again", "temporary": true}
        }))))
        .await
        .unwrap();

    // Wait for resubscribe attempt
    time::sleep(Duration::from_millis(200)).await;
    let cmd2 = read_command(&mut conn).await;
    assert_eq!(cmd2["subscribe"]["channel"], "ch");
    let mut found_error = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Error(_)) {
            found_error = true;
            break;
        }
    }
    assert!(found_error);
}

// =========================================================================
// L. Token expired (109) on subscribe
// =========================================================================

#[tokio::test]
async fn subscribe_token_expired_refreshes_and_retries() {
    let token_called = Arc::new(AtomicU32::new(0));
    let tc = token_called.clone();
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                token: "old-token".into(),
                get_token: Some(Box::new(move |_channel| {
                    let tc = tc.clone();
                    Box::pin(async move {
                        tc.fetch_add(1, Ordering::Relaxed);
                        Ok("new-token".to_string())
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;

    let sub2 = sub.clone();
    let _sub_task = tokio::spawn(async move { sub2.subscribe().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["subscribe"]["token"], "old-token");
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 109, "message": "token expired"}
        }))))
        .await
        .unwrap();

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
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.publish(br#"{"msg":"hello"}"#.to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["publish"]["channel"], "ch");
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "publish": {}}),
        )))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn subscription_history() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move {
        s.history(centrifuge_client::HistoryOptions {
            limit: 5,
            ..Default::default()
        })
        .await
    });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "history": {"publications": [], "epoch": "e1", "offset": 0}
        }))))
        .await
        .unwrap();
    assert_eq!(task.await.unwrap().unwrap().epoch, "e1");
}

#[tokio::test]
async fn subscription_presence() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.presence().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "presence": {"presence": {"c1": {"user": "u1", "client": "c1"}}}
        }))))
        .await
        .unwrap();
    assert!(task.await.unwrap().unwrap().presence.contains_key("c1"));
}

#[tokio::test]
async fn subscription_presence_stats() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.presence_stats().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "presence_stats": {"num_clients": 3, "num_users": 2}
        }))))
        .await
        .unwrap();
    let r = task.await.unwrap().unwrap();
    assert_eq!(r.num_clients, 3);
    assert_eq!(r.num_users, 2);
}

#[tokio::test]
async fn subscription_history_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.history(centrifuge_client::HistoryOptions::default()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!(
            {"id": id, "error": {"code": 103, "message": "denied"}}
        ))))
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

#[tokio::test]
async fn subscription_presence_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.presence().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!(
            {"id": id, "error": {"code": 108, "message": "not available"}}
        ))))
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

#[tokio::test]
async fn subscription_presence_stats_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.presence_stats().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!(
            {"id": id, "error": {"code": 108, "message": "not available"}}
        ))))
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

#[tokio::test]
async fn subscription_publish_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    let s = sub.clone();
    let task = tokio::spawn(async move { s.publish(br#"{"x":1}"#.to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!(
            {"id": id, "error": {"code": 103, "message": "denied"}}
        ))))
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

// =========================================================================
// N. Disconnect with active subs and server subs
// =========================================================================

#[tokio::test]
async fn disconnect_unsubscribes_active_and_server_subs() {
    let (client, mut conn, _) = make_client(default_config());
    let mut client_events = client.events().expect("events");
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut sub_events = sub.events().expect("events");

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

    let mut found_sub_unsub = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), sub_events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Unsubscribed(_)) {
            found_sub_unsub = true;
            break;
        }
    }
    assert!(found_sub_unsub);

    let mut server_unsubs = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), client_events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerUnsubscribed(ctx) = e {
            server_unsubs.push(ctx.channel);
        }
    }
    assert!(server_unsubs.contains(&"notif".to_string()));
}

// =========================================================================
// Bug regression: subscription-related
// =========================================================================

/// Bug #1: subscribe() before connect() resolves when connection succeeds,
/// NOT when the subscription is actually confirmed by the server.
/// The subscribe() future MUST wait for the server's subscribe reply.
#[tokio::test]
async fn bug1_subscribe_before_connect_must_wait_for_server_confirm() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();

    // Subscribe BEFORE connect — starts the subscribe future
    let s = sub.clone();
    let sub_task = tokio::spawn(async move { s.subscribe().await });

    // Connect — complete the handshake
    let c = client.clone();
    let conn_task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut conn).await;
    conn_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // At this point the connection succeeded, but subscribe reply hasn't been sent.
    // The subscribe future MUST NOT have resolved yet.
    assert!(
        !sub_task.is_finished(),
        "subscribe() should NOT resolve before server confirms the subscription"
    );

    // Now feed the subscribe reply
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["subscribe"]["channel"], "ch");
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {}}),
        )))
        .await
        .unwrap();

    // NOW the subscribe future should resolve
    let result = time::timeout(Duration::from_secs(2), sub_task)
        .await
        .expect("subscribe task should complete")
        .expect("subscribe task should not panic");
    assert!(result.is_ok(), "subscribe should succeed after server confirms");
}

/// Bug #2: Operations (publish, history, etc.) on a connected client should
/// timeout if the server never responds, not hang forever.
#[tokio::test]
async fn bug2_operation_timeout_when_server_never_responds() {
    let config = centrifuge_client::config::ClientConfig {
        timeout: Duration::from_millis(500), // 500ms timeout
        ..default_config()
    };
    let (client, mut conn, _) = make_client(config);
    connect_client(&client, &mut conn).await;

    // Send a publish but never feed a reply
    let c = client.clone();
    let task = tokio::spawn(async move { c.publish("ch", br#"{"d":1}"#.to_vec()).await });

    // Read the command (so we know it was sent) but don't reply
    let _cmd = read_command(&mut conn).await;

    // The publish should timeout, not hang forever
    let result = time::timeout(Duration::from_secs(3), task)
        .await
        .expect("task should complete (not hang)")
        .expect("task should not panic");
    assert!(
        matches!(result, Err(CentrifugeError::Timeout)),
        "should timeout, got: {result:?}"
    );
}

/// Bug #6: When subscribe is queued (before connect) and the subscription
/// token callback fails during resubscribe, the subscribe_waiters must be
/// resolved with an error. Otherwise the caller hangs forever.
#[tokio::test]
async fn bug6_subscribe_waiter_resolved_on_token_failure_during_resubscribe() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                get_token: Some(Box::new(|_| Box::pin(async { Err(CentrifugeError::Unauthorized) }))),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Subscribe before connect — waiter will be stored
    let s = sub.clone();
    let sub_task = tokio::spawn(async move { s.subscribe().await });

    // Connect — triggers resubscribe_all -> do_subscribe -> get_token fails
    connect_client(&client, &mut conn).await;

    // The subscribe future must resolve with error, not hang
    let result = time::timeout(Duration::from_secs(2), sub_task)
        .await
        .expect("subscribe should not hang when token fails")
        .expect("should not panic");

    assert!(
        result.is_err(),
        "subscribe should fail when token callback returns unauthorized, got: {result:?}"
    );
}

// =========================================================================
// Coverage: subscribe permanent error drains waiters
// =========================================================================

/// subscriptions.rs:96-99 -- permanent subscribe error drains subscribe_waiters
#[tokio::test]
async fn subscribe_permanent_error_fails_waiters() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription_default("ch").await.unwrap();

    // Subscribe before connect -- waiter stored
    let s = sub.clone();
    let sub_task = tokio::spawn(async move { s.subscribe().await });

    // Connect
    let c = client.clone();
    let conn_task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut conn).await;
    conn_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // Reply with permanent error
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 200, "message": "permanent", "temporary": false}
        }))))
        .await
        .unwrap();

    let result = sub_task.await.unwrap();
    assert!(result.is_err());
}

// =========================================================================
// Coverage: do_subscribe token error retries
// =========================================================================

/// subscriptions.rs:134-156 -- do_subscribe token generic error triggers retry.
/// This path is hit during resubscribe_all after reconnect.
#[tokio::test]
async fn do_subscribe_token_error_retries() {
    let call_count = Arc::new(AtomicU32::new(0));
    let cc = call_count.clone();

    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = centrifuge_client::Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    // Create sub with get_token that always fails
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                get_token: Some(Box::new(move |_| {
                    let cc = cc.clone();
                    Box::pin(async move {
                        cc.fetch_add(1, Ordering::Relaxed);
                        Err(CentrifugeError::Transport("network error".into()))
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    // Connect and subscribe (with empty token, server accepts)
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let mut first = MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    };
    do_connect(&mut first).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Subscribe -- uses command handler path (no get_token call since token is empty string)
    subscribe_sub(&sub, &mut first).await;

    // Trigger reconnect -- sub goes to Subscribing, then resubscribe_all calls do_subscribe
    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    // Reconnect
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(20)).await;

    // do_subscribe sees empty token + get_token -> calls get_token -> error -> retries
    time::sleep(Duration::from_millis(500)).await;
    assert!(
        call_count.load(Ordering::Relaxed) >= 1,
        "get_token should be called during resubscribe"
    );
    let mut found_error = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Error(_)) {
            found_error = true;
            break;
        }
    }
    assert!(found_error, "Error event should fire on token fetch failure");
}

// =========================================================================
// Coverage: subscribe/unsubscribe edge cases
// =========================================================================

/// commands.rs:120 -- subscribe on unregistered channel
#[tokio::test]
async fn subscribe_already_subscribed_returns_ok() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription_default("ch").await.unwrap();
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    // Subscribe again while already subscribed -- should return Ok immediately
    sub.subscribe().await.unwrap();
}

/// commands.rs:158,161 -- unsubscribe on non-subscribed channel
#[tokio::test]
async fn unsubscribe_when_not_subscribed_returns_ok() {
    let (client, _conn, _) = make_client(default_config());
    let sub = client.new_subscription_default("ch").await.unwrap();
    // Unsubscribe without ever subscribing -- should be Ok
    sub.unsubscribe().await.unwrap();
}

// =========================================================================
// Coverage: close/disconnect drains subscribe waiters
// =========================================================================

/// state.rs:115 -- close drains subscribe_waiters
#[tokio::test]
async fn close_drains_subscribe_waiters() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription_default("ch").await.unwrap();

    let s = sub.clone();
    let sub_task = tokio::spawn(async move { s.subscribe().await });

    let c = client.clone();
    let conn_task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut conn).await;
    conn_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    let _cmd = read_command(&mut conn).await;
    client.close().await.unwrap();

    let result = sub_task.await.unwrap();
    assert!(result.is_err());
}

/// state.rs:72 -- disconnect drains subscribe_waiters on subscribing subs
#[tokio::test]
async fn disconnect_drains_subscribe_waiters() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client.new_subscription_default("ch").await.unwrap();

    // Subscribe before connect
    let s = sub.clone();
    let sub_task = tokio::spawn(async move { s.subscribe().await });

    let c = client.clone();
    let conn_task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut conn).await;
    conn_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // Don't reply to subscribe -- leave it in-flight, then disconnect
    let _cmd = read_command(&mut conn).await;
    client.disconnect().await.unwrap();

    // The subscribe waiter should be failed
    let result = sub_task.await.unwrap();
    assert!(result.is_err());
}

// =========================================================================
// Coverage: close with active subscription
// =========================================================================

#[tokio::test]
async fn close_with_active_subscription_unsubscribes() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    client.close().await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::SubEvent::Unsubscribed(ctx) = e
            && ctx.reason == "client closed"
        {
            found = true;
            break;
        }
    }
    assert!(found);
}

// =========================================================================
// Coverage: transport close emits server subscribing
// =========================================================================

#[tokio::test]
async fn transport_close_emits_server_subscribing() {
    let (transport, conn) = MockTransport::new();
    let _conn2 = transport.add_connection();
    let client = centrifuge_client::Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut MockConnection {
        incoming_tx: conn.incoming_tx.clone(),
        outgoing_rx: conn.outgoing_rx,
    })
    .await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"notif": {}}}
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // Transport close -> should emit ServerSubscribing
    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    time::sleep(Duration::from_millis(200)).await;
    let mut server_subscribing = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerSubscribing(ctx) = e {
            server_subscribing.push(ctx.channel);
        }
    }
    assert!(server_subscribing.contains(&"notif".to_string()));
}

// =========================================================================
// Coverage: subscription channel() getter
// =========================================================================

#[tokio::test]
async fn subscription_channel_getter() {
    let (client, _conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("my-channel", SubscriptionConfig::default())
        .await
        .unwrap();
    assert_eq!(sub.channel(), "my-channel");
}

// =========================================================================
// Stream-based subscription events
// =========================================================================

#[tokio::test]
async fn stream_api_subscribe_receive_publication() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    // Use the stream API -- spawn to avoid deadlock
    let c = client.clone();
    let sub_task = tokio::spawn(async move { c.subscribe("ch").await });

    // Complete the subscribe handshake
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "subscribe": {}}),
        )))
        .await
        .unwrap();

    let (_sub, mut events) = sub_task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // Push a publication
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"channel": "ch", "pub": {"data": {"msg": "via stream"}}}
        }))))
        .await
        .unwrap();

    // Receive via stream — drain non-publication events first
    loop {
        let event = time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("should receive event")
            .expect("channel not closed");
        if let centrifuge_client::SubEvent::Publication(pub_data) = event {
            assert!(!pub_data.data.is_empty());
            break;
        }
    }
}

// =========================================================================
// Coverage: recovered publications
// =========================================================================

/// subscriptions.rs:53-57 -- recovered publications with delta applied
#[tokio::test]
async fn subscribe_with_recovered_publications() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription(
            "ch",
            SubscriptionConfig {
                recoverable: true,
                since: Some(centrifuge_client::StreamPosition {
                    offset: 1,
                    epoch: "e1".into(),
                }),
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

    // Reply with recovered publications
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "subscribe": {
                "recoverable": true, "recovered": true,
                "offset": 3, "epoch": "e1",
                "publications": [
                    {"data": {"msg": "recovered1"}, "offset": 2},
                    {"data": {"msg": "recovered2"}, "offset": 3}
                ]
            }
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    let mut pub_count = 0;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Publication(_)) {
            pub_count += 1;
        }
    }
    assert_eq!(pub_count, 2);
}

// =========================================================================
// Unsubscribe send error triggers reconnect
// =========================================================================

#[tokio::test]
async fn unsubscribe_send_error_triggers_reconnect() {
    let (transport, mut conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    connect_client(&client, &mut conn).await;

    let sub = client
        .new_subscription("unsub_err", SubscriptionConfig::default())
        .await
        .unwrap();
    subscribe_sub(&sub, &mut conn).await;

    // Drop the transport sink by closing the incoming channel — next send will fail
    drop(conn.incoming_tx);
    time::sleep(Duration::from_millis(50)).await;

    // Unsubscribe will try to send the command and fail — should trigger reconnect
    sub.unsubscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;

    // Should reconnect on the second connection
    let cmd = time::timeout(Duration::from_secs(2), read_command(&mut conn2)).await;
    assert!(cmd.is_ok(), "should reconnect after unsubscribe send error");

    client.disconnect().await.unwrap();
}
