mod actor_helpers;
use actor_helpers::*;

use std::time::Duration;

use tokio::time;

use centrifuge_client::Client;
use centrifuge_client::config::SubscriptionConfig;
use centrifuge_client::transport::{DisconnectInfo, TransportFrame};

// =========================================================================
// F. Server-Side Subscriptions
// =========================================================================

#[tokio::test]
async fn server_subscribe_on_connect() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    let mut channels = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerSubscribed(ctx) = e {
            channels.push(ctx.channel);
        }
    }
    assert!(channels.contains(&"notif".to_string()));
    assert!(channels.contains(&"updates".to_string()));
}

#[tokio::test]
async fn server_sub_disappears_on_reconnect() {
    let (transport, conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));
    let mut events = client.events().expect("events");

    // First connect with server sub
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
            "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true,
                "subs": {"notif": {"recoverable": true, "offset": 5, "epoch": "e1"}}
            }
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    // Trigger reconnect
    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    // Second connect without server sub
    let cmd2 = read_command(&mut conn2).await;
    let id2 = cmd2["id"].as_u64().unwrap() as u32;
    // Verify recovery info was sent
    assert!(cmd2["connect"]["subs"]["notif"]["recover"].as_bool().unwrap_or(false));
    conn2
        .incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id2, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {}}
        }))))
        .await
        .unwrap();

    time::sleep(Duration::from_millis(100)).await;
    let mut unsubs = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerUnsubscribed(ctx) = e {
            unsubs.push(ctx.channel);
        }
    }
    assert!(unsubs.contains(&"notif".to_string()));
}

// =========================================================================
// G. Push Message Handling
// =========================================================================

#[tokio::test]
async fn publication_to_client_sub() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"channel": "ch", "pub": {"data": {"msg": "hello"}, "offset": 1}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut pub_count = 0;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Publication(_)) {
            pub_count += 1;
        }
    }
    assert_eq!(pub_count, 1);
}

#[tokio::test]
async fn publication_to_server_sub() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"updates": {}}}
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"channel": "updates", "pub": {"data": {"msg": "world"}, "offset": 1}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerPublication(ctx) = e
            && ctx.channel == "updates"
        {
            found = true;
            break;
        }
    }
    assert!(found);
}

#[tokio::test]
async fn join_and_leave_pushes() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"push": {"channel": "ch", "join": {"info": {"client": "c1", "user": "u1"}}}}),
        )))
        .await
        .unwrap();
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"push": {"channel": "ch", "leave": {"info": {"client": "c2", "user": "u2"}}}}),
        )))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut join_count = 0;
    let mut leave_count = 0;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        match e {
            centrifuge_client::SubEvent::Join(_) => join_count += 1,
            centrifuge_client::SubEvent::Leave(_) => leave_count += 1,
            _ => {}
        }
    }
    assert_eq!(join_count, 1);
    assert_eq!(leave_count, 1);
}

#[tokio::test]
async fn disconnect_push_reconnectable() {
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

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"disconnect": {"code": 3001, "reason": "restart", "reconnect": true}}
        }))))
        .await
        .unwrap();
    do_connect(&mut conn2).await;
    time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn disconnect_push_terminal() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"disconnect": {"code": 3500, "reason": "banned", "reconnect": false}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Disconnected(_)) {
            found = true;
            break;
        }
    }
    assert!(found);
}

// Terminal disconnect code (3500) with reconnect=true set on the push must still
// be treated as terminal: the Centrifuge spec and the Go/JS SDKs decide reconnect
// purely from the code range, ignoring the proto `reconnect` field on pushes.
#[tokio::test]
async fn disconnect_push_terminal_code_wins_over_reconnect_flag() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;

    // Drain the startup Connecting/Connected events.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(50);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(drain_deadline) => break,
            _ = events.recv() => {}
        }
    }

    // Push a Disconnect with terminal code 3500 but reconnect=true on the proto.
    // The proto reconnect flag must be ignored — code range wins.
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"disconnect": {"code": 3500, "reason": "banned", "reconnect": true}}
        }))))
        .await
        .unwrap();

    // Bounded total-duration drain. If the bug is live the client loops forever
    // on reconnect errors, so a per-recv timeout would never expire.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let mut disconnected_code = None;
    let mut saw_connecting = false;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            evt = events.recv() => match evt {
                Some(centrifuge_client::ClientEvent::Disconnected(ctx)) => {
                    disconnected_code = Some(ctx.code);
                }
                Some(centrifuge_client::ClientEvent::Connecting(_)) => {
                    saw_connecting = true;
                }
                Some(_) => {}
                None => break,
            }
        }
    }

    assert_eq!(
        disconnected_code,
        Some(3500),
        "terminal code 3500 must produce Disconnected event, not a reconnect attempt"
    );
    assert!(
        !saw_connecting,
        "must not emit Connecting after a terminal disconnect push"
    );
}

#[tokio::test]
async fn message_push() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"message": {"data": {"alert": "hello"}}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Message(_)) {
            found = true;
            break;
        }
    }
    assert!(found);
}

// =========================================================================
// P. Server join/leave on server-side subscriptions
// =========================================================================

#[tokio::test]
async fn server_sub_join_and_leave() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"notif": {}}}
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"push": {"channel": "notif", "join": {"info": {"client": "c1", "user": "u1"}}}}),
        )))
        .await
        .unwrap();
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"push": {"channel": "notif", "leave": {"info": {"client": "c2", "user": "u2"}}}}),
        )))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut join_count = 0;
    let mut leave_count = 0;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        match &e {
            centrifuge_client::ClientEvent::ServerJoin(_) => join_count += 1,
            centrifuge_client::ClientEvent::ServerLeave(_) => leave_count += 1,
            _ => {}
        }
    }
    assert_eq!(join_count, 1);
    assert_eq!(leave_count, 1);
}

// =========================================================================
// Q. Server-side unsubscribe and subscribe pushes
// =========================================================================

#[tokio::test]
async fn server_sub_unsubscribe_push() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "connect": {"client": "test", "version": "1.0", "ping": 25, "pong": true, "subs": {"notif": {}}}
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"channel": "notif", "unsubscribe": {"code": 2000, "reason": "removed"}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut unsubs = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerUnsubscribed(ctx) = e {
            unsubs.push(ctx.channel);
        }
    }
    assert!(unsubs.contains(&"notif".to_string()));
}

#[tokio::test]
async fn server_subscribe_push_mid_connection() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"channel": "alerts", "subscribe": {"recoverable": true, "offset": 1, "epoch": "e1"}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut subs = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerSubscribed(ctx) = e {
            subs.push(ctx.channel);
        }
    }
    assert!(subs.contains(&"alerts".to_string()));
}

// =========================================================================
// Client-side unsubscribe pushes from server
// =========================================================================

#[tokio::test]
async fn server_unsubscribe_resubscribable() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    // Server unsubscribe with code >= 2500 -> resubscribe
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"channel": "ch", "unsubscribe": {"code": 2500, "reason": "temporary"}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(200)).await;
    let mut subscribing_count = 0u32;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Subscribing(_)) {
            subscribing_count += 1;
        }
    }
    assert!(subscribing_count >= 2);
}

#[tokio::test]
async fn server_unsubscribe_terminal() {
    let (client, mut conn, _) = make_client(default_config());
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");
    connect_client(&client, &mut conn).await;
    subscribe_sub(&sub, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "push": {"channel": "ch", "unsubscribe": {"code": 2000, "reason": "revoked"}}
        }))))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Unsubscribed(_)) {
            found = true;
            break;
        }
    }
    assert!(found);
}

// =========================================================================
// Server-side subscription with recovery and publications
// =========================================================================

#[tokio::test]
async fn server_sub_with_recovered_publications() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;

    // Server sub with recoverable=true, positioned=true, and recovered publications
    let reply = serde_json::json!({
        "id": id, "connect": {
            "client": "test", "version": "1.0", "ping": 25, "pong": true,
            "subs": {
                "recovery_ch": {
                    "recoverable": true,
                    "positioned": true,
                    "offset": 5,
                    "epoch": "epoch1",
                    "recovered": true,
                    "publications": [
                        {"data": {"msg": "recovered1"}, "offset": 3},
                        {"data": {"msg": "recovered2"}, "offset": 5}
                    ]
                }
            }
        }
    });
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    let mut subscribed = false;
    let mut publications = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        match e {
            centrifuge_client::ClientEvent::ServerSubscribed(ctx) => {
                assert_eq!(ctx.channel, "recovery_ch");
                assert!(ctx.recoverable);
                assert!(ctx.positioned);
                assert!(ctx.stream_position.is_some());
                let pos = ctx.stream_position.unwrap();
                assert_eq!(pos.offset, 5);
                assert_eq!(pos.epoch, "epoch1");
                assert!(ctx.has_recovered_publications);
                subscribed = true;
            }
            centrifuge_client::ClientEvent::ServerPublication(ctx) => {
                assert_eq!(ctx.channel, "recovery_ch");
                publications.push(ctx.publication);
            }
            _ => {}
        }
    }
    assert!(subscribed, "should receive ServerSubscribed event");
    assert_eq!(publications.len(), 2, "should receive 2 recovered publications");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn server_sub_without_positioned_has_no_stream_position() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;

    // Server sub without recoverable/positioned
    let reply = serde_json::json!({
        "id": id, "connect": {
            "client": "test", "version": "1.0", "ping": 25, "pong": true,
            "subs": {
                "simple_ch": {}
            }
        }
    });
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&reply)))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(50)).await;

    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerSubscribed(ctx) = e {
            assert!(!ctx.recoverable);
            assert!(!ctx.positioned);
            assert!(ctx.stream_position.is_none());
            assert!(!ctx.has_recovered_publications);
            found = true;
            break;
        }
    }
    assert!(found);

    client.disconnect().await.unwrap();
}
