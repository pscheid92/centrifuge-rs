mod actor_helpers;
use actor_helpers::*;

use std::time::Duration;

use tokio::time;

use centrifuge_client::CentrifugeError;
use centrifuge_client::config::SubscriptionConfig;
use centrifuge_client::transport::TransportFrame;

// =========================================================================
// D. Request/Reply Commands
// =========================================================================

#[tokio::test]
async fn publish_success() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.publish("test", br#"{"msg":"hello"}"#.to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    assert_eq!(cmd["publish"]["channel"], "test");
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "publish": {}}),
        )))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publish_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.publish("test", br#"{"msg":"hello"}"#.to_vec()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "error": {"code": 103, "message": "permission denied"}
        }))))
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

#[tokio::test]
async fn publish_when_disconnected() {
    let (client, _conn, _) = make_client(default_config());
    assert!(matches!(
        client.publish("test", b"hi".to_vec()).await,
        Err(CentrifugeError::ClientDisconnected)
    ));
}

#[tokio::test]
async fn history_success() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move {
        c.history(
            "ch",
            centrifuge_client::HistoryOptions {
                limit: 10,
                ..Default::default()
            },
        )
        .await
    });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "history": {"publications": [{"data": {"msg": "hi"}, "offset": 1}], "epoch": "abc", "offset": 1}
        }))))
        .await
        .unwrap();
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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "presence": {"presence": {"c1": {"user": "u1", "client": "c1"}}}
        }))))
        .await
        .unwrap();
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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "presence_stats": {"num_clients": 5, "num_users": 3}
        }))))
        .await
        .unwrap();
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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "rpc": {"data": {"result": "ok"}}
        }))))
        .await
        .unwrap();
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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id2, "rpc": {"data": {"r": 2}}}),
        )))
        .await
        .unwrap();
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id1, "rpc": {"data": {"r": 1}}}),
        )))
        .await
        .unwrap();

    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();
}

// =========================================================================
// I. Edge Cases
// =========================================================================

#[tokio::test]
async fn reply_for_unknown_id() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": 99999, "publish": {}}),
        )))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;
    // Should not panic, client still functional
    client.send(br#"{"t":1}"#.to_vec()).await.unwrap();
}

#[tokio::test]
async fn malformed_data_from_transport() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");
    connect_client(&client, &mut conn).await;

    // Malformed data should trigger a terminal disconnect (BAD_PROTOCOL)
    conn.incoming_tx
        .send(TransportFrame::Data(b"not json {{{{".to_vec()))
        .await
        .unwrap();
    time::sleep(Duration::from_millis(100)).await;

    let mut disconnected = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        if let centrifuge_client::ClientEvent::Disconnected(ctx) = e {
            assert_eq!(ctx.code, 2, "should disconnect with BAD_PROTOCOL code");
            disconnected = true;
            break;
        }
    }
    assert!(disconnected, "malformed data should cause terminal disconnect");
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
    let combined =
        format!("{{\"id\":{id1},\"rpc\":{{\"data\":{{\"r\":1}}}}}}\n{{\"id\":{id2},\"rpc\":{{\"data\":{{\"r\":2}}}}}}");
    conn.incoming_tx
        .send(TransportFrame::Data(combined.into_bytes()))
        .await
        .unwrap();

    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();
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
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "error": {"code": 100, "message": "internal"}}),
        )))
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

#[tokio::test]
async fn history_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.history("ch", centrifuge_client::HistoryOptions::default()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(
            &serde_json::json!({"id": id, "error": {"code": 103, "message": "denied"}}),
        )))
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(CentrifugeError::Server(_))));
}

/// mod.rs:206,209,220 -- client-level history/presence server errors
#[tokio::test]
async fn client_history_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.history("ch", centrifuge_client::HistoryOptions::default()).await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!(
            {"id": id, "error": {"code": 103, "message": "denied"}}
        ))))
        .await
        .unwrap();
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn client_presence_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.presence("ch").await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!(
            {"id": id, "error": {"code": 108, "message": "not available"}}
        ))))
        .await
        .unwrap();
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn client_presence_stats_server_error() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let c = client.clone();
    let task = tokio::spawn(async move { c.presence_stats("ch").await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!(
            {"id": id, "error": {"code": 108, "message": "not available"}}
        ))))
        .await
        .unwrap();
    assert!(task.await.unwrap().is_err());
}

// =========================================================================
// Batching
// =========================================================================

#[tokio::test]
async fn batching_sends_commands_as_single_frame() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    let sub1 = client
        .new_subscription("batch1", SubscriptionConfig::default())
        .await
        .unwrap();
    let sub2 = client
        .new_subscription("batch2", SubscriptionConfig::default())
        .await
        .unwrap();

    // Start batching — subscribe commands should be queued
    client.start_batching();
    time::sleep(Duration::from_millis(20)).await;

    let s1 = sub1.clone();
    let s2 = sub2.clone();
    let t1 = tokio::spawn(async move { s1.subscribe().await });
    let t2 = tokio::spawn(async move { s2.subscribe().await });

    // Give time for commands to reach the actor (they should be queued, not sent)
    time::sleep(Duration::from_millis(50)).await;

    // Nothing should have been sent to the transport yet
    let result = time::timeout(Duration::from_millis(50), conn.outgoing_rx.recv()).await;
    assert!(result.is_err(), "no commands should be sent while batching");

    // Stop batching — all queued commands should be sent as one frame
    client.stop_batching();

    // Now we should receive the batched data
    let data = time::timeout(Duration::from_millis(500), conn.outgoing_rx.recv())
        .await
        .expect("should receive batched frame")
        .expect("channel open");

    // The frame should contain two subscribe commands (newline-delimited JSON)
    let text = String::from_utf8_lossy(&data);
    let commands: Vec<serde_json::Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(commands.len(), 2, "batch should contain 2 commands, got: {text}");
    assert!(commands.iter().any(|c| c.get("subscribe").is_some()));

    // Reply to both subscribes
    for cmd in &commands {
        let id = cmd["id"].as_u64().unwrap() as u32;
        conn.incoming_tx
            .send(TransportFrame::Data(encode_reply(&serde_json::json!(
                {"id": id, "subscribe": {}}
            ))))
            .await
            .unwrap();
    }
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    client.disconnect().await.unwrap();
}
