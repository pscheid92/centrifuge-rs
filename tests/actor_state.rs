mod actor_helpers;
use actor_helpers::*;

use std::time::Duration;

use tokio::time;

use centrifuge_client::Client;
use centrifuge_client::config::SubscriptionConfig;
use centrifuge_client::transport::{DisconnectInfo, TransportFrame};

// =========================================================================
// Coverage: state() and subscriptions() methods
// =========================================================================

#[tokio::test]
async fn sync_state_method() {
    let (client, mut conn, _) = make_client(default_config());
    // No .await -- state() is sync now
    assert_eq!(client.state(), centrifuge_client::ClientState::Disconnected);

    connect_client(&client, &mut conn).await;
    assert_eq!(client.state(), centrifuge_client::ClientState::Connected);

    client.disconnect().await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.state(), centrifuge_client::ClientState::Disconnected);
}

#[tokio::test]
async fn client_state_disconnected() {
    let (client, _conn, _) = make_client(default_config());
    assert_eq!(client.state(), centrifuge_client::ClientState::Disconnected);
}

#[tokio::test]
async fn client_state_connected() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;
    assert_eq!(client.state(), centrifuge_client::ClientState::Connected);
}

#[tokio::test]
async fn client_state_closed() {
    let (client, _conn, _) = make_client(default_config());
    client.close().await.unwrap();
    assert_eq!(client.state(), centrifuge_client::ClientState::Closed);
}

#[tokio::test]
async fn client_subscriptions_list() {
    let (client, mut conn, _) = make_client(default_config());
    client.new_subscription_default("ch1").await.unwrap();
    client.new_subscription_default("ch2").await.unwrap();

    let subs = client.subscriptions().await;
    assert_eq!(subs.len(), 2);

    // Both should be Unsubscribed initially
    for (_, state) in &subs {
        assert_eq!(*state, centrifuge_client::SubscriptionState::Unsubscribed);
    }

    // Subscribe one
    connect_client(&client, &mut conn).await;
    let sub1 = client.get_subscription("ch1").await.unwrap().unwrap();
    subscribe_sub(&sub1, &mut conn).await;

    let subs = client.subscriptions().await;
    let ch1_state = subs.iter().find(|(ch, _)| ch == "ch1").unwrap().1;
    let ch2_state = subs.iter().find(|(ch, _)| ch == "ch2").unwrap().1;
    assert_eq!(ch1_state, centrifuge_client::SubscriptionState::Subscribed);
    assert_eq!(ch2_state, centrifuge_client::SubscriptionState::Unsubscribed);
}

#[tokio::test]
async fn client_subscriptions_empty() {
    let (client, _conn, _) = make_client(default_config());
    assert!(client.subscriptions().await.is_empty());
}

// =========================================================================
// Client events stream
// =========================================================================

#[tokio::test]
async fn client_events_stream() {
    let (client, mut conn, _) = make_client(default_config());
    let mut events = client.events().expect("events");

    connect_client(&client, &mut conn).await;

    // Should have received Connecting then Connected
    let e1 = time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(e1, centrifuge_client::ClientEvent::Connecting(_)));

    let e2 = time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(e2, centrifuge_client::ClientEvent::Connected(_)));
}

// =========================================================================
// Edge case: event receiver dropped while events emitted
// =========================================================================

#[tokio::test]
async fn event_receiver_drop_does_not_panic() {
    let (client, mut conn, _) = make_client(default_config());
    connect_client(&client, &mut conn).await;

    // Set client event channel and immediately drop the receiver.
    // Actor will store the sender; emitting to it must not panic.
    drop(client.events().expect("events"));
    time::sleep(Duration::from_millis(20)).await;

    // Subscribe with dropped sub event receiver
    let sub = client
        .new_subscription("ch", SubscriptionConfig::default())
        .await
        .unwrap();
    drop(sub.events().expect("events"));
    time::sleep(Duration::from_millis(20)).await;

    // Subscribe emits Subscribing/Subscribed to the closed sub event channel
    subscribe_sub(&sub, &mut conn).await;

    // Disconnect emits ClientEvent::Disconnected to the closed channel
    client.disconnect().await.unwrap();
    // If we reach here, the actor handled all closed channels gracefully.
}

// =========================================================================
// set_token / set_data
// =========================================================================

#[tokio::test]
async fn set_token_updates_for_next_reconnect() {
    let (transport, mut conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    // Connect with original token
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    let cmd = read_command(&mut conn).await;
    let id = cmd["id"].as_u64().unwrap() as u32;
    let original_token = cmd["connect"]["token"].as_str().unwrap_or("").to_string();
    conn.incoming_tx
        .send(TransportFrame::Data(encode_reply(&serde_json::json!({
            "id": id, "connect": {"client": "c1", "version": "1.0.0", "ping": 25, "pong": true}
        }))))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Update token
    client.set_token("new-dynamic-token");
    time::sleep(Duration::from_millis(20)).await;

    // Force reconnect
    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    // Read new connect command — should have the updated token
    let cmd2 = read_command(&mut conn2).await;
    let new_token = cmd2["connect"]["token"].as_str().unwrap_or("").to_string();
    assert_eq!(new_token, "new-dynamic-token", "reconnect should use updated token");
    assert_ne!(new_token, original_token);

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn set_data_updates_for_next_reconnect() {
    let (transport, mut conn) = MockTransport::new();
    let mut conn2 = transport.add_connection();
    let client = Client::new_with_transport(default_config(), Box::new(ArcTransport(transport)));

    // Connect
    let c = client.clone();
    let task = tokio::spawn(async move { c.connect().await });
    do_connect(&mut conn).await;
    task.await.unwrap().unwrap();
    time::sleep(Duration::from_millis(20)).await;

    // Update data
    client.set_data(br#"{"cursor":42}"#.to_vec());
    time::sleep(Duration::from_millis(20)).await;

    // Force reconnect
    conn.incoming_tx
        .send(TransportFrame::Close(Some(DisconnectInfo {
            code: 3001,
            reason: "restart".into(),
            reconnect: true,
        })))
        .await
        .unwrap();

    // Read new connect command — should have the updated data
    let cmd2 = read_command(&mut conn2).await;
    let data = cmd2["connect"]["data"].as_object();
    assert!(data.is_some(), "reconnect should include updated data");
    assert_eq!(data.unwrap()["cursor"], 42);

    client.disconnect().await.unwrap();
}
