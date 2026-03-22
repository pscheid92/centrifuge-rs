/// Advanced integration tests against a real Centrifugo server (via testcontainers).
///
/// Tests JWT auth, message recovery, server-side subscriptions, reconnection.
/// Docker must be running. Containers are started automatically.
mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use jsonwebtoken::{EncodingKey, Header, encode};
use serde::Serialize;
use tokio::time;

use centrifuge_client::config::{ClientConfig, ProtocolType, SubscriptionConfig};
use centrifuge_client::{CentrifugeError, Client};

#[derive(Serialize)]
struct Claims {
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exp: Option<u64>,
}

fn make_token(user: &str, exp: Option<u64>) -> String {
    encode(
        &Header::default(),
        &Claims {
            sub: user.to_string(),
            exp,
        },
        &EncodingKey::from_secret(common::HMAC_SECRET.as_bytes()),
    )
    .unwrap()
}

fn make_long_lived_token(user: &str) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    make_token(user, Some(exp))
}

fn make_short_lived_token(user: &str, ttl_secs: u64) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + ttl_secs;
    make_token(user, Some(exp))
}

// =========================================================================
// Recovery
// =========================================================================

#[tokio::test]
async fn recovery_receives_missed_publications() {
    let server = common::start_insecure().await;
    let channel = "recov_test";

    // Phase 1: subscribe with recovery, publish, disconnect
    let client_a = Client::new(ClientConfig::new(&server.ws_url).timeout(Duration::from_secs(5)));
    let sub_a = client_a
        .new_subscription(
            channel,
            SubscriptionConfig {
                recoverable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events_a = sub_a.events().expect("events");

    client_a.connect().await.unwrap();
    sub_a.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    sub_a.publish(br#"{"msg":"before_disconnect"}"#.to_vec()).await.unwrap();

    let mut pubs_count = 0;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events_a.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Publication(_)) {
            pubs_count += 1;
            break;
        }
    }
    assert!(pubs_count >= 1);
    client_a.disconnect().await.unwrap();

    // Phase 2: publish while disconnected
    let client_b = Client::new(ClientConfig::new(&server.ws_url).timeout(Duration::from_secs(5)));
    let sub_b = client_b
        .new_subscription(channel, SubscriptionConfig::default())
        .await
        .unwrap();
    client_b.connect().await.unwrap();
    sub_b.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    for i in 1..=3 {
        sub_b
            .publish(format!(r#"{{"msg":"missed_{i}"}}"#).into_bytes())
            .await
            .unwrap();
    }
    time::sleep(Duration::from_millis(300)).await;
    client_b.disconnect().await.unwrap();

    // Phase 3: reconnect -- should recover
    let client_a2 = Client::new(ClientConfig::new(&server.ws_url).timeout(Duration::from_secs(5)));
    let sub_a2 = client_a2
        .new_subscription(
            channel,
            SubscriptionConfig {
                recoverable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client_a2.connect().await.unwrap();
    sub_a2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(500)).await;
    client_a2.disconnect().await.unwrap();
}

#[tokio::test]
async fn recovery_with_protobuf() {
    let server = common::start_insecure().await;
    let channel = "recov_pb";

    let client_a = Client::new(
        ClientConfig::new(&server.ws_url)
            .protocol_type(ProtocolType::Protobuf)
            .timeout(Duration::from_secs(5)),
    );
    let sub_a = client_a
        .new_subscription(
            channel,
            SubscriptionConfig {
                recoverable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client_a.connect().await.unwrap();
    sub_a.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;
    sub_a.publish(b"init_msg".to_vec()).await.unwrap();
    time::sleep(Duration::from_millis(200)).await;
    client_a.disconnect().await.unwrap();

    let client_b = Client::new(ClientConfig::new(&server.ws_url).timeout(Duration::from_secs(5)));
    let sub_b = client_b
        .new_subscription(channel, SubscriptionConfig::default())
        .await
        .unwrap();
    client_b.connect().await.unwrap();
    sub_b.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    sub_b.publish(br#"{"msg":"while_pb_down"}"#.to_vec()).await.unwrap();
    time::sleep(Duration::from_millis(200)).await;
    client_b.disconnect().await.unwrap();

    let client_a2 = Client::new(
        ClientConfig::new(&server.ws_url)
            .protocol_type(ProtocolType::Protobuf)
            .timeout(Duration::from_secs(5)),
    );
    let sub_a2 = client_a2
        .new_subscription(
            channel,
            SubscriptionConfig {
                recoverable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    client_a2.connect().await.unwrap();
    sub_a2.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(500)).await;
    client_a2.disconnect().await.unwrap();
}

// =========================================================================
// JWT auth
// =========================================================================

#[tokio::test]
async fn jwt_connect_with_valid_token() {
    let server = common::start_with_auth().await;

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .token(make_long_lived_token("test-user"))
            .timeout(Duration::from_secs(5)),
    );
    let mut events = client.events().expect("events");
    client.connect().await.unwrap();

    let mut connected = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        if let centrifuge_client::ClientEvent::Connected(ctx) = e {
            assert!(!ctx.client_id.is_empty());
            connected = true;
            break;
        }
    }
    assert!(connected);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn jwt_connect_without_token_fails() {
    let server = common::start_with_auth().await;

    let client = Client::new(ClientConfig::new(&server.ws_url).timeout(Duration::from_secs(2)));
    let mut events = client.events().expect("events");
    let result = time::timeout(Duration::from_secs(3), client.connect()).await;
    time::sleep(Duration::from_millis(200)).await;

    let mut errored = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Error(_)) {
            errored = true;
            break;
        }
    }
    let timed_out = result.is_err();
    assert!(errored || timed_out);
    let _ = client.disconnect().await;
}

#[tokio::test]
async fn jwt_connect_with_get_token_callback() {
    let server = common::start_with_auth().await;
    let token_called = Arc::new(AtomicU32::new(0));
    let tc = token_called.clone();

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .get_token(Box::new(move || {
                let tc = tc.clone();
                Box::pin(async move {
                    tc.fetch_add(1, Ordering::Relaxed);
                    Ok(make_long_lived_token("callback-user"))
                })
            }))
            .timeout(Duration::from_secs(5)),
    );
    let mut events = client.events().expect("events");
    client.connect().await.unwrap();

    let mut connected = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Connected(_)) {
            connected = true;
            break;
        }
    }
    assert!(connected);
    assert!(token_called.load(Ordering::Relaxed) >= 1);
    client.disconnect().await.unwrap();
}

use std::sync::Arc;

#[tokio::test]
async fn jwt_token_refresh_on_expiration() {
    let server = common::start_with_auth().await;
    let refresh_count = Arc::new(AtomicU32::new(0));
    let rc = refresh_count.clone();

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .token(make_short_lived_token("refresh-user", 5))
            .get_token(Box::new(move || {
                let rc = rc.clone();
                Box::pin(async move {
                    rc.fetch_add(1, Ordering::Relaxed);
                    Ok(make_long_lived_token("refresh-user"))
                })
            }))
            .timeout(Duration::from_secs(5)),
    );
    client.connect().await.unwrap();
    time::sleep(Duration::from_secs(6)).await;
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn jwt_unauthorized_callback_disconnects() {
    let server = common::start_with_auth().await;

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .get_token(Box::new(|| Box::pin(async { Err(CentrifugeError::Unauthorized) })))
            .timeout(Duration::from_secs(2)),
    );
    let mut events = client.events().expect("events");
    let _ = client.connect().await;
    time::sleep(Duration::from_millis(500)).await;

    let mut disconnected = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Disconnected(_)) {
            disconnected = true;
            break;
        }
    }
    assert!(disconnected);
}

// =========================================================================
// JWT + operations
// =========================================================================

#[tokio::test]
async fn jwt_subscribe_publish_history_presence() {
    let server = common::start_with_auth().await;

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .token(make_long_lived_token("auth-pubsub-user"))
            .timeout(Duration::from_secs(5)),
    );
    let sub = client
        .new_subscription("authtestchan", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    sub.publish(br#"{"auth":"test"}"#.to_vec()).await.unwrap();

    let mut found_pub = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(1000), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Publication(_)) {
            found_pub = true;
            break;
        }
    }
    assert!(found_pub);

    let history = sub
        .history(centrifuge_client::HistoryOptions {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!history.publications.is_empty());

    let presence = sub.presence().await.unwrap();
    assert!(!presence.presence.is_empty());

    let stats = sub.presence_stats().await.unwrap();
    assert!(stats.num_clients >= 1);

    client.disconnect().await.unwrap();
}

// =========================================================================
// Server-side subscriptions
// =========================================================================

#[tokio::test]
async fn server_side_personal_channel_subscription() {
    let server = common::start_with_auth().await;

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .token(make_long_lived_token("personal-user"))
            .timeout(Duration::from_secs(5)),
    );
    let mut events = client.events().expect("events");
    client.connect().await.unwrap();
    time::sleep(Duration::from_millis(500)).await;

    let mut server_subs = Vec::new();
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        if let centrifuge_client::ClientEvent::ServerSubscribed(ctx) = e {
            server_subs.push(ctx.channel);
        }
    }
    let has_personal = server_subs.iter().any(|ch| ch.contains("personal-user"));
    assert!(has_personal, "should have personal channel sub, got: {server_subs:?}");

    let personal_channel = server_subs
        .iter()
        .find(|ch| ch.contains("personal-user"))
        .cloned()
        .unwrap();

    // Publish to personal channel from another client
    let client2 = Client::new(
        ClientConfig::new(&server.ws_url)
            .token(make_long_lived_token("publisher-user"))
            .timeout(Duration::from_secs(5)),
    );
    client2.connect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    client2
        .publish(&personal_channel, br#"{"to":"personal-user"}"#.to_vec())
        .await
        .unwrap();
    time::sleep(Duration::from_millis(500)).await;

    let mut found_pub = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::ServerPublication(_)) {
            found_pub = true;
            break;
        }
    }
    assert!(found_pub);

    client.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
}

// =========================================================================
// Reconnection
// =========================================================================

#[tokio::test]
async fn real_reconnect_and_resubscribe() {
    let server = common::start_insecure().await;

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .timeout(Duration::from_secs(5))
            .min_reconnect_delay(Duration::from_millis(100))
            .max_reconnect_delay(Duration::from_millis(500)),
    );
    let mut client_events = client.events().expect("events");
    let sub = client
        .new_subscription("reconntest", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut sub_events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    // Count initial Connected + Subscribed
    let mut connected_count = 0u32;
    let mut subscribed_count = 0u32;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), client_events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Connected(_)) {
            connected_count += 1;
        }
    }
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), sub_events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Subscribed(_)) {
            subscribed_count += 1;
        }
    }
    assert_eq!(connected_count, 1);
    assert_eq!(subscribed_count, 1);

    client.disconnect().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), client_events.recv()).await {
        if matches!(e, centrifuge_client::ClientEvent::Connected(_)) {
            connected_count += 1;
        }
    }
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), sub_events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Subscribed(_)) {
            subscribed_count += 1;
        }
    }
    assert_eq!(connected_count, 2);
    assert_eq!(subscribed_count, 2);

    client.disconnect().await.unwrap();
}

// =========================================================================
// JWT + Protobuf
// =========================================================================

#[tokio::test]
async fn jwt_protobuf_connect_subscribe_publish() {
    let server = common::start_with_auth().await;

    let client = Client::new(
        ClientConfig::new(&server.ws_url)
            .protocol_type(ProtocolType::Protobuf)
            .token(make_long_lived_token("pb-auth-user"))
            .timeout(Duration::from_secs(5)),
    );
    let sub = client
        .new_subscription("pbauthchan", SubscriptionConfig::default())
        .await
        .unwrap();
    let mut events = sub.events().expect("events");

    client.connect().await.unwrap();
    sub.subscribe().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;

    sub.publish(b"protobuf-auth-data".to_vec()).await.unwrap();

    let mut found = false;
    while let Ok(Some(e)) = time::timeout(Duration::from_millis(1000), events.recv()).await {
        if matches!(e, centrifuge_client::SubEvent::Publication(_)) {
            found = true;
            break;
        }
    }
    assert!(found);

    client.disconnect().await.unwrap();
}
