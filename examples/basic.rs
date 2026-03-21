use centrifuge::{
    Client, ClientConfig, SubscriptionConfig,
    events::{ClientEventHandlers, SubscriptionEventHandlers},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let events = ClientEventHandlers::default()
        .on_connecting(|ctx| {
            println!("connecting: {} (code {})", ctx.reason, ctx.code);
        })
        .on_connected(|ctx| {
            println!("connected: client_id={}", ctx.client_id);
        })
        .on_disconnected(|ctx| {
            println!("disconnected: {} (code {})", ctx.reason, ctx.code);
        });

    let config = ClientConfig::new("ws://localhost:8000/connection/websocket")
        .events(events);

    let client = Client::new(config);

    let sub_events = SubscriptionEventHandlers::default()
        .on_subscribing(|ctx| {
            println!("subscribing: {}", ctx.reason);
        })
        .on_subscribed(|ctx| {
            println!("subscribed to {}", ctx.channel);
        })
        .on_publication(|ctx| {
            println!(
                "publication on {}: {} bytes",
                ctx.channel,
                ctx.publication.data.len()
            );
        })
        .on_unsubscribed(|ctx| {
            println!("unsubscribed: {} (code {})", ctx.reason, ctx.code);
        });

    let sub_config = SubscriptionConfig {
        events: sub_events,
        ..Default::default()
    };

    let sub = client.new_subscription("example", sub_config).await?;
    sub.subscribe().await?;

    client.connect().await?;

    println!("Press Ctrl+C to exit...");
    tokio::signal::ctrl_c().await?;

    client.disconnect().await?;
    Ok(())
}
