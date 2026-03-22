use centrifuge_client::{Client, ClientConfig, ClientEvent, SubEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientConfig::new("ws://localhost:8000/connection/websocket"));

    let mut client_events = client.events()?;

    let (sub, mut sub_events) = client.subscribe("example").await?;

    client.connect().await?;

    println!("Connected. Press Ctrl+C to exit...");

    loop {
        tokio::select! {
            Some(event) = sub_events.recv() => match event {
                SubEvent::Publication(pub_data) => {
                    println!("publication on {}: {} bytes", sub.channel(), pub_data.data.len());
                }
                SubEvent::Subscribed(ctx) => {
                    println!("subscribed to {}", ctx.channel);
                }
                SubEvent::Unsubscribed(ctx) => {
                    println!("unsubscribed: {}", ctx.reason);
                }
                _ => {}
            },
            Some(event) = client_events.recv() => match event {
                ClientEvent::Connecting(ctx) => println!("connecting: {}", ctx.reason),
                ClientEvent::Connected(ctx) => println!("connected: client_id={}", ctx.client_id),
                ClientEvent::Disconnected(ctx) => println!("disconnected: {}", ctx.reason),
                _ => {}
            },
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    client.disconnect().await?;
    Ok(())
}
