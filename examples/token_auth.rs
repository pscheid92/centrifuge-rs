use centrifuge_client::{Client, ClientConfig, ClientEvent, get_token_fn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("ws://localhost:8000/connection/websocket").get_token(get_token_fn(|| async {
        // In a real app, fetch a JWT from your backend:
        // let resp = reqwest::get("http://localhost:3000/centrifugo/token").await?;
        // let token = resp.text().await?;
        Ok("your-jwt-token-here".to_string())
    }));

    let client = Client::new(config);
    let mut events = client.events()?;

    client.connect().await?;

    println!("Connected with token auth. Press Ctrl+C to exit...");

    loop {
        tokio::select! {
            Some(event) = events.recv() => match event {
                ClientEvent::Connected(ctx) => println!("connected: {}", ctx.client_id),
                ClientEvent::Disconnected(ctx) => println!("disconnected: {}", ctx.reason),
                ClientEvent::Error(ctx) => eprintln!("error: {}", ctx.error),
                _ => {}
            },
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    client.disconnect().await?;
    Ok(())
}
