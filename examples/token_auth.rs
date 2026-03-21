use centrifuge::{
    Client, ClientConfig,
    events::ClientEventHandlers,
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
        })
        .on_error(|ctx| {
            eprintln!("error: {}", ctx.error);
        });

    let config = ClientConfig::new("ws://localhost:8000/connection/websocket")
        .get_token(Box::new(|| {
            Box::pin(async {
                // In a real app, you'd fetch a JWT from your backend:
                // let resp = reqwest::get("http://localhost:3000/centrifugo/token").await?;
                // let token = resp.text().await?;
                Ok("your-jwt-token-here".to_string())
            })
        }))
        .events(events);

    let client = Client::new(config);

    client.connect().await?;

    println!("Connected with token auth. Press Ctrl+C to exit...");
    tokio::signal::ctrl_c().await?;

    client.disconnect().await?;
    Ok(())
}
