use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub struct CentrifugoContainer {
    _container: ContainerAsync<GenericImage>,
    pub ws_url: String,
}

pub async fn start_insecure() -> CentrifugoContainer {
    let container = GenericImage::new("centrifugo/centrifugo", "v6")
        .with_exposed_port(8000.tcp())
        .with_wait_for(WaitFor::message_on_stderr("serving websocket"))
        .with_cmd(["centrifugo", "--client.insecure"])
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_SIZE", "100")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_TTL", "300s")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_FORCE_RECOVERY", "true")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_PRESENCE", "true")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_JOIN_LEAVE", "true")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_FORCE_PUSH_JOIN_LEAVE", "true")
        .with_env_var("CENTRIFUGO_CLIENT_CHANNEL_LIMIT", "50")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_DELTA_PUBLISH", "true")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOWED_DELTA_TYPES", "fossil")
        .start()
        .await
        .expect("failed to start centrifugo container");

    let port = container.get_host_port_ipv4(8000).await.expect("failed to get port");

    CentrifugoContainer {
        ws_url: format!("ws://127.0.0.1:{port}/connection/websocket"),
        _container: container,
    }
}

#[allow(dead_code)]
pub async fn start_with_auth() -> CentrifugoContainer {
    let container = GenericImage::new("centrifugo/centrifugo", "v6")
        .with_exposed_port(8000.tcp())
        .with_wait_for(WaitFor::message_on_stderr("serving websocket"))
        .with_cmd(["centrifugo"])
        .with_env_var("CENTRIFUGO_CLIENT_TOKEN_HMAC_SECRET_KEY", HMAC_SECRET)
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_SIZE", "100")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_TTL", "300s")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_FORCE_RECOVERY", "true")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_PRESENCE", "true")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_JOIN_LEAVE", "true")
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_FORCE_PUSH_JOIN_LEAVE", "true")
        .with_env_var(
            "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_SUBSCRIBER",
            "true",
        )
        .with_env_var("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_CLIENT", "true")
        .with_env_var(
            "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_HISTORY_FOR_SUBSCRIBER",
            "true",
        )
        .with_env_var(
            "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PRESENCE_FOR_SUBSCRIBER",
            "true",
        )
        .with_env_var(
            "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_CLIENT",
            "true",
        )
        .with_env_var("CENTRIFUGO_CLIENT_SUBSCRIBE_TO_USER_PERSONAL_CHANNEL_ENABLED", "true")
        .with_env_var(
            "CENTRIFUGO_CLIENT_SUBSCRIBE_TO_USER_PERSONAL_CHANNEL_PERSONAL_CHANNEL_NAMESPACE",
            "",
        )
        .with_env_var("CENTRIFUGO_CLIENT_CHANNEL_LIMIT", "50")
        .start()
        .await
        .expect("failed to start centrifugo-auth container");

    let port = container.get_host_port_ipv4(8000).await.expect("failed to get port");

    CentrifugoContainer {
        ws_url: format!("ws://127.0.0.1:{port}/connection/websocket"),
        _container: container,
    }
}

#[allow(dead_code)]
pub const HMAC_SECRET: &str = "test-secret-key-for-integration-tests";
