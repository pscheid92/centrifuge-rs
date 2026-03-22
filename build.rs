fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();

    // Add serde derives to all generated types
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config.type_attribute(".", "#[serde(default)]");

    // The Centrifuge JSON protocol treats `bytes` fields as embedded JSON objects,
    // not base64 strings. Apply a custom serde module to every byte field.
    let bytes_fields = [
        "centrifugal.centrifuge.protocol.EmulationRequest.data",
        "centrifugal.centrifuge.protocol.ClientInfo.conn_info",
        "centrifugal.centrifuge.protocol.ClientInfo.chan_info",
        "centrifugal.centrifuge.protocol.Publication.data",
        "centrifugal.centrifuge.protocol.Subscribe.data",
        "centrifugal.centrifuge.protocol.Message.data",
        "centrifugal.centrifuge.protocol.Connect.data",
        "centrifugal.centrifuge.protocol.ConnectRequest.data",
        "centrifugal.centrifuge.protocol.ConnectResult.data",
        "centrifugal.centrifuge.protocol.SubscribeRequest.data",
        "centrifugal.centrifuge.protocol.SubscribeResult.data",
        "centrifugal.centrifuge.protocol.PublishRequest.data",
        "centrifugal.centrifuge.protocol.RPCRequest.data",
        "centrifugal.centrifuge.protocol.RPCResult.data",
        "centrifugal.centrifuge.protocol.SendRequest.data",
    ];
    for field in &bytes_fields {
        config.field_attribute(field, "#[serde(with = \"crate::codec::embedded_json\")]");
    }

    // The Push message has a field named "pub", which is a Rust keyword.
    // Prost renames it to `r#pub`, but serde needs to know the JSON name.
    config.field_attribute("centrifugal.centrifuge.protocol.Push.pub", "#[serde(rename = \"pub\")]");

    // Skip serializing default values for compact JSON output
    config.field_attribute(
        "centrifugal.centrifuge.protocol.Command.id",
        "#[serde(skip_serializing_if = \"crate::codec::is_zero_u32\")]",
    );
    config.field_attribute(
        "centrifugal.centrifuge.protocol.Reply.id",
        "#[serde(skip_serializing_if = \"crate::codec::is_zero_u32\")]",
    );

    config.compile_protos(&["proto/client.proto"], &["proto/"])?;
    Ok(())
}
