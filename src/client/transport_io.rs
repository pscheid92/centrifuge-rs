use tokio::time;
use tokio_stream::StreamExt;

use super::actor::{ActorCommand, ConnectionActor};
use crate::codes;
use crate::config::ProtocolType;
use crate::errors::{CentrifugeError, Result};
use crate::protocol::{proto, types::*};
use crate::transport::{self, TransportFrame};

impl ConnectionActor {
    pub(super) async fn on_transport_close(&mut self, info: Option<transport::DisconnectInfo>) {
        let info = info.unwrap_or(transport::DisconnectInfo {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason: "transport closed".into(),
            reconnect: true,
        });

        self.sink = None;
        self.stream = None;
        self.fail_all_pending();

        if info.reconnect && self.connect_requested {
            for sub in self.subs.values_mut() {
                if sub.state == SubscriptionState::Subscribed {
                    sub.state = SubscriptionState::Subscribing;
                    sub.resubscribe_attempts = 0;
                    sub.emit(SubEvent::Subscribing(SubscribingContext {
                        code: codes::subscribing::TRANSPORT_CLOSED,
                        reason: info.reason.clone(),
                    }));
                }
            }

            for channel in self.server_subs.keys() {
                self.emit_client_event(ClientEvent::ServerSubscribing(ServerSubscribingContext {
                    channel: channel.clone(),
                    code: codes::subscribing::TRANSPORT_CLOSED,
                    reason: info.reason.clone(),
                }));
            }

            self.move_to_connecting(info.code, &info.reason);
        } else {
            self.move_to_disconnected(info.code, &info.reason);
        }
    }

    pub(super) fn schedule_request_timeout(&self, id: u32) {
        let timeout = self.config.timeout;
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            time::sleep(timeout).await;
            let _ = tx.send(ActorCommand::RequestTimeout { id }).await;
        });
    }

    pub(super) async fn send_command(&mut self, cmd: &proto::Command) -> Result<()> {
        let data = self.codec.encode_commands(std::slice::from_ref(cmd))?;
        if self.batch.active {
            self.batch.queue.push(data);
            return Ok(());
        }
        if let Some(ref mut sink) = self.sink {
            sink.send_data(data).await.map_err(CentrifugeError::Transport)?;
            Ok(())
        } else {
            Err(CentrifugeError::ClientDisconnected)
        }
    }

    pub(super) async fn flush_batch(&mut self) {
        self.batch.active = false;
        if self.batch.queue.is_empty() {
            return;
        }
        // Each entry was individually encoded by encode_commands(&[cmd]).
        // For JSON, entries need a newline separator. For protobuf, varint
        // length prefixes are self-delimiting so plain concatenation works.
        let combined = if self.config.protocol_type == ProtocolType::Json {
            self.batch.queue.drain(..).collect::<Vec<_>>().join(&b'\n')
        } else {
            self.batch.queue.drain(..).flatten().collect()
        };
        if let Some(ref mut sink) = self.sink {
            let _ = sink.send_data(combined).await;
        }
    }

    pub(super) async fn read_reply_with_timeout(&mut self, expected_id: u32) -> Result<proto::Reply> {
        let timeout = self.config.timeout;
        let deadline = time::sleep(timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                frame = async {
                    if let Some(ref mut stream) = self.stream {
                        stream.next().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    match frame {
                        Some(TransportFrame::Data(data)) => {
                            let replies = self.codec.decode_replies(&data)?;
                            for reply in replies {
                                if reply.id == expected_id {
                                    return Ok(reply);
                                }
                                self.dispatch_reply(reply).await;
                            }
                        }
                        Some(TransportFrame::Close(_)) | None => {
                            return Err(CentrifugeError::ClientDisconnected);
                        }
                    }
                }
                _ = &mut deadline => {
                    return Err(CentrifugeError::Timeout);
                }
            }
        }
    }
}
