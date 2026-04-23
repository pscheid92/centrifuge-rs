use super::actor::{ConnectionActor, ServerSubState};
use crate::codes;
use crate::protocol::{proto, types::*};

impl ConnectionActor {
    pub(super) fn process_server_subs(&mut self, result: &proto::ConnectResult) {
        let old_channels: Vec<String> = self.server_subs.keys().cloned().collect();

        for (channel, sub_result) in &result.subs {
            let was_recovering = self.server_subs.contains_key(channel);

            self.server_subs.insert(
                channel.clone(),
                ServerSubState {
                    recoverable: sub_result.recoverable,
                    offset: sub_result.offset,
                    epoch: sub_result.epoch.clone(),
                },
            );

            self.emit_client_event(ClientEvent::ServerSubscribed(ServerSubscribedContext {
                channel: channel.clone(),
                recoverable: sub_result.recoverable,
                positioned: sub_result.positioned,
                stream_position: if sub_result.positioned || sub_result.recoverable {
                    Some(StreamPosition {
                        offset: sub_result.offset,
                        epoch: sub_result.epoch.clone(),
                    })
                } else {
                    None
                },
                was_recovering,
                recovered: sub_result.recovered,
                has_recovered_publications: !sub_result.publications.is_empty(),
                data: sub_result.data.clone(),
            }));

            for pub_msg in &sub_result.publications {
                if let Some(ss) = self.server_subs.get_mut(channel)
                    && pub_msg.offset > 0
                {
                    ss.offset = pub_msg.offset;
                }
                self.emit_client_event(ClientEvent::ServerPublication(ServerPublicationContext {
                    channel: channel.clone(),
                    publication: Publication::from(pub_msg),
                }));
            }
        }

        for channel in old_channels {
            if !result.subs.contains_key(&channel) {
                self.server_subs.remove(&channel);
                self.emit_client_event(ClientEvent::ServerUnsubscribed(ServerUnsubscribedContext {
                    channel,
                    code: codes::unsubscribed::SERVER_SUB_REMOVED,
                    reason: "subscription not found after reconnect".into(),
                }));
            }
        }
    }
}
