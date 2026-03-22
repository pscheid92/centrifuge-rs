# SDK Specification Compliance

Line-by-line mapping of [Client-SDK-specification.md](docs/Client-SDK-specification.md) requirements to our implementation.

## Client Connection States (spec lines 7-90)

| Requirement | Status | Implementation |
|---|---|---|
| 4 states: disconnected, connecting, connected, closed | Done | `ClientState` enum in `protocol.rs` |
| Initial state is disconnected | Done | `ConnectionActor::new()` sets `Disconnected` |
| `connect()` moves to connecting | Done | `commands.rs` `Connect` handler calls `move_to_connecting` |
| Exponential backoff with full jitter on reconnect | Done | `backoff.rs` implements AWS full jitter strategy |
| Connected on successful connection | Done | `connect.rs` `do_connect_cycle` calls `move_to_connected` |
| Lost connection → connecting (auto-reconnect) | Done | `transport_io.rs` `on_transport_close` with `reconnect: true` |
| `disconnect()` → disconnected | Done | `commands.rs` `Disconnect` handler calls `move_to_disconnected` |
| Terminal server advice → disconnected | Done | `push.rs` `handle_disconnect_push` with `reconnect: false` |
| Connecting event has code + reason | Done | `ClientEvent::Connecting { code, reason }` |
| Connected event has context | Done | `ClientEvent::Connected(ConnectedContext)` with `client_id` |
| Disconnected event has code + reason | Done | `ClientEvent::Disconnected { code, reason }` |
| Error event for internal errors | Done | `ClientEvent::Error(String)` emitted for transport/token/handshake errors |
| `disconnect()` fires disconnected event | Done | `state.rs` `move_to_disconnected` emits event |
| `closed` state cleans up resources | Done | `state.rs` `move_to_closed` unsubscribes all, actor loop exits |
| All subs go to unsubscribed on close | Done | `state.rs` `move_to_closed` iterates subs |

## Client Common Options (spec lines 92-102)

| Requirement | Status | Implementation |
|---|---|---|
| Connection token option | Done | `ClientConfig.token` |
| Token refresh callback | Done | `ClientConfig.get_token: Option<GetTokenFn>` |
| Connect data option | Done | `ClientConfig.data` |
| getData callback | Done | `ClientConfig.get_data: Option<GetDataFn>` |
| Operation timeout | Done | `ClientConfig.timeout` (default 5s) |
| Reconnect backoff tweaks (min/max delay) | Done | `ClientConfig.min_reconnect_delay`, `max_reconnect_delay` |
| Max server ping delay | Done | `ClientConfig.max_server_ping_delay` (default 10s) |
| WebSocket upgrade headers | Done | `ClientConfig.headers`, wired into tungstenite request |
| Client name and version | Done | `ClientConfig.name` (default "rs"), `ClientConfig.version` |

## Client Methods (spec lines 104-111)

| Requirement | Status | Implementation |
|---|---|---|
| `connect()` | Done | `Client::connect()` |
| `disconnect()` | Done | `Client::disconnect()` |
| `close()` | Done | `Client::close()` |
| `send(data)` | Done | `Client::send(data)` (fire-and-forget, no reply) |
| `rpc(method, data)` | Done | `Client::rpc(method, data)` |
| `publish(channel, data)` | Done | `Client::publish(channel, data)` |
| `history(channel, options)` | Done | `Client::history(channel, opts)` |
| `presence(channel)` | Done | `Client::presence(channel)` |
| `presenceStats(channel)` | Done | `Client::presence_stats(channel)` |
| `setToken(token)` | Done | `Client::set_token(token)` |
| `setData(data)` | Done | `Client::set_data(data)` |
| `startBatching()` / `stopBatching()` | Done | `Client::start_batching()`, `Client::stop_batching()` |

## Client Connection Token (spec lines 112-157)

| Requirement | Status | Implementation |
|---|---|---|
| Initial token via config | Done | `ClientConfig.token` |
| Token refresh via callback on expiration | Done | `token.rs` `schedule_token_refresh` + `do_refresh_token` |
| Callback must return new token | Done | `GetTokenFn` returns `Result<String>` |
| Retry on callback error with jitter | Done | `token.rs` `schedule_token_refresh_retry` (10s delay) |
| UnauthorizedError from callback → disconnect | Done | `token.rs` matches `CentrifugeError::Unauthorized` → `move_to_disconnected` |
| Empty token from callback → disconnect | Done | `connect.rs` `do_token_refresh` returns `Unauthorized` on empty |
| No initial token + getToken → fetch before connect | Done | `connect.rs` line 54 checks `token.is_empty() && get_token.is_some()` |

## Connection PING/PONG (spec line 159-161)

| Requirement | Status | Implementation |
|---|---|---|
| Client answers PONG to server PING | Done | `connect.rs` `dispatch_reply` sends pong on empty reply |
| Pong can be suppressed by server (pong=false) | Done | `state.rs` `move_to_connected` reads `result.pong` |
| No ping for too long → reconnect | Done | `connect.rs` `do_connected_loop` deadline check → `NO_PING` disconnect |

## Subscription States (spec lines 163-240)

| Requirement | Status | Implementation |
|---|---|---|
| 3 states: unsubscribed, subscribing, subscribed | Done | `SubscriptionState` enum |
| Initial state is unsubscribed | Done | `SubState::new()` sets `Unsubscribed` |
| `subscribe()` → subscribing | Done | `commands.rs` `handle_subscribe` sets `Subscribing` |
| Successful subscribe → subscribed | Done | `subscriptions.rs` `handle_subscribe_success` sets `Subscribed` |
| Temporary error → auto-resubscribe with backoff | Done | `subscriptions.rs` `handle_subscribe_error` with `is_temporary_error` |
| Non-temporary error → unsubscribed | Done | `subscriptions.rs` `handle_subscribe_error` else branch |
| Transport lost → subscribing (auto-resubscribe) | Done | `transport_io.rs` `on_transport_close` sets `Subscribing` |
| Subscribing event has code + reason | Done | `SubEvent::Subscribing { code, reason }` |
| Subscribed event has context | Done | `SubEvent::Subscribed(SubscribedContext)` |
| Unsubscribed event has code + reason | Done | `SubEvent::Unsubscribed { code, reason }` |
| Error event for internal sub errors | Done | `SubEvent::Error(String)` |
| `unsubscribe()` → unsubscribed, kept in registry | Done | `commands.rs` `handle_unsubscribe`, sub stays in `subs` map |
| `subscribe()` again after unsubscribe works | Done | `commands.rs` `handle_subscribe` handles re-subscribe |
| No duplicate subscriptions to same channel | Done | `commands.rs` `NewSubscription` checks `Entry::Vacant` |

## Subscription Management (spec lines 242-252)

| Requirement | Status | Implementation |
|---|---|---|
| `newSubscription(channel, options)` | Done | `Client::new_subscription(channel, config)` |
| `getSubscription(channel)` | Done | `Client::get_subscription(channel)` |
| `removeSubscription(sub)` | Done | `Client::remove_subscription(sub)` — auto-unsubscribes |
| `subscriptions()` returns all registered | Done | `Client::subscriptions()` returns `Vec<(String, SubscriptionState)>` |

## Publications (spec lines 254-291)

| Requirement | Status | Implementation |
|---|---|---|
| Publication has data | Done | `Publication.data` |
| Publication has optional offset | Done | `Publication.offset` |
| Publication has optional tags | Done | `Publication.tags` |
| Publication has optional info (publisher) | Done | `Publication.info: Option<ClientInfo>` |
| Subscribe before connect works | Done | `commands.rs` queues in `subscribe_waiters` |

## Subscription Recovery (spec lines 293-302)

| Requirement | Status | Implementation |
|---|---|---|
| Maintain stream position internally | Done | `SubState.offset`, `SubState.epoch` updated on each publication |
| `unsubscribe()` does NOT clear position | Done | `handle_unsubscribe` doesn't touch offset/epoch |
| `wasRecovering` in subscribed context | Done | `SubscribedContext.was_recovering` |
| `recovered` in subscribed context | Done | `SubscribedContext.recovered` |
| Recovered publications delivered on resubscribe | Done | `subscriptions.rs` `handle_subscribe_success` iterates `result.publications` |

## Subscription Common Options (spec lines 304-314)

| Requirement | Status | Implementation |
|---|---|---|
| Subscription token option | Done | `SubscriptionConfig.token` |
| Subscription token refresh callback | Done | `SubscriptionConfig.get_token` |
| Subscription data option | Done | `SubscriptionConfig.data` |
| Subscription getData callback | Done | `SubscriptionConfig.get_data` |
| Resubscribe backoff tweaks | Done | `SubscriptionConfig.min_resubscribe_delay`, `max_resubscribe_delay` |
| `since` stream position option | Done | `SubscriptionConfig.since` |
| `positioned` option | Done | `SubscriptionConfig.positioned` |
| `recoverable` option | Done | `SubscriptionConfig.recoverable` |
| `join_leave` option | Done | `SubscriptionConfig.join_leave` |

## Subscription Methods (spec lines 316-323)

| Requirement | Status | Implementation |
|---|---|---|
| `subscribe()` | Done | `Subscription::subscribe()` |
| `unsubscribe()` | Done | `Subscription::unsubscribe()` |
| `publish(data)` | Done | `Subscription::publish(data)` |
| `history(options)` | Done | `Subscription::history(opts)` |
| `presence()` | Done | `Subscription::presence()` |
| `presenceStats()` | Done | `Subscription::presence_stats()` |

## Subscription Token (spec lines 325-374)

| Requirement | Status | Implementation |
|---|---|---|
| Initial subscription token via config | Done | `SubscriptionConfig.token` |
| Token refresh via callback on expiration | Done | `token.rs` `schedule_sub_token_refresh` + `do_refresh_sub_token` |
| Empty token from callback → unsubscribe | Done | `token.rs` `do_refresh_sub_token` calls `unsubscribe_unauthorized` |
| UnauthorizedError → unsubscribe | Done | `token.rs` matches `Unauthorized` → `unsubscribe_unauthorized` |
| Retry on callback error with jitter | Done | `token.rs` schedules retry via `schedule_sub_token_refresh` |
| No initial token + getToken → fetch before subscribe | Done | `subscriptions.rs` `do_subscribe` checks `token.is_empty() && get_token.is_some()` |

## Server-Side Subscriptions (spec lines 376-423)

| Requirement | Status | Implementation |
|---|---|---|
| Server-side subs in internal registry | Done | `actor.rs` `server_subs: HashMap<String, ServerSubState>` |
| ServerSubscribed event on connect | Done | `server_subs.rs` `process_server_subs` emits events |
| ServerSubscribing event on connection loss | Done | `transport_io.rs` `on_transport_close` emits `ServerSubscribing` |
| ServerUnsubscribed on server push or disappearance | Done | `push.rs` + `server_subs.rs` handle both cases |
| ServerPublication event | Done | `ClientEvent::ServerPublication { channel, publication }` |
| Client-level publish/history/presence/presenceStats | Done | `Client::publish`, `history`, `presence`, `presence_stats` |

## Error Codes (spec lines 424-430)

| Requirement | Status | Implementation |
|---|---|---|
| Server error codes 100-1999 | Done | `codes.rs` `INTERNAL_ERROR = 100` |
| Client-side codes 0-99 | Done | `codes.rs` disconnect/connecting/subscribing modules |
| `temporary` flag on server errors | Done | `ServerError.temporary`, used in `is_temporary_error` |
| TOKEN_EXPIRED (109) handled specially | Done | `codes.rs` `TOKEN_EXPIRED = 109`, triggers refresh |
| ALREADY_SUBSCRIBED (105) tolerated | Done | `connect.rs` `resolve_pending` treats 105 as success |

## Unsubscribe Codes (spec lines 432-438)

| Requirement | Status | Implementation |
|---|---|---|
| Server codes in range 2000-2999 | Done | `codes.rs` `should_resubscribe_on_unsubscribe` |
| Code >= 2500 → auto-resubscribe | Done | `push.rs` `handle_server_unsubscribe` → `Subscribing` state |
| Code < 2500 → terminal unsubscribe | Done | `push.rs` `handle_server_unsubscribe` → `Unsubscribed` state |
| Client codes < 2000 | Done | `unsubscribed::UNSUBSCRIBE_CALLED = 0`, `UNAUTHORIZED = 1`, `CLIENT_CLOSED = 2` |

## Disconnect Codes (spec lines 440-446)

| Requirement | Status | Implementation |
|---|---|---|
| Server codes in range 3000-4999 | Done | `codes.rs` `should_reconnect_on_disconnect` |
| 3000-3499 → reconnect | Done | Covered by `!(3500..5000) \|\| (4000..4500)` |
| 3500-3999 → terminal | Done | Falls into 3500..5000 range, not in 4000..4500 |
| 4000-4499 → reconnect | Done | Explicit `(4000..4500).contains` |
| 4500-4999 → terminal | Done | Falls into 3500..5000 range, not in 4000..4500 |
| Codes >= 5000 → reconnect | Done | `!(3500..5000)` is true for >= 5000 |
| Client codes < 3000 | Done | `disconnect::DISCONNECT_CALLED = 0`, `UNAUTHORIZED = 1`, `BAD_PROTOCOL = 2`, `MESSAGE_SIZE_LIMIT = 3` |

## RPC (spec lines 448-457)

| Requirement | Status | Implementation |
|---|---|---|
| `rpc(method, data)` sends over WebSocket | Done | `Client::rpc()` sends `RpcRequest` |
| Returns response data | Done | Returns `RpcResult { data }` |

## History API (spec lines 459-490)

| Requirement | Status | Implementation |
|---|---|---|
| `history()` returns offset + epoch | Done | `HistoryResult { publications, offset, epoch }` |
| `limit` option | Done | `HistoryOptions.limit` |
| `since` option (offset + epoch) | Done | `HistoryOptions.since: Option<StreamPosition>` |
| `reverse` option | Done | `HistoryOptions.reverse` |

## Presence API (spec lines 492-509)

| Requirement | Status | Implementation |
|---|---|---|
| `presence()` returns map of client info | Done | `PresenceResult { presence: HashMap }` |
| `presenceStats()` returns counts | Done | `PresenceStatsResult { num_clients, num_users }` |

## Best Practices (spec lines 511-515)

| Requirement | Status | Implementation |
|---|---|---|
| Callbacks must be fast (non-blocking) | N/A | Our event stream model avoids this — events are sent via `try_send`, reader loop never blocks on user code |
| Don't blindly rely on state | N/A | All operations return `Result` with appropriate errors |
| Disconnect on mobile background | N/A | Library concern, not SDK concern — users call `disconnect()` |

## Protocol-Level Requirements (from Client-protocol.md)

| Requirement | Status | Implementation |
|---|---|---|
| Command-Reply with incremental u32 ID | Done | `actor.rs` `next_id: AtomicU32` |
| Reply matching by ID | Done | `connect.rs` `dispatch_reply` matches `reply.id` to `pending` |
| Async pushes (id=0) | Done | `connect.rs` `dispatch_reply` handles `reply.id == 0` |
| JSON: newline-delimited batching | Done | `codec.rs` `JsonCodec` joins with `\n` |
| Protobuf: varint-length-delimited batching | Done | `codec.rs` `ProtobufCodec` uses varint prefix |
| Server ping → client pong | Done | `connect.rs` `dispatch_reply` detects empty reply |
| Disconnect advice in WebSocket close frame | Done | `transport.rs` `parse_close` extracts JSON advice |
| Connect errors → reconnect with backoff | Done | `connect.rs` `do_connect_cycle` loops with backoff |
| Permanent connect errors → disconnect | Done | `connect.rs` checks `!is_temporary_error` → `move_to_disconnected` |
| Subscribe errors → reconnect on code 100 | Done | `commands.rs` `SUBSCRIBE_TIMEOUT` triggers reconnect |
| ALREADY_SUBSCRIBED (105) tolerated | Done | `connect.rs` `resolve_pending` treats as success |
| Token expired (109) → refresh + retry | Done | `connect.rs` sets `refresh_required`, `subscriptions.rs` clears token |
| BAD_PROTOCOL on decode error | Done | `connect.rs` `handle_transport_data` → terminal disconnect |
| Client name max 16 chars, version max 64 | Done | `ClientConfig::name()` and `version()` truncate to spec limits |
