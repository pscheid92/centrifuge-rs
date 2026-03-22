# Test Analysis — centrifuge-client

## Test Inventory

### Inline Unit Tests (src/)

| File | Tests | What They Cover |
|------|------:|-----------------|
| `src/codec.rs` | 21 | JSON encode/decode (commands, replies, pushes, empty pings, errors, batched), Protobuf roundtrip (single, reply, batch, empty data), varint decoding, embedded_json serde module, Protobuf error handling (truncated varint, varint too long, frame exceeds data, garbage data, partial frame after valid, zero-length frame) |
| `src/delta.rs` | 13 | Fossil delta: insert-only, copy-only, combined, bad checksum, missing newline, copy past source, copy/insert exceeds limit, unknown operator, unterminated, size mismatch, empty |
| `src/config.rs` | 4 | ClientConfig builder + defaults, SubscriptionConfig defaults, ProtocolType default |
| `src/transport.rs` | 5 | Close frame parsing: JSON advice, terminal code, reconnectable code, message-too-big, default mapping |
| `src/backoff.rs` | 5 | Max delay enforcement, attempt-0 bounded by min, exponential growth, zero min delay, jitter randomness |
| `src/codes.rs` | 3 | Disconnect reconnect classification, resubscribe threshold, temporary error classification |
| `src/errors.rs` | 2 | Display formatting for all variants, std::error::Error trait impl |
| **Total** | **53** | |

### Actor Tests (tests/actor_tests.rs) — No Docker Required

Uses a `MockTransport` implementing the `Transport` trait with pre-scripted server responses.

| Category | Tests | What They Cover |
|----------|------:|-----------------|
| Connection lifecycle | 6 | connect, disconnect, close, connect-when-closed, connect-when-connected, transport failure retries |
| Reconnection | 6 | Server error retry, token expired refresh, unauthorized disconnect, reconnectable close, terminal close, stream end |
| Ping/pong | 3 | Pong send, pong suppression, ping timeout reconnect |
| Request/reply commands | 9 | publish, history, presence, presence_stats, rpc, send (success + server error + disconnected), concurrent requests |
| Client-side subscriptions | 7 | new_subscription, duplicate error, recovery flag, unsubscribe, permanent error, get/remove subscription |
| Server-side subscriptions | 5 | On connect, disappears on reconnect, join/leave, unsubscribe push, mid-connection subscribe push |
| Push handling | 5 | Publication to client/server sub, join/leave, disconnect (reconnectable + terminal), message |
| Token refresh | 6 | Connection refresh during connected, sub refresh during subscribed, unauthorized, empty token, error retry |
| Subscribe edge cases | 7 | Before connect, temporary error retry, token expired, permanent error (fails waiters + unsubscribes), already subscribed, recovered publications |
| Waiter draining | 4 | Disconnect/close drain subscribe waiters, close while connecting, transport close while subscribe inflight |
| State/stream methods | 5 | state() sync access, events() stream, subscriptions() list, state values |
| Concurrency/edge cases | 5 | Connect while connecting, unsubscribe when not subscribed, drop handles during backoff, unknown reply ID, malformed data |
| Subscription handle methods | 8 | publish, history, presence, presence_stats (success + server error for each) |
| Bug regressions | 6 | #1 subscribe waiter timing, #2 operation timeout, #3 token refresh before TTL, #4 transport close mid-subscribe, #5 disconnect fails subscribing waiters, #6 token failure on resubscribe |
| Other | 2 | Multiple replies in single frame, transport close emits subscribing |
| Dropped receivers | 1 | event receiver drop doesn't panic actor |
| Concurrent refresh | 1 | connection + subscription token refresh simultaneously |
| **Total** | **101** | |

### Integration Tests (tests/integration_test.rs) — Requires Docker

Uses `common::start_insecure()` — Centrifugo with `--client.insecure` (no auth).

| Category | Tests | What They Cover |
|----------|------:|-----------------|
| JSON protocol | 8 | connect/disconnect, subscribe/publish, history, presence, recovery, RPC, cross-client publish, join/leave |
| Protobuf protocol | 7 | connect/disconnect, subscribe/publish, history+presence, cross-client publish, RPC, join/leave, recovery |
| Cross-protocol | 1 | JSON publisher → Protobuf subscriber |
| Go parity | 9 | publisher info, unknown namespace error, fresh channel history, 50 concurrent publishes, wrong address, concurrent close/disconnect, concurrent publish+subscribe+disconnect, invalid JSON, delta compression |
| JS parity | 3 | rapid subscribe/unsubscribe loop, unsubscribe before subscribe reply, 20 concurrent subscriptions |
| Timeout behavior | 1 | all operations complete within configured timeout against real server |
| Edge cases | 5 | large payload (32KB), empty payload, special channel names, message ordering (100 sequential), stress test (200 messages from 5 publishers) |
| **Total** | **34** | |

### Advanced Integration Tests (tests/integration_advanced_test.rs) — Requires Docker

Uses `common::start_with_auth()` — Centrifugo with JWT authentication.

| Category | Tests | What They Cover |
|----------|------:|-----------------|
| Recovery | 2 | Missed publication recovery (JSON + Protobuf) |
| JWT auth | 5 | Valid token, missing token, get_token callback, token refresh on expiration, unauthorized callback |
| JWT + operations | 1 | Subscribe + publish + history + presence with auth |
| Server-side subs | 1 | Personal channel auto-subscription |
| Reconnection | 1 | Real reconnect with resubscribe (counts events across reconnections) |
| JWT + Protobuf | 1 | Full ops with auth + Protobuf encoding |
| **Total** | **11** | |

### Grand Total: 200 tests

---

## What Was Fixed

- [x] **Custom headers wired** — `ClientConfig.headers` are now applied to the WebSocket request via `HeaderName`/`HeaderValue` parsing. The `#[allow(dead_code)]` annotation has been removed.

- [x] **Protobuf integration parity** — Added 3 new Protobuf tests: `protobuf_rpc`, `protobuf_join_leave_events`, `protobuf_subscribe_with_recovery`. Protobuf now has 7 integration tests (was 4), matching JSON coverage for all major operations.

- [x] **Codec error handling** — Added 6 Protobuf codec error tests: truncated varint, varint too long, frame exceeds data length, garbage data in valid-looking frame, partial frame after valid frame, zero-length frame. All error paths in `ProtobufCodec::decode_replies` are now covered.

- [x] **Operation timeout** — Added integration test verifying all operations (subscribe, publish, history, presence, presence_stats) complete within the configured timeout against a real server. Note: timeout *firing* (when server never responds) is tested in actor_tests with a mock transport — Centrifugo responds in <1ms so it can't be triggered in integration.

---

## Remaining Coverage Gaps

### Known Gap — TLS

- [ ] **No TLS tests** — `wss://` paths are not tested end-to-end. Centrifugo v6's TLS configuration + self-signed cert generation + macOS native-tls hostname verification makes automated TLS testing complex. TLS is handled by tokio-tungstenite which has its own test coverage upstream. This is an acceptable gap for 0.1.0 but should be addressed when adding custom TLS connector support to `WsTransport`.

### Nice to Have — All Done

All previously identified "nice to have" gaps have been addressed:

- [x] **Large payloads** — 32KB JSON payload test
- [x] **Empty payloads** — Empty JSON object `{}` round-trip test
- [x] **Channel names with special characters** — dashes, underscores, dots, uppercase, numbers
- [x] **Concurrent token refreshes** — connection + subscription token refresh fire simultaneously
- [x] **Event receiver drop** — dropped client + subscription event receivers don't panic the actor
- [x] **Message ordering** — 100 sequential publications verified in order
- [x] **Stress test** — 200 messages from 5 concurrent publishers, all received

### Not Needed

- **Custom Transport implementations** — The `Transport` trait is already tested via `MockTransport` in actor_tests. Testing additional implementations would be testing user code, not library code.
- **Protobuf-to-JSON cross-talk** — Already tested (JSON→Protobuf). The reverse is the same server path.
- **Recovery with protocol switch** — Not a real use case. Clients pick a protocol at connection time and don't switch.
