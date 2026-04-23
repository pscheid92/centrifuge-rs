# centrifuge-rs Todo List

Actionable follow-ups from a full-codebase review, cross-referenced against the Go and JS reference SDKs in `additionals/`. Items are ordered so you can work top-down; each has enough context to pick up without re-reading the review.

**Baseline state:** `cargo check`, `cargo clippy` (default), and 68 library unit tests all pass. None of the fixes below should regress that.

Legend: `[ ]` todo · `[x]` done · `[~]` partial

---

## Phase 1 — Behavioral bugs (user-observable, diverges from Go/JS)

### [ ] 1. Disconnect-push reconnect logic: use code ranges only

**File:** `src/client/push.rs:155` (`handle_disconnect_push`)

**Current:**
```rust
let reconnect = disconnect.reconnect || codes::should_reconnect_on_disconnect(disconnect.code);
```

**Problem:** Neither Go nor JS consult the `reconnect` field. Rust's OR makes the client reconnect even when the server sends a terminal code with `reconnect=false`.

**Reference:**
- Go `client.go:897-904`: `reconnect := code < 3500 || code >= 5000 || (code >= 4000 && code < 4500)`
- JS `centrifuge.ts:1738-1745`: default `true`, set `false` if `(3500..4000) || (4500..5000)`

**Fix:**
```rust
let reconnect = codes::should_reconnect_on_disconnect(disconnect.code);
```

**Done when:** `handle_disconnect_push` ignores `disconnect.reconnect`; new unit test covers server sending `{code: 3500, reconnect: true}` → client goes to Disconnected (not Connecting).

---

### [ ] 2. Delta apply must branch on protocol type

**File:** `src/subscription.rs:210-231` (`SubState::apply_delta`)

**Problem:** `unwrap_json_string(pub_data)` runs for every protocol. For Protobuf + Fossil, if the raw delta bytes happen to parse as a JSON string literal (e.g. `"hello"`), the quotes get stripped and the delta is corrupted. The non-delta path has the same issue for `prev_data` bookkeeping.

**Reference:**
- Go `subscription.go:546-586` branches explicitly on `s.centrifuge.protocolType == protocol.TypeJSON` — JSON does `json.Unmarshal` into a string first, Protobuf uses `pub.Data` directly.
- JS has two separate implementations: `json.ts` `applyDeltaIfNeeded` vs `protobuf.codec.ts` `applyDeltaIfNeeded`.

**Fix sketch:**
- Plumb `ProtocolType` into `SubState` (add a field, set from `ClientConfig` in `ConnectionActor::new` and pass through `SubState::new`).
- Or take `protocol_type: ProtocolType` as a parameter to `apply_delta`.
- Branch: JSON → current unwrap path; Protobuf → skip unwrap, use raw bytes for delta input and `prev_data`.

**Done when:** `apply_delta` takes the protocol type into account; Protobuf path never calls `unwrap_json_string`; new unit tests cover both (a) JSON delta correctly unwrapped and (b) Protobuf delta bytes that happen to match a JSON string literal are preserved.

---

### [ ] 3. `remove_subscription` must emit `Unsubscribed`

**File:** `src/client/commands.rs:210-226` (`handle_remove_subscription`)

**Problem:** Sends the unsubscribe wire command but never emits `SubEvent::Unsubscribed`. Event-loop consumers miss it.

**Reference:**
- JS `centrifuge.ts:191-200`: if not unsubscribed, calls `sub.unsubscribe()` first (which emits), then removes from registry.
- Go `client.go:277-287`: returns an error if state isn't `Unsubscribed` — forces the caller to unsubscribe first.

**Fix (JS-style, matches existing Rust shape):**
- Before `self.subs.remove(&channel)`, if sub is Subscribed/Subscribing, call the same path `handle_unsubscribe` uses (emit `Unsubscribed(UNSUBSCRIBE_CALLED)`, drain subscribe_waiters, send unsubscribe wire command if connected).
- Then remove from the map.
- Also delete the dead `sub.state = SubscriptionState::Unsubscribed` after the move.
- Track the unsubscribe reply in `pending` (or route through `handle_unsubscribe`) so the server reply isn't logged as unknown.

**Done when:** Removing a Subscribed subscription emits `Unsubscribed` on its event channel before the channel is dropped; unit test covers it.

---

### [ ] 4. `handle_subscribe` must not re-emit `Subscribing`

**File:** `src/client/commands.rs:127-150` (`handle_subscribe`)

**Problem:** Lines 138-143 unconditionally set state, reset `resubscribe_attempts`, and emit `Subscribing`. If already `Subscribing`, duplicate event + backoff counter reset mid-backoff.

**Reference:**
- Go `subscription.go:401-407`: early return if `SubStateSubscribed || SubStateSubscribing`.
- JS `subscription.ts:319-322`: `_setSubscribing` early returns if `_isSubscribing()`.

**Fix:** After the `Subscribed` early-return, add:
```rust
if sub.state == SubscriptionState::Subscribing {
    sub.subscribe_waiters.push(reply);
    return;
}
```
Then the existing `sub.state = Subscribing; resubscribe_attempts = 0; emit Subscribing;` only runs for the Unsubscribed→Subscribing transition.

**Done when:** Calling `subscribe()` twice in a row on the same sub emits exactly one `Subscribing` event; both futures resolve once the server replies.

---

### [ ] 5. Resubscribe path must schedule a request timeout

**File:** `src/client/subscriptions.rs:211-228` (`do_subscribe`)

**Problem:** Inserts `PendingRequest::Subscribe` but doesn't call `self.schedule_request_timeout(id)`. If the server never replies to a resubscribe, the pending entry lives until the ping-timeout path eventually closes the transport.

**Compare with:** `handle_subscribe` at `commands.rs:160` — does call `schedule_request_timeout`.

**Fix:** Add `self.schedule_request_timeout(id);` after `self.pending.insert(...)` in `do_subscribe`.

**Done when:** Both subscribe paths schedule a timeout; unit test (mock transport that never replies) confirms the resubscribe path times out.

---

### [ ] 6. Empty byte fields must not serialize as JSON `null`

**File:** `src/codec.rs:20-30` (`embedded_json::serialize`)

**Current:**
```rust
if data.is_empty() { serializer.serialize_none() } else { ... }
```

**Problem:** Produces `"data": null`. Spec (`client_protocol.md`) describes byte fields as "embedded JSON" — says nothing about accepting `null`.

**Fix (preferred):** Add `skip_serializing_if = "Vec::is_empty"` via `build.rs` for the embedded-JSON byte fields so the field is omitted entirely when empty. Alternative: replace `serialize_none()` with the server-expected shape (check what Go/JS send for empty `data`).

**Investigate first:** Encode an empty publish via JS and Go, capture the wire JSON (docker-compose harness is in both `additionals/*-sdk-code/`). Match that shape.

**Done when:** An empty `data` field on publish/rpc/etc. serializes to the same shape as the reference SDKs; round-trip test passes.

---

## Phase 2 — Correctness edge cases

### [ ] 7. Batch queue must be cleared on disconnect

**Files:** `src/client/state.rs:50` (`move_to_disconnected`), `src/client/state.rs:89` (`move_to_closed`), `src/client/transport_io.rs:12` (`on_transport_close`)

**Problem:** `batch.queue` only drains in `flush_batch`. If disconnect happens mid-batch, stale bytes get flushed onto the next connection when `stop_batching()` is called — pending entries are already cleared by `fail_all_pending`, so replies are ignored.

Note: JS has the same structural quirk. Low-impact, but defensive.

**Fix:** In `move_to_disconnected` and `move_to_closed`, call `self.batch.queue.clear()`. Leave `batch.active` alone so the caller's paired `stop_batching()` still drains into the empty queue.

**Done when:** Reconnect + stop_batching after a mid-batch disconnect does not send anything on the new connection.

---

### [ ] 8. Fire-and-forget commands should not register fake pending entries

**Files:** `src/client/commands.rs:192-194` (unsubscribe), `src/client/subscriptions.rs:216` (resubscribe), `src/client/token.rs:58` (refresh), `src/client/token.rs:115` (sub_refresh)

**Problem:** Pattern `let (tx, _) = oneshot::channel(); self.pending.insert(id, ...); self.schedule_request_timeout(id);` — receiver dropped immediately, so timeout fires into nothing but still spawns a task.

**Fix (two options, pick one per callsite):**
1. Skip the pending bookkeeping entirely for true fire-and-forget sends (unsubscribe from `handle_unsubscribe`, token refreshes).
2. Keep the receiver and `await` or log the result (useful if you want retry-on-error for token refreshes).

Recommended: option 1 for unsubscribe/remove, option 2 for refresh paths so a server error response can trigger retry logic.

**Done when:** No `let (_, _) = oneshot::channel()` with an immediately dropped receiver combined with a scheduled timeout.

---

### [ ] 9. Replace `String::from_utf8_lossy` for outgoing JSON frames

**File:** `src/transport.rs:194`

**Current:**
```rust
Message::Text(String::from_utf8_lossy(&data).into_owned().into())
```

**Problem:** `serde_json` always emits valid UTF-8; `_lossy` would only fire on a codec bug and would silently corrupt the frame. Also allocates on the happy path.

**Fix:**
```rust
Message::Text(String::from_utf8(data).expect("json codec produces valid utf-8").into())
```

**Done when:** No `_lossy` UTF-8 conversion on the hot path.

---

### [ ] 10. `handle_server_unsubscribe` must back off resubscribes

**File:** `src/client/push.rs:100-104`

**Problem:** Bare `tokio::spawn` with no delay sends `Resubscribe` immediately. A misbehaving server can loop the client.

**Fix:** Route through `self.schedule_resubscribe(channel)` so the first attempt respects `min_resubscribe_delay` (with jitter).

**Done when:** Unsubscribe push with a reconnectable code triggers a delayed resubscribe, not an immediate one.

---

### [ ] 11. Use a defined code in `process_server_subs` unsubscribe

**File:** `src/client/server_subs.rs:54-58`

**Problem:** Emits `ServerUnsubscribed` with `code: 0`. Spec reserves server unsub codes `>= 2000`; codes `0-1999` are available for client-side reasons.

**Fix:** Add a constant, e.g. `codes::unsubscribed::SERVER_SUB_REMOVED: u32 = 3` (next unused in the 0-1999 range), and use it here.

**Done when:** No `code: 0` in emitted unsubscribe events; constants used consistently.

---

## Phase 3 — Cleanup / clippy

### [ ] 12. Drop `async` on functions that never await

**Files:**
- `src/client/connect.rs:321` `resolve_pending`
- `src/client/transport_io.rs:12` `on_transport_close`

**Fix:** Remove `async` and the `.await`s at the call sites. Run clippy pedantic (`-W clippy::unused_async`) to confirm.

---

### [ ] 13. Remove redundant deadline check

**File:** `src/client/connect.rs:257-267`

**Fix:** `time::sleep_until(deadline)` never returns early; delete the `if Instant::now() >= deadline` guard and just run the close-and-return logic.

---

### [ ] 14. Minor clippy cleanups

- `src/subscription.rs:190-191`: `config.since.as_ref().map_or(0, |s| s.offset)` (apply to both `offset` and `epoch`).
- `src/subscription.rs:219,228`: `self.prev_data.clone_from(&full_data)` instead of assign-clone.
- `src/transport.rs:75,77`: `u32::from(c)` instead of `c as u32`.

---

## Phase 4 — API polish (pre-1.0)

### [ ] 15. Make config callbacks `Arc<dyn Fn>` so `ClientConfig` is `Clone`

**File:** `src/config.rs`

Current `GetTokenFn = Box<dyn Fn(...) + Send + Sync>` prevents `ClientConfig` from deriving `Clone`. Users who want to build a config template and spawn multiple clients have to rebuild the callbacks every time.

**Fix:** Change each `Box<dyn Fn>` type alias to `Arc<dyn Fn>`, update helpers (`get_token_fn`, etc.) to return `Arc`, derive/implement `Clone` on `ClientConfig` and `SubscriptionConfig`.

---

### [ ] 16. Preserve transport error source

**File:** `src/errors.rs:17-18`

Current `CentrifugeError::Transport(Box<dyn Error + Send + Sync>)` drops the source when displayed as `{}`. Add `#[source]` or split into dedicated variants (ConnectionRefused, Tls, Io, …) so downstream error handlers can pattern-match.

---

### [ ] 17. Construction-time validation on config

**File:** `src/config.rs`

- `min_reconnect_delay <= max_reconnect_delay`
- `min_resubscribe_delay <= max_resubscribe_delay`
- Non-empty URL
- `timeout > 0`

Either `debug_assert!` in the builder methods or return `Result` from a terminal `.build()` step. Keep the existing `new(url)` → chain API ergonomic.

---

### [ ] 18. Add `# Errors` doc sections to public `async fn`

Clippy pedantic flagged dozens in `src/client/mod.rs` and `src/subscription.rs`. Document which `CentrifugeError` variants each method can return. Worth doing before a stable release.

---

### [ ] 19. Batch client-side subscribes into the handshake `ConnectRequest.subs`

**File:** `src/client/connect.rs:173-187` (`do_handshake`)

**Opportunity:** Cuts reconnect round-trips when a client has many subscriptions.

**Reference:** JS uses `startBatching/stopBatching` around `_sendSubscribeCommands` (`centrifuge.ts:1507-1509`) to batch all subscribes into one frame.

**Fix sketch:** Include client-side subs in `subs_map` alongside server subs, populating `SubscribeRequest` from `SubState` (token, data, positioned, recoverable, etc.). Server's `ConnectResult.subs` response handler (`process_server_subs`?) would need to route client-side channel results into `handle_subscribe_success` instead of the server-sub path — so the logic there needs splitting.

**Done when:** After reconnect with N client-side subs, one `Connect` command goes out instead of one `Connect` + N `Subscribe`s.

---

## Not action items — intentional divergences

- **Spec requires full reconnect on subscribe error code 100 (internal error).** Neither Go nor Rust implement this; Go just treats it as non-temporary → `moveToUnsubscribed`. Consistent ecosystem behavior, leave alone unless the spec wins this argument elsewhere.
- **`handle_subscribe_error` doesn't clear `subscribe_waiters` on temporary errors.** Intentional — waiters wait through backoff. Add a code comment noting the lifecycle; no behavior change.

---

## Suggested ordering

- **Sprint 1 (1-2 days):** Phase 1 items #1-#5 — all are small, behavioral, user-visible.
- **Sprint 2 (half day):** Phase 1 #6 after you capture reference wire shapes from docker-compose.
- **Sprint 3 (half day):** Phase 2 #7-#11.
- **Sprint 4:** Phase 3 cleanups — drop into a single "clippy pass" commit.
- **Before 1.0:** Phase 4 items, which change public API surface.
