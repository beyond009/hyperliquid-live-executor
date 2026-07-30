# Hyperliquid Execution Roadmap

This roadmap is ordered by safety dependency. A later milestone must not bypass an earlier
milestone's acceptance criteria.

## M0: Shadow execution core (complete)

- Versioned `TradeIntent` contract with decimal strings.
- Intent, order, and position transition rules.
- Durable idempotency keys and append-only event cursors.
- Strategy-scoped idempotency with payload-conflict detection.
- Atomic shadow admission, kill-switch decision, state update, and event persistence.
- Explicit intent and database schema versions with legacy shadow migration.
- Server-owned admission limits and persisted risk decisions.
- Persisted, expiring manual confirmation bound to the admitted intent and risk decision.
- Persistent kill switch.
- Local-only HTTP control API.
- No private key loading and no exchange order submission.

Acceptance:

- Duplicate idempotency keys return the original intent without another lifecycle event.
- Invalid exits, expired intents, and invalid risk limits are rejected before persistence.
- Restart preserves intents, event cursors, and kill-switch state.

## M1: TypeScript outbox and shadow projection

- Add an execution outbox table to the signal service.
- Persist the signal call and executor intent in one SQLite transaction.
- Dispatch pending intents with bounded retries and `Idempotency-Key`.
- Consume `/v1/events` with a durable cursor.
- Display executor state separately from simulated signal state.

Acceptance:

- Crashing before or after HTTP submission neither loses nor duplicates an intent.
- The signal service can rebuild its executor projection from cursor zero.
- The feature is disabled by default and does not alter Telegram or simulation behavior.

## M2: Read-only Hyperliquid adapter

Current progress: REST mainnet metadata, mids, clearinghouse state, open orders, fills, and funding
are implemented with the official Rust SDK. Snapshots and deduplicated history are persisted;
internal position-notional reconciliation and freshness are included in readiness. WebSocket
fills, funding, order updates, and a mids heartbeat feed the same deduplicated facts. Full
exchange-vs-local order lifecycle reconciliation now imports a startup baseline and detects
unexplained new orders and position-size changes. The resulting close-only state is persistent;
one poll is allowed for event-stream ordering before escalation. Read-only stop coverage now checks
full remaining-size `reduceOnly` trigger coverage against the current mid; automatic close-only
recovery remains. Manual recovery requires three clean snapshots, reason-bound operator
acknowledgement, and emits an audit event.

- Integrate the official Hyperliquid Rust SDK.
- Load mainnet meta and `szDecimals` without loading a private key.
- Subscribe to BBO/L2, active asset context, clearinghouse state, open orders, user fills,
  user funding, and order updates.
- Persist raw exchange events before applying projections.
- Add stale-feed and reconnect state to `/ready`.

Acceptance:

- Restart reconstructs the same account, position, order, fill, and funding projections.
- Gaps or stale subscriptions make the executor unready.
- Symbols absent from Hyperliquid or below execution-liquidity limits are rejected.

## M3: Mainnet API wallet and order submission

- Load an approved, dedicated API wallet from a secret provider.
- Implement a single-writer atomic nonce manager.
- Derive deterministic `cloid` values from intent and order role.
- Round price and size using live metadata.
- Submit aggressive IOC entries with a strict slippage cap.
- Resolve timeouts by `cloid` lookup before any retry.

Acceptance:

- No code path can submit twice for one `cloid`.
- Partial fills create positions only from confirmed fills.
- Rejections and timeouts are represented explicitly and are replayable.

## M4: Protective orders and reconciliation

- Place exchange-native reduce-only stop loss and take-profit orders from confirmed fill size.
- Track `open_unprotected` until protection is confirmed.
- Immediately reduce-only close when protection cannot be established.
- Reconcile local projections against clearinghouse state and open orders at startup and
  periodically.
- Add dead-man cancellation and automatic reduce-only handling while in close-only mode.

Acceptance:

- A protected position always has verified exchange-native stop coverage for its open size.
- Any unexplained account difference blocks new entries.
- Restart with an existing position restores protection before accepting new intents.

## M5: Account risk and tiny-mainnet mode

- Size by account equity, stop distance, and maximum risk.
- Enforce isolated leverage, total exposure, symbol exposure, daily loss, consecutive loss,
  liquidation distance, and maximum concurrent positions.
- Default to one position, 1x leverage, and a small hard notional cap.
- Require explicit operator confirmation until disabled by a separate audited setting.

Acceptance:

- Risk limits cannot be relaxed by a `TradeIntent`.
- Kill switch and close-only mode remain available when strategy services are offline.
- Every balance-changing action has a request, response, exchange event, and reconciliation trail.

## M6: High-frequency strategy runtime

- Keep HTTP as the control and slow-signal plane.
- Run sub-second strategies in the Rust process through bounded Tokio channels.
- Move persistence off the hot path while preserving ordered event journaling.
- Add backpressure, market-data freshness gates, latency histograms, and per-strategy limits.

Acceptance:

- A slow or failed strategy cannot delay reconciliation or protective-order handling.
- Queue saturation has an explicit drop/backpressure policy.
- Strategy modules cannot access signing keys or bypass the risk engine.
