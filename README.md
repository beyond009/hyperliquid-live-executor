# Hyperliquid Executor

Independent Rust execution service for Hyperliquid. `shadow` is the default. Explicit `testnet`
and heavily gated `tiny-mainnet` modes execute confirmed intents through a persistent single
writer; selecting a live mode can submit real exchange orders.

## Run

```bash
cargo run
```

Defaults:

- API: `127.0.0.1:31800`
- database: `data/executor.db`
- mode: `shadow`

Override with `HL_EXECUTOR_BIND`, `HL_EXECUTOR_DB`, and `RUST_LOG`. Set
`HL_EXECUTOR_RISK_POLICY` to a JSON policy path; `risk-policy.example.json` is the closed-strategy
template. Without it, the service uses tiny shadow limits. `tiny-mainnet` refuses that example
policy.

## Encrypted API wallet

Create a dedicated Hyperliquid API wallet. Do not use the master account private key. Encrypt its
private key from a hidden terminal prompt:

```bash
cargo run -- encrypt-key secrets/api-wallet.key.enc
```

The command asks for the private key, encryption password, and password confirmation. It uses
Argon2id plus AES-256-GCM, refuses to overwrite an existing file, and writes Unix permissions
`0600`. Never pass a private key or encryption password as a command-line argument or environment
variable.

Start and unlock the wallet interactively:

```bash
HL_EXECUTOR_KEY_FILE=secrets/api-wallet.key.enc cargo run
```

Startup asks `Executor key password:` without terminal echo. Files with group/other permissions or
symbolic links are rejected. Loading a wallet alone never changes the default `shadow` mode.

Testnet write mode requires the encrypted key, its public API-wallet address, and the account whose
positions are being managed:

```bash
HL_EXECUTOR_MODE=testnet \
HL_EXECUTOR_KEY_FILE=secrets/api-wallet.key.enc \
HL_API_WALLET_ADDRESS=0x... \
HL_ACCOUNT_ADDRESS=0x... \
cargo run
```

`tiny-mainnet` additionally requires `HL_MAINNET_ACCOUNT_ADDRESS` equal to
`HL_ACCOUNT_ADDRESS`, a non-example `HL_EXECUTOR_RISK_POLICY`, and the exact acknowledgement
`HL_TINY_MAINNET_ACK=I_UNDERSTAND_REAL_FUNDS_ARE_AT_RISK`. The decrypted key's derived address
must equal `HL_API_WALLET_ADDRESS`. Any unresolved entry or close outcome locks worker startup.

Set `HL_MAINNET_ACCOUNT_ADDRESS` to a public `0x...` master or sub-account address to enable the
official SDK's mainnet read-only synchronizer. It polls metadata, mids, clearinghouse state, and
open orders every five seconds, persists the latest 1000 snapshots, and makes `/ready` return 503
when a configured snapshot is missing, failed, internally inconsistent, or older than 15 seconds.
It also backfills recent fills and funding every minute into idempotent fact tables. No key is
required. WebSocket subscriptions for mids, fills, funding, and order updates feed the same fact
tables; mids act as a connection heartbeat, and stale WebSocket activity makes the service unready.
If the network requires an HTTP proxy for WebSockets, set `HL_WS_PROXY=http://host:port`. The
executor establishes an explicit HTTP CONNECT tunnel and rejects authenticated or non-HTTP proxy
URLs. REST continues to use the standard `HTTP_PROXY`/`HTTPS_PROXY` variables.
The first authoritative snapshot is imported as the account baseline. A later open order without
a fresh order update, or position-size change without a known fill, immediately halts new entries.
If the discrepancy survives the next poll after event backfill, the account persistently switches
to `close_only`. New intents and manual confirmations return 503 while either gate is active. There
is no automatic reset: after three consecutive clean snapshots, an operator must acknowledge the
exact account and reason through `POST /v1/control/reconciliation/acknowledge`.
For every nonzero position, the reader also requires full remaining-size coverage from opposing-side
`reduceOnly` trigger orders whose trigger price is on the stop-loss side of the current mid. Missing
or insufficient coverage enters `close_only` immediately; an order-status lookup that conflicts
with the open-order list fails the snapshot instead of assuming protection exists.

## Safety boundary

- TypeScript owns market research and signal generation.
- This service owns execution state, idempotency, risk decisions, and eventually Hyperliquid keys.
- A signal is an immutable `TradeIntent`; it is never treated as proof of an order or position.
- Every intent carries a contract version plus stable strategy, instance, signal, and account IDs.
- Idempotency keys are scoped by `strategyId`; reusing a key with different content is rejected.
- Strategy amounts are ceilings. The executor computes approved notional from its own policy and
  stop distance, then persists the complete risk decision with the intent.
- The execution database is private to this service and uses SQLite `WAL` with `synchronous=FULL`.
- A confirmed live intent is atomically claimed once. Account/strategy-instance `signalId`, entry
  `cloid`, and account/symbol active execution are independently deduplicated.
- Entry uses IOC. The worker queries order status by `cloid`, aggregates authoritative fills by
  `oid`, and creates reduce-only trigger protection from the actual filled size.
- Protection succeeds only after authoritative open orders prove full stop coverage. Failure uses
  a deterministic reduce-only IOC close; uncertain outcomes enter persistent reconciliation and
  `close_only` rather than being resent.
- Recovery acknowledgement is rejected unless the configured account, current reason code, and
  three-snapshot clean streak all match. Every successful acknowledgement is appended to the
  execution audit log.

The versioned contract is in `contracts/openapi.yaml`.

The local operations console consumes `/v1/console/stream` over WebSocket. The stream publishes a
fresh readiness/account projection every second plus cursor-based audit event deltas. Clients must
fall back to the HTTP endpoints when disconnected; the stream never carries private keys or order
signatures.

## Risk enforcement status

Enforced for every intent:

- account and strategy allowlists;
- server-side order notional, stop-risk, slippage, and stop-distance limits;
- executor-calculated approved notional and a persisted risk decision;
- global kill switch in the same admission transaction.
- SHA-256-bound manual confirmation with a two-minute maximum lifetime; confirmation rechecks the
  kill switch and live mode queues exactly one persistent execution.

The typed pre-trade engine also supports:

- total, symbol, and strategy exposure;
- position count, daily and consecutive losses, effective leverage, and liquidation distance;
- market-data age, spread, reference-price deviation, and top-of-book depth.

Live confirmation revalidates the intent and policy, requires a fresh reconciled account snapshot,
and rejects an existing position in the target symbol. The gateway repeats that position check
immediately before signing the IOC. Full spread/depth and daily-loss context still depends on the
upstream snapshots supplied to `evaluate_pre_trade`; keep mainnet policy limits conservative.
