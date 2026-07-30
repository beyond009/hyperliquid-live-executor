use std::sync::Mutex;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    domain::{ManualConfirmation, TradeIntent, TradeIntentStatus},
    live_execution::{
        ConfirmedFill, LiveExecution, LiveExecutionStatus, OrderRole, deterministic_cloid,
    },
    mainnet_read::{
        MainnetAccountSnapshot, MainnetFillRecord, MainnetFundingRecord, MainnetOrderUpdateRecord,
    },
    risk::RiskDecision,
};

const STORE_SCHEMA_VERSION: i64 = 9;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Normal,
    HaltNewEntries,
    CloseOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationState {
    pub account_address: String,
    pub mode: ExecutionMode,
    pub ready: bool,
    pub reason_code: Option<String>,
    pub detail: Option<String>,
    pub baseline_established_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
    pub clean_streak: u32,
    pub recovery_eligible: bool,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid stored UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
    #[error("invalid stored timestamp")]
    InvalidTimestamp,
    #[error("invalid stored status: {0}")]
    InvalidStatus(String),
    #[error("illegal intent transition from {from:?} to {to:?}")]
    IllegalTransition {
        from: TradeIntentStatus,
        to: TradeIntentStatus,
    },
    #[error("idempotency key was already used with a different trade intent")]
    IdempotencyConflict,
    #[error("intentId was already used by another request")]
    IntentIdConflict,
    #[error("signalId was already admitted for this account and strategy instance")]
    SignalReplay,
    #[error("intent state changed concurrently")]
    ConcurrentTransition,
    #[error("database schema version {0} is newer than this executor supports")]
    UnsupportedDatabaseVersion(i64),
    #[error("manual confirmation digest does not match the admitted intent")]
    ConfirmationMismatch,
    #[error("manual confirmation has expired")]
    ConfirmationExpired,
    #[error("intent is not awaiting manual confirmation")]
    NotAwaitingConfirmation,
    #[error("account is not in close-only mode")]
    ReconciliationNotCloseOnly,
    #[error("close-only recovery requires three consecutive clean snapshots")]
    ReconciliationRecoveryNotEligible,
    #[error("reconciliation reason changed; refresh status before acknowledging")]
    ReconciliationReasonMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StoredIntent {
    pub intent: TradeIntent,
    pub idempotency_key: String,
    pub status: TradeIntentStatus,
    pub status_reason: Option<String>,
    pub risk_decision: Option<RiskDecision>,
    pub manual_confirmation: Option<ManualConfirmation>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionEvent {
    pub cursor: i64,
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    #[serde(rename = "type")]
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub data: Value,
}

pub struct ExecutionStore {
    connection: Mutex<Connection>,
}

impl ExecutionStore {
    pub fn open(path: &str) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let schema_version = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if schema_version > STORE_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedDatabaseVersion(schema_version));
        }
        let has_trade_intents = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'trade_intents')",
                [],
                |row| row.get::<_, bool>(0),
            )?;
        let has_strategy_id = has_trade_intents && {
            let mut statement = connection.prepare("PRAGMA table_info(trade_intents)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .any(|column| column == "strategy_id")
        };
        if has_trade_intents && !has_strategy_id {
            let transaction = connection.transaction()?;
            transaction.execute_batch(
                "ALTER TABLE trade_intents RENAME TO trade_intents_legacy;
                 CREATE TABLE trade_intents (
                    intent_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    status_reason TEXT,
                    risk_decision_json TEXT,
                    confirmation_digest TEXT,
                    confirmation_expires_at TEXT,
                    confirmed_at TEXT,
                    confirmed_by TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(strategy_id, idempotency_key)
                 );
                 INSERT INTO trade_intents
                    (intent_id, strategy_id, idempotency_key, payload_json, status, status_reason, risk_decision_json, confirmation_digest, confirmation_expires_at, confirmed_at, confirmed_by, created_at, updated_at)
                 SELECT intent_id, 'legacy', idempotency_key,
                    json_set(payload_json,
                        '$.schemaVersion', 1,
                        '$.strategyId', 'legacy',
                        '$.strategyInstanceId', 'legacy',
                        '$.signalId', intent_id,
                        '$.accountId', 'default',
                        '$.createdAt', created_at),
                    status, status_reason, NULL, NULL, NULL, NULL, NULL, created_at, updated_at
                 FROM trade_intents_legacy;
                 DROP TABLE trade_intents_legacy;",
            )?;
            transaction.commit()?;
        }
        let has_risk_decision = {
            let mut statement = connection.prepare("PRAGMA table_info(trade_intents)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .any(|column| column == "risk_decision_json")
        };
        if has_trade_intents && !has_risk_decision {
            connection.execute(
                "ALTER TABLE trade_intents ADD COLUMN risk_decision_json TEXT",
                [],
            )?;
        }
        if has_trade_intents {
            let columns = {
                let mut statement = connection.prepare("PRAGMA table_info(trade_intents)")?;
                statement
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            for column in [
                "confirmation_digest",
                "confirmation_expires_at",
                "confirmed_at",
                "confirmed_by",
            ] {
                if !columns.iter().any(|existing| existing == column) {
                    connection.execute(
                        &format!("ALTER TABLE trade_intents ADD COLUMN {column} TEXT"),
                        [],
                    )?;
                }
            }
        }
        let has_mainnet_funding = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mainnet_funding')",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if schema_version < 6 && has_mainnet_funding {
            let transaction = connection.transaction()?;
            transaction.execute_batch(
                "ALTER TABLE mainnet_funding RENAME TO mainnet_funding_v5;
                 CREATE TABLE mainnet_funding (
                    account_address TEXT NOT NULL,
                    exchange_hash TEXT NOT NULL,
                    time_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    usdc TEXT NOT NULL,
                    signed_size TEXT NOT NULL,
                    funding_rate TEXT NOT NULL,
                    PRIMARY KEY(account_address, time_ms, coin, usdc, signed_size, funding_rate)
                 );
                 INSERT OR IGNORE INTO mainnet_funding
                    SELECT account_address, exchange_hash, time_ms, coin, usdc, signed_size, funding_rate
                    FROM mainnet_funding_v5;
                 DROP TABLE mainnet_funding_v5;",
            )?;
            transaction.commit()?;
        }
        if schema_version == 7 {
            connection.execute(
                "ALTER TABLE mainnet_reconciliation_state ADD COLUMN clean_streak INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS trade_intents (
                intent_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                status_reason TEXT,
                risk_decision_json TEXT,
                confirmation_digest TEXT,
                confirmation_expires_at TEXT,
                confirmed_at TEXT,
                confirmed_by TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(strategy_id, idempotency_key)
            );
            CREATE TABLE IF NOT EXISTS execution_events (
                cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                data_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_execution_events_aggregate
                ON execution_events(aggregate_id, cursor);
            CREATE TABLE IF NOT EXISTS executor_control (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                kill_switch_enabled INTEGER NOT NULL,
                reason TEXT,
                updated_at TEXT NOT NULL
            );
            INSERT OR IGNORE INTO executor_control(singleton, kill_switch_enabled, reason, updated_at)
                VALUES (1, 0, NULL, '1970-01-01T00:00:00Z');
            CREATE TABLE IF NOT EXISTS mainnet_account_snapshots (
                cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                account_address TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                data_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_mainnet_snapshots_account_cursor
                ON mainnet_account_snapshots(account_address, cursor DESC);
            CREATE TABLE IF NOT EXISTS mainnet_fills (
                account_address TEXT NOT NULL,
                exchange_hash TEXT NOT NULL,
                oid INTEGER NOT NULL,
                time_ms INTEGER NOT NULL,
                coin TEXT NOT NULL,
                side TEXT NOT NULL,
                direction TEXT NOT NULL,
                crossed INTEGER NOT NULL,
                price TEXT NOT NULL,
                size TEXT NOT NULL,
                closed_pnl_usd TEXT NOT NULL,
                fee_usd TEXT NOT NULL,
                PRIMARY KEY(account_address, exchange_hash, oid, time_ms, price, size)
            );
            CREATE INDEX IF NOT EXISTS idx_mainnet_fills_account_time
                ON mainnet_fills(account_address, time_ms);
            CREATE TABLE IF NOT EXISTS mainnet_funding (
                account_address TEXT NOT NULL,
                exchange_hash TEXT NOT NULL,
                time_ms INTEGER NOT NULL,
                coin TEXT NOT NULL,
                usdc TEXT NOT NULL,
                signed_size TEXT NOT NULL,
                funding_rate TEXT NOT NULL,
                PRIMARY KEY(account_address, time_ms, coin, usdc, signed_size, funding_rate)
            );
            CREATE INDEX IF NOT EXISTS idx_mainnet_funding_account_time
                ON mainnet_funding(account_address, time_ms);
            CREATE TABLE IF NOT EXISTS mainnet_order_updates (
                account_address TEXT NOT NULL,
                oid INTEGER NOT NULL,
                status TEXT NOT NULL,
                status_timestamp_ms INTEGER NOT NULL,
                coin TEXT NOT NULL,
                side TEXT NOT NULL,
                limit_price TEXT NOT NULL,
                size TEXT NOT NULL,
                original_size TEXT NOT NULL,
                cloid TEXT,
                PRIMARY KEY(account_address, oid, status, status_timestamp_ms)
            );
            CREATE INDEX IF NOT EXISTS idx_mainnet_order_updates_account_time
                ON mainnet_order_updates(account_address, status_timestamp_ms);
            CREATE TABLE IF NOT EXISTS mainnet_reconciliation_state (
                account_address TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                ready INTEGER NOT NULL,
                reason_code TEXT,
                detail TEXT,
                baseline_established_at TEXT,
                last_reconciled_at TEXT,
                clean_streak INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS mainnet_order_projection (
                account_address TEXT NOT NULL,
                oid INTEGER NOT NULL,
                coin TEXT NOT NULL,
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY(account_address, oid)
            );
            CREATE TABLE IF NOT EXISTS mainnet_position_projection (
                account_address TEXT NOT NULL,
                coin TEXT NOT NULL,
                signed_size TEXT NOT NULL,
                source TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY(account_address, coin)
            );
            CREATE TABLE IF NOT EXISTS live_executions (
                intent_id TEXT PRIMARY KEY REFERENCES trade_intents(intent_id),
                account_address TEXT NOT NULL,
                entry_cloid TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_live_executions_status_created
                ON live_executions(status, created_at);
            CREATE TABLE IF NOT EXISTS live_orders (
                cloid TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL REFERENCES live_executions(intent_id),
                role TEXT NOT NULL,
                exchange_oid INTEGER,
                status TEXT NOT NULL,
                requested_size TEXT NOT NULL,
                filled_size TEXT NOT NULL DEFAULT '0',
                average_fill_price TEXT,
                response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(intent_id, role)
            );
            ",
        )?;
        connection.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
        Ok(())
    }

    pub fn create_intent(
        &self,
        intent: &TradeIntent,
        idempotency_key: &str,
    ) -> Result<(StoredIntent, bool), StoreError> {
        let now = Utc::now();
        let payload = serde_json::to_string(intent)?;
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            find_by_idempotency_key_tx(&transaction, &intent.strategy_id, idempotency_key)?
        {
            if existing.intent == *intent {
                return Ok((existing, false));
            }
            return Err(StoreError::IdempotencyConflict);
        }
        if get_intent_tx(&transaction, intent.intent_id)?.is_some() {
            return Err(StoreError::IntentIdConflict);
        }
        reject_signal_replay_tx(&transaction, intent)?;
        transaction.execute(
            "INSERT INTO trade_intents
             (intent_id, strategy_id, idempotency_key, payload_json, status, status_reason, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'received', NULL, ?5, ?5)",
            params![intent.intent_id.to_string(), intent.strategy_id, idempotency_key, payload, now.to_rfc3339()],
        )?;
        insert_event_tx(
            &transaction,
            intent.intent_id,
            "intent.received",
            serde_json::json!({ "idempotencyKey": idempotency_key }),
            now,
        )?;
        transaction.commit()?;
        Ok((
            StoredIntent {
                intent: intent.clone(),
                idempotency_key: idempotency_key.to_owned(),
                status: TradeIntentStatus::Received,
                status_reason: None,
                risk_decision: None,
                manual_confirmation: None,
                created_at: now,
                updated_at: now,
            },
            true,
        ))
    }

    pub fn submit_shadow_intent(
        &self,
        intent: &TradeIntent,
        idempotency_key: &str,
        risk_decision: &RiskDecision,
        confirmation: Option<&ManualConfirmation>,
    ) -> Result<(StoredIntent, bool), StoreError> {
        let now = Utc::now();
        let payload = serde_json::to_string(intent)?;
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            find_by_idempotency_key_tx(&transaction, &intent.strategy_id, idempotency_key)?
        {
            if existing.intent == *intent {
                return Ok((existing, false));
            }
            return Err(StoreError::IdempotencyConflict);
        }
        if get_intent_tx(&transaction, intent.intent_id)?.is_some() {
            return Err(StoreError::IntentIdConflict);
        }
        reject_signal_replay_tx(&transaction, intent)?;
        let kill_switch_enabled = transaction.query_row(
            "SELECT kill_switch_enabled FROM executor_control WHERE singleton = 1",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        let next = if kill_switch_enabled || !risk_decision.is_approved() {
            TradeIntentStatus::RiskRejected
        } else if confirmation.is_some() {
            TradeIntentStatus::AwaitingConfirmation
        } else {
            TradeIntentStatus::ShadowAccepted
        };
        let reason = if kill_switch_enabled {
            "kill_switch_enabled"
        } else if !risk_decision.is_approved() {
            "risk_policy_rejected"
        } else if confirmation.is_some() {
            "awaiting_manual_confirmation"
        } else {
            "shadow_mode_no_order_submitted"
        };
        transaction.execute(
            "INSERT INTO trade_intents
             (intent_id, strategy_id, idempotency_key, payload_json, status, status_reason,
              risk_decision_json, confirmation_digest, confirmation_expires_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
            params![
                intent.intent_id.to_string(),
                intent.strategy_id,
                idempotency_key,
                payload,
                status_to_str(next),
                reason,
                serde_json::to_string(risk_decision)?,
                confirmation.map(|value| value.digest.as_str()),
                confirmation.map(|value| value.expires_at.to_rfc3339()),
                now.to_rfc3339(),
            ],
        )?;
        insert_event_tx(
            &transaction,
            intent.intent_id,
            "intent.received",
            serde_json::json!({
                "strategyId": intent.strategy_id,
                "idempotencyKey": idempotency_key
            }),
            now,
        )?;
        insert_event_tx(
            &transaction,
            intent.intent_id,
            &format!("intent.{}", status_to_str(next)),
            serde_json::json!({ "reason": reason, "riskDecision": risk_decision }),
            now,
        )?;
        transaction.commit()?;
        let stored = StoredIntent {
            intent: intent.clone(),
            idempotency_key: idempotency_key.to_owned(),
            status: next,
            status_reason: Some(reason.to_owned()),
            risk_decision: Some(risk_decision.clone()),
            manual_confirmation: confirmation.cloned(),
            created_at: now,
            updated_at: now,
        };
        Ok((stored, true))
    }

    pub fn transition_intent(
        &self,
        intent_id: Uuid,
        next: TradeIntentStatus,
        reason: Option<&str>,
    ) -> Result<Option<StoredIntent>, StoreError> {
        let now = Utc::now();
        let status = status_to_str(next);
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(current) = get_intent_tx(&transaction, intent_id)? else {
            return Ok(None);
        };
        if !current.status.can_transition_to(next) {
            return Err(StoreError::IllegalTransition {
                from: current.status,
                to: next,
            });
        }
        let changed = transaction.execute(
            "UPDATE trade_intents SET status = ?1, status_reason = ?2, updated_at = ?3
             WHERE intent_id = ?4 AND status = ?5",
            params![
                status,
                reason,
                now.to_rfc3339(),
                intent_id.to_string(),
                status_to_str(current.status)
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::ConcurrentTransition);
        }
        insert_event_tx(
            &transaction,
            intent_id,
            &format!("intent.{status}"),
            serde_json::json!({ "reason": reason }),
            now,
        )?;
        let stored = get_intent_tx(&transaction, intent_id)?;
        transaction.commit()?;
        Ok(stored)
    }

    pub fn confirm_intent(
        &self,
        intent_id: Uuid,
        digest: &str,
        confirmed_by: &str,
    ) -> Result<StoredIntent, StoreError> {
        let now = Utc::now();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            get_intent_tx(&transaction, intent_id)?.ok_or(StoreError::NotAwaitingConfirmation)?;
        let confirmation = current
            .manual_confirmation
            .as_ref()
            .ok_or(StoreError::NotAwaitingConfirmation)?;
        if confirmation.digest != digest {
            return Err(StoreError::ConfirmationMismatch);
        }
        if current.status == TradeIntentStatus::ShadowAccepted
            && confirmation.confirmed_at.is_some()
        {
            return Ok(current);
        }
        if current.status != TradeIntentStatus::AwaitingConfirmation {
            return Err(StoreError::NotAwaitingConfirmation);
        }
        if confirmation.expires_at <= now || current.intent.expires_at <= now {
            transaction.execute(
                "UPDATE trade_intents SET status = 'risk_rejected', status_reason = 'confirmation_expired', updated_at = ?1
                 WHERE intent_id = ?2 AND status = 'awaiting_confirmation'",
                params![now.to_rfc3339(), intent_id.to_string()],
            )?;
            insert_event_tx(
                &transaction,
                intent_id,
                "intent.risk_rejected",
                serde_json::json!({ "reason": "confirmation_expired" }),
                now,
            )?;
            transaction.commit()?;
            return Err(StoreError::ConfirmationExpired);
        }
        let kill_switch_enabled = transaction.query_row(
            "SELECT kill_switch_enabled FROM executor_control WHERE singleton = 1",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if kill_switch_enabled {
            transaction.execute(
                "UPDATE trade_intents SET status = 'risk_rejected', status_reason = 'kill_switch_enabled', updated_at = ?1
                 WHERE intent_id = ?2 AND status = 'awaiting_confirmation'",
                params![now.to_rfc3339(), intent_id.to_string()],
            )?;
            insert_event_tx(
                &transaction,
                intent_id,
                "intent.risk_rejected",
                serde_json::json!({ "reason": "kill_switch_enabled" }),
                now,
            )?;
            let stored = get_intent_tx(&transaction, intent_id)?
                .expect("intent exists in confirmation transaction");
            transaction.commit()?;
            return Ok(stored);
        }
        let changed = transaction.execute(
            "UPDATE trade_intents
             SET status = 'shadow_accepted', status_reason = 'manually_confirmed_shadow_only',
                 confirmed_at = ?1, confirmed_by = ?2, updated_at = ?1
             WHERE intent_id = ?3 AND status = 'awaiting_confirmation'",
            params![now.to_rfc3339(), confirmed_by, intent_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::ConcurrentTransition);
        }
        insert_event_tx(
            &transaction,
            intent_id,
            "intent.manually_confirmed",
            serde_json::json!({ "digest": digest, "confirmedBy": confirmed_by }),
            now,
        )?;
        insert_event_tx(
            &transaction,
            intent_id,
            "intent.shadow_accepted",
            serde_json::json!({ "reason": "manually_confirmed_shadow_only" }),
            now,
        )?;
        let stored = get_intent_tx(&transaction, intent_id)?
            .expect("intent exists in confirmation transaction");
        transaction.commit()?;
        Ok(stored)
    }

    pub fn confirm_intent_for_execution(
        &self,
        intent_id: Uuid,
        digest: &str,
        confirmed_by: &str,
        account_address: &str,
    ) -> Result<StoredIntent, StoreError> {
        let now = Utc::now();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            get_intent_tx(&transaction, intent_id)?.ok_or(StoreError::NotAwaitingConfirmation)?;
        let confirmation = current
            .manual_confirmation
            .as_ref()
            .ok_or(StoreError::NotAwaitingConfirmation)?;
        if confirmation.digest != digest {
            return Err(StoreError::ConfirmationMismatch);
        }
        if current.status == TradeIntentStatus::Approved && confirmation.confirmed_at.is_some() {
            return Ok(current);
        }
        if current.status != TradeIntentStatus::AwaitingConfirmation {
            return Err(StoreError::NotAwaitingConfirmation);
        }
        if confirmation.expires_at <= now || current.intent.expires_at <= now {
            return Err(StoreError::ConfirmationExpired);
        }
        let kill_switch_enabled = transaction.query_row(
            "SELECT kill_switch_enabled FROM executor_control WHERE singleton = 1",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if kill_switch_enabled {
            return Err(StoreError::ConcurrentTransition);
        }
        let entry_cloid = deterministic_cloid(intent_id, OrderRole::Entry);
        let changed = transaction.execute(
            "UPDATE trade_intents
             SET status = 'approved', status_reason = 'manually_confirmed_for_execution',
                 confirmed_at = ?1, confirmed_by = ?2, updated_at = ?1
             WHERE intent_id = ?3 AND status = 'awaiting_confirmation'",
            params![now.to_rfc3339(), confirmed_by, intent_id.to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::ConcurrentTransition);
        }
        transaction.execute(
            "INSERT INTO live_executions
             (intent_id, account_address, entry_cloid, status, attempt, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'pending', 0, ?4, ?4)",
            params![
                intent_id.to_string(),
                account_address,
                entry_cloid.to_string(),
                now.to_rfc3339()
            ],
        )?;
        insert_event_tx(
            &transaction,
            intent_id,
            "intent.manually_confirmed",
            serde_json::json!({ "digest": digest, "confirmedBy": confirmed_by }),
            now,
        )?;
        insert_event_tx(
            &transaction,
            intent_id,
            "execution.created",
            serde_json::json!({
                "accountAddress": account_address,
                "entryCloid": entry_cloid
            }),
            now,
        )?;
        let stored = get_intent_tx(&transaction, intent_id)?
            .expect("intent exists in live confirmation transaction");
        transaction.commit()?;
        Ok(stored)
    }

    pub fn claim_next_live_execution(
        &self,
        account_address: &str,
    ) -> Result<Option<(LiveExecution, StoredIntent)>, StoreError> {
        let now = Utc::now();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent_id = transaction
            .query_row(
                "SELECT candidate.intent_id FROM live_executions candidate
                 JOIN trade_intents candidate_intent ON candidate_intent.intent_id = candidate.intent_id
                 WHERE candidate.account_address = ?1 AND candidate.status = 'pending'
                   AND NOT EXISTS (
                     SELECT 1 FROM live_executions active
                     JOIN trade_intents active_intent ON active_intent.intent_id = active.intent_id
                     WHERE active.account_address = candidate.account_address
                       AND active.intent_id != candidate.intent_id
                       AND active.status NOT IN ('pending', 'closed', 'failed')
                       AND json_extract(active_intent.payload_json, '$.symbol') =
                           json_extract(candidate_intent.payload_json, '$.symbol')
                   )
                 ORDER BY candidate.created_at LIMIT 1",
                [account_address],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(intent_id) = intent_id else {
            return Ok(None);
        };
        let intent_uuid = Uuid::parse_str(&intent_id)?;
        let execution_changed = transaction.execute(
            "UPDATE live_executions SET status = 'entry_submitting', attempt = attempt + 1,
             updated_at = ?1 WHERE intent_id = ?2 AND status = 'pending'",
            params![now.to_rfc3339(), intent_id],
        )?;
        let intent_changed = transaction.execute(
            "UPDATE trade_intents SET status = 'submitting', status_reason = 'entry_claimed',
             updated_at = ?1 WHERE intent_id = ?2 AND status = 'approved'",
            params![now.to_rfc3339(), intent_id],
        )?;
        if execution_changed != 1 || intent_changed != 1 {
            return Err(StoreError::ConcurrentTransition);
        }
        insert_event_tx(
            &transaction,
            intent_uuid,
            "execution.entry_claimed",
            serde_json::json!({}),
            now,
        )?;
        let execution = get_live_execution_tx(&transaction, intent_uuid)?
            .expect("claimed live execution exists");
        let intent = get_intent_tx(&transaction, intent_uuid)?.expect("claimed live intent exists");
        transaction.commit()?;
        Ok(Some((execution, intent)))
    }

    pub fn get_live_execution(&self, intent_id: Uuid) -> Result<Option<LiveExecution>, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        get_live_execution_connection(&connection, intent_id)
    }

    pub fn unresolved_live_execution_count(
        &self,
        account_address: &str,
    ) -> Result<u64, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        connection
            .query_row(
                "SELECT COUNT(*) FROM live_executions
                 WHERE account_address = ?1 AND status IN
                   ('entry_submitting', 'entry_unknown', 'emergency_closing', 'reconciliation_required')",
                [account_address],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    pub fn mark_live_execution_failed(
        &self,
        intent_id: Uuid,
        reason: &str,
        detail: &str,
    ) -> Result<(), StoreError> {
        self.finish_live_transition(
            intent_id,
            LiveExecutionStatus::EntrySubmitting,
            LiveExecutionStatus::Failed,
            TradeIntentStatus::Submitting,
            TradeIntentStatus::Failed,
            reason,
            Some(detail),
            false,
        )
    }

    pub fn mark_live_entry_unknown(&self, intent_id: Uuid, detail: &str) -> Result<(), StoreError> {
        self.finish_live_transition(
            intent_id,
            LiveExecutionStatus::EntrySubmitting,
            LiveExecutionStatus::EntryUnknown,
            TradeIntentStatus::Submitting,
            TradeIntentStatus::Failed,
            "entry_outcome_unknown",
            Some(detail),
            true,
        )
    }

    pub fn record_live_entry_fill(
        &self,
        intent_id: Uuid,
        fill: &ConfirmedFill,
    ) -> Result<(), StoreError> {
        let now = Utc::now();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let execution = get_live_execution_tx(&transaction, intent_id)?
            .ok_or(StoreError::ConcurrentTransition)?;
        if execution.status != LiveExecutionStatus::EntrySubmitting {
            return Err(StoreError::ConcurrentTransition);
        }
        transaction.execute(
            "INSERT INTO live_orders
             (cloid, intent_id, role, exchange_oid, status, requested_size, filled_size,
              average_fill_price, created_at, updated_at)
             VALUES (?1, ?2, 'entry', ?3, 'filled', ?4, ?4, ?5, ?6, ?6)",
            params![
                execution.entry_cloid.to_string(),
                intent_id.to_string(),
                fill.exchange_oid,
                fill.size.to_string(),
                fill.average_price.to_string(),
                now.to_rfc3339()
            ],
        )?;
        update_live_and_intent_tx(
            &transaction,
            intent_id,
            LiveExecutionStatus::EntrySubmitting,
            LiveExecutionStatus::EntryFilled,
            TradeIntentStatus::Submitting,
            TradeIntentStatus::Executing,
            "entry_fill_confirmed",
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_live_execution_protected(&self, intent_id: Uuid) -> Result<(), StoreError> {
        self.finish_live_transition(
            intent_id,
            LiveExecutionStatus::EntryFilled,
            LiveExecutionStatus::Protected,
            TradeIntentStatus::Executing,
            TradeIntentStatus::Completed,
            "protection_verified",
            None,
            false,
        )
    }

    pub fn record_live_protection_orders(
        &self,
        intent: &TradeIntent,
        fill: &ConfirmedFill,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let execution = get_live_execution_tx(&transaction, intent.intent_id)?
            .ok_or(StoreError::ConcurrentTransition)?;
        if execution.status != LiveExecutionStatus::EntryFilled {
            return Err(StoreError::ConcurrentTransition);
        }
        transaction.execute(
            "INSERT INTO live_orders
             (cloid, intent_id, role, status, requested_size, created_at, updated_at)
             VALUES (?1, ?2, 'stop_loss', 'open_verified', ?3, ?4, ?4)",
            params![
                deterministic_cloid(intent.intent_id, OrderRole::StopLoss).to_string(),
                intent.intent_id.to_string(),
                fill.size.to_string(),
                now
            ],
        )?;
        for (index, target) in intent.exit.take_profit.iter().enumerate() {
            let size = fill.size * Decimal::from(target.position_pct) / Decimal::ONE_HUNDRED;
            let role = OrderRole::TakeProfit(index as u8).stable_name();
            transaction.execute(
                "INSERT INTO live_orders
                 (cloid, intent_id, role, status, requested_size, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'open_verified', ?4, ?5, ?5)",
                params![
                    deterministic_cloid(intent.intent_id, OrderRole::TakeProfit(index as u8))
                        .to_string(),
                    intent.intent_id.to_string(),
                    role,
                    size.to_string(),
                    now
                ],
            )?;
        }
        insert_event_tx(
            &transaction,
            intent.intent_id,
            "execution.protection_orders_verified",
            serde_json::json!({ "coveredSize": fill.size }),
            Utc::now(),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_live_emergency_closing(
        &self,
        intent_id: Uuid,
        detail: &str,
    ) -> Result<(), StoreError> {
        self.finish_live_transition(
            intent_id,
            LiveExecutionStatus::EntryFilled,
            LiveExecutionStatus::EmergencyClosing,
            TradeIntentStatus::Executing,
            TradeIntentStatus::Executing,
            "protection_failed",
            Some(detail),
            true,
        )
    }

    pub fn mark_live_execution_closed(&self, intent_id: Uuid) -> Result<(), StoreError> {
        self.finish_live_transition(
            intent_id,
            LiveExecutionStatus::EmergencyClosing,
            LiveExecutionStatus::Closed,
            TradeIntentStatus::Executing,
            TradeIntentStatus::Failed,
            "unprotected_entry_emergency_closed",
            None,
            true,
        )
    }

    pub fn mark_live_reconciliation_required(
        &self,
        intent_id: Uuid,
        detail: &str,
    ) -> Result<(), StoreError> {
        self.finish_live_transition(
            intent_id,
            LiveExecutionStatus::EmergencyClosing,
            LiveExecutionStatus::ReconciliationRequired,
            TradeIntentStatus::Executing,
            TradeIntentStatus::Failed,
            "emergency_close_outcome_unknown",
            Some(detail),
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_live_transition(
        &self,
        intent_id: Uuid,
        expected_live: LiveExecutionStatus,
        next_live: LiveExecutionStatus,
        expected_intent: TradeIntentStatus,
        next_intent: TradeIntentStatus,
        reason: &str,
        detail: Option<&str>,
        force_close_only: bool,
    ) -> Result<(), StoreError> {
        let now = Utc::now();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        update_live_and_intent_tx(
            &transaction,
            intent_id,
            expected_live,
            next_live,
            expected_intent,
            next_intent,
            reason,
            detail,
            now,
        )?;
        if force_close_only {
            let account = get_live_execution_tx(&transaction, intent_id)?
                .ok_or(StoreError::ConcurrentTransition)?
                .account_address;
            transaction.execute(
                "INSERT INTO mainnet_reconciliation_state
                 (account_address, mode, ready, reason_code, detail, clean_streak)
                 VALUES (?1, 'close_only', 0, ?2, ?3, 0)
                 ON CONFLICT(account_address) DO UPDATE SET mode = 'close_only', ready = 0,
                   reason_code = excluded.reason_code, detail = excluded.detail, clean_streak = 0",
                params![account, reason, detail],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn get_intent(&self, intent_id: Uuid) -> Result<Option<StoredIntent>, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let row = connection
            .query_row(
                "SELECT payload_json, idempotency_key, status, status_reason, risk_decision_json,
                        confirmation_digest, confirmation_expires_at, confirmed_at, confirmed_by,
                        created_at, updated_at
                 FROM trade_intents WHERE intent_id = ?1",
                [intent_id.to_string()],
                map_stored_intent,
            )
            .optional()?;
        row.map(parse_stored_intent).transpose()
    }

    pub fn events_after(
        &self,
        after: i64,
        limit: usize,
    ) -> Result<Vec<ExecutionEvent>, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT cursor, event_id, aggregate_id, event_type, occurred_at, data_json
             FROM execution_events WHERE cursor > ?1 ORDER BY cursor ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![after, limit.min(500) as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        rows.map(|row| {
            let (cursor, event_id, aggregate_id, event_type, occurred_at, data_json) = row?;
            Ok(ExecutionEvent {
                cursor,
                event_id: Uuid::parse_str(&event_id)?,
                aggregate_id: Uuid::parse_str(&aggregate_id)?,
                event_type,
                occurred_at: DateTime::parse_from_rfc3339(&occurred_at)
                    .map_err(|_| StoreError::InvalidTimestamp)?
                    .with_timezone(&Utc),
                data: serde_json::from_str(&data_json)?,
            })
        })
        .collect()
    }

    pub fn set_kill_switch(&self, enabled: bool, reason: Option<&str>) -> Result<(), StoreError> {
        let now = Utc::now();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE executor_control SET kill_switch_enabled = ?1, reason = ?2, updated_at = ?3 WHERE singleton = 1",
            params![enabled, reason, now.to_rfc3339()],
        )?;
        insert_event_tx(
            &transaction,
            Uuid::nil(),
            if enabled {
                "control.kill_switch_enabled"
            } else {
                "control.kill_switch_disabled"
            },
            serde_json::json!({ "reason": reason }),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn kill_switch_enabled(&self) -> Result<bool, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        Ok(connection.query_row(
            "SELECT kill_switch_enabled FROM executor_control WHERE singleton = 1",
            [],
            |row| row.get::<_, bool>(0),
        )?)
    }

    pub fn record_mainnet_snapshot(
        &self,
        snapshot: &MainnetAccountSnapshot,
    ) -> Result<ReconciliationState, StoreError> {
        let account_address = &snapshot.account_address;
        let observed_at = snapshot.observed_at;
        let data = serde_json::to_value(snapshot)?;
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO mainnet_account_snapshots(account_address, observed_at, data_json)
             VALUES (?1, ?2, ?3)",
            params![
                account_address,
                observed_at.to_rfc3339(),
                serde_json::to_string(&data)?
            ],
        )?;
        let previous = reconciliation_state_tx(&transaction, account_address)?;
        let baseline = previous
            .as_ref()
            .and_then(|state| state.baseline_established_at)
            .is_none();
        let previous_reconciled_ms = previous
            .as_ref()
            .and_then(|state| state.last_reconciled_at)
            .map(|time| time.timestamp_millis().max(0) as u64)
            .unwrap_or(0);
        let mut unexplained_orders = Vec::new();
        for order in &snapshot.open_orders {
            let projected = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM mainnet_order_projection
                 WHERE account_address = ?1 AND oid = ?2)",
                params![account_address, order.oid],
                |row| row.get::<_, bool>(0),
            )?;
            let has_update = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM mainnet_order_updates
                 WHERE account_address = ?1 AND oid = ?2 AND status_timestamp_ms >= ?3)",
                params![account_address, order.oid, previous_reconciled_ms],
                |row| row.get::<_, bool>(0),
            )?;
            if !baseline && !projected && !has_update {
                unexplained_orders.push(order.oid);
                continue;
            }
            transaction.execute(
                "INSERT INTO mainnet_order_projection
                 (account_address, oid, coin, status, source, last_seen_at)
                 VALUES (?1, ?2, ?3, 'open', ?4, ?5)
                 ON CONFLICT(account_address, oid) DO UPDATE SET
                    coin = excluded.coin, status = excluded.status,
                    last_seen_at = excluded.last_seen_at",
                params![
                    account_address,
                    order.oid,
                    order.coin,
                    if baseline { "baseline" } else { "snapshot" },
                    observed_at.to_rfc3339()
                ],
            )?;
        }
        let mut unexplained_positions = Vec::new();
        let current_coins = snapshot
            .positions
            .iter()
            .map(|position| position.coin.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for position in &snapshot.positions {
            let prior_size = transaction
                .query_row(
                    "SELECT signed_size FROM mainnet_position_projection
                     WHERE account_address = ?1 AND coin = ?2",
                    params![account_address, position.coin],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let changed = prior_size
                .as_deref()
                .is_some_and(|size| size != position.signed_size.to_string());
            let has_fill = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM mainnet_fills
                 WHERE account_address = ?1 AND coin = ?2 AND time_ms >= ?3)",
                params![account_address, position.coin, previous_reconciled_ms],
                |row| row.get::<_, bool>(0),
            )?;
            if !baseline && changed && !has_fill {
                unexplained_positions.push(position.coin.clone());
                continue;
            }
            transaction.execute(
                "INSERT INTO mainnet_position_projection
                 (account_address, coin, signed_size, source, last_seen_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(account_address, coin) DO UPDATE SET
                    signed_size = excluded.signed_size,
                    last_seen_at = excluded.last_seen_at",
                params![
                    account_address,
                    position.coin,
                    position.signed_size.to_string(),
                    if baseline { "baseline" } else { "snapshot" },
                    observed_at.to_rfc3339()
                ],
            )?;
        }
        let mut statement = transaction.prepare(
            "SELECT coin FROM mainnet_position_projection
             WHERE account_address = ?1 AND signed_size != '0'",
        )?;
        let prior_coins = statement
            .query_map([account_address], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for coin in prior_coins {
            if !current_coins.contains(coin.as_str()) {
                let has_fill = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM mainnet_fills
                     WHERE account_address = ?1 AND coin = ?2 AND time_ms >= ?3)",
                    params![account_address, coin, previous_reconciled_ms],
                    |row| row.get::<_, bool>(0),
                )?;
                if !baseline && !has_fill {
                    unexplained_positions.push(coin.clone());
                    continue;
                }
                transaction.execute(
                    "UPDATE mainnet_position_projection SET signed_size = '0',
                     last_seen_at = ?3 WHERE account_address = ?1 AND coin = ?2",
                    params![account_address, coin, observed_at.to_rfc3339()],
                )?;
            }
        }
        let unprotected_positions = snapshot
            .protection_coverage
            .iter()
            .filter(|coverage| !coverage.covered)
            .map(|coverage| coverage.coin.clone())
            .collect::<Vec<_>>();
        let prior_close_only = previous
            .as_ref()
            .is_some_and(|state| state.mode == ExecutionMode::CloseOnly);
        let prior_pending = previous
            .as_ref()
            .is_some_and(|state| state.mode == ExecutionMode::HaltNewEntries);
        let has_discrepancy = !unexplained_orders.is_empty()
            || !unexplained_positions.is_empty()
            || !unprotected_positions.is_empty();
        let clean_streak = if prior_close_only && !has_discrepancy {
            previous
                .as_ref()
                .map_or(1, |state| state.clean_streak.saturating_add(1))
        } else {
            0
        };
        let (mode, ready, reason_code, detail) = if !unprotected_positions.is_empty() {
            (
                ExecutionMode::CloseOnly,
                false,
                Some("unprotected_position".to_owned()),
                Some(format!(
                    "positions without sufficient exchange-native stop coverage: {unprotected_positions:?}"
                )),
            )
        } else if prior_close_only {
            let state = previous.as_ref().expect("checked above");
            (
                ExecutionMode::CloseOnly,
                false,
                state.reason_code.clone(),
                state.detail.clone(),
            )
        } else if prior_pending && has_discrepancy && !baseline {
            (
                ExecutionMode::CloseOnly,
                false,
                Some(if !unexplained_orders.is_empty() {
                    "unknown_exchange_order".to_owned()
                } else {
                    "unexplained_position_change".to_owned()
                }),
                Some(discrepancy_detail(
                    &unexplained_orders,
                    &unexplained_positions,
                )),
            )
        } else if has_discrepancy {
            (
                ExecutionMode::HaltNewEntries,
                false,
                Some("reconciliation_discrepancy_pending".to_owned()),
                Some(discrepancy_detail(
                    &unexplained_orders,
                    &unexplained_positions,
                )),
            )
        } else {
            (ExecutionMode::Normal, true, None, None)
        };
        let baseline_at = previous
            .as_ref()
            .and_then(|state| state.baseline_established_at)
            .unwrap_or(observed_at);
        transaction.execute(
            "INSERT INTO mainnet_reconciliation_state
             (account_address, mode, ready, reason_code, detail,
              baseline_established_at, last_reconciled_at, clean_streak)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(account_address) DO UPDATE SET
                mode = excluded.mode, ready = excluded.ready,
                reason_code = excluded.reason_code, detail = excluded.detail,
                baseline_established_at = excluded.baseline_established_at,
                last_reconciled_at = excluded.last_reconciled_at,
                clean_streak = excluded.clean_streak",
            params![
                account_address,
                execution_mode_str(&mode),
                ready,
                reason_code,
                detail,
                baseline_at.to_rfc3339(),
                observed_at.to_rfc3339(),
                clean_streak
            ],
        )?;
        transaction.execute(
            "DELETE FROM mainnet_account_snapshots
             WHERE account_address = ?1 AND cursor NOT IN (
                SELECT cursor FROM mainnet_account_snapshots
                WHERE account_address = ?1 ORDER BY cursor DESC LIMIT 1000
             )",
            [account_address],
        )?;
        insert_event_tx(
            &transaction,
            Uuid::nil(),
            "exchange.account_snapshot_recorded",
            serde_json::json!({
                "accountAddress": account_address,
                "observedAt": observed_at
            }),
            observed_at,
        )?;
        transaction.commit()?;
        let recovery_eligible = mode == ExecutionMode::CloseOnly && clean_streak >= 3;
        Ok(ReconciliationState {
            account_address: account_address.clone(),
            mode,
            ready,
            reason_code,
            detail,
            baseline_established_at: Some(baseline_at),
            last_reconciled_at: Some(observed_at),
            clean_streak,
            recovery_eligible,
        })
    }

    pub fn configure_mainnet_account(&self, account_address: &str) -> Result<(), StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        connection.execute(
            "INSERT OR IGNORE INTO mainnet_reconciliation_state
             (account_address, mode, ready, reason_code, detail)
             VALUES (?1, 'halt_new_entries', 0, 'startup_reconciliation_pending',
                     'waiting for the first authoritative account snapshot')",
            [account_address],
        )?;
        Ok(())
    }

    pub fn reconciliation_state(
        &self,
        account_address: &str,
    ) -> Result<Option<ReconciliationState>, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        reconciliation_state_connection(&connection, account_address)
    }

    pub fn acknowledge_reconciliation_recovery(
        &self,
        account_address: &str,
        expected_reason_code: &str,
        acknowledged_by: &str,
    ) -> Result<ReconciliationState, StoreError> {
        let now = Utc::now();
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = reconciliation_state_tx(&transaction, account_address)?
            .ok_or(StoreError::ReconciliationNotCloseOnly)?;
        if state.mode != ExecutionMode::CloseOnly {
            return Err(StoreError::ReconciliationNotCloseOnly);
        }
        if state.reason_code.as_deref() != Some(expected_reason_code) {
            return Err(StoreError::ReconciliationReasonMismatch);
        }
        if !state.recovery_eligible {
            return Err(StoreError::ReconciliationRecoveryNotEligible);
        }
        transaction.execute(
            "UPDATE mainnet_reconciliation_state
             SET mode = 'normal', ready = 1, reason_code = NULL, detail = NULL,
                 clean_streak = 0
             WHERE account_address = ?1 AND mode = 'close_only'",
            [account_address],
        )?;
        insert_event_tx(
            &transaction,
            Uuid::nil(),
            "control.reconciliation_recovery_acknowledged",
            serde_json::json!({
                "accountAddress": account_address,
                "previousReasonCode": expected_reason_code,
                "acknowledgedBy": acknowledged_by
            }),
            now,
        )?;
        transaction.commit()?;
        drop(connection);
        self.reconciliation_state(account_address)?
            .ok_or(StoreError::ReconciliationNotCloseOnly)
    }

    pub fn admission_block(&self) -> Result<Option<ReconciliationState>, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let account = connection
            .query_row(
                "SELECT account_address FROM mainnet_reconciliation_state
                 WHERE ready = 0 OR mode != 'normal' ORDER BY account_address LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        account
            .map(|account| reconciliation_state_connection(&connection, &account))
            .transpose()
            .map(Option::flatten)
    }

    pub fn latest_mainnet_snapshot(
        &self,
        account_address: &str,
    ) -> Result<Option<Value>, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let json = connection
            .query_row(
                "SELECT data_json FROM mainnet_account_snapshots
                 WHERE account_address = ?1 ORDER BY cursor DESC LIMIT 1",
                [account_address],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        json.map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub fn mainnet_history_cursor(&self, account_address: &str) -> Result<Option<u64>, StoreError> {
        let connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let funding_time = connection.query_row(
            "SELECT MAX(time_ms) FROM mainnet_funding WHERE account_address = ?1",
            [account_address],
            |row| row.get::<_, Option<u64>>(0),
        )?;
        Ok(funding_time)
    }

    pub fn record_mainnet_history(
        &self,
        account_address: &str,
        fills: &[MainnetFillRecord],
        funding: &[MainnetFundingRecord],
    ) -> Result<(), StoreError> {
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut inserted_fills = 0usize;
        for fill in fills {
            inserted_fills += transaction.execute(
                "INSERT OR IGNORE INTO mainnet_fills
                 (account_address, exchange_hash, oid, time_ms, coin, side, direction, crossed,
                  price, size, closed_pnl_usd, fee_usd)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    account_address,
                    fill.hash,
                    fill.oid,
                    fill.time_ms,
                    fill.coin,
                    fill.side,
                    fill.direction,
                    fill.crossed,
                    fill.price.to_string(),
                    fill.size.to_string(),
                    fill.closed_pnl_usd.to_string(),
                    fill.fee_usd.to_string()
                ],
            )?;
        }
        let mut inserted_funding = 0usize;
        for entry in funding {
            inserted_funding += transaction.execute(
                "INSERT OR IGNORE INTO mainnet_funding
                 (account_address, exchange_hash, time_ms, coin, usdc, signed_size, funding_rate)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    account_address,
                    entry.hash,
                    entry.time_ms,
                    entry.coin,
                    entry.usdc.to_string(),
                    entry.signed_size.to_string(),
                    entry.funding_rate.to_string()
                ],
            )?;
        }
        if inserted_fills > 0 || inserted_funding > 0 {
            insert_event_tx(
                &transaction,
                Uuid::nil(),
                "exchange.history_backfilled",
                serde_json::json!({
                    "accountAddress": account_address,
                    "insertedFills": inserted_fills,
                    "insertedFunding": inserted_funding
                }),
                Utc::now(),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn record_mainnet_order_updates(
        &self,
        account_address: &str,
        updates: &[MainnetOrderUpdateRecord],
    ) -> Result<(), StoreError> {
        let mut connection = self
            .connection
            .lock()
            .expect("execution database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut inserted = 0usize;
        for update in updates {
            inserted += transaction.execute(
                "INSERT OR IGNORE INTO mainnet_order_updates
                 (account_address, oid, status, status_timestamp_ms, coin, side, limit_price,
                  size, original_size, cloid)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    account_address,
                    update.oid,
                    update.status,
                    update.status_timestamp_ms,
                    update.coin,
                    update.side,
                    update.limit_price.to_string(),
                    update.size.to_string(),
                    update.original_size.to_string(),
                    update.cloid
                ],
            )?;
        }
        if inserted > 0 {
            insert_event_tx(
                &transaction,
                Uuid::nil(),
                "exchange.order_updates_recorded",
                serde_json::json!({
                    "accountAddress": account_address,
                    "inserted": inserted
                }),
                Utc::now(),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

type StoredIntentRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
);

fn get_live_execution_connection(
    connection: &Connection,
    intent_id: Uuid,
) -> Result<Option<LiveExecution>, StoreError> {
    let row = connection
        .query_row(
            "SELECT intent_id, account_address, entry_cloid, status, attempt, last_error,
                    created_at, updated_at
             FROM live_executions WHERE intent_id = ?1",
            [intent_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, u32>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(
            intent_id,
            account_address,
            entry_cloid,
            status,
            attempt,
            last_error,
            created,
            updated,
        )| {
            Ok(LiveExecution {
                intent_id: Uuid::parse_str(&intent_id)?,
                account_address,
                entry_cloid: Uuid::parse_str(&entry_cloid)?,
                status: parse_live_execution_status(&status)?,
                attempt,
                last_error,
                created_at: DateTime::parse_from_rfc3339(&created)
                    .map_err(|_| StoreError::InvalidTimestamp)?
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&updated)
                    .map_err(|_| StoreError::InvalidTimestamp)?
                    .with_timezone(&Utc),
            })
        },
    )
    .transpose()
}

fn get_live_execution_tx(
    transaction: &rusqlite::Transaction<'_>,
    intent_id: Uuid,
) -> Result<Option<LiveExecution>, StoreError> {
    get_live_execution_connection(transaction, intent_id)
}

fn parse_live_execution_status(value: &str) -> Result<LiveExecutionStatus, StoreError> {
    use LiveExecutionStatus::*;
    match value {
        "pending" => Ok(Pending),
        "entry_submitting" => Ok(EntrySubmitting),
        "entry_unknown" => Ok(EntryUnknown),
        "entry_filled" => Ok(EntryFilled),
        "protecting" => Ok(Protecting),
        "protected" => Ok(Protected),
        "emergency_closing" => Ok(EmergencyClosing),
        "closed" => Ok(Closed),
        "failed" => Ok(Failed),
        "reconciliation_required" => Ok(ReconciliationRequired),
        _ => Err(StoreError::InvalidStatus(value.into())),
    }
}

fn live_execution_status_str(status: LiveExecutionStatus) -> &'static str {
    use LiveExecutionStatus::*;
    match status {
        Pending => "pending",
        EntrySubmitting => "entry_submitting",
        EntryUnknown => "entry_unknown",
        EntryFilled => "entry_filled",
        Protecting => "protecting",
        Protected => "protected",
        EmergencyClosing => "emergency_closing",
        Closed => "closed",
        Failed => "failed",
        ReconciliationRequired => "reconciliation_required",
    }
}

#[allow(clippy::too_many_arguments)]
fn update_live_and_intent_tx(
    transaction: &rusqlite::Transaction<'_>,
    intent_id: Uuid,
    expected_live: LiveExecutionStatus,
    next_live: LiveExecutionStatus,
    expected_intent: TradeIntentStatus,
    next_intent: TradeIntentStatus,
    reason: &str,
    detail: Option<&str>,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let execution_changed = transaction.execute(
        "UPDATE live_executions SET status = ?1, last_error = ?2, updated_at = ?3
         WHERE intent_id = ?4 AND status = ?5",
        params![
            live_execution_status_str(next_live),
            detail,
            now.to_rfc3339(),
            intent_id.to_string(),
            live_execution_status_str(expected_live)
        ],
    )?;
    let intent_changed = transaction.execute(
        "UPDATE trade_intents SET status = ?1, status_reason = ?2, updated_at = ?3
         WHERE intent_id = ?4 AND status = ?5",
        params![
            status_to_str(next_intent),
            reason,
            now.to_rfc3339(),
            intent_id.to_string(),
            status_to_str(expected_intent)
        ],
    )?;
    if execution_changed != 1 || intent_changed != 1 {
        return Err(StoreError::ConcurrentTransition);
    }
    insert_event_tx(
        transaction,
        intent_id,
        &format!("execution.{}", live_execution_status_str(next_live)),
        serde_json::json!({ "reason": reason, "detail": detail }),
        now,
    )?;
    Ok(())
}

fn execution_mode_str(mode: &ExecutionMode) -> &'static str {
    match mode {
        ExecutionMode::Normal => "normal",
        ExecutionMode::HaltNewEntries => "halt_new_entries",
        ExecutionMode::CloseOnly => "close_only",
    }
}

fn discrepancy_detail(orders: &[u64], positions: &[String]) -> String {
    format!("unexplained orders: {orders:?}; unexplained position changes: {positions:?}")
}

fn parse_execution_mode(value: String) -> Result<ExecutionMode, StoreError> {
    match value.as_str() {
        "normal" => Ok(ExecutionMode::Normal),
        "halt_new_entries" => Ok(ExecutionMode::HaltNewEntries),
        "close_only" => Ok(ExecutionMode::CloseOnly),
        _ => Err(StoreError::InvalidStatus(value)),
    }
}

fn reconciliation_state_connection(
    connection: &Connection,
    account_address: &str,
) -> Result<Option<ReconciliationState>, StoreError> {
    let row = connection
        .query_row(
            "SELECT account_address, mode, ready, reason_code, detail,
                    baseline_established_at, last_reconciled_at, clean_streak
             FROM mainnet_reconciliation_state WHERE account_address = ?1",
            [account_address],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, u32>(7)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(account_address, mode, ready, reason_code, detail, baseline, last, clean_streak)| {
            let mode = parse_execution_mode(mode)?;
            Ok(ReconciliationState {
                account_address,
                recovery_eligible: mode == ExecutionMode::CloseOnly && clean_streak >= 3,
                mode,
                ready,
                reason_code,
                detail,
                baseline_established_at: baseline
                    .map(|value| DateTime::parse_from_rfc3339(&value))
                    .transpose()
                    .map_err(|_| StoreError::InvalidTimestamp)?
                    .map(|value| value.with_timezone(&Utc)),
                last_reconciled_at: last
                    .map(|value| DateTime::parse_from_rfc3339(&value))
                    .transpose()
                    .map_err(|_| StoreError::InvalidTimestamp)?
                    .map(|value| value.with_timezone(&Utc)),
                clean_streak,
            })
        },
    )
    .transpose()
}

fn reconciliation_state_tx(
    transaction: &rusqlite::Transaction<'_>,
    account_address: &str,
) -> Result<Option<ReconciliationState>, StoreError> {
    reconciliation_state_connection(transaction, account_address)
}

fn map_stored_intent(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredIntentRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
    ))
}

fn parse_stored_intent(row: StoredIntentRow) -> Result<StoredIntent, StoreError> {
    let (
        payload,
        idempotency_key,
        status,
        status_reason,
        risk_decision,
        confirmation_digest,
        confirmation_expires_at,
        confirmed_at,
        confirmed_by,
        created_at,
        updated_at,
    ) = row;
    Ok(StoredIntent {
        intent: serde_json::from_str(&payload)?,
        idempotency_key,
        status: status_from_str(&status)?,
        status_reason,
        risk_decision: risk_decision
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        manual_confirmation: match (confirmation_digest, confirmation_expires_at) {
            (Some(digest), Some(expires_at)) => Some(ManualConfirmation {
                digest,
                expires_at: parse_timestamp(&expires_at)?,
                confirmed_at: confirmed_at
                    .map(|value| parse_timestamp(&value))
                    .transpose()?,
                confirmed_by,
            }),
            _ => None,
        },
        created_at: parse_timestamp(&created_at)?,
        updated_at: parse_timestamp(&updated_at)?,
    })
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, StoreError> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|_| StoreError::InvalidTimestamp)?
        .with_timezone(&Utc))
}

fn get_intent_tx(
    transaction: &rusqlite::Transaction<'_>,
    intent_id: Uuid,
) -> Result<Option<StoredIntent>, StoreError> {
    let row = transaction
        .query_row(
            "SELECT payload_json, idempotency_key, status, status_reason, risk_decision_json,
                    confirmation_digest, confirmation_expires_at, confirmed_at, confirmed_by,
                    created_at, updated_at
             FROM trade_intents WHERE intent_id = ?1",
            [intent_id.to_string()],
            map_stored_intent,
        )
        .optional()?;
    row.map(parse_stored_intent).transpose()
}

fn find_by_idempotency_key_tx(
    transaction: &rusqlite::Transaction<'_>,
    strategy_id: &str,
    key: &str,
) -> Result<Option<StoredIntent>, StoreError> {
    let row = transaction
        .query_row(
            "SELECT payload_json, idempotency_key, status, status_reason, risk_decision_json,
                    confirmation_digest, confirmation_expires_at, confirmed_at, confirmed_by,
                    created_at, updated_at
             FROM trade_intents WHERE strategy_id = ?1 AND idempotency_key = ?2",
            params![strategy_id, key],
            map_stored_intent,
        )
        .optional()?;
    row.map(parse_stored_intent).transpose()
}

fn reject_signal_replay_tx(
    transaction: &rusqlite::Transaction<'_>,
    intent: &TradeIntent,
) -> Result<(), StoreError> {
    let exists = transaction.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM trade_intents
           WHERE json_extract(payload_json, '$.accountId') = ?1
             AND json_extract(payload_json, '$.strategyInstanceId') = ?2
             AND json_extract(payload_json, '$.signalId') = ?3
         )",
        params![
            intent.account_id,
            intent.strategy_instance_id,
            intent.signal_id
        ],
        |row| row.get::<_, bool>(0),
    )?;
    if exists {
        Err(StoreError::SignalReplay)
    } else {
        Ok(())
    }
}

fn insert_event_tx(
    transaction: &rusqlite::Transaction<'_>,
    aggregate_id: Uuid,
    event_type: &str,
    data: Value,
    occurred_at: DateTime<Utc>,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO execution_events(event_id, aggregate_id, event_type, occurred_at, data_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            Uuid::new_v4().to_string(),
            aggregate_id.to_string(),
            event_type,
            occurred_at.to_rfc3339(),
            serde_json::to_string(&data)?,
        ],
    )?;
    Ok(())
}

fn status_to_str(status: TradeIntentStatus) -> &'static str {
    use TradeIntentStatus::*;
    match status {
        Received => "received",
        AwaitingConfirmation => "awaiting_confirmation",
        RiskRejected => "risk_rejected",
        ShadowAccepted => "shadow_accepted",
        Approved => "approved",
        Submitting => "submitting",
        Executing => "executing",
        Completed => "completed",
        CancelPending => "cancel_pending",
        Canceled => "canceled",
        Failed => "failed",
    }
}

fn status_from_str(status: &str) -> Result<TradeIntentStatus, StoreError> {
    use TradeIntentStatus::*;
    match status {
        "received" => Ok(Received),
        "awaiting_confirmation" => Ok(AwaitingConfirmation),
        "risk_rejected" => Ok(RiskRejected),
        "shadow_accepted" => Ok(ShadowAccepted),
        "approved" => Ok(Approved),
        "submitting" => Ok(Submitting),
        "executing" => Ok(Executing),
        "completed" => Ok(Completed),
        "cancel_pending" => Ok(CancelPending),
        "canceled" => Ok(Canceled),
        "failed" => Ok(Failed),
        value => Err(StoreError::InvalidStatus(value.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        EntryPolicy, EntryType, ExitPolicy, Side, TRADE_INTENT_SCHEMA_VERSION, TakeProfitTarget,
        confirmation_digest,
    };
    use crate::risk::{RiskEngine, RiskPolicy};
    use chrono::Duration;
    use rust_decimal::Decimal;
    use std::sync::{Arc, Barrier};

    fn intent() -> TradeIntent {
        TradeIntent {
            schema_version: TRADE_INTENT_SCHEMA_VERSION,
            intent_id: Uuid::new_v4(),
            strategy_id: "test".into(),
            strategy_version: "test-v1".into(),
            strategy_instance_id: "test-primary".into(),
            signal_id: Uuid::new_v4().to_string(),
            account_id: "main".into(),
            symbol: "BTC".into(),
            side: Side::Long,
            reference_price: Decimal::new(100_000, 0),
            max_notional_usd: Decimal::new(20, 0),
            max_risk_usd: Decimal::new(1, 0),
            entry: EntryPolicy {
                kind: EntryType::MarketIoc,
                max_slippage_bps: 20,
            },
            exit: ExitPolicy {
                stop_loss_price: Decimal::new(95_000, 0),
                take_profit: vec![TakeProfitTarget {
                    price: Decimal::new(110_000, 0),
                    position_pct: 100,
                }],
            },
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::minutes(5),
        }
    }

    fn risk_decision(intent: &TradeIntent) -> RiskDecision {
        RiskEngine::new(RiskPolicy::shadow_tiny_default())
            .unwrap()
            .evaluate_admission(intent)
    }

    fn confirmation(intent: &TradeIntent, decision: &RiskDecision) -> ManualConfirmation {
        let expires_at = Utc::now() + Duration::minutes(2);
        ManualConfirmation {
            digest: confirmation_digest(intent, decision, expires_at).unwrap(),
            expires_at,
            confirmed_at: None,
            confirmed_by: None,
        }
    }

    #[test]
    fn idempotency_key_returns_the_original_intent() {
        let store = ExecutionStore::in_memory().unwrap();
        let first_intent = intent();
        let (_, created) = store.create_intent(&first_intent, "same-key").unwrap();
        assert!(created);
        let (stored, created) = store.create_intent(&first_intent, "same-key").unwrap();
        assert!(!created);
        assert_eq!(stored.intent.intent_id, first_intent.intent_id);
        assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
    }

    #[test]
    fn idempotency_key_rejects_a_different_payload() {
        let store = ExecutionStore::in_memory().unwrap();
        let first_intent = intent();
        store.create_intent(&first_intent, "same-key").unwrap();

        let second_intent = intent();
        assert!(matches!(
            store.create_intent(&second_intent, "same-key"),
            Err(StoreError::IdempotencyConflict)
        ));
        assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
    }

    #[test]
    fn idempotency_keys_are_scoped_by_strategy() {
        let store = ExecutionStore::in_memory().unwrap();
        let first_intent = intent();
        store.create_intent(&first_intent, "shared-key").unwrap();

        let mut second_intent = intent();
        second_intent.strategy_id = "another-strategy".into();
        let (_, created) = store.create_intent(&second_intent, "shared-key").unwrap();
        assert!(created);
    }

    #[test]
    fn signal_replay_cannot_open_under_a_new_intent_and_idempotency_key() {
        let store = ExecutionStore::in_memory().unwrap();
        let first = intent();
        store.create_intent(&first, "first-key").unwrap();
        let mut replay = first.clone();
        replay.intent_id = Uuid::new_v4();
        assert!(matches!(
            store.create_intent(&replay, "second-key"),
            Err(StoreError::SignalReplay)
        ));
    }

    #[test]
    fn shadow_submission_is_persisted_as_one_replayable_operation() {
        let store = ExecutionStore::in_memory().unwrap();
        let intent = intent();
        let decision = risk_decision(&intent);
        let (stored, created) = store
            .submit_shadow_intent(&intent, "shadow-key", &decision, None)
            .unwrap();

        assert!(created);
        assert_eq!(stored.status, TradeIntentStatus::ShadowAccepted);
        assert_eq!(stored.risk_decision, Some(decision.clone()));
        assert_eq!(
            store
                .get_intent(intent.intent_id)
                .unwrap()
                .unwrap()
                .risk_decision,
            Some(decision)
        );
        let events = store.events_after(0, 100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "intent.received");
        assert_eq!(events[1].event_type, "intent.shadow_accepted");
    }

    #[test]
    fn kill_switch_rejects_shadow_submission_in_the_same_transaction() {
        let store = ExecutionStore::in_memory().unwrap();
        store.set_kill_switch(true, Some("operator stop")).unwrap();
        let intent = intent();

        let decision = risk_decision(&intent);
        let (stored, created) = store
            .submit_shadow_intent(&intent, "blocked-key", &decision, None)
            .unwrap();
        assert!(created);
        assert_eq!(stored.status, TradeIntentStatus::RiskRejected);
        assert_eq!(stored.status_reason.as_deref(), Some("kill_switch_enabled"));
        assert_eq!(store.events_after(0, 100).unwrap().len(), 3);
    }

    #[test]
    fn manual_confirmation_is_bound_and_replayable() {
        let store = ExecutionStore::in_memory().unwrap();
        let intent = intent();
        let decision = risk_decision(&intent);
        let confirmation = confirmation(&intent, &decision);
        let (pending, _) = store
            .submit_shadow_intent(&intent, "confirm-key", &decision, Some(&confirmation))
            .unwrap();
        assert_eq!(pending.status, TradeIntentStatus::AwaitingConfirmation);

        assert!(matches!(
            store.confirm_intent(intent.intent_id, &"0".repeat(64), "operator"),
            Err(StoreError::ConfirmationMismatch)
        ));
        let confirmed = store
            .confirm_intent(intent.intent_id, &confirmation.digest, "operator")
            .unwrap();
        assert_eq!(confirmed.status, TradeIntentStatus::ShadowAccepted);
        assert_eq!(
            confirmed
                .manual_confirmation
                .as_ref()
                .and_then(|value| value.confirmed_by.as_deref()),
            Some("operator")
        );
        let replay = store
            .confirm_intent(intent.intent_id, &confirmation.digest, "operator")
            .unwrap();
        assert_eq!(replay, confirmed);
        assert_eq!(store.events_after(0, 100).unwrap().len(), 4);
    }

    #[test]
    fn expired_confirmation_is_persistently_rejected() {
        let store = ExecutionStore::in_memory().unwrap();
        let intent = intent();
        let decision = risk_decision(&intent);
        let expires_at = Utc::now() - Duration::seconds(1);
        let confirmation = ManualConfirmation {
            digest: confirmation_digest(&intent, &decision, expires_at).unwrap(),
            expires_at,
            confirmed_at: None,
            confirmed_by: None,
        };
        store
            .submit_shadow_intent(&intent, "expired-key", &decision, Some(&confirmation))
            .unwrap();

        assert!(matches!(
            store.confirm_intent(intent.intent_id, &confirmation.digest, "operator"),
            Err(StoreError::ConfirmationExpired)
        ));
        assert_eq!(
            store.get_intent(intent.intent_id).unwrap().unwrap().status,
            TradeIntentStatus::RiskRejected
        );
    }

    #[test]
    fn kill_switch_is_rechecked_at_confirmation_time() {
        let store = ExecutionStore::in_memory().unwrap();
        let intent = intent();
        let decision = risk_decision(&intent);
        let confirmation = confirmation(&intent, &decision);
        store
            .submit_shadow_intent(&intent, "kill-confirm-key", &decision, Some(&confirmation))
            .unwrap();
        store.set_kill_switch(true, Some("operator stop")).unwrap();

        let rejected = store
            .confirm_intent(intent.intent_id, &confirmation.digest, "operator")
            .unwrap();
        assert_eq!(rejected.status, TradeIntentStatus::RiskRejected);
        assert_eq!(
            rejected.status_reason.as_deref(),
            Some("kill_switch_enabled")
        );
        assert!(rejected.manual_confirmation.unwrap().confirmed_at.is_none());
    }

    #[test]
    fn live_confirmation_creates_one_execution_and_claims_it_once() {
        let store = ExecutionStore::in_memory().unwrap();
        let intent = intent();
        let decision = risk_decision(&intent);
        let confirmation = confirmation(&intent, &decision);
        store
            .submit_shadow_intent(&intent, "live-key", &decision, Some(&confirmation))
            .unwrap();

        let confirmed = store
            .confirm_intent_for_execution(
                intent.intent_id,
                &confirmation.digest,
                "operator",
                "0x0000000000000000000000000000000000000001",
            )
            .unwrap();
        assert_eq!(confirmed.status, TradeIntentStatus::Approved);
        let replay = store
            .confirm_intent_for_execution(
                intent.intent_id,
                &confirmation.digest,
                "operator",
                "0x0000000000000000000000000000000000000001",
            )
            .unwrap();
        assert_eq!(replay.status, TradeIntentStatus::Approved);

        let (execution, claimed_intent) = store
            .claim_next_live_execution("0x0000000000000000000000000000000000000001")
            .unwrap()
            .unwrap();
        assert_eq!(execution.status, LiveExecutionStatus::EntrySubmitting);
        assert_eq!(execution.attempt, 1);
        assert_eq!(claimed_intent.status, TradeIntentStatus::Submitting);
        assert!(
            store
                .claim_next_live_execution("0x0000000000000000000000000000000000000001")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_live_execution(intent.intent_id)
                .unwrap()
                .unwrap()
                .entry_cloid,
            deterministic_cloid(intent.intent_id, OrderRole::Entry)
        );
    }

    #[test]
    fn only_one_execution_per_account_and_symbol_can_be_active() {
        let store = ExecutionStore::in_memory().unwrap();
        let first = intent();
        let mut second = intent();
        second.signal_id = Uuid::new_v4().to_string();
        for (candidate, key) in [(&first, "symbol-first"), (&second, "symbol-second")] {
            let decision = risk_decision(candidate);
            let confirmation = confirmation(candidate, &decision);
            store
                .submit_shadow_intent(candidate, key, &decision, Some(&confirmation))
                .unwrap();
            store
                .confirm_intent_for_execution(
                    candidate.intent_id,
                    &confirmation.digest,
                    "operator",
                    "0x0000000000000000000000000000000000000001",
                )
                .unwrap();
        }
        let claimed = store
            .claim_next_live_execution("0x0000000000000000000000000000000000000001")
            .unwrap()
            .unwrap();
        assert_eq!(claimed.0.intent_id, first.intent_id);
        assert!(
            store
                .claim_next_live_execution("0x0000000000000000000000000000000000000001",)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn transitions_append_replayable_events() {
        let store = ExecutionStore::in_memory().unwrap();
        let intent = intent();
        store.create_intent(&intent, "key").unwrap();
        store
            .transition_intent(intent.intent_id, TradeIntentStatus::ShadowAccepted, None)
            .unwrap();
        let events = store.events_after(0, 100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type, "intent.shadow_accepted");
    }

    #[test]
    fn concurrent_transitions_cannot_both_commit() {
        let store = Arc::new(ExecutionStore::in_memory().unwrap());
        let intent = intent();
        store.create_intent(&intent, "key").unwrap();
        let barrier = Arc::new(Barrier::new(3));

        let handles = [
            TradeIntentStatus::ShadowAccepted,
            TradeIntentStatus::RiskRejected,
        ]
        .into_iter()
        .map(|next| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let intent_id = intent.intent_id;
            std::thread::spawn(move || {
                barrier.wait();
                store.transition_intent(intent_id, next, None)
            })
        })
        .collect::<Vec<_>>();
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(store.events_after(0, 100).unwrap().len(), 2);
    }

    #[test]
    fn file_store_preserves_state_across_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("executor.db");
        let intent = intent();
        {
            let store = ExecutionStore::open(path.to_str().unwrap()).unwrap();
            let decision = risk_decision(&intent);
            store
                .submit_shadow_intent(&intent, "restart-key", &decision, None)
                .unwrap();
            store.set_kill_switch(true, Some("test")).unwrap();
        }

        let reopened = ExecutionStore::open(path.to_str().unwrap()).unwrap();
        assert_eq!(
            reopened
                .get_intent(intent.intent_id)
                .unwrap()
                .unwrap()
                .status,
            TradeIntentStatus::ShadowAccepted
        );
        assert!(reopened.kill_switch_enabled().unwrap());
        assert_eq!(reopened.events_after(0, 100).unwrap().len(), 3);
    }

    #[test]
    fn migrates_the_legacy_shadow_schema_without_losing_intents() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("legacy.db");
        let intent = intent();
        let now = Utc::now().to_rfc3339();
        let mut legacy_payload = serde_json::to_value(&intent).unwrap();
        let object = legacy_payload.as_object_mut().unwrap();
        for field in [
            "schemaVersion",
            "strategyId",
            "strategyInstanceId",
            "signalId",
            "accountId",
            "createdAt",
        ] {
            object.remove(field);
        }
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE trade_intents (
                        intent_id TEXT PRIMARY KEY,
                        idempotency_key TEXT NOT NULL UNIQUE,
                        payload_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        status_reason TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO trade_intents VALUES (?1, 'legacy-key', ?2, 'shadow_accepted', NULL, ?3, ?3)",
                    params![intent.intent_id.to_string(), legacy_payload.to_string(), now],
                )
                .unwrap();
        }

        let store = ExecutionStore::open(path.to_str().unwrap()).unwrap();
        let migrated = store.get_intent(intent.intent_id).unwrap().unwrap();
        assert_eq!(migrated.intent.schema_version, TRADE_INTENT_SCHEMA_VERSION);
        assert_eq!(migrated.intent.strategy_id, "legacy");
        assert_eq!(migrated.intent.signal_id, intent.intent_id.to_string());
    }

    #[test]
    fn refuses_a_database_created_by_a_newer_executor() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("future.db");
        {
            let connection = Connection::open(&path).unwrap();
            connection.pragma_update(None, "user_version", 999).unwrap();
        }

        assert!(matches!(
            ExecutionStore::open(path.to_str().unwrap()),
            Err(StoreError::UnsupportedDatabaseVersion(999))
        ));
    }

    #[test]
    fn persists_and_reads_the_latest_mainnet_snapshot() {
        let store = ExecutionStore::in_memory().unwrap();
        let address = "0x0000000000000000000000000000000000000001";
        let mut snapshot = test_snapshot(address);
        snapshot.account_value_usd = Decimal::TEN;
        store.record_mainnet_snapshot(&snapshot).unwrap();
        snapshot.observed_at = Utc::now();
        snapshot.account_value_usd = Decimal::new(11, 0);
        store.record_mainnet_snapshot(&snapshot).unwrap();

        assert_eq!(
            store.latest_mainnet_snapshot(address).unwrap().unwrap()["accountValueUsd"],
            serde_json::json!("11")
        );
        assert_eq!(store.events_after(0, 100).unwrap().len(), 2);
    }

    fn test_snapshot(address: &str) -> MainnetAccountSnapshot {
        MainnetAccountSnapshot {
            account_address: address.into(),
            observed_at: Utc::now(),
            account_value_usd: Decimal::ZERO,
            withdrawable_usd: Decimal::ZERO,
            total_margin_used_usd: Decimal::ZERO,
            total_position_notional_usd: Decimal::ZERO,
            positions: Vec::new(),
            open_orders: Vec::new(),
            protection_coverage: Vec::new(),
            asset_size_decimals: Default::default(),
            mids: Default::default(),
        }
    }

    #[test]
    fn reconciliation_imports_baseline_then_sticks_on_unknown_order() {
        use crate::mainnet_read::MainnetOpenOrderSnapshot;

        let store = ExecutionStore::in_memory().unwrap();
        let address = "0x0000000000000000000000000000000000000001";
        store.configure_mainnet_account(address).unwrap();
        assert_eq!(
            store.admission_block().unwrap().unwrap().mode,
            ExecutionMode::HaltNewEntries
        );

        let mut snapshot = test_snapshot(address);
        snapshot.open_orders.push(MainnetOpenOrderSnapshot {
            coin: "BTC".into(),
            oid: 1,
            side: "B".into(),
            limit_price: Decimal::new(100, 0),
            size: Decimal::ONE,
            timestamp_ms: 1,
            is_trigger: false,
            reduce_only: false,
            trigger_condition: "N/A".into(),
            trigger_price: None,
        });
        assert_eq!(
            store.record_mainnet_snapshot(&snapshot).unwrap().mode,
            ExecutionMode::Normal
        );
        assert!(store.admission_block().unwrap().is_none());

        snapshot.observed_at += chrono::Duration::seconds(1);
        snapshot.open_orders.push(MainnetOpenOrderSnapshot {
            oid: 2,
            ..snapshot.open_orders[0].clone()
        });
        let state = store.record_mainnet_snapshot(&snapshot).unwrap();
        assert_eq!(state.mode, ExecutionMode::HaltNewEntries);
        assert_eq!(
            state.reason_code.as_deref(),
            Some("reconciliation_discrepancy_pending")
        );

        snapshot.observed_at += chrono::Duration::seconds(1);
        let state = store.record_mainnet_snapshot(&snapshot).unwrap();
        assert_eq!(state.mode, ExecutionMode::CloseOnly);
        assert_eq!(state.reason_code.as_deref(), Some("unknown_exchange_order"));

        snapshot.observed_at += chrono::Duration::seconds(1);
        snapshot.open_orders.pop();
        let state = store.record_mainnet_snapshot(&snapshot).unwrap();
        assert_eq!(state.mode, ExecutionMode::CloseOnly);
        assert_eq!(state.clean_streak, 1);
        assert!(!state.recovery_eligible);
        assert!(matches!(
            store.acknowledge_reconciliation_recovery(
                address,
                "unknown_exchange_order",
                "operator"
            ),
            Err(StoreError::ReconciliationRecoveryNotEligible)
        ));

        for expected_streak in 2..=3 {
            snapshot.observed_at += chrono::Duration::seconds(1);
            let state = store.record_mainnet_snapshot(&snapshot).unwrap();
            assert_eq!(state.clean_streak, expected_streak);
        }
        assert!(matches!(
            store.acknowledge_reconciliation_recovery(address, "wrong_reason", "operator"),
            Err(StoreError::ReconciliationReasonMismatch)
        ));
        let recovered = store
            .acknowledge_reconciliation_recovery(address, "unknown_exchange_order", "operator")
            .unwrap();
        assert_eq!(recovered.mode, ExecutionMode::Normal);
        assert!(recovered.ready);
        assert!(
            store.events_after(0, 100).unwrap().iter().any(|event| {
                event.event_type == "control.reconciliation_recovery_acknowledged"
            })
        );
    }

    #[test]
    fn reconciliation_accepts_order_with_a_fresh_exchange_update() {
        use crate::mainnet_read::MainnetOpenOrderSnapshot;

        let store = ExecutionStore::in_memory().unwrap();
        let address = "0x0000000000000000000000000000000000000001";
        let mut snapshot = test_snapshot(address);
        store.record_mainnet_snapshot(&snapshot).unwrap();
        snapshot.observed_at += chrono::Duration::seconds(1);
        let update_time = snapshot.observed_at.timestamp_millis() as u64;
        store
            .record_mainnet_order_updates(
                address,
                &[MainnetOrderUpdateRecord {
                    oid: 42,
                    status: "open".into(),
                    status_timestamp_ms: update_time,
                    coin: "BTC".into(),
                    side: "B".into(),
                    limit_price: Decimal::new(100, 0),
                    size: Decimal::ONE,
                    original_size: Decimal::ONE,
                    cloid: None,
                }],
            )
            .unwrap();
        snapshot.open_orders.push(MainnetOpenOrderSnapshot {
            coin: "BTC".into(),
            oid: 42,
            side: "B".into(),
            limit_price: Decimal::new(100, 0),
            size: Decimal::ONE,
            timestamp_ms: update_time,
            is_trigger: false,
            reduce_only: false,
            trigger_condition: "N/A".into(),
            trigger_price: None,
        });
        assert_eq!(
            store.record_mainnet_snapshot(&snapshot).unwrap().mode,
            ExecutionMode::Normal
        );
    }

    #[test]
    fn reconciliation_rejects_position_change_without_fill() {
        use crate::mainnet_read::MainnetPositionSnapshot;

        let store = ExecutionStore::in_memory().unwrap();
        let address = "0x0000000000000000000000000000000000000001";
        let mut snapshot = test_snapshot(address);
        snapshot.positions.push(MainnetPositionSnapshot {
            coin: "BTC".into(),
            signed_size: Decimal::ONE,
            position_value_usd: Decimal::new(100, 0),
            unrealized_pnl_usd: Decimal::ZERO,
            entry_price: Some(Decimal::new(100, 0)),
            liquidation_price: None,
            leverage: 1,
            leverage_type: "cross".into(),
        });
        store.record_mainnet_snapshot(&snapshot).unwrap();
        snapshot.observed_at += chrono::Duration::seconds(1);
        snapshot.positions[0].signed_size = Decimal::new(2, 0);
        assert_eq!(
            store.record_mainnet_snapshot(&snapshot).unwrap().mode,
            ExecutionMode::HaltNewEntries
        );
        snapshot.observed_at += chrono::Duration::seconds(1);
        let state = store.record_mainnet_snapshot(&snapshot).unwrap();
        assert_eq!(state.mode, ExecutionMode::CloseOnly);
        assert_eq!(
            state.reason_code.as_deref(),
            Some("unexplained_position_change")
        );
    }

    #[test]
    fn unprotected_position_immediately_enters_close_only() {
        use crate::mainnet_read::{MainnetPositionSnapshot, MainnetProtectionCoverage};

        let store = ExecutionStore::in_memory().unwrap();
        let address = "0x0000000000000000000000000000000000000001";
        let mut snapshot = test_snapshot(address);
        snapshot.positions.push(MainnetPositionSnapshot {
            coin: "BTC".into(),
            signed_size: Decimal::ONE,
            position_value_usd: Decimal::new(100, 0),
            unrealized_pnl_usd: Decimal::ZERO,
            entry_price: Some(Decimal::new(100, 0)),
            liquidation_price: None,
            leverage: 1,
            leverage_type: "cross".into(),
        });
        snapshot
            .protection_coverage
            .push(MainnetProtectionCoverage {
                coin: "BTC".into(),
                required_size: Decimal::ONE,
                covered_size: Decimal::ZERO,
                stop_order_count: 0,
                covered: false,
                reason_code: Some("insufficient_reduce_only_stop_coverage".into()),
            });

        let state = store.record_mainnet_snapshot(&snapshot).unwrap();
        assert_eq!(state.mode, ExecutionMode::CloseOnly);
        assert_eq!(state.reason_code.as_deref(), Some("unprotected_position"));
    }

    #[test]
    fn mainnet_history_backfill_is_idempotent_and_advances_funding_cursor() {
        let store = ExecutionStore::in_memory().unwrap();
        let address = "0x0000000000000000000000000000000000000001";
        let fills = vec![MainnetFillRecord {
            hash: "0xfill".into(),
            oid: 7,
            time_ms: 1_000,
            coin: "BTC".into(),
            side: "B".into(),
            direction: "Open Long".into(),
            crossed: true,
            price: Decimal::new(100, 0),
            size: Decimal::ONE,
            closed_pnl_usd: Decimal::ZERO,
            fee_usd: Decimal::new(5, 2),
        }];
        let funding = vec![MainnetFundingRecord {
            hash: "0xfunding".into(),
            time_ms: 2_000,
            coin: "BTC".into(),
            usdc: Decimal::new(-1, 1),
            signed_size: Decimal::ONE,
            funding_rate: Decimal::new(1, 5),
        }];
        store
            .record_mainnet_history(address, &fills, &funding)
            .unwrap();
        let mut websocket_funding = funding.clone();
        websocket_funding[0].hash.clear();
        store
            .record_mainnet_history(address, &fills, &websocket_funding)
            .unwrap();

        assert_eq!(store.mainnet_history_cursor(address).unwrap(), Some(2_000));
        let connection = store.connection.lock().unwrap();
        let fill_count: u64 = connection
            .query_row("SELECT COUNT(*) FROM mainnet_fills", [], |row| row.get(0))
            .unwrap();
        let funding_count: u64 = connection
            .query_row("SELECT COUNT(*) FROM mainnet_funding", [], |row| row.get(0))
            .unwrap();
        assert_eq!((fill_count, funding_count), (1, 1));
    }

    #[test]
    fn mainnet_order_updates_are_idempotent() {
        let store = ExecutionStore::in_memory().unwrap();
        let address = "0x0000000000000000000000000000000000000001";
        let updates = vec![MainnetOrderUpdateRecord {
            oid: 42,
            status: "open".into(),
            status_timestamp_ms: 1_000,
            coin: "BTC".into(),
            side: "B".into(),
            limit_price: Decimal::new(100, 0),
            size: Decimal::ONE,
            original_size: Decimal::ONE,
            cloid: Some("0xcloid".into()),
        }];
        store
            .record_mainnet_order_updates(address, &updates)
            .unwrap();
        store
            .record_mainnet_order_updates(address, &updates)
            .unwrap();

        let connection = store.connection.lock().unwrap();
        let count: u64 = connection
            .query_row("SELECT COUNT(*) FROM mainnet_order_updates", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn migrates_v5_funding_to_cross_source_deduplication() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("v5.db");
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA user_version = 5;
                     CREATE TABLE mainnet_funding (
                        account_address TEXT NOT NULL,
                        exchange_hash TEXT NOT NULL,
                        time_ms INTEGER NOT NULL,
                        coin TEXT NOT NULL,
                        usdc TEXT NOT NULL,
                        signed_size TEXT NOT NULL,
                        funding_rate TEXT NOT NULL,
                        PRIMARY KEY(account_address, exchange_hash, time_ms, coin, usdc)
                     );
                     INSERT INTO mainnet_funding VALUES
                        ('account', 'rest-hash', 1000, 'BTC', '-0.1', '1', '0.00001'),
                        ('account', '',          1000, 'BTC', '-0.1', '1', '0.00001');",
                )
                .unwrap();
        }

        let store = ExecutionStore::open(path.to_str().unwrap()).unwrap();
        let connection = store.connection.lock().unwrap();
        let count: u64 = connection
            .query_row("SELECT COUNT(*) FROM mainnet_funding", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn migrates_v7_reconciliation_state_with_a_safe_recovery_default() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("v7.db");
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA user_version = 7;
                     CREATE TABLE mainnet_reconciliation_state (
                        account_address TEXT PRIMARY KEY,
                        mode TEXT NOT NULL,
                        ready INTEGER NOT NULL,
                        reason_code TEXT,
                        detail TEXT,
                        baseline_established_at TEXT,
                        last_reconciled_at TEXT
                     );
                     INSERT INTO mainnet_reconciliation_state VALUES
                        ('account', 'close_only', 0, 'unknown_exchange_order',
                         'detail', NULL, NULL);",
                )
                .unwrap();
        }

        let store = ExecutionStore::open(path.to_str().unwrap()).unwrap();
        let state = store.reconciliation_state("account").unwrap().unwrap();
        assert_eq!(state.clean_streak, 0);
        assert!(!state.recovery_eligible);
    }
}
