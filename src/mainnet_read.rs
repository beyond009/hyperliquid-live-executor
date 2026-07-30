use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use hyperliquid_rust_sdk::{BaseUrl, InfoClient, Message as HyperliquidMessage, Subscription};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls, connect_async,
    tungstenite::Message as WsMessage,
};
use url::Url;

use crate::store::ExecutionStore;
use crate::store::{ExecutionMode, ReconciliationState};

#[derive(Debug, Error)]
pub enum MainnetReadError {
    #[error("invalid Hyperliquid account address")]
    InvalidAddress,
    #[error("Hyperliquid SDK error: {0}")]
    Sdk(String),
    #[error("invalid decimal in {field}: {value}")]
    InvalidDecimal { field: String, value: String },
    #[error("failed to persist account snapshot: {0}")]
    Persistence(String),
    #[error("mainnet reconciliation mismatch: {0}")]
    ReconciliationMismatch(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MainnetPositionSnapshot {
    pub coin: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub signed_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub position_value_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub entry_price: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub liquidation_price: Option<Decimal>,
    pub leverage: u32,
    pub leverage_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MainnetOpenOrderSnapshot {
    pub coin: String,
    pub oid: u64,
    pub side: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub limit_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    pub timestamp_ms: u64,
    pub is_trigger: bool,
    pub reduce_only: bool,
    pub trigger_condition: String,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub trigger_price: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MainnetProtectionCoverage {
    pub coin: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub required_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub covered_size: Decimal,
    pub stop_order_count: usize,
    pub covered: bool,
    pub reason_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MainnetAccountSnapshot {
    pub account_address: String,
    pub observed_at: DateTime<Utc>,
    #[serde(with = "rust_decimal::serde::str")]
    pub account_value_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub withdrawable_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub total_margin_used_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub total_position_notional_usd: Decimal,
    pub positions: Vec<MainnetPositionSnapshot>,
    pub open_orders: Vec<MainnetOpenOrderSnapshot>,
    pub protection_coverage: Vec<MainnetProtectionCoverage>,
    pub asset_size_decimals: BTreeMap<String, u32>,
    pub mids: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MainnetFillRecord {
    pub hash: String,
    pub oid: u64,
    pub time_ms: u64,
    pub coin: String,
    pub side: String,
    pub direction: String,
    pub crossed: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub closed_pnl_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fee_usd: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MainnetFundingRecord {
    pub hash: String,
    pub time_ms: u64,
    pub coin: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub usdc: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub signed_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_rate: Decimal,
}

#[derive(Debug, Clone)]
pub struct MainnetHistoryBatch {
    pub fills: Vec<MainnetFillRecord>,
    pub funding: Vec<MainnetFundingRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MainnetOrderUpdateRecord {
    pub oid: u64,
    pub status: String,
    pub status_timestamp_ms: u64,
    pub coin: String,
    pub side: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub limit_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub original_size: Decimal,
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MainnetReadStatus {
    pub configured: bool,
    pub account_address: Option<String>,
    pub ready: bool,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub history_ready: bool,
    pub last_history_success_at: Option<DateTime<Utc>>,
    pub reconciliation_ready: bool,
    pub execution_mode: ExecutionMode,
    pub reconciliation_reason_code: Option<String>,
    pub reconciliation_detail: Option<String>,
    pub reconciliation_clean_streak: u32,
    pub reconciliation_recovery_eligible: bool,
    pub websocket_ready: bool,
    pub last_websocket_event_at: Option<DateTime<Utc>>,
    pub last_websocket_error: Option<String>,
    pub position_count: usize,
    pub open_order_count: usize,
}

impl MainnetReadStatus {
    pub fn unconfigured() -> Self {
        Self {
            configured: false,
            account_address: None,
            ready: false,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
            history_ready: false,
            last_history_success_at: None,
            reconciliation_ready: false,
            execution_mode: ExecutionMode::HaltNewEntries,
            reconciliation_reason_code: Some("mainnet_not_configured".into()),
            reconciliation_detail: None,
            reconciliation_clean_streak: 0,
            reconciliation_recovery_eligible: false,
            websocket_ready: false,
            last_websocket_event_at: None,
            last_websocket_error: None,
            position_count: 0,
            open_order_count: 0,
        }
    }
}

#[derive(Clone)]
pub struct MainnetReadHandle {
    status: Arc<RwLock<MainnetReadStatus>>,
}

impl MainnetReadHandle {
    pub async fn status(&self) -> MainnetReadStatus {
        self.status.read().await.clone()
    }
}

pub struct MainnetReadAdapter {
    account_address: String,
}

impl MainnetReadAdapter {
    pub fn new(account_address: String) -> Result<Self, MainnetReadError> {
        let normalized = account_address.to_ascii_lowercase();
        if normalized.len() != 42
            || !normalized.starts_with("0x")
            || !normalized[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(MainnetReadError::InvalidAddress);
        }
        Ok(Self {
            account_address: normalized,
        })
    }

    pub fn account_address(&self) -> &str {
        &self.account_address
    }

    pub async fn fetch_snapshot(&self) -> Result<MainnetAccountSnapshot, MainnetReadError> {
        let client = InfoClient::new(None, Some(BaseUrl::Mainnet))
            .await
            .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;
        let address = self
            .account_address
            .parse()
            .map_err(|_| MainnetReadError::InvalidAddress)?;
        let (meta, mids, user_state, basic_open_orders) = tokio::try_join!(
            client.meta(),
            client.all_mids(),
            client.user_state(address),
            client.open_orders(address),
        )
        .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;

        let positions = user_state
            .asset_positions
            .into_iter()
            .map(|asset| {
                let position = asset.position;
                Ok(MainnetPositionSnapshot {
                    coin: position.coin,
                    signed_size: decimal("position.szi", &position.szi)?,
                    position_value_usd: decimal(
                        "position.positionValue",
                        &position.position_value,
                    )?,
                    unrealized_pnl_usd: decimal(
                        "position.unrealizedPnl",
                        &position.unrealized_pnl,
                    )?,
                    entry_price: optional_decimal(
                        "position.entryPx",
                        position.entry_px.as_deref(),
                    )?,
                    liquidation_price: optional_decimal(
                        "position.liquidationPx",
                        position.liquidation_px.as_deref(),
                    )?,
                    leverage: position.leverage.value,
                    leverage_type: position.leverage.type_string,
                })
            })
            .collect::<Result<Vec<_>, MainnetReadError>>()?;
        let mut open_orders = Vec::with_capacity(basic_open_orders.len());
        for basic_order in basic_open_orders {
            let detail = client
                .query_order_by_oid(address, basic_order.oid)
                .await
                .map_err(|error| MainnetReadError::Sdk(error.to_string()))?
                .order
                .ok_or_else(|| {
                    MainnetReadError::ReconciliationMismatch(format!(
                        "open order {} has no order-status detail",
                        basic_order.oid
                    ))
                })?;
            let order = detail.order;
            if order.oid != basic_order.oid
                || order.coin != basic_order.coin
                || order.side != basic_order.side
                || decimal("orderStatus.limitPx", &order.limit_px)?
                    != decimal("openOrder.limitPx", &basic_order.limit_px)?
                || decimal("orderStatus.sz", &order.sz)?
                    != decimal("openOrder.sz", &basic_order.sz)?
                || order.timestamp != basic_order.timestamp
            {
                return Err(MainnetReadError::ReconciliationMismatch(format!(
                    "open order {} does not match its order-status detail",
                    basic_order.oid
                )));
            }
            let trigger_price = if order.is_trigger {
                Some(decimal("openOrder.triggerPx", &order.trigger_px)?)
            } else {
                None
            };
            open_orders.push(MainnetOpenOrderSnapshot {
                coin: order.coin,
                oid: order.oid,
                side: order.side,
                limit_price: decimal("openOrder.limitPx", &order.limit_px)?,
                size: decimal("openOrder.sz", &order.sz)?,
                timestamp_ms: order.timestamp,
                is_trigger: order.is_trigger,
                reduce_only: order.reduce_only,
                trigger_condition: order.trigger_condition,
                trigger_price,
            });
        }
        let relevant_coins = positions
            .iter()
            .map(|position| position.coin.as_str())
            .chain(open_orders.iter().map(|order| order.coin.as_str()))
            .collect::<BTreeSet<_>>();
        let mids = mids
            .into_iter()
            .filter(|(coin, _)| relevant_coins.contains(coin.as_str()))
            .map(|(coin, value)| {
                decimal(&format!("mids.{coin}"), &value)?;
                Ok((coin, value))
            })
            .collect::<Result<BTreeMap<_, _>, MainnetReadError>>()?;

        let asset_size_decimals = meta
            .universe
            .into_iter()
            .map(|asset| (asset.name, asset.sz_decimals))
            .collect::<BTreeMap<_, _>>();
        for coin in &relevant_coins {
            if !asset_size_decimals.contains_key(*coin) {
                return Err(MainnetReadError::ReconciliationMismatch(format!(
                    "active coin {coin} is absent from perpetual metadata"
                )));
            }
        }
        let total_position_notional = decimal(
            "marginSummary.totalNtlPos",
            &user_state.margin_summary.total_ntl_pos,
        )?;
        let projected_notional = positions
            .iter()
            .map(|position| position.position_value_usd.abs())
            .sum::<Decimal>();
        if (projected_notional - total_position_notional).abs() > Decimal::new(1, 2) {
            return Err(MainnetReadError::ReconciliationMismatch(format!(
                "position notional {projected_notional} differs from account summary {total_position_notional}"
            )));
        }
        let protection_coverage = evaluate_protection_coverage(&positions, &open_orders, &mids);

        Ok(MainnetAccountSnapshot {
            account_address: self.account_address.clone(),
            observed_at: Utc::now(),
            account_value_usd: decimal(
                "marginSummary.accountValue",
                &user_state.margin_summary.account_value,
            )?,
            withdrawable_usd: decimal("withdrawable", &user_state.withdrawable)?,
            total_margin_used_usd: decimal(
                "marginSummary.totalMarginUsed",
                &user_state.margin_summary.total_margin_used,
            )?,
            total_position_notional_usd: total_position_notional,
            positions,
            open_orders,
            protection_coverage,
            asset_size_decimals,
            mids,
        })
    }

    pub async fn fetch_history(
        &self,
        funding_start_ms: u64,
    ) -> Result<MainnetHistoryBatch, MainnetReadError> {
        let client = InfoClient::new(None, Some(BaseUrl::Mainnet))
            .await
            .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;
        let address = self
            .account_address
            .parse()
            .map_err(|_| MainnetReadError::InvalidAddress)?;
        let fills = client
            .user_fills(address)
            .await
            .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;
        let funding = client
            .user_funding_history(address, funding_start_ms, None)
            .await
            .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;
        Ok(MainnetHistoryBatch {
            fills: fills
                .into_iter()
                .map(|fill| {
                    Ok(MainnetFillRecord {
                        hash: fill.hash,
                        oid: fill.oid,
                        time_ms: fill.time,
                        coin: fill.coin,
                        side: fill.side,
                        direction: fill.dir,
                        crossed: fill.crossed,
                        price: decimal("fill.px", &fill.px)?,
                        size: decimal("fill.sz", &fill.sz)?,
                        closed_pnl_usd: decimal("fill.closedPnl", &fill.closed_pnl)?,
                        fee_usd: decimal("fill.fee", &fill.fee)?,
                    })
                })
                .collect::<Result<Vec<_>, MainnetReadError>>()?,
            funding: funding
                .into_iter()
                .map(|entry| {
                    Ok(MainnetFundingRecord {
                        hash: entry.hash,
                        time_ms: entry.time,
                        coin: entry.delta.coin,
                        usdc: decimal("funding.usdc", &entry.delta.usdc)?,
                        signed_size: decimal("funding.szi", &entry.delta.szi)?,
                        funding_rate: decimal("funding.fundingRate", &entry.delta.funding_rate)?,
                    })
                })
                .collect::<Result<Vec<_>, MainnetReadError>>()?,
        })
    }
}

fn evaluate_protection_coverage(
    positions: &[MainnetPositionSnapshot],
    open_orders: &[MainnetOpenOrderSnapshot],
    mids: &BTreeMap<String, String>,
) -> Vec<MainnetProtectionCoverage> {
    positions
        .iter()
        .filter(|position| !position.signed_size.is_zero())
        .map(|position| {
            let required_size = position.signed_size.abs();
            let reference_price = mids
                .get(&position.coin)
                .and_then(|value| value.parse::<Decimal>().ok())
                .or(position.entry_price);
            let eligible_orders = reference_price.map_or_else(Vec::new, |reference_price| {
                open_orders
                    .iter()
                    .filter(|order| is_stop_for_position(position, order, reference_price))
                    .collect::<Vec<_>>()
            });
            let covered_size = eligible_orders
                .iter()
                .map(|order| order.size)
                .sum::<Decimal>();
            let covered = covered_size >= required_size;
            MainnetProtectionCoverage {
                coin: position.coin.clone(),
                required_size,
                covered_size,
                stop_order_count: eligible_orders.len(),
                covered,
                reason_code: if covered {
                    None
                } else if reference_price.is_none() {
                    Some("missing_protection_reference_price".into())
                } else {
                    Some("insufficient_reduce_only_stop_coverage".into())
                },
            }
        })
        .collect()
}

fn is_stop_for_position(
    position: &MainnetPositionSnapshot,
    order: &MainnetOpenOrderSnapshot,
    reference_price: Decimal,
) -> bool {
    let Some(trigger_price) = order.trigger_price else {
        return false;
    };
    let is_long = position.signed_size.is_sign_positive();
    order.coin == position.coin
        && order.reduce_only
        && order.is_trigger
        && ((is_long && order.side == "A" && trigger_price < reference_price)
            || (!is_long && order.side == "B" && trigger_price > reference_price))
}

pub fn start_mainnet_read_service(
    adapter: MainnetReadAdapter,
    store: Arc<ExecutionStore>,
    poll_interval: Duration,
    stale_after: Duration,
) -> MainnetReadHandle {
    store
        .configure_mainnet_account(&adapter.account_address)
        .expect("failed to initialize mainnet reconciliation state");
    let status = Arc::new(RwLock::new(MainnetReadStatus {
        configured: true,
        account_address: Some(adapter.account_address.clone()),
        ready: false,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
        history_ready: false,
        last_history_success_at: None,
        reconciliation_ready: false,
        execution_mode: ExecutionMode::HaltNewEntries,
        reconciliation_reason_code: Some("startup_reconciliation_pending".into()),
        reconciliation_detail: Some("waiting for the first authoritative account snapshot".into()),
        reconciliation_clean_streak: 0,
        reconciliation_recovery_eligible: false,
        websocket_ready: false,
        last_websocket_event_at: None,
        last_websocket_error: None,
        position_count: 0,
        open_order_count: 0,
    }));
    let task_status = Arc::clone(&status);
    start_mainnet_websocket(
        adapter.account_address.clone(),
        Arc::clone(&store),
        Arc::clone(&status),
    );
    tokio::spawn(async move {
        let mut last_history_attempt: Option<DateTime<Utc>> = None;
        loop {
            match tokio::time::timeout(Duration::from_secs(10), adapter.fetch_snapshot()).await {
                Ok(Ok(snapshot)) => {
                    let observed_at = snapshot.observed_at;
                    let position_count = snapshot.positions.len();
                    let open_order_count = snapshot.open_orders.len();
                    let snapshot_store = Arc::clone(&store);
                    let persisted = tokio::task::spawn_blocking(move || {
                        snapshot_store
                            .record_mainnet_snapshot(&snapshot)
                            .map_err(|error| MainnetReadError::Persistence(error.to_string()))
                    })
                    .await
                    .map_err(|error| MainnetReadError::Persistence(error.to_string()))
                    .and_then(|result| result);
                    let mut current = task_status.write().await;
                    match persisted {
                        Ok(reconciliation) => {
                            current.last_success_at = Some(observed_at);
                            apply_reconciliation(&mut current, reconciliation);
                            current.last_error = None;
                            current.consecutive_failures = 0;
                            current.position_count = position_count;
                            current.open_order_count = open_order_count;
                        }
                        Err(error) => {
                            current.reconciliation_ready = false;
                            mark_failure(&mut current, error.to_string());
                        }
                    }
                }
                Ok(Err(error)) => {
                    let mut current = task_status.write().await;
                    current.reconciliation_ready = false;
                    mark_failure(&mut current, error.to_string());
                }
                Err(_) => {
                    let mut current = task_status.write().await;
                    current.reconciliation_ready = false;
                    mark_failure(&mut current, "mainnet snapshot request timed out".into());
                }
            }
            let history_due = last_history_attempt
                .is_none_or(|last| Utc::now().signed_duration_since(last).num_seconds() >= 60);
            if history_due {
                last_history_attempt = Some(Utc::now());
                let history_start = store
                    .mainnet_history_cursor(&adapter.account_address)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| {
                        (Utc::now() - chrono::Duration::days(7)).timestamp_millis() as u64
                    })
                    .saturating_sub(3_600_000);
                let history_result = tokio::time::timeout(
                    Duration::from_secs(10),
                    adapter.fetch_history(history_start),
                )
                .await;
                let result = match history_result {
                    Ok(Ok(batch)) => {
                        let history_store = Arc::clone(&store);
                        let address = adapter.account_address.clone();
                        tokio::task::spawn_blocking(move || {
                            history_store
                                .record_mainnet_history(&address, &batch.fills, &batch.funding)
                                .map_err(|error| MainnetReadError::Persistence(error.to_string()))
                        })
                        .await
                        .map_err(|error| MainnetReadError::Persistence(error.to_string()))
                        .and_then(|result| result)
                    }
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(MainnetReadError::Sdk("history request timed out".into())),
                };
                let mut current = task_status.write().await;
                match result {
                    Ok(()) => {
                        let now = Utc::now();
                        current.history_ready = true;
                        current.last_history_success_at = Some(now);
                    }
                    Err(error) => {
                        current.history_ready = false;
                        mark_failure(&mut current, error.to_string());
                    }
                }
            }
            {
                let mut current = task_status.write().await;
                let snapshot_fresh = current.last_success_at.is_some_and(|last| {
                    Utc::now()
                        .signed_duration_since(last)
                        .to_std()
                        .unwrap_or(Duration::MAX)
                        <= stale_after
                });
                let websocket_fresh = current.last_websocket_event_at.is_some_and(|last| {
                    Utc::now()
                        .signed_duration_since(last)
                        .to_std()
                        .unwrap_or(Duration::MAX)
                        <= stale_after
                });
                current.ready = snapshot_fresh
                    && current.history_ready
                    && current.reconciliation_ready
                    && current.websocket_ready
                    && websocket_fresh;
            }
            tokio::time::sleep(poll_interval).await;
        }
    });
    MainnetReadHandle { status }
}

fn apply_reconciliation(status: &mut MainnetReadStatus, state: ReconciliationState) {
    status.reconciliation_ready = state.ready;
    status.execution_mode = state.mode;
    status.reconciliation_reason_code = state.reason_code;
    status.reconciliation_detail = state.detail;
    status.reconciliation_clean_streak = state.clean_streak;
    status.reconciliation_recovery_eligible = state.recovery_eligible;
}

fn start_mainnet_websocket(
    account_address: String,
    store: Arc<ExecutionStore>,
    status: Arc<RwLock<MainnetReadStatus>>,
) {
    tokio::spawn(async move {
        loop {
            let result =
                run_mainnet_websocket(&account_address, Arc::clone(&store), Arc::clone(&status))
                    .await;
            {
                let mut current = status.write().await;
                current.websocket_ready = false;
                current.last_websocket_error = Some(result.err().map_or_else(
                    || "mainnet websocket channel closed".into(),
                    |error| error.to_string(),
                ));
                current.ready = false;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn run_mainnet_websocket(
    account_address: &str,
    store: Arc<ExecutionStore>,
    status: Arc<RwLock<MainnetReadStatus>>,
) -> Result<(), MainnetReadError> {
    let websocket = connect_mainnet_websocket(std::env::var("HL_WS_PROXY").ok().as_deref()).await?;
    let (mut writer, mut reader) = websocket.split();
    for subscription in [
        Subscription::AllMids,
        Subscription::UserFills {
            user: account_address
                .parse()
                .map_err(|_| MainnetReadError::InvalidAddress)?,
        },
        Subscription::UserFundings {
            user: account_address
                .parse()
                .map_err(|_| MainnetReadError::InvalidAddress)?,
        },
        Subscription::OrderUpdates {
            user: account_address
                .parse()
                .map_err(|_| MainnetReadError::InvalidAddress)?,
        },
    ] {
        let payload = serde_json::json!({
            "method": "subscribe",
            "subscription": subscription
        });
        writer
            .send(WsMessage::Text(payload.to_string().into()))
            .await
            .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;
    }
    {
        let mut current = status.write().await;
        current.last_websocket_error = None;
    }

    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let message = tokio::select! {
            _ = ping.tick() => {
                writer.send(WsMessage::Text(r#"{"method":"ping"}"#.into()))
                    .await
                    .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;
                continue;
            }
            message = reader.next() => match message {
                Some(Ok(WsMessage::Text(payload))) => serde_json::from_str::<HyperliquidMessage>(&payload)
                    .map_err(|error| MainnetReadError::Sdk(format!("invalid websocket payload: {error}")))?,
                Some(Ok(WsMessage::Ping(payload))) => {
                    writer.send(WsMessage::Pong(payload)).await
                        .map_err(|error| MainnetReadError::Sdk(error.to_string()))?;
                    continue;
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    return Err(MainnetReadError::Sdk(format!("websocket closed: {frame:?}")));
                }
                Some(Ok(_)) => continue,
                Some(Err(error)) => return Err(MainnetReadError::Sdk(error.to_string())),
                None => return Err(MainnetReadError::Sdk("websocket stream ended".into())),
            }
        };
        {
            let mut current = status.write().await;
            current.websocket_ready = true;
            current.last_websocket_event_at = Some(Utc::now());
        }
        let persistence = match message {
            HyperliquidMessage::UserFills(message) => {
                let fills = message
                    .data
                    .fills
                    .into_iter()
                    .map(ws_fill_record)
                    .collect::<Result<Vec<_>, _>>()?;
                let store = Arc::clone(&store);
                let account = account_address.to_owned();
                Some(tokio::task::spawn_blocking(move || {
                    store.record_mainnet_history(&account, &fills, &[])
                }))
            }
            HyperliquidMessage::UserFundings(message) => {
                let funding = message
                    .data
                    .fundings
                    .into_iter()
                    .map(ws_funding_record)
                    .collect::<Result<Vec<_>, _>>()?;
                let store = Arc::clone(&store);
                let account = account_address.to_owned();
                Some(tokio::task::spawn_blocking(move || {
                    store.record_mainnet_history(&account, &[], &funding)
                }))
            }
            HyperliquidMessage::OrderUpdates(message) => {
                let updates = message
                    .data
                    .into_iter()
                    .map(ws_order_update_record)
                    .collect::<Result<Vec<_>, _>>()?;
                let store = Arc::clone(&store);
                let account = account_address.to_owned();
                Some(tokio::task::spawn_blocking(move || {
                    store.record_mainnet_order_updates(&account, &updates)
                }))
            }
            HyperliquidMessage::HyperliquidError(error) => {
                return Err(MainnetReadError::Sdk(format!("websocket error: {error}")));
            }
            _ => None,
        };
        if let Some(persistence) = persistence {
            persistence
                .await
                .map_err(|error| MainnetReadError::Persistence(error.to_string()))?
                .map_err(|error| MainnetReadError::Persistence(error.to_string()))?;
        }
    }
}

async fn connect_mainnet_websocket(
    proxy: Option<&str>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, MainnetReadError> {
    const WEBSOCKET_URL: &str = "wss://api.hyperliquid.xyz/ws";
    let Some(proxy) = proxy else {
        return connect_async(WEBSOCKET_URL)
            .await
            .map(|(stream, _)| stream)
            .map_err(|error| MainnetReadError::Sdk(error.to_string()));
    };
    let proxy = Url::parse(proxy)
        .map_err(|error| MainnetReadError::Sdk(format!("invalid HL_WS_PROXY: {error}")))?;
    if proxy.scheme() != "http" || !proxy.username().is_empty() || proxy.password().is_some() {
        return Err(MainnetReadError::Sdk(
            "HL_WS_PROXY must be an unauthenticated http:// URL".into(),
        ));
    }
    let proxy_host = proxy
        .host_str()
        .ok_or_else(|| MainnetReadError::Sdk("HL_WS_PROXY has no host".into()))?;
    let proxy_port = proxy
        .port_or_known_default()
        .ok_or_else(|| MainnetReadError::Sdk("HL_WS_PROXY has no port".into()))?;
    let mut stream = TcpStream::connect((proxy_host, proxy_port))
        .await
        .map_err(|error| MainnetReadError::Sdk(format!("proxy connection failed: {error}")))?;
    stream
        .write_all(
            b"CONNECT api.hyperliquid.xyz:443 HTTP/1.1\r\nHost: api.hyperliquid.xyz:443\r\nProxy-Connection: Keep-Alive\r\n\r\n",
        )
        .await
        .map_err(|error| MainnetReadError::Sdk(format!("proxy CONNECT failed: {error}")))?;
    let mut response = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        if response.len() >= 8192 {
            return Err(MainnetReadError::Sdk(
                "proxy CONNECT response too large".into(),
            ));
        }
        stream.read_exact(&mut byte).await.map_err(|error| {
            MainnetReadError::Sdk(format!("proxy CONNECT response failed: {error}"))
        })?;
        response.push(byte[0]);
    }
    let status_line = String::from_utf8_lossy(&response)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    if !status_line.contains(" 200 ") {
        return Err(MainnetReadError::Sdk(format!(
            "proxy CONNECT rejected: {status_line}"
        )));
    }
    client_async_tls(WEBSOCKET_URL, stream)
        .await
        .map(|(stream, _)| stream)
        .map_err(|error| MainnetReadError::Sdk(error.to_string()))
}

fn ws_fill_record(
    fill: hyperliquid_rust_sdk::TradeInfo,
) -> Result<MainnetFillRecord, MainnetReadError> {
    Ok(MainnetFillRecord {
        hash: fill.hash,
        oid: fill.oid,
        time_ms: fill.time,
        coin: fill.coin,
        side: fill.side,
        direction: fill.dir,
        crossed: fill.crossed,
        price: decimal("wsFill.px", &fill.px)?,
        size: decimal("wsFill.sz", &fill.sz)?,
        closed_pnl_usd: decimal("wsFill.closedPnl", &fill.closed_pnl)?,
        fee_usd: decimal("wsFill.fee", &fill.fee)?,
    })
}

fn ws_funding_record(
    entry: hyperliquid_rust_sdk::UserFunding,
) -> Result<MainnetFundingRecord, MainnetReadError> {
    Ok(MainnetFundingRecord {
        hash: String::new(),
        time_ms: entry.time,
        coin: entry.coin,
        usdc: decimal("wsFunding.usdc", &entry.usdc)?,
        signed_size: decimal("wsFunding.szi", &entry.szi)?,
        funding_rate: decimal("wsFunding.fundingRate", &entry.funding_rate)?,
    })
}

fn ws_order_update_record(
    entry: hyperliquid_rust_sdk::OrderUpdate,
) -> Result<MainnetOrderUpdateRecord, MainnetReadError> {
    Ok(MainnetOrderUpdateRecord {
        oid: entry.order.oid,
        status: entry.status,
        status_timestamp_ms: entry.status_timestamp,
        coin: entry.order.coin,
        side: entry.order.side,
        limit_price: decimal("wsOrder.limitPx", &entry.order.limit_px)?,
        size: decimal("wsOrder.sz", &entry.order.sz)?,
        original_size: decimal("wsOrder.origSz", &entry.order.orig_sz)?,
        cloid: entry.order.cloid,
    })
}

fn mark_failure(status: &mut MainnetReadStatus, error: String) {
    status.ready = false;
    status.last_error = Some(error);
    status.consecutive_failures = status.consecutive_failures.saturating_add(1);
}

fn decimal(field: &str, value: &str) -> Result<Decimal, MainnetReadError> {
    value.parse().map_err(|_| MainnetReadError::InvalidDecimal {
        field: field.into(),
        value: value.into(),
    })
}

fn optional_decimal(field: &str, value: Option<&str>) -> Result<Option<Decimal>, MainnetReadError> {
    value.map(|value| decimal(field, value)).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_normalizes_account_addresses() {
        let adapter =
            MainnetReadAdapter::new("0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD".into()).unwrap();
        assert_eq!(
            adapter.account_address,
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        assert!(MainnetReadAdapter::new("not-an-address".into()).is_err());
    }

    #[test]
    fn decimal_parser_rejects_non_numeric_exchange_values() {
        assert_eq!(decimal("field", "1.25").unwrap(), Decimal::new(125, 2));
        assert!(decimal("field", "NaN").is_err());
    }

    fn position(coin: &str, signed_size: Decimal, entry_price: Decimal) -> MainnetPositionSnapshot {
        MainnetPositionSnapshot {
            coin: coin.into(),
            signed_size,
            position_value_usd: signed_size.abs() * entry_price,
            unrealized_pnl_usd: Decimal::ZERO,
            entry_price: Some(entry_price),
            liquidation_price: None,
            leverage: 1,
            leverage_type: "cross".into(),
        }
    }

    fn order(side: &str, size: Decimal, trigger_price: Decimal) -> MainnetOpenOrderSnapshot {
        MainnetOpenOrderSnapshot {
            coin: "BTC".into(),
            oid: 1,
            side: side.into(),
            limit_price: trigger_price,
            size,
            timestamp_ms: 1,
            is_trigger: true,
            reduce_only: true,
            trigger_condition: "Price below".into(),
            trigger_price: Some(trigger_price),
        }
    }

    #[test]
    fn recognizes_only_opposing_reduce_only_trigger_orders_as_stops() {
        let long = position("BTC", Decimal::new(2, 0), Decimal::new(100, 0));
        let short = position("BTC", Decimal::new(-2, 0), Decimal::new(100, 0));
        let long_stop = order("A", Decimal::new(2, 0), Decimal::new(95, 0));
        let short_stop = order("B", Decimal::new(2, 0), Decimal::new(105, 0));

        assert!(evaluate_protection_coverage(&[long], &[long_stop], &BTreeMap::new())[0].covered);
        assert!(evaluate_protection_coverage(&[short], &[short_stop], &BTreeMap::new())[0].covered);

        let take_profit = order("A", Decimal::new(2, 0), Decimal::new(105, 0));
        assert!(
            !evaluate_protection_coverage(
                &[position("BTC", Decimal::new(2, 0), Decimal::new(100, 0))],
                &[take_profit],
                &BTreeMap::new()
            )[0]
            .covered
        );

        let mut mids = BTreeMap::new();
        mids.insert("BTC".into(), "90".into());
        assert!(
            !evaluate_protection_coverage(
                &[position("BTC", Decimal::new(2, 0), Decimal::new(100, 0))],
                &[order("A", Decimal::new(2, 0), Decimal::new(95, 0))],
                &mids
            )[0]
            .covered
        );
    }
}
