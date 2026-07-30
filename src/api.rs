use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    domain::TradeIntent,
    engine::{EngineError, ShadowExecutionEngine},
    mainnet_read::{MainnetReadHandle, MainnetReadStatus},
    store::{ExecutionEvent, StoreError, StoredIntent},
};

#[derive(Clone)]
pub struct ApiState {
    pub engine: ShadowExecutionEngine,
    pub mainnet_read: Option<MainnetReadHandle>,
}

pub fn router(engine: ShadowExecutionEngine) -> Router {
    router_with_mainnet(engine, None)
}

pub fn router_with_mainnet(
    engine: ShadowExecutionEngine,
    mainnet_read: Option<MainnetReadHandle>,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/trade-intents", post(create_intent))
        .route("/v1/trade-intents/{intent_id}", get(get_intent))
        .route(
            "/v1/trade-intents/{intent_id}/confirm",
            post(confirm_intent),
        )
        .route("/v1/events", get(get_events))
        .route("/v1/mainnet/status", get(mainnet_status))
        .route("/v1/mainnet/account-snapshot", get(mainnet_snapshot))
        .route("/v1/console/stream", get(console_stream))
        .route("/v1/control/kill-switch", post(set_kill_switch))
        .route(
            "/v1/control/reconciliation/acknowledge",
            post(acknowledge_reconciliation),
        )
        .with_state(Arc::new(ApiState {
            engine,
            mainnet_read,
        }))
}

async fn health(State(state): State<Arc<ApiState>>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "mode": "shadow",
        "walletLoaded": state.engine.wallet_loaded()
    }))
}

async fn ready(
    State(state): State<Arc<ApiState>>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let kill_switch_enabled = state.engine.store().kill_switch_enabled()?;
    let mainnet = mainnet_status_value(&state).await;
    let ready = !mainnet.configured || mainnet.ready;
    Ok((
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "status": if ready { "ready" } else { "not_ready" },
            "mode": "shadow",
            "killSwitchEnabled": kill_switch_enabled,
            "riskPolicyVersion": state.engine.risk().policy().version,
            "manualConfirmationRequired": true,
            "walletLoaded": state.engine.wallet_loaded(),
            "mainnetRead": mainnet
        })),
    ))
}

async fn mainnet_status_value(state: &ApiState) -> MainnetReadStatus {
    match &state.mainnet_read {
        Some(handle) => handle.status().await,
        None => MainnetReadStatus::unconfigured(),
    }
}

async fn mainnet_status(State(state): State<Arc<ApiState>>) -> Json<MainnetReadStatus> {
    Json(mainnet_status_value(&state).await)
}

async fn mainnet_snapshot(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let status = mainnet_status_value(&state).await;
    let address = status.account_address.ok_or(ApiError::NotFound)?;
    state
        .engine
        .store()
        .latest_mainnet_snapshot(&address)?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

async fn console_stream(
    websocket: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
) -> Response {
    websocket.on_upgrade(move |socket| run_console_stream(socket, state))
}

async fn run_console_stream(mut socket: WebSocket, state: Arc<ApiState>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut event_cursor = 0i64;
    let mut sequence = 0u64;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                sequence = sequence.saturating_add(1);
                let payload = match console_stream_payload(&state, event_cursor, sequence).await {
                    Ok(payload) => payload,
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to build console stream payload");
                        continue;
                    }
                };
                event_cursor = payload["events"]["nextCursor"].as_i64().unwrap_or(event_cursor);
                if socket.send(Message::Text(payload.to_string().into())).await.is_err() {
                    break;
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn console_stream_payload(
    state: &ApiState,
    event_cursor: i64,
    sequence: u64,
) -> Result<serde_json::Value, StoreError> {
    let mainnet = mainnet_status_value(state).await;
    let kill_switch_enabled = state.engine.store().kill_switch_enabled()?;
    let ready = !mainnet.configured || mainnet.ready;
    let snapshot = match mainnet.account_address.as_deref() {
        Some(address) => state.engine.store().latest_mainnet_snapshot(address)?,
        None => None,
    };
    let events = state.engine.store().events_after(event_cursor, 100)?;
    let next_cursor = events.last().map_or(event_cursor, |event| event.cursor);
    Ok(json!({
        "type": "console_state",
        "sequence": sequence,
        "emittedAt": Utc::now(),
        "ready": {
            "status": if ready { "ready" } else { "not_ready" },
            "mode": "shadow",
            "killSwitchEnabled": kill_switch_enabled,
            "riskPolicyVersion": state.engine.risk().policy().version,
            "manualConfirmationRequired": true,
            "walletLoaded": state.engine.wallet_loaded(),
            "mainnetRead": mainnet
        },
        "snapshot": snapshot,
        "events": {
            "events": events,
            "nextCursor": next_cursor
        }
    }))
}

async fn create_intent(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Json(intent): Json<TradeIntent>,
) -> Result<(StatusCode, Json<StoredIntent>), ApiError> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::BadRequest("Idempotency-Key header is required".into()))?;
    if key.len() > 128 {
        return Err(ApiError::BadRequest("Idempotency-Key is too long".into()));
    }
    let (stored, created) = state.engine.submit(intent, key)?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(stored),
    ))
}

async fn get_intent(
    State(state): State<Arc<ApiState>>,
    Path(intent_id): Path<Uuid>,
) -> Result<Json<StoredIntent>, ApiError> {
    state
        .engine
        .store()
        .get_intent(intent_id)?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfirmIntentRequest {
    digest: String,
    confirmed_by: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReconciliationAcknowledgeRequest {
    account_address: String,
    expected_reason_code: String,
    acknowledged_by: String,
}

async fn confirm_intent(
    State(state): State<Arc<ApiState>>,
    Path(intent_id): Path<Uuid>,
    Json(request): Json<ConfirmIntentRequest>,
) -> Result<Json<StoredIntent>, ApiError> {
    if request.digest.len() != 64 || !request.digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest("invalid confirmation digest".into()));
    }
    if request.confirmed_by.trim().is_empty() || request.confirmed_by.len() > 64 {
        return Err(ApiError::BadRequest("confirmedBy is invalid".into()));
    }
    Ok(Json(state.engine.confirm(
        intent_id,
        &request.digest,
        &request.confirmed_by,
    )?))
}

#[derive(Debug, Deserialize)]
pub struct EventQuery {
    #[serde(default)]
    after: i64,
    #[serde(default = "default_event_limit")]
    limit: usize,
}

fn default_event_limit() -> usize {
    100
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventPage {
    events: Vec<ExecutionEvent>,
    next_cursor: i64,
}

async fn get_events(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<EventQuery>,
) -> Result<Json<EventPage>, ApiError> {
    let events = state
        .engine
        .store()
        .events_after(query.after.max(0), query.limit.max(1))?;
    let next_cursor = events
        .last()
        .map(|event| event.cursor)
        .unwrap_or(query.after.max(0));
    Ok(Json(EventPage {
        events,
        next_cursor,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KillSwitchRequest {
    enabled: bool,
    reason: Option<String>,
}

async fn set_kill_switch(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<KillSwitchRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if request.enabled
        && request
            .reason
            .as_deref()
            .is_none_or(|reason| reason.trim().is_empty())
    {
        return Err(ApiError::BadRequest(
            "reason is required when enabling kill switch".into(),
        ));
    }
    state
        .engine
        .store()
        .set_kill_switch(request.enabled, request.reason.as_deref())?;
    Ok(Json(
        json!({ "enabled": request.enabled, "reason": request.reason }),
    ))
}

async fn acknowledge_reconciliation(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<ReconciliationAcknowledgeRequest>,
) -> Result<Json<crate::store::ReconciliationState>, ApiError> {
    if request.acknowledged_by.trim().is_empty() || request.acknowledged_by.len() > 64 {
        return Err(ApiError::BadRequest("acknowledgedBy is invalid".into()));
    }
    if request.expected_reason_code.trim().is_empty() || request.expected_reason_code.len() > 128 {
        return Err(ApiError::BadRequest("expectedReasonCode is invalid".into()));
    }
    let mainnet = mainnet_status_value(&state).await;
    if mainnet.account_address.as_deref() != Some(request.account_address.as_str()) {
        return Err(ApiError::BadRequest(
            "accountAddress does not match the configured mainnet account".into(),
        ));
    }
    Ok(Json(
        state.engine.store().acknowledge_reconciliation_recovery(
            &request.account_address,
            &request.expected_reason_code,
            &request.acknowledged_by,
        )?,
    ))
}

pub enum ApiError {
    BadRequest(String),
    Conflict(String),
    Gone(String),
    NotFound,
    Internal(String),
    Unavailable(String),
}

impl From<StoreError> for ApiError {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::IdempotencyConflict
            | StoreError::IntentIdConflict
            | StoreError::SignalReplay
            | StoreError::ConfirmationMismatch
            | StoreError::NotAwaitingConfirmation
            | StoreError::ReconciliationNotCloseOnly
            | StoreError::ReconciliationRecoveryNotEligible
            | StoreError::ReconciliationReasonMismatch => Self::Conflict(value.to_string()),
            StoreError::ConfirmationExpired => Self::Gone(value.to_string()),
            _ => Self::Internal(value.to_string()),
        }
    }
}

impl From<EngineError> for ApiError {
    fn from(value: EngineError) -> Self {
        match value {
            EngineError::Validation(error) => Self::BadRequest(error.to_string()),
            EngineError::ConfirmationSerialization(error) => Self::Internal(error.to_string()),
            EngineError::ReconciliationBlocked(error) => Self::Unavailable(error),
            EngineError::PreTradeRejected(error) => Self::Conflict(error),
            EngineError::Store(error) => error.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::Gone(message) => (StatusCode::GONE, message),
            Self::NotFound => (StatusCode::NOT_FOUND, "not found".into()),
            Self::Internal(message) => {
                tracing::error!(error = %message, "executor API error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
            }
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            EntryPolicy, EntryType, ExitPolicy, Side, TRADE_INTENT_SCHEMA_VERSION, TakeProfitTarget,
        },
        store::ExecutionStore,
    };
    use axum::{body::Body, http::Request};
    use chrono::{Duration, Utc};
    use rust_decimal::Decimal;
    use tower::ServiceExt;

    fn intent() -> TradeIntent {
        TradeIntent {
            schema_version: TRADE_INTENT_SCHEMA_VERSION,
            intent_id: Uuid::new_v4(),
            strategy_id: "api-test".into(),
            strategy_version: "v1".into(),
            strategy_instance_id: "primary".into(),
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

    fn request(intent: &TradeIntent, key: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/trade-intents")
            .header("content-type", "application/json")
            .header("idempotency-key", key)
            .body(Body::from(serde_json::to_vec(intent).unwrap()))
            .unwrap()
    }

    fn confirmation_request(intent_id: Uuid, digest: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(format!("/v1/trade-intents/{intent_id}/confirm"))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "digest": digest,
                    "confirmedBy": "local-operator"
                })
                .to_string(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn reports_payload_mismatch_as_conflict() {
        let store = Arc::new(ExecutionStore::in_memory().unwrap());
        let app = router(ShadowExecutionEngine::with_default_risk(store).unwrap());
        let first = app
            .clone()
            .oneshot(request(&intent(), "same-key"))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::CREATED);

        let second = app.oneshot(request(&intent(), "same-key")).await.unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn rejects_unknown_contract_versions() {
        let store = Arc::new(ExecutionStore::in_memory().unwrap());
        let app = router(ShadowExecutionEngine::with_default_risk(store).unwrap());
        let mut invalid = intent();
        invalid.schema_version += 1;

        let response = app.oneshot(request(&invalid, "version-key")).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn persists_policy_rejections_as_auditable_intents() {
        let store = Arc::new(ExecutionStore::in_memory().unwrap());
        let app = router(ShadowExecutionEngine::with_default_risk(store.clone()).unwrap());
        let mut rejected = intent();
        rejected.account_id = "not-allowed".into();

        let response = app
            .oneshot(request(&rejected, "rejected-key"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let stored = store.get_intent(rejected.intent_id).unwrap().unwrap();
        assert_eq!(
            stored.status,
            crate::domain::TradeIntentStatus::RiskRejected
        );
        assert_eq!(
            stored.status_reason.as_deref(),
            Some("risk_policy_rejected")
        );
    }

    #[tokio::test]
    async fn requires_bound_manual_confirmation_before_shadow_acceptance() {
        let store = Arc::new(ExecutionStore::in_memory().unwrap());
        let app = router(ShadowExecutionEngine::with_default_risk(store.clone()).unwrap());
        let intent = intent();
        let response = app
            .clone()
            .oneshot(request(&intent, "manual-key"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let pending = store.get_intent(intent.intent_id).unwrap().unwrap();
        assert_eq!(
            pending.status,
            crate::domain::TradeIntentStatus::AwaitingConfirmation
        );
        let digest = pending.manual_confirmation.unwrap().digest;

        let response = app
            .oneshot(confirmation_request(intent.intent_id, &digest))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            store.get_intent(intent.intent_id).unwrap().unwrap().status,
            crate::domain::TradeIntentStatus::ShadowAccepted
        );
    }
}
