use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    domain::TradeIntent,
    store::{ExecutionMode, ExecutionStore, StoreError, StoredIntent},
};
use rust_decimal::Decimal;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveExecutionStatus {
    Pending,
    EntrySubmitting,
    EntryUnknown,
    EntryFilled,
    Protecting,
    Protected,
    EmergencyClosing,
    Closed,
    Failed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderRole {
    Entry,
    StopLoss,
    TakeProfit(u8),
    EmergencyClose,
}

impl OrderRole {
    pub fn stable_name(self) -> String {
        match self {
            Self::Entry => "entry".into(),
            Self::StopLoss => "stop_loss".into(),
            Self::TakeProfit(index) => format!("take_profit_{index}"),
            Self::EmergencyClose => "emergency_close".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LiveExecution {
    pub intent_id: Uuid,
    pub account_address: String,
    pub entry_cloid: Uuid,
    pub status: LiveExecutionStatus,
    pub attempt: u32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfirmedFill {
    pub exchange_oid: u64,
    pub size: Decimal,
    pub average_price: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayFailure {
    Definite(String),
    OutcomeUnknown(String),
}

pub trait LiveExecutionGateway: Send + Sync + 'static {
    fn submit_entry(
        &self,
        intent: &TradeIntent,
        approved_notional: Decimal,
        cloid: Uuid,
    ) -> impl std::future::Future<Output = Result<ConfirmedFill, GatewayFailure>> + Send;

    fn establish_protection(
        &self,
        intent: &TradeIntent,
        fill: &ConfirmedFill,
    ) -> impl std::future::Future<Output = Result<(), GatewayFailure>> + Send;

    fn emergency_close(
        &self,
        intent: &TradeIntent,
        fill: &ConfirmedFill,
        cloid: Uuid,
    ) -> impl std::future::Future<Output = Result<(), GatewayFailure>> + Send;
}

pub struct LiveExecutionWorker<G> {
    store: Arc<ExecutionStore>,
    gateway: G,
    account_address: String,
}

impl<G: LiveExecutionGateway> LiveExecutionWorker<G> {
    pub fn new(store: Arc<ExecutionStore>, gateway: G, account_address: String) -> Self {
        Self {
            store,
            gateway,
            account_address,
        }
    }

    pub async fn run_once(&self) -> Result<bool, StoreError> {
        if let Some(state) = self.store.reconciliation_state(&self.account_address)?
            && (!state.ready || state.mode != ExecutionMode::Normal)
        {
            return Ok(false);
        }
        let Some((execution, stored)) = self
            .store
            .claim_next_live_execution(&self.account_address)?
        else {
            return Ok(false);
        };
        let approved_notional = stored
            .risk_decision
            .as_ref()
            .and_then(|decision| decision.approved_notional_usd)
            .expect("only approved risk decisions can enter live execution");
        match self
            .gateway
            .submit_entry(&stored.intent, approved_notional, execution.entry_cloid)
            .await
        {
            Ok(fill) => self.handle_fill(&stored, &fill).await?,
            Err(GatewayFailure::Definite(error)) => {
                self.store.mark_live_execution_failed(
                    stored.intent.intent_id,
                    "entry_rejected",
                    &error,
                )?;
            }
            Err(GatewayFailure::OutcomeUnknown(error)) => {
                self.store
                    .mark_live_entry_unknown(stored.intent.intent_id, &error)?;
            }
        }
        Ok(true)
    }

    async fn handle_fill(
        &self,
        stored: &StoredIntent,
        fill: &ConfirmedFill,
    ) -> Result<(), StoreError> {
        self.store
            .record_live_entry_fill(stored.intent.intent_id, fill)?;
        match self
            .gateway
            .establish_protection(&stored.intent, fill)
            .await
        {
            Ok(()) => {
                self.store
                    .record_live_protection_orders(&stored.intent, fill)?;
                self.store
                    .mark_live_execution_protected(stored.intent.intent_id)?;
            }
            Err(error) => {
                self.store.mark_live_emergency_closing(
                    stored.intent.intent_id,
                    gateway_error_text(&error),
                )?;
                let close_cloid =
                    deterministic_cloid(stored.intent.intent_id, OrderRole::EmergencyClose);
                match self
                    .gateway
                    .emergency_close(&stored.intent, fill, close_cloid)
                    .await
                {
                    Ok(()) => self
                        .store
                        .mark_live_execution_closed(stored.intent.intent_id)?,
                    Err(close_error) => self.store.mark_live_reconciliation_required(
                        stored.intent.intent_id,
                        gateway_error_text(&close_error),
                    )?,
                }
            }
        }
        Ok(())
    }
}

fn gateway_error_text(error: &GatewayFailure) -> &str {
    match error {
        GatewayFailure::Definite(error) | GatewayFailure::OutcomeUnknown(error) => error,
    }
}

pub fn deterministic_cloid(intent_id: Uuid, role: OrderRole) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"hyperliquid-executor-cloid-v1");
    digest.update(intent_id.as_bytes());
    digest.update(role.stable_name().as_bytes());
    let hash = digest.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_decimal::Decimal;
    use std::sync::Mutex;

    use crate::{
        domain::{
            EntryPolicy, EntryType, ExitPolicy, ManualConfirmation, Side,
            TRADE_INTENT_SCHEMA_VERSION, TakeProfitTarget, TradeIntentStatus, confirmation_digest,
        },
        risk::{RiskEngine, RiskPolicy},
    };

    #[derive(Clone, Copy)]
    enum FakeOutcome {
        Success,
        EntryDefinite,
        EntryUnknown,
        ProtectionFails,
        EmergencyFails,
    }

    struct FakeGateway {
        outcome: FakeOutcome,
        calls: Mutex<Vec<(OrderRole, Uuid)>>,
    }

    impl LiveExecutionGateway for Arc<FakeGateway> {
        async fn submit_entry(
            &self,
            _intent: &TradeIntent,
            _approved_notional: Decimal,
            cloid: Uuid,
        ) -> Result<ConfirmedFill, GatewayFailure> {
            self.calls.lock().unwrap().push((OrderRole::Entry, cloid));
            match self.outcome {
                FakeOutcome::EntryDefinite => Err(GatewayFailure::Definite("rejected".into())),
                FakeOutcome::EntryUnknown => Err(GatewayFailure::OutcomeUnknown("timeout".into())),
                _ => Ok(ConfirmedFill {
                    exchange_oid: 42,
                    size: Decimal::new(1, 3),
                    average_price: Decimal::new(100_000, 0),
                }),
            }
        }

        async fn establish_protection(
            &self,
            intent: &TradeIntent,
            _fill: &ConfirmedFill,
        ) -> Result<(), GatewayFailure> {
            self.calls.lock().unwrap().push((
                OrderRole::StopLoss,
                deterministic_cloid(intent.intent_id, OrderRole::StopLoss),
            ));
            match self.outcome {
                FakeOutcome::ProtectionFails | FakeOutcome::EmergencyFails => {
                    Err(GatewayFailure::Definite("stop rejected".into()))
                }
                _ => Ok(()),
            }
        }

        async fn emergency_close(
            &self,
            _intent: &TradeIntent,
            _fill: &ConfirmedFill,
            cloid: Uuid,
        ) -> Result<(), GatewayFailure> {
            self.calls
                .lock()
                .unwrap()
                .push((OrderRole::EmergencyClose, cloid));
            match self.outcome {
                FakeOutcome::EmergencyFails => {
                    Err(GatewayFailure::OutcomeUnknown("close timeout".into()))
                }
                _ => Ok(()),
            }
        }
    }

    fn intent() -> TradeIntent {
        TradeIntent {
            schema_version: TRADE_INTENT_SCHEMA_VERSION,
            intent_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4().to_string(),
            strategy_id: "worker-test".into(),
            strategy_version: "1".into(),
            strategy_instance_id: "worker-test-1".into(),
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

    fn prepared_worker(
        outcome: FakeOutcome,
    ) -> (
        Arc<ExecutionStore>,
        LiveExecutionWorker<Arc<FakeGateway>>,
        Arc<FakeGateway>,
        Uuid,
    ) {
        let store = Arc::new(ExecutionStore::in_memory().unwrap());
        let intent = intent();
        let decision = RiskEngine::new(RiskPolicy::shadow_tiny_default())
            .unwrap()
            .evaluate_admission(&intent);
        let expires_at = Utc::now() + Duration::minutes(2);
        let confirmation = ManualConfirmation {
            digest: confirmation_digest(&intent, &decision, expires_at).unwrap(),
            expires_at,
            confirmed_at: None,
            confirmed_by: None,
        };
        store
            .submit_shadow_intent(&intent, "worker-key", &decision, Some(&confirmation))
            .unwrap();
        store
            .confirm_intent_for_execution(
                intent.intent_id,
                &confirmation.digest,
                "operator",
                "0x0000000000000000000000000000000000000001",
            )
            .unwrap();
        let gateway = Arc::new(FakeGateway {
            outcome,
            calls: Mutex::new(vec![]),
        });
        let worker = LiveExecutionWorker::new(
            store.clone(),
            gateway.clone(),
            "0x0000000000000000000000000000000000000001".into(),
        );
        (store, worker, gateway, intent.intent_id)
    }

    #[test]
    fn cloids_are_stable_and_role_scoped() {
        let intent = Uuid::parse_str("018f6f61-1d72-7e87-a5db-c3a3a61d7468").unwrap();
        assert_eq!(
            deterministic_cloid(intent, OrderRole::Entry),
            deterministic_cloid(intent, OrderRole::Entry)
        );
        assert_ne!(
            deterministic_cloid(intent, OrderRole::Entry),
            deterministic_cloid(intent, OrderRole::StopLoss)
        );
        assert_ne!(
            deterministic_cloid(intent, OrderRole::TakeProfit(0)),
            deterministic_cloid(intent, OrderRole::TakeProfit(1))
        );
    }

    #[tokio::test]
    async fn successful_execution_is_claimed_once_and_protected() {
        let (store, worker, gateway, intent_id) = prepared_worker(FakeOutcome::Success);
        assert!(worker.run_once().await.unwrap());
        assert!(!worker.run_once().await.unwrap());
        assert_eq!(
            store.get_live_execution(intent_id).unwrap().unwrap().status,
            LiveExecutionStatus::Protected
        );
        assert_eq!(
            store.get_intent(intent_id).unwrap().unwrap().status,
            TradeIntentStatus::Completed
        );
        assert_eq!(
            gateway
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(role, _)| *role == OrderRole::Entry)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn unknown_entry_is_never_retried_and_locks_close_only() {
        let (store, worker, gateway, intent_id) = prepared_worker(FakeOutcome::EntryUnknown);
        assert!(worker.run_once().await.unwrap());
        assert!(!worker.run_once().await.unwrap());
        assert_eq!(
            store.get_live_execution(intent_id).unwrap().unwrap().status,
            LiveExecutionStatus::EntryUnknown
        );
        assert_eq!(gateway.calls.lock().unwrap().len(), 1);
        let state = store
            .reconciliation_state("0x0000000000000000000000000000000000000001")
            .unwrap()
            .unwrap();
        assert_eq!(state.mode, ExecutionMode::CloseOnly);
    }

    #[tokio::test]
    async fn definite_entry_rejection_does_not_emergency_close() {
        let (store, worker, gateway, intent_id) = prepared_worker(FakeOutcome::EntryDefinite);
        assert!(worker.run_once().await.unwrap());
        assert_eq!(
            store.get_live_execution(intent_id).unwrap().unwrap().status,
            LiveExecutionStatus::Failed
        );
        assert_eq!(gateway.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failed_protection_uses_one_deterministic_emergency_close() {
        let (store, worker, gateway, intent_id) = prepared_worker(FakeOutcome::ProtectionFails);
        assert!(worker.run_once().await.unwrap());
        assert_eq!(
            store.get_live_execution(intent_id).unwrap().unwrap().status,
            LiveExecutionStatus::Closed
        );
        let calls = gateway.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|(role, _)| *role == OrderRole::EmergencyClose)
                .count(),
            1
        );
        assert!(calls.contains(&(
            OrderRole::EmergencyClose,
            deterministic_cloid(intent_id, OrderRole::EmergencyClose)
        )));
    }

    #[tokio::test]
    async fn unknown_emergency_close_requires_reconciliation() {
        let (store, worker, _, intent_id) = prepared_worker(FakeOutcome::EmergencyFails);
        assert!(worker.run_once().await.unwrap());
        assert_eq!(
            store.get_live_execution(intent_id).unwrap().unwrap().status,
            LiveExecutionStatus::ReconciliationRequired
        );
    }
}
