use std::sync::Arc;

use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    domain::{IntentValidationError, ManualConfirmation, TradeIntent, confirmation_digest},
    risk::{RiskEngine, RiskPolicy, RiskPolicyError},
    store::{ExecutionStore, StoreError, StoredIntent},
};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("intent validation failed: {0}")]
    Validation(#[from] IntentValidationError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("failed to build manual confirmation: {0}")]
    ConfirmationSerialization(#[from] serde_json::Error),
    #[error("new entries blocked by reconciliation: {0}")]
    ReconciliationBlocked(String),
    #[error("pre-trade risk rejected confirmation: {0}")]
    PreTradeRejected(String),
}

#[derive(Clone)]
pub struct ShadowExecutionEngine {
    store: Arc<ExecutionStore>,
    risk: RiskEngine,
    confirmation_ttl: Duration,
    reconciliation_account: Option<String>,
    wallet_loaded: bool,
    live_execution_account: Option<String>,
}

impl ShadowExecutionEngine {
    pub fn new(store: Arc<ExecutionStore>, risk: RiskEngine) -> Self {
        Self {
            store,
            risk,
            confirmation_ttl: Duration::minutes(2),
            reconciliation_account: None,
            wallet_loaded: false,
            live_execution_account: None,
        }
    }

    pub fn with_mainnet_reconciliation(mut self, account_address: String) -> Self {
        self.reconciliation_account = Some(account_address);
        self
    }

    pub fn with_wallet_loaded(mut self) -> Self {
        self.wallet_loaded = true;
        self
    }

    pub fn with_live_execution(mut self, account_address: String) -> Self {
        self.live_execution_account = Some(account_address);
        self
    }

    pub fn with_default_risk(store: Arc<ExecutionStore>) -> Result<Self, RiskPolicyError> {
        Ok(Self::new(
            store,
            RiskEngine::new(RiskPolicy::shadow_tiny_default())?,
        ))
    }

    pub fn submit(
        &self,
        intent: TradeIntent,
        idempotency_key: &str,
    ) -> Result<(StoredIntent, bool), EngineError> {
        self.ensure_reconciliation_ready()?;
        intent.validate(Utc::now())?;
        let risk_decision = self.risk.evaluate_admission(&intent);
        let confirmation = if risk_decision.is_approved() {
            let expires_at = intent.expires_at.min(Utc::now() + self.confirmation_ttl);
            Some(ManualConfirmation {
                digest: confirmation_digest(&intent, &risk_decision, expires_at)?,
                expires_at,
                confirmed_at: None,
                confirmed_by: None,
            })
        } else {
            None
        };
        Ok(self.store.submit_shadow_intent(
            &intent,
            idempotency_key,
            &risk_decision,
            confirmation.as_ref(),
        )?)
    }

    pub fn confirm(
        &self,
        intent_id: uuid::Uuid,
        digest: &str,
        confirmed_by: &str,
    ) -> Result<StoredIntent, EngineError> {
        self.ensure_reconciliation_ready()?;
        let stored = self
            .store
            .get_intent(intent_id)?
            .ok_or_else(|| EngineError::PreTradeRejected("intent_not_found".into()))?;
        stored.intent.validate(Utc::now())?;
        let fresh_decision = self.risk.evaluate_admission(&stored.intent);
        if !fresh_decision.is_approved() {
            self.store.transition_intent(
                intent_id,
                crate::domain::TradeIntentStatus::RiskRejected,
                Some("confirmation_pre_trade_rejected"),
            )?;
            return Err(EngineError::PreTradeRejected(format!(
                "{:?}",
                fresh_decision.reason_codes
            )));
        }
        self.ensure_no_existing_position(&stored.intent.symbol)?;
        match &self.live_execution_account {
            Some(account) => Ok(self.store.confirm_intent_for_execution(
                intent_id,
                digest,
                confirmed_by,
                account,
            )?),
            None => Ok(self.store.confirm_intent(intent_id, digest, confirmed_by)?),
        }
    }

    fn ensure_reconciliation_ready(&self) -> Result<(), EngineError> {
        let Some(account) = &self.reconciliation_account else {
            return Ok(());
        };
        match self.store.reconciliation_state(account)? {
            Some(state) if !state.ready => {
                return Err(EngineError::ReconciliationBlocked(
                    state
                        .reason_code
                        .unwrap_or_else(|| "reconciliation_not_ready".into()),
                ));
            }
            Some(state)
                if state.last_reconciled_at.is_none_or(|observed| {
                    Utc::now().signed_duration_since(observed) > Duration::seconds(20)
                }) =>
            {
                return Err(EngineError::ReconciliationBlocked(
                    "authoritative_snapshot_stale".into(),
                ));
            }
            None => {
                return Err(EngineError::ReconciliationBlocked(
                    "startup_reconciliation_pending".into(),
                ));
            }
            Some(_) => {}
        }
        Ok(())
    }

    fn ensure_no_existing_position(&self, symbol: &str) -> Result<(), EngineError> {
        let Some(account) = &self.reconciliation_account else {
            return Ok(());
        };
        let snapshot = self
            .store
            .latest_mainnet_snapshot(account)?
            .ok_or_else(|| {
                EngineError::ReconciliationBlocked("authoritative_snapshot_missing".into())
            })?;
        let positions = snapshot
            .get("positions")
            .and_then(|value| value.as_array())
            .ok_or_else(|| {
                EngineError::ReconciliationBlocked("authoritative_positions_missing".into())
            })?;
        for position in positions {
            let same_symbol = position.get("coin").and_then(|value| value.as_str()) == Some(symbol);
            let nonzero = position
                .get("signedSize")
                .and_then(|value| value.as_str())
                .and_then(|value| value.parse::<Decimal>().ok())
                .is_some_and(|size| !size.is_zero());
            if same_symbol && nonzero {
                return Err(EngineError::PreTradeRejected(
                    "existing_symbol_position".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn store(&self) -> &Arc<ExecutionStore> {
        &self.store
    }

    pub fn risk(&self) -> &RiskEngine {
        &self.risk
    }

    pub fn wallet_loaded(&self) -> bool {
        self.wallet_loaded
    }
}
