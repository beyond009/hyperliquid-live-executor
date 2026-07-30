use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const TRADE_INTENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Long,
    Short,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EntryPolicy {
    #[serde(rename = "type")]
    pub kind: EntryType,
    pub max_slippage_bps: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntryType {
    MarketIoc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TakeProfitTarget {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    pub position_pct: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExitPolicy {
    #[serde(with = "rust_decimal::serde::str")]
    pub stop_loss_price: Decimal,
    pub take_profit: Vec<TakeProfitTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TradeIntent {
    pub schema_version: u16,
    pub intent_id: Uuid,
    pub strategy_id: String,
    pub strategy_version: String,
    pub strategy_instance_id: String,
    pub signal_id: String,
    pub account_id: String,
    pub symbol: String,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub reference_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_notional_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_risk_usd: Decimal,
    pub entry: EntryPolicy,
    pub exit: ExitPolicy,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TradeIntentStatus {
    Received,
    AwaitingConfirmation,
    RiskRejected,
    ShadowAccepted,
    Approved,
    Submitting,
    Executing,
    Completed,
    CancelPending,
    Canceled,
    Failed,
}

impl TradeIntentStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        use TradeIntentStatus::*;
        matches!(
            (self, next),
            (
                Received,
                RiskRejected | AwaitingConfirmation | ShadowAccepted | Approved | Canceled
            ) | (
                AwaitingConfirmation,
                RiskRejected | ShadowAccepted | Approved | Canceled
            ) | (ShadowAccepted, Canceled)
                | (Approved, Submitting | Canceled | Failed)
                | (Submitting, Executing | Failed | CancelPending)
                | (Executing, Completed | Failed | CancelPending)
                | (CancelPending, Canceled | Completed | Failed)
        )
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IntentValidationError {
    #[error("unsupported schemaVersion; expected {TRADE_INTENT_SCHEMA_VERSION}")]
    UnsupportedSchemaVersion,
    #[error("strategyId is invalid")]
    InvalidStrategyId,
    #[error("strategyVersion is required")]
    MissingStrategyVersion,
    #[error("strategyInstanceId is invalid")]
    InvalidStrategyInstanceId,
    #[error("signalId is invalid")]
    InvalidSignalId,
    #[error("accountId is invalid")]
    InvalidAccountId,
    #[error(
        "symbol must contain only uppercase letters, digits, ':' or '-' and be at most 32 characters"
    )]
    InvalidSymbol,
    #[error("maxNotionalUsd must be positive")]
    InvalidNotional,
    #[error("referencePrice must be positive")]
    InvalidReferencePrice,
    #[error("maxRiskUsd must be positive and no larger than maxNotionalUsd")]
    InvalidRisk,
    #[error("maxSlippageBps must be between 1 and 500")]
    InvalidSlippage,
    #[error("stopLossPrice must be positive")]
    InvalidStopLoss,
    #[error("take-profit targets must be positive and total exactly 100%")]
    InvalidTakeProfit,
    #[error("stop-loss and take-profit prices are inconsistent with the trade side")]
    InvalidExitDirection,
    #[error("expiresAt must be in the future")]
    Expired,
    #[error("createdAt is over 30 seconds in the future or expiresAt is not after createdAt")]
    InvalidValidityWindow,
}

impl TradeIntent {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<(), IntentValidationError> {
        if self.schema_version != TRADE_INTENT_SCHEMA_VERSION {
            return Err(IntentValidationError::UnsupportedSchemaVersion);
        }
        if !valid_identifier(&self.strategy_id, 64) {
            return Err(IntentValidationError::InvalidStrategyId);
        }
        if self.strategy_version.trim().is_empty() {
            return Err(IntentValidationError::MissingStrategyVersion);
        }
        if self.strategy_version.len() > 64 {
            return Err(IntentValidationError::MissingStrategyVersion);
        }
        if !valid_identifier(&self.strategy_instance_id, 128) {
            return Err(IntentValidationError::InvalidStrategyInstanceId);
        }
        if !valid_identifier(&self.signal_id, 128) {
            return Err(IntentValidationError::InvalidSignalId);
        }
        if !valid_identifier(&self.account_id, 64) {
            return Err(IntentValidationError::InvalidAccountId);
        }
        if self.symbol.is_empty()
            || self.symbol.len() > 32
            || !self
                .symbol
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == ':' || c == '-')
        {
            return Err(IntentValidationError::InvalidSymbol);
        }
        if self.max_notional_usd <= Decimal::ZERO {
            return Err(IntentValidationError::InvalidNotional);
        }
        if self.reference_price <= Decimal::ZERO {
            return Err(IntentValidationError::InvalidReferencePrice);
        }
        if self.max_risk_usd <= Decimal::ZERO || self.max_risk_usd > self.max_notional_usd {
            return Err(IntentValidationError::InvalidRisk);
        }
        if !(1..=500).contains(&self.entry.max_slippage_bps) {
            return Err(IntentValidationError::InvalidSlippage);
        }
        if self.exit.stop_loss_price <= Decimal::ZERO {
            return Err(IntentValidationError::InvalidStopLoss);
        }
        if self.exit.take_profit.is_empty()
            || self
                .exit
                .take_profit
                .iter()
                .any(|target| target.price <= Decimal::ZERO)
            || self
                .exit
                .take_profit
                .iter()
                .map(|target| u16::from(target.position_pct))
                .sum::<u16>()
                != 100
        {
            return Err(IntentValidationError::InvalidTakeProfit);
        }
        let exit_direction_valid = match self.side {
            Side::Long => {
                self.exit.stop_loss_price < self.reference_price
                    && self
                        .exit
                        .take_profit
                        .iter()
                        .all(|target| target.price > self.reference_price)
            }
            Side::Short => {
                self.exit.stop_loss_price > self.reference_price
                    && self
                        .exit
                        .take_profit
                        .iter()
                        .all(|target| target.price < self.reference_price)
            }
        };
        if !exit_direction_valid {
            return Err(IntentValidationError::InvalidExitDirection);
        }
        if self.created_at > now + Duration::seconds(30) || self.expires_at <= self.created_at {
            return Err(IntentValidationError::InvalidValidityWindow);
        }
        if self.expires_at <= now {
            return Err(IntentValidationError::Expired);
        }
        Ok(())
    }
}

fn valid_identifier(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn valid_intent() -> TradeIntent {
        TradeIntent {
            schema_version: TRADE_INTENT_SCHEMA_VERSION,
            intent_id: Uuid::new_v4(),
            strategy_id: "rules".into(),
            strategy_version: "rules-v2".into(),
            strategy_instance_id: "rules-primary".into(),
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

    #[test]
    fn validates_a_complete_intent() {
        assert_eq!(valid_intent().validate(Utc::now()), Ok(()));
    }

    #[test]
    fn rejects_invalid_take_profit_allocation() {
        let mut intent = valid_intent();
        intent.exit.take_profit[0].position_pct = 50;
        assert_eq!(
            intent.validate(Utc::now()),
            Err(IntentValidationError::InvalidTakeProfit)
        );
    }

    #[test]
    fn rejects_a_long_stop_above_entry() {
        let mut intent = valid_intent();
        intent.exit.stop_loss_price = Decimal::new(101_000, 0);
        assert_eq!(
            intent.validate(Utc::now()),
            Err(IntentValidationError::InvalidExitDirection)
        );
    }

    #[test]
    fn rejects_unknown_contract_versions() {
        let mut intent = valid_intent();
        intent.schema_version = TRADE_INTENT_SCHEMA_VERSION + 1;
        assert_eq!(
            intent.validate(Utc::now()),
            Err(IntentValidationError::UnsupportedSchemaVersion)
        );
    }

    #[test]
    fn rejects_unscoped_or_unsafe_strategy_identifiers() {
        let mut intent = valid_intent();
        intent.strategy_id = "../../other-strategy".into();
        assert_eq!(
            intent.validate(Utc::now()),
            Err(IntentValidationError::InvalidStrategyId)
        );
    }

    #[test]
    fn rejects_invalid_validity_windows() {
        let mut intent = valid_intent();
        intent.created_at = Utc::now() + Duration::minutes(1);
        assert_eq!(
            intent.validate(Utc::now()),
            Err(IntentValidationError::InvalidValidityWindow)
        );
    }

    #[test]
    fn enforces_intent_state_transitions() {
        assert!(TradeIntentStatus::Received.can_transition_to(TradeIntentStatus::ShadowAccepted));
        assert!(!TradeIntentStatus::Received.can_transition_to(TradeIntentStatus::Completed));
        assert!(!TradeIntentStatus::Completed.can_transition_to(TradeIntentStatus::Submitting));
    }
}
