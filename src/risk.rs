use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::TradeIntent;

pub const DEFAULT_RISK_POLICY_VERSION: &str = "shadow-tiny-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RiskLimits {
    #[serde(with = "rust_decimal::serde::str")]
    pub min_order_notional_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_risk_usd: Decimal,
    pub max_slippage_bps: u32,
    pub max_stop_distance_bps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RiskPolicy {
    pub version: String,
    pub allowed_accounts: BTreeSet<String>,
    pub allow_unregistered_strategies: bool,
    pub default_limits: RiskLimits,
    pub portfolio_limits: PortfolioRiskLimits,
    pub market_limits: MarketRiskLimits,
    #[serde(default)]
    pub strategy_limits: BTreeMap<String, RiskLimits>,
}

impl RiskPolicy {
    pub fn shadow_tiny_default() -> Self {
        Self {
            version: DEFAULT_RISK_POLICY_VERSION.into(),
            allowed_accounts: BTreeSet::from(["main".into()]),
            allow_unregistered_strategies: true,
            default_limits: RiskLimits {
                min_order_notional_usd: Decimal::new(5, 0),
                max_order_notional_usd: Decimal::new(25, 0),
                max_order_risk_usd: Decimal::new(1, 0),
                max_slippage_bps: 50,
                max_stop_distance_bps: 1_500,
            },
            portfolio_limits: PortfolioRiskLimits {
                max_total_exposure_usd: Decimal::new(100, 0),
                max_symbol_exposure_usd: Decimal::new(25, 0),
                max_strategy_exposure_usd: Decimal::new(25, 0),
                max_open_positions: 1,
                max_daily_loss_usd: Decimal::new(2, 0),
                max_consecutive_losses: 2,
                max_effective_leverage: Decimal::ONE,
                min_liquidation_distance_bps: 2_000,
            },
            market_limits: MarketRiskLimits {
                max_market_age_ms: 2_000,
                max_spread_bps: 50,
                max_reference_deviation_bps: 100,
                min_top_of_book_depth_usd: Decimal::new(1_000, 0),
            },
            strategy_limits: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<(), RiskPolicyError> {
        if self.version.trim().is_empty() {
            return Err(RiskPolicyError::MissingVersion);
        }
        if self.allowed_accounts.is_empty() {
            return Err(RiskPolicyError::NoAllowedAccounts);
        }
        validate_limits(&self.default_limits)?;
        self.portfolio_limits.validate()?;
        self.market_limits.validate()?;
        for limits in self.strategy_limits.values() {
            validate_limits(limits)?;
        }
        Ok(())
    }

    fn limits_for(&self, strategy_id: &str) -> Option<&RiskLimits> {
        self.strategy_limits.get(strategy_id).or_else(|| {
            self.allow_unregistered_strategies
                .then_some(&self.default_limits)
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PortfolioRiskLimits {
    #[serde(with = "rust_decimal::serde::str")]
    pub max_total_exposure_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_symbol_exposure_usd: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_strategy_exposure_usd: Decimal,
    pub max_open_positions: u32,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_daily_loss_usd: Decimal,
    pub max_consecutive_losses: u32,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_effective_leverage: Decimal,
    pub min_liquidation_distance_bps: u32,
}

impl PortfolioRiskLimits {
    fn validate(&self) -> Result<(), RiskPolicyError> {
        if self.max_total_exposure_usd <= Decimal::ZERO
            || self.max_symbol_exposure_usd <= Decimal::ZERO
            || self.max_strategy_exposure_usd <= Decimal::ZERO
            || self.max_open_positions == 0
            || self.max_daily_loss_usd <= Decimal::ZERO
            || self.max_consecutive_losses == 0
            || self.max_effective_leverage <= Decimal::ZERO
            || self.min_liquidation_distance_bps == 0
        {
            return Err(RiskPolicyError::InvalidPortfolioLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MarketRiskLimits {
    pub max_market_age_ms: i64,
    pub max_spread_bps: u32,
    pub max_reference_deviation_bps: u32,
    #[serde(with = "rust_decimal::serde::str")]
    pub min_top_of_book_depth_usd: Decimal,
}

impl MarketRiskLimits {
    fn validate(&self) -> Result<(), RiskPolicyError> {
        if self.max_market_age_ms <= 0
            || self.max_spread_bps == 0
            || self.max_reference_deviation_bps == 0
            || self.min_top_of_book_depth_usd <= Decimal::ZERO
        {
            return Err(RiskPolicyError::InvalidMarketLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RiskPolicyError {
    #[error("risk policy version is required")]
    MissingVersion,
    #[error("risk policy must allow at least one account")]
    NoAllowedAccounts,
    #[error("risk limits must be positive and min notional must not exceed max notional")]
    InvalidMonetaryLimits,
    #[error("risk limit basis points must be positive")]
    InvalidBasisPointLimits,
    #[error("portfolio risk limits must be positive")]
    InvalidPortfolioLimits,
    #[error("market risk limits must be positive")]
    InvalidMarketLimits,
}

#[derive(Debug, Error)]
pub enum RiskPolicyLoadError {
    #[error("failed to read risk policy: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse risk policy JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Invalid(#[from] RiskPolicyError),
}

pub fn load_risk_policy(path: &str) -> Result<RiskPolicy, RiskPolicyLoadError> {
    let policy: RiskPolicy = serde_json::from_slice(&std::fs::read(path)?)?;
    policy.validate()?;
    Ok(policy)
}

fn validate_limits(limits: &RiskLimits) -> Result<(), RiskPolicyError> {
    if limits.min_order_notional_usd <= Decimal::ZERO
        || limits.max_order_notional_usd <= Decimal::ZERO
        || limits.max_order_risk_usd <= Decimal::ZERO
        || limits.min_order_notional_usd > limits.max_order_notional_usd
    {
        return Err(RiskPolicyError::InvalidMonetaryLimits);
    }
    if limits.max_slippage_bps == 0 || limits.max_stop_distance_bps == 0 {
        return Err(RiskPolicyError::InvalidBasisPointLimits);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskDecisionStatus {
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskReasonCode {
    AccountNotAllowed,
    StrategyNotRegistered,
    SlippageLimitExceeded,
    StopDistanceExceeded,
    BelowMinimumNotional,
    AccountSnapshotMismatch,
    InvalidAccountEquity,
    DailyLossLimitExceeded,
    ConsecutiveLossLimitExceeded,
    MaximumPositionsExceeded,
    TotalExposureExceeded,
    SymbolExposureExceeded,
    StrategyExposureExceeded,
    EffectiveLeverageExceeded,
    LiquidationDistanceTooSmall,
    MarketDataStale,
    InvalidMarketQuote,
    SpreadTooWide,
    ReferencePriceDeviationExceeded,
    InsufficientMarketDepth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RiskDecision {
    pub status: RiskDecisionStatus,
    pub policy_version: String,
    pub reason_codes: Vec<RiskReasonCode>,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub approved_notional_usd: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub estimated_stop_risk_usd: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub stop_distance_bps: Option<Decimal>,
}

impl RiskDecision {
    pub fn is_approved(&self) -> bool {
        self.status == RiskDecisionStatus::Approved
    }
}

#[derive(Debug, Clone)]
pub struct RiskEngine {
    policy: RiskPolicy,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountRiskSnapshot {
    pub account_id: String,
    pub equity_usd: Decimal,
    pub total_exposure_usd: Decimal,
    pub symbol_exposure_usd: Decimal,
    pub strategy_exposure_usd: Decimal,
    pub open_positions: u32,
    pub consecutive_losses: u32,
    pub daily_realized_pnl_usd: Decimal,
    pub nearest_liquidation_distance_bps: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketRiskSnapshot {
    pub observed_at: DateTime<Utc>,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub top_of_book_depth_usd: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreTradeRiskContext {
    pub account: AccountRiskSnapshot,
    pub market: MarketRiskSnapshot,
}

impl RiskEngine {
    pub fn new(policy: RiskPolicy) -> Result<Self, RiskPolicyError> {
        policy.validate()?;
        Ok(Self { policy })
    }

    pub fn policy(&self) -> &RiskPolicy {
        &self.policy
    }

    pub fn evaluate_admission(&self, intent: &TradeIntent) -> RiskDecision {
        if !self.policy.allowed_accounts.contains(&intent.account_id) {
            return self.rejected(RiskReasonCode::AccountNotAllowed, None);
        }
        let Some(limits) = self.policy.limits_for(&intent.strategy_id) else {
            return self.rejected(RiskReasonCode::StrategyNotRegistered, None);
        };
        if intent.entry.max_slippage_bps > limits.max_slippage_bps {
            return self.rejected(RiskReasonCode::SlippageLimitExceeded, None);
        }

        let stop_distance = (intent.reference_price - intent.exit.stop_loss_price).abs();
        let stop_fraction = stop_distance / intent.reference_price;
        let stop_distance_bps = stop_fraction * Decimal::new(10_000, 0);
        if stop_distance_bps > Decimal::from(limits.max_stop_distance_bps) {
            return self.rejected(
                RiskReasonCode::StopDistanceExceeded,
                Some(stop_distance_bps),
            );
        }

        let risk_cap = intent.max_risk_usd.min(limits.max_order_risk_usd);
        let risk_sized_notional = risk_cap / stop_fraction;
        let approved_notional = intent
            .max_notional_usd
            .min(limits.max_order_notional_usd)
            .min(risk_sized_notional);
        if approved_notional < limits.min_order_notional_usd {
            return self.rejected(
                RiskReasonCode::BelowMinimumNotional,
                Some(stop_distance_bps),
            );
        }
        RiskDecision {
            status: RiskDecisionStatus::Approved,
            policy_version: self.policy.version.clone(),
            reason_codes: Vec::new(),
            approved_notional_usd: Some(approved_notional),
            estimated_stop_risk_usd: Some(approved_notional * stop_fraction),
            stop_distance_bps: Some(stop_distance_bps),
        }
    }

    pub fn evaluate_pre_trade(
        &self,
        intent: &TradeIntent,
        admission: &RiskDecision,
        context: &PreTradeRiskContext,
        now: DateTime<Utc>,
    ) -> RiskDecision {
        if !admission.is_approved() {
            return admission.clone();
        }
        let approved_notional = admission
            .approved_notional_usd
            .expect("approved admission has an approved notional");
        let portfolio = &self.policy.portfolio_limits;
        let market = &self.policy.market_limits;

        let reason = if context.account.account_id != intent.account_id {
            Some(RiskReasonCode::AccountSnapshotMismatch)
        } else if context.account.equity_usd <= Decimal::ZERO {
            Some(RiskReasonCode::InvalidAccountEquity)
        } else if context.account.daily_realized_pnl_usd <= -portfolio.max_daily_loss_usd {
            Some(RiskReasonCode::DailyLossLimitExceeded)
        } else if context.account.consecutive_losses >= portfolio.max_consecutive_losses {
            Some(RiskReasonCode::ConsecutiveLossLimitExceeded)
        } else if context.account.symbol_exposure_usd == Decimal::ZERO
            && context.account.open_positions >= portfolio.max_open_positions
        {
            Some(RiskReasonCode::MaximumPositionsExceeded)
        } else if context.account.total_exposure_usd + approved_notional
            > portfolio.max_total_exposure_usd
        {
            Some(RiskReasonCode::TotalExposureExceeded)
        } else if context.account.symbol_exposure_usd + approved_notional
            > portfolio.max_symbol_exposure_usd
        {
            Some(RiskReasonCode::SymbolExposureExceeded)
        } else if context.account.strategy_exposure_usd + approved_notional
            > portfolio.max_strategy_exposure_usd
        {
            Some(RiskReasonCode::StrategyExposureExceeded)
        } else if (context.account.total_exposure_usd + approved_notional)
            / context.account.equity_usd
            > portfolio.max_effective_leverage
        {
            Some(RiskReasonCode::EffectiveLeverageExceeded)
        } else if context
            .account
            .nearest_liquidation_distance_bps
            .is_some_and(|distance| {
                distance < Decimal::from(portfolio.min_liquidation_distance_bps)
            })
        {
            Some(RiskReasonCode::LiquidationDistanceTooSmall)
        } else if now
            .signed_duration_since(context.market.observed_at)
            .num_milliseconds()
            > market.max_market_age_ms
        {
            Some(RiskReasonCode::MarketDataStale)
        } else if context.market.best_bid <= Decimal::ZERO
            || context.market.best_ask <= context.market.best_bid
        {
            Some(RiskReasonCode::InvalidMarketQuote)
        } else {
            let mid = (context.market.best_bid + context.market.best_ask) / Decimal::TWO;
            let spread_bps =
                (context.market.best_ask - context.market.best_bid) / mid * Decimal::new(10_000, 0);
            let reference_deviation_bps = (mid - intent.reference_price).abs()
                / intent.reference_price
                * Decimal::new(10_000, 0);
            if spread_bps > Decimal::from(market.max_spread_bps) {
                Some(RiskReasonCode::SpreadTooWide)
            } else if reference_deviation_bps > Decimal::from(market.max_reference_deviation_bps) {
                Some(RiskReasonCode::ReferencePriceDeviationExceeded)
            } else if context.market.top_of_book_depth_usd
                < market.min_top_of_book_depth_usd.max(approved_notional)
            {
                Some(RiskReasonCode::InsufficientMarketDepth)
            } else {
                None
            }
        };
        reason.map_or_else(
            || admission.clone(),
            |reason| {
                let mut rejected = admission.clone();
                rejected.status = RiskDecisionStatus::Rejected;
                rejected.reason_codes = vec![reason];
                rejected
            },
        )
    }

    fn rejected(&self, reason: RiskReasonCode, stop_distance_bps: Option<Decimal>) -> RiskDecision {
        RiskDecision {
            status: RiskDecisionStatus::Rejected,
            policy_version: self.policy.version.clone(),
            reason_codes: vec![reason],
            approved_notional_usd: None,
            estimated_stop_risk_usd: None,
            stop_distance_bps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        EntryPolicy, EntryType, ExitPolicy, Side, TRADE_INTENT_SCHEMA_VERSION, TakeProfitTarget,
    };
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    fn intent() -> TradeIntent {
        TradeIntent {
            schema_version: TRADE_INTENT_SCHEMA_VERSION,
            intent_id: Uuid::new_v4(),
            strategy_id: "test".into(),
            strategy_version: "v1".into(),
            strategy_instance_id: "primary".into(),
            signal_id: Uuid::new_v4().to_string(),
            account_id: "main".into(),
            symbol: "BTC".into(),
            side: Side::Long,
            reference_price: Decimal::new(100, 0),
            max_notional_usd: Decimal::new(100, 0),
            max_risk_usd: Decimal::new(10, 0),
            entry: EntryPolicy {
                kind: EntryType::MarketIoc,
                max_slippage_bps: 20,
            },
            exit: ExitPolicy {
                stop_loss_price: Decimal::new(95, 0),
                take_profit: vec![TakeProfitTarget {
                    price: Decimal::new(110, 0),
                    position_pct: 100,
                }],
            },
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::minutes(5),
        }
    }

    fn context(now: DateTime<Utc>) -> PreTradeRiskContext {
        PreTradeRiskContext {
            account: AccountRiskSnapshot {
                account_id: "main".into(),
                equity_usd: Decimal::new(1_000, 0),
                total_exposure_usd: Decimal::ZERO,
                symbol_exposure_usd: Decimal::ZERO,
                strategy_exposure_usd: Decimal::ZERO,
                open_positions: 0,
                consecutive_losses: 0,
                daily_realized_pnl_usd: Decimal::ZERO,
                nearest_liquidation_distance_bps: None,
            },
            market: MarketRiskSnapshot {
                observed_at: now,
                best_bid: Decimal::new(9_995, 2),
                best_ask: Decimal::new(10_005, 2),
                top_of_book_depth_usd: Decimal::new(10_000, 0),
            },
        }
    }

    #[test]
    fn sizes_notional_from_the_server_risk_cap_and_stop_distance() {
        let engine = RiskEngine::new(RiskPolicy::shadow_tiny_default()).unwrap();
        let decision = engine.evaluate_admission(&intent());
        assert!(decision.is_approved());
        assert_eq!(decision.approved_notional_usd, Some(Decimal::new(20, 0)));
        assert_eq!(decision.estimated_stop_risk_usd, Some(Decimal::ONE));
    }

    #[test]
    fn rejects_an_unapproved_account() {
        let engine = RiskEngine::new(RiskPolicy::shadow_tiny_default()).unwrap();
        let mut intent = intent();
        intent.account_id = "unknown".into();
        let decision = engine.evaluate_admission(&intent);
        assert_eq!(
            decision.reason_codes,
            vec![RiskReasonCode::AccountNotAllowed]
        );
    }

    #[test]
    fn requires_registered_strategies_when_the_policy_is_closed() {
        let mut policy = RiskPolicy::shadow_tiny_default();
        policy.allow_unregistered_strategies = false;
        let engine = RiskEngine::new(policy).unwrap();
        let decision = engine.evaluate_admission(&intent());
        assert_eq!(
            decision.reason_codes,
            vec![RiskReasonCode::StrategyNotRegistered]
        );
    }

    #[test]
    fn approves_a_healthy_pre_trade_snapshot() {
        let engine = RiskEngine::new(RiskPolicy::shadow_tiny_default()).unwrap();
        let intent = intent();
        let admission = engine.evaluate_admission(&intent);
        let now = Utc::now();
        let decision = engine.evaluate_pre_trade(&intent, &admission, &context(now), now);
        assert!(decision.is_approved());
    }

    #[test]
    fn rejects_when_the_daily_loss_limit_is_reached() {
        let engine = RiskEngine::new(RiskPolicy::shadow_tiny_default()).unwrap();
        let intent = intent();
        let admission = engine.evaluate_admission(&intent);
        let now = Utc::now();
        let mut context = context(now);
        context.account.daily_realized_pnl_usd = Decimal::new(-2, 0);
        let decision = engine.evaluate_pre_trade(&intent, &admission, &context, now);
        assert_eq!(
            decision.reason_codes,
            vec![RiskReasonCode::DailyLossLimitExceeded]
        );
    }

    #[test]
    fn rejects_stale_market_data() {
        let engine = RiskEngine::new(RiskPolicy::shadow_tiny_default()).unwrap();
        let intent = intent();
        let admission = engine.evaluate_admission(&intent);
        let now = Utc::now();
        let mut context = context(now);
        context.market.observed_at = now - Duration::seconds(3);
        let decision = engine.evaluate_pre_trade(&intent, &admission, &context, now);
        assert_eq!(decision.reason_codes, vec![RiskReasonCode::MarketDataStale]);
    }

    #[test]
    fn rejects_symbol_exposure_above_the_hard_limit() {
        let engine = RiskEngine::new(RiskPolicy::shadow_tiny_default()).unwrap();
        let intent = intent();
        let admission = engine.evaluate_admission(&intent);
        let now = Utc::now();
        let mut context = context(now);
        context.account.symbol_exposure_usd = Decimal::new(10, 0);
        let decision = engine.evaluate_pre_trade(&intent, &admission, &context, now);
        assert_eq!(
            decision.reason_codes,
            vec![RiskReasonCode::SymbolExposureExceeded]
        );
    }

    #[test]
    fn example_policy_is_strict_and_valid() {
        let policy: RiskPolicy =
            serde_json::from_str(include_str!("../risk-policy.example.json")).unwrap();
        policy.validate().unwrap();
        assert!(!policy.allow_unregistered_strategies);
        assert!(policy.strategy_limits.contains_key("rules"));
    }
}
