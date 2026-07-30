use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{domain::TradeIntent, risk::RiskDecision};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManualConfirmation {
    pub digest: String,
    pub expires_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub confirmed_by: Option<String>,
}

pub fn confirmation_digest(
    intent: &TradeIntent,
    risk_decision: &RiskDecision,
    expires_at: DateTime<Utc>,
) -> Result<String, serde_json::Error> {
    let canonical = serde_json::to_vec(&(intent, risk_decision, expires_at))?;
    Ok(format!("{:x}", Sha256::digest(canonical)))
}
