use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PositionStatus {
    Opening,
    OpenUnprotected,
    OpenProtected,
    Reducing,
    Closing,
    Closed,
    ReconciliationRequired,
}

impl PositionStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        use PositionStatus::*;
        matches!(
            (self, next),
            (Opening, OpenUnprotected | Closed | ReconciliationRequired)
                | (
                    OpenUnprotected,
                    OpenProtected | Closing | ReconciliationRequired
                )
                | (OpenProtected, Reducing | Closing | ReconciliationRequired)
                | (
                    Reducing,
                    OpenProtected | Closing | Closed | ReconciliationRequired
                )
                | (Closing, Closed | ReconciliationRequired)
                | (
                    ReconciliationRequired,
                    OpenUnprotected | OpenProtected | Closing | Closed
                )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_protection_before_normal_open_state() {
        assert!(PositionStatus::Opening.can_transition_to(PositionStatus::OpenUnprotected));
        assert!(PositionStatus::OpenUnprotected.can_transition_to(PositionStatus::OpenProtected));
        assert!(!PositionStatus::Opening.can_transition_to(PositionStatus::OpenProtected));
    }
}
