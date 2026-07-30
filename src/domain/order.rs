use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    PendingSubmit,
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    CancelPending,
    Canceled,
    Rejected,
    Expired,
    ReconciliationRequired,
}

impl OrderStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        use OrderStatus::*;
        matches!(
            (self, next),
            (PendingSubmit, Submitted | Rejected)
                | (
                    Submitted,
                    Accepted | PartiallyFilled | Filled | Rejected | ReconciliationRequired
                )
                | (
                    Accepted,
                    PartiallyFilled
                        | Filled
                        | CancelPending
                        | Canceled
                        | Expired
                        | ReconciliationRequired
                )
                | (
                    PartiallyFilled,
                    Filled | CancelPending | Canceled | Expired | ReconciliationRequired
                )
                | (
                    CancelPending,
                    Canceled | PartiallyFilled | Filled | ReconciliationRequired
                )
                | (
                    ReconciliationRequired,
                    Accepted | PartiallyFilled | Filled | Canceled | Rejected | Expired
                )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permits_partial_fill_then_fill() {
        assert!(OrderStatus::Accepted.can_transition_to(OrderStatus::PartiallyFilled));
        assert!(OrderStatus::PartiallyFilled.can_transition_to(OrderStatus::Filled));
        assert!(!OrderStatus::Filled.can_transition_to(OrderStatus::Submitted));
    }
}
