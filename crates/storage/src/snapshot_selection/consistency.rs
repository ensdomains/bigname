use super::error::{SnapshotSelectionError, SnapshotSelectionResult};
use crate::lineage::CanonicalityState;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SnapshotConsistency {
    #[default]
    Head,
    Safe,
    Finalized,
}

impl SnapshotConsistency {
    pub fn parse(value: Option<&str>) -> SnapshotSelectionResult<Self> {
        match value.unwrap_or("head") {
            "head" => Ok(Self::Head),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            other => Err(SnapshotSelectionError::invalid_input(format!(
                "unsupported snapshot consistency {other}"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }

    pub(super) fn allows(self, state: CanonicalityState) -> bool {
        match self {
            Self::Head => matches!(
                state,
                CanonicalityState::Canonical
                    | CanonicalityState::Safe
                    | CanonicalityState::Finalized
            ),
            Self::Safe => matches!(
                state,
                CanonicalityState::Safe | CanonicalityState::Finalized
            ),
            Self::Finalized => state == CanonicalityState::Finalized,
        }
    }
}
