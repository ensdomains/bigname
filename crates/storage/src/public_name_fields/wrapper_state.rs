use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WrapperState {
    Wrapped,
    Emancipated,
    Locked,
    Lapsed,
    Unwrapped,
    Unknown,
}

impl WrapperState {
    pub fn is_backed(self) -> bool {
        matches!(self, Self::Wrapped | Self::Emancipated | Self::Locked)
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "wrapped" => Some(Self::Wrapped),
            "emancipated" => Some(Self::Emancipated),
            "locked" => Some(Self::Locked),
            "lapsed" => Some(Self::Lapsed),
            "unwrapped" => Some(Self::Unwrapped),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}
