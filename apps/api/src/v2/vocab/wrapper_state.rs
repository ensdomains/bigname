use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WrapperState {
    Wrapped,
    Emancipated,
    Locked,
}

impl WrapperState {
    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        match value {
            "wrapped" => Some(Self::Wrapped),
            "emancipated" => Some(Self::Emancipated),
            "locked" => Some(Self::Locked),
            _ => None,
        }
    }
}
