use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WrapperFuses {
    pub(crate) fuses: u32,
    pub(crate) cannot_unwrap: bool,
    pub(crate) cannot_burn_fuses: bool,
    pub(crate) cannot_transfer: bool,
    pub(crate) cannot_set_resolver: bool,
    pub(crate) cannot_set_ttl: bool,
    pub(crate) cannot_create_subdomain: bool,
    pub(crate) cannot_approve: bool,
    pub(crate) parent_cannot_control: bool,
    pub(crate) is_dot_eth: bool,
    pub(crate) can_extend_expiry: bool,
}

impl WrapperFuses {
    pub(crate) fn from_summary(summary: &Value) -> Option<Self> {
        let value = summary.get("wrapper_fuses")?;
        let fuses = serde_json::from_value::<Self>(value.clone()).ok()?;
        fuses.is_consistent().then_some(fuses)
    }

    fn is_consistent(self) -> bool {
        self.cannot_unwrap == self.has(1)
            && self.cannot_burn_fuses == self.has(2)
            && self.cannot_transfer == self.has(4)
            && self.cannot_set_resolver == self.has(8)
            && self.cannot_set_ttl == self.has(16)
            && self.cannot_create_subdomain == self.has(32)
            && self.cannot_approve == self.has(64)
            && self.parent_cannot_control == self.has(1 << 16)
            && self.is_dot_eth == self.has(1 << 17)
            && self.can_extend_expiry == self.has(1 << 18)
    }

    const fn has(self, fuse: u32) -> bool {
        self.fuses & fuse != 0
    }
}
