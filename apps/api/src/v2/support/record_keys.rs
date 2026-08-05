#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolutionMode {
    Declared,
    Verified,
    Both,
}

impl ResolutionMode {
    pub(crate) fn includes_verified(self) -> bool {
        matches!(self, Self::Verified | Self::Both)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolutionRecordKey {
    pub(crate) record_key: String,
    pub(crate) record_family: String,
    pub(crate) selector_key: Option<String>,
}
