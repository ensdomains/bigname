/// Anchor selection for normalized-event history reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryScope {
    Surface,
    Resource,
    Both,
}

impl HistoryScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Resource => "resource",
            Self::Both => "both",
        }
    }
}

/// Keyset direction over the shared chain-position sort. `Asc` is the exact
/// reverse of `Desc`, so both directions page over the same total order.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HistoryOrder {
    #[default]
    Desc,
    Asc,
}

impl HistoryOrder {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Desc => "desc",
            Self::Asc => "asc",
        }
    }
}

/// Inclusive block-number bounds for one chain, resolved from lineage timestamps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainBlockRange {
    pub chain_id: String,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
}

/// Per-chain block windows applied as one disjunction; an empty window matches
/// no row, which is what a timestamp range after the last known block means.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HistoryBlockWindow {
    pub ranges: Vec<ChainBlockRange>,
}

/// Read-side options shared by the anchored history page loaders.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HistoryPageOptions {
    pub order: HistoryOrder,
    pub event_kinds: Vec<String>,
    pub bind_cursor_anchor_to_event_kinds: bool,
    pub block_window: Option<HistoryBlockWindow>,
    /// Publication upper bounds for expanding bindings and historical ownership anchors.
    pub publication_block_bounds: Option<std::collections::BTreeMap<String, i64>>,
}
