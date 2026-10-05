//! What hydration did in one family run. RPC activity is counted when it happens, so a block whose
//! publication later fails still reports its calls; row writes are counted only once their block
//! commits.

/// One kind of hydration (reverse names or text records) in one family run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HydrationKindOutcome {
    /// Multicall3 aggregates sent, the halves of a split aggregate included.
    pub rpc_calls: u64,
    /// Aggregates that failed as a whole: a transport or JSON-RPC error, a timeout or an answer
    /// that could not be decoded.
    pub rpc_failures: u64,
    /// Selectors whose aggregate was answered, a selector whose own call failed included.
    pub answered: u64,
    /// Answered selectors whose own call failed inside the aggregate.
    pub failed_calls: u64,
    /// Selectors whose aggregate failed while the endpoint served the block.
    pub deferred: u64,
    /// Selectors not observed: the endpoint did not serve the block, or the pass ran out of time.
    pub not_observed: u64,
    /// Committed rows whose hydrated value or the block it was observed at changed, a cleared
    /// obsolete value included.
    pub value_writes: u64,
    /// Committed rows moved behind the rest of their queue with nothing observed for them.
    pub schedule_writes: u64,
}

/// Hydration in one family run: only a follow block that is the highest readable block hydrates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HydrationOutcome {
    /// Head blocks whose hydration reads were prepared, whether or not the block then published.
    pub passes: u64,
    /// Passes in which the endpoint did not answer an aggregate at the head block.
    pub unserved_passes: u64,
    /// Passes that ran out of time before every selector was read.
    pub timed_out_passes: u64,
    /// One-call aggregates sent to learn whether the endpoint serves the block.
    pub probes: u64,
    pub probe_failures: u64,
    pub reverse: HydrationKindOutcome,
    pub text: HydrationKindOutcome,
    /// Wall time of the passes' RPC reads.
    pub rpc_ms: u64,
    /// Seconds between the newest hydrated head block's timestamp and its pass.
    pub head_age_seconds: Option<u64>,
}

/// Row writes of one block's hydration, counted into the run once the block commits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Writes {
    pub(crate) values: u64,
    pub(crate) schedules: u64,
}

impl HydrationOutcome {
    pub(crate) fn committed(&mut self, reverse: Writes, text: Writes) {
        self.reverse.value_writes += reverse.values;
        self.reverse.schedule_writes += reverse.schedules;
        self.text.value_writes += text.values;
        self.text.schedule_writes += text.schedules;
    }
}
