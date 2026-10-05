//! What hydration did in one family run. RPC activity is counted when it happens, so a block whose
//! publication later fails still reports its calls; row writes are counted only once their block
//! commits.

use serde_json::Value;

use super::super::store::Row;

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
    /// Selectors whose aggregate failed while the endpoint answered other calls at the block.
    pub deferred: u64,
    /// Selectors not observed: the endpoint did not serve the block, or the pass had no time or
    /// no call left for them.
    pub not_observed: u64,
    /// Committed rows whose observation changed: the hydrated value, or for a reverse tuple the
    /// block and selector it was observed at. A cleared obsolete value is one.
    pub value_writes: u64,
    /// Committed rows of which only the scheduling columns changed: the place in the queue, the
    /// aggregate size limit or the failure count.
    pub schedule_writes: u64,
}

/// Hydration in one family run: only a follow block that is the highest readable block hydrates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HydrationOutcome {
    /// Head blocks whose hydration reads were prepared, whether or not the block then published.
    pub passes: u64,
    /// Passes that stopped because the endpoint did not serve the head block. A pass ends as
    /// exactly one of timed out, unserved or neither.
    pub unserved_passes: u64,
    /// Passes that ran out of time before every selector was read, whatever else they learned.
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

impl Writes {
    /// Count one row's change: a value write when a column of `observation` changed, a schedule
    /// write when only its scheduling columns did.
    pub(crate) fn count(&mut self, before: &Row, after: &Row, observation: &[&str]) {
        // A column the row does not carry yet is a null one.
        let differs = |column: &str| {
            before.get(column).unwrap_or(&Value::Null) != after.get(column).unwrap_or(&Value::Null)
        };
        if observation.iter().any(|column| differs(column)) {
            self.values += 1;
        } else if before
            .keys()
            .chain(after.keys())
            .any(|column| differs(column))
        {
            self.schedules += 1;
        }
    }
}

impl HydrationOutcome {
    pub(crate) fn committed(&mut self, reverse: Writes, text: Writes) {
        self.reverse.value_writes += reverse.values;
        self.reverse.schedule_writes += reverse.schedules;
        self.text.value_writes += text.values;
        self.text.schedule_writes += text.schedules;
    }
}
