//! One head block's hydration reads, shared by the reverse and text kinds. Selectors go out as
//! Multicall3 aggregates of at most [`BATCH_LIMIT`] calls, and every selector ends as one [`Read`].
//!
//! An aggregate the endpoint answers gives each selector its own result, a failed call included.
//! An aggregate that fails as a whole says nothing about any selector in it, so it never becomes
//! a per-selector failure. What happens next depends on whether the endpoint serves the block:
//!
//! - It does not (a one-call aggregate at the same block hash fails too, or an earlier one in
//!   this pass did): every selector not yet answered is [`Read::Unobserved`] and no further call
//!   is made for this block, by either kind.
//! - It does: the failure comes from what the aggregate holds (its encoded size, execution cost
//!   or response size, or one selector). The aggregate is split in halves and each half sent
//!   again, at most [`ISOLATION_CALLS`] extra calls per kind and block, so the selectors that
//!   can be answered are. Those still unanswered when the calls are spent, or alone in an
//!   aggregate that fails, are [`Read::Deferred`].
//!
//! Every call and the whole pass are bounded by [`HydrationTimeLimits`]; a selector the pass has
//! no time left for is [`Read::Unobserved`].
use std::{
    ops::Range,
    time::{Duration, Instant},
};

use bigname_lookup::{
    ChainRpcUrls, EnsReverseNameMulticallBlock, EnsReverseNameMulticallRequest, MULTICALL3_ADDRESS,
    execute_ens_reverse_name_multicall,
};

use super::{
    super::input::BlockHeader,
    outcome::{HydrationKindOutcome, HydrationOutcome},
};

/// The most calls one Multicall3 aggregate carries.
pub(super) const BATCH_LIMIT: usize = 250;

/// How long hydration may wait on RPC. The publication never waits longer: reads that do not fit
/// are not observed and the block is published without them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HydrationTimeLimits {
    /// The longest one RPC call may take before it counts as failed.
    pub call: Duration,
    /// The longest one block's hydration reads may take in all, both kinds together.
    pub block: Duration,
}

impl Default for HydrationTimeLimits {
    fn default() -> Self {
        Self {
            call: Duration::from_secs(10),
            block: Duration::from_secs(30),
        }
    }
}

/// Extra calls one kind may spend per block splitting failed aggregates: two per split, so
/// sixteen follow one failing selector down from 250 calls to its own.
const ISOLATION_CALLS: u32 = 16;

/// What one hydration pass learned about one selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Read<T> {
    /// The endpoint answered the selector's aggregate; this is the selector's own result, which
    /// may itself be a failed call.
    Answered(T),
    /// The selector's aggregate failed while the endpoint served the block. Nothing was observed:
    /// the stored value and the block it was observed at stay, and the selector moves behind the
    /// rest of its queue.
    Deferred,
    /// Not observed because the RPC batch failed and the endpoint did not serve the block, or the
    /// pass had no time left. The selector's row is left exactly as it is.
    Unobserved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Kind {
    Reverse,
    Text,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Self::Reverse => "reverse",
            Self::Text => "text",
        }
    }

    pub(super) fn of(self, outcome: &mut HydrationOutcome) -> &mut HydrationKindOutcome {
        match self {
            Self::Reverse => &mut outcome.reverse,
            Self::Text => &mut outcome.text,
        }
    }
}

/// One kind's way of sending an aggregate: one answer per request, or why the whole aggregate
/// failed.
pub(super) trait Aggregate: Sync {
    type Request: Sync;
    type Answer: Send;

    fn send(
        &self,
        chunk: &[Self::Request],
    ) -> impl Future<Output = std::result::Result<Vec<Self::Answer>, String>> + Send;
}

/// The reads of one head block.
pub(super) struct Session<'a> {
    pub(super) chain_id: &'a str,
    pub(super) rpc_urls: &'a ChainRpcUrls,
    pub(super) head: &'a BlockHeader,
    pub(super) stats: &'a mut HydrationOutcome,
    limits: HydrationTimeLimits,
    started: Instant,
    /// Whether the endpoint answered an aggregate at this block; `None` until one was tried.
    serves: Option<bool>,
    out_of_time: bool,
}

impl<'a> Session<'a> {
    pub(super) fn new(
        chain_id: &'a str,
        rpc_urls: &'a ChainRpcUrls,
        head: &'a BlockHeader,
        limits: HydrationTimeLimits,
        stats: &'a mut HydrationOutcome,
    ) -> Self {
        Self {
            chain_id,
            rpc_urls,
            head,
            stats,
            limits,
            started: Instant::now(),
            serves: None,
            out_of_time: false,
        }
    }

    /// Count the pass's time and how it ended into the run's outcome.
    pub(super) fn finish(self) {
        self.stats.rpc_ms += u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.stats.unserved_passes += u64::from(self.serves == Some(false));
        self.stats.timed_out_passes += u64::from(self.out_of_time);
    }

    /// Read `requests` through `call`, which sends one aggregate. The reads come back in request
    /// order.
    pub(super) async fn read<A: Aggregate>(
        &mut self,
        kind: Kind,
        requests: &[A::Request],
        call: &A,
    ) -> Vec<Read<A::Answer>> {
        let mut reads: Vec<Option<Read<A::Answer>>> = requests.iter().map(|_| None).collect();
        let mut spare = ISOLATION_CALLS;
        let mut pending: Vec<Range<usize>> = (0..requests.len())
            .step_by(BATCH_LIMIT)
            .map(|start| start..(start + BATCH_LIMIT).min(requests.len()))
            .rev()
            .collect();
        while let Some(range) = pending.pop() {
            match self.aggregate(kind, &requests[range.clone()], call).await {
                Some(Ok(found)) => {
                    for (slot, value) in reads[range].iter_mut().zip(found) {
                        *slot = Some(Read::Answered(value));
                    }
                }
                None => reads[range].fill_with(|| Some(Read::Unobserved)),
                Some(Err(())) => {
                    if self.serves.is_none() {
                        self.probe().await;
                    }
                    if self.serves != Some(true) {
                        reads[range].fill_with(|| Some(Read::Unobserved));
                    } else if range.len() > 1 && spare >= 2 {
                        spare -= 2;
                        let middle = range.start + range.len() / 2;
                        pending.push(middle..range.end);
                        pending.push(range.start..middle);
                    } else {
                        reads[range].fill_with(|| Some(Read::Deferred));
                    }
                }
            }
        }
        let reads: Vec<Read<A::Answer>> = reads
            .into_iter()
            .map(|read| read.expect("every range of requests was resolved"))
            .collect();
        let stats = kind.of(self.stats);
        for read in &reads {
            match read {
                Read::Answered(_) => stats.answered += 1,
                Read::Deferred => stats.deferred += 1,
                Read::Unobserved => stats.not_observed += 1,
            }
        }
        reads
    }

    /// Send one aggregate. `None` when it was not sent: the endpoint does not serve the block, or
    /// the pass has no time left.
    async fn aggregate<A: Aggregate>(
        &mut self,
        kind: Kind,
        chunk: &[A::Request],
        call: &A,
    ) -> Option<std::result::Result<Vec<A::Answer>, ()>> {
        if self.serves == Some(false) {
            return None;
        }
        let limit = self.time_left()?;
        kind.of(self.stats).rpc_calls += 1;
        let error = match tokio::time::timeout(limit, call.send(chunk)).await {
            Ok(Ok(found)) if found.len() == chunk.len() => {
                self.serves = Some(true);
                return Some(Ok(found));
            }
            Ok(Ok(found)) => format!("{} results for {} calls", found.len(), chunk.len()),
            Ok(Err(error)) => error,
            Err(_) => format!("no answer within {} ms", limit.as_millis()),
        };
        kind.of(self.stats).rpc_failures += 1;
        tracing::warn!(
            target: "bigname_project::families",
            chain_id = self.chain_id,
            block_number = self.head.number,
            block_hash = %self.head.hash,
            kind = kind.label(),
            selectors = chunk.len(),
            error = %error,
            "a hydration RPC batch failed; none of its selectors was observed by it"
        );
        Some(Err(()))
    }

    /// Ask the endpoint for a one-call aggregate at the block. The call's own result is ignored:
    /// only whether the endpoint answered an aggregate at this block hash counts.
    async fn probe(&mut self) {
        let block = EnsReverseNameMulticallBlock {
            block_number: self.head.number,
            block_hash: self.head.hash.clone(),
        };
        let request = [EnsReverseNameMulticallRequest {
            resolver_address: MULTICALL3_ADDRESS.to_owned(),
            reverse_node: format!("0x{}", "0".repeat(64)),
        }];
        let served = match self.time_left() {
            Some(limit) => {
                self.stats.probes += 1;
                let answer = tokio::time::timeout(
                    limit,
                    execute_ens_reverse_name_multicall(
                        self.rpc_urls,
                        self.chain_id,
                        MULTICALL3_ADDRESS,
                        &block,
                        &request,
                    ),
                )
                .await;
                let served = matches!(answer, Ok(Ok(_)));
                if !served {
                    self.stats.probe_failures += 1;
                }
                served
            }
            None => false,
        };
        if !served {
            tracing::warn!(
                target: "bigname_project::families",
                chain_id = self.chain_id,
                block_number = self.head.number,
                block_hash = %self.head.hash,
                "the hydration endpoint does not serve this block; its remaining selectors are \
                 not observed and their rows are left unchanged"
            );
        }
        self.serves = Some(served);
    }

    /// The time the next call may take, or `None` when the pass has spent its budget.
    fn time_left(&mut self) -> Option<Duration> {
        let left = self.limits.block.saturating_sub(self.started.elapsed());
        if left.is_zero() {
            if !self.out_of_time {
                self.out_of_time = true;
                tracing::warn!(
                    target: "bigname_project::families",
                    chain_id = self.chain_id,
                    block_number = self.head.number,
                    block_hash = %self.head.hash,
                    budget_ms = self.limits.block.as_millis() as u64,
                    "hydration spent its time for this block; the remaining selectors are not \
                     observed and their rows are left unchanged"
                );
            }
            return None;
        }
        Some(left.min(self.limits.call))
    }
}
