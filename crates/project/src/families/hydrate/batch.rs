//! One head block's hydration reads, shared by the reverse and text kinds. Selectors go out as
//! Multicall3 aggregates of at most [`BATCH_LIMIT`] calls, and every selector ends as one [`Read`].
//!
//! An aggregate the endpoint answers gives each selector its own result, a failed call included.
//! An aggregate that fails as a whole says nothing about any selector in it, so it never becomes
//! a per-selector failure. What happens next depends on what is known about the endpoint:
//!
//! - It does not serve the block: its JSON-RPC error says so
//!   (`rpc_error_reports_block_unavailable`, at any point of the pass), or nothing has been
//!   answered yet in this pass and a one-call aggregate at the same block hash fails too. Every
//!   selector not yet answered is [`Read::Unobserved`] and no further call is made for this
//!   block, by either kind.
//! - It answered something at this block, a real aggregate or that one-call probe. This is
//!   evidence, not proof, that the failure comes from what the aggregate holds (its encoded
//!   size, execution cost or response size, or one selector): an endpoint can answer a small
//!   call and still refuse a real one. The aggregate is split in halves and each half sent
//!   again while the pass has calls and time for them; otherwise its selectors are
//!   [`Read::Deferred`] with the size of aggregate they may next be sent in, at most half of the
//!   one that failed. A later head starts from those sizes, so the splitting continues where
//!   this block left it instead of starting again from the whole aggregate.
//!
//! One kind sends at most one aggregate per [`BATCH_LIMIT`] selectors plus [`ISOLATION_CALLS`]
//! per block. A selector whose first aggregate the block has no call left for is
//! [`Read::Unobserved`] and keeps its place for the next head.
//!
//! Every call and the whole pass are bounded by [`HydrationTimeLimits`]; the kind read first has
//! half of the pass's time while the other has selectors waiting. A failed aggregate is split
//! only while its kind has time for both halves and for every half already waiting, each at the
//! longest any call of this pass has taken. A call the kind's time cuts short, and everything
//! of that kind after it, is [`Read::Unobserved`].
use std::time::{Duration, Instant};

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

/// How long hydration may wait on RPC. This bounds the time spent waiting for RPC answers, not
/// the publication: selecting the block's selectors before the reads and publishing after them
/// take their own time. Reads that do not fit are not observed and the block is published
/// without them.
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

/// Calls one kind may send per block beyond one per [`BATCH_LIMIT`] selectors: two per split,
/// so sixteen follow one failing selector down from 250 calls to its own.
const ISOLATION_CALLS: usize = 16;

/// What one hydration pass learned about one selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Read<T> {
    /// The endpoint answered the selector's aggregate; this is the selector's own result, which
    /// may itself be a failed call.
    Answered(T),
    /// The selector's aggregate failed while the endpoint answered other calls at the block.
    /// Nothing was observed: the stored value and the block it was observed at stay, the
    /// selector moves behind the rest of its queue, and it is next sent in an aggregate of at
    /// most `limit` calls.
    Deferred { limit: usize },
    /// Not observed: the endpoint did not serve the block, or the pass had no time or no call
    /// left. The selector's row is left exactly as it is.
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

/// Why an aggregate failed as a whole.
pub(super) struct Failure {
    pub(super) message: String,
    /// The endpoint's error says it cannot serve the block.
    pub(super) block_unavailable: bool,
}

/// One kind's way of sending an aggregate: one answer per request, or why the whole aggregate
/// failed.
pub(super) trait Aggregate: Sync {
    type Request: Clone + Send + Sync;
    type Answer: Send;

    fn send(
        &self,
        chunk: &[Self::Request],
    ) -> impl Future<Output = std::result::Result<Vec<Self::Answer>, Failure>> + Send;
}

/// Selectors that go out in one aggregate.
struct Group {
    members: Vec<usize>,
    /// A half of an aggregate that failed in this pass, as opposed to a first aggregate.
    half: bool,
}

/// The aggregates a pass starts with. Selectors without a size limit fill aggregates of
/// [`BATCH_LIMIT`] in request order. The others are packed smallest limit first, each aggregate
/// no larger than the smallest limit in it. Aggregates go out in the order of their earliest
/// request, so the queue order the requests arrive in decides who is read first.
fn groups(limits: &[Option<usize>]) -> Vec<Group> {
    let limit =
        |index: usize| limits[index].map_or(BATCH_LIMIT, |limit| limit.clamp(1, BATCH_LIMIT));
    let (mut limited, free): (Vec<usize>, Vec<usize>) =
        (0..limits.len()).partition(|index| limits[*index].is_some());
    limited.sort_by_key(|index| (limit(*index), *index));
    let mut groups: Vec<Vec<usize>> = free.chunks(BATCH_LIMIT).map(<[usize]>::to_vec).collect();
    let mut open: Vec<usize> = Vec::new();
    for index in limited {
        if open
            .first()
            .is_some_and(|first| open.len() >= limit(*first))
        {
            groups.push(std::mem::take(&mut open));
        }
        open.push(index);
    }
    groups.extend(Some(open).filter(|open| !open.is_empty()));
    groups.sort_by_key(|members| members.iter().copied().min());
    groups
        .into_iter()
        .map(|members| Group {
            members,
            half: false,
        })
        .collect()
}

fn settle<T>(reads: &mut [Option<Read<T>>], members: &[usize], read: impl Fn() -> Read<T>) {
    for member in members {
        reads[*member] = Some(read());
    }
}

enum Sent<T> {
    Answered(Vec<T>),
    /// The aggregate failed and the pass goes on.
    Failed,
    /// Not sent, or cut short by the pass's time limit: the pass makes no further call.
    Stopped,
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
    /// The time since the start at which the kind being read stops, and whether it has.
    until: Duration,
    stopped: bool,
    /// The longest a call of this pass took, a call that ran into its limit included.
    slowest: Duration,
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
            until: limits.block,
            stopped: false,
            slowest: Duration::ZERO,
        }
    }

    /// Give the kind read next one part in `parts` of the time the pass has left. The first
    /// kind takes a part so that it cannot spend the whole pass while the other has selectors
    /// waiting.
    pub(super) fn share(&mut self, parts: u32) {
        let elapsed = self.started.elapsed();
        let left = self.limits.block.saturating_sub(elapsed);
        self.until = elapsed + left / parts.max(1);
        self.stopped = false;
    }

    /// Count the pass's time and its one result into the run's outcome. A pass that ran out of
    /// time is counted as that, whatever else it learned.
    pub(super) fn finish(self) {
        self.stats.rpc_ms += u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if self.out_of_time {
            self.stats.timed_out_passes += 1;
        } else if self.serves == Some(false) {
            self.stats.unserved_passes += 1;
        }
    }

    /// Read `requests` through `call`, which sends one aggregate. `limits` holds, per request,
    /// the size of aggregate an earlier head left it with. The reads come back in request order.
    pub(super) async fn read<A: Aggregate>(
        &mut self,
        kind: Kind,
        requests: &[A::Request],
        limits: &[Option<usize>],
        call: &A,
    ) -> Vec<Read<A::Answer>> {
        let mut reads: Vec<Option<Read<A::Answer>>> = requests.iter().map(|_| None).collect();
        let mut calls = requests.len().div_ceil(BATCH_LIMIT) + ISOLATION_CALLS;
        let mut pending = groups(limits);
        pending.reverse();
        while let Some(Group { members, half }) = pending.pop() {
            let size = members.len();
            if self.serves == Some(false) || self.time_left().is_none() {
                settle(&mut reads, &members, || Read::Unobserved);
                continue;
            }
            if calls == 0 {
                // A half was part of an aggregate that failed; a first aggregate was not tried.
                settle(&mut reads, &members, || match half {
                    true => Read::Deferred { limit: size },
                    false => Read::Unobserved,
                });
                continue;
            }
            let chunk: Vec<A::Request> = members
                .iter()
                .map(|member| requests[*member].clone())
                .collect();
            match self.aggregate(kind, &chunk, call).await {
                Sent::Answered(found) => {
                    calls -= 1;
                    for (member, value) in members.iter().zip(found) {
                        reads[*member] = Some(Read::Answered(value));
                    }
                }
                Sent::Stopped => settle(&mut reads, &members, || Read::Unobserved),
                Sent::Failed => {
                    calls -= 1;
                    if self.serves.is_none() {
                        self.probe().await;
                    }
                    let waiting = pending.iter().filter(|group| group.half).count();
                    if self.serves != Some(true) || self.stopped {
                        settle(&mut reads, &members, || Read::Unobserved);
                    } else if size > 1 && calls > 0 && self.has_time_for(waiting + 2) {
                        let (left, right) = members.split_at(size / 2);
                        for members in [right, left] {
                            pending.push(Group {
                                members: members.to_vec(),
                                half: true,
                            });
                        }
                    } else {
                        settle(&mut reads, &members, || Read::Deferred {
                            limit: (size / 2).max(1),
                        });
                    }
                }
            }
        }
        let reads: Vec<Read<A::Answer>> = reads
            .into_iter()
            .map(|read| read.expect("every group of requests was resolved"))
            .collect();
        let stats = kind.of(self.stats);
        for read in &reads {
            match read {
                Read::Answered(_) => stats.answered += 1,
                Read::Deferred { .. } => stats.deferred += 1,
                Read::Unobserved => stats.not_observed += 1,
            }
        }
        reads
    }

    /// Send one aggregate, unless the pass has stopped.
    async fn aggregate<A: Aggregate>(
        &mut self,
        kind: Kind,
        chunk: &[A::Request],
        call: &A,
    ) -> Sent<A::Answer> {
        if self.serves == Some(false) {
            return Sent::Stopped;
        }
        let Some(limit) = self.time_left() else {
            return Sent::Stopped;
        };
        kind.of(self.stats).rpc_calls += 1;
        let began = Instant::now();
        let answer = tokio::time::timeout(limit, call.send(chunk)).await;
        // The pass's own limit, not the call's, ended a call that was given less than a call's
        // time and used all of it: nothing is known about the aggregate.
        let cut = answer.is_err() && limit < self.limits.call;
        let failure = match answer {
            Ok(Ok(found)) if found.len() == chunk.len() => {
                self.slowest = self.slowest.max(began.elapsed());
                self.serves = Some(true);
                return Sent::Answered(found);
            }
            Ok(Ok(found)) => Failure {
                message: format!("{} results for {} calls", found.len(), chunk.len()),
                block_unavailable: false,
            },
            Ok(Err(failure)) => failure,
            Err(_) => Failure {
                message: format!("no answer within {} ms", limit.as_millis()),
                block_unavailable: false,
            },
        };
        kind.of(self.stats).rpc_failures += 1;
        tracing::warn!(
            target: "bigname_project::families",
            chain_id = self.chain_id,
            block_number = self.head.number,
            block_hash = %self.head.hash,
            kind = kind.label(),
            selectors = chunk.len(),
            error = %failure.message,
            "a hydration RPC batch failed; none of its selectors was observed by it"
        );
        if cut {
            self.spent();
            return Sent::Stopped;
        }
        self.slowest = self.slowest.max(began.elapsed().min(self.limits.call));
        if failure.block_unavailable {
            self.unserved();
        }
        Sent::Failed
    }

    /// Ask the endpoint for a one-call aggregate at the block, when nothing has been answered at
    /// it yet. The call's own result is ignored: only whether the endpoint answered an aggregate
    /// at this block hash counts. A probe the pass has no time to send, or to wait for, says
    /// nothing about the endpoint.
    async fn probe(&mut self) {
        let Some(limit) = self.time_left() else {
            return;
        };
        let block = EnsReverseNameMulticallBlock {
            block_number: self.head.number,
            block_hash: self.head.hash.clone(),
        };
        let request = [EnsReverseNameMulticallRequest {
            resolver_address: MULTICALL3_ADDRESS.to_owned(),
            reverse_node: format!("0x{}", "0".repeat(64)),
        }];
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
        match answer {
            Ok(Ok(_)) => self.serves = Some(true),
            Err(_) if limit < self.limits.call => {
                self.stats.probe_failures += 1;
                self.spent();
            }
            _ => {
                self.stats.probe_failures += 1;
                self.unserved();
            }
        }
    }

    fn unserved(&mut self) {
        self.serves = Some(false);
        tracing::warn!(
            target: "bigname_project::families",
            chain_id = self.chain_id,
            block_number = self.head.number,
            block_hash = %self.head.hash,
            "the hydration endpoint does not serve this block; its remaining selectors are \
             not observed and their rows are left unchanged"
        );
    }

    fn spent(&mut self) {
        if !self.stopped {
            self.stopped = true;
            self.out_of_time = true;
            tracing::warn!(
                target: "bigname_project::families",
                chain_id = self.chain_id,
                block_number = self.head.number,
                block_hash = %self.head.hash,
                budget_ms = self.until.as_millis() as u64,
                "hydration spent its time for this block; the selectors it was still reading \
                 are not observed and their rows are left unchanged"
            );
        }
    }

    /// Whether the pass has time for `calls` more calls as slow as its slowest so far.
    fn has_time_for(&self, calls: usize) -> bool {
        let left = self.until.saturating_sub(self.started.elapsed());
        left >= self
            .slowest
            .saturating_mul(u32::try_from(calls).unwrap_or(u32::MAX))
    }

    /// The time the next call may take, or `None` when the pass has spent its budget.
    fn time_left(&mut self) -> Option<Duration> {
        if self.stopped {
            return None;
        }
        let left = self.until.saturating_sub(self.started.elapsed());
        if left.is_zero() {
            self.spent();
            return None;
        }
        Some(left.min(self.limits.call))
    }
}

#[cfg(test)]
#[path = "batch/tests.rs"]
mod tests;
