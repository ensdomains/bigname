//! What a failed hydration read writes. An aggregate the endpoint does not answer observes
//! nothing: with an endpoint that does not serve the block, no hydration column changes; with one
//! that does, the aggregate is split, and a selector that still cannot be read keeps its value and
//! only moves back in its queue. A call that fails inside an answered aggregate is the one case
//! that clears a value (fail closed).
#[path = "families_hydration/reverse.rs"]
mod reverse;
#[path = "families_hydration/rpc.rs"]
mod rpc;
#[path = "families_support/mod.rs"]
mod support;

use std::sync::{Arc, Mutex};

use anyhow::Result;
use bigname_project::families::{FamilyMode, HydrationTimeLimits};
use reverse::{CHAIN, apply, attempts, claim, journalled, node, options, run, seed, tuple};
use support::{Fixture, hash, marker};

const TUPLES: &str = "project_reverse_tuple";
const HYDRATION: &str = "canonical_head_multicall_hydration";

async fn fixture(label: &str, blocks: i64) -> Result<(Fixture, rpc::Rpc)> {
    let fixture = Fixture::new(label, blocks).await?;
    fixture.lineage(CHAIN, blocks).await?;
    Ok((fixture, rpc::Rpc::new().await?))
}

/// Aggregate sizes the endpoint received at `block`, in order.
fn sizes(rpc: &rpc::Rpc, block: i64) -> Vec<usize> {
    rpc.calls()
        .into_iter()
        .filter(|(at, _)| *at == hash(block))
        .map(|(_, count)| count)
        .collect()
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn an_endpoint_that_does_not_serve_the_head_writes_no_hydration_and_logs_the_batch()
-> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_unserved", 4).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    rpc.answer(1, Some("kept.eth"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let observed = tuple(&fixture, 1).await?;
    let served = claim(&fixture, 1).await?;
    assert_eq!(served.row.raw_claim_name.as_deref(), Some("kept.eth"));

    // Block 2 claims a second address, and the endpoint fails every aggregate sent at block 2.
    seed(&fixture, 2, 2).await?;
    let logs = Logs::default();
    let writer = logs.clone();
    let outcome = {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        run(&fixture, 2, FamilyMode::Normal, &rpc).await?
    };
    // No hydration write: the first tuple keeps its name and the block it was observed at.
    assert_eq!(tuple(&fixture, 1).await?, observed);
    let unchanged = claim(&fixture, 1).await?;
    assert_eq!(unchanged.row.raw_claim_name.as_deref(), Some("kept.eth"));
    assert_eq!(
        unchanged.row.claim_provenance[HYDRATION]["block_hash"],
        hash(1)
    );
    // The family write the block's own event makes is published all the same.
    assert_eq!(journalled(&fixture, 2, TUPLES).await?, 1);
    let claimed = tuple(&fixture, 2).await?;
    assert!(claimed["hydrated_name"].is_null());
    assert!(claimed["attempt_block"].is_null());
    assert!(claimed["attempt_ordinal"].is_null());
    let hydration = &outcome.hydration;
    assert_eq!(
        (
            hydration.reverse.rpc_calls,
            hydration.reverse.rpc_failures,
            hydration.reverse.not_observed,
            hydration.reverse.answered,
            hydration.reverse.deferred,
        ),
        (1, 1, 2, 0, 0)
    );
    assert_eq!(
        hydration.reverse.value_writes + hydration.reverse.schedule_writes,
        0
    );
    assert_eq!((hydration.probes, hydration.probe_failures), (1, 1));
    assert_eq!((hydration.passes, hydration.unserved_passes), (1, 1));
    assert_eq!(hydration.text.rpc_calls, 0);
    let logs = String::from_utf8(logs.0.lock().unwrap().clone())?;
    assert!(logs.contains("a hydration RPC batch failed"), "{logs}");
    assert!(logs.contains("kind=\"reverse\""), "{logs}");
    assert!(logs.contains("selectors=2"), "{logs}");
    assert!(logs.contains("does not serve this block"), "{logs}");

    // A second failing head costs one aggregate and one probe again, with no retry fan-out. It
    // holds the never-read tuple, which is first in the rotation.
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!((sizes(&rpc, 3), rpc.probes()), (vec![1], 2));
    assert_eq!(tuple(&fixture, 1).await?, observed);
    assert_eq!(journalled(&fixture, 3, TUPLES).await?, 0);

    // Once the endpoint answers, the tuple that kept its place is read.
    rpc.answer(4, Some("back.eth"));
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    let read = tuple(&fixture, 2).await?;
    assert_eq!(read["hydrated_name"], "back.eth");
    assert_eq!(read["attempt_block"], 4);
    assert_eq!(tuple(&fixture, 1).await?, observed);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_failed_call_inside_an_answered_aggregate_clears_only_its_own_tuple() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_failed_call", 3).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    seed(&fixture, 1, 2).await?;
    rpc.answer(1, Some("first.eth"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    // The aggregate at block 2 is answered; the call for the second tuple fails inside it.
    rpc.answer(2, Some("second.eth"));
    rpc.fail_call(&node(2));
    let outcome = run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    rpc.clear_faults();
    let answered = tuple(&fixture, 1).await?;
    assert_eq!(answered["hydrated_name"], "second.eth");
    let failed = tuple(&fixture, 2).await?;
    assert!(failed["hydrated_name"].is_null(), "fail closed");
    assert_eq!(failed["attempt_block"], 2);
    assert_eq!(failed["attempt_hash"], hash(2));
    let served = claim(&fixture, 2).await?;
    assert_eq!(served.row.claim_status.as_str(), "not_found");
    assert!(served.row.claim_provenance.get(HYDRATION).is_none());
    let reverse = outcome.hydration.reverse;
    assert_eq!(
        (
            reverse.rpc_calls,
            reverse.rpc_failures,
            reverse.answered,
            reverse.failed_calls,
            reverse.value_writes,
            reverse.schedule_writes,
        ),
        (1, 0, 2, 1, 2, 0)
    );
    assert_eq!(outcome.hydration.probes, 0);

    // A successful empty answer is an answer: it replaces the name it finds.
    rpc.answer(3, Some(""));
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    for index in [1, 2] {
        let row = tuple(&fixture, index).await?;
        assert_eq!(row["hydrated_name"], "");
        assert_eq!(row["attempt_block"], 3);
        let served = claim(&fixture, index).await?;
        assert_eq!(served.row.claim_status.as_str(), "not_found");
        assert_eq!(
            served.row.claim_provenance[HYDRATION]["block_hash"],
            hash(3)
        );
    }
    fixture.cleanup().await
}

#[tokio::test]
async fn a_failed_aggregate_among_answered_ones_is_split_and_its_other_tuples_are_read()
-> Result<()> {
    const CHANGED: i64 = 600;
    let (fixture, rpc) = fixture("family_hydration_mixed_chunks", 1).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    // More tuples change in the head block than one aggregate holds: all are read, in three.
    for index in 1..=CHANGED {
        seed(&fixture, 1, index).await?;
    }
    rpc.answer(1, Some("read.eth"));
    // Any aggregate holding tuple 300 fails whole: the second of the three.
    rpc.poison(&node(300));
    let outcome = run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let reverse = outcome.hydration.reverse;
    let received = sizes(&rpc, 1);
    assert_eq!(received[..2], [250, 250]);
    assert_eq!(received.last(), Some(&100));
    assert_eq!(reverse.rpc_calls, received.len() as u64);
    assert_eq!(received.len(), 3 + 16, "the split is bounded");
    assert_eq!(
        rpc.probes(),
        0,
        "the first aggregate showed the endpoint serves"
    );
    // The tuples that shared the failed aggregate are read; the one that cannot be read, and at
    // most the one left beside it when the split calls ran out, are not.
    assert!((1..=2).contains(&reverse.deferred), "{reverse:?}");
    assert_eq!(reverse.answered + reverse.deferred, CHANGED as u64);
    assert_eq!(reverse.not_observed, 0);
    assert_eq!(reverse.value_writes, reverse.answered);
    assert_eq!(reverse.schedule_writes, reverse.deferred);
    let hydrated: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_reverse_tuple WHERE hydrated_name = 'read.eth'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(hydrated as u64, reverse.answered);
    let poisoned = tuple(&fixture, 300).await?;
    assert!(poisoned["hydrated_name"].is_null());
    assert!(poisoned["attempt_block"].is_null(), "nothing was observed");
    assert!(poisoned["attempt_ordinal"].is_i64(), "it only moved back");
    fixture.cleanup().await
}

#[tokio::test]
async fn an_aggregate_too_large_for_the_endpoint_is_read_in_smaller_ones_every_block() -> Result<()>
{
    let (fixture, rpc) = fixture("family_hydration_too_large", 4).await?;
    for index in 1..=251 {
        seed(&fixture, 1, index).await?;
    }
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    // The endpoint never answers an aggregate of more than 100 calls, at any block.
    rpc.limit(Some(100));
    for block in 2..=4 {
        rpc.answer(block, Some("read.eth"));
    }
    let outcome = run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    assert_eq!(sizes(&rpc, 2), vec![250, 125, 62, 63, 125, 62, 63]);
    let reverse = outcome.hydration.reverse;
    assert_eq!(
        (reverse.answered, reverse.deferred, reverse.not_observed),
        (250, 0, 0)
    );
    assert_eq!((reverse.rpc_calls, reverse.rpc_failures), (7, 3));
    assert_eq!(outcome.hydration.probes, 1);
    // The same failure every block neither blocks the queue nor grows: the tuple behind the
    // page is read next, then the page again.
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(sizes(&rpc, 3), vec![1]);
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    assert_eq!(sizes(&rpc, 4).len(), 7);
    assert_eq!(
        attempts(&fixture).await?,
        vec![(Some(3), 1), (Some(4), 250)]
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn an_unreadable_tuple_in_the_oldest_cohort_keeps_its_name_and_yields_its_place() -> Result<()>
{
    let (fixture, rpc) = fixture("family_hydration_isolated", 6).await?;
    for index in 1..=251 {
        seed(&fixture, 1, index).await?;
    }
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    rpc.answer(2, Some("first.eth"));
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    rpc.answer(3, Some("first.eth"));
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        attempts(&fixture).await?,
        vec![(Some(2), 250), (Some(3), 1)]
    );
    let observed = tuple(&fixture, 5).await?;

    // From block 4 on, any aggregate holding tuple 5 fails whole. Block 4 refreshes the oldest
    // cohort, the 250 tuples read at block 2, tuple 5 among them.
    rpc.poison(&node(5));
    for block in 4..=6 {
        rpc.answer(block, Some("second.eth"));
    }
    let outcome = run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    let reverse = outcome.hydration.reverse;
    assert_eq!(
        (reverse.answered, reverse.deferred, reverse.not_observed),
        (249, 1, 0)
    );
    assert_eq!((reverse.value_writes, reverse.schedule_writes), (249, 1));
    assert_eq!(sizes(&rpc, 4).len(), 1 + 16);
    assert_eq!(outcome.hydration.probes, 1);
    // Tuple 5 keeps the last name observed for it, with the block it was observed at. Only its
    // place in the rotation moved, to the cohort the others were just read into.
    let deferred = tuple(&fixture, 5).await?;
    for column in ["hydrated_name", "attempt_block", "attempt_hash", "baseline"] {
        assert_eq!(deferred[column], observed[column], "{column}");
    }
    assert_eq!(deferred["attempt_block"], 2);
    let refreshed = tuple(&fixture, 1).await?;
    assert_eq!(refreshed["hydrated_name"], "second.eth");
    assert_eq!(deferred["attempt_ordinal"], refreshed["attempt_ordinal"]);
    assert_ne!(deferred["attempt_ordinal"], observed["attempt_ordinal"]);
    let served = claim(&fixture, 5).await?;
    assert_eq!(served.row.raw_claim_name.as_deref(), Some("first.eth"));
    assert_eq!(served.row.claim_provenance[HYDRATION]["block_number"], 2);
    assert_eq!(
        served.row.claim_provenance[HYDRATION]["block_hash"],
        hash(2)
    );

    // Block 5 therefore reads the next cohort, the tuple read at block 3, instead of tuple 5
    // alone again.
    run(&fixture, 5, FamilyMode::Normal, &rpc).await?;
    assert_eq!(sizes(&rpc, 5), vec![1]);
    assert_eq!(tuple(&fixture, 251).await?["attempt_block"], 5);
    // And block 6 reaches tuple 5's cohort again, where it is still the only one unread.
    let outcome = run(&fixture, 6, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.hydration.reverse.deferred, 1);
    assert_eq!(
        attempts(&fixture).await?,
        vec![(Some(2), 1), (Some(5), 1), (Some(6), 249)]
    );
    assert_eq!(tuple(&fixture, 5).await?["hydrated_name"], "first.eth");
    fixture.cleanup().await
}

#[tokio::test]
async fn hydration_that_runs_out_of_time_publishes_the_block_without_the_unread_tuples()
-> Result<()> {
    use std::time::Duration;
    let (fixture, rpc) = fixture("family_hydration_time", 2).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    // 600 tuples change in the head block: three aggregates. The endpoint takes 300 ms an
    // answer and the block's reads may take 400 ms, so the second aggregate is cut off when the
    // time is spent (or, on a slow machine, never sent) and the third is never sent.
    for index in 1..=600 {
        seed(&fixture, 1, index).await?;
    }
    rpc.answer(1, Some("read.eth"));
    rpc.delay(Duration::from_millis(300));
    rpc::head(&fixture.pool, 1).await?;
    let limits = HydrationTimeLimits {
        call: Duration::from_secs(5),
        block: Duration::from_millis(400),
    };
    let options = options(&rpc).with_hydration_time_limits(limits);
    let (outcome, error) = apply(&fixture, &marker(1), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(marker(1)), "the block is published");
    let reverse = outcome.hydration.reverse;
    assert_eq!(outcome.hydration.timed_out_passes, 1);
    assert!(
        reverse.answered == 500 || reverse.answered == 250,
        "{reverse:?}"
    );
    assert_eq!(reverse.not_observed, 600 - reverse.answered);
    assert!(reverse.rpc_failures <= 1, "{reverse:?}");
    assert_eq!(reverse.deferred, 0, "out of time is not a deferral");
    assert_eq!(reverse.value_writes, reverse.answered);
    let unread = tuple(&fixture, 600).await?;
    assert!(unread["hydrated_name"].is_null());
    assert!(
        unread["attempt_ordinal"].is_null(),
        "not observed is no write"
    );

    // One call that outlasts its own limit is a failed batch; the probe outlasts it too, so the
    // endpoint counts as not serving the block and nothing is written.
    rpc.answer(2, Some("late.eth"));
    rpc::head(&fixture.pool, 2).await?;
    let limits = HydrationTimeLimits {
        call: Duration::from_millis(100),
        block: Duration::from_secs(5),
    };
    let options = options.with_hydration_time_limits(limits);
    let before = fixture.rows("project_reverse_tuple").await?;
    let (outcome, error) = apply(&fixture, &marker(2), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(marker(2)));
    let reverse = outcome.hydration.reverse;
    assert_eq!((reverse.rpc_calls, reverse.rpc_failures), (1, 1));
    assert_eq!(
        (outcome.hydration.probes, outcome.hydration.unserved_passes),
        (1, 1)
    );
    assert_eq!(fixture.rows("project_reverse_tuple").await?, before);
    fixture.cleanup().await
}
