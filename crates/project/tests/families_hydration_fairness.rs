//! Failed aggregates across heads. A read that cannot finish splitting a failed aggregate leaves
//! each tuple the size of aggregate it may next be sent in, so later heads go on from there:
//! every tuple the endpoint can answer is observed, however the unreadable ones are placed and
//! however slowly their aggregates fail. That scheduling state is journalled with the tuple.
#[path = "families_hydration/reverse.rs"]
mod reverse;
#[path = "families_hydration/rpc.rs"]
mod rpc;
#[path = "families_support/mod.rs"]
mod support;

use std::time::Duration;

use anyhow::Result;
use bigname_project::families::{FamilyMode, HydrationTimeLimits};
use reverse::{CHAIN, apply, fork_block, journalled, node, options, run, seed, tuple, work_index};
use support::{Fixture, hash, marker};

async fn fixture(label: &str, blocks: i64) -> Result<(Fixture, rpc::Rpc)> {
    let fixture = Fixture::new(label, blocks).await?;
    fixture.lineage(CHAIN, blocks).await?;
    let rpc = rpc::Rpc::new().await?;
    for block in 0..=blocks {
        rpc.answer(block, Some(&format!("block{block}.eth")));
    }
    Ok((fixture, rpc))
}

/// Aggregate sizes the endpoint received at `block`, in order.
fn sizes(rpc: &rpc::Rpc, block: i64) -> Vec<usize> {
    rpc.calls()
        .into_iter()
        .filter(|(at, _)| *at == hash(block))
        .map(|(_, count)| count)
        .collect()
}

/// Tuples with an observed name.
async fn observed(fixture: &Fixture) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM project_reverse_tuple WHERE hydrated_name IS NOT NULL",
    )
    .fetch_one(&fixture.pool)
    .await?)
}

#[tokio::test]
async fn tuples_beside_two_unreadable_ones_are_all_observed_on_the_next_head() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_fair_pair", 4).await?;
    for index in 1..=250 {
        seed(&fixture, 1, index).await?;
    }
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    // Any aggregate holding the first or the last tuple of the page fails whole, at every block.
    rpc.poison(&node(1));
    rpc.poison(&node(250));

    // The first head spends its calls following the first tuple down and cannot split the last
    // quarter of the page: 62 readable tuples are left unread beside the two unreadable ones.
    let outcome = run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    let reverse = outcome.hydration.reverse;
    assert_eq!((reverse.answered, reverse.deferred), (186, 64));
    assert_eq!((reverse.rpc_calls, reverse.not_observed), (17, 0));
    assert_eq!(observed(&fixture).await?, 186);
    assert_eq!(tuple(&fixture, 1).await?["attempt_limit"], 1);
    assert_eq!(tuple(&fixture, 200).await?["attempt_limit"], 31);
    assert!(tuple(&fixture, 100).await?["attempt_limit"].is_null());

    // The second head starts from those sizes, not from the whole page again: the unreadable
    // first tuple goes alone, the tuples already read go together, and the unfinished quarter
    // goes in aggregates of half its size, which leaves the last tuple alone.
    let outcome = run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(sizes(&rpc, 3), vec![1, 186, 31, 31, 1]);
    let reverse = outcome.hydration.reverse;
    assert_eq!((reverse.answered, reverse.deferred), (248, 2));
    assert_eq!(observed(&fixture).await?, 248, "every readable tuple");
    for index in [1, 250] {
        let unreadable = tuple(&fixture, index).await?;
        assert!(unreadable["hydrated_name"].is_null());
        assert_eq!(unreadable["attempt_limit"], 1);
        assert_eq!(unreadable["attempt_failures"], 2);
    }
    let read = tuple(&fixture, 249).await?;
    assert_eq!(read["hydrated_name"], "block3.eth");
    assert!(read["attempt_limit"].is_null() && read["attempt_failures"].is_null());

    // From then on the two unreadable tuples cost one call each and hold nobody back.
    let outcome = run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    assert_eq!(sizes(&rpc, 4), vec![1, 248, 1]);
    assert_eq!(outcome.hydration.reverse.answered, 248);
    let refreshed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_reverse_tuple WHERE hydrated_name = 'block4.eth'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(refreshed, 248);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_slow_failing_aggregate_gives_up_its_place_and_its_other_tuples_are_observed()
-> Result<()> {
    const HEADS: i64 = 24;
    let (fixture, rpc) = fixture("family_hydration_fair_slow", HEADS).await?;
    for index in 1..=251 {
        seed(&fixture, 1, index).await?;
    }
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    // Any aggregate holding tuple 3 outlasts a call's time; every other aggregate, and the
    // one-call probe, answers at once.
    rpc.slow(&node(3), Duration::from_secs(5));
    let limits = HydrationTimeLimits {
        call: Duration::from_millis(500),
        block: Duration::from_millis(1500),
    };
    let options = options(&rpc).with_hydration_time_limits(limits);

    // The page's one aggregate times out. Two halves as slow would not fit the block's time, so
    // it is not split: the page gives up its place with half the size, and the pass is not cut.
    rpc::head(&fixture.pool, 2).await?;
    let (outcome, error) = apply(&fixture, &marker(2), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    let reverse = outcome.hydration.reverse;
    assert_eq!(sizes(&rpc, 2), vec![250]);
    assert_eq!((reverse.deferred, reverse.schedule_writes), (250, 250));
    assert_eq!((reverse.not_observed, reverse.value_writes), (0, 0));
    assert_eq!(
        (
            outcome.hydration.timed_out_passes,
            outcome.hydration.unserved_passes
        ),
        (0, 0)
    );
    assert_eq!(tuple(&fixture, 3).await?["attempt_limit"], 125);

    // So the tuple behind the page is read on the very next head.
    rpc::head(&fixture.pool, 3).await?;
    let (_, error) = apply(&fixture, &marker(3), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(sizes(&rpc, 3), vec![1]);
    assert_eq!(tuple(&fixture, 251).await?["hydrated_name"], "block3.eth");

    // Later heads halve the slow aggregate until tuple 3 is alone: every other tuple is observed.
    let mut heads = 3;
    while observed(&fixture).await? < 250 {
        heads += 1;
        assert!(heads <= HEADS, "readable tuples were never observed");
        rpc::head(&fixture.pool, heads).await?;
        let (outcome, error) = apply(&fixture, &marker(heads), FamilyMode::Normal, &options).await;
        assert!(error.is_none(), "{error:?}");
        assert_eq!(outcome.hydration.timed_out_passes, 0, "head {heads}");
        assert!(
            outcome.hydration.rpc_ms < 1500,
            "head {heads}: {:?}",
            outcome.hydration
        );
    }
    let slow = tuple(&fixture, 3).await?;
    assert!(slow["hydrated_name"].is_null());
    assert_eq!(slow["attempt_limit"], 1);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_deferred_tuple_is_restored_by_undo_and_replayed_without_the_endpoint() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_fair_undo", 3).await?;
    for index in 1..=3 {
        seed(&fixture, 1, index).await?;
    }
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    run(&fixture, 2, FamilyMode::Normal, &rpc).await?;
    let before = fixture.rows("project_reverse_tuple").await?;
    let (index_before, derived) = work_index(&fixture).await?;
    assert_eq!(index_before, derived);

    // At block 3 the aggregate holding tuple 2 fails: the other two are read, tuple 2 is not.
    rpc.poison(&node(2));
    let outcome = run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    let reverse = outcome.hydration.reverse;
    assert_eq!((reverse.answered, reverse.deferred), (2, 1));
    assert_eq!((reverse.value_writes, reverse.schedule_writes), (2, 1));
    let deferred = tuple(&fixture, 2).await?;
    assert_eq!(deferred["hydrated_name"], "block2.eth");
    assert_eq!(deferred["attempt_block"], 2);
    assert_eq!(deferred["attempt_limit"], 1);
    assert_eq!(deferred["attempt_failures"], 1);
    assert_eq!(journalled(&fixture, 3, "project_reverse_tuple").await?, 3);
    let (index, derived) = work_index(&fixture).await?;
    assert_eq!(index, derived, "the work index follows the deferred tuple");
    assert_ne!(index, index_before);

    // Block 3 is replaced. The first run undoes it and stops on its one-block budget.
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 3",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    let replacement = fork_block(&fixture, 3, &hash(2)).await?;
    let calls = (rpc.calls(), rpc.probes());
    let options = options(&rpc).with_max_blocks_per_run(1);
    let (outcome, error) = apply(&fixture, &replacement, FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!((outcome.undone_blocks, outcome.blocks), (1, 0));
    assert_eq!(fixture.rows("project_reverse_tuple").await?, before);
    let (index, derived) = work_index(&fixture).await?;
    assert_eq!(index, derived);
    assert_eq!(index, index_before, "undo restores the scheduling state");

    // The replay of the replacement makes no call and leaves the restored rows as they are.
    let (outcome, error) = apply(&fixture, &replacement, FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(replacement.clone()));
    assert_eq!(outcome.hydration.passes, 0);
    assert_eq!((rpc.calls(), rpc.probes()), calls);
    assert_eq!(fixture.rows("project_reverse_tuple").await?, before);
    let (index, derived) = work_index(&fixture).await?;
    assert_eq!((index, &derived), (index_before, &derived));
    fixture.cleanup().await
}
