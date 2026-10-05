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
    rpc.slow(&node(3), Duration::from_secs(10));
    let limits = HydrationTimeLimits {
        call: Duration::from_secs(1),
        block: Duration::from_secs(3),
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
        assert_eq!(outcome.hydration.unserved_passes, 0, "head {heads}");
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

#[tokio::test]
async fn continuing_nineteen_changed_singletons_cannot_starve_the_rolling_reverse_page()
-> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_arrivals", 4).await?;
    for index in 1..=269 {
        seed(&fixture, 1, index).await?;
    }
    run(&fixture, 1, FamilyMode::Rebuild, &rpc).await?;
    // The first nineteen are the source state of already-isolated outer failures. They have
    // never observed a name, so no observation height is fabricated for this scheduling state.
    sqlx::query(
        "UPDATE project_reverse_tuple SET attempt_limit=1, attempt_failures=1 WHERE address <= $1",
    )
    .bind(reverse::address(19))
    .execute(&fixture.pool)
    .await?;
    sqlx::query("UPDATE project_reverse_hydration_work SET attempt_failures=1 WHERE address <= $1")
        .bind(reverse::address(19))
        .execute(&fixture.pool)
        .await?;
    for index in 1..=19 {
        rpc.poison(&node(index));
    }
    for block in 2..=4 {
        for index in 1..=19 {
            seed(&fixture, block, index).await?;
        }
        let outcome = run(&fixture, block, FamilyMode::Normal, &rpc).await?;
        assert!(outcome.hydration.reverse.rpc_calls <= 18);
        let read: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_reverse_tuple WHERE address > $1 AND attempt_block=$2 AND hydrated_name IS NOT NULL"
        ).bind(reverse::address(19)).bind(block).fetch_one(&fixture.pool).await?;
        assert_eq!(
            read, 250,
            "continuing changed work cannot take the rolling page's service"
        );
        assert_eq!(sizes(&rpc, block).first(), Some(&250));
    }
    fixture.cleanup().await
}

#[tokio::test]
async fn failed_reverse_children_cool_until_expiry_or_fresh_selector_evidence() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_retry_delay", 6).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    for index in [1, 2] {
        seed(&fixture, 1, index).await?;
    }
    rpc.fail_call(&node(2));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let failed = tuple(&fixture, 2).await?;
    assert!(failed["hydrated_name"].is_null());
    assert_eq!(failed["attempt_failures"], 1);
    rpc.clear_faults();
    for block in [2, 3] {
        let outcome = run(&fixture, block, FamilyMode::Normal, &rpc).await?;
        assert_eq!(outcome.hydration.reverse.answered, 1);
        assert_eq!(tuple(&fixture, 2).await?, failed);
    }
    assert!(
        !reverse::selected_addresses(&fixture, 7200)
            .await?
            .contains(&reverse::address(2))
    );
    assert!(
        reverse::selected_addresses(&fixture, 7201)
            .await?
            .contains(&reverse::address(2))
    );
    let before = fixture.rows("project_reverse_tuple").await?;
    let (work, derived) = work_index(&fixture).await?;
    assert_eq!(work, derived);
    // A fresh claim is published while catching up; only the later head performs the reads.
    seed(&fixture, 4, 2).await?;
    let outcome = run(&fixture, 5, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.hydration.passes, 1);
    let changed = tuple(&fixture, 2).await?;
    assert_eq!(changed["hydrated_name"], "block5.eth");
    assert!(changed["attempt_failures"].is_null());
    bigname_project::families::undo_to(&fixture.pool, CHAIN, 3).await?;
    assert_eq!(fixture.rows("project_reverse_tuple").await?, before);
    assert_eq!(work_index(&fixture).await?, (work.clone(), work));
    let calls = rpc.calls();
    run(&fixture, 5, FamilyMode::Redo { from: 4, to: 5 }, &rpc).await?;
    assert_eq!(rpc.calls(), calls);
    assert!(tuple(&fixture, 2).await?["attempt_failures"].is_null());
    fixture.cleanup().await
}

async fn fresh_reverse_evidence(fixture: &Fixture, block: i64) -> Result<()> {
    use serde_json::json;
    use support::Event;

    for (index, kind, family, after) in [
        (
            1,
            "ReverseChanged",
            "ens_v1_reverse_l1",
            json!({
                "source_event":"ReverseClaimed", "address":reverse::address(1),
                "coin_type":"60", "namespace":"ens", "reverse_node":node(1)
            }),
        ),
        (
            2,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "source_event":"NameChanged", "node":node(2), "resolver":reverse::SILENT,
                "record_key":"name", "record_family":"name", "raw_name":"fresh.eth"
            }),
        ),
        (
            3,
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({
                "source_event":"NewResolver", "node":node(3),
                "resolver":"0x0000000000000000000000000000000000000000"
            }),
        ),
    ] {
        fixture
            .event(
                Event::new(
                    &format!("fresh:{block}:{index}"),
                    block,
                    index,
                    kind,
                    family,
                )
                .on(CHAIN)
                .after(after),
            )
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn fresh_evidence_resets_never_observed_reverse_splits_without_restarting_unchanged_work()
-> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_unobserved_reset", 5).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    for index in 1..=4 {
        seed(&fixture, 1, index).await?;
        rpc.poison(&node(index));
    }
    let first = run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    assert_eq!(
        (
            first.hydration.reverse.answered,
            first.hydration.reverse.deferred
        ),
        (0, 4)
    );
    for block in [1, 2] {
        if block == 2 {
            run(&fixture, block, FamilyMode::Normal, &rpc).await?;
            assert_eq!(
                sizes(&rpc, block),
                vec![1, 1, 1, 1],
                "resume the saved splits"
            );
        }
        for index in 1..=4 {
            let deferred = tuple(&fixture, index).await?;
            assert_eq!(deferred["attempt_limit"], 1);
            assert_eq!(deferred["attempt_failures"], block);
            for field in ["attempt_block", "attempt_hash", "baseline", "hydrated_name"] {
                assert!(deferred[field].is_null(), "{field} is still unobserved");
            }
        }
    }

    // Exercise all three producer paths in the actual head preview and publication. Tuple 4
    // has no new evidence: it keeps its singleton progress while tuples 1 and 2 regroup.
    fresh_reverse_evidence(&fixture, 3).await?;
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(sizes(&rpc, 3), vec![1, 2, 1, 1]);
    for index in [1, 2] {
        assert_eq!(tuple(&fixture, index).await?["attempt_failures"], 1);
    }
    let retired = tuple(&fixture, 3).await?;
    assert!(retired["attempt_limit"].is_null() && retired["attempt_failures"].is_null());
    assert_eq!(tuple(&fixture, 4).await?["attempt_failures"], 3);
    let before = fixture.rows("project_reverse_tuple").await?;
    let before_work = fixture.rows("project_reverse_hydration_work").await?;

    // Fresh evidence below the readable head must clear the schedule before any RPC occurs.
    fresh_reverse_evidence(&fixture, 4).await?;
    rpc::head(&fixture.pool, 5).await?;
    let calls = rpc.calls();
    let (outcome, error) = apply(&fixture, &marker(4), FamilyMode::Normal, &options(&rpc)).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.hydration.passes, 0);
    for index in [1, 2] {
        let reset = tuple(&fixture, index).await?;
        assert!(reset["attempt_limit"].is_null() && reset["attempt_failures"].is_null());
        assert!(reset["attempt_block"].is_null() && reset["baseline"].is_null());
    }
    assert_eq!(tuple(&fixture, 4).await?["attempt_failures"], 3);
    let reset_rows = fixture.rows("project_reverse_tuple").await?;
    let reset_work = fixture.rows("project_reverse_hydration_work").await?;
    bigname_project::families::undo_to(&fixture.pool, CHAIN, 3).await?;
    assert_eq!(fixture.rows("project_reverse_tuple").await?, before);
    assert_eq!(
        fixture.rows("project_reverse_hydration_work").await?,
        before_work
    );
    let (_, error) = apply(
        &fixture,
        &marker(4),
        FamilyMode::Redo { from: 4, to: 4 },
        &options(&rpc),
    )
    .await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(rpc.calls(), calls, "catch-up and replay are provider-free");
    assert_eq!(fixture.rows("project_reverse_tuple").await?, reset_rows);
    assert_eq!(
        fixture.rows("project_reverse_hydration_work").await?,
        reset_work
    );
    fixture.cleanup().await
}
