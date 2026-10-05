//! Which block hydrates: only an ordinary follow block that is the highest readable block the
//! run captured, by number and hash. Catch-up blocks, the block a budget stops on, replayed and
//! rebuilt blocks and a target below the readable head never call RPC.
#[path = "families_hydration/reverse.rs"]
mod reverse;
#[path = "families_hydration/rpc.rs"]
mod rpc;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_project::{
    Marker,
    families::{FamilyMode, RebuildRanges},
};
use reverse::{CHAIN, apply, fork_block, journalled, options, run, seed, tuple};
use support::{Fixture, hash, marker};

const TUPLES: &str = "project_reverse_tuple";

async fn fixture(label: &str, blocks: i64) -> Result<(Fixture, rpc::Rpc)> {
    let fixture = Fixture::new(label, blocks).await?;
    fixture.lineage(CHAIN, blocks).await?;
    let rpc = rpc::Rpc::new().await?;
    for block in 0..=blocks {
        rpc.answer(block, Some(&format!("block{block}.eth")));
    }
    Ok((fixture, rpc))
}

#[tokio::test]
async fn only_the_readable_head_of_a_multi_block_follow_run_hydrates() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_head_run", 5).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    seed(&fixture, 3, 2).await?;
    let outcome = run(&fixture, 5, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.blocks, 5);
    assert_eq!(
        rpc.calls(),
        vec![(hash(5), 2)],
        "blocks 1 to 4 issue no hydration RPC; block 5 reads both tuples"
    );
    assert_eq!(outcome.hydration.passes, 1);
    // The catch-up blocks still publish what their events wrote: no hydration writes is not no
    // family writes.
    let rows = [
        journalled(&fixture, 1, TUPLES).await?,
        journalled(&fixture, 2, TUPLES).await?,
        journalled(&fixture, 3, TUPLES).await?,
        journalled(&fixture, 4, TUPLES).await?,
        journalled(&fixture, 5, TUPLES).await?,
    ];
    assert_eq!(rows, [1, 0, 1, 0, 2]);
    for index in [1, 2] {
        let row = tuple(&fixture, index).await?;
        assert_eq!(row["hydrated_name"], "block5.eth");
        assert_eq!(row["attempt_block"], 5);
        assert_eq!(row["attempt_hash"], hash(5));
    }
    assert_eq!(outcome.hydration.reverse.value_writes, 2);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_run_that_stops_on_its_budget_does_not_hydrate_the_block_it_stops_on() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_head_budget", 6).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    rpc::head(&fixture.pool, 6).await?;
    let options = options(&rpc).with_max_blocks_per_run(2);
    for stop in [2, 4] {
        let (outcome, error) = apply(&fixture, &marker(6), FamilyMode::Normal, &options).await;
        assert!(error.is_none(), "{error:?}");
        assert!(outcome.budget_exhausted);
        assert_eq!(outcome.marker, Some(marker(stop)));
        assert_eq!(outcome.hydration.passes, 0);
        assert!(
            rpc.calls().is_empty(),
            "block {stop} ended a run, not the chain"
        );
    }
    // The continuation that reaches the head hydrates there.
    let (outcome, error) = apply(&fixture, &marker(6), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(marker(6)));
    assert_eq!(rpc.calls(), vec![(hash(6), 1)]);
    assert_eq!(tuple(&fixture, 1).await?["hydrated_name"], "block6.eth");
    fixture.cleanup().await
}

#[tokio::test]
async fn a_replaced_head_reached_by_undo_and_replay_does_not_hydrate() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_head_replay", 4).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    for block in 1..=3 {
        run(&fixture, block, FamilyMode::Normal, &rpc).await?;
    }
    assert_eq!(tuple(&fixture, 1).await?["hydrated_name"], "block3.eth");
    // Block 3 is replaced. The replacement is the highest readable block, at the same number.
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 3",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    let replacement = fork_block(&fixture, 3, &hash(2)).await?;
    rpc.answer_hash(&replacement.hash, Some("replacement.eth"));
    let calls = rpc.calls();
    // One block of budget: the first run undoes block 3 and stops, the second resumes the
    // repair and replays the replacement.
    let options = options(&rpc).with_max_blocks_per_run(1);
    let (outcome, error) = apply(&fixture, &replacement, FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!((outcome.undone_blocks, outcome.blocks), (1, 0));
    assert_eq!(outcome.marker, Some(marker(2)));
    let (outcome, error) = apply(&fixture, &replacement, FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(replacement.clone()));
    assert_eq!(outcome.hydration.passes, 0);
    assert_eq!(rpc.calls(), calls, "a replayed head makes no hydration RPC");
    let restored = tuple(&fixture, 1).await?;
    assert_eq!(restored["hydrated_name"], "block2.eth");
    assert_eq!(restored["attempt_hash"], hash(2));
    // The next ordinary follow block on the new branch is a head again.
    let next = fork_block(&fixture, 4, &replacement.hash).await?;
    rpc.answer_hash(&next.hash, Some("after.eth"));
    let (_, error) = apply(&fixture, &next, FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(rpc.calls().last(), Some(&(next.hash.clone(), 1)));
    assert_eq!(tuple(&fixture, 1).await?["hydrated_name"], "after.eth");
    fixture.cleanup().await
}

#[tokio::test]
async fn a_head_replaced_after_the_run_captured_it_is_published_without_hydration() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_head_swap", 4).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    // The run to block 3 captures block 3 as the head when it starts. While it publishes block
    // 2, another block takes height 3: the trigger commits the replacement with block 2.
    let replacement = Marker {
        number: 3,
        hash: format!("0x{}03", "f".repeat(62)),
    };
    rpc.answer_hash(&replacement.hash, Some("replacement.eth"));
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION replace_head() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.current_block_number = 2 THEN
                 UPDATE chain_lineage SET canonicality_state = 'orphaned'
                 WHERE chain_id = NEW.chain_id AND block_number = 3
                   AND canonicality_state = 'canonical';
                 INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                     block_timestamp, canonicality_state)
                 VALUES (NEW.chain_id, '{}', '{}', 3, to_timestamp(1800000036), 'canonical')
                 ON CONFLICT DO NOTHING;
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER replace_head AFTER INSERT OR UPDATE ON project_family_marker
             FOR EACH ROW EXECUTE FUNCTION replace_head();",
        replacement.hash,
        hash(2)
    ))
    .execute(&fixture.pool)
    .await?;
    rpc::head(&fixture.pool, 3).await?;
    let calls = rpc.calls();
    let (outcome, _) = apply(&fixture, &marker(3), FamilyMode::Normal, &options(&rpc)).await;
    sqlx::raw_sql(
        "DROP TRIGGER replace_head ON project_family_marker; DROP FUNCTION replace_head();",
    )
    .execute(&fixture.pool)
    .await?;
    // The replacement is published at the captured head's height, with no pass and no call.
    let published: (i64, String) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM project_family_marker
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(published, (3, replacement.hash.clone()));
    assert_eq!(outcome.hydration.passes, 0);
    assert_eq!(rpc.calls(), calls);
    let unhydrated = tuple(&fixture, 1).await?;
    assert_eq!(unhydrated["hydrated_name"], "block1.eth");
    assert_eq!(unhydrated["attempt_block"], 1);
    // The next ordinary follow block on the new branch is a head again.
    let next = fork_block(&fixture, 4, &replacement.hash).await?;
    rpc.answer_hash(&next.hash, Some("after.eth"));
    let (_, error) = apply(&fixture, &next, FamilyMode::Normal, &options(&rpc)).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(tuple(&fixture, 1).await?["hydrated_name"], "after.eth");
    fixture.cleanup().await
}

#[tokio::test]
async fn a_rebuild_resumed_over_sparse_blocks_never_hydrates_its_last_block() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_head_rebuild", 7).await?;
    seed(&fixture, 1, 1).await?;
    seed(&fixture, 4, 2).await?;
    rpc::head(&fixture.pool, 6).await?;
    let options = options(&rpc)
        .with_max_blocks_per_run(1)
        .with_rebuild_ranges(RebuildRanges::Off);
    // The first run starts the rebuild; each later one resumes it in normal mode, as the Project
    // phase continues a batch that spent its budget.
    let mut mode = FamilyMode::Rebuild;
    let mut runs = 0;
    loop {
        let (outcome, error) = apply(&fixture, &marker(6), mode, &options).await;
        assert!(error.is_none(), "{error:?}");
        runs += 1;
        assert!(runs < 20, "the rebuild did not finish");
        if outcome.marker == Some(marker(6)) && !outcome.budget_exhausted {
            break;
        }
        mode = FamilyMode::Normal;
    }
    assert!(runs > 1, "the rebuild was resumed");
    assert!(
        rpc.calls().is_empty(),
        "a rebuild's last block is the readable head, and still makes no hydration RPC"
    );
    assert!(tuple(&fixture, 2).await?["hydrated_name"].is_null());
    run(&fixture, 7, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls(), vec![(hash(7), 2)]);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_restart_before_the_head_commits_retries_it_and_after_it_commits_does_not() -> Result<()>
{
    let (fixture, rpc) = fixture("family_hydration_head_restart", 1).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    rpc::head(&fixture.pool, 1).await?;
    // The run stops between its RPC read and its publication, as a process killed there does:
    // the marker generation moves under it, so the publication fence refuses the block.
    rpc.before_reply(
        &fixture.pool,
        "UPDATE project_family_marker SET sequence = sequence + 1
         WHERE chain_id = 'ethereum-mainnet'",
    );
    let (outcome, error) = apply(&fixture, &marker(1), FamilyMode::Normal, &options(&rpc)).await;
    assert!(error.is_some());
    assert_eq!(outcome.marker, Some(marker(0)));
    assert_eq!(
        (
            outcome.hydration.reverse.rpc_calls,
            outcome.hydration.reverse.value_writes
        ),
        (1, 0),
        "the attempt is counted; nothing it read is committed"
    );
    assert_eq!(journalled(&fixture, 1, TUPLES).await?, 0);
    // Restarted before the head committed: the same head is read again and published.
    let (outcome, error) = apply(&fixture, &marker(1), FamilyMode::Normal, &options(&rpc)).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(marker(1)));
    assert_eq!(rpc.calls(), vec![(hash(1), 1), (hash(1), 1)]);
    assert_eq!(tuple(&fixture, 1).await?["hydrated_name"], "block1.eth");
    // Restarted after it committed: a published head is not another hydration tick.
    let (outcome, error) = apply(&fixture, &marker(1), FamilyMode::Normal, &options(&rpc)).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!((outcome.blocks, outcome.hydration.passes), (0, 0));
    assert_eq!(rpc.calls().len(), 2);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_target_or_redo_endpoint_below_the_readable_head_does_not_hydrate() -> Result<()> {
    let (fixture, rpc) = fixture("family_hydration_head_below", 5).await?;
    run(&fixture, 0, FamilyMode::Normal, &rpc).await?;
    seed(&fixture, 1, 1).await?;
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    assert_eq!(rpc.calls(), vec![(hash(3), 1)]);
    // Blocks 4 and 5 arrive. A redo of blocks 2 to 3 replays to block 3, below the head.
    rpc::head(&fixture.pool, 5).await?;
    let redo = FamilyMode::Redo { from: 2, to: 3 };
    let (outcome, error) = apply(&fixture, &marker(3), redo, &options(&rpc)).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(marker(3)));
    assert_eq!(outcome.undone_blocks, 2);
    assert_eq!(rpc.calls().len(), 1, "a redo endpoint is not a head");
    assert!(
        tuple(&fixture, 1).await?["hydrated_name"].is_null(),
        "the redo undid block 3's read and replay made none"
    );
    // A caller's follow target one block short of the readable head is not the head either:
    // Project reads the head itself.
    let (outcome, error) = apply(&fixture, &marker(4), FamilyMode::Normal, &options(&rpc)).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.marker, Some(marker(4)));
    assert_eq!((outcome.blocks, outcome.hydration.passes), (1, 0));
    assert_eq!(rpc.calls().len(), 1);
    let (outcome, error) = apply(&fixture, &marker(5), FamilyMode::Normal, &options(&rpc)).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.hydration.passes, 1);
    assert_eq!(rpc.calls().last(), Some(&(hash(5), 1)));
    fixture.cleanup().await
}
