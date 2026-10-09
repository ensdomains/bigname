use std::time::Duration;

use super::support::*;
use crate::{Marker, RunMode, StateLoader};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_queued_canonical_block_reloads_its_raw_facts() -> TestResult {
    let serial = database("interpret_spec_reorg_serial", "mainnet").await?;
    let parallel = database("interpret_spec_reorg_parallel", "mainnet").await?;
    let ordered = engine(serial.pool(), 1, 1);
    let speculative = engine(parallel.pool(), 4, 1);
    for (pool, interpreter) in [(serial.pool(), &ordered), (parallel.pool(), &speculative)] {
        seed_records(pool, 2, 1, false).await?;
        let first = interpreter
            .run_batch(request(CHAIN, FIRST, FIRST + 1, None, RunMode::Normal))
            .await?;
        assert_eq!(first.current.number, FIRST);
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_number = $2",
        )
        .bind(CHAIN)
        .bind(FIRST + 1)
        .execute(pool)
        .await?;
        let replacement = format!("0x{:064x}", FIRST + 1_000_000);
        seed_block_with_hash(
            pool,
            CHAIN,
            1,
            12,
            &replacement,
            vec![(RESOLVER, text(node("replacement"), "canonical replacement"))],
        )
        .await?;
        let last = interpreter
            .run_batch(request(
                CHAIN,
                FIRST,
                FIRST + 1,
                Some(Marker {
                    number: FIRST,
                    hash: block_hash(FIRST),
                }),
                RunMode::Normal,
            ))
            .await?;
        assert!(last.complete);
        assert_eq!(last.current.hash, replacement);
    }
    assert!(speculative.speculation_stats().retried > 0);
    let actual = snapshot(parallel.pool(), CHAIN).await?;
    let final_events = events(&actual)
        .into_iter()
        .filter(|row| row["block_number"] == FIRST + 1)
        .collect::<Vec<_>>();
    assert_eq!(final_events.len(), 1);
    assert_eq!(
        final_events[0]["after_state"]["value"],
        "canonical replacement"
    );
    assert_snapshots(&actual, &snapshot(serial.pool(), CHAIN).await?);
    drop(ordered);
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forcing_full_state_uses_ordered_fallback_and_matches_rows() -> TestResult {
    let serial = database("interpret_spec_full_serial", "mainnet").await?;
    let parallel = database("interpret_spec_full_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        seed_records(pool, 4, 2, true).await?;
    }
    complete(
        &engine(serial.pool(), 1, 1),
        CHAIN,
        FIRST,
        FIRST + 3,
        RunMode::Normal,
    )
    .await?;
    let speculative = engine(parallel.pool(), 4, 1).with_full_state_loader_forced(true);
    complete(&speculative, CHAIN, FIRST, FIRST + 3, RunMode::Normal).await?;
    let stats = speculative.speculation_stats();
    assert_eq!(stats.prepared, 0);
    assert_eq!(stats.accepted, 0);
    assert_eq!(stats.fallback, 4);
    assert_eq!(stats.peak_active_workers, 0);
    assert!(matches!(
        speculative.chosen_loader(CHAIN)?,
        Some(StateLoader::FullState { .. })
    ));
    assert_snapshots(
        &snapshot(parallel.pool(), CHAIN).await?,
        &snapshot(serial.pool(), CHAIN).await?,
    );
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_preparation_window_releases_workers_without_publishing() -> TestResult {
    let serial = database("interpret_spec_cancel_serial", "mainnet").await?;
    let parallel = database("interpret_spec_cancel_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        seed_records(pool, 6, 1, false).await?;
    }
    let speculative = engine(parallel.pool(), 4, 1);
    let initial = snapshot(parallel.pool(), CHAIN).await?;
    let mut blocker = parallel.pool().begin().await?;
    sqlx::query("LOCK TABLE raw_logs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;
    let mut pending =
        Box::pin(speculative.run_batch(request(CHAIN, FIRST, FIRST + 5, None, RunMode::Normal)));
    let entered = async {
        loop {
            if speculative.speculation_stats().active_workers == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        result = &mut pending => panic!("preparation passed an exclusive raw-log lock: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(30), entered) => result?,
    }
    drop(pending);
    blocker.rollback().await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        while speculative.speculation_stats().active_workers != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_snapshots(&snapshot(parallel.pool(), CHAIN).await?, &initial);
    complete(&speculative, CHAIN, FIRST, FIRST + 5, RunMode::Normal).await?;
    complete(
        &engine(serial.pool(), 1, 1),
        CHAIN,
        FIRST,
        FIRST + 5,
        RunMode::Normal,
    )
    .await?;
    assert_eq!(speculative.speculation_stats().peak_active_workers, 4);
    assert_snapshots(
        &snapshot(parallel.pool(), CHAIN).await?,
        &snapshot(serial.pool(), CHAIN).await?,
    );
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}
