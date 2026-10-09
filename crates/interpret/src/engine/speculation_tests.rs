//! Speculative preparation must publish the same events, identity rows and discovery
//! results as ordered interpretation over complete blocks.
use alloy_primitives::{Address, LogData};
use alloy_sol_types::SolEvent;
use serde_json::json;

use crate::{Marker, RunMode};

#[path = "speculation_tests_support.rs"]
mod support;
use support::*;

#[path = "speculation_tests_bench.rs"]
mod benchmark;
#[path = "speculation_tests_lifecycle.rs"]
mod lifecycle;
#[path = "speculation_tests_v2.rs"]
mod v2;

#[tokio::test]
async fn independent_names_accept_prepared_batches_and_match_serial_rows() -> TestResult {
    let serial = database("interpret_spec_disjoint_serial", "mainnet").await?;
    let parallel = database("interpret_spec_disjoint_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        seed_records(pool, 8, 3, false).await?;
    }
    complete(
        &engine(serial.pool(), 1, 2),
        CHAIN,
        FIRST,
        FIRST + 7,
        RunMode::Normal,
    )
    .await?;
    let speculative = engine(parallel.pool(), 4, 2);
    complete(&speculative, CHAIN, FIRST, FIRST + 7, RunMode::Normal).await?;
    let stats = speculative.speculation_stats();
    assert!(
        stats.accepted > 0,
        "independent work was not accepted: {stats:?}"
    );
    assert_eq!(
        stats.retried, 0,
        "independent names unexpectedly conflicted: {stats:?}"
    );
    assert!(
        stats.peak_active_workers > 1,
        "workers did not overlap: {stats:?}"
    );
    assert!(
        stats.peak_active_workers <= 4,
        "worker bound exceeded: {stats:?}"
    );
    assert_eq!(stats.active_workers, 0);
    let actual = snapshot(parallel.pool(), CHAIN).await?;
    assert_eq!(
        events(&actual)
            .iter()
            .filter(|row| row["event_kind"] == "RecordChanged")
            .count(),
        72
    );
    assert_snapshots(&actual, &snapshot(serial.pool(), CHAIN).await?);
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_to_the_same_name_retry_and_keep_current_before_states() -> TestResult {
    let serial = database("interpret_spec_hot_serial", "mainnet").await?;
    let parallel = database("interpret_spec_hot_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        seed_records(pool, 8, 2, true).await?;
    }
    complete(
        &engine(serial.pool(), 1, 2),
        CHAIN,
        FIRST,
        FIRST + 7,
        RunMode::Normal,
    )
    .await?;
    let speculative = engine(parallel.pool(), 4, 2);
    complete(&speculative, CHAIN, FIRST, FIRST + 7, RunMode::Normal).await?;
    let stats = speculative.speculation_stats();
    assert!(
        stats.retried > 0,
        "the prior name records changed after preparation: {stats:?}"
    );
    let actual = snapshot(parallel.pool(), CHAIN).await?;
    let last = events(&actual)
        .into_iter()
        .find(|row| {
            row["block_number"] == FIRST + 7
                && row["after_state"]["node"] == format!("{:#x}", node("specimen0"))
                && row["after_state"]["record_key"] == "text:description"
        })
        .expect("last text record");
    assert!(
        last["before_state"]["value"]
            .as_str()
            .unwrap()
            .starts_with("Revision 6:")
    );
    assert_snapshots(&actual, &snapshot(serial.pool(), CHAIN).await?);
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_approval_tail_is_loaded_at_retirement_without_reinterpretation() -> TestResult {
    let serial = database("interpret_spec_approval_serial", "mainnet").await?;
    let parallel = database("interpret_spec_approval_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        for offset in 0..6 {
            let approval = ApprovalForAll {
                owner: OWNER.parse()?,
                operator: Address::from([0x44; 20]),
                approved: offset % 2 == 0,
            };
            seed_block(
                pool,
                CHAIN,
                offset,
                offset * 12,
                vec![(REGISTRY, approval.encode_log_data())],
            )
            .await?;
        }
    }
    complete(
        &engine(serial.pool(), 1, 1),
        CHAIN,
        FIRST,
        FIRST + 5,
        RunMode::Normal,
    )
    .await?;
    let speculative = engine(parallel.pool(), 4, 1);
    complete(&speculative, CHAIN, FIRST, FIRST + 5, RunMode::Normal).await?;
    let stats = speculative.speculation_stats();
    assert!(
        stats.accepted > 0,
        "account-only preparation was not accepted: {stats:?}"
    );
    assert_eq!(
        stats.retried, 0,
        "late stream tails need no protocol retry: {stats:?}"
    );
    let actual = snapshot(parallel.pool(), CHAIN).await?;
    let mut approvals = events(&actual)
        .into_iter()
        .filter(|row| row["event_kind"] == "AccountPermissionChanged")
        .collect::<Vec<_>>();
    approvals.sort_by_key(|row| row["block_number"].as_i64());
    assert_eq!(approvals.len(), 6);
    assert_eq!(approvals[0]["before_state"], json!({}));
    for pair in approvals.windows(2) {
        assert_eq!(pair[1]["before_state"], pair[0]["after_state"]);
    }
    assert_snapshots(&actual, &snapshot(serial.pool(), CHAIN).await?);
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn earlier_discovery_invalidates_a_future_unselected_resolver_log() -> TestResult {
    let serial = database("interpret_spec_discovery_serial", "mainnet").await?;
    let parallel = database("interpret_spec_discovery_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        let mut first = registration("discovery", START + 3 * GRACE)?;
        first.push((
            REGISTRY,
            NewResolver {
                node: node("discovery"),
                resolver: DISCOVERED.parse()?,
            }
            .encode_log_data(),
        ));
        seed_block(pool, CHAIN, 0, 0, first).await?;
        seed_block(
            pool,
            CHAIN,
            1,
            12,
            vec![(DISCOVERED, text(node("discovery"), "discovered record"))],
        )
        .await?;
        seed_block(pool, CHAIN, 2, 24, vec![]).await?;
        seed_block(pool, CHAIN, 3, 36, vec![]).await?;
    }
    complete(
        &engine(serial.pool(), 1, 1),
        CHAIN,
        FIRST,
        FIRST + 3,
        RunMode::Normal,
    )
    .await?;
    let speculative = engine(parallel.pool(), 4, 1);
    complete(&speculative, CHAIN, FIRST, FIRST + 3, RunMode::Normal).await?;
    let stats = speculative.speculation_stats();
    assert!(
        stats.retried > 0,
        "an earlier admission must invalidate the old catalog: {stats:?}"
    );
    let actual = snapshot(parallel.pool(), CHAIN).await?;
    assert!(
        events(&actual)
            .iter()
            .any(|row| row["after_state"]["value"] == "discovered record")
    );
    assert_snapshots(&actual, &snapshot(serial.pool(), CHAIN).await?);
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quiet_future_batch_reloads_newly_due_names() -> TestResult {
    let serial = database("interpret_spec_expiry_serial", "mainnet").await?;
    let parallel = database("interpret_spec_expiry_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        seed_block(pool, CHAIN, 0, 0, registration("expiry", START + 10)?).await?;
        seed_block(pool, CHAIN, 1, GRACE + 11, vec![]).await?;
        seed_block(pool, CHAIN, 2, GRACE + 23, vec![]).await?;
        seed_block(pool, CHAIN, 3, GRACE + 35, vec![]).await?;
    }
    complete(
        &engine(serial.pool(), 1, 1),
        CHAIN,
        FIRST,
        FIRST + 3,
        RunMode::Normal,
    )
    .await?;
    let speculative = engine(parallel.pool(), 4, 1);
    complete(&speculative, CHAIN, FIRST, FIRST + 3, RunMode::Normal).await?;
    let stats = speculative.speculation_stats();
    assert!(
        stats.retried > 0,
        "new due-name query results must invalidate preparation: {stats:?}"
    );
    let actual = snapshot(parallel.pool(), CHAIN).await?;
    let released = events(&actual)
        .into_iter()
        .filter(|row| row["event_kind"] == "RegistrationReleased")
        .collect::<Vec<_>>();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0]["block_number"], FIRST + 1);
    assert_snapshots(&actual, &snapshot(serial.pool(), CHAIN).await?);
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redo_replaces_stale_derived_history_before_accepting_future_work() -> TestResult {
    let database = database("interpret_spec_redo", "mainnet").await?;
    let pool = database.pool();
    seed_records(pool, 6, 2, true).await?;
    complete(
        &engine(pool, 1, 1),
        CHAIN,
        FIRST,
        FIRST + 5,
        RunMode::Normal,
    )
    .await?;
    let expected = snapshot(pool, CHAIN).await?;
    sqlx::query("UPDATE normalized_events SET after_state = jsonb_set(after_state, '{value}', '\"stale derived value\"') WHERE chain_id = $1 AND event_kind = 'RecordChanged' AND after_state ? 'value'")
        .bind(CHAIN).execute(pool).await?;
    let speculative = engine(pool, 4, 1);
    complete(&speculative, CHAIN, FIRST, FIRST + 5, RunMode::Redo).await?;
    assert!(speculative.speculation_stats().retried > 0);
    assert_snapshots(&snapshot(pool, CHAIN).await?, &expected);
    drop(speculative);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_future_batches_do_not_write_and_range_changes_drop_old_work() -> TestResult {
    let serial = database("interpret_spec_range_serial", "mainnet").await?;
    let parallel = database("interpret_spec_range_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        seed_records(pool, 8, 1, false).await?;
    }
    let speculative = engine(parallel.pool(), 4, 1);
    let first = speculative
        .run_batch(request(CHAIN, FIRST, FIRST + 7, None, RunMode::Normal))
        .await?;
    assert_eq!(first.current.number, FIRST);
    let last_written: i64 = sqlx::query_scalar("SELECT max(block_number) FROM normalized_events WHERE chain_id = $1 AND block_number IS NOT NULL")
        .bind(CHAIN).fetch_one(parallel.pool()).await?;
    assert_eq!(last_written, FIRST, "only the returned batch may write");
    let mut current = Some(first.current);
    loop {
        let outcome = speculative
            .run_batch(request(CHAIN, FIRST, FIRST + 3, current, RunMode::Normal))
            .await?;
        if outcome.complete {
            break;
        }
        current = Some(outcome.current);
    }
    complete(
        &engine(serial.pool(), 1, 1),
        CHAIN,
        FIRST,
        FIRST + 3,
        RunMode::Normal,
    )
    .await?;
    assert_snapshots(
        &snapshot(parallel.pool(), CHAIN).await?,
        &snapshot(serial.pool(), CHAIN).await?,
    );
    drop(speculative);
    tokio::task::yield_now().await;
    let last_written: i64 = sqlx::query_scalar("SELECT max(block_number) FROM normalized_events WHERE chain_id = $1 AND block_number IS NOT NULL")
        .bind(CHAIN).fetch_one(parallel.pool()).await?;
    assert_eq!(
        last_written,
        FIRST + 3,
        "abandoned work must remain read-only"
    );
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_future_decode_error_is_reported_only_when_its_batch_retires() -> TestResult {
    let serial = database("interpret_spec_error_serial", "mainnet").await?;
    let parallel = database("interpret_spec_error_parallel", "mainnet").await?;
    for pool in [serial.pool(), parallel.pool()] {
        seed_block(
            pool,
            CHAIN,
            0,
            0,
            vec![(RESOLVER, text(node("before-error"), "valid"))],
        )
        .await?;
        let malformed = LogData::new_unchecked(
            vec![
                TextChanged::SIGNATURE_HASH,
                node("malformed"),
                alloy_primitives::keccak256(b"description"),
            ],
            vec![1_u8].into(),
        );
        seed_block(pool, CHAIN, 1, 12, vec![(RESOLVER, malformed)]).await?;
        seed_block(pool, CHAIN, 2, 24, vec![]).await?;
    }
    for workers in [1, 4] {
        let pool = if workers == 1 {
            serial.pool()
        } else {
            parallel.pool()
        };
        let interpreter = engine(pool, workers, 1);
        let first = interpreter
            .run_batch(request(CHAIN, FIRST, FIRST + 2, None, RunMode::Normal))
            .await?;
        assert_eq!(first.current.number, FIRST);
        let error = interpreter
            .run_batch(request(
                CHAIN,
                FIRST,
                FIRST + 2,
                Some(Marker {
                    number: FIRST,
                    hash: block_hash(FIRST),
                }),
                RunMode::Normal,
            ))
            .await
            .expect_err("declared malformed log must fail at retirement");
        assert!(error.to_string().contains("malformed"), "{error}");
        if workers > 1 {
            assert!(interpreter.speculation_stats().fallback > 0);
        }
    }
    assert_snapshots(
        &snapshot(parallel.pool(), CHAIN).await?,
        &snapshot(serial.pool(), CHAIN).await?,
    );
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}
