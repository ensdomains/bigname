//! Database tests of how the lookahead closure reads prior events: each round asks only for
//! what the batch has not asked for yet, and what it returns is what one query over the
//! final set of names, resources and ENSv2 state keys returns.
use std::{collections::BTreeSet, num::NonZeroU32};

use sqlx::PgPool;

use super::{
    CLOSURES, Closure, ensv2_equivalence_tests,
    equivalence_tests::{self, CAPACITY, FIRST_BLOCK, History, database, database_with_manifests},
};
use crate::{BatchRequest, Engine, Marker, RunMode};

type TestResult<T = ()> = anyhow::Result<T>;

/// Loads every batch of `blocks` blocks through the lookahead loader, interpreting each with
/// the engine before loading the next, and returns each batch's closure. Each attempt's
/// restore input must equal one query over the dependencies it was loaded for.
async fn walk_closures(
    pool: &PgPool,
    chain: &'static str,
    blocks: usize,
    blocks_per_batch: u32,
) -> TestResult<Vec<(i64, Closure)>> {
    let engine = Engine::new(pool.clone())
        .with_blocks_per_batch(NonZeroU32::new(blocks_per_batch).expect("positive batch"));
    let last_block = FIRST_BLOCK + i64::try_from(blocks)? - 1;
    let mut current: Option<Marker> = None;
    let mut closures = Vec::new();
    let mut from = FIRST_BLOCK;
    while from <= last_block {
        let to = (from + i64::from(blocks_per_batch) - 1).min(last_block);
        CLOSURES.take();
        let super::Attempt::Loaded(_) =
            super::batch_input(pool, chain, from, to, None, CAPACITY, None).await?
        else {
            anyhow::bail!("{chain} manifests must choose lookahead");
        };
        let closure = CLOSURES
            .take()
            .pop()
            .expect("the batch recorded its closure");
        let mut connection = pool.acquire().await?;
        for (prior, dependencies) in &closure.attempts {
            let names: Vec<_> = dependencies
                .nodes
                .iter()
                .map(|request| format!("{}:{}", request.namespace, request.node))
                .collect();
            let resources: Vec<_> = dependencies.resource_ids.iter().copied().collect();
            let v2_keys: Vec<_> = dependencies.v2_keys.iter().cloned().collect();
            let mut once = super::super::lookahead_query::ordered_events(
                &mut connection,
                chain,
                from,
                &names,
                &resources,
                &v2_keys,
            )
            .await?;
            once.sort_by_key(|ordered| ordered.order);
            let once: Vec<_> = once.into_iter().map(|ordered| ordered.event).collect();
            assert!(
                *prior == once,
                "blocks {from}..={to}: the closure restores {} events, one query over its \
                 final set returns {}",
                prior.len(),
                once.len()
            );
        }
        drop(connection);
        closures.push((from, closure));
        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: chain.to_owned(),
                from_block: FIRST_BLOCK,
                to_block: last_block,
                resume_current: current.clone(),
                mode: RunMode::Normal,
            })
            .await?;
        assert_eq!(outcome.current.number, to);
        current = Some(outcome.current);
        from = to + 1;
    }
    CLOSURES.take();
    Ok(closures)
}

/// The closures of the twenty-label subname history, one block per batch.
async fn deep_subname_closures() -> TestResult<Vec<(i64, Closure)>> {
    let database = database("interpret_lookahead_closure").await?;
    equivalence_tests::seed_history(database.pool(), History::DeepSubname).await?;
    let closures = walk_closures(
        database.pool(),
        equivalence_tests::CHAIN,
        History::DeepSubname.offsets().len(),
        1,
    )
    .await?;
    database.cleanup().await?;
    Ok(closures)
}

/// The closures of the Sepolia ENSv1 and ENSv2 history, whose batches retry after reading
/// keys the closure could not name in advance.
async fn ensv2_closures(blocks_per_batch: u32) -> TestResult<Vec<(i64, Closure)>> {
    let database = database_with_manifests("interpret_lookahead_closure_ensv2", "sepolia").await?;
    ensv2_equivalence_tests::seed_history(database.pool(), &ensv2_equivalence_tests::OFFSETS)
        .await?;
    ensv2_equivalence_tests::stamp_interpreter_hash(database.pool()).await?;
    let closures = walk_closures(
        database.pool(),
        ensv2_equivalence_tests::CHAIN,
        ensv2_equivalence_tests::OFFSETS.len(),
        blocks_per_batch,
    )
    .await?;
    database.cleanup().await?;
    Ok(closures)
}

/// Each round's request holds only names, resources and ENSv2 state keys no earlier round or
/// attempt of the same batch asked for.
fn assert_each_element_requested_once(closures: &[(i64, Closure)]) {
    for (from, closure) in closures {
        let mut names = BTreeSet::new();
        let mut resources = BTreeSet::new();
        let mut v2_keys = BTreeSet::new();
        for (round, (round_names, round_resources, round_keys)) in
            closure.requests.iter().enumerate()
        {
            for name in round_names {
                assert!(
                    names.insert(name),
                    "batch {from} round {round} asks for name {name} again"
                );
            }
            for resource in round_resources {
                assert!(
                    resources.insert(resource),
                    "batch {from} round {round} asks for resource {resource} again"
                );
            }
            for key in round_keys {
                assert!(
                    v2_keys.insert(key),
                    "batch {from} round {round} asks for ENSv2 state key {key} again"
                );
            }
        }
    }
}

#[tokio::test]
async fn closure_requests_each_name_resource_and_key_once_per_batch() -> TestResult {
    let deep = deep_subname_closures().await?;
    assert!(
        deep.iter().any(|(_, closure)| closure.requests.len() > 20),
        "the deepest name must need a round per label"
    );
    assert_each_element_requested_once(&deep);
    for blocks_per_batch in [1, 3, 500] {
        let ensv2 = ensv2_closures(blocks_per_batch).await?;
        assert!(
            ensv2.iter().any(|(_, closure)| closure.attempts.len() > 1),
            "no batch was retried at {blocks_per_batch} blocks per batch"
        );
        assert_each_element_requested_once(&ensv2);
    }
    Ok(())
}

/// The closure's restore input is what one query over its final set returns, order
/// included, both after many rounds and after a retry that adds keys the restore read.
/// `walk_closures` checks every attempt.
#[tokio::test]
async fn delta_closure_returns_one_full_query_over_the_final_set() -> TestResult {
    let deep = deep_subname_closures().await?;
    assert!(
        deep.iter()
            .any(|(_, closure)| closure.requests.len() > 20 && !closure.attempts[0].0.is_empty()),
        "a batch must restore events after a round per label"
    );
    // A 500-block batch starts at the first block and has no history to restore.
    for blocks_per_batch in [1, 3] {
        let ensv2 = ensv2_closures(blocks_per_batch).await?;
        assert!(
            ensv2.iter().any(|(_, closure)| {
                closure.attempts.len() > 1
                    && closure
                        .attempts
                        .last()
                        .is_some_and(|(prior, _)| prior.len() > closure.attempts[0].0.len())
            }),
            "no retry restored more events at {blocks_per_batch} blocks per batch"
        );
    }
    Ok(())
}

/// A batch makes one query per round, the last of them for the elements the previous round
/// added (an empty result there is what proves they have no history), and together its
/// queries probe each element of the final set once.
#[tokio::test]
async fn events_sql_calls_per_batch() -> TestResult {
    let mut closures = deep_subname_closures().await?;
    let (_, deep) = closures
        .iter()
        .max_by_key(|(_, closure)| closure.requests.len())
        .expect("a batch");
    // The deepest name's own round, one per label above it up to `deep`, then the round
    // that reads `eth` and adds nothing.
    assert_eq!(deep.requests.len(), 22);
    closures.extend(ensv2_closures(1).await?);
    closures.extend(ensv2_closures(3).await?);
    for (from, closure) in &closures {
        let probed: usize = closure
            .requests
            .iter()
            .map(|(names, resources, keys)| names.len() + resources.len() + keys.len())
            .sum();
        let (_, last) = closure.attempts.last().expect("an attempt");
        assert_eq!(
            probed,
            last.nodes.len() + last.resource_ids.len() + last.v2_keys.len(),
            "batch {from} probes more than its final set over {} queries",
            closure.requests.len()
        );
    }
    Ok(())
}
