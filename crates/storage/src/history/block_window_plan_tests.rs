//! Plan-shape test for the history timestamp window. Each bound must be one ordered probe of
//! `chain_lineage_timestamp_idx`, with the chain and the bound as its index condition and no
//! Sort. Sequential scans stay enabled. The fixture holds enough blocks that, without the
//! index, the statement reads every readable block of the chain and sorts them, which the test
//! also checks, so the index is what the plan depends on.
//!
//! Two chains share the table. Every 50th block is orphaned and its height has a readable
//! replacement, so the canonicality filter is a recheck on the probed rows.

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgConnection, Row, raw_sql};

use super::{FIRST_AT_OR_AFTER_SQL, LAST_AT_OR_BEFORE_SQL};

const CHAIN: &str = "ethereum-mainnet";
const OTHER_CHAIN: &str = "base-mainnet";
const BLOCKS: i64 = 40_000;
const INDEX: &str = "chain_lineage_timestamp_idx";
const PLAN_MODES: [&str; 2] = ["force_generic_plan", "force_custom_plan"];

#[tokio::test]
async fn timestamp_window_bounds_probe_the_timestamp_index() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("history_block_window_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install(&mut connection).await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn install(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    raw_sql(include_str!("../../schema/baseline/01_chain.sql"))
        .execute(&mut *connection)
        .await?;
    // Block n of each chain is at n * 12 seconds. The orphaned block at every 50th height
    // carries the same timestamp as its readable replacement.
    raw_sql(&format!(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT chain, chain || '-block-' || n, n, to_timestamp(n * 12), 'canonical'
         FROM generate_series(1, {BLOCKS}) n
         CROSS JOIN (VALUES ('{CHAIN}'), ('{OTHER_CHAIN}')) chains (chain);

         INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT '{CHAIN}', '{CHAIN}-orphan-' || n, n, to_timestamp(n * 12), 'orphaned'
         FROM generate_series(50, {BLOCKS}, 50) n;

         ANALYZE chain_lineage;
         SET jit = off"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn check(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "PREPARE first_at_or_after (text, timestamptz) AS {FIRST_AT_OR_AFTER_SQL};
         PREPARE last_at_or_before (text, timestamptz) AS {LAST_AT_OR_BEFORE_SQL}"
    ))
    .execute(&mut *connection)
    .await
    .context("prepare the window statements")?;
    // A bound between blocks 20,000 and 20,001, and one on block 20,000 exactly.
    let between = format!("'{CHAIN}', to_timestamp(20000 * 12 + 5)");
    let on_block = format!("'{CHAIN}', to_timestamp(20000 * 12)");
    let statements = [
        ("first_at_or_after", between.as_str(), 20_001),
        ("first_at_or_after", on_block.as_str(), 20_000),
        ("last_at_or_before", between.as_str(), 20_000),
        ("last_at_or_before", on_block.as_str(), 20_000),
    ];
    let mut failures = Vec::new();
    for (statement, values, block) in statements {
        for mode in PLAN_MODES {
            let plan = explain(connection, mode, statement, values).await?;
            let label = format!("{statement} ({values}, {mode})");
            let probed = plan.windows(2).any(|lines| {
                lines[0].contains(&format!("using {INDEX} on"))
                    && lines[1].contains("Index Cond:")
                    && lines[1].contains("(chain_id = ")
                    && lines[1].contains("(block_timestamp ")
            });
            if !probed || plan.iter().any(|line| line.contains("Sort")) {
                failures.push(format!(
                    "{label}: no ordered probe of {INDEX}\n{}",
                    plan.join("\n")
                ));
            }
            let found: i64 = sqlx::query_scalar(&format!("EXECUTE {statement} ({values})"))
                .persistent(false)
                .fetch_one(&mut *connection)
                .await
                .with_context(|| label.clone())?;
            if found != block {
                failures.push(format!("{label}: block {found}, expected {block}"));
            }
        }
    }

    // Without the index the statement sorts the chain's readable blocks.
    raw_sql(&format!("BEGIN; DROP INDEX {INDEX}"))
        .execute(&mut *connection)
        .await?;
    for statement in ["first_at_or_after", "last_at_or_before"] {
        let plan = explain(connection, "force_custom_plan", statement, &between).await?;
        if !plan.iter().any(|line| line.contains("Sort")) {
            failures.push(format!(
                "{statement} without {INDEX}: expected a sort of the chain's blocks\n{}",
                plan.join("\n")
            ));
        }
    }
    raw_sql("ROLLBACK").execute(&mut *connection).await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
    Ok(())
}

async fn explain(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    values: &str,
) -> Result<Vec<String>> {
    raw_sql(&format!(
        "SET plan_cache_mode = {mode}; EXPLAIN (COSTS OFF) EXECUTE {statement} ({values})"
    ))
    .fetch_all(&mut *connection)
    .await
    .with_context(|| format!("{statement} {mode}"))?
    .iter()
    .map(|row| row.try_get(0).map_err(Into::into))
    .collect()
}
