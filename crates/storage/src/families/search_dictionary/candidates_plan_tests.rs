//! Plan-shape test for the raw-name statement of `storage:families.name.search_candidates`. A
//! prefix search must read `name_surfaces_name_prefix_idx` as one range on the raw name, so it
//! visits only the surfaces with the prefix.
//!
//! The custom plan is checked: the prefix is a bound value, and only a plan made for the value
//! can turn the `LIKE` into a range. Sequential scans and sorts stay enabled. The fixture holds
//! enough surfaces that the planner picks the range on cost, sorting the few matched rows into
//! name order. The test also checks that without the index the `LIKE` stays a filter on the
//! rows read, as it does on any collation other than C, so the index is what the plan depends
//! on. The rows are the readable surfaces with the prefix in name order, read without the
//! statement.

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, raw_sql};

use super::RAW_SHORT;
use crate::families::id_index_plan_tests::{Probe, explain_execute, missing_probes, with_database};

const CHAIN: &str = "ethereum-mainnet";
const SURFACES: i64 = 50_000;
const INDEX: &str = "name_surfaces_name_prefix_idx";

type Candidate = (String, String, String, String);

#[tokio::test]
async fn raw_name_prefix_search_reads_the_prefix_index_range() -> Result<()> {
    with_database("family_search_prefix_plan", async |connection| {
        install_fixture(connection).await?;
        check(connection).await
    })
    .await
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    // Surface n is the name of the first ten hex digits of md5(n), on block n. Every 40th is
    // a shadow and every 70th block is orphaned.
    raw_sql(&format!(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT '{CHAIN}', 'block-' || n, n, to_timestamp(n * 12),
                (CASE WHEN n % 70 = 0 THEN 'orphaned' ELSE 'canonical' END)::canonicality_state
         FROM generate_series(1, {SURFACES}) n;

         INSERT INTO project_family_marker (chain_id, current_block_number, current_block_hash,
             state)
         VALUES ('{CHAIN}', {SURFACES}, 'block-' || {SURFACES}, 'live');

         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT 'ens:' || namehash, 'ens', label || '.eth', ARRAY[label, 'eth'], '\\x00',
                namehash, ARRAY[namehash, namehash], 'v1',
                CASE WHEN n % 40 = 0 THEN 'shadow' ELSE 'active' END,
                CASE WHEN n % 40 = 0 THEN 'invalid' END, CASE WHEN n % 40 = 0 THEN now() END,
                '{CHAIN}', 'block-' || n, n,
                (CASE WHEN n % 70 = 0 THEN 'orphaned' ELSE 'canonical' END)::canonicality_state
         FROM generate_series(1, {SURFACES}) n,
         LATERAL (
             SELECT substr(md5(n::text), 1, 10) AS label,
                    '0x' || lpad(to_hex(n), 64, '0') AS namehash
         ) surface;

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn check(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "PREPARE raw_short (text[], text, text, text, text, text, bigint, bool) AS {RAW_SHORT}"
    ))
    .execute(&mut *connection)
    .await
    .context("prepare raw_short")?;
    let mut failures = Vec::new();
    for prefix in ["abc", "a1b", "ff0"] {
        let values = format!("'{{ens}}', NULL, '{prefix}%', NULL, NULL, NULL, 201, false");
        let plan = explain_execute(connection, "force_custom_plan", "raw_short", &values).await?;
        failures.extend(missing_probes(
            &format!("raw_short {prefix}%"),
            &plan,
            &[Probe {
                index: INDEX,
                conditions: &["(raw_name ~>=~ ", "(raw_name ~<~ "],
            }],
        ));
        let found: Vec<Candidate> = sqlx::query_as(&format!("EXECUTE raw_short ({values})"))
            .persistent(false)
            .fetch_all(&mut *connection)
            .await?;
        let expected: Vec<Candidate> = sqlx::query_as(&format!(
            "SELECT surface.logical_name_id, surface.raw_name, surface.namespace,
                    surface.namehash
             FROM name_surfaces surface
             JOIN chain_lineage lineage
               ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
             WHERE surface.visibility_state = 'active'
               AND surface.canonicality_state = 'canonical'
               AND lineage.canonicality_state = 'canonical'
               AND left(surface.raw_name, 3) = '{prefix}'
             ORDER BY surface.raw_name, surface.namespace, surface.namehash"
        ))
        .fetch_all(&mut *connection)
        .await?;
        if expected.is_empty() || found != expected {
            failures.push(format!(
                "raw_short {prefix}%: {} rows, expected {}",
                found.len(),
                expected.len()
            ));
        }
    }

    // Without the index the prefix is a filter on the rows read.
    raw_sql(&format!("BEGIN; DROP INDEX {INDEX}"))
        .execute(&mut *connection)
        .await?;
    let values = "'{ens}', NULL, 'abc%', NULL, NULL, NULL, 201, false";
    let plan = explain_execute(connection, "force_custom_plan", "raw_short", values).await?;
    let ranged = plan
        .iter()
        .any(|line| line.contains("Index Cond:") && line.contains("raw_name ~>=~"));
    let filtered = plan
        .iter()
        .any(|line| line.contains("Filter:") && line.contains("raw_name ~~"));
    if ranged || !filtered {
        failures.push(format!(
            "raw_short without {INDEX}: expected the prefix as a filter\n{}",
            plan.join("\n")
        ));
    }
    raw_sql("ROLLBACK").execute(&mut *connection).await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
    Ok(())
}
