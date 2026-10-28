//! Plan-shape test for `storage:families.records.names_reaching_resources`. Its registry
//! pointer arm must probe `project_registry_pointer_resource_idx` by chain and resource rather
//! than scan the chain's registry pointers.
//!
//! Sequential scans stay enabled. The fixture holds enough registry pointers that, without the
//! index, the arm is a sequential scan of the table, which the test also checks, so the index
//! is what the plan depends on. The rows are the names the candidate resources' registry
//! pointers carry, read without the statement.

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, raw_sql};
use uuid::Uuid;

use super::NAMES_REACHING_SQL;
use crate::families::id_index_plan_tests::{
    PLAN_MODES, Probe, explain_execute, missing_probes, with_database,
};

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY_POINTERS: i64 = 60_000;
/// Every 700th registry pointer's resource is a candidate, 85 in all.
const STEP: i64 = 700;
const INDEX: &str = "project_registry_pointer_resource_idx";

#[tokio::test]
async fn names_reaching_resources_probe_the_registry_pointer_resource_index() -> Result<()> {
    with_database("family_names_reaching_plan", async |connection| {
        install_fixture(connection).await?;
        check(connection).await
    })
    .await
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    // Registry pointer n is node n with resource n, written by event n. The candidates'
    // events carry the name of their node, a surface with no raw evidence.
    raw_sql(&format!(
        "INSERT INTO project_registry_pointer
             (chain_id, namespace, node, block_number, event_identity, resolver_address,
              resource_id, source_family)
         SELECT '{CHAIN}', 'ens', '0x' || lpad(to_hex(n), 64, '0'), n, 'registry:' || n,
                '0x' || lpad(to_hex(n % 50), 40, '0'), lpad(to_hex(n), 32, '0')::uuid,
                'ens_v1_registry_l1'
         FROM generate_series(1, {REGISTRY_POINTERS}) n;

         INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('{CHAIN}', 'block-1', 1, to_timestamp(12), 'canonical');

         INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT 'ens:' || namehash, 'ens', namehash, ARRAY[namehash], 'v1', 'active',
                '{CHAIN}', 'block-1', 1, 'canonical'
         FROM generate_series({STEP}, {REGISTRY_POINTERS}, {STEP}) n,
         LATERAL (SELECT '0x' || lpad(to_hex(n), 64, '0') AS namehash) surface;

         INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, derivation_kind, logical_name_id)
         SELECT 'registry:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
                '{CHAIN}', 'ens_v1_unwrapped_authority',
                'ens:0x' || lpad(to_hex(n), 64, '0')
         FROM generate_series({STEP}, {REGISTRY_POINTERS}, {STEP}) n;

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn check(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "PREPARE names_reaching (text, uuid[], text[]) AS {NAMES_REACHING_SQL}"
    ))
    .execute(&mut *connection)
    .await
    .context("prepare names_reaching")?;
    let candidates: Vec<i64> = (1..)
        .map(|k| k * STEP)
        .take_while(|n| *n <= REGISTRY_POINTERS)
        .collect();
    ensure!(candidates.len() == 85, "{} candidates", candidates.len());
    let resources: Vec<Uuid> = candidates
        .iter()
        .map(|n| Uuid::from_u128(*n as u128))
        .collect();
    let values = format!(
        "'{CHAIN}', '{{{}}}', NULL",
        resources
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    let probe = [Probe {
        index: INDEX,
        conditions: &["(chain_id = ", "(resource_id = ANY "],
    }];
    let mut failures = Vec::new();
    for mode in PLAN_MODES {
        let plan = explain_execute(connection, mode, "names_reaching", &values).await?;
        failures.extend(missing_probes(
            &format!("names_reaching ({mode})"),
            &plan,
            &probe,
        ));
    }

    let mut names: Vec<String> = sqlx::query_scalar(NAMES_REACHING_SQL)
        .bind(CHAIN)
        .bind(&resources)
        .bind(None::<Vec<String>>)
        .fetch_all(&mut *connection)
        .await?;
    names.sort();
    let mut expected: Vec<String> = candidates
        .iter()
        .map(|n| format!("ens:0x{n:064x}"))
        .collect();
    expected.sort();
    if names != expected {
        failures.push(format!(
            "names_reaching: {} names, expected {}",
            names.len(),
            expected.len()
        ));
    }

    // Without the index the arm reads every registry pointer.
    raw_sql(&format!("BEGIN; DROP INDEX {INDEX}"))
        .execute(&mut *connection)
        .await?;
    let plan = explain_execute(connection, "force_custom_plan", "names_reaching", &values).await?;
    if !plan
        .iter()
        .any(|line| line.contains("Seq Scan on project_registry_pointer"))
    {
        failures.push(format!(
            "names_reaching without {INDEX}: expected a sequential scan of the registry pointers\n{}",
            plan.join("\n")
        ));
    }
    raw_sql("ROLLBACK").execute(&mut *connection).await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
    Ok(())
}
