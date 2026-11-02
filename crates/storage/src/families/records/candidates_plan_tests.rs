//! Plan-shape test for the `resolves_to` candidates. The two pieces that join the resource
//! pointers on chain, namehash and resolver must probe
//! `project_resource_pointer_namehash_resolver_idx` with all three as its index condition.
//!
//! The fixture has the shape that makes the resolver index a poor path: almost every pointer
//! is at one resolver, as on a chain where most names use the public resolver. The resolver
//! index then finds every pointer at that resolver and filters the namehash afterwards, once
//! for each inverse-index row. Sequential scans stay enabled, so the planner picks the namehash
//! index on cost. The test also checks that without it those pieces scan the whole pointer
//! table, so the index is what the plan depends on.

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, raw_sql};
use uuid::Uuid;

use super::CANDIDATES;
use crate::families::id_index_plan_tests::{
    PLAN_MODES, Probe, explain_execute, missing_probes, with_database,
};

const CHAIN: &str = "ethereum-mainnet";
const POINTERS: i64 = 40_000;
/// The resolver almost every pointer names.
const PUBLIC_RESOLVER: &str = "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63";
const ADDRESS: &str = "0xabababababababababababababababababababab";
const INDEX: &str = "project_resource_pointer_namehash_resolver_idx";

#[tokio::test]
async fn resolves_to_candidates_probe_the_pointer_namehash_index() -> Result<()> {
    with_database("family_candidates_pointer_plan", async |connection| {
        install_fixture(connection).await?;
        check(connection).await
    })
    .await
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    // Pointer n is resource n at node n. All but every hundredth name the public resolver.
    // The address has a record at the public resolver for nodes 10, 20, ... 200, written
    // under each node's name, and other addresses have records for the other nodes.
    raw_sql(&format!(
        "INSERT INTO project_resource_pointer
             (chain_id, resource_id, block_number, event_identity, resolver_address,
              pointer_position, namespace, source_family, namehash)
         SELECT '{CHAIN}', lpad(to_hex(n), 32, '0')::uuid, n, 'pointer:' || n,
                CASE WHEN n % 100 = 0 THEN '0x' || lpad(to_hex(n), 40, '0')
                     ELSE '{PUBLIC_RESOLVER}' END,
                jsonb_build_object('event_identity', 'pointer:' || n, 'block_number', n),
                'ens', 'ens_v1_registry_l1', '0x' || lpad(to_hex(n), 64, '0')
         FROM generate_series(1, {POINTERS}) n;

         INSERT INTO project_address_record_node_index
             (address, coin_type, chain_id, resolver_address, node, logical_name_id)
         SELECT CASE WHEN n % 10 = 0 AND n <= 200 THEN '{ADDRESS}'
                     ELSE '0x' || lpad(to_hex(n), 40, '0') END,
                '60', '{CHAIN}', '{PUBLIC_RESOLVER}', '0x' || lpad(to_hex(n), 64, '0'),
                'ens:0x' || lpad(to_hex(n), 64, '0')
         FROM generate_series(1, 5000) n;

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn check(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "PREPARE candidates (text, text[], text[]) AS {CANDIDATES}"
    ))
    .execute(&mut *connection)
    .await
    .context("prepare candidates")?;
    let values = format!("'{ADDRESS}', '{{60}}', '{{ens}}'");
    // The CTE is scanned once in each piece, so the second scan's alias carries a suffix.
    let probes = [
        Probe {
            index: INDEX,
            conditions: &[
                "(chain_id = admitted",
                ".node) AND (resolver_address = admitted",
            ],
        },
        Probe {
            index: INDEX,
            conditions: &[
                "(chain_id = admitted",
                ".named_namehash) AND (resolver_address = admitted",
            ],
        },
    ];
    let mut failures = Vec::new();
    for mode in PLAN_MODES {
        let plan = explain_execute(connection, mode, "candidates", &values).await?;
        failures.extend(missing_probes(
            &format!("candidates ({mode})"),
            &plan,
            &probes,
        ));
    }
    let found: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT resource_id::text FROM ({CANDIDATES}) found ORDER BY 1"
    ))
    .bind(ADDRESS)
    .bind(vec!["60".to_owned()])
    .bind(vec!["ens".to_owned()])
    .fetch_all(&mut *connection)
    .await?;
    // Nodes 10, 20, ... 200 at the public resolver, less 100 and 200, whose pointers name
    // another resolver. Each is found by its node and by its name, and counted once.
    let expected: Vec<String> = (1..=20)
        .map(|n| n * 10)
        .filter(|n| n % 100 != 0)
        .map(|n| Uuid::from_u128(n).to_string())
        .collect();
    if found != expected {
        failures.push(format!("candidates: {found:?}, expected {expected:?}"));
    }

    // Without the index the first two pieces read the whole pointer table.
    raw_sql(&format!("BEGIN; DROP INDEX {INDEX}"))
        .execute(&mut *connection)
        .await?;
    let plan = explain_execute(connection, "force_custom_plan", "candidates", &values).await?;
    if !plan
        .iter()
        .any(|line| line.contains("Seq Scan on project_resource_pointer pointer"))
    {
        failures.push(format!(
            "candidates without {INDEX}: expected a sequential scan of the resource pointers\n{}",
            plan.join("\n")
        ));
    }
    raw_sql("ROLLBACK").execute(&mut *connection).await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
    Ok(())
}
