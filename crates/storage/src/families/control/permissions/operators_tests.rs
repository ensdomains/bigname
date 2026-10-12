//! The registry-binding observation lookup of a permission page: which indexes it probes, and
//! that it returns the rows of the statement it replaced.
//!
//! The tests set `plan_cache_mode`, and the shared schema setup sets `jit = off`. No `enable_*`
//! switch is set, so sequential scans stay enabled and a plan that reads a table whole is the
//! planner's own choice. The fixture has 100,000 observations, so that reading the table whole
//! costs more than the keyed lookups for the one resource of a per-resource read and for the 64
//! resources of a key batch.

use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{PgConnection, raw_sql};
use uuid::Uuid;

use super::BINDING_OBSERVATIONS_SQL;
use crate::families::id_index_plan_tests::{
    PLAN_MODES, Probe, explain_execute, missing_probes, with_database,
};

const CHAIN: &str = "ethereum-sepolia";
const OTHER_CHAIN: &str = "base-sepolia";
const ROWS: i64 = 100_000;

/// The statement before each condition became its own keyed lookup.
const BEFORE_SQL: &str = "SELECT to_jsonb(observation)
     FROM bigname_phase.project_registry_binding_observation observation
     WHERE observation.chain_id = $1
       AND (observation.target_resource_id = ANY($2::uuid[])
            OR observation.resource_id = ANY($2::uuid[])
            OR observation.observation_identity IN (
                SELECT candidate.logical_name_id
                FROM bigname_phase.project_binding_candidate candidate
                WHERE candidate.resource_id = ANY($2::uuid[])))";

#[tokio::test]
async fn binding_observations_probe_an_index_in_every_arm() -> Result<()> {
    with_database("binding_observation_plan", async |connection| {
        install_fixture(connection).await?;
        raw_sql(&format!(
            "PREPARE binding_observations (text, uuid[]) AS {BINDING_OBSERVATIONS_SQL}"
        ))
        .execute(&mut *connection)
        .await?;
        let probes = [
            Probe {
                index: "project_registry_binding_observation_target_idx",
                conditions: &["chain_id = ", "target_resource_id = ANY "],
            },
            Probe {
                index: "project_registry_binding_observation_resource_idx",
                conditions: &["chain_id = ", "resource_id = ANY "],
            },
            Probe {
                index: "project_binding_candidate_resource_idx",
                conditions: &["chain_id = ", "resource_id = ANY "],
            },
            Probe {
                index: "project_registry_binding_observation_pkey",
                conditions: &["chain_id = ", "observation_identity = "],
            },
        ];
        let one = vec![resource(20)];
        let batch: Vec<Uuid> = (1..=64).map(resource).collect();
        let mut failures = Vec::new();
        for (ids_label, ids) in [("one resource", &one), ("64 resources", &batch)] {
            let values = format!("'{CHAIN}', {}", uuid_array(ids));
            for mode in PLAN_MODES {
                let label = format!("binding_observations, {ids_label}, {mode}");
                let plan =
                    explain_execute(connection, mode, "binding_observations", &values).await?;
                // The bound ids make a custom plan's lines long. A failure needs only the node
                // and the head of its condition.
                let plan: Vec<String> = plan
                    .iter()
                    .map(|line| line.chars().take(160).collect())
                    .collect();
                if plan.iter().any(|line| line.contains("Seq Scan on ")) {
                    failures.push(format!("{label}: reads a table whole\n{}", plan.join("\n")));
                }
                failures.extend(missing_probes(&label, &plan, &probes));
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn binding_observations_return_the_rows_of_the_statement_before() -> Result<()> {
    with_database("binding_observation_rows", async |connection| {
        install_fixture(connection).await?;
        // The reader binds resources of the bound chain only (`page.rs` groups a page's
        // resources by their chain). The stray resource is the one exception here: its only
        // candidate is on the other chain, under a name only that chain observes, so a lookup
        // that did not key the candidates by the bound chain would return that chain's row.
        let page: Vec<Uuid> = (1..=200).map(resource).collect();
        let cases: [(&str, Vec<Uuid>, usize); 9] = [
            (
                "an unnamed observation's own resource",
                vec![resource(11)],
                1,
            ),
            // Reached by its resource and again through its name: one row.
            ("a named observation's own resource", vec![resource(20)], 1),
            ("a target the observation moved to", vec![moved(20)], 1),
            ("another resource bound to the name", vec![alias(10)], 1),
            ("another resource of an unobserved name", vec![alias(15)], 0),
            ("a resource with no rows", vec![resource(ROWS + 5)], 0),
            ("a candidate on the other chain only", vec![stray()], 0),
            (
                "every arm at once",
                vec![resource(11), alias(10), moved(20), resource(ROWS + 5)],
                3,
            ),
            ("a page of resources", page, 200),
        ];
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *connection)
                .await?;
            for (label, ids, expected) in &cases {
                let rows = sorted_rows(connection, BINDING_OBSERVATIONS_SQL, ids).await?;
                let before = sorted_rows(connection, BEFORE_SQL, ids).await?;
                ensure!(
                    rows == before,
                    "{mode}, {label}: rows differ from the statement before\n{rows:?}\n{before:?}"
                );
                ensure!(
                    rows.len() == *expected,
                    "{mode}, {label}: {} rows, expected {expected}",
                    rows.len()
                );
                let served = identities(connection, ids).await?;
                ensure!(
                    served.is_sorted_by(|a, b| a.as_bytes() < b.as_bytes()),
                    "{mode}, {label}: rows are not in primary-key order\n{served:?}"
                );
            }
        }
        Ok(())
    })
    .await
}

async fn sorted_rows(
    connection: &mut PgConnection,
    sql: &str,
    ids: &[Uuid],
) -> Result<Vec<String>> {
    let rows: Vec<Value> = sqlx::query_scalar(sql)
        .bind(CHAIN)
        .bind(ids)
        .fetch_all(&mut *connection)
        .await?;
    let mut rows: Vec<String> = rows.iter().map(Value::to_string).collect();
    rows.sort();
    Ok(rows)
}

/// The observation identities in the order the statement returns them.
async fn identities(connection: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<String>> {
    let rows: Vec<Value> = sqlx::query_scalar(BINDING_OBSERVATIONS_SQL)
        .bind(CHAIN)
        .bind(ids)
        .fetch_all(&mut *connection)
        .await?;
    Ok(rows
        .iter()
        .filter_map(|row| row["observation_identity"].as_str().map(str::to_owned))
        .collect())
}

/// Resource `n` of the fixture.
fn resource(n: i64) -> Uuid {
    Uuid::from_u128(n as u128)
}

/// A second resource bound to name `n`, on every fifth name.
fn alias(n: i64) -> Uuid {
    Uuid::from_u128((1_u128 << 64) | n as u128)
}

/// The resource the observation of name `n` reaches, on every tenth name.
fn moved(n: i64) -> Uuid {
    Uuid::from_u128((2_u128 << 64) | n as u128)
}

/// A resource whose only binding candidate is on the other chain.
fn stray() -> Uuid {
    Uuid::from_u128(3_u128 << 64)
}

fn uuid_array(ids: &[Uuid]) -> String {
    let ids: Vec<String> = ids.iter().map(Uuid::to_string).collect();
    format!("'{{{}}}'", ids.join(","))
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    // Observation n is of resource n. An even n is a named observation of name n, and an odd n
    // is unnamed. On every tenth name the observation reaches resource 2 * 2^64 + n instead of
    // its own. Name n is bound to resource n, and on every fifth name also to resource 2^64 + n.
    // The first hundred observations repeat on another chain, which never matches. That chain
    // also observes one name of its own, bound there to the stray resource.
    raw_sql(&format!(
        "INSERT INTO project_registry_binding_observation
             (chain_id, observation_identity, logical_name_id, resource_id, attributed_via,
              target_resource_id, block_number, event_identity, event_kind, registry_owner,
              registry_contract, provenance, applicable)
         SELECT chain,
                CASE WHEN n % 2 = 0 THEN 'ens:' || n
                     ELSE lpad(to_hex(n), 32, '0')::uuid::text END,
                CASE WHEN n % 2 = 0 THEN 'ens:' || n END,
                lpad(to_hex(n), 32, '0')::uuid,
                CASE WHEN n % 2 = 0 THEN 'name' ELSE 'own' END,
                CASE WHEN n % 10 = 0
                     THEN ('0000000000000002' || lpad(to_hex(n), 16, '0'))::uuid
                     ELSE lpad(to_hex(n), 32, '0')::uuid END,
                n, 'observation:' || n, 'AuthorityTransferred',
                '0x' || lpad(to_hex(n), 40, '0'), '0x' || lpad(to_hex(n % 7), 40, '0'),
                jsonb_build_object('name', 'name-' || n), true
         FROM generate_series(1, {ROWS}) n
         CROSS JOIN LATERAL (VALUES ('{CHAIN}'), ('{OTHER_CHAIN}')) chains (chain)
         WHERE chain = '{CHAIN}' OR n <= 100;

         INSERT INTO project_binding_candidate
             (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
              resource_id, binding_kind, canonicality_state, block_number, event_identity)
         SELECT (lpad(to_hex(copy), 16, '0') || lpad(to_hex(n), 16, '0'))::uuid, 'ens:' || n,
                'ens', '{CHAIN}', 'ens_v1',
                (lpad(to_hex(copy), 16, '0') || lpad(to_hex(n), 16, '0'))::uuid,
                'registration', 'canonical', n, 'candidate:' || copy || ':' || n
         FROM generate_series(1, {ROWS}) n
         CROSS JOIN generate_series(0, 1) copy
         WHERE copy = 0 OR n % 5 = 0;

         INSERT INTO project_registry_binding_observation
             (chain_id, observation_identity, logical_name_id, resource_id, attributed_via,
              target_resource_id, block_number, event_identity, event_kind, applicable)
         VALUES ('{OTHER_CHAIN}', 'ens:stray', 'ens:stray',
                 '00000000-0000-0004-0000-000000000000', 'name',
                 '00000000-0000-0004-0000-000000000000', 1, 'observation:stray',
                 'AuthorityTransferred', true);

         INSERT INTO project_binding_candidate
             (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
              resource_id, binding_kind, canonicality_state, block_number, event_identity)
         VALUES ('00000000-0000-0003-0000-000000000000', 'ens:stray', 'ens', '{OTHER_CHAIN}',
                 'ens_v1', '00000000-0000-0003-0000-000000000000', 'registration', 'canonical',
                 1, 'candidate:stray');

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
