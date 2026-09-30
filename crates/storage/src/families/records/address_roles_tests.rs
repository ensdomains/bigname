//! A registration with many role holders: the role load of one address reads that address's
//! grants only, by an index probe on the subject, and returns its one holder row. An untrusted
//! subregistry can grant roles on one registration to any number of accounts.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgConnection, Row, raw_sql};

use super::{ROLE_GRANTS_SQL, RoleHolderLoad, role_holders};

const CHAIN: &str = "ethereum-sepolia";
const HOLDERS: i64 = 5_000;
const RESOURCE: &str = "00000000-0000-0000-0000-00000000b200";
const OTHER_RESOURCE: &str = "00000000-0000-0000-0000-00000000b201";
const TARGET: &str = "0x0000000000000000000000000000000000a11ce5";

#[tokio::test]
async fn one_address_reads_only_its_own_grants_among_many_holders() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("address_role_holders").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install(&mut connection).await?;
        check_rows(&mut connection).await?;
        check_plan(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn check_rows(connection: &mut PgConnection) -> Result<()> {
    let resources = vec![RESOURCE.to_owned(), OTHER_RESOURCE.to_owned()];
    // The requested address in mixed case: subjects are stored lowercase.
    let requested = TARGET.to_ascii_uppercase().replace("0X", "0x");
    let holders = role_holders(
        connection,
        CHAIN,
        &resources,
        &BTreeMap::new(),
        0,
        RoleHolderLoad::Subject(&requested),
    )
    .await?;
    let found: Vec<(String, String, i64)> = holders
        .iter()
        .flat_map(|(resource, holders)| {
            holders.iter().map(|holder| {
                (
                    resource.clone(),
                    holder.subject.clone(),
                    holder.position.block_number,
                )
            })
        })
        .collect();
    // The target's grant on RESOURCE; its revoked grant on OTHER_RESOURCE adds nothing.
    ensure!(
        found == [(RESOURCE.to_owned(), TARGET.to_owned(), 7)],
        "role holders of {TARGET}: {found:?}"
    );
    let skipped = role_holders(
        connection,
        CHAIN,
        &resources,
        &BTreeMap::new(),
        0,
        RoleHolderLoad::Skip,
    )
    .await?;
    ensure!(skipped.is_empty(), "a skipped load read {skipped:?}");
    Ok(())
}

async fn check_plan(connection: &mut PgConnection) -> Result<()> {
    // Prepared with the types the loader binds; sequential scans stay enabled. Under either
    // plan the grant table is probed by an index condition on the subject, never scanned.
    raw_sql(&format!(
        "PREPARE role_grants (text, text[], text) AS {ROLE_GRANTS_SQL}"
    ))
    .execute(&mut *connection)
    .await
    .context("prepare role_grants")?;
    let values = format!("'{CHAIN}', '{{{RESOURCE},{OTHER_RESOURCE}}}', '{TARGET}'");
    for mode in ["force_generic_plan", "force_custom_plan"] {
        let plan: Vec<String> = raw_sql(&format!(
            "SET plan_cache_mode = {mode}; EXPLAIN (COSTS OFF) EXECUTE role_grants ({values})"
        ))
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("explain role_grants {mode}"))?
        .iter()
        .map(|row| row.try_get(0))
        .collect::<Result<_, _>>()?;
        let probes_subject = plan.windows(2).any(|lines| {
            lines[0].contains("Index")
                && lines[0].contains("project_grant")
                && lines[1].contains("Index Cond:")
                && lines[1].contains("subject = ")
        });
        ensure!(
            probes_subject && !plan.iter().any(|line| line.contains("Seq Scan")),
            "role_grants ({mode}) does not probe the subject:\n{}",
            plan.join("\n")
        );
    }
    Ok(())
}

async fn install(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../schema/baseline/01_chain.sql"),
        include_str!("../../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../../schema/baseline/03_identity.sql"),
        include_str!("../../../schema/baseline/04_manifests.sql"),
        include_str!("../../../schema/baseline/05_normalized_events.sql"),
        include_str!("../../../schema/baseline/06_projections.sql"),
    ] {
        raw_sql(baseline).execute(&mut *connection).await?;
    }
    // HOLDERS other accounts hold a role on RESOURCE and on OTHER_RESOURCE; the target holds
    // one on RESOURCE, a revoked one on OTHER_RESOURCE, and a root-scope one on RESOURCE.
    raw_sql(&format!(
        "SET jit = off;
         INSERT INTO project_grant
             (chain_id, resource_id, subject, scope, block_number, transaction_index,
              log_index, event_identity, event_kind, scope_kind, effective_powers, revoked)
         SELECT '{CHAIN}', resource::uuid, '0x' || lpad(to_hex(n), 40, '0'), 'registry:0xb2e',
                n, 0, 0, 'grant:' || resource || ':' || n, 'PermissionChanged', 'registry',
                '[\"renew\"]', false
         FROM generate_series(1, {HOLDERS}) n
         CROSS JOIN (VALUES ('{RESOURCE}'), ('{OTHER_RESOURCE}')) resources (resource);

         INSERT INTO project_grant
             (chain_id, resource_id, subject, scope, block_number, transaction_index,
              log_index, event_identity, event_kind, scope_kind, effective_powers, revoked)
         VALUES
             ('{CHAIN}', '{RESOURCE}', '{TARGET}', 'registry:0xb2e', 7, 0, 0, 'target',
              'PermissionChanged', 'registry', '[\"set_resolver\"]', false),
             ('{CHAIN}', '{OTHER_RESOURCE}', '{TARGET}', 'registry:0xb2e', 8, 0, 0,
              'target-revoked', 'PermissionChanged', 'registry', '[]', true),
             ('{CHAIN}', '{RESOURCE}', '{TARGET}', 'root:0xb2e', 9, 0, 0, 'target-root',
              'RootPermissionChanged', 'root', '[\"set_resolver\"]', false);

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
