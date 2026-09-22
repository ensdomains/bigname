use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Postgres, Transaction};

const OLD: &str = include_str!("declaration_precedence_previous.sql");
const CHAIN: &str = "ethereum-sepolia";

async fn rows(tx: &mut Transaction<'_, Postgres>, ctes: &str, full: bool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(&format!(
        "WITH {ctes} SELECT to_jsonb(d)::text FROM discovered d
         WHERE ($3 OR EXISTS (SELECT 1 FROM project_scope_resolvers s
                    WHERE lower(s.resolver_address)=lower(d.resolver_address)))
           AND NOT EXISTS(SELECT 1 FROM project_scope_resolver_passthrough p
                    WHERE lower(p.resolver_address)=lower(d.resolver_address))
         ORDER BY to_jsonb(d)::text COLLATE \"C\""
    ))
    .bind(CHAIN)
    .bind(10_i64)
    .bind(full)
    .fetch_all(&mut **tx)
    .await?)
}

#[tokio::test]
async fn discovery_admission_scope_preserves_multisets_and_activation_precedence() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("resolver_admission_scope")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql(include_str!("discovery_fixture.sql"))
        .execute(&mut *tx)
        .await?;
    for (count, passthrough, full) in [
        (0, false, false),
        (1, false, false),
        (40, false, false),
        (500, true, false),
        (0, true, true),
    ] {
        sqlx::raw_sql("TRUNCATE project_scope_resolvers,project_scope_resolver_passthrough")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO project_scope_resolvers SELECT upper('address-'||i) FROM generate_series(1,$1)i")
            .bind(count).execute(&mut *tx).await?;
        if passthrough {
            sqlx::query("INSERT INTO project_scope_resolver_passthrough VALUES('AdDrEsS-1')")
                .execute(&mut *tx)
                .await?;
        }
        let expected = rows(&mut tx, OLD, full).await?;
        let ctes = super::discovery_ctes(full);
        ensure!(
            rows(&mut tx, &ctes, full).await? == expected,
            "scope={count}, passthrough={passthrough}, full={full}"
        );
        let admissions: i64 = sqlx::query_scalar(&format!(
            "WITH {ctes} SELECT count(*) FROM active_discovery_admissions"
        ))
        .bind(CHAIN)
        .bind(10_i64)
        .fetch_one(&mut *tx)
        .await?;
        if !full {
            ensure!(
                admissions <= i64::from(count) * 3,
                "unrelated admissions were materialized"
            );
        }
        if count == 1 {
            ensure!(
                expected.len() == 6,
                "fixture must retain three family/namespace admissions and matching declarations"
            );
        }
    }
    // Pin every original predicate: invalid edge/address histories must remain excluded while
    // all source families and namespace-specific declarations for eligible addresses remain.
    for mutation in [
        "UPDATE discovery_edges SET deactivated_at=now() WHERE to_contract_instance_id=1",
        "UPDATE contract_instance_addresses SET deactivated_at=now() WHERE contract_instance_id=1",
        "UPDATE discovery_edges SET active_to_block_number=10 WHERE to_contract_instance_id=1",
        "UPDATE contract_instance_addresses SET active_to_block_number=10 WHERE contract_instance_id=1",
        "UPDATE discovery_edges SET active_from_block_number=11 WHERE to_contract_instance_id=1",
        "UPDATE contract_instance_addresses SET active_from_block_number=11 WHERE contract_instance_id=1",
        "UPDATE discovery_edges SET canonicality_state='orphaned' WHERE to_contract_instance_id=1",
        "UPDATE chain_lineage SET canonicality_state='orphaned'",
        "UPDATE discovery_edges SET active_from_block_hash='wrong' WHERE to_contract_instance_id=1",
        "UPDATE contract_instance_addresses SET active_from_block_hash='wrong' WHERE contract_instance_id=1",
        "UPDATE project_manifests SET namespace='different' WHERE manifest_id=1",
        "UPDATE project_manifests SET source_family='unmapped' WHERE manifest_id=1",
    ] {
        sqlx::query("SAVEPOINT variant").execute(&mut *tx).await?;
        sqlx::raw_sql("TRUNCATE project_scope_resolvers,project_scope_resolver_passthrough; INSERT INTO project_scope_resolvers VALUES('ADDRESS-1')").execute(&mut *tx).await?;
        sqlx::raw_sql(mutation).execute(&mut *tx).await?;
        ensure!(
            rows(&mut tx, &super::discovery_ctes(false), false).await?
                == rows(&mut tx, OLD, false).await?,
            "{mutation}"
        );
        sqlx::query("ROLLBACK TO SAVEPOINT variant")
            .execute(&mut *tx)
            .await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

const TABLES: &[&str] = &[
    "name_current",
    "children_current",
    "permissions_current",
    "account_permission_state_current",
    "permissions_current_resource_summary",
    "record_inventory_current",
    "resolver_current",
    "address_names_current",
    "address_records_current",
    "primary_names_current",
];
async fn outputs(tx: &mut Transaction<'_, Postgres>) -> Result<Vec<Vec<String>>> {
    let mut result = Vec::new();
    for table in TABLES {
        result.push(sqlx::query_scalar(&format!("SELECT (to_jsonb(row)-'inserted_at'-'last_recomputed_at')::text FROM {table} row ORDER BY (to_jsonb(row)-'inserted_at'-'last_recomputed_at')::text COLLATE \"C\""))
            .fetch_all(&mut **tx).await?);
    }
    Ok(result)
}

#[tokio::test]
async fn scoped_discovery_preserves_all_ten_project_outputs() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("resolver_discovery_outputs")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase,public")
        .execute(&mut *tx)
        .await?;
    for sql in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        sqlx::raw_sql(sql).execute(&mut *tx).await?;
    }
    sqlx::raw_sql(
        &include_str!("../../../tests/rebuild_performance/seed.sql")
            .replace("__NAMES__", "20")
            .replace("__CHAIN__", CHAIN),
    )
    .execute(&mut *tx)
    .await?;
    sqlx::raw_sql("INSERT INTO contract_instances(contract_instance_id,chain_id,contract_kind)
        SELECT md5('discovery-'||i)::uuid,'ethereum-sepolia','contract' FROM generate_series(1,100)i;
        INSERT INTO contract_instance_addresses(contract_instance_id,chain_id,address,source_manifest_id)
        SELECT contract_instance_id,chain_id,CASE WHEN contract_instance_id=md5('discovery-1')::uuid THEN '0x00000000000000000000000000000000000000b1' ELSE '0x'||substr(contract_instance_id::text,1,8)||repeat('1',32) END,(SELECT manifest_id FROM manifest_versions LIMIT 1) FROM contract_instances;
        INSERT INTO discovery_edges(chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,canonicality_state)
        SELECT chain_id,'resolver',contract_instance_id,contract_instance_id,'ResolverCreated','fixture',(SELECT manifest_id FROM manifest_versions LIMIT 1),'canonical' FROM contract_instances;")
        .execute(&mut *tx).await?;
    tx.commit().await?;
    let mut tx = database.pool().begin().await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase,public")
        .execute(&mut *tx)
        .await?;
    let target = crate::Marker {
        number: 300,
        hash: format!("0x{:064x}", 300),
    };
    for full in [true, false] {
        sqlx::query("SAVEPOINT mode").execute(&mut *tx).await?;
        crate::stage::prepare(&mut tx, CHAIN, &target).await?;
        crate::scope::initialize(
            &mut tx,
            CHAIN,
            &target,
            crate::scope::Window {
                previous: Some(&target),
                from_block: 1,
                to_block: 300,
                full_rebuild: full,
                retain_retracted: false,
            },
        )
        .await?;
        crate::stage::inputs(&mut tx, CHAIN, &target, full).await?;
        sqlx::query("SAVEPOINT staged").execute(&mut *tx).await?;
        let mut expected = None;
        for reference in [true, false] {
            sqlx::query("SELECT set_config('bigname.benchmark_reference',$1,true)")
                .bind(if reference { "on" } else { "off" })
                .execute(&mut *tx)
                .await?;
            crate::builders::build_all(&mut tx, CHAIN, &target, full).await?;
            crate::integrity::assert_publishable(&mut tx, CHAIN, &target).await?;
            crate::publish::swap(&mut tx, CHAIN, full).await?;
            let result = outputs(&mut tx).await?;
            ensure!(
                !result[0].is_empty() && !result[6].is_empty(),
                "fixture must publish names and resolvers"
            );
            if let Some(old) = &expected {
                ensure!(&result == old, "full={full}");
            } else {
                expected = Some(result);
            }
            sqlx::query("ROLLBACK TO SAVEPOINT staged")
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("ROLLBACK TO SAVEPOINT mode")
            .execute(&mut *tx)
            .await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
