use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Postgres, Transaction, raw_sql};

use super::include_changed_node_record_dependents;

async fn compare(
    transaction: &mut Transaction<'_, Postgres>,
    label: &str,
) -> Result<Vec<(String, String)>> {
    let old = format!(
        "SELECT logical_name_id, resource_id::text FROM ({}) result ORDER BY 1, 2",
        include_str!("changed_node_tests/baseline.sql")
    );
    let expected: Vec<(String, String)> = sqlx::query_as(&old)
        .bind("bench")
        .fetch_all(&mut **transaction)
        .await
        .with_context(|| format!("query failed for {label}"))?;
    include_changed_node_record_dependents(transaction, "bench")
        .await
        .with_context(|| format!("candidate failed for {label}"))?;
    let actual: Vec<(String, String)> = sqlx::query_as("SELECT logical_name_id, resource_id::text FROM project_changed_node_record_dependents ORDER BY 1, 2").fetch_all(&mut **transaction).await?;
    assert_eq!(actual, expected, "full dependent pairs differ: {label}");
    let names: Vec<String> =
        sqlx::query_scalar("SELECT logical_name_id FROM project_scope_names ORDER BY 1")
            .fetch_all(&mut **transaction)
            .await?;
    let resources: Vec<String> =
        sqlx::query_scalar("SELECT resource_id::text FROM project_scope_resources ORDER BY 1")
            .fetch_all(&mut **transaction)
            .await?;
    let mut expected_names: Vec<_> = expected.iter().map(|(n, _)| n.clone()).collect();
    expected_names.sort();
    expected_names.dedup();
    let mut expected_resources: Vec<_> = expected.iter().map(|(_, r)| r.clone()).collect();
    expected_resources.sort();
    expected_resources.dedup();
    assert_eq!(names, expected_names, "name seeds differ: {label}");
    assert_eq!(
        resources, expected_resources,
        "resource seeds differ: {label}"
    );
    raw_sql("DROP TABLE project_changed_node_record_keys, project_changed_node_record_dependents")
        .execute(&mut **transaction)
        .await?;
    Ok(actual)
}

#[tokio::test]
async fn changed_node_dependents_preserve_exact_pointer_and_all_guards() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("changed_node_guards")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(include_str!("changed_node_tests/schema.sql"))
        .execute(&mut *tx)
        .await?;
    let cases = [
        (
            "unrelated_malformed_resolver_manifest",
            r#"INSERT INTO resolver_current SELECT chain_id,'0xunrelated',support_status,declared_summary,'{"manifest_id":"not-a-bigint"}' FROM resolver_current"#,
            true,
        ),
        (
            "duplicate_inventory_versions",
            "INSERT INTO record_inventory_current SELECT * FROM record_inventory_current",
            true,
        ),
        ("old_cited_pointer_not_latest", r#""#, true),
        (
            "duplicate_changes",
            r#"INSERT INTO project_changed_events SELECT * FROM project_changed_events"#,
            true,
        ),
        (
            "duplicate_surfaces",
            r#"INSERT INTO name_surfaces SELECT * FROM name_surfaces"#,
            true,
        ),
        (
            "duplicate_declarations",
            r#"INSERT INTO project_declared_resolver_addresses SELECT * FROM project_declared_resolver_addresses"#,
            true,
        ),
        (
            "record_version",
            r#"UPDATE project_changed_events SET event_kind='RecordVersionChanged'"#,
            true,
        ),
        (
            "project_changed_events_chain_id_other",
            r#"UPDATE project_changed_events SET chain_id='other'"#,
            false,
        ),
        (
            "project_changed_events_event_kind_ResolverChanged",
            r#"UPDATE project_changed_events SET event_kind='ResolverChanged'"#,
            false,
        ),
        (
            "project_changed_events_logical_name_id_name-1",
            r#"UPDATE project_changed_events SET logical_name_id='name-1'"#,
            false,
        ),
        (
            "project_changed_events_source_family_other",
            r#"UPDATE project_changed_events SET source_family='other'"#,
            false,
        ),
        (
            "name_surfaces_namehash_NODE-1",
            r#"UPDATE name_surfaces SET namehash='NODE-1'"#,
            false,
        ),
        (
            "name_surfaces_namehash_other",
            r#"UPDATE name_surfaces SET namehash='other'"#,
            false,
        ),
        (
            "name_surfaces_chain_id_other",
            r#"UPDATE name_surfaces SET chain_id='other'"#,
            false,
        ),
        (
            "name_surfaces_canonicality_state_safe",
            r#"UPDATE name_surfaces SET canonicality_state='safe'"#,
            true,
        ),
        (
            "name_surfaces_canonicality_state_finalized",
            r#"UPDATE name_surfaces SET canonicality_state='finalized'"#,
            true,
        ),
        (
            "name_surfaces_canonicality_state_orphaned",
            r#"UPDATE name_surfaces SET canonicality_state='orphaned'"#,
            false,
        ),
        (
            "normalized_events_canonicality_state_safe",
            r#"UPDATE normalized_events SET canonicality_state='safe'"#,
            true,
        ),
        (
            "normalized_events_canonicality_state_finalized",
            r#"UPDATE normalized_events SET canonicality_state='finalized'"#,
            true,
        ),
        (
            "normalized_events_canonicality_state_orphaned",
            r#"UPDATE normalized_events SET canonicality_state='orphaned'"#,
            false,
        ),
        (
            "normalized_events_chain_id_other",
            r#"UPDATE normalized_events SET chain_id='other'"#,
            false,
        ),
        (
            "normalized_events_source_family_ens_v2_root_l1",
            r#"UPDATE normalized_events SET source_family='ens_v2_root_l1'"#,
            true,
        ),
        (
            "normalized_events_source_family_ens_v1_registry_l1",
            r#"UPDATE normalized_events SET source_family='ens_v1_registry_l1'"#,
            false,
        ),
        (
            "normalized_events_namespace_other",
            r#"UPDATE normalized_events SET namespace='other'"#,
            false,
        ),
        (
            "normalized_events_event_kind_RecordChanged",
            r#"UPDATE normalized_events SET event_kind='RecordChanged'"#,
            false,
        ),
        (
            "record_inventory_current_support_status_unsupported",
            r#"UPDATE record_inventory_current SET support_status='unsupported'"#,
            false,
        ),
        (
            "resolver_current_support_status_unsupported",
            r#"UPDATE resolver_current SET support_status='unsupported'"#,
            false,
        ),
        (
            "resolver_current_chain_id_other",
            r#"UPDATE resolver_current SET chain_id='other'"#,
            false,
        ),
        (
            "resolver_current_resolver_address_0xAB",
            r#"UPDATE resolver_current SET resolver_address='0xAB'"#,
            true,
        ),
        (
            "resolver_current_resolver_address_0xff",
            r#"UPDATE resolver_current SET resolver_address='0xff'"#,
            false,
        ),
        (
            "project_declared_resolver_addresses_namespace_different",
            r#"UPDATE project_declared_resolver_addresses SET namespace='different'"#,
            false,
        ),
        (
            "project_declared_resolver_addresses_resolver_address_0xAB",
            r#"UPDATE project_declared_resolver_addresses SET resolver_address='0xAB'"#,
            false,
        ),
        (
            "null_node",
            r#"UPDATE project_changed_events SET after_state='{}'"#,
            false,
        ),
        (
            "empty_node",
            r#"UPDATE project_changed_events SET after_state='{"node":""}'"#,
            false,
        ),
        (
            "resolver_precedence",
            r#"UPDATE project_changed_events SET after_state='{"node":"node-1","resolver":"0xff"}'"#,
            false,
        ),
        (
            "resolver_empty_fallback",
            r#"UPDATE project_changed_events SET after_state='{"node":"node-1","resolver":""}'"#,
            true,
        ),
        (
            "resolver_null_fallback",
            r#"UPDATE project_changed_events SET after_state='{"node":"node-1","resolver":null}'"#,
            true,
        ),
        (
            "resolver_explicit",
            r#"UPDATE project_changed_events SET after_state='{"node":"node-1","resolver":"0xAb"}'"#,
            true,
        ),
        (
            "emitter_null",
            r#"UPDATE project_changed_events SET raw_fact_ref='{}'"#,
            false,
        ),
        (
            "emitter_empty",
            r#"UPDATE project_changed_events SET raw_fact_ref='{"emitting_address":""}'"#,
            false,
        ),
        (
            "inventory_wrong_pointer",
            r#"UPDATE record_inventory_current SET provenance='{"chain_id":"bench","resolver_pointer_event_id":2}'"#,
            false,
        ),
        (
            "inventory_missing_pointer",
            r#"UPDATE record_inventory_current SET provenance='{"chain_id":"bench"}'"#,
            false,
        ),
        (
            "inventory_other_chain",
            r#"UPDATE record_inventory_current SET provenance='{"chain_id":"other","resolver_pointer_event_id":1}'"#,
            false,
        ),
        (
            "resolver_wrong_basis",
            r#"UPDATE resolver_current SET declared_summary='{"classification":{"source_family":"ens_v1_resolver_l1","basis":"observed"}}'"#,
            false,
        ),
        (
            "resolver_missing_manifest",
            r#"UPDATE resolver_current SET provenance='{}'"#,
            false,
        ),
        (
            "removed_inventory",
            r#"DELETE FROM record_inventory_current"#,
            false,
        ),
        (
            "removed_cited_pointer",
            r#"DELETE FROM normalized_events WHERE normalized_event_id=1"#,
            false,
        ),
        (
            "null_pointer_resource",
            r#"UPDATE normalized_events SET resource_id=NULL"#,
            false,
        ),
        (
            "wrong_inventory_resource",
            r#"UPDATE record_inventory_current SET resource_id=md5('other')::uuid"#,
            false,
        ),
        (
            "null_surface_name",
            r#"UPDATE name_surfaces SET logical_name_id=NULL"#,
            false,
        ),
        (
            "v2_valid",
            r#"UPDATE resolver_current SET declared_summary='{"classification":{"source_family":"ens_v2_resolver_l1","role":"public_resolver_v2","basis":"manifest_declared_address"}}'; UPDATE project_changed_events SET source_family='ens_v2_resolver_l1',namespace='ens',source_manifest_id=1;"#,
            true,
        ),
        (
            "v2_wrong_namespace",
            r#"UPDATE resolver_current SET declared_summary='{"classification":{"source_family":"ens_v2_resolver_l1","role":"public_resolver_v2","basis":"manifest_declared_address"}}'; UPDATE project_changed_events SET source_family='ens_v2_resolver_l1',namespace='ens',source_manifest_id=1;UPDATE project_changed_events SET namespace='other'"#,
            false,
        ),
        (
            "v2_null_namespace",
            r#"UPDATE resolver_current SET declared_summary='{"classification":{"source_family":"ens_v2_resolver_l1","role":"public_resolver_v2","basis":"manifest_declared_address"}}'; UPDATE project_changed_events SET source_family='ens_v2_resolver_l1',namespace='ens',source_manifest_id=1;UPDATE project_changed_events SET namespace=NULL"#,
            false,
        ),
        (
            "v2_wrong_manifest",
            r#"UPDATE resolver_current SET declared_summary='{"classification":{"source_family":"ens_v2_resolver_l1","role":"public_resolver_v2","basis":"manifest_declared_address"}}'; UPDATE project_changed_events SET source_family='ens_v2_resolver_l1',namespace='ens',source_manifest_id=1;UPDATE project_changed_events SET source_manifest_id=2"#,
            false,
        ),
        (
            "v2_null_manifest",
            r#"UPDATE resolver_current SET declared_summary='{"classification":{"source_family":"ens_v2_resolver_l1","role":"public_resolver_v2","basis":"manifest_declared_address"}}'; UPDATE project_changed_events SET source_family='ens_v2_resolver_l1',namespace='ens',source_manifest_id=1;UPDATE project_changed_events SET source_manifest_id=NULL"#,
            false,
        ),
        (
            "v2_wrong_role",
            r#"UPDATE resolver_current SET declared_summary='{"classification":{"source_family":"ens_v2_resolver_l1","role":"public_resolver_v2","basis":"manifest_declared_address"}}'; UPDATE project_changed_events SET source_family='ens_v2_resolver_l1',namespace='ens',source_manifest_id=1;UPDATE resolver_current SET declared_summary=jsonb_set(declared_summary,'{classification,role}','"other"')"#,
            false,
        ),
    ];
    for (label, mutation, present) in cases {
        raw_sql(include_str!("changed_node_tests/seed.sql"))
            .execute(&mut *tx)
            .await?;
        if !mutation.is_empty() {
            raw_sql(mutation).execute(&mut *tx).await?;
        }
        let actual = compare(&mut tx, label).await?;
        assert_eq!(
            actual.len(),
            usize::from(present),
            "expected eligibility: {label}"
        );
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn changed_node_dependents_match_original_for_mixed_histories() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("changed_node_mixed")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(include_str!("changed_node_tests/schema.sql"))
        .execute(&mut *tx)
        .await?;
    for seed in 0..48 {
        raw_sql(include_str!("changed_node_tests/seed.sql"))
            .execute(&mut *tx)
            .await?;
        raw_sql(&format!(r#"
            TRUNCATE project_changed_events,name_surfaces,normalized_events,record_inventory_current;
            INSERT INTO normalized_events(normalized_event_id,chain_id,logical_name_id,resource_id,event_kind,source_family,namespace,canonicality_state,after_state)
            SELECT n*10+h,'bench','name-'||n,md5(n::text)::uuid,'ResolverChanged',
                   CASE WHEN (n+{seed})%7=0 THEN 'ens_v2_root_l1' ELSE 'ens_v2_registry_l1' END,
                   CASE WHEN (n+{seed})%11=0 THEN 'other' ELSE 'ens' END,
                   CASE WHEN (n+h+{seed})%13=0 THEN 'orphaned' ELSE 'canonical' END,
                   jsonb_build_object('resolver',CASE WHEN (n+h+{seed})%3=0 THEN '0xff' ELSE '0xAB' END)
            FROM generate_series(1,60) n CROSS JOIN generate_series(1,8) h;
            INSERT INTO name_surfaces(chain_id,namehash,logical_name_id,canonicality_state)
            SELECT 'bench',CASE WHEN (n+{seed})%17=0 THEN 'NODE-' ELSE 'node-' END || (n%40), 'name-'||n,
                   CASE WHEN (n+{seed})%19=0 THEN 'orphaned' ELSE 'finalized' END
            FROM generate_series(1,60) n CROSS JOIN generate_series(1,2) dup;
            INSERT INTO record_inventory_current SELECT md5(n::text)::uuid,
                jsonb_build_object('chain_id',CASE WHEN (n+{seed})%9=0 THEN 'other' ELSE 'bench' END,
                                   'resolver_pointer_event_id',n*10+1+(n+{seed})%8),
                CASE WHEN (n+{seed})%23=0 THEN 'unsupported' ELSE 'supported' END FROM generate_series(1,60) n;
            INSERT INTO project_changed_events SELECT 'bench',NULL,NULL,'ens_v1_resolver_l1',
                CASE WHEN (n+{seed})%2=0 THEN 'RecordChanged' ELSE 'RecordVersionChanged' END,
                CASE WHEN (n+{seed})%29=0 THEN 'already-linked' ELSE NULL END,
                jsonb_build_object('node','NODE-'||(n%40),'resolver',CASE WHEN (n+{seed})%5=0 THEN '0xff' ELSE '' END),
                '{{"emitting_address":"0xab"}}' FROM generate_series(1,80) n CROSS JOIN generate_series(1,3) dup;
        "#)).execute(&mut *tx).await?;
        compare(&mut tx, &format!("seed {seed}")).await?;
    }
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn changed_node_dependents_still_reject_relevant_malformed_pointer_ids() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("changed_node_bad_id")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(include_str!("changed_node_tests/schema.sql"))
        .execute(&mut *tx)
        .await?;
    raw_sql(include_str!("changed_node_tests/seed.sql"))
        .execute(&mut *tx)
        .await?;
    raw_sql("UPDATE record_inventory_current SET provenance=jsonb_set(provenance,'{resolver_pointer_event_id}','\"not-a-bigint\"'); SAVEPOINT invalid_id").execute(&mut *tx).await?;
    let old = sqlx::query(include_str!("changed_node_tests/baseline.sql"))
        .bind("bench")
        .fetch_all(&mut *tx)
        .await
        .unwrap_err();
    assert_eq!(
        old.as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("22P02")
    );
    raw_sql("ROLLBACK TO invalid_id").execute(&mut *tx).await?;
    let new = include_changed_node_record_dependents(&mut tx, "bench").await;
    assert!(
        new.is_err(),
        "a matched malformed pointer must remain a database error"
    );
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn changed_node_dependents_skip_unrelated_malformed_inventory() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("changed_node_unrelated_bad_id")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql(include_str!("changed_node_tests/unrelated_malformed.sql"))
        .execute(&mut *tx)
        .await?;
    let actual = compare(&mut tx, "unrelated malformed inventory").await?;
    assert_eq!(actual.len(), 1);
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
