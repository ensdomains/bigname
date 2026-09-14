use anyhow::{Result, ensure};
use bigname_storage::{
    EffectivePermissionScope, PermissionGrantRelation,
    explain_effective_permissions_account_resource_page,
    explain_effective_permissions_account_resource_summary,
    explain_effective_permissions_by_resource_ids,
    load_effective_permissions_account_resource_page,
    load_effective_permissions_account_resource_page_count_summary,
    load_effective_permissions_by_resource_ids,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgPool, raw_sql};
use uuid::Uuid;

const CHAIN: &str = "effective-permissions-test";
const HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NAMESPACE_HASH: &str = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const ORPHAN_HASH: &str = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const OWNER: &str = "0x0000000000000000000000000000000000000a11";
const SUBJECT: &str = "0x0000000000000000000000000000000000000b22";
const REGISTRY: &str = "0x0000000000000000000000000000000000000c33";
const BASELINE: &[&str] = &[
    include_str!("../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
    include_str!("../../../schema-v2/baseline/06_projections.sql"),
    include_str!("../../../schema-v2/baseline/07_labels.sql"),
    include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
    include_str!("../../../schema-v2/baseline/09_divergence.sql"),
    include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
    include_str!("../../../schema-v2/baseline/12_project_generation_failures.sql"),
    include_str!("../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
    include_str!("../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
];

async fn fixture() -> Result<(TestDatabase, Uuid)> {
    let db = TestDatabase::create(TestDatabaseConfig::new("effective_permissions")).await?;
    let mut tx = db.pool().begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase,public")
        .execute(&mut *tx)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    let resource = Uuid::from_u128(0x60501);
    sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,1,now(),'canonical'),($1,$3,2,now(),'canonical')")
        .bind(CHAIN).bind(HASH).bind(NAMESPACE_HASH).execute(db.pool()).await?;
    sqlx::query("INSERT INTO bigname_phase.resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,$2,$3,1,'canonical')")
        .bind(resource).bind(CHAIN).bind(HASH).execute(db.pool()).await?;
    sqlx::query("INSERT INTO bigname_phase.name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ('ens:fixture','ens','fixture.eth',ARRAY['fixture','eth'],'','fixture',ARRAY['fixture','eth'],'test','active',$1,$2,2,'canonical')")
        .bind(CHAIN).bind(NAMESPACE_HASH).execute(db.pool()).await?;
    sqlx::query("INSERT INTO bigname_phase.surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ('00000000-0000-0000-0000-000000000606','ens:fixture',$1,'declared_registry_path','ens_v1',now(),$2,$3,2,'canonical')")
        .bind(resource).bind(CHAIN).bind(NAMESPACE_HASH).execute(db.pool()).await?;
    sqlx::query(
        r#"INSERT INTO bigname_phase.permissions_current_resource_summary (
        resource_id,authority_kind,registry_owner,registry_contract,
        registry_binding_provenance,registry_binding_chain_positions,
        support_status,unsupported_reason,provenance,chain_positions,
        canonicality_summary,manifest_version)
        VALUES ($1,'registrar',$2,$3,jsonb_build_object('chain_id',$4::text),
        jsonb_build_object('block_hash',$5::text),'unsupported',
        'operator_approval_surfaces_not_ingested',jsonb_build_object('chain_id',$4::text),
        jsonb_build_object('target_block_hash',$5::text),'{"state":"canonical_lineage"}',1)"#,
    )
    .bind(resource)
    .bind(OWNER)
    .bind(REGISTRY)
    .bind(CHAIN)
    .bind(HASH)
    .execute(db.pool())
    .await?;
    sqlx::query(
        r#"INSERT INTO bigname_phase.account_permission_state_current (
        chain_id,authority_kind,authority_contract,authority_contract_instance_id,
        owner,subject,relation_kind,approved,effective_powers,grant_source,
        inheritance_path,transfer_behavior,provenance,chain_positions,
        canonicality_summary,manifest_version)
        VALUES ($1,'registry',$2,'00000000-0000-0000-0000-000000000605',$3,$4,
        'operator',true,'["registry_control"]','{"kind":"event"}','[]','{}',
        jsonb_build_object('chain_id',$1::text),jsonb_build_object('target_block_hash',$5::text),
        '{"state":"canonical"}',1)"#,
    )
    .bind(CHAIN)
    .bind(REGISTRY)
    .bind(OWNER)
    .bind(SUBJECT)
    .bind(HASH)
    .execute(db.pool())
    .await?;
    Ok((db, resource))
}

async fn operator_count(pool: &PgPool, resource: Uuid) -> Result<usize> {
    Ok(load_effective_permissions_account_resource_page(
        pool,
        Some(SUBJECT),
        Some(resource),
        None,
        None,
        10,
    )
    .await?
    .rows
    .into_iter()
    .filter(|row| row.grant_relation == Some(PermissionGrantRelation::Operator))
    .count())
}

async fn namespaced_count(pool: &PgPool, resource: Uuid) -> Result<usize> {
    Ok(load_effective_permissions_account_resource_page(
        pool,
        Some(SUBJECT),
        Some(resource),
        Some("ens"),
        None,
        10,
    )
    .await?
    .rows
    .len())
}

#[tokio::test]
async fn effective_permissions_require_matching_chain_contract_and_owner() -> Result<()> {
    for column in ["registry_owner", "registry_contract"] {
        let (db, resource) = fixture().await?;
        assert_eq!(operator_count(db.pool(), resource).await?, 1);
        sqlx::query(&format!("UPDATE bigname_phase.permissions_current_resource_summary SET {column}='0x0000000000000000000000000000000000000d44' WHERE resource_id=$1"))
            .bind(resource).execute(db.pool()).await?;
        assert_eq!(operator_count(db.pool(), resource).await?, 0, "{column}");
        db.cleanup().await?;
    }
    let (db, resource) = fixture().await?;
    sqlx::query("UPDATE bigname_phase.account_permission_state_current SET chain_id='other-chain'")
        .execute(db.pool())
        .await?;
    assert_eq!(operator_count(db.pool(), resource).await?, 0, "chain_id");
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn effective_permissions_serve_only_approved_operator_rows() -> Result<()> {
    let (db, resource) = fixture().await?;
    sqlx::query("UPDATE bigname_phase.account_permission_state_current SET approved=false,effective_powers='[]',revocation_source='{}'")
        .execute(db.pool()).await?;
    assert_eq!(operator_count(db.pool(), resource).await?, 0);
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_do_not_cross_registry_generations() -> Result<()> {
    let (db, resource) = fixture().await?;
    sqlx::query("UPDATE bigname_phase.permissions_current_resource_summary SET registry_contract='0x0000000000000000000000000000000000000d44'")
        .execute(db.pool()).await?;
    assert_eq!(operator_count(db.pool(), resource).await?, 0);
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_require_a_current_registry_owner_binding() -> Result<()> {
    let (db, resource) = fixture().await?;
    sqlx::query("UPDATE bigname_phase.permissions_current_resource_summary SET registry_owner=NULL,registry_contract=NULL,registry_binding_provenance=NULL,registry_binding_chain_positions=NULL")
        .execute(db.pool()).await?;
    assert_eq!(operator_count(db.pool(), resource).await?, 0);
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_fail_closed_for_orphaned_account_and_binding_lineage() -> Result<()>
{
    for evidence in ["account", "binding"] {
        let (db, resource) = fixture().await?;
        let orphan = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,2,now(),'orphaned')")
            .bind(CHAIN).bind(orphan).execute(db.pool()).await?;
        let sql = if evidence == "account" {
            "UPDATE bigname_phase.account_permission_state_current SET chain_positions=jsonb_build_object('target_block_hash',$1::text)"
        } else {
            "UPDATE bigname_phase.permissions_current_resource_summary SET registry_binding_chain_positions=jsonb_build_object('block_hash',$1::text)"
        };
        sqlx::query(sql).bind(orphan).execute(db.pool()).await?;
        assert_eq!(operator_count(db.pool(), resource).await?, 0, "{evidence}");
        db.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn effective_permissions_namespace_filter_rejects_orphaned_identity_lineage() -> Result<()> {
    let (db, resource) = fixture().await?;
    assert_eq!(namespaced_count(db.pool(), resource).await?, 1);
    sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,3,now(),'orphaned')")
        .bind(CHAIN).bind(ORPHAN_HASH).execute(db.pool()).await?;
    sqlx::query("WITH surface AS (UPDATE bigname_phase.name_surfaces SET block_hash=$1,block_number=3 WHERE logical_name_id='ens:fixture') UPDATE bigname_phase.surface_bindings SET block_hash=$1,block_number=3 WHERE logical_name_id='ens:fixture'")
        .bind(ORPHAN_HASH).execute(db.pool()).await?;
    assert_eq!(namespaced_count(db.pool(), resource).await?, 0);
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_page_direct_and_operator_rows_without_gaps() -> Result<()> {
    let (db, resource) = fixture().await?;
    sqlx::query(r#"INSERT INTO bigname_phase.permissions_current (resource_id,subject,scope,scope_kind,effective_powers,grant_source,provenance,chain_positions,canonicality_summary,manifest_version)
        VALUES ($1,$2,'registry','registry','["set_resolver"]','{}',jsonb_build_object('chain_id',$3::text),jsonb_build_object('target_block_hash',$4::text),'{"state":"canonical"}',1)"#)
        .bind(resource).bind(SUBJECT).bind(CHAIN).bind(HASH).execute(db.pool()).await?;
    let first = load_effective_permissions_account_resource_page(
        db.pool(),
        Some(SUBJECT),
        Some(resource),
        None,
        None,
        1,
    )
    .await?;
    assert!(
        first.summary.is_none(),
        "a page read must not run a whole-relation count or aggregate"
    );
    let second = load_effective_permissions_account_resource_page(
        db.pool(),
        Some(SUBJECT),
        Some(resource),
        None,
        first.next_cursor.as_ref(),
        1,
    )
    .await?;
    ensure!(
        first.rows.len() == 1
            && second.rows.len() == 1
            && first.rows[0].scope != second.rows[0].scope,
        "direct/operator boundary duplicated or omitted a row"
    );
    sqlx::query("UPDATE bigname_phase.permissions_current SET scope='owner'")
        .execute(db.pool())
        .await?;
    let error = load_effective_permissions_by_resource_ids(db.pool(), &[resource], None)
        .await
        .expect_err("effective reads must reject a mismatched direct scope key");
    ensure!(format!("{error:#}").contains("scope mismatch"));
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_summary_uses_the_same_relation() -> Result<()> {
    let (db, resource) = fixture().await?;
    let page = load_effective_permissions_account_resource_page_count_summary(
        db.pool(),
        Some(SUBJECT),
        Some(resource),
        None,
        None,
        10,
    )
    .await?;
    assert_eq!(page.summary.expect("count summary").row_count, 1);
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_resource_batch_uses_one_read() -> Result<()> {
    let (db, resource) = fixture().await?;
    let rows = load_effective_permissions_by_resource_ids(db.pool(), &[resource], None).await?;
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].scope,
        EffectivePermissionScope::Account { .. }
    ));
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_require_an_account_or_resource_anchor() -> Result<()> {
    let (db, _) = fixture().await?;
    let error =
        load_effective_permissions_account_resource_page(db.pool(), None, None, None, None, 10)
            .await
            .expect_err("an unanchored effective permission scan must be rejected");
    assert!(format!("{error:#}").contains("subject or resource_id"));
    db.cleanup().await
}

async fn assert_plan(plan: Value, indexes: &[&str]) -> Result<()> {
    let text = serde_json::to_string(&plan)?;
    ensure!(
        !text.contains("Seq Scan") || !text.contains("account_permission_state_current"),
        "account-state sequential scan: {text}"
    );
    ensure!(
        !text.contains("Seq Scan") || !text.contains("permissions_current_resource_summary"),
        "resource-summary sequential scan: {text}"
    );
    for index in indexes {
        ensure!(text.contains(index), "missing index {index}: {text}");
    }
    Ok(())
}

async fn assert_page_plan(plan: Value, indexes: &[&str]) -> Result<()> {
    let text = serde_json::to_string(&plan)?;
    ensure!(
        !text.contains("\"Node Type\":\"Aggregate\""),
        "page plan performed a whole-relation aggregate: {text}"
    );
    assert_plan(plan, indexes).await
}

#[tokio::test]
async fn effective_permissions_address_page_uses_active_subject_and_binding_indexes() -> Result<()>
{
    let (db, _) = fixture().await?;
    assert_page_plan(
        explain_effective_permissions_account_resource_page(
            db.pool(),
            Some(SUBJECT),
            None,
            Some("ens"),
            None,
            2,
            true,
        )
        .await?,
        &[
            "account_permission_state_current_active_subject_idx",
            "permissions_current_resource_registry_binding_idx",
            "surface_bindings_resource_idx",
        ],
    )
    .await?;
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_resource_page_uses_applicability_index() -> Result<()> {
    let (db, resource) = fixture().await?;
    assert_page_plan(
        explain_effective_permissions_account_resource_page(
            db.pool(),
            None,
            Some(resource),
            None,
            None,
            2,
            true,
        )
        .await?,
        &["account_permission_state_current_applicability_idx"],
    )
    .await?;
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_summary_uses_the_anchored_effective_plan() -> Result<()> {
    let (db, _) = fixture().await?;
    assert_plan(
        explain_effective_permissions_account_resource_summary(
            db.pool(),
            Some(SUBJECT),
            None,
            Some("ens"),
        )
        .await?,
        &[
            "account_permission_state_current_active_subject_idx",
            "permissions_current_resource_registry_binding_idx",
            "surface_bindings_resource_idx",
        ],
    )
    .await?;
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_resource_batch_uses_applicability_index() -> Result<()> {
    let (db, resource) = fixture().await?;
    assert_plan(
        explain_effective_permissions_by_resource_ids(db.pool(), &[resource], Some("ens")).await?,
        &[
            "account_permission_state_current_applicability_idx",
            "name_surfaces_visibility_idx",
            "surface_bindings_no_overlap",
        ],
    )
    .await?;
    db.cleanup().await
}

// Clone valid baseline rows, changing keys explicitly; never bypass schema checks.
async fn page_fixture(grants: i32, per_grant: i32) -> Result<(TestDatabase, Vec<Uuid>)> {
    let (db, original) = fixture().await?;
    let pool = db.pool();
    sqlx::query("CREATE TABLE bigname_phase.page_seed AS SELECT g, n, md5(g::text||':'||n::text)::uuid AS id, '0x'||lpad(to_hex(g),40,'0') AS owner FROM generate_series(1,$1) g CROSS JOIN generate_series(1,$2) n")
        .bind(grants).bind(per_grant).execute(pool).await?;
    for (table, patch) in [
        ("resources", "jsonb_build_object('resource_id',s.id)"),
        (
            "name_surfaces",
            "jsonb_build_object('logical_name_id','ens:'||s.id,'namehash',s.id::text,'raw_name',s.id::text||'.eth')",
        ),
        (
            "surface_bindings",
            "jsonb_build_object('surface_binding_id',s.id,'logical_name_id','ens:'||s.id,'resource_id',s.id)",
        ),
        (
            "permissions_current_resource_summary",
            "jsonb_build_object('resource_id',s.id,'registry_owner',s.owner)",
        ),
    ] {
        let query = format!(
            "INSERT INTO bigname_phase.{table} SELECT (jsonb_populate_record(NULL::bigname_phase.{table},to_jsonb(t)||{patch})).* FROM bigname_phase.{table} t CROSS JOIN bigname_phase.page_seed s"
        );
        sqlx::query(&query).execute(pool).await?;
    }
    sqlx::query("INSERT INTO bigname_phase.account_permission_state_current SELECT (jsonb_populate_record(NULL::bigname_phase.account_permission_state_current,to_jsonb(t)||jsonb_build_object('owner',s.owner))).* FROM bigname_phase.account_permission_state_current t CROSS JOIN (SELECT DISTINCT owner FROM bigname_phase.page_seed) s")
        .execute(pool).await?;
    let mut ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM bigname_phase.page_seed ORDER BY id")
            .fetch_all(pool)
            .await?;
    ids.push(original);
    Ok((db, ids))
}

async fn assert_page_equivalence(
    pool: &PgPool,
    ids: &[Uuid],
    namespace: Option<&str>,
) -> Result<()> {
    let mut expected = load_effective_permissions_by_resource_ids(pool, ids, namespace).await?;
    expected.sort_by_key(|row| {
        (
            row.subject.clone(),
            row.resource_id,
            row.scope.storage_key(),
        )
    });
    for size in [1_u64, 2, 25] {
        let mut cursor = None;
        let mut seen = Vec::new();
        loop {
            let page = load_effective_permissions_account_resource_page(
                pool,
                Some(SUBJECT),
                None,
                namespace,
                cursor.as_ref(),
                size,
            )
            .await?;
            let end = (seen.len() + size as usize).min(expected.len());
            assert_eq!(page.rows, expected[seen.len()..end]);
            assert!(page.summary.is_none());
            let next = (end < expected.len()).then(|| {
                bigname_storage::PermissionsCurrentAccountResourceCursor::from(&expected[end - 1])
            });
            assert_eq!(page.next_cursor, next);
            seen.extend(page.rows);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, expected);
        if let Some(last) = expected.last() {
            let terminal = bigname_storage::PermissionsCurrentAccountResourceCursor::from(last);
            assert!(
                load_effective_permissions_account_resource_page(
                    pool,
                    Some(SUBJECT),
                    None,
                    namespace,
                    Some(&terminal),
                    size,
                )
                .await?
                .rows
                .is_empty()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn effective_permissions_grant_pages_match_unlimited_relation() -> Result<()> {
    let (db, ids) = page_fixture(4, 12).await?;
    sqlx::query("INSERT INTO bigname_phase.permissions_current(resource_id,subject,scope,scope_kind,effective_powers,grant_source,provenance,chain_positions,canonicality_summary,manifest_version) SELECT id,$1,'registry','registry','[\"set_resolver\"]','{}',jsonb_build_object('chain_id',$2::text),jsonb_build_object('target_block_hash',$3::text),'{\"state\":\"canonical\"}',1 FROM bigname_phase.page_seed WHERE n%3=0")
        .bind(SUBJECT).bind(CHAIN).bind(HASH).execute(db.pool()).await?;
    assert_page_equivalence(db.pool(), &ids, None).await?;
    assert_page_equivalence(db.pool(), &ids, Some("ens")).await?;
    // Sparse matches must be filtered before the grant-local limit.
    sqlx::query("UPDATE bigname_phase.name_surfaces SET canonicality_state='orphaned' WHERE logical_name_id IN (SELECT 'ens:'||id FROM bigname_phase.page_seed WHERE n%5<>0)")
        .execute(db.pool()).await?;
    assert_page_equivalence(db.pool(), &ids, Some("ens")).await?;
    sqlx::query("UPDATE bigname_phase.account_permission_state_current SET approved=false,effective_powers='[]' WHERE owner=(SELECT owner FROM bigname_phase.page_seed WHERE g=2 LIMIT 1)")
        .execute(db.pool()).await?;
    sqlx::query("UPDATE bigname_phase.permissions_current_resource_summary SET registry_binding_chain_positions=jsonb_build_object('block_hash',$1::text) WHERE resource_id IN (SELECT id FROM bigname_phase.page_seed WHERE n%2=0)")
        .bind(ORPHAN_HASH).execute(db.pool()).await?;
    assert_page_equivalence(db.pool(), &ids, None).await?;
    assert_page_equivalence(db.pool(), &ids, Some("ens")).await?;
    db.cleanup().await
}

fn plan_nodes<'a>(node: &'a Value, out: &mut Vec<&'a Value>) {
    out.push(node);
    if let Some(children) = node.get("Plans").and_then(Value::as_array) {
        for child in children {
            plan_nodes(child, out);
        }
    }
}

#[tokio::test]
async fn effective_permissions_default_plan_bounds_dense_payload_lookups() -> Result<()> {
    let (db, _) = page_fixture(4, 6000).await?;
    sqlx::query("ANALYZE").execute(db.pool()).await?;
    let plan = explain_effective_permissions_account_resource_page(
        db.pool(),
        Some(SUBJECT),
        None,
        None,
        None,
        25,
        false,
    )
    .await?;
    eprintln!("861_FULL_ROOT_PLAN dense-grants {plan}");
    let mut nodes = Vec::new();
    plan_nodes(&plan[0]["Plan"], &mut nodes);
    assert!(
        nodes.iter().any(|node| {
            let mut descendants = Vec::new();
            plan_nodes(node, &mut descendants);
            node["Node Type"] == "Limit"
                && node["Actual Loops"].as_u64().unwrap_or(0) >= 4
                && node["Actual Rows"].as_u64().unwrap_or(u64::MAX) <= 26
                && descendants.iter().any(|child| child["Alias"] == "k")
                && descendants.iter().any(|child| child["Alias"] == "summary")
        }),
        "missing grant-local limit over ordered keys and eligible payloads"
    );
    // Limited-beta acceptance bounds payload probes for this dense fixture only.
    // Four generated grants plus the original grant may each return K = 25 + 1.
    // The retained pre-acceptance plan made 105 probes, below this derived 130 cap.
    // Owner-key scans/sorts are explicitly permitted here; #861 remains open,
    // and sparse eligibility can require more than K payload probes per grant.
    let grant_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bigname_phase.account_permission_state_current")
            .fetch_one(db.pool())
            .await?;
    let max_payload_probes = grant_count as u64 * 26;
    let payload_scans: Vec<_> = nodes
        .iter()
        .filter(|node| {
            node["Alias"] == "summary"
                && node["Relation Name"] == "permissions_current_resource_summary"
        })
        .collect();
    assert!(!payload_scans.is_empty(), "missing payload scan");
    let mut payload_probes = 0;
    for node in payload_scans {
        assert_eq!(node["Node Type"], "Index Scan", "payload lookup: {node}");
        assert_eq!(
            node["Index Name"], "permissions_current_resource_summary_pkey",
            "payload must use resource primary key: {node}"
        );
        payload_probes += node["Actual Loops"].as_u64().expect("payload scan loops");
    }
    assert!(
        payload_probes > 0 && payload_probes <= max_payload_probes,
        "dense payload probes {payload_probes} exceed per-grant K bound {max_payload_probes}"
    );
    db.cleanup().await
}

#[tokio::test]
async fn effective_permissions_resource_plan_ignores_unrelated_grants() -> Result<()> {
    let (db, resource) = fixture().await?;
    sqlx::query("INSERT INTO bigname_phase.account_permission_state_current SELECT (jsonb_populate_record(NULL::bigname_phase.account_permission_state_current,to_jsonb(t)||jsonb_build_object('owner','0x'||lpad(to_hex(n),40,'0')))).* FROM bigname_phase.account_permission_state_current t CROSS JOIN generate_series(10000,10999) n")
        .execute(db.pool()).await?;
    sqlx::query("ANALYZE").execute(db.pool()).await?;
    let plan = explain_effective_permissions_account_resource_page(
        db.pool(),
        None,
        Some(resource),
        None,
        None,
        25,
        false,
    )
    .await?;
    eprintln!("861_FULL_ROOT_PLAN resource-control {plan}");
    let mut nodes = Vec::new();
    plan_nodes(&plan[0]["Plan"], &mut nodes);
    for node in nodes
        .iter()
        .filter(|n| n["Relation Name"] == "account_permission_state_current")
    {
        let rows = node["Actual Rows"].as_u64().unwrap_or(0)
            + node["Rows Removed by Filter"].as_u64().unwrap_or(0);
        assert!(
            rows * node["Actual Loops"].as_u64().unwrap_or(1) < 10,
            "resource lookup visited unrelated grants: {node}"
        );
    }
    let page = load_effective_permissions_account_resource_page(
        db.pool(),
        None,
        Some(resource),
        None,
        None,
        25,
    )
    .await?;
    assert_eq!(
        page.rows,
        load_effective_permissions_by_resource_ids(db.pool(), &[resource], None).await?
    );
    db.cleanup().await
}
