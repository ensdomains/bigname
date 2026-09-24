use super::*;

const REGISTRY: &str = "0x0000000000000000000000000000000000000822";
const OWNER: &str = "0x0000000000000000000000000000000000000823";
const PROFILE: &str = "ens_v2_sepolia_post_audit";

async fn manifest(pool: &PgPool, family: &str, profile: &str, contracts: Value) -> Result<i64> {
    let payload = json!({"deployment_epoch":profile,"contracts":contracts,
        "capability_flags":{"exact_name_profile":{"status":"supported"}}});
    let id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens',$1,$2,$3,'active','ensip15',$1,$4) RETURNING manifest_id")
        .bind(family).bind(CHAIN).bind(profile).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','SourceManifestUpdated',$1,1,$2,$3,'manifest_sync','canonical',$4)")
        .bind(family).bind(id).bind(CHAIN).bind(json!({"rollout_status":"active","manifest_payload":payload})).execute(pool).await?;
    Ok(id)
}

async fn attach(pool: &PgPool, id: i64, manifest: i64) -> Result<()> {
    sqlx::query("UPDATE normalized_events SET source_manifest_id=$2, raw_fact_ref=jsonb_build_object('emitting_address',$3::text) WHERE normalized_event_id=$1")
        .bind(id).bind(manifest).bind(REGISTRY).execute(pool).await?;
    Ok(())
}

struct Fixture {
    logical: String,
    boundary: i64,
    migration_manifest: i64,
}

async fn fixture(pool: &PgPool) -> Result<Fixture> {
    let logical = surface(pool, 822, "migrated.eth", &["ens_v2"]).await?;
    let retained = uuid(2, 822);
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')")
        .bind(&retained).bind(CHAIN).bind(HASH).execute(pool).await?;
    event(pool, "retained-v1-grant", &logical, Some(&retained), Event {
        family: "ens_v1_registrar_l1", kind: "RegistrationGranted", log: 0,
        after: json!({"registrant":"0x0000000000000000000000000000000000000999","expiry":1999999999}),
    }).await?;
    let registry_manifest = manifest(
        pool,
        "ens_v2_registry_l1",
        PROFILE,
        json!([{"role":"registry","address":REGISTRY,"start_block":1}]),
    )
    .await?;
    let migration_manifest = manifest(pool, "ens_v2_migration_l1", PROFILE, json!([])).await?;
    let registry = uuid(8, 822);
    sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')")
        .bind(&registry).bind(CHAIN).execute(pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number) VALUES ($1::uuid,$2,$3,1)")
        .bind(&registry).bind(CHAIN).bind(REGISTRY).execute(pool).await?;
    for (kind, log, after) in [
        (
            "RegistrationGranted",
            1,
            json!({"registrant":OWNER,"owner":OWNER,"expiry":2000000000,"token_id":labelhash("migrated"),"registry_contract_instance_id":registry}),
        ),
        (
            "SurfaceBound",
            2,
            json!({"surface_binding_id":uuid(3,822),"registry_contract_instance_id":registry}),
        ),
    ] {
        let id = event(
            pool,
            kind,
            &logical,
            Some(&uuid(1, 822)),
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
        attach(pool, id, registry_manifest).await?;
    }
    let boundary = event(pool, "migration-profile-boundary", &logical, None, Event {
        family:"ens_v2_migration_l1",kind:"MigrationApplied",log:1,
        after:json!({"migration_path":"unwrapped","successor_registry_contract_instance_id":registry,
            "successor_binding":{"binding_id":uuid(3,822),"resource_id":uuid(1,822)}}),
    }).await?;
    attach(pool, boundary, migration_manifest).await?;
    Ok(Fixture {
        logical,
        boundary,
        migration_manifest,
    })
}

async fn project(pool: &PgPool, mode: RunMode) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 10,
            affected_from_block: 10,
            affected_to_block: 10,
            resume_current: None,
            mode,
        })
        .await?;
    Ok(())
}

async fn status(pool: &PgPool, logical: &str) -> Result<(String, Option<String>, Option<String>)> {
    Ok(sqlx::query_as("SELECT support_status,unsupported_reason,resource_id::text FROM name_current WHERE logical_name_id=$1")
        .bind(logical).fetch_one(pool).await?)
}

async fn selected_authority(
    pool: &PgPool,
    logical: &str,
) -> Result<(Option<String>, Option<String>, Option<String>)> {
    Ok(sqlx::query_as("SELECT provenance #>> '{authority_selection,authority_arm}', provenance #>> '{authority_selection,unsupported_reason}', provenance #>> '{authority_selection,proof_event_identity}' FROM name_current WHERE logical_name_id=$1")
        .bind(logical).fetch_one(pool).await?)
}

// Each mutation breaks one thing the old exact-name profile gate checked. Support now follows the
// authority decision alone, so a mutation that leaves the selected ENSv2 registration without an
// authority refusal serves it, and only a mutation that breaks authority selection stays refused.
// Authority selection does not check the proof's manifest (`missing_manifest`, `inactive_manifest`,
// `wrong_proof_family`, `wrong_namespace`, `unadmitted_latest_proof` keep their proof); Interpret
// writes events only from active manifests, so these rows exist only in fixtures.
// These cases pin that support follows the authority result; they do not show that a malformed
// proof is rejected. Several keep a proof that should not establish a migration (`wrong_resource`
// keeps one whose successor is another resource), and the registration is served because it is
// independently current, not because the proof is sound. Checking proof association belongs to
// authority selection, not to the support decision this suite covers.
#[tokio::test]
async fn migration_boundary_mutations_refuse_only_through_authority_selection() -> Result<()> {
    for case in [
        "supported",
        "missing",
        "candidate",
        "orphan",
        "wrong_binding",
        "wrong_resource",
        "stale",
        "wrong_namespace",
        "wrong_chain",
        "old_profile",
        "custom_registry",
        "future_declaration",
        "expired_address",
        "wrong_instance",
        "missing_manifest",
        "inactive_manifest",
        "wrong_proof_family",
        "unadmitted_latest_proof",
    ] {
        let (db, pool) = database("migration_profile").await?;
        let f = fixture(&pool).await?;
        let mutation = match case {
            "supported" => None,
            "missing" => Some("DELETE FROM normalized_events WHERE event_kind='MigrationApplied'"),
            "candidate" => Some(
                "UPDATE normalized_events SET consumer_visibility='candidate' WHERE event_kind='MigrationApplied'",
            ),
            "orphan" => Some(
                "UPDATE normalized_events SET canonicality_state='orphaned' WHERE event_kind='MigrationApplied'",
            ),
            "wrong_binding" => Some(
                "UPDATE normalized_events SET after_state=jsonb_set(after_state,'{successor_binding,binding_id}',to_jsonb('00000003-0000-0000-0000-000000000999'::text)) WHERE event_kind='MigrationApplied'",
            ),
            "wrong_resource" => Some(
                "UPDATE normalized_events SET after_state=jsonb_set(after_state,'{successor_binding,resource_id}',to_jsonb('00000001-0000-0000-0000-000000000999'::text)) WHERE event_kind='MigrationApplied'",
            ),
            "wrong_namespace" => None,
            "old_profile" => Some(
                "UPDATE normalized_events SET after_state=jsonb_set(after_state,'{manifest_payload,deployment_epoch}',to_jsonb('ens_v2_sepolia_dev'::text)) WHERE event_kind='SourceManifestUpdated' AND source_family='ens_v2_registry_l1'",
            ),
            "custom_registry" => Some(
                "UPDATE normalized_events SET after_state=jsonb_set(after_state,'{manifest_payload,contracts}', '[]') WHERE event_kind='SourceManifestUpdated' AND source_family='ens_v2_registry_l1'",
            ),
            "future_declaration" => Some(
                "UPDATE normalized_events SET after_state=jsonb_set(after_state,'{manifest_payload,contracts,0,start_block}', '11') WHERE event_kind='SourceManifestUpdated' AND source_family='ens_v2_registry_l1'",
            ),
            "expired_address" => {
                Some("UPDATE contract_instance_addresses SET active_to_block_number=9")
            }
            "wrong_instance" => Some(
                "UPDATE normalized_events SET after_state=jsonb_set(after_state,'{successor_registry_contract_instance_id}',to_jsonb('00000008-0000-0000-0000-000000000999'::text)) WHERE event_kind='MigrationApplied'",
            ),
            "missing_manifest" => Some(
                "UPDATE normalized_events SET source_manifest_id=NULL WHERE event_kind='MigrationApplied'",
            ),
            "inactive_manifest" => Some(
                "UPDATE normalized_events SET after_state=jsonb_set(after_state,'{rollout_status}',to_jsonb('deprecated'::text)) WHERE event_kind='SourceManifestUpdated' AND source_family='ens_v2_migration_l1'",
            ),
            "wrong_proof_family" => Some(
                "UPDATE normalized_events SET source_manifest_id=NULL,source_family='ens_v2_registry_l1' WHERE event_kind='MigrationApplied'",
            ),
            "stale" | "wrong_chain" | "unadmitted_latest_proof" => None,
            _ => unreachable!(),
        };
        if let Some(sql) = mutation {
            sqlx::query(sql).execute(&pool).await?;
        }
        if case == "unadmitted_latest_proof" {
            // An earlier admitted boundary cannot substitute for the selected later proof.
            event(&pool, "unadmitted-later-boundary", &f.logical, None, Event {
                family:"ens_v2_migration_l1",kind:"MigrationApplied",log:3,
                after:json!({"migration_path":"unwrapped","successor_registry_contract_instance_id":uuid(8,822),
                    "successor_binding":{"binding_id":uuid(3,822),"resource_id":uuid(1,822)}}),
            }).await?;
        }
        if case == "wrong_namespace" {
            // Preserve FK-valid evidence, but its admitted namespace differs from the name.
            sqlx::query("UPDATE normalized_events SET source_manifest_id=NULL WHERE source_family='ens_v2_migration_l1'")
                .execute(&pool).await?;
            sqlx::query("UPDATE manifest_versions SET namespace='other' WHERE manifest_id=$1")
                .bind(f.migration_manifest)
                .execute(&pool)
                .await?;
            sqlx::query("UPDATE normalized_events SET namespace='other',source_manifest_id=$1 WHERE source_family='ens_v2_migration_l1'")
                .bind(f.migration_manifest).execute(&pool).await?;
        }
        if case == "stale" {
            sqlx::query("UPDATE surface_bindings SET active_to='2026-08-25T01:00:00Z'")
                .execute(&pool)
                .await?;
            sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')")
                .bind(uuid(1,823)).bind(CHAIN).bind(HASH).execute(&pool).await?;
            sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,provenance,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v2','2026-08-25T02:00:00Z',$4,$5,10,'{\"transaction_index\":0,\"log_index\":3}','canonical')")
                .bind(uuid(3,823)).bind(&f.logical).bind(uuid(1,823)).bind(CHAIN).bind(HASH).execute(&pool).await?;
        }
        if case == "wrong_chain" {
            // A boundary on another canonical chain is not this name's authority proof.
            sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ('ethereum-mainnet',$1,10,'2026-08-26T00:00:00Z','canonical')")
                .bind(HASH).execute(&pool).await?;
            sqlx::query("UPDATE normalized_events SET source_manifest_id=NULL,logical_name_id=NULL,chain_id='ethereum-mainnet' WHERE normalized_event_id=$1")
                .bind(f.boundary).execute(&pool).await?;
        }
        project(&pool, RunMode::Normal).await?;
        let normal = status(&pool, &f.logical).await?;
        let authority_refusal = match case {
            "wrong_binding" => Some("current_authority_not_projected"),
            _ => None,
        };
        let proof = match case {
            "missing" | "candidate" | "orphan" | "wrong_chain" => None,
            "unadmitted_latest_proof" => Some("unadmitted-later-boundary"),
            _ => Some("migration-profile-boundary"),
        };
        assert_eq!(
            selected_authority(&pool, &f.logical).await?,
            (
                Some("ens_v2".to_owned()),
                authority_refusal.map(str::to_owned),
                proof.map(str::to_owned)
            ),
            "{case}: authority"
        );
        assert_eq!(
            (normal.0.as_str(), normal.1.as_deref()),
            match authority_refusal {
                Some(reason) => ("unsupported", Some(reason)),
                None => ("supported", None),
            },
            "{case}: {normal:?}"
        );
        // `stale` binds a resource with no registration events, and the later boundary in
        // `unadmitted_latest_proof` starts the authority epoch after the grant, so these fixtures
        // carry no current registration to compare; only their authority result is checked.
        if authority_refusal.is_none() && !matches!(case, "stale" | "unadmitted_latest_proof") {
            assert_eq!(normal.2, Some(uuid(1, 822)), "{case}");
            let registrant: Option<String> = sqlx::query_scalar("SELECT declared_summary #>> '{registration,registrant}' FROM name_current WHERE logical_name_id=$1")
                .bind(&f.logical).fetch_one(&pool).await?;
            assert_eq!(
                registrant.as_deref(),
                Some(OWNER),
                "{case}: retained ENSv1 owner must stay historical"
            );
        }
        let registrar_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events WHERE source_family='ens_v2_registrar_l1'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(registrar_count, 0);
        project(&pool, RunMode::Redo).await?;
        assert_eq!(status(&pool, &f.logical).await?, normal, "redo {case}");
        db.cleanup().await?;
    }
    Ok(())
}

async fn current_registration(
    pool: &PgPool,
    logical: &str,
) -> Result<(String, Option<String>, Option<String>, Value, Value)> {
    Ok(sqlx::query_as("SELECT support_status,unsupported_reason,resource_id::text,declared_summary->'registration',declared_summary->'control' FROM name_current WHERE logical_name_id=$1")
        .bind(logical).fetch_one(pool).await?)
}

// A registration in an admitted ENSv2 registry is served on its own; the registrar's matching
// NameRegistered adds history but changes no current value.
#[tokio::test]
async fn registry_registration_serves_the_same_with_or_without_a_registrar_event() -> Result<()> {
    let (db, pool) = database("registrar_profile_control").await?;
    let f = fixture(&pool).await?;
    sqlx::query("DELETE FROM normalized_events WHERE source_family='ens_v1_registrar_l1'")
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM normalized_events WHERE normalized_event_id=$1")
        .bind(f.boundary)
        .execute(&pool)
        .await?;
    let mut without = Vec::new();
    for mode in [RunMode::Normal, RunMode::Redo] {
        project(&pool, mode).await?;
        let current = current_registration(&pool, &f.logical).await?;
        assert_eq!(
            (current.0.as_str(), current.1.as_deref(), current.2.clone()),
            ("supported", None, Some(uuid(1, 822))),
            "{current:?}"
        );
        assert_eq!(current.3["registrant"], OWNER, "{current:?}");
        without.push(current);
    }
    assert_eq!(without[0], without[1], "redo");
    let registrar = manifest(&pool, "ens_v2_registrar_l1", PROFILE, json!([])).await?;
    let id = event(
        &pool,
        "ordinary-registrar",
        &f.logical,
        Some(&uuid(1, 822)),
        Event {
            family: "ens_v2_registrar_l1",
            kind: "RegistrarNameRegistered",
            log: 2,
            after: json!({"registrant":OWNER}),
        },
    )
    .await?;
    attach(&pool, id, registrar).await?;
    for mode in [RunMode::Normal, RunMode::Redo] {
        project(&pool, mode).await?;
        assert_eq!(current_registration(&pool, &f.logical).await?, without[0]);
    }
    db.cleanup().await?;
    Ok(())
}
