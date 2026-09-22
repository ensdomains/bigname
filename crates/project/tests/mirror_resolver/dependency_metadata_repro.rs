//! Minimal metadata evidence for the proposed mirror dependency/output separation.
use super::*;

#[tokio::test]
async fn unchanged_mirror_sibling_and_evidence_ancestor_retain_all_fields() -> Result<()> {
    const SIBLING: &str = "sibling.fixture";
    const SIBLING_RESOURCE: &str = "69100000-0000-0000-0000-000000000004";
    const SIBLING_BINDING: &str = "69100000-0000-0000-0000-000000000104";
    let fixture = Fixture::declared("mirror_metadata_repro", V1Side::Absent).with_ancestor(
        Ancestor::Direct {
            pointer_block_offset: 0,
        },
    );
    let (database, pool) = database(fixture.id).await?;
    seed(&pool, &fixture).await?;
    // A legitimate node-only ancestor pointer has no named creation evidence.
    sqlx::query("UPDATE normalized_events SET logical_name_id=NULL WHERE event_identity=$1")
        .bind(format!("{}:parent-pointer", fixture.id))
        .execute(&pool)
        .await?;
    let parent_id = format!("ens:{}", bigname_lookup::ens_namehash_hex(PARENT_NAME)?);
    let sibling_node = bigname_lookup::ens_namehash_hex(SIBLING)?;
    let sibling_id = format!("ens:{sibling_node}");
    let labelhashes: Vec<_> = SIBLING
        .split('.')
        .map(|label| format!("{:#x}", alloy_primitives::keccak256(label)))
        .collect();
    sqlx::query("INSERT INTO name_surfaces(logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
        VALUES($1,'ens',$2,string_to_array($2,'.'),$3,$4,$5,'fixture','active',$6,$7,10,'canonical')")
        .bind(&sibling_id).bind(SIBLING).bind(b"\x07sibling\x07fixture\0".as_slice()).bind(&sibling_node).bind(labelhashes).bind(CHAIN).bind(block_hash(10)).execute(&pool).await?;
    sqlx::query("INSERT INTO resources(resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES($1::uuid,$2,$3,10,'canonical')")
        .bind(SIBLING_RESOURCE).bind(CHAIN).bind(block_hash(10)).execute(&pool).await?;
    sqlx::query("INSERT INTO surface_bindings(surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state)
        VALUES($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v2',to_timestamp(1800000010),$4,$5,10,'canonical')")
        .bind(SIBLING_BINDING).bind(&sibling_id).bind(SIBLING_RESOURCE).bind(CHAIN).bind(block_hash(10)).execute(&pool).await?;
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state,raw_fact_ref)
        SELECT 'sibling-pointer','ens',$1,$2::uuid,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,derivation_kind,canonicality_state,$3,raw_fact_ref
        FROM normalized_events WHERE event_identity=$4")
        .bind(&sibling_id).bind(SIBLING_RESOURCE).bind(json!({"node":sibling_node,"resolver":MIRROR})).bind(format!("{}:v2-pointer",fixture.id)).execute(&pool).await?;
    run(&pool, 11, 0, 11, None, RunMode::Normal).await?;
    let sibling_all_before = selected_rows(&pool, &sibling_id, SIBLING_RESOURCE, false).await?;
    let ancestor_resolver_before = resolver_current(&pool, PARENT_RESOLVER).await?;
    let before = name_current(&pool, &sibling_id)
        .await?
        .context("sibling before")?;
    let inventory_before = inventory(&pool, SIBLING_RESOURCE).await?;
    let parent_before = name_current(&pool, &parent_id)
        .await?
        .context("ancestor before")?;
    assert!(parent_before["declared_summary"]["registration"]["created_at"].is_null());
    assert!(parent_before["declared_summary"]["history"]["created_at"].is_null());
    let changed_id = format!("ens:{}", bigname_lookup::ens_namehash_hex(NAME)?);
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state)
        VALUES('left-label-only-update','ens',$1,'PreimageObserved','ens_v2_root_l1',1,$2,12,$3,'ens_v1_unwrapped_authority','canonical','{\"label\":\"mirror\"}')")
        .bind(&changed_id).bind(CHAIN).bind(block_hash(12)).execute(&pool).await?;
    run(&pool, 12, 12, 12, Some(11), RunMode::Normal).await?;
    let after = name_current(&pool, &sibling_id)
        .await?
        .context("sibling after")?;
    let inventory_after = inventory(&pool, SIBLING_RESOURCE).await?;
    let parent_after = name_current(&pool, &parent_id)
        .await?
        .context("ancestor after")?;
    assert!(parent_after["declared_summary"]["registration"]["created_at"].is_null());
    assert!(parent_after["declared_summary"]["history"]["created_at"].is_null());
    assert_eq!(
        parent_before, parent_after,
        "input-only ancestor must retain every field"
    );
    assert_eq!(before, after, "unaffected sibling must retain every field");
    assert_eq!(
        inventory_before, inventory_after,
        "unaffected inventory must retain every field"
    );
    assert_eq!(after["chain_positions"][CHAIN]["block_number"], 11);
    let unchanged_events: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE block_number=12 AND (logical_name_id=$1 OR resource_id=$2::uuid)")
        .bind(&sibling_id).bind(SIBLING_RESOURCE).fetch_one(&pool).await?;
    assert_eq!(unchanged_events, 0);
    assert_eq!(
        sibling_all_before,
        selected_rows(&pool, &sibling_id, SIBLING_RESOURCE, false).await?,
        "unaffected sibling rows retain every column, including operational timestamps"
    );
    assert_eq!(
        ancestor_resolver_before,
        resolver_current(&pool, PARENT_RESOLVER).await?,
        "classification read must not republish its resolver"
    );
    let affected = selected_rows(&pool, &changed_id, V2_RESOURCE, true).await?;
    // The rebuilt mirror still selects the evidence-only ancestor, which the mirror rejects.
    let rebuilt = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(rebuilt["chain_positions"]["target_block_number"], 12);
    assert_eq!(
        rebuilt["provenance"]["mirror"]["mirrored_unsupported_reason"],
        "ancestor_resolver_not_extended"
    );
    run(&pool, 12, 0, 12, None, RunMode::Normal).await?;
    let full = selected_rows(&pool, &changed_id, V2_RESOURCE, true).await?;
    assert_rows(&affected, &full);

    // A real node-only ancestor clear invalidates both subscribers, even though the
    // ancestor was previously only a read dependency of the left update.
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state) VALUES('ancestor-clear','ens','ResolverChanged','ens_v1_registry_l1',1,$1,13,$2,'ens_v1_unwrapped_authority','canonical',$3)")
        .bind(CHAIN).bind(block_hash(13)).bind(json!({"node":bigname_lookup::ens_namehash_hex(PARENT_NAME)?,"resolver":ZERO20})).execute(&pool).await?;
    run(&pool, 13, 13, 13, Some(12), RunMode::Normal).await?;
    for resource in [V2_RESOURCE, SIBLING_RESOURCE] {
        let row = inventory(&pool, resource).await?;
        assert_eq!(row["support_status"], "unsupported");
        assert_eq!(row["unsupported_reason"], "mirrored_resolver_not_projected");
        assert_eq!(row["entries"], json!([]));
        assert_eq!(row["chain_positions"]["target_block_number"], 13);
    }
    let left_clear = selected_rows(&pool, &changed_id, V2_RESOURCE, true).await?;
    let right_clear = selected_rows(&pool, &sibling_id, SIBLING_RESOURCE, true).await?;
    run(&pool, 13, 0, 13, None, RunMode::Normal).await?;
    assert_eq!(
        left_clear,
        selected_rows(&pool, &changed_id, V2_RESOURCE, true).await?
    );
    assert_eq!(
        right_clear,
        selected_rows(&pool, &sibling_id, SIBLING_RESOURCE, true).await?
    );
    database.cleanup().await?;
    Ok(())
}

// The expected keys come from the fixture's two independent names/resources, not
// from Project's computed scope. Keep every meaningful field when comparing builds.
async fn selected_rows(
    pool: &PgPool,
    name: &str,
    resource: &str,
    omit_operational: bool,
) -> Result<Value> {
    let mut result = serde_json::Map::new();
    for table in [
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
    ] {
        let projection = if omit_operational {
            "to_jsonb(current) - 'last_recomputed_at' - 'inserted_at'"
        } else {
            "to_jsonb(current)"
        };
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(row ORDER BY row::text),'[]'::jsonb)
            FROM (SELECT {projection} AS row FROM {table} current) rows
            WHERE row ->> 'logical_name_id'=$1 OR row ->> 'parent_logical_name_id'=$1
               OR row ->> 'child_logical_name_id'=$1 OR row ->> 'resource_id'=$2
               OR row ->> 'record_resource_id'=$2"
        );
        let rows: Value = sqlx::query_scalar(&sql)
            .bind(name)
            .bind(resource)
            .fetch_one(pool)
            .await?;
        result.insert(table.into(), rows);
    }
    Ok(Value::Object(result))
}

fn assert_rows(actual: &Value, expected: &Value) {
    let mut differences = Vec::new();
    fn compare(path: &str, left: &Value, right: &Value, differences: &mut Vec<String>) {
        if let (Some(left), Some(right)) = (left.as_object(), right.as_object()) {
            let keys: std::collections::BTreeSet<_> = left.keys().chain(right.keys()).collect();
            for key in keys {
                compare(
                    &format!("{path}.{key}"),
                    left.get(key).unwrap_or(&Value::Null),
                    right.get(key).unwrap_or(&Value::Null),
                    differences,
                );
            }
        } else if let (Some(left), Some(right)) = (left.as_array(), right.as_array()) {
            if left.len() != right.len() {
                differences.push(format!("{path}: lengths {} / {}", left.len(), right.len()));
            } else {
                for (i, (left, right)) in left.iter().zip(right).enumerate() {
                    compare(&format!("{path}[{i}]"), left, right, differences);
                }
            }
        } else if left != right {
            differences.push(format!("{path}: {left} / {right}"));
        }
    }
    compare("rows", actual, expected, &mut differences);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

#[tokio::test]
async fn affected_mirror_matches_full_rows_with_ancestor_registration_and_expiry_history()
-> Result<()> {
    let fixture = Fixture::declared("mirror_ancestor_registration_inputs", V1Side::Absent)
        .with_ancestor(Ancestor::Direct {
            pointer_block_offset: 0,
        });
    let (database, pool) = database(fixture.id).await?;
    seed(&pool, &fixture).await?;
    let parent_id = format!("ens:{}", bigname_lookup::ens_namehash_hex(PARENT_NAME)?);
    let child_id = format!("ens:{}", bigname_lookup::ens_namehash_hex(NAME)?);
    for (number, kind, expiry) in [
        (10_i64, "RegistrationGranted", 1_900_000_000_i64),
        (11, "RegistrationRenewed", 1_950_000_000),
    ] {
        sqlx::query("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state)
            VALUES($1,'ens',$2,$3::uuid,$4,'ens_v1_registrar_l1',1,$5,$6,$7,'ens_v1_unwrapped_authority','canonical',$8)")
            .bind(format!("ancestor-lifecycle-{number}")).bind(&parent_id).bind(PARENT_V1_RESOURCE).bind(kind).bind(CHAIN).bind(number).bind(block_hash(number))
            .bind(json!({"registrant":"0x0000000000000000000000000000000000000999","expiry":expiry})).execute(&pool).await?;
    }
    run(&pool, 11, 0, 11, None, RunMode::Normal).await?;
    let before_parent = name_current(&pool, &parent_id)
        .await?
        .context("registered ancestor")?;
    assert!(!before_parent["declared_summary"]["registration"]["created_at"].is_null());
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state)
        VALUES('registered-parent-child-update','ens',$1,'PreimageObserved','ens_v2_root_l1',1,$2,12,$3,'ens_v1_unwrapped_authority','canonical','{\"label\":\"mirror\"}')")
        .bind(&child_id).bind(CHAIN).bind(block_hash(12)).execute(&pool).await?;
    run(&pool, 12, 12, 12, Some(11), RunMode::Normal).await?;
    assert_eq!(
        before_parent,
        name_current(&pool, &parent_id)
            .await?
            .context("retained ancestor")?
    );
    let affected = selected_rows(&pool, &child_id, V2_RESOURCE, true).await?;
    run(&pool, 12, 0, 12, None, RunMode::Normal).await?;
    assert_rows(
        &affected,
        &selected_rows(&pool, &child_id, V2_RESOURCE, true).await?,
    );
    database.cleanup().await?;
    Ok(())
}
