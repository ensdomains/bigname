//! A mirror's selected resolver is a dependency even for a node-only ancestor pointer.
use super::*;

#[tokio::test]
async fn node_only_ancestor_classification_change_invalidates_mirror() -> Result<()> {
    let fixture = Fixture::declared("mirror_input_classification", V1Side::Absent).with_ancestor(
        Ancestor::Direct {
            pointer_block_offset: 0,
        },
    );
    let (database, pool) = database(fixture.id).await?;
    seed(&pool, &fixture).await?;
    sqlx::query("UPDATE normalized_events SET logical_name_id=NULL,resource_id=NULL WHERE event_identity=$1")
        .bind(format!("{}:parent-pointer", fixture.id)).execute(&pool).await?;
    run(&pool, 11, 0, 11, None, RunMode::Normal).await?;
    let before = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(before["support_status"], "unsupported");
    assert_eq!(
        before["provenance"]["mirror"]["mirrored_unsupported_reason"],
        "ancestor_resolver_not_extended"
    );
    // Publish a new admitted classification at block12; no registry pointer or
    // record changes accompany it. The ancestor is still not derived through, but the row
    // must be rebuilt because the reason moves to the extended-resolver case.
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state,raw_fact_ref)
        SELECT 'parent-classification-update',namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,12,$1,derivation_kind,canonicality_state,
               jsonb_set(after_state,'{manifest_payload,contracts,1,read_features}','[\"ensip10_extended_resolver\"]'),raw_fact_ref
        FROM normalized_events WHERE event_kind='SourceManifestUpdated' AND source_family='ens_v1_resolver_l1'")
        .bind(block_hash(12)).execute(&pool).await?;
    run(&pool, 12, 12, 12, Some(11), RunMode::Normal).await?;
    let affected = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(affected["chain_positions"]["target_block_number"], 12);
    assert_eq!(affected["support_status"], "unsupported");
    assert_eq!(
        affected["provenance"]["mirror"]["mirrored_unsupported_reason"],
        "ensip10_extended_resolver"
    );
    run(&pool, 12, 0, 12, None, RunMode::Normal).await?;
    assert_eq!(affected, inventory(&pool, V2_RESOURCE).await?);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn stale_mirror_classification_is_rebuilt_even_on_record_only_update() -> Result<()> {
    let fixture = Fixture::declared("mirror_input_stale_classification", V1Side::Absent)
        .with_ancestor(Ancestor::Direct {
            pointer_block_offset: 0,
        });
    let (database, pool) = database(fixture.id).await?;
    seed(&pool, &fixture).await?;
    run(&pool, 11, 0, 11, None, RunMode::Normal).await?;
    // A future retained classification is unusable for this earlier selected target.
    sqlx::query("UPDATE resolver_current SET support_status='unsupported',unsupported_reason='stale_test_classification',chain_positions=jsonb_build_object('target_block_number',13,'target_block_hash',$1) WHERE chain_id=$2 AND resolver_address=$3")
        .bind(block_hash(13)).bind(CHAIN).bind(PARENT_RESOLVER).execute(&pool).await?;
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref)
        SELECT 'child-record-update',namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,12,$1,transaction_hash,0,0,derivation_kind,canonicality_state,jsonb_set(after_state,'{value}','\"new child value\"'),raw_fact_ref
        FROM normalized_events WHERE event_identity=$2")
        .bind(block_hash(12)).bind(format!("{}:parent-child-text",fixture.id)).execute(&pool).await?;
    run(&pool, 12, 12, 12, Some(11), RunMode::Normal).await?;
    let affected = inventory(&pool, V2_RESOURCE).await?;
    let direct = inventory(&pool, PARENT_V1_RESOURCE).await?;
    assert_eq!(
        direct["chain_positions"]["target_block_number"], 12,
        "direct users of forced classification also rebuild"
    );
    assert_eq!(
        resolver_current(&pool, PARENT_RESOLVER).await?["support_status"],
        "supported"
    );
    // The mirror still rejects the non-extended ancestor, and the rebuilt row names that reason
    // from the fresh classification, not the stale retained one.
    assert_eq!(affected["support_status"], "unsupported");
    assert_eq!(
        affected["provenance"]["mirror"]["mirrored_unsupported_reason"],
        "ancestor_resolver_not_extended"
    );
    assert_eq!(affected["entries"], json!([]));
    assert_eq!(affected["chain_positions"]["target_block_number"], 12);
    run(&pool, 12, 0, 12, None, RunMode::Normal).await?;
    assert_eq!(affected, inventory(&pool, V2_RESOURCE).await?);
    assert_eq!(direct, inventory(&pool, PARENT_V1_RESOURCE).await?);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn absent_input_ancestor_stays_absent_while_missing_classification_rebuilds() -> Result<()> {
    let fixture =
        Fixture::declared("mirror_input_absence", V1Side::Absent).with_ancestor(Ancestor::Direct {
            pointer_block_offset: 0,
        });
    let (database, pool) = database(fixture.id).await?;
    seed(&pool, &fixture).await?;
    sqlx::query("UPDATE normalized_events SET logical_name_id=NULL,resource_id=NULL WHERE event_identity=$1")
        .bind(format!("{}:parent-pointer", fixture.id)).execute(&pool).await?;
    run(&pool, 11, 0, 11, None, RunMode::Normal).await?;
    let parent_id = format!("ens:{}", bigname_lookup::ens_namehash_hex(PARENT_NAME)?);
    let child_id = format!("ens:{}", bigname_lookup::ens_namehash_hex(NAME)?);
    sqlx::query("DELETE FROM name_current WHERE logical_name_id=$1")
        .bind(&parent_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM resolver_current WHERE chain_id=$1 AND resolver_address=$2")
        .bind(CHAIN)
        .bind(PARENT_RESOLVER)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state)
        VALUES('left-absence-update','ens',$1,'PreimageObserved','ens_v2_root_l1',1,$2,12,$3,'ens_v1_unwrapped_authority','canonical','{\"label\":\"mirror\"}')")
        .bind(&child_id).bind(CHAIN).bind(block_hash(12)).execute(&pool).await?;
    run(&pool, 12, 12, 12, Some(11), RunMode::Normal).await?;
    assert!(name_current(&pool, &parent_id).await?.is_none());
    assert_eq!(
        resolver_current(&pool, PARENT_RESOLVER).await?["support_status"],
        "supported"
    );
    let affected = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(affected["support_status"], "unsupported");
    assert_eq!(
        affected["provenance"]["mirror"]["mirrored_unsupported_reason"],
        "ancestor_resolver_not_extended"
    );
    run(&pool, 12, 0, 12, None, RunMode::Normal).await?;
    assert_eq!(affected, inventory(&pool, V2_RESOURCE).await?);
    database.cleanup().await?;
    Ok(())
}
