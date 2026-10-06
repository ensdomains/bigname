use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[test]
fn dns_labels_and_hex_fallback_round_trip_raw_bytes() {
    assert_eq!(
        decode_dns_labels(&[5, b'a', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0]),
        Ok(vec![b"alice".to_vec(), b"eth".to_vec()])
    );
    assert_eq!(decode_hex("00ff41", "ens:test").unwrap(), [0, 255, 65]);
}

#[test]
fn label_flag_matches_the_interpreter_normalization_gate() {
    assert!(normalization_flag(b"alice").normalized);
    assert_eq!(
        normalization_flag(b"Alice").error.as_deref(),
        Some("raw label is not byte-identical to its normalized form")
    );
    for raw_label in [&[0xff][..], b"alice\0"] {
        let flag = normalization_flag(raw_label);
        assert!(!flag.normalized);
        assert_eq!(
            flag.error.as_deref(),
            Some("raw label has no PostgreSQL-safe UTF-8 decoding")
        );
    }
}

#[test]
fn missing_surface_position_does_not_change_the_block_timestamp() {
    let timestamp = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
    let surface = SurfaceRow {
        logical_name_id: "ens:test".to_owned(),
        raw_labels: Some(vec!["Alice".to_owned()]),
        dns_encoded_name: Some(Vec::new()),
        normalizer_version: "old".to_owned(),
        visibility_state: "active".to_owned(),
        normalization_errors: json!([]),
        deactivation_reason: None,
        deactivated_at: None,
        block_number: 1,
        block_timestamp: timestamp,
        preimage_event_identity: None,
        fallback_raw_labels_hex: None,
        fallback_block_timestamp: None,
        witness_event_identity: None,
        witness_block_timestamp: None,
    };
    let desired = surface_normalization(&surface).unwrap();
    assert_eq!(desired.deactivated_at, Some(timestamp));
}

#[tokio::test]
async fn surface_loader_ignores_orphaned_surfaces_and_fallback_events() -> TestResult {
    let database = bigname_test_support::TestDatabase::create(
        bigname_test_support::TestDatabaseConfig::new("recompute_canonical_surface_labels"),
    )
    .await?;
    for sql in [
        include_str!("../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../storage/schema/baseline/07_labels.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('recompute', 'winning', 10, to_timestamp(10), 'canonical'),
                ('recompute', 'losing', 10, to_timestamp(10), 'orphaned')",
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
             (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
              labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
              block_number, canonicality_state)
         VALUES ('ens:winning-name', 'ens', '', ARRAY[]::text[], ''::bytea,
                 'winning-name', ARRAY[]::text[], 'old', 'active', 'recompute', 'winning', 10,
                 'canonical'),
                ('ens:losing-name', 'ens', 'orphan', ARRAY['orphan'], ''::bytea,
                 'losing-name', ARRAY['label'], 'old', 'active', 'recompute', 'losing', 10,
                 'orphaned')",
    )
    .execute(database.pool())
    .await?;
    for (identity, hash, state, log_index, raw_label) in [
        ("winning-label", "winning", "canonical", 0_i64, "616c696365"),
        ("losing-label", "losing", "orphaned", 1_i64, "416c696365"),
    ] {
        sqlx::query(
            "INSERT INTO normalized_events
                 (event_identity, namespace, logical_name_id, event_kind, source_family,
                  manifest_version, chain_id, block_number, block_hash, transaction_hash,
                  transaction_index, log_index, derivation_kind, canonicality_state,
                  after_state)
             VALUES ($1, 'ens', 'ens:winning-name', 'RegistrationGranted',
                     'ens_v2_registry_l1', 1, 'recompute', 10, $2, 'tx', 0, $3,
                     'ens_v2_registry_resource_surface', $4::canonicality_state,
                     jsonb_build_object('raw_labels_hex', jsonb_build_array($5::text)))",
        )
        .bind(identity)
        .bind(hash)
        .bind(log_index)
        .bind(state)
        .bind(raw_label)
        .execute(database.pool())
        .await?;
    }

    let mut transaction = database.pool().begin().await?;
    let surfaces = load_surfaces(&mut transaction, "recompute", 10, 10).await?;
    assert_eq!(surfaces.len(), 1);
    assert_eq!(surfaces[0].logical_name_id, "ens:winning-name");
    assert_eq!(
        surface_normalization(&surfaces[0])?.visibility_state,
        "active"
    );
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn recompute_keeps_a_surface_without_raw_bytes_visible_and_unknown() -> TestResult {
    let database = bigname_test_support::TestDatabase::create(
        bigname_test_support::TestDatabaseConfig::new("recompute_surface_without_raw_bytes"),
    )
    .await?;
    for sql in [
        include_str!("../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../storage/schema/baseline/07_labels.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::raw_sql(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('recompute', 'block', 10, to_timestamp(10), 'canonical');
         INSERT INTO name_surfaces
             (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
              visibility_state, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('ens:0xchild', 'ens', '0xchild', ARRAY['0xchildlabel', '0xeth'], 'old',
                 'active', 'recompute', 'block', 10, 'canonical')",
    )
    .execute(database.pool())
    .await?;

    let mut transaction = database.pool().begin().await?;
    let summary = finalize_recompute_flags(&mut transaction, "recompute", 0, 20).await?;
    transaction.commit().await?;

    assert_eq!(
        summary,
        RecomputeSummary {
            same_class_names: 1,
            ..RecomputeSummary::default()
        }
    );
    let row: (Option<String>, String, String, serde_json::Value) = sqlx::query_as(
        "SELECT raw_name, normalizer_version, visibility_state, normalization_errors
         FROM name_surfaces",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        row,
        (
            None,
            ENS_NORMALIZER_VERSION.to_owned(),
            "active".to_owned(),
            json!([])
        )
    );
    database.cleanup().await?;
    Ok(())
}

/// Bytes first observed after the identity's anchor, in a later block or later in the anchor's
/// block, deactivate the name at their own event.
#[tokio::test]
async fn recompute_deactivates_at_the_later_preimage_witness() -> TestResult {
    for (witness_block, witness_hash) in [(12, "evidence"), (10, "anchor")] {
        let database = bigname_test_support::TestDatabase::create(
            bigname_test_support::TestDatabaseConfig::new("recompute_later_witness"),
        )
        .await?;
        for sql in [
            include_str!("../../storage/schema/baseline/01_chain.sql"),
            include_str!("../../storage/schema/baseline/03_identity.sql"),
            include_str!("../../storage/schema/baseline/04_manifests.sql"),
            include_str!("../../storage/schema/baseline/05_normalized_events.sql"),
            include_str!("../../storage/schema/baseline/07_labels.sql"),
        ] {
            sqlx::raw_sql(sql).execute(database.pool()).await?;
        }
        sqlx::raw_sql(
            &"INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('recompute', 'anchor', 10, to_timestamp(10), 'canonical'),
                ('recompute', 'evidence', 12, to_timestamp(12), 'canonical');
         INSERT INTO name_surfaces
             (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
              labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
              block_number, provenance, canonicality_state, preimage_event_identity)
         VALUES ('ens:0xchild', 'ens', 'Alice', ARRAY['Alice'], '\\x05416c69636500', '0xchild',
                 ARRAY['0xalice'], 'old', 'active', 'recompute', 'anchor', 10,
                 '{\"log_index\": 1}', 'canonical', 'preimage-12');
         INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             after_state
         ) VALUES ('preimage-12', 'ens', 'ens:0xchild', 'PREIMAGE_KIND', 'ens_v1_registry_l1',
                   1, 'recompute', WITNESS_BLOCK, 'WITNESS_HASH', '0xtx', 0, 4, '{}',
                   'raw_log_preimage_observation', 'canonical', '{}')"
                .replace("PREIMAGE_KIND", PREIMAGE_OBSERVATION_EVENT_KIND)
                .replace("WITNESS_BLOCK", &witness_block.to_string())
                .replace("WITNESS_HASH", witness_hash),
        )
        .execute(database.pool())
        .await?;

        let mut transaction = database.pool().begin().await?;
        finalize_recompute_flags(&mut transaction, "recompute", 0, 20).await?;
        transaction.commit().await?;

        let row: (String, Option<OffsetDateTime>) =
            sqlx::query_as("SELECT visibility_state, deactivated_at FROM name_surfaces")
                .fetch_one(database.pool())
                .await?;
        assert_eq!(
            row,
            (
                "shadow".to_owned(),
                Some(OffsetDateTime::from_unix_timestamp(witness_block)?)
            )
        );
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn recompute_reads_later_raw_hex_and_repairs_same_block_null_position_witness() -> TestResult
{
    let database = bigname_test_support::TestDatabase::create(
        bigname_test_support::TestDatabaseConfig::new("recompute_later_raw_hex"),
    )
    .await?;
    for sql in [
        include_str!("../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../storage/schema/baseline/07_labels.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::raw_sql(&"INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ('recompute','anchor',10,to_timestamp(10),'canonical'),('recompute','bytes',12,to_timestamp(12),'canonical');
        INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state,preimage_event_identity,deactivation_reason,deactivated_at)
        VALUES ('ens:node','ens','','{}','','node',ARRAY['label'],'old','shadow','recompute','anchor',10,'canonical','later','normalization_gate',to_timestamp(12)+interval '7 microseconds');
        INSERT INTO normalized_events (event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
        VALUES ('later','ens','ens:node','PREIMAGE_KIND','ens_v1_wrapper_l1',1,'recompute',12,'bytes','tx',0,7,'raw_log_preimage_observation','canonical','{\"raw_labels_hex\":[\"ff\"]}'),
               ('null-earlier','ens','ens:node','PREIMAGE_KIND','ens_v1_wrapper_l1',1,'recompute',12,'bytes','tx',NULL,NULL,'raw_log_preimage_observation','canonical','{\"raw_labels_hex\":[\"ff\"]}');".replace("PREIMAGE_KIND", PREIMAGE_OBSERVATION_EVENT_KIND))
        .execute(database.pool()).await?;
    let mut transaction = database.pool().begin().await?;
    let surfaces = load_surfaces(&mut transaction, "recompute", 10, 10).await?;
    assert_eq!(surfaces.len(), 1);
    assert_eq!(surfaces[0].fallback_raw_labels_hex, Some(json!(["ff"])));
    finalize_recompute_flags(&mut transaction, "recompute", 10, 10).await?;
    transaction.commit().await?;
    let row: (String, Option<String>, Option<OffsetDateTime>) = sqlx::query_as(
        "SELECT visibility_state,preimage_event_identity,deactivated_at FROM name_surfaces",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        row,
        (
            "shadow".into(),
            Some("null-earlier".into()),
            Some(OffsetDateTime::from_unix_timestamp(12)?)
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn recompute_and_actual_rainbow_import_preserve_both_changes_after_waits() -> TestResult {
    use std::time::{Duration, Instant};
    let database = bigname_test_support::TestDatabase::create(
        bigname_test_support::TestDatabaseConfig::new("recompute_search_import_overlap")
            .pool_max_connections(6),
    )
    .await?;
    for source in [
        include_str!("../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../storage/schema/baseline/07_labels.sql"),
    ] {
        sqlx::raw_sql(source).execute(database.pool()).await?;
    }
    let first = format!("{:#x}", alloy_primitives::keccak256(b"first"));
    let second = format!("{:#x}", alloy_primitives::keccak256(b"second"));
    sqlx::raw_sql("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
        VALUES ('recompute','one',1,to_timestamp(1),'canonical')").execute(database.pool()).await?;
    sqlx::query("INSERT INTO name_surfaces(logical_name_id,namespace,namehash,labelhashes,normalizer_version,
        visibility_state,chain_id,block_hash,block_number,canonicality_state)
        VALUES ('ens:overlap','ens','overlap',$1,'old','active','recompute','one',1,'canonical')")
        .bind(vec![first.clone(),second.clone()]).execute(database.pool()).await?;
    sqlx::query(
        "INSERT INTO label_preimages(labelhash,raw_label,decoded_label,normalizer_version,
        normalized_under_version,normalization_error,source_kind,source_priority,provenance)
        VALUES ($1,$2,'first','old',false,'old verdict',$3,10,'{}')",
    )
    .bind(first)
    .bind(b"first".as_slice())
    .bind(bigname_storage::ENS_RAINBOW_SOURCE_KIND)
    .execute(database.pool())
    .await?;
    sqlx::query("INSERT INTO ens_names VALUES ($1,'second')")
        .bind(second)
        .execute(database.pool())
        .await?;
    let mut blocker = database.pool().begin().await?;
    sqlx::query("SELECT logical_name_id FROM name_surfaces FOR NO KEY UPDATE")
        .execute(&mut *blocker)
        .await?;
    async fn wait(pool: &sqlx::PgPool, text: &str) -> TestResult {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity
                WHERE datname=current_database() AND pid<>pg_backend_pid()
                  AND wait_event_type='Lock' AND query LIKE $1",
            )
            .bind(format!("%{text}%"))
            .fetch_one(pool)
            .await?;
            if n > 0 {
                return Ok(());
            }
            assert!(Instant::now() < deadline, "writer did not wait at {text}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let pool = database.pool().clone();
    let recompute = tokio::spawn(async move { run(&pool, "recompute", 0, 1).await });
    wait(database.pool(), "identity_search.name_locks").await?;
    let pool = database.pool().clone();
    let importer = tokio::spawn(async move {
        bigname_storage::import_label_preimages_from_ens_names_table(&pool, Some(1), Some(1)).await
    });
    wait(database.pool(), "identity_search.label_locks").await?;
    blocker.commit().await?;
    let _ = tokio::time::timeout(Duration::from_secs(10), recompute).await???;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), importer)
            .await???
            .retained_row_count,
        1
    );
    let name: String = sqlx::query_scalar("SELECT name FROM name_search_documents")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(name, "first.second");
    let hits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_search_postings
        WHERE token_kind=2 AND token_bytes=$1",
    )
    .bind(b"fir".as_slice())
    .fetch_one(database.pool())
    .await?;
    assert_eq!(hits, 1);
    database.cleanup().await?;
    Ok(())
}
