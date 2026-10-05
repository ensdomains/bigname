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
fn missing_surface_position_uses_the_conventional_sentinel() {
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
        provenance: json!({}),
        fallback_raw_labels_hex: None,
        later_witness_block_timestamp: None,
        later_witness_log_index: None,
    };
    assert_eq!(surface_log_index(&surface.provenance), -1);
    let desired = surface_normalization(&surface).unwrap();
    assert_eq!(
        desired.deactivated_at,
        Some(bigname_adapters::schema_v2::seam::event_time(timestamp, -1))
    );
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

/// Bytes first observed after the identity's anchor deactivate the name at their own event.
#[tokio::test]
async fn recompute_deactivates_at_the_later_preimage_witness() -> TestResult {
    let database = bigname_test_support::TestDatabase::create(
        bigname_test_support::TestDatabaseConfig::new("recompute_later_witness"),
    )
    .await?;
    for sql in [
        include_str!("../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../storage/schema/baseline/05_normalized_events.sql"),
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
                   1, 'recompute', 12, 'evidence', '0xtx', 0, 4, '{}',
                   'raw_log_preimage_observation', 'canonical', '{}')"
            .replace("PREIMAGE_KIND", PREIMAGE_OBSERVATION_EVENT_KIND),
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
            Some(bigname_adapters::schema_v2::seam::event_time(
                OffsetDateTime::from_unix_timestamp(12)?,
                4
            ))
        )
    );
    database.cleanup().await?;
    Ok(())
}
