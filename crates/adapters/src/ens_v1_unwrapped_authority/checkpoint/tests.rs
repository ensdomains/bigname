use super::*;
use sqlx::postgres::PgPoolOptions;

#[test]
fn checkpoint_payload_encoding_round_trips_nul_strings() -> Result<()> {
    let payload = json!({
        "record": "before\0after",
        "nested": [
            { "raw_name": "name\0with\0nuls" },
            "plain"
        ]
    });

    let encoded = CHECKPOINT_CODEC.encode(payload.clone());
    let encoded_json = serde_json::to_string(&encoded)?;

    assert_eq!(
        encoded,
        json!({
            "record": {
                "__bigname_unwrapped_authority_checkpoint_string_v1_hex":
                    "6265666f7265006166746572"
            },
            "nested": [
                {
                    "raw_name": {
                        "__bigname_unwrapped_authority_checkpoint_string_v1_hex":
                            "6e616d650077697468006e756c73"
                    }
                },
                "plain"
            ]
        })
    );
    assert!(!encoded_json.contains("\\u0000"));
    assert_eq!(CHECKPOINT_CODEC.decode(encoded)?, payload);
    Ok(())
}

#[test]
fn checkpoint_item_codec_preserves_error_contexts() {
    let codec_error = decode_item::<Value>(
        json!({"__bigname_unwrapped_authority_checkpoint_string_v1_hex": 1}),
        ITEM_KIND_HISTORY,
    )
    .expect_err("invalid checkpoint envelope must fail");
    assert_eq!(
        format!("{codec_error:#}"),
        "failed to decode unwrapped-authority checkpoint JSONB encoding: checkpoint escaped string payload is not a string"
    );

    let payload_error =
        decode_item::<u64>(Value::String("not a number".to_owned()), ITEM_KIND_HISTORY)
            .expect_err("invalid checkpoint item payload must fail");
    assert!(format!("{payload_error:#}").starts_with(
        "failed to decode unwrapped-authority checkpoint item name_history: invalid type"
    ));
}

#[test]
fn checkpoint_payload_encoding_leaves_jsonb_safe_strings_plain() -> Result<()> {
    let payload = json!({
        "record": "ordinary text",
        "nested": ["also ordinary"]
    });

    let encoded = CHECKPOINT_CODEC.encode(payload.clone());

    assert_eq!(encoded, payload);
    assert_eq!(CHECKPOINT_CODEC.decode(encoded)?, payload);
    Ok(())
}

#[test]
fn checkpoint_item_rows_prune_empty_pending_observations() -> Result<()> {
    let histories = BTreeMap::<String, NameHistory>::new();
    let reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
    let known_names_by_namehash = HashMap::<String, NameMetadata>::new();
    let known_name_refs_by_namehash = HashMap::<String, ObservationRef>::new();
    let namehash_to_labelhash = HashMap::<String, String>::new();
    let mut pending_namehash_observations = HashMap::<String, Vec<AuthorityObservation>>::new();
    pending_namehash_observations.insert("cleared".to_owned(), Vec::new());
    let migrated_registry_nodes = MigratedRegistryNodes::empty();
    let state = UnwrappedAuthorityReplayCheckpointStateRef {
        histories: &histories,
        reverse_histories: &reverse_histories,
        known_names_by_namehash: &known_names_by_namehash,
        known_name_refs_by_namehash: &known_name_refs_by_namehash,
        namehash_to_labelhash: &namehash_to_labelhash,
        pending_namehash_observations: &pending_namehash_observations,
        migrated_registry_nodes: &migrated_registry_nodes,
    };
    let mut delta = UnwrappedAuthorityReplayCheckpointDelta::default();
    delta.mark_pending_observations("cleared");
    delta.mark_pending_observations("missing");

    let rows = checkpoint_item_rows(&state, &delta)?;
    let delete_keys = checkpoint_pending_observation_delete_keys(&state, &delta);

    assert!(
        rows.iter()
            .all(|(item_kind, _, _)| *item_kind != ITEM_KIND_PENDING_OBSERVATIONS)
    );
    assert_eq!(
        delete_keys,
        vec!["cleared".to_owned(), "missing".to_owned()]
    );
    Ok(())
}

#[tokio::test]
async fn partial_startup_checkpoint_resets_when_raw_log_version_is_unknown() -> Result<()> {
    let pool =
        PgPoolOptions::new().connect_lazy_with(bigname_storage::stamp_projection_replay_version(
            "postgres://bigname:bigname@127.0.0.1:1/bigname".parse()?,
        ));
    let checkpoint = UnwrappedAuthorityReplayCheckpoint {
        context: AdapterCheckpointContext {
            deployment_profile: "test".to_owned(),
            cursor_kind: "startup_adapter_owned_raw_log_state".to_owned(),
            checkpoint_scope: "startup_adapter_sync",
            range_start_block_number: 0,
            target_block_number: 10,
            startup_discovery_admission_epoch: Some(0),
            startup_canonical_lineage_head: None,
            startup_adapter_semantic_version: Some(1),
            startup_schema_migration_state: Some((1, 1)),
        },
        chain: "ethereum-mainnet".to_owned(),
        status: "running".to_owned(),
        last_block_number: Some(5),
        scanned_log_count: 1,
        matched_log_count: 1,
        flushed_events: UnwrappedAuthorityReplayFlushedEvents::default(),
        state_payload: json!({ "version": SNAPSHOT_VERSION }),
        raw_log_input_version: RawLogStagingInputVersion::default(),
        adapter_semantic_version: Some(1),
        schema_migration_count: Some(1),
        schema_migration_max_version: Some(1),
    };

    assert!(
        checkpoint.raw_log_input_requires_reset(&pool, None).await?,
        "a missing strict revision row is unknown cross-boot authority"
    );
    Ok(())
}
