use super::*;

const OLD_CANDIDATE_INTERPRETER_HASH: &str =
    "keccak256:e6204544522d7693363416c514984e9c2291979292794f002a8757ec4d964a0d";

#[tokio::test]
async fn semantic_end_state_keeps_surface_deactivated_at() -> TestResult {
    let database = database("interpret_semantic_surface_deactivation").await?;
    install_stage_capture(database.pool()).await?;
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('semantic-deactivation', 'deactivated-block', 42,
                 '2026-08-19 12:34:56+00'::timestamptz, 'canonical')",
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
             (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
              labelhashes, normalizer_version, visibility_state, deactivation_reason,
              deactivated_at, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('ens:deactivated-surface', 'ens', 'deactivated-surface',
                 ARRAY['deactivated-surface'], ''::bytea, 'deactivated-surface',
                 ARRAY['deactivated-labelhash'], 'test', 'shadow', 'normalization_failed',
                 '2026-08-19 12:34:56+00'::timestamptz, 'semantic-deactivation',
                 'deactivated-block', 42, 'canonical')",
    )
    .execute(database.pool())
    .await?;

    let snapshot = semantic_end_state(database.pool()).await?;
    let surfaces = snapshot
        .iter()
        .find_map(|(table, rows)| (table == "name_surfaces").then_some(rows))
        .expect("name_surfaces semantic snapshot");
    assert_eq!(
        surfaces[0]["deactivated_at"],
        serde_json::json!("2026-08-19T12:34:56+00:00")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn fresh_activation_and_candidate_state_redo_publish_identical_end_state() -> TestResult {
    let fresh = database("interpret_activation_fresh_equivalence").await?;
    let redo = database("interpret_activation_redo_equivalence").await?;
    seed_activation_corpus(fresh.pool()).await?;
    seed_activation_corpus(redo.pool()).await?;

    write_migration_range(fresh.pool(), false, false).await?;
    project_full(fresh.pool()).await?;

    // Post-#526 main retained the candidate ENSv1→ENSv2 migration derivation
    // unchanged; stamp that state with main's hash.
    stamp_interpreter_hash(redo.pool(), OLD_CANDIDATE_INTERPRETER_HASH).await?;
    let candidate = write_migration_range(redo.pool(), true, false).await?;
    assert!(candidate.migration_authority_transitions.is_empty());
    let candidate_hashes: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT interpreter_content_hash
         FROM migration_event_associations ORDER BY interpreter_content_hash",
    )
    .fetch_all(redo.pool())
    .await?;
    assert_eq!(candidate_hashes, [OLD_CANDIDATE_INTERPRETER_HASH]);
    let logical_name_id = format!("ens:{:#x}", eth_namehash(keccak256(b"activation-gate")));
    let successor: Uuid = sqlx::query_scalar(
        "SELECT surface_binding_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v2' AND active_to IS NULL",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_one(redo.pool())
    .await?;
    project_full(redo.pool()).await?;
    let candidate_proof: Option<String> = sqlx::query_scalar(
        "SELECT provenance #>> '{authority_selection,proof_kind}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(redo.pool())
    .await?;
    assert_ne!(
        candidate_proof.as_deref(),
        Some("migration_authority_transition"),
        "the old candidate-only generation must not give Project an activated proof"
    );
    plant_valid_replay_marker(redo.pool(), &logical_name_id, successor).await?;
    stamp_interpreter_hash(redo.pool(), bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    write_migration_range(redo.pool(), false, true).await?;
    project_full(redo.pool()).await?;

    let fresh_state = semantic_end_state(fresh.pool()).await?;
    let redo_state = semantic_end_state(redo.pool()).await?;
    for required in [
        "migration_event_associations",
        "migration_discovery_associations",
        "migration_candidate_identity_effects",
        "binding_closure_positions",
        "activation_project_stage_capture",
        "name_current",
    ] {
        let rows = fresh_state
            .iter()
            .find_map(|(table, rows)| (table == required).then_some(rows))
            .expect("required semantic snapshot");
        assert_ne!(
            rows,
            &serde_json::json!([]),
            "{required} must be non-vacuous"
        );
    }
    let children = fresh_state
        .iter()
        .find_map(|(table, rows)| (table == "children_current").then_some(rows))
        .expect("children_current semantic snapshot");
    assert_eq!(
        children,
        &serde_json::json!([]),
        "the unlocked parent makes the fixture's retained ENSv1 child unreachable"
    );
    for ((fresh_table, fresh_rows), (redo_table, redo_rows)) in fresh_state.iter().zip(&redo_state)
    {
        assert_eq!(fresh_table, redo_table);
        assert!(
            fresh_rows == redo_rows,
            "fresh and exact-range redo paths differ in {fresh_table}: {}",
            first_json_difference(fresh_rows, redo_rows, "$"),
        );
    }
    fresh.cleanup().await?;
    redo.cleanup().await?;
    Ok(())
}

async fn seed_activation_corpus(pool: &PgPool) -> TestResult {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;
    let label = b"activation-gate";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    seed_lineage(pool).await?;
    seed_predecessor_facts(pool, labelhash, namehash).await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: PREDECESSOR_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    seed_migration_facts(pool, label, labelhash).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    install_stage_capture(pool).await?;
    Ok(())
}

async fn write_migration_range(
    pool: &PgPool,
    candidate_only: bool,
    redo: bool,
) -> TestResult<bigname_adapters::schema_v2::BatchOutput> {
    let loaded = load::batch_input(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        MIGRATION_BLOCK,
        None,
        None,
        StateCacheCapacity::Entries(65_536),
    )
    .await?;
    let expected_orphaning_epoch = loaded.prior_cache.validated_orphaning_epoch;
    let prepared = prepare_schema_v2_batch_incremental(
        loaded.input,
        loaded.adapter_session,
        StateCacheCapacity::Entries(65_536),
    )?;
    let state_values = load::prior_state_values(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        prepared.state_value_requests(),
    )
    .await?;
    let (mut output, _) = prepared.finish(state_values)?;
    if candidate_only {
        for event in &mut output.normalized_events {
            if event.source_family == "ens_v2_migration_l1" {
                event.consumer_visibility = "candidate".to_owned();
                if event.after_state.get("consumer_visibility").is_some() {
                    event.after_state["consumer_visibility"] = serde_json::json!("candidate");
                    event.after_state["candidate_authority_transition"] = serde_json::json!(true);
                }
            }
        }
        for association in &mut output.migration_event_associations {
            association.consumer_visibility = "candidate".to_owned();
        }
        for association in &mut output.migration_discovery_associations {
            association.consumer_visibility = "candidate".to_owned();
        }
        output.migration_authority_transitions.clear();
    }
    write::batch(
        pool,
        CHAIN,
        redo.then_some((MIGRATION_BLOCK, MIGRATION_BLOCK)),
        redo,
        true,
        expected_orphaning_epoch,
        &[(MIGRATION_BLOCK, block_hash(MIGRATION_BLOCK))],
        &output,
    )
    .await?;
    Ok(output)
}

async fn project_full(pool: &PgPool) -> TestResult {
    sqlx::query("DELETE FROM activation_project_stage_capture")
        .execute(pool)
        .await?;
    ProjectEngine::new(pool.clone())
        .run_batch(ProjectBatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: MIGRATION_BLOCK,
            affected_from_block: SETUP_BLOCK,
            affected_to_block: MIGRATION_BLOCK,
            resume_current: None,
            mode: ProjectRunMode::Normal,
        })
        .await?;
    Ok(())
}

pub(super) async fn install_stage_capture(pool: &PgPool) -> TestResult {
    sqlx::raw_sql(
        "CREATE TABLE activation_project_stage_capture (
             stage_kind text NOT NULL,
             row_value jsonb NOT NULL
         );
         CREATE FUNCTION capture_activation_project_stage() RETURNS trigger AS $$
         BEGIN
             INSERT INTO activation_project_stage_capture
             SELECT 'project_name_authority', to_jsonb(stage)
             FROM project_name_authority stage
             WHERE stage.logical_name_id = NEW.logical_name_id;
             INSERT INTO activation_project_stage_capture
             SELECT 'project_binding_candidates', to_jsonb(stage)
             FROM project_binding_candidates stage
             WHERE stage.logical_name_id = NEW.logical_name_id;
             INSERT INTO activation_project_stage_capture
             SELECT 'project_authority_events', to_jsonb(stage)
             FROM project_authority_events stage
             WHERE stage.logical_name_id = NEW.logical_name_id;
             INSERT INTO activation_project_stage_capture
             SELECT 'project_child_candidates', to_jsonb(stage)
             FROM project_child_candidates stage
             WHERE stage.child_logical_name_id = NEW.logical_name_id;
             RETURN NEW;
         END
         $$ LANGUAGE plpgsql;
         CREATE TRIGGER capture_activation_project_stage
         AFTER INSERT OR UPDATE ON name_current
         FOR EACH ROW EXECUTE FUNCTION capture_activation_project_stage();",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn plant_valid_replay_marker(
    pool: &PgPool,
    logical_name_id: &str,
    successor: Uuid,
) -> TestResult {
    sqlx::query(&format!(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state,
             before_state, after_state, consumer_visibility
         ) VALUES (
             'activation-equivalence-replay-marker', 'ens', $1, '{PREIMAGE_OBSERVATION_EVENT_KIND}',
             'ens_v2_registry_l1', 1, $2, $3, $4, 'activation-equivalence-marker-tx',
             0, 0, 'raw_log_preimage_observation', 'canonical', '{{}}'::jsonb,
             jsonb_build_object(
                 '{ARM_WIDE_BINDING_CLOSE_KEY}', true,
                 '{CLOSED_AUTHORITY_ARM_KEY}', 'ens_v2',
                 '{SURFACE_BINDING_ID_KEY}', $5::uuid::text
             ), 'activated'
         )"
    ))
    .bind(logical_name_id)
    .bind(CHAIN)
    .bind(MIGRATION_BLOCK)
    .bind(block_hash(MIGRATION_BLOCK))
    .bind(successor)
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) async fn semantic_end_state(
    pool: &PgPool,
) -> TestResult<Vec<(String, serde_json::Value)>> {
    let tables = [
        ("normalized_events", "normalized_event_id"),
        ("migration_event_associations", ""),
        ("migration_discovery_associations", ""),
        ("migration_candidate_identity_effects", ""),
        ("migration_candidate_discovery_effects", ""),
        ("name_surfaces", ""),
        ("token_lineages", ""),
        ("resources", ""),
        ("surface_bindings", ""),
        ("activation_project_stage_capture", ""),
        ("name_current", ""),
        ("children_current", ""),
    ];
    let mut snapshot = Vec::new();
    for (table, generated_id) in tables {
        let generated = if generated_id.is_empty() {
            String::new()
        } else {
            format!(" - '{generated_id}'")
        };
        let row = if matches!(table, "name_current" | "children_current") {
            "jsonb_set(
                 to_jsonb(value), '{provenance}',
                 (to_jsonb(value) -> 'provenance')
                     - 'selected_event_ids' - 'normalized_event_ids'
             )"
        } else {
            "to_jsonb(value)"
        };
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(row ORDER BY row::text), '[]'::jsonb)
             FROM (
                 SELECT {row} {generated}
                        - 'observed_at' - 'inserted_at' - 'last_recomputed_at'
                        AS row
                 FROM {table} value
             ) semantic"
        );
        let mut rows: serde_json::Value = sqlx::query_scalar(&sql).fetch_one(pool).await?;
        remove_generated_event_ids(&mut rows);
        snapshot.push((table.to_owned(), rows));
    }
    let closure_positions: serde_json::Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(value) ORDER BY value.surface_binding_id), '[]'::jsonb)
         FROM (
             SELECT surface_binding_id, authority_arm, active_to
             FROM surface_bindings
             WHERE active_to IS NOT NULL
         ) value",
    )
    .fetch_one(pool)
    .await?;
    snapshot.push(("binding_closure_positions".to_owned(), closure_positions));
    Ok(snapshot)
}

fn remove_generated_event_ids(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                remove_generated_event_ids(value);
            }
        }
        serde_json::Value::Object(object) => {
            let generated_keys = object
                .iter()
                .filter(|(key, value)| key.ends_with("_event_id") && value.is_number())
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in generated_keys {
                object.remove(&key);
            }
            object.remove("selected_event_ids");
            object.remove("normalized_event_ids");
            object.remove("normalized_event_id");
            object.remove("observed_at");
            object.remove("inserted_at");
            object.remove("last_recomputed_at");
            for value in object.values_mut() {
                remove_generated_event_ids(value);
            }
        }
        _ => {}
    }
}

pub(super) fn first_json_difference(
    left: &serde_json::Value,
    right: &serde_json::Value,
    path: &str,
) -> String {
    match (left, right) {
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            if left.len() != right.len() {
                return format!("{path} length {} != {}", left.len(), right.len());
            }
            for (index, (left, right)) in left.iter().zip(right).enumerate() {
                if left != right {
                    return first_json_difference(left, right, &format!("{path}[{index}]"));
                }
            }
        }
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            for key in left.keys().chain(right.keys()) {
                if left.get(key) != right.get(key) {
                    return match (left.get(key), right.get(key)) {
                        (Some(left), Some(right)) => {
                            first_json_difference(left, right, &format!("{path}.{key}"))
                        }
                        values => format!("{path}.{key}: {values:?}"),
                    };
                }
            }
        }
        _ => return format!("{path}: {left} != {right}"),
    }
    format!("{path}: values differ")
}
