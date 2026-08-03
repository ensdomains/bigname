#[allow(dead_code)]
mod support;

use anyhow::Result;
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use phase_runner::{INTERPRETER_CONTENT_HASH, state::PhaseStore};
use sqlx::types::Uuid;

use support::ScratchDatabase;

type AddressEpoch = (i64, Uuid, Option<i64>, Option<i64>, bool);

#[tokio::test]
async fn schema_v2_manifest_sync_is_idempotent_and_retires_absent_history() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = load_repository(&root)?;

    let first = sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let manifest_ids: Vec<i64> =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions ORDER BY manifest_id")
            .fetch_all(scratch.pool())
            .await?;
    let first_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'SourceManifestUpdated'",
    )
    .fetch_one(scratch.pool())
    .await?;
    let second = sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let repeated_manifest_ids: Vec<i64> =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions ORDER BY manifest_id")
            .fetch_all(scratch.pool())
            .await?;

    assert!(first.manifest_count > 0);
    assert!(first.declaration_count > 0);
    assert!(first.discovery_rule_count > 0);
    assert_eq!(first, second);
    assert_eq!(manifest_ids, repeated_manifest_ids);
    assert_eq!(first_event_count, first.manifest_count as i64);
    let repeated_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'SourceManifestUpdated'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repeated_event_count, first_event_count);
    let mismatched_epoch_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM manifest_versions
        WHERE deployment_label <> manifest_payload ->> 'deployment_epoch'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(mismatched_epoch_count, 0);

    let retained_manifest: (i64, String, i64) = sqlx::query_as(
        "
        SELECT manifest.manifest_id,
               manifest.rollout_status,
               count(declaration.manifest_contract_instance_id)
        FROM manifest_versions manifest
        LEFT JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
        WHERE manifest.namespace = 'ens'
          AND manifest.source_family = 'ens_v1_reverse_l1'
          AND manifest.chain_id = 'ethereum-mainnet'
        GROUP BY manifest.manifest_id, manifest.rollout_status
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(retained_manifest.1, "active");
    assert!(retained_manifest.2 > 0);

    seed_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;
    let base_repository = load_repository(root.join("base"))?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    let after_subset_sync: (i64, String, i64) = sqlx::query_as(
        "
        SELECT manifest.manifest_id,
               manifest.rollout_status,
               count(declaration.manifest_contract_instance_id)
        FROM manifest_versions manifest
        LEFT JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
        WHERE manifest.namespace = 'ens'
          AND manifest.source_family = 'ens_v1_reverse_l1'
          AND manifest.chain_id = 'ethereum-mainnet'
        GROUP BY manifest.manifest_id, manifest.rollout_status
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after_subset_sync.0, retained_manifest.0);
    assert_eq!(after_subset_sync.1, "deprecated");
    assert_eq!(after_subset_sync.2, retained_manifest.2);
    let manifest_event_states: Vec<(Option<String>, String)> = sqlx::query_as(
        "
        SELECT before_state ->> 'rollout_status',
               after_state ->> 'rollout_status'
        FROM normalized_events
        WHERE source_manifest_id = $1
          AND event_kind = 'SourceManifestUpdated'
        ORDER BY normalized_event_id
        ",
    )
    .bind(retained_manifest.0)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        manifest_event_states,
        [
            (None, "active".into()),
            (Some("active".into()), "deprecated".into()),
        ]
    );

    sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let manifest_event_states: Vec<(Option<String>, String)> = sqlx::query_as(
        "
        SELECT before_state ->> 'rollout_status',
               after_state ->> 'rollout_status'
        FROM normalized_events
        WHERE source_manifest_id = $1
          AND event_kind = 'SourceManifestUpdated'
        ORDER BY normalized_event_id
        ",
    )
    .bind(retained_manifest.0)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        manifest_event_states,
        [
            (None, "active".into()),
            (Some("active".into()), "deprecated".into()),
            (Some("deprecated".into()), "active".into()),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn schema_v2_manifest_sync_refuses_a_running_chain_phase() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_phase_lock").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = load_repository(&root)?;
    let mut lock_connection = scratch.pool().acquire().await?;
    let lock_name = "phase-runner:ethereum-mainnet:interpret";
    let acquired: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(lock_name)
            .fetch_one(&mut *lock_connection)
            .await?;
    assert!(acquired);

    let error = sync_schema_v2_repository(scratch.pool(), &repository)
        .await
        .expect_err("manifest sync must not race a running phase");
    assert!(error.to_string().contains("phase advisory lock"));
    let released: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
            .bind(lock_name)
            .fetch_one(&mut *lock_connection)
            .await?;
    assert!(released);
    drop(lock_connection);

    let manifests: i64 = sqlx::query_scalar("SELECT count(*) FROM manifest_versions")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(manifests, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn schema_v2_manifest_authority_change_requires_derived_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_invalidation").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(scratch.pool(), &load_repository(&root)?).await?;

    let chain_id = "ethereum-mainnet";
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(chain_id)
        .await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            input_content_hash = $2,
            started_at = now(),
            finished_at = now()
        WHERE chain_id = $1
          AND phase_name IN ('interpret', 'project')
        ",
    )
    .bind(chain_id)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;

    seed_chain_head(scratch.pool(), chain_id, 30_000_000).await?;
    let base_repository = load_repository(root.join("base"))?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    let hashes: Vec<String> = sqlx::query_scalar(
        "
        SELECT input_content_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name IN ('interpret', 'project')
        ORDER BY phase_name
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(hashes.len(), 2);
    assert!(
        hashes
            .iter()
            .all(|hash| hash.starts_with("manifest-authority:"))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn basenames_execution_retirement_invalidates_the_base_project_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_basenames_dependency").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(scratch.pool(), &load_repository(&root)?).await?;

    let store = PhaseStore::new(scratch.pool().clone());
    for chain_id in ["ethereum-mainnet", "base-mainnet"] {
        store.initialize_chain(chain_id).await?;
        sqlx::query(
            "
            UPDATE chain_phase_state
            SET phase_status = 'completed',
                input_content_hash = $2,
                started_at = now(),
                finished_at = now()
            WHERE chain_id = $1
              AND phase_name IN ('interpret', 'project')
            ",
        )
        .bind(chain_id)
        .bind(INTERPRETER_CONTENT_HASH)
        .execute(scratch.pool())
        .await?;
    }

    seed_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(root.join("base"))?).await?;

    let project_hashes: Vec<(String, String)> = sqlx::query_as(
        "
        SELECT chain_id, input_content_hash
        FROM chain_phase_state
        WHERE chain_id IN ('ethereum-mainnet', 'base-mainnet')
          AND phase_name = 'project'
        ORDER BY chain_id
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(project_hashes.len(), 2);
    assert!(
        project_hashes
            .iter()
            .all(|(_, hash)| hash.starts_with("manifest-authority:")),
        "both the Ethereum authority owner and its Base projection consumer must redo: {project_hashes:?}"
    );
    let base_interpret_hash: String = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = 'base-mainnet' AND phase_name = 'interpret'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(base_interpret_hash, INTERPRETER_CONTENT_HASH);
    scratch.cleanup().await
}

#[tokio::test]
async fn schema_v2_manifest_readmission_appends_an_address_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_address_epoch").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = load_repository(&root)?;
    sync_schema_v2_repository(scratch.pool(), &repository).await?;

    let address = "0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb";
    let (first_row_id, instance_id, active_from): (i64, Uuid, Option<i64>) = sqlx::query_as(
        "
        SELECT contract_instance_address_id,
               contract_instance_id,
               active_from_block_number
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND lower(address) = $1
          AND deactivated_at IS NULL
        ",
    )
    .bind(address)
    .fetch_one(scratch.pool())
    .await?;

    let inactive_at = 30_000_000;
    seed_chain_head(scratch.pool(), "ethereum-mainnet", inactive_at).await?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(root.join("base"))?).await?;
    sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let rows: Vec<AddressEpoch> = sqlx::query_as(
        "
        SELECT contract_instance_address_id,
               contract_instance_id,
               active_from_block_number,
               active_to_block_number,
               deactivated_at IS NULL
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND lower(address) = $1
        ORDER BY contract_instance_address_id
        ",
    )
    .bind(address)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        (
            first_row_id,
            instance_id,
            active_from,
            Some(inactive_at),
            false
        )
    );
    assert_eq!(rows[1].1, instance_id);
    assert_eq!(rows[1].2, Some(inactive_at + 1));
    assert_eq!(rows[1].3, None);
    assert!(rows[1].4);
    scratch.cleanup().await
}

async fn seed_chain_head(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-manifest-sync-head-{number}");
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, to_timestamp($3), 'canonical')
        ",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(number)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
        VALUES ($1, $2, $3)
        ",
    )
    .bind(chain_id)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}
