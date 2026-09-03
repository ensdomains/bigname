#[allow(dead_code)]
mod support;

use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use alloy_primitives::keccak256;
use anyhow::Result;
use bigname_manifests::{ManifestRepository, load_repository};
use phase_runner::{
    error::{ErrorKind, RunnerError},
    manifest_startup::sync_loaded_manifests,
};

use support::ScratchDatabase;

#[tokio::test]
async fn sequential_single_ens_deployment_refuses_before_manifest_sync() -> Result<()> {
    let scratch = ScratchDatabase::create("sequential_ens_deployment_refusal").await?;
    let mainnet_root = manifest_root("mainnet");
    let mainnet_repository = load_repository(&mainnet_root)?;
    sync_loaded_manifests(
        scratch.pool(),
        &mainnet_root,
        &mainnet_repository,
        "mainnet",
    )
    .await?;
    seed_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;
    seed_chain_head(scratch.pool(), "base-mainnet", 30_000_000).await?;
    seed_retained_ens_name(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;

    let before = manifest_control_snapshot(scratch.pool()).await?;
    assert!(
        before["manifest_versions"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty()),
        "the pre-sync manifest snapshot must be nonempty"
    );
    let retained_chain: String =
        sqlx::query_scalar("SELECT DISTINCT chain_id FROM name_surfaces WHERE namespace = 'ens'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(retained_chain, "ethereum-mainnet");

    let sepolia_root = manifest_root("sepolia");
    let sepolia_repository = load_repository(&sepolia_root)?;
    assert_eq!(ens_chains(&sepolia_repository), ["ethereum-sepolia"]);

    let error = sync_loaded_manifests(
        scratch.pool(),
        &sepolia_root,
        &sepolia_repository,
        "sepolia",
    )
    .await
    .expect_err("a different ENS deployment must be refused before synchronization");
    println!("deployment-profile synchronization error: {error:#}");
    let runner_error = error
        .downcast_ref::<RunnerError>()
        .expect("the refusal must be a classified runner error");
    assert_eq!(runner_error.kind(), ErrorKind::Configuration);
    let message = runner_error.to_string();
    for expected in [
        "ethereum-mainnet",
        "ethereum-sepolia",
        "separate database/schema",
        "explicitly reviewed full phase-schema replacement procedure",
    ] {
        assert!(
            message.contains(expected),
            "missing {expected:?}: {message}"
        );
    }

    let after = manifest_control_snapshot(scratch.pool()).await?;
    assert_eq!(
        after, before,
        "refusal must not mutate manifest-control rows"
    );
    println!(
        "manifest snapshot digest: {:#x}",
        keccak256(before.to_string())
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn active_ens_manifests_refuse_another_deployment_without_name_surfaces() -> Result<()> {
    let scratch = ScratchDatabase::create("active_ens_manifest_refusal").await?;
    let mainnet_root = manifest_root("mainnet");
    let mainnet_repository = load_repository(&mainnet_root)?;
    sync_loaded_manifests(
        scratch.pool(),
        &mainnet_root,
        &mainnet_repository,
        "mainnet",
    )
    .await?;

    let retained_surfaces: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_surfaces WHERE namespace = 'ens'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(retained_surfaces, 0);
    let active_ens_chains: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT chain_id FROM manifest_versions
         WHERE namespace = 'ens' AND rollout_status = 'active' ORDER BY chain_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(active_ens_chains, ["ethereum-mainnet"]);

    sync_loaded_manifests(
        scratch.pool(),
        &mainnet_root,
        &mainnet_repository,
        "mainnet",
    )
    .await?;
    let before = manifest_control_snapshot(scratch.pool()).await?;

    let sepolia_root = manifest_root("sepolia");
    let sepolia_repository = load_repository(&sepolia_root)?;
    let error = sync_loaded_manifests(
        scratch.pool(),
        &sepolia_root,
        &sepolia_repository,
        "sepolia",
    )
    .await
    .expect_err("active ENS manifests must retain their deployment chain");
    let runner_error = error
        .downcast_ref::<RunnerError>()
        .expect("the refusal must be a classified runner error");
    assert_eq!(runner_error.kind(), ErrorKind::Configuration);
    let message = runner_error.to_string();
    assert!(message.contains("ethereum-mainnet"), "{message}");
    assert!(message.contains("ethereum-sepolia"), "{message}");

    let after = manifest_control_snapshot(scratch.pool()).await?;
    assert_eq!(after, before, "refusal must precede manifest mutation");
    scratch.cleanup().await
}

#[tokio::test]
async fn concurrent_startup_waits_then_revalidates_active_ens_manifests() -> Result<()> {
    let scratch = ScratchDatabase::create("concurrent_ens_manifest_refusal").await?;
    let mut table_lock = scratch.pool().begin().await?;
    sqlx::query("LOCK TABLE manifest_contract_instances IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *table_lock)
        .await?;

    let mainnet_root = Arc::new(manifest_root("mainnet"));
    let mainnet_repository = Arc::new(load_repository(mainnet_root.as_path())?);
    let mainnet_pool = scratch.pool().clone();
    let first = tokio::spawn(async move {
        sync_loaded_manifests(
            &mainnet_pool,
            mainnet_root.as_path(),
            mainnet_repository.as_ref(),
            "mainnet",
        )
        .await
    });
    wait_for_manifest_declaration_lock_waiter(scratch.pool()).await?;

    let mut probe = scratch.pool().begin().await?;
    let startup_lock_available: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(hashtextextended(
             'phase-runner:manifest-startup', 0::bigint
         ))",
    )
    .fetch_one(&mut *probe)
    .await?;
    assert!(
        !startup_lock_available,
        "the first synchronization must retain the startup lock while its writes wait"
    );
    drop(probe);

    let sepolia_root = Arc::new(manifest_root("sepolia"));
    let sepolia_repository = Arc::new(load_repository(sepolia_root.as_path())?);
    let sepolia_pool = scratch.pool().clone();
    let mut second = tokio::spawn(async move {
        sync_loaded_manifests(
            &sepolia_pool,
            sepolia_root.as_path(),
            sepolia_repository.as_ref(),
            "sepolia",
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut second)
            .await
            .is_err(),
        "the second synchronization must wait for the startup lock"
    );

    table_lock.commit().await?;
    first.await??;
    let before = manifest_control_snapshot(scratch.pool()).await?;
    let error = second
        .await
        .expect("the second synchronization task must not panic")
        .expect_err("the second deployment must revalidate after waiting");
    let runner_error = error
        .downcast_ref::<RunnerError>()
        .expect("the refusal must be a classified runner error");
    assert_eq!(runner_error.kind(), ErrorKind::Configuration);
    assert_eq!(manifest_control_snapshot(scratch.pool()).await?, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn terminated_startup_session_aborts_manifest_transaction() -> Result<()> {
    let scratch = ScratchDatabase::create("terminated_manifest_startup_session").await?;
    let before = manifest_control_snapshot(scratch.pool()).await?;
    let mut table_lock = scratch.pool().begin().await?;
    sqlx::query("LOCK TABLE manifest_contract_instances IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *table_lock)
        .await?;

    let mainnet_root = Arc::new(manifest_root("mainnet"));
    let mainnet_repository = Arc::new(load_repository(mainnet_root.as_path())?);
    let mainnet_pool = scratch.pool().clone();
    let first = tokio::spawn(async move {
        sync_loaded_manifests(
            &mainnet_pool,
            mainnet_root.as_path(),
            mainnet_repository.as_ref(),
            "mainnet",
        )
        .await
    });
    wait_for_manifest_declaration_lock_waiter(scratch.pool()).await?;

    let startup_pid = startup_lock_backend_pid(scratch.pool()).await?;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(startup_pid)
        .fetch_one(scratch.pool())
        .await?;
    assert!(terminated, "the startup-lock backend must be terminated");
    table_lock.rollback().await?;

    first
        .await
        .expect("the first synchronization task must not panic")
        .expect_err("losing the startup session must fail synchronization");
    assert_eq!(
        manifest_control_snapshot(scratch.pool()).await?,
        before,
        "losing the startup session must abort all manifest mutation"
    );

    let sepolia_root = manifest_root("sepolia");
    let sepolia_repository = load_repository(&sepolia_root)?;
    sync_loaded_manifests(
        scratch.pool(),
        &sepolia_root,
        &sepolia_repository,
        "sepolia",
    )
    .await?;
    let active_ens_chains: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT chain_id FROM manifest_versions
         WHERE namespace = 'ens' AND rollout_status = 'active' ORDER BY chain_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(active_ens_chains, ["ethereum-sepolia"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn empty_or_matching_retained_ens_profile_is_admitted() -> Result<()> {
    for (case, profile, retained_chain) in [
        ("empty", "sepolia", None),
        ("matching", "mainnet", Some("ethereum-mainnet")),
    ] {
        let scratch = ScratchDatabase::create(&format!("ens_profile_admitted_{case}")).await?;
        if let Some(chain_id) = retained_chain {
            seed_chain_head(scratch.pool(), chain_id, 30_000_000).await?;
            seed_retained_ens_name(scratch.pool(), chain_id, 30_000_000).await?;
        }
        let root = manifest_root(profile);
        let repository = load_repository(&root)?;
        let incoming = ens_chains(&repository);
        assert_eq!(
            incoming.len(),
            1,
            "{case} repository must have one ENS chain"
        );

        sync_loaded_manifests(scratch.pool(), &root, &repository, profile).await?;
        let stored: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT chain_id FROM manifest_versions
             WHERE namespace = 'ens' AND rollout_status = 'active' ORDER BY chain_id",
        )
        .fetch_all(scratch.pool())
        .await?;
        assert_eq!(
            stored, incoming,
            "{case} must expose the incoming ENS manifests"
        );
        let active_versions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM manifest_versions
             WHERE namespace = 'ens' AND chain_id = $1 AND rollout_status = 'active'",
        )
        .bind(&incoming[0])
        .fetch_one(scratch.pool())
        .await?;
        assert!(
            active_versions > 0,
            "{case} must expose an active manifest version"
        );
        scratch.cleanup().await?;
    }
    Ok(())
}

fn manifest_root(profile: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests")
        .join(profile)
}

fn ens_chains(repository: &ManifestRepository) -> Vec<String> {
    repository
        .manifests()
        .iter()
        .filter(|loaded| loaded.manifest.namespace == "ens")
        .map(|loaded| loaded.manifest.chain.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn seed_chain_head(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-deployment-topology-head-{number}");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(chain_id)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_retained_ens_name(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-deployment-topology-head-{number}");
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:deployment-topology', 'ens', 'retained.eth', ARRAY['retained', 'eth'],
             decode('00', 'hex'), 'deployment-topology', ARRAY['retained', 'eth'],
             'test', 'active', $1, $2, $3, 'canonical'
         )",
    )
    .bind(chain_id)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn wait_for_manifest_declaration_lock_waiter(pool: &sqlx::PgPool) -> Result<()> {
    for _ in 0..500 {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_locks
                 WHERE database = (
                           SELECT oid FROM pg_database WHERE datname = current_database()
                       )
                   AND relation = 'bigname_phase.manifest_contract_instances'::regclass
                   AND NOT granted
             )",
        )
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("manifest synchronization did not reach the blocked declaration table")
}

async fn startup_lock_backend_pid(pool: &sqlx::PgPool) -> Result<i32> {
    Ok(sqlx::query_scalar(
        "SELECT pid
         FROM pg_locks
         WHERE locktype = 'advisory'
           AND granted
           AND database = (
               SELECT oid FROM pg_database WHERE datname = current_database()
           )
           AND classid::bigint = (
               hashtextextended('phase-runner:manifest-startup', 0::bigint) >> 32
           ) & 4294967295
           AND objid::bigint =
               hashtextextended('phase-runner:manifest-startup', 0::bigint) & 4294967295
           AND objsubid = 1",
    )
    .fetch_one(pool)
    .await?)
}

async fn manifest_control_snapshot(pool: &sqlx::PgPool) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'manifest_versions', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.manifest_id)
                 FROM (SELECT * FROM manifest_versions) row
             ), '[]'::jsonb),
             'manifest_contract_instances', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.manifest_contract_instance_id)
                 FROM (SELECT * FROM manifest_contract_instances) row
             ), '[]'::jsonb),
             'manifest_discovery_rules', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.manifest_discovery_rule_id)
                 FROM (SELECT * FROM manifest_discovery_rules) row
             ), '[]'::jsonb),
             'contract_instances', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.contract_instance_id)
                 FROM (
                     SELECT instance.* FROM contract_instances instance
                     WHERE EXISTS (
                         SELECT 1 FROM manifest_contract_instances declaration
                         WHERE declaration.contract_instance_id = instance.contract_instance_id
                     )
                 ) row
             ), '[]'::jsonb),
             'contract_instance_addresses', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.contract_instance_address_id)
                 FROM (
                     SELECT address.* FROM contract_instance_addresses address
                     WHERE EXISTS (
                         SELECT 1 FROM manifest_contract_instances declaration
                         WHERE declaration.contract_instance_id = address.contract_instance_id
                     )
                 ) row
             ), '[]'::jsonb),
             'discovery_edges', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.discovery_edge_id)
                 FROM (
                     SELECT edge.* FROM discovery_edges edge
                     WHERE edge.discovery_source IN ('manifest', 'manifest_declared_proxy')
                 ) row
             ), '[]'::jsonb),
             'normalized_events', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.normalized_event_id)
                 FROM (
                     SELECT * FROM normalized_events
                     WHERE event_kind = 'SourceManifestUpdated'
                 ) row
             ), '[]'::jsonb),
             'chain_phase_state', COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY row.chain_id, row.phase_name)
                 FROM (SELECT * FROM chain_phase_state) row
             ), '[]'::jsonb)
         )",
    )
    .fetch_one(pool)
    .await?)
}
