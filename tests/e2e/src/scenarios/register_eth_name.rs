use std::collections::HashMap;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::harness::{anvil::Anvil, db::HarnessDb, ens_v1, manifests, pipeline, repo_root};

/// Walking skeleton: deploy the pinned ENSv1 stack onto a local chain,
/// register alice.eth through the real registrar controller, ingest the
/// chain with the real indexer, replay projections with the real worker,
/// and assert at three layers — persisted raw logs, normalized events, and
/// public API output. Verified-resolution (execution-trace) coverage is
/// deliberately out of scope for this scenario: no execution RPC is
/// configured.
#[tokio::test]
async fn register_eth_name_end_to_end() -> Result<()> {
    let repo_root = repo_root();
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    // --- on-chain scenario ---
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root).await?;
    let user = rpc.accounts().await?[1];
    let registered = ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "alice",
        user,
        365 * 24 * 60 * 60,
        deployment.public_resolver.address,
    )
    .await?;
    rpc.mine(2).await?;
    let head = rpc.block_number().await?;

    // --- manifest profile for the local chain ---
    let scratch = tempdir()?;
    let local_targets: HashMap<&str, (alloy_primitives::Address, u64)> = HashMap::from([
        (
            "ENSRegistry",
            (
                deployment.registry.address,
                deployment.registry.block_number,
            ),
        ),
        (
            "registry",
            (
                deployment.registry.address,
                deployment.registry.block_number,
            ),
        ),
        (
            "ETHRegistrar",
            (
                deployment.base_registrar.address,
                deployment.base_registrar.block_number,
            ),
        ),
        (
            "registrar",
            (
                deployment.base_registrar.address,
                deployment.base_registrar.block_number,
            ),
        ),
        (
            "unwrapped_registrar_controller",
            (
                deployment.controller.address,
                deployment.controller.block_number,
            ),
        ),
        (
            "public_resolver",
            (
                deployment.public_resolver.address,
                deployment.public_resolver.block_number,
            ),
        ),
        (
            "reverse_registrar",
            (
                deployment.reverse_registrar.address,
                deployment.reverse_registrar.block_number,
            ),
        ),
        (
            "name_wrapper",
            (
                deployment.name_wrapper.address,
                deployment.name_wrapper.block_number,
            ),
        ),
    ]);
    let profile = manifests::generate_local_profile(scratch.path(), &repo_root, &local_targets)?;

    // --- pipeline: live intake -> projections ---
    let db = HarnessDb::create().await?;
    pipeline::indexer_run_until_checkpoint(
        &repo_root,
        &db.url,
        &db.pool,
        &profile.root,
        &anvil.url,
        head,
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:alice.eth' AND canonicality_state = 'canonical')",
        ),
    )
    .await?;
    pipeline::worker_replay_all_current_projections(&repo_root, &db.url).await?;

    // --- layer 1: raw facts ---
    // The controller emits label-bearing NameRegistered at the register block
    // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116 @ ens_v1@91c966f).
    let controller_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs WHERE emitting_address = $1 AND block_number = $2",
    )
    .bind(format!("{:#x}", deployment.controller.address))
    .bind(registered.register_block as i64)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        controller_logs >= 1,
        "expected controller logs persisted at register block {}",
        registered.register_block
    );

    let alice_node = format!("{:#x}", ens_v1::namehash("alice.eth"));
    let registry_topic0s: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT topics[1] FROM raw_logs WHERE emitting_address = $1 AND topics[2] = $2",
    )
    .bind(format!("{:#x}", deployment.registry.address))
    .bind(&alice_node)
    .fetch_all(&db.pool)
    .await?;
    // NewResolver(bytes32,address) — the register call carried a resolver, so
    // the registry must have observed the binding on-chain
    // (upstream: .refs/ens_v1/contracts/registry/ENS.sol @ ens_v1@91c966f).
    let new_resolver_topic0 = format!(
        "{:#x}",
        alloy_primitives::keccak256("NewResolver(bytes32,address)")
    );
    assert!(
        registry_topic0s.contains(&new_resolver_topic0),
        "expected registry NewResolver raw log for alice.eth node; saw {registry_topic0s:?}"
    );

    // --- layer 2: normalized events ---
    let event_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth' AND canonicality_state = 'canonical'",
    )
    .fetch_all(&db.pool)
    .await?;
    for expected in [
        "RegistrationGranted",
        "SurfaceBound",
        "ExpiryChanged",
        "AuthorityEpochChanged",
    ] {
        assert!(
            event_kinds.iter().any(|kind| kind == expected),
            "expected canonical {expected} normalized event for ens:alice.eth; saw {event_kinds:?}"
        );
    }

    // --- layer 4: public API output ---
    let api = pipeline::ApiServer::start(&repo_root, &db.url).await?;
    let (status, body) = api.get_json("/v1/names/ens/alice.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    let pointer = |path: &str| -> Value { body.pointer(path).cloned().unwrap_or(Value::Null) };
    assert_eq!(
        pointer("/data/normalized_name"),
        "alice.eth",
        "body: {body}"
    );
    assert_eq!(pointer("/data/logical_name_id"), "ens:alice.eth");
    assert_eq!(pointer("/data/binding_kind"), "declared_registry_path");
    assert_eq!(pointer("/coverage/status"), "full");
    assert_eq!(pointer("/coverage/exhaustiveness"), "authoritative");
    assert_eq!(pointer("/declared_state/registration/status"), "active");
    assert_eq!(
        pointer("/declared_state/registration/registrant"),
        format!("{user:#x}"),
        "registrant should be the registering account"
    );
    let expiry = pointer("/declared_state/registration/expiry")
        .as_u64()
        .context("registration expiry missing")?;
    let registered_for = expiry - 365 * 24 * 60 * 60;
    assert!(
        (crate::harness::anvil::GENESIS_TIMESTAMP..crate::harness::anvil::GENESIS_TIMESTAMP + 300)
            .contains(&registered_for),
        "expiry {expiry} should be ~duration past the warped genesis timestamp"
    );

    db.cleanup().await?;
    Ok(())
}

fn tempdir() -> Result<tempfile_lite::TempDir> {
    tempfile_lite::TempDir::create()
}

/// Minimal scratch-dir helper so the package does not need the tempfile crate.
mod tempfile_lite {
    use std::path::{Path, PathBuf};

    pub struct TempDir(PathBuf);

    impl TempDir {
        pub fn create() -> anyhow::Result<Self> {
            let dir = std::env::temp_dir().join(format!(
                "bigname-e2e-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .subsec_nanos()
            ));
            std::fs::create_dir_all(&dir)?;
            Ok(Self(dir))
        }

        pub fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
