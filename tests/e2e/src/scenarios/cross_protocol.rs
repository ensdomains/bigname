use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result};
use serde_json::Value;

use super::support::{self, TempDir};
use crate::harness::{
    anvil::Anvil, basenames, db::HarnessDb, ens_v1, ens_v2_migration, manifests, pipeline,
    repo_root, responses::pointer,
};

const YEAR: u64 = 365 * 24 * 60 * 60;
const ETH_REORG_CHAIN: &str = "ethereum-e2e-composed-reorg";
const BASE_REORG_CHAIN: &str = "base-e2e-composed-reorg";

/// Strip corpus-minted identifiers and read-time fields so route bodies
/// from two corpora over the SAME chain compare equal on everything that
/// matters (chain positions, hashes, and timestamps are chain-derived and
/// identical by construction).
fn strip_corpus_minted(value: &mut Value) {
    const VOLATILE: &[&str] = &[
        "resource_id",
        "token_lineage_id",
        "surface_binding_id",
        "normalized_event_id",
        "normalized_event_ids",
        "selected_event_ids",
        "source_manifest_id",
        "interpreter_state_key",
        "last_updated",
    ];
    match value {
        Value::Object(map) => {
            for key in VOLATILE {
                map.remove(*key);
            }
            // authority_key's third segment is the corpus-minted contract
            // instance ordinal; everything else in it is chain-derived.
            if let Some(Value::String(key)) = map.get_mut("authority_key") {
                let mut parts: Vec<&str> = key.split(':').collect();
                if parts.len() > 3 {
                    parts[2] = "N";
                    *key = parts.join(":");
                }
            }
            for entry in map.values_mut() {
                strip_corpus_minted(entry);
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                strip_corpus_minted(entry);
            }
        }
        _ => {}
    }
}

async fn body(run: &support::PipelineRun, path: &str) -> Result<(u16, Value)> {
    let (status, body) = run.api.get_json(path).await?;
    Ok((status.as_u16(), body))
}

async fn parent_migration_path(run: &support::PipelineRun, parent: &str) -> Result<String> {
    let logical_name_id = format!("ens:{:#x}", ens_v1::namehash(parent));
    let rows: Vec<(String, String, String, bool)> = sqlx::query_as(
        "SELECT after_state->>'migration_path', consumer_visibility, \
                after_state->>'consumer_visibility', \
                (after_state->>'candidate_authority_transition')::boolean \
         FROM normalized_events \
         WHERE source_family = 'ens_v2_migration_l1' \
           AND event_kind = 'MigrationApplied' \
           AND canonicality_state = 'canonical' \
           AND logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        rows.len(),
        1,
        "expected one activated parent migration for {parent}"
    );
    assert_eq!(rows[0].1, "activated");
    assert_eq!(rows[0].2, "activated");
    assert!(!rows[0].3);
    Ok(rows[0].0.clone())
}

#[tokio::test]
async fn unlocked_parent_hides_retained_ens_v1_children() -> Result<()> {
    let harness = support::deploy_connected_migration_harness().await?;
    let path = support::create_unlocked_migration_path(&harness).await?;
    assert_eq!(
        path.child_owner_after_clear,
        harness.migration.graveyard.address
    );
    assert_ne!(path.child_owner_after_clear, Address::ZERO);

    let run = support::ingest_ens_v1_v2_migration_sepolia_and_serve(
        &harness.anvil,
        &harness.ens_v1,
        &harness.ens_v2,
        &harness.migration,
        None,
    )
    .await?;
    assert_eq!(
        parent_migration_path(&run, "unlock-migration.eth").await?,
        "unlocked_wrapped"
    );
    let child_owner_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE source_family = 'ens_v1_registry_l1' \
           AND event_kind = 'SubregistryChanged' \
           AND canonicality_state = 'canonical' \
           AND after_state->>'source_event' = 'NewOwner' \
           AND after_state->>'child_node' = $1 \
           AND lower(after_state->>'owner') = $2",
    )
    .bind(format!(
        "{:#x}",
        ens_v1::namehash("child.unlock-migration.eth")
    ))
    .bind(format!("{:#x}", path.child_owner_before_migration))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        child_owner_events, 1,
        "the pre-migration ENSv1 child owner fact was not interpreted"
    );
    let (children_status, children_body) =
        body(&run, "/v1/names/ens/unlock-migration.eth/children").await?;
    assert_eq!(
        children_status, 200,
        "children route failed: {children_body}"
    );
    let children = children_body
        .pointer("/data")
        .and_then(Value::as_array)
        .context("children response lacks data array")?;
    assert!(
        children.iter().all(|child| {
            child["normalized_name"] != "child.unlock-migration.eth"
                && child["logical_name_id"]
                    != format!("ens:{:#x}", ens_v1::namehash("child.unlock-migration.eth"))
        }),
        "unlocked migration retained the ENSv1 child: {children_body}"
    );
    println!(
        "unlocked reachability: graveyard={:#x} children_status={} child_listed=false",
        path.child_owner_after_clear, children_status
    );
    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn locked_parent_publishes_only_migratable_ens_v1_children() -> Result<()> {
    let harness = support::deploy_connected_migration_harness().await?;
    let path = support::create_locked_migration_path(&harness).await?;
    let rpc = harness.anvil.client();
    let bridged_expiry =
        ens_v2_migration::registry_expiry(&rpc, path.wrapper_registry, "bridged").await?;
    let blocked_expiry =
        ens_v2_migration::registry_expiry(&rpc, path.wrapper_registry, "blocked").await?;
    assert_eq!(bridged_expiry, 0);
    assert_eq!(blocked_expiry, 0);
    assert_ne!(path.bridged_owner, Address::ZERO);
    assert_ne!(path.blocked_owner, Address::ZERO);
    assert_ne!(path.bridged.fuses & ens_v1::PARENT_CANNOT_CONTROL, 0);
    assert_eq!(path.blocked.fuses & ens_v1::PARENT_CANNOT_CONTROL, 0);
    assert_eq!(path.bridged.fuses & ens_v1::IS_DOT_ETH, 0);
    assert_eq!(path.blocked.fuses & ens_v1::IS_DOT_ETH, 0);

    let run = support::ingest_ens_v1_v2_migration_sepolia_and_serve(
        &harness.anvil,
        &harness.ens_v1,
        &harness.ens_v2,
        &harness.migration,
        None,
    )
    .await?;
    assert_eq!(
        parent_migration_path(&run, "locked-migration.eth").await?,
        "locked_wrapped"
    );
    let bridged_logical_name_id = format!(
        "ens:{:#x}",
        ens_v1::namehash("bridged.locked-migration.eth")
    );
    let blocked_node = format!("{:#x}", ens_v1::namehash("blocked.locked-migration.eth"));
    let blocked_logical_name_id = format!("ens:{blocked_node}");
    let (blocked_registry_input, blocked_wrapper_input): (bool, bool) = sqlx::query_as(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
             WHERE source_family = 'ens_v1_registry_l1' \
               AND event_kind = 'SubregistryChanged' \
               AND canonicality_state = 'canonical' \
               AND after_state->>'source_event' = 'NewOwner' \
               AND after_state->>'child_node' = $1), \
           EXISTS (SELECT 1 FROM normalized_events \
             WHERE source_family = 'ens_v1_wrapper_l1' \
               AND event_kind = 'PermissionScopeChanged' \
               AND canonicality_state = 'canonical' \
               AND logical_name_id = $2 \
               AND after_state->>'source_event' = 'NameWrapped' \
               AND (after_state->>'fuses')::BIGINT = 0)",
    )
    .bind(&blocked_node)
    .bind(&blocked_logical_name_id)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        blocked_registry_input && blocked_wrapper_input,
        "blocked child inputs did not reach normalized events"
    );
    let (status, children_body) = body(&run, "/v1/names/ens/locked-migration.eth/children").await?;
    assert_eq!(status, 200, "children route failed: {children_body}");
    let children = children_body
        .pointer("/data")
        .and_then(Value::as_array)
        .context("children response lacks data array")?;
    let tested = children
        .iter()
        .filter(|child| {
            child["logical_name_id"] == bridged_logical_name_id
                || child["logical_name_id"] == blocked_logical_name_id
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tested.len(),
        1,
        "expected exactly one tested child: {children_body}"
    );
    let bridged = tested[0];
    assert_eq!(bridged["normalized_name"], "bridged.locked-migration.eth");
    let manifest_versions = bridged
        .pointer("/provenance/manifest_versions")
        .and_then(Value::as_array)
        .context("bridged child lacks provenance manifest versions")?;
    assert!(
        manifest_versions
            .iter()
            .any(|version| version["source_family"] == "ens_v1_registry_l1"),
        "bridged child lost ENSv1 provenance: {bridged}"
    );
    assert!(
        children
            .iter()
            .all(|child| child["logical_name_id"] != blocked_logical_name_id),
        "blocked child was published: {children_body}"
    );
    println!(
        "locked reachability: proxy={:#x} bridged_expiry={} blocked_expiry={} children_status={} bridged_listed=true blocked_listed=false",
        path.wrapper_registry, bridged_expiry, blocked_expiry, status
    );
    run.db.cleanup().await?;
    Ok(())
}

fn collect_diffs(lhs: &Value, rhs: &Value, path: &str, out: &mut Vec<String>) {
    match (lhs, rhs) {
        (Value::Object(a), Value::Object(b)) => {
            let keys: std::collections::BTreeSet<&String> = a.keys().chain(b.keys()).collect();
            for key in keys {
                collect_diffs(
                    a.get(key.as_str()).unwrap_or(&Value::Null),
                    b.get(key.as_str()).unwrap_or(&Value::Null),
                    &format!("{path}/{key}"),
                    out,
                );
            }
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
            for (index, (left, right)) in a.iter().zip(b).enumerate() {
                collect_diffs(left, right, &format!("{path}/{index}"), out);
            }
        }
        _ if lhs != rhs => out.push(format!("{path}: composed={lhs} control={rhs}")),
        _ => {}
    }
}

fn assert_bodies_equivalent(composed: &Value, control: &Value, label: &str) {
    let mut lhs = composed.clone();
    let mut rhs = control.clone();
    strip_corpus_minted(&mut lhs);
    strip_corpus_minted(&mut rhs);
    let mut diffs = Vec::new();
    collect_diffs(&lhs, &rhs, "", &mut diffs);
    assert!(
        diffs.is_empty(),
        "composition must not change the {label} body; differing fields:\n{}",
        diffs.join("\n")
    );
}

/// Rows 1–4 and 6: one corpus ingests the eleven non-`ens_execution` mainnet
/// families — five ENSv1 intake families, four Basenames base families, and
/// the two ethereum-chain glue families. Shadow `ens_execution` is exercised
/// separately by the verified-resolution scenario. This corpus serves both
/// protocols exactly as their single-protocol baselines do, with no
/// cross-chain leakage in names, address collections, or primary candidates.
#[tokio::test]
async fn composed_mainnet_profile_serves_both_protocols_without_leakage() -> Result<()> {
    let eth = Anvil::spawn().await?;
    let base = Anvil::spawn_base_mainnet().await?;
    let eth_rpc = eth.client();
    let base_rpc = base.client();
    let root = repo_root();

    let ens_deployment = ens_v1::deploy_ens_v1(&eth_rpc, &root).await?;
    let basenames_deployment = basenames::deploy_basenames(&base_rpc, &root).await?;
    let alice = eth_rpc.accounts().await?[1];

    ens_v1::register_eth_name(
        &eth_rpc,
        &ens_deployment,
        "alice",
        alice,
        YEAR,
        ens_deployment.public_resolver.address,
    )
    .await?;
    ens_v1::set_reverse_name(&eth_rpc, &ens_deployment, alice, "alice.eth").await?;
    basenames::register_base_name(
        &base_rpc,
        &basenames_deployment,
        alice,
        "alicebase",
        alice,
        YEAR,
    )
    .await?;
    basenames::set_primary_name(
        &base_rpc,
        &basenames_deployment,
        alice,
        "alicebase.base.eth",
    )
    .await?;

    let ready_sql = "SELECT \
         EXISTS (SELECT 1 FROM normalized_events \
          WHERE logical_name_id = 'ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec' \
            AND event_kind = 'RegistrationGranted' \
            AND canonicality_state = 'canonical') \
       AND EXISTS (SELECT 1 FROM normalized_events \
          WHERE logical_name_id = 'basenames:0xd194a2d2e1620922f3d56a474bc28bfc4f71402475e8fa8a6b3e787fbd3403d3' \
            AND event_kind = 'RegistrationGranted' \
            AND canonicality_state = 'canonical')";
    let composed = support::ingest_mainnet_composed_and_serve(
        &eth,
        &ens_deployment,
        &base,
        &basenames_deployment,
        Some(ready_sql),
    )
    .await?;

    // Both chains hold their own published schema-v2 heads in one corpus.
    let published_head_chains: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT chain_id FROM chain_heads ORDER BY chain_id")
            .fetch_all(&composed.db.pool)
            .await?;
    assert_eq!(
        published_head_chains,
        vec!["base-mainnet".to_owned(), "ethereum-mainnet".to_owned()],
        "both chains must publish independent stored heads"
    );

    // Row 1: per-protocol route bodies equal the single-protocol baselines
    // over the same chains (controls ingest at the same heads).
    let (status, composed_alice) = body(&composed, "/v1/names/ens/alice.eth").await?;
    assert_eq!(status, 200, "composed alice.eth failed: {composed_alice}");
    let (status, composed_base_name) =
        body(&composed, "/v1/names/basenames/alicebase.base.eth").await?;
    assert_eq!(
        status, 200,
        "composed alicebase.base.eth failed: {composed_base_name}"
    );

    let ens_control = support::ingest_at_current_head(&eth, &ens_deployment, None).await?;
    let (status, control_alice) = body(&ens_control, "/v1/names/ens/alice.eth").await?;
    assert_eq!(status, 200, "control alice.eth failed: {control_alice}");
    ens_control.db.cleanup().await?;
    let base_control =
        support::ingest_basenames_at_current_head(&base, &basenames_deployment, None).await?;
    let (status, control_base_name) =
        body(&base_control, "/v1/names/basenames/alicebase.base.eth").await?;
    assert_eq!(
        status, 200,
        "control alicebase.base.eth failed: {control_base_name}"
    );
    base_control.db.cleanup().await?;

    assert_bodies_equivalent(&composed_alice, &control_alice, "ENSv1 exact-name");
    assert_bodies_equivalent(
        &composed_base_name,
        &control_base_name,
        "Basenames exact-name",
    );

    // Row 2: the namespace boundary at base.eth — nothing ENSv1-side, and
    // chain positions never leak across.
    let (status, ens_base) = body(&composed, "/v1/names/ens/base.eth").await?;
    assert_eq!(
        status, 404,
        "base.eth has no ENSv1-side registration in this corpus: {ens_base}"
    );
    assert!(
        composed_alice["chain_positions"].get("base").is_none()
            && composed_alice["chain_positions"]["ethereum"]["chain_id"] == "ethereum-mainnet",
        "alice.eth must carry only ethereum positions: {composed_alice}"
    );
    assert!(
        composed_base_name["chain_positions"]
            .get("ethereum")
            .is_none()
            && composed_base_name["chain_positions"]["base"]["chain_id"] == "base-mainnet",
        "alicebase.base.eth must carry only base positions: {composed_base_name}"
    );

    // Row 3: address collections stay namespace-scoped with distinct
    // backing resources.
    let (status, ens_names) = body(
        &composed,
        &format!("/v1/addresses/{alice:#x}/names?namespace=ens&relation=registrant"),
    )
    .await?;
    assert_eq!(status, 200, "ens address names failed: {ens_names}");
    let ens_entries = ens_names["data"].as_array().cloned().unwrap_or_default();
    assert_eq!(ens_entries.len(), 1, "exactly alice.eth: {ens_names}");
    assert_eq!(ens_entries[0]["normalized_name"], "alice.eth");
    let (status, base_names) = body(
        &composed,
        &format!("/v1/addresses/{alice:#x}/names?namespace=basenames&relation=registrant"),
    )
    .await?;
    assert_eq!(status, 200, "basenames address names failed: {base_names}");
    let base_entries = base_names["data"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        base_entries.len(),
        1,
        "exactly alicebase.base.eth: {base_names}"
    );
    assert_eq!(base_entries[0]["normalized_name"], "alicebase.base.eth");
    assert_ne!(
        ens_entries[0]["resource_id"], base_entries[0]["resource_id"],
        "cross-protocol names must keep distinct resources"
    );

    // Row 4: primary claims remain namespace-scoped. The generic ENSv1
    // resolver emits `NameChanged`
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f);
    // schema-v2 preserves it but does not admit it as a primary-name claim.
    // The Basenames reverse registrar remains admitted for its deployment's
    // coin type 2147492101
    // (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L8 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f).
    let (status, ens_primary) = body(
        &composed,
        &format!("/v1/primary-names/{alice:#x}?namespace=ens&coin_type=60&mode=declared"),
    )
    .await?;
    assert_eq!(status, 200, "ens primary failed: {ens_primary}");
    assert_eq!(
        pointer(&ens_primary, "/declared_state/claimed_primary_name/status"),
        "not_found"
    );
    let (status, base_primary) = body(
        &composed,
        &format!(
            "/v1/primary-names/{alice:#x}?namespace=basenames&coin_type={}&mode=declared",
            basenames::BASE_PRIMARY_COIN_TYPE
        ),
    )
    .await?;
    assert_eq!(status, 200, "base primary failed: {base_primary}");
    assert_eq!(
        pointer(&base_primary, "/declared_state/claimed_primary_name/name"),
        "alicebase.base.eth"
    );

    // Row 6: the glue families' admission syncs into the corpus as stored
    // manifest state on the ethereum chain, and their placeholder role stays
    // silent.
    let glue_manifests: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT source_family, chain_id, manifest_version FROM manifest_versions \
         WHERE source_family IN ('basenames_l1_compat', 'basenames_execution') \
         ORDER BY source_family, manifest_version",
    )
    .fetch_all(&composed.db.pool)
    .await?;
    assert_eq!(
        glue_manifests,
        vec![
            (
                "basenames_execution".to_owned(),
                "ethereum-mainnet".to_owned(),
                1,
            ),
            (
                "basenames_execution".to_owned(),
                "ethereum-mainnet".to_owned(),
                2,
            ),
            (
                "basenames_l1_compat".to_owned(),
                "ethereum-mainnet".to_owned(),
                1,
            ),
        ],
        "glue-family admission must sync on the ethereum chain"
    );
    let (status, manifest_body) = body(&composed, "/v1/manifests/basenames").await?;
    assert_eq!(
        status, 200,
        "Basenames manifest route failed: {manifest_body}"
    );
    let mut served_glue_manifests = manifest_body["declared_state"]["manifests"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let source_family = entry["source_family"].as_str()?;
            if !matches!(source_family, "basenames_l1_compat" | "basenames_execution") {
                return None;
            }
            Some((
                source_family.to_owned(),
                entry["chain"].as_str()?.to_owned(),
                entry["version"].as_i64()?,
            ))
        })
        .collect::<Vec<_>>();
    served_glue_manifests.sort();
    assert_eq!(
        served_glue_manifests, glue_manifests,
        "the public manifest route must serve both admitted ethereum-chain glue families: {manifest_body}"
    );
    let l1_resolver_placeholder =
        Address::from_slice(&keccak256("bigname-e2e-placeholder:l1_resolver".as_bytes())[12..]);
    let placeholder_logs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE lower(emitting_address) = $1")
            .bind(format!("{l1_resolver_placeholder:#x}"))
            .fetch_one(&composed.db.pool)
            .await?;
    assert_eq!(
        placeholder_logs, 0,
        "the undeployed glue role stays silent while its admission syncs"
    );

    composed.db.cleanup().await?;
    Ok(())
}

/// Row 5: a reorg on ONE chain of the composed corpus converges that chain
/// to the winning branch while the other chain's canonicality is untouched.
#[tokio::test]
async fn base_reorg_leaves_ethereum_canonicality_untouched() -> Result<()> {
    let eth = Anvil::spawn().await?;
    let base = Anvil::spawn_base_mainnet().await?;
    let eth_rpc = eth.client();
    let base_rpc = base.client();
    let root = repo_root();

    let ens_deployment = ens_v1::deploy_ens_v1(&eth_rpc, &root).await?;
    let basenames_deployment = basenames::deploy_basenames(&base_rpc, &root).await?;
    let alice = eth_rpc.accounts().await?[1];

    ens_v1::register_eth_name(
        &eth_rpc,
        &ens_deployment,
        "steady",
        alice,
        YEAR,
        Address::ZERO,
    )
    .await?;
    basenames::register_base_name(
        &base_rpc,
        &basenames_deployment,
        alice,
        "churner",
        alice,
        YEAR,
    )
    .await?;
    eth_rpc.mine(2).await?;
    base_rpc.mine(2).await?;
    let eth_head = eth_rpc.block_number().await?;
    let base_head = base_rpc.block_number().await?;
    let base_ancestor_hash = base_rpc.block_hash(base_head).await?;
    let base_snapshot = base_rpc.evm_snapshot().await?;

    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_mainnet_composed_profile(
        scratch.path(),
        &root,
        &ens_deployment.manifest_targets(),
        &basenames_deployment.manifest_targets(),
    )?;
    profile.retarget_chain("ethereum-mainnet", ETH_REORG_CHAIN)?;
    profile.retarget_chain("base-mainnet", BASE_REORG_CHAIN)?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [
        (ETH_REORG_CHAIN, eth.url.as_str()),
        (BASE_REORG_CHAIN, base.url.as_str()),
    ];
    pipeline::run_rpc_ingest_redo(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        ETH_REORG_CHAIN,
        &eth.url,
        0,
        eth_head,
    )
    .await?;
    pipeline::run_existing_raw_spine(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        ETH_REORG_CHAIN,
        &eth.url,
        eth_head,
    )
    .await?;
    pipeline::run_rpc_ingest_redo(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        BASE_REORG_CHAIN,
        &base.url,
        0,
        base_head,
    )
    .await?;
    pipeline::run_existing_raw_spine(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        BASE_REORG_CHAIN,
        &base.url,
        base_head,
    )
    .await?;
    let eth_head_before: i64 =
        sqlx::query_scalar("SELECT max(latest_block_number) FROM chain_heads WHERE chain_id = $1")
            .bind(ETH_REORG_CHAIN)
            .fetch_one(&db.pool)
            .await?;

    // Losing branch on Base only.
    basenames::set_base_text_record(
        &base_rpc,
        basenames_deployment.l2_resolver.address,
        alice,
        "churner.base.eth",
        "branch",
        "losing",
    )
    .await?;
    let losing_event_block = base_rpc.block_number().await?;
    let losing_hash = base_rpc.block_hash(losing_event_block).await?;
    base_rpc.mine(3).await?;
    let losing_head = base_rpc.block_number().await?;
    let losing_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events event \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE event.chain_id = '{BASE_REORG_CHAIN}' \
           AND event.logical_name_id = 'basenames:0x4d5ef02a96a4ee46c5ebdf480853dd812e13ca50d2dd5807c77d5db1a6e2f940' \
           AND event.event_kind = 'RecordChanged' \
           AND event.after_state->>'record_key' = 'text:branch' \
           AND event.after_state->>'value' = 'losing' \
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))"
    );
    pipeline::run_rpc_ingest_redo(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        BASE_REORG_CHAIN,
        &base.url,
        base_head + 1,
        losing_head,
    )
    .await?;
    pipeline::run_existing_raw_spine(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        BASE_REORG_CHAIN,
        &base.url,
        losing_head,
    )
    .await?;
    let losing_ready: bool = sqlx::query_scalar(&losing_ready_sql)
        .fetch_one(&db.pool)
        .await?;
    assert!(losing_ready, "losing Base branch was not interpreted");

    base_rpc.evm_revert(&base_snapshot).await?;
    basenames::set_base_text_record(
        &base_rpc,
        basenames_deployment.l2_resolver.address,
        alice,
        "churner.base.eth",
        "branch",
        "winning",
    )
    .await?;
    let winning_event_block = base_rpc.block_number().await?;
    let winning_hash = base_rpc.block_hash(winning_event_block).await?;
    base_rpc.mine(3).await?;
    let winning_head = base_rpc.block_number().await?;
    assert_eq!(
        winning_head, losing_head,
        "both Base forks stay at one height so the rewind stamp covers the winner"
    );
    let winning_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events event \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE event.chain_id = '{BASE_REORG_CHAIN}' \
           AND event.logical_name_id = 'basenames:0x4d5ef02a96a4ee46c5ebdf480853dd812e13ca50d2dd5807c77d5db1a6e2f940' \
           AND event.event_kind = 'RecordChanged' \
           AND event.after_state->>'record_key' = 'text:branch' \
           AND event.after_state->>'value' = 'winning' \
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))"
    );
    pipeline::rewind_to_ancestor(
        &root,
        &db.url,
        BASE_REORG_CHAIN,
        base_head,
        &base_ancestor_hash,
    )
    .await?;
    let losing_rows_before_redo: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(BASE_REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        losing_rows_before_redo > 0,
        "head publication must retain losing Base normalized rows until stamped redo starts"
    );
    let readable_losing_rows_before_redo: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events event
         JOIN chain_lineage lineage USING (chain_id, block_hash)
         WHERE event.chain_id = $1 AND event.block_hash = $2
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(BASE_REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        readable_losing_rows_before_redo, 0,
        "the lineage join must exclude losing Base normalized rows before stamped redo"
    );
    pipeline::run_rpc_ingest_redo(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        BASE_REORG_CHAIN,
        &base.url,
        base_head + 1,
        winning_head,
    )
    .await?;
    pipeline::run_required_reorg_spine(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        BASE_REORG_CHAIN,
        &base.url,
    )
    .await?;
    let winning_ready: bool = sqlx::query_scalar(&winning_ready_sql)
        .fetch_one(&db.pool)
        .await?;
    assert!(winning_ready, "winning Base branch was not projected");

    let retained_losing_raw_logs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE chain_id = $1 AND block_hash = $2")
            .bind(BASE_REORG_CHAIN)
            .bind(&losing_hash)
            .fetch_one(&db.pool)
            .await?;
    assert!(
        retained_losing_raw_logs > 0,
        "the losing Base branch must retain its immutable raw logs"
    );
    let losing_lineage_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM chain_lineage
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(BASE_REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        losing_lineage_state, "orphaned",
        "the losing Base branch must remain in permanent orphaned lineage"
    );
    let losing_normalized_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(BASE_REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        losing_normalized_events, 0,
        "completed interpret redo must remove the losing Base normalized derivation"
    );
    let winning_record: Option<String> = sqlx::query_scalar(
        "SELECT event.after_state->>'value' FROM normalized_events event \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE event.chain_id = $1 AND event.block_hash = $2 \
           AND event.logical_name_id = 'basenames:0x4d5ef02a96a4ee46c5ebdf480853dd812e13ca50d2dd5807c77d5db1a6e2f940' \
           AND event.event_kind = 'RecordChanged' \
           AND event.after_state->>'record_key' = 'text:branch' \
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY event.block_number DESC LIMIT 1",
    )
    .bind(BASE_REORG_CHAIN)
    .bind(&winning_hash)
    .fetch_optional(&db.pool)
    .await?;
    assert_eq!(
        winning_record.as_deref(),
        Some("winning"),
        "the winning Base branch must be readable through canonical lineage"
    );

    // The ethereum chain never reorged: zero orphaned rows, its stored intake
    // head stays fixed, and the name still serves.
    let orphaned_eth_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE raw.chain_id = $1 \
           AND lineage.canonicality_state = 'orphaned'",
    )
    .bind(ETH_REORG_CHAIN)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        orphaned_eth_rows, 0,
        "a Base reorg must not orphan ethereum rows"
    );
    let eth_head_after: i64 =
        sqlx::query_scalar("SELECT max(latest_block_number) FROM chain_heads WHERE chain_id = $1")
            .bind(ETH_REORG_CHAIN)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        eth_head_after, eth_head_before,
        "the ethereum stored intake head must not move during a Base-only reorg"
    );
    let api = pipeline::ProjectionReader::start(&root, &db.url, &chain_rpc_urls).await?;
    let (status, steady) = api.get_json("/v1/names/ens/steady.eth").await?;
    assert_eq!(status, 200, "the ethereum name must still serve: {steady}");
    assert_eq!(
        pointer(&steady, "/declared_state/registration/status"),
        "active",
        "the ethereum name must stay served: {steady}"
    );
    assert_eq!(
        pointer(&steady, "/data/normalized_name"),
        "steady.eth",
        "the still-canonical ethereum surface must remain the public result: {steady}"
    );
    drop(api);

    db.cleanup().await?;
    Ok(())
}
