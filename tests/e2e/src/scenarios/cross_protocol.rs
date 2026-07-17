use alloy_primitives::{Address, keccak256};
use anyhow::Result;
use serde_json::Value;

use super::support::{self, TempDir};
use crate::harness::{
    anvil::Anvil, basenames, db::HarnessDb, ens_v1, manifests, pipeline, repo_root,
    responses::pointer,
};

const YEAR: u64 = 365 * 24 * 60 * 60;

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
          WHERE logical_name_id = 'ens:alice.eth' \
            AND event_kind = 'RegistrationGranted' \
            AND canonicality_state = 'canonical') \
       AND EXISTS (SELECT 1 FROM normalized_events \
          WHERE logical_name_id = 'basenames:alicebase.base.eth' \
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

    // Row 1: both chains hold their own canonical checkpoints in one corpus.
    let checkpoint_chains: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT chain_id FROM chain_checkpoints ORDER BY chain_id")
            .fetch_all(&composed.db.pool)
            .await?;
    assert_eq!(
        checkpoint_chains,
        vec!["base-mainnet".to_owned(), "ethereum-mainnet".to_owned()],
        "both chains must checkpoint independently"
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

    // Row 4: primary candidates coexist per coin type without leaking.
    let (status, ens_primary) = body(
        &composed,
        &format!("/v1/primary-names/{alice:#x}?namespace=ens&coin_type=60&mode=declared"),
    )
    .await?;
    assert_eq!(status, 200, "ens primary failed: {ens_primary}");
    assert_eq!(
        pointer(&ens_primary, "/declared_state/claimed_primary_name/name"),
        "alice.eth"
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
    // manifest state on the ethereum chain (live runs derive no manifest
    // bookkeeping events — those are backfill-only extras per the phase-3
    // parity pin), and their placeholder role stays silent.
    let glue_manifests: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT source_family, chain FROM manifest_versions \
         WHERE source_family IN ('basenames_l1_compat', 'basenames_execution') \
         ORDER BY source_family",
    )
    .fetch_all(&composed.db.pool)
    .await?;
    assert_eq!(
        glue_manifests,
        vec![
            (
                "basenames_execution".to_owned(),
                "ethereum-mainnet".to_owned()
            ),
            (
                "basenames_l1_compat".to_owned(),
                "ethereum-mainnet".to_owned()
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
    let base_snapshot = base_rpc.evm_snapshot().await?;

    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_mainnet_composed_profile(
        scratch.path(),
        &root,
        &ens_deployment.manifest_targets(),
        &basenames_deployment.manifest_targets(),
    )?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [
        ("ethereum-mainnet", eth.url.as_str()),
        ("base-mainnet", base.url.as_str()),
    ];
    // This scenario asserts same-session live reorg convergence. Keep the
    // automatic historical replay handoff out of the readiness path so the
    // losing and winning record facts must each be normalized by the live
    // poll that observed them.
    let mut session = pipeline::IndexerRunSession::start_with_live_poll_adapter_sync(
        &root,
        &db.url,
        &profile.root,
        &chain_rpc_urls,
        "composed-reorg",
    )
    .await?;
    session
        .wait_for_chain_checkpoint(&db.pool, "ethereum-mainnet", eth_head, None)
        .await?;
    session
        .wait_for_chain_checkpoint(&db.pool, "base-mainnet", base_head, None)
        .await?;
    let eth_checkpoint_before: i64 = sqlx::query_scalar(
        "SELECT max(canonical_block_number) FROM chain_checkpoints WHERE chain_id = 'ethereum-mainnet'",
    )
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
    let losing_head = base_rpc.block_number().await?;
    let losing_hash = base_rpc.block_hash(losing_head).await?;
    let losing_ready_sql = "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'basenames:churner.base.eth' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:branch' \
           AND after_state->>'value' = 'losing' \
           AND canonicality_state = 'canonical')";
    session
        .wait_for_chain_checkpoint(
            &db.pool,
            "base-mainnet",
            losing_head,
            Some(losing_ready_sql),
        )
        .await?;

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
    base_rpc.mine(3).await?;
    let winning_head = base_rpc.block_number().await?;
    let winning_ready_sql = "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'basenames:churner.base.eth' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:branch' \
           AND after_state->>'value' = 'winning' \
           AND canonicality_state = 'canonical')";
    session
        .wait_for_chain_checkpoint(
            &db.pool,
            "base-mainnet",
            winning_head,
            Some(winning_ready_sql),
        )
        .await?;
    session.stop().await?;
    pipeline::worker_replay_all_current_projections(&root, &db.url).await?;

    let orphaned_base_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE block_hash = $1 AND canonicality_state = 'orphaned'",
    )
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        orphaned_base_rows > 0,
        "the losing Base branch must retain orphaned rows"
    );
    let orphaned_losing_records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE block_hash = $1 \
           AND logical_name_id = 'basenames:churner.base.eth' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:branch' \
           AND after_state->>'value' = 'losing' \
           AND canonicality_state = 'orphaned'",
    )
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        orphaned_losing_records, 1,
        "the losing Base record must be retained exactly once as orphaned normalized history"
    );
    let canonical_losing_records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE block_hash = $1 \
           AND logical_name_id = 'basenames:churner.base.eth' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:branch' \
           AND after_state->>'value' = 'losing' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        canonical_losing_records, 0,
        "the losing Base record must not survive as canonical normalized history"
    );
    let winning_record: Option<String> = sqlx::query_scalar(
        "SELECT after_state->>'value' FROM normalized_events \
         WHERE logical_name_id = 'basenames:churner.base.eth' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:branch' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number DESC LIMIT 1",
    )
    .fetch_optional(&db.pool)
    .await?;
    assert_eq!(
        winning_record.as_deref(),
        Some("winning"),
        "the winning Base branch must be canonical"
    );

    // The ethereum chain never reorged: zero orphaned rows, checkpoint
    // unmoved, and the name still serves.
    let orphaned_eth_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE chain_id = 'ethereum-mainnet' AND canonicality_state = 'orphaned'",
    )
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        orphaned_eth_rows, 0,
        "a Base reorg must not orphan ethereum rows"
    );
    let eth_checkpoint_after: i64 = sqlx::query_scalar(
        "SELECT max(canonical_block_number) FROM chain_checkpoints WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        eth_checkpoint_after, eth_checkpoint_before,
        "the ethereum checkpoint must not move during a Base-only reorg"
    );
    let api = pipeline::ApiServer::start(&root, &db.url).await?;
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
