use std::collections::BTreeMap;

use alloy_primitives::{Address, keccak256};
use anyhow::{Result, anyhow, ensure};
use serde_json::{Value, json};

use super::support;
use crate::harness::{
    anvil::Anvil, db::HarnessDb, ens_v1, manifests, perturb, pipeline, repo_root, rpc::RpcClient,
};

const CHAIN: &str = "ethereum-mainnet";
const DEPLOYMENT_PROFILE: &str = "e2e";
const NAME: &str = "catchupeq.eth";
const LABEL: &str = "catchupeq";
const SUB_LABEL: &str = "sub";
const WRAPPED_NAME: &str = "catchupeqwrapped.eth";
const WRAPPED_LABEL: &str = "catchupeqwrapped";
const RESTORED_NAME: &str = "catchupeqrestored.eth";
const RESTORED_LABEL: &str = "catchupeqrestored";
const TEXT_KEY: &str = "com.twitter";
const YEAR: u64 = 365 * 24 * 60 * 60;
const FIXTURE_FUSE: u16 = 1 | 4;
// Anvil's finalized anchor trails the head by 64 blocks, so 66 is load-bearing:
// fixture events must finalize for `live_ready_sql`, and the post-handoff cold-start
// safe/finalized anchor must land above the last fixture event so live adapter sync
// never touches fixture blocks. The roughly two-block headroom prevents post-handoff
// live sync from masking a replay omission in the catch-up corpus.
const FINALITY_MARGIN_BLOCKS: u64 = 66;
const REGISTRAR_MANIFEST_VERSION: i64 = 1;
const REGISTRAR_SOURCE_MANIFEST_ID: i64 = 1;
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116 @ ens_v1@91c966f)
const NAME_REGISTERED_EVENT_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)";

const CATCHUP_EQUIVALENCE_CONTRACT: perturb::CatchupEquivalenceContract =
    perturb::CatchupEquivalenceContract::Full;

struct CatchupChain {
    deployment: ens_v1::EnsV1Deployment,
    owner: Address,
    record_target: Address,
    child_owner: Address,
    resolver: Address,
}

struct PreparedCorpus {
    db: HarnessDb,
    scratch: support::TempDir,
    profile: manifests::LocalProfile,
}

struct CatchupFixture {
    last_event_block: u64,
    expected_preimages: Vec<perturb::StatelessLabelPreimage>,
}

impl CatchupChain {
    fn subjects(&self) -> perturb::RouteSnapshotSubjects {
        perturb::RouteSnapshotSubjects::new(
            [NAME],
            [
                format!("{:#x}", self.owner),
                format!("{:#x}", self.record_target),
                format!("{:#x}", self.child_owner),
            ],
        )
    }

    fn wrapper_reverse_subjects(&self) -> perturb::RouteSnapshotSubjects {
        perturb::RouteSnapshotSubjects::new(
            [WRAPPED_NAME, RESTORED_NAME],
            [
                format!("{:#x}", self.owner),
                format!("{:#x}", self.record_target),
                format!("{:#x}", self.child_owner),
            ],
        )
    }
}

fn rpc_string<'a>(value: &'a Value, path: &str) -> Result<&'a str> {
    value
        .pointer(path)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("fixture receipt lacks string field {path}"))
}

fn rpc_quantity(value: &Value, path: &str) -> Result<u64> {
    let encoded = rpc_string(value, path)?;
    Ok(u64::from_str_radix(
        encoded.strip_prefix("0x").unwrap_or(encoded),
        16,
    )?)
}

async fn expected_preimage_from_registration(
    rpc: &RpcClient,
    chain: &CatchupChain,
    registration: &ens_v1::RegisteredName,
    name: &str,
    label: &str,
) -> Result<perturb::StatelessLabelPreimage> {
    let receipt = rpc
        .call(
            "eth_getTransactionReceipt",
            json!([registration.register_tx_hash]),
        )
        .await?;
    let emitter = format!("{:#x}", chain.deployment.controller.address);
    let event_topic = format!(
        "{:#x}",
        keccak256(NAME_REGISTERED_EVENT_SIGNATURE.as_bytes())
    );
    let event_log = receipt
        .pointer("/logs")
        .and_then(Value::as_array)
        .and_then(|logs| {
            logs.iter().find(|log| {
                log.get("address")
                    .and_then(Value::as_str)
                    .is_some_and(|address| address.eq_ignore_ascii_case(&emitter))
                    && log
                        .pointer("/topics/0")
                        .and_then(Value::as_str)
                        .is_some_and(|topic| topic.eq_ignore_ascii_case(&event_topic))
            })
        })
        .ok_or_else(|| anyhow!("registration receipt lacks the controller NameRegistered log"))?;
    let block_number = rpc_quantity(event_log, "/blockNumber")?;
    let transaction_index = rpc_quantity(event_log, "/transactionIndex")?;
    let log_index = rpc_quantity(event_log, "/logIndex")?;
    let block_hash = rpc_string(event_log, "/blockHash")?.to_ascii_lowercase();
    let transaction_hash = rpc_string(event_log, "/transactionHash")?.to_ascii_lowercase();
    ensure!(
        block_number == registration.register_block
            && transaction_hash.eq_ignore_ascii_case(&registration.register_tx_hash),
        "NameRegistered log position does not match the registration receipt"
    );
    let topics = event_log
        .get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("NameRegistered log lacks topics"))?;
    ensure!(
        topics.len() == 3,
        "NameRegistered log must carry its signature and two indexed values"
    );
    let topic = |index: usize| -> Result<String> {
        topics[index]
            .as_str()
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| anyhow!("NameRegistered topic {index} is not a string"))
    };
    let data_hex = rpc_string(event_log, "/data")?
        .strip_prefix("0x")
        .unwrap_or(rpc_string(event_log, "/data")?)
        .to_ascii_lowercase();
    let dns_encoded_name = format!("{:#x}", ens_v1::dns_encode_name(name)?);
    let event_identity = format!(
        "raw_log_preimage_observed:{REGISTRAR_SOURCE_MANIFEST_ID}:{block_hash}:{transaction_hash}:{log_index}:{emitter}"
    );
    perturb::StatelessLabelPreimage::from_expected_row(json!({
        "event_identity": event_identity,
        "namespace": "ens",
        "logical_name_id": null,
        "resource_id": null,
        "event_kind": "PreimageObserved",
        "source_family": "ens_v1_registrar_l1",
        "manifest_version": REGISTRAR_MANIFEST_VERSION,
        "source_manifest_id": REGISTRAR_SOURCE_MANIFEST_ID,
        "chain_id": CHAIN,
        "block_number": block_number,
        "block_hash": block_hash,
        "transaction_hash": transaction_hash,
        "log_index": log_index,
        "raw_fact_ref": {
            "kind": "raw_log",
            "chain_id": CHAIN,
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": transaction_hash,
            "transaction_index": transaction_index,
            "log_index": log_index,
            "emitting_address": emitter,
            "topic0": topic(0)?,
            "topic1": topic(1)?,
            "topic2": topic(2)?,
            "data_hex": data_hex,
        },
        "derivation_kind": "raw_log_preimage_observation",
        "canonicality_state": "finalized",
        "before_state": {},
        "after_state": {
            "source_event": "NameRegistered",
            "dns_encoded_name": dns_encoded_name,
            "decoded_name": name,
            "labelhashes": [
                format!("{:#x}", ens_v1::labelhash(label)),
                format!("{:#x}", ens_v1::labelhash("eth")),
            ],
            "namehash": format!("{:#x}", ens_v1::namehash(name)),
        },
    }))
}

async fn add_rich_name_fixture(anvil: &Anvil, chain: &CatchupChain) -> Result<CatchupFixture> {
    let rpc = anvil.client();
    let registration = ens_v1::register_eth_name(
        &rpc,
        &chain.deployment,
        LABEL,
        chain.owner,
        YEAR,
        chain.resolver,
    )
    .await?;
    let expected_preimage =
        expected_preimage_from_registration(&rpc, chain, &registration, NAME, LABEL).await?;
    ens_v1::set_addr_record(&rpc, chain.resolver, chain.owner, NAME, chain.record_target).await?;
    ens_v1::set_text_record(&rpc, chain.resolver, chain.owner, NAME, TEXT_KEY, "catchup").await?;
    ens_v1::create_subname(
        &rpc,
        &chain.deployment,
        chain.owner,
        NAME,
        SUB_LABEL,
        chain.child_owner,
    )
    .await?;
    let last_event_block = rpc.block_number().await?;
    rpc.mine(FINALITY_MARGIN_BLOCKS).await?;
    Ok(CatchupFixture {
        last_event_block,
        expected_preimages: vec![expected_preimage],
    })
}

async fn add_wrapper_reverse_fixture(
    anvil: &Anvil,
    chain: &CatchupChain,
) -> Result<CatchupFixture> {
    let rpc = anvil.client();
    let wrapped_registration = ens_v1::register_eth_name(
        &rpc,
        &chain.deployment,
        WRAPPED_LABEL,
        chain.owner,
        YEAR,
        chain.resolver,
    )
    .await?;
    let wrapped_preimage = expected_preimage_from_registration(
        &rpc,
        chain,
        &wrapped_registration,
        WRAPPED_NAME,
        WRAPPED_LABEL,
    )
    .await?;
    ens_v1::wrap_eth_2ld(
        &rpc,
        &chain.deployment,
        chain.owner,
        WRAPPED_LABEL,
        chain.record_target,
        0,
        chain.resolver,
    )
    .await?;
    ens_v1::set_wrapper_fuses(
        &rpc,
        &chain.deployment,
        chain.record_target,
        WRAPPED_NAME,
        FIXTURE_FUSE,
    )
    .await?;

    let restored_registration = ens_v1::register_eth_name(
        &rpc,
        &chain.deployment,
        RESTORED_LABEL,
        chain.owner,
        YEAR,
        chain.resolver,
    )
    .await?;
    let restored_preimage = expected_preimage_from_registration(
        &rpc,
        chain,
        &restored_registration,
        RESTORED_NAME,
        RESTORED_LABEL,
    )
    .await?;
    ens_v1::wrap_eth_2ld(
        &rpc,
        &chain.deployment,
        chain.owner,
        RESTORED_LABEL,
        chain.record_target,
        0,
        chain.resolver,
    )
    .await?;
    ens_v1::unwrap_eth_2ld(
        &rpc,
        &chain.deployment,
        chain.record_target,
        RESTORED_LABEL,
        chain.child_owner,
        chain.child_owner,
    )
    .await?;
    ens_v1::set_reverse_name(&rpc, &chain.deployment, chain.child_owner, RESTORED_NAME).await?;

    let last_event_block = rpc.block_number().await?;
    rpc.mine(FINALITY_MARGIN_BLOCKS).await?;
    Ok(CatchupFixture {
        last_event_block,
        expected_preimages: vec![wrapped_preimage, restored_preimage],
    })
}

fn normalize_primary_route_contract_instance_ids(
    value: &mut Value,
    contract_instances: &BTreeMap<String, String>,
) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_primary_route_contract_instance_ids(value, contract_instances)?;
            }
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                if key == "contract_instance_id" && !value.is_null() {
                    let id = value.as_str().ok_or_else(|| {
                        anyhow!("primary-name contract_instance_id is not a string: {value}")
                    })?;
                    let stable_key = contract_instances.get(id).ok_or_else(|| {
                        anyhow!("primary-name route references unknown contract instance {id}")
                    })?;
                    *value = Value::String(format!("<contract:{stable_key}>"));
                } else {
                    normalize_primary_route_contract_instance_ids(value, contract_instances)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[test]
fn primary_route_normalization_preserves_contract_instance_identity() {
    let live_id = "00000000-0000-0000-0000-000000000001";
    let catchup_id = "00000000-0000-0000-0000-000000000002";
    let mut live = json!({
        "claimed_primary_name": {
            "source": {"contract_instance_id": live_id}
        }
    });
    let mut catchup = json!({
        "claimed_primary_name": {
            "source": {"contract_instance_id": catchup_id}
        }
    });
    let live_instances = BTreeMap::from([(
        live_id.to_owned(),
        "ethereum-mainnet:0x0000000000000000000000000000000000000001".to_owned(),
    )]);
    let catchup_instances = BTreeMap::from([(
        catchup_id.to_owned(),
        "ethereum-mainnet:0x0000000000000000000000000000000000000002".to_owned(),
    )]);

    normalize_primary_route_contract_instance_ids(&mut live, &live_instances).unwrap();
    normalize_primary_route_contract_instance_ids(&mut catchup, &catchup_instances).unwrap();

    assert_ne!(
        live, catchup,
        "normalization must not hide a contract-instance provenance mismatch"
    );
}

fn normalize_primary_route_snapshot(
    value: &mut Value,
    contract_instances: &BTreeMap<String, String>,
) -> Result<()> {
    normalize_primary_route_contract_instance_ids(value, contract_instances)?;
    let last_updated = value
        .get_mut("last_updated")
        .ok_or_else(|| anyhow!("primary-name route snapshot lacks last_updated"))?;
    ensure!(
        last_updated.is_string(),
        "primary-name route snapshot last_updated is not a string: {last_updated}"
    );
    *last_updated = Value::String("<last_updated>".to_owned());
    Ok(())
}

async fn wrapper_reverse_route_snapshots(
    run: &support::PipelineRun,
    chain: &CatchupChain,
) -> Result<perturb::RouteSnapshots> {
    let mut snapshots = support::route_snapshots(run, &chain.wrapper_reverse_subjects()).await?;
    let wrapped_key = format!("GET /v1/names/ens/{WRAPPED_NAME}");
    let wrapped = snapshots
        .get(&wrapped_key)
        .ok_or_else(|| anyhow!("missing {wrapped_key} route snapshot"))?;
    ensure!(
        wrapped
            .pointer("/declared_state/control/status")
            .and_then(Value::as_str)
            == Some("unsupported")
            && wrapped
                .pointer("/declared_state/control/unsupported_reason")
                .and_then(Value::as_str)
                == Some("ENSv1 wrapper effective control is not yet projected"),
        "{WRAPPED_NAME} route snapshot does not expose the wrapper control boundary: {wrapped}"
    );
    let restored_key = format!("GET /v1/names/ens/{RESTORED_NAME}");
    let restored = snapshots
        .get(&restored_key)
        .ok_or_else(|| anyhow!("missing {restored_key} route snapshot"))?;
    ensure!(
        restored
            .pointer("/declared_state/registration/authority_kind")
            .and_then(Value::as_str)
            == Some("registrar"),
        "{RESTORED_NAME} route snapshot is not registrar-authoritative after unwrap: {restored}"
    );

    let claimant = format!("{:#x}", chain.child_owner);
    let primary_path =
        format!("/v1/primary-names/{claimant}?namespace=ens&coin_type=60&mode=declared");
    let (status, mut primary) = run.api.get_json(&primary_path).await?;
    ensure!(
        status.is_success(),
        "GET {primary_path} returned {status}: {primary}"
    );
    ensure!(
        primary
            .pointer("/declared_state/claimed_primary_name/status")
            .and_then(Value::as_str)
            == Some("success")
            && primary
                .pointer("/declared_state/claimed_primary_name/name")
                .and_then(Value::as_str)
                == Some(RESTORED_NAME),
        "{claimant} route snapshot does not carry the {RESTORED_NAME} primary-name claim: {primary}"
    );
    let contract_instances = perturb::contract_instance_stable_keys(&run.db.pool).await?;
    normalize_primary_route_snapshot(&mut primary, &contract_instances)?;
    snapshots.insert(format!("GET {primary_path}"), primary);
    Ok(snapshots)
}

fn derived_output_ready_expression(chain: &CatchupChain) -> String {
    let parent_node = format!("{:#x}", ens_v1::namehash(NAME));
    let sub_labelhash = format!("{:#x}", ens_v1::labelhash(SUB_LABEL));
    let resolver_profile_ready = support::resolver_code_hash_comparison_sql(
        chain.resolver,
        chain.deployment.public_resolver.address,
        true,
    );
    format!(
        "EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:{NAME}' AND event_kind = 'ResolverChanged' \
         AND canonicality_state = 'finalized' \
         AND lower(after_state->>'resolver') = '{resolver:#x}') \
         AND (SELECT count(DISTINCT after_state->>'record_key') >= 2 FROM normalized_events \
         WHERE logical_name_id = 'ens:{NAME}' AND event_kind = 'RecordChanged' \
         AND canonicality_state = 'finalized' \
         AND after_state->>'record_key' IN ('addr:60', 'text:{TEXT_KEY}')) \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
         AND canonicality_state = 'finalized' \
         AND lower(after_state->>'parent_node') = '{parent_node}' \
         AND lower(after_state->>'labelhash') = '{sub_labelhash}' \
         AND lower(after_state->>'owner') = '{child_owner:#x}') \
         AND {resolver_profile_ready}",
        resolver = chain.resolver,
        child_owner = chain.child_owner,
    )
}

fn live_ready_sql(chain: &CatchupChain) -> String {
    let labelhash = format!("{:#x}", ens_v1::labelhash(LABEL));
    format!(
        "SELECT {} \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'PreimageObserved' \
         AND source_family = 'ens_v1_registrar_l1' \
         AND derivation_kind = 'raw_log_preimage_observation' \
         AND after_state->>'decoded_name' = '{NAME}' \
         AND after_state->'labelhashes'->>0 = '{labelhash}' \
         AND canonicality_state = 'finalized')",
        derived_output_ready_expression(chain),
    )
}

fn wrapper_reverse_derived_output_ready_expression(chain: &CatchupChain) -> String {
    let resolver_profile_ready = support::resolver_code_hash_comparison_sql(
        chain.resolver,
        chain.deployment.public_resolver.address,
        true,
    );
    format!(
        "EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:{WRAPPED_NAME}' \
         AND event_kind = 'AuthorityEpochChanged' \
         AND source_family = 'ens_v1_wrapper_l1' \
         AND canonicality_state = 'finalized' \
         AND before_state->>'authority_kind' = 'registrar' \
         AND after_state->>'authority_kind' = 'wrapper') \
         AND (SELECT count(*) >= 2 FROM normalized_events \
         WHERE logical_name_id = 'ens:{WRAPPED_NAME}' \
         AND event_kind = 'PermissionScopeChanged' \
         AND source_family = 'ens_v1_wrapper_l1' \
         AND canonicality_state = 'finalized') \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:{WRAPPED_NAME}' \
         AND event_kind = 'PermissionScopeChanged' \
         AND source_family = 'ens_v1_wrapper_l1' \
         AND canonicality_state = 'finalized' \
         AND ((after_state->>'fuses')::BIGINT & {FIXTURE_FUSE}) = {FIXTURE_FUSE}) \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:{RESTORED_NAME}' \
         AND event_kind = 'AuthorityEpochChanged' \
         AND canonicality_state = 'finalized' \
         AND before_state->>'authority_kind' = 'wrapper' \
         AND after_state->>'authority_kind' = 'registrar') \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'ReverseChanged' \
         AND source_family = 'ens_v1_reverse_l1' \
         AND canonicality_state = 'finalized' \
         AND lower(after_state->>'address') = '{claimant:#x}') \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
         AND canonicality_state = 'finalized' \
         AND after_state->>'raw_name' = '{RESTORED_NAME}' \
         AND lower(after_state->'primary_claim_source'->>'address') = '{claimant:#x}') \
         AND {resolver_profile_ready}",
        claimant = chain.child_owner,
    )
}

fn wrapper_reverse_live_ready_sql(chain: &CatchupChain) -> String {
    format!(
        "SELECT {} \
         AND (SELECT count(DISTINCT after_state->>'decoded_name') = 2 \
         FROM normalized_events \
         WHERE event_kind = 'PreimageObserved' \
         AND source_family = 'ens_v1_registrar_l1' \
         AND derivation_kind = 'raw_log_preimage_observation' \
         AND after_state->>'decoded_name' IN ('{WRAPPED_NAME}', '{RESTORED_NAME}') \
         AND canonicality_state = 'finalized')",
        wrapper_reverse_derived_output_ready_expression(chain),
    )
}

fn catchup_cursor_ready_expression(last_fixture_event_block: u64) -> String {
    format!(
        "EXISTS (SELECT 1 FROM normalized_replay_cursors \
         WHERE deployment_profile = '{DEPLOYMENT_PROFILE}' \
         AND chain_id = '{CHAIN}' \
         AND cursor_kind = 'raw_fact_normalized_events' \
         AND range_start_block_number <= {last_fixture_event_block} \
         AND target_block_number >= {last_fixture_event_block} \
         AND next_block_number > target_block_number \
         AND last_completed_block_number = target_block_number \
         AND last_replayed_at IS NOT NULL \
         AND last_failure_reason IS NULL)"
    )
}

fn catchup_ready_sql(chain: &CatchupChain, last_fixture_event_block: u64) -> String {
    format!(
        "SELECT {} AND {}",
        derived_output_ready_expression(chain),
        catchup_cursor_ready_expression(last_fixture_event_block),
    )
}

fn wrapper_reverse_catchup_ready_sql(
    chain: &CatchupChain,
    last_fixture_event_block: u64,
) -> String {
    format!(
        "SELECT {} AND {}",
        wrapper_reverse_derived_output_ready_expression(chain),
        catchup_cursor_ready_expression(last_fixture_event_block),
    )
}

async fn prepare_baseline(
    anvil: &Anvil,
    chain: &CatchupChain,
    log_suffix: &str,
) -> Result<PreparedCorpus> {
    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &root,
        &chain.deployment.manifest_targets(),
    )?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [(CHAIN, anvil.url.as_str())];
    let mut baseline_session =
        pipeline::IndexerRunSession::start_with_inline_bootstrap_and_live_poll_adapter_sync(
            &root,
            &db.url,
            &profile.root,
            &chain_rpc_urls,
            log_suffix,
        )
        .await?;
    let deployment_head = anvil.client().block_number().await?;
    baseline_session
        .wait_for_checkpoint(
            &db.pool,
            deployment_head,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE derivation_kind = 'manifest_sync' \
                 AND event_kind = 'CapabilityChanged')",
            ),
        )
        .await?;
    baseline_session.stop().await?;
    Ok(PreparedCorpus {
        db,
        scratch,
        profile,
    })
}

async fn live_ingest(
    anvil: &Anvil,
    prepared: PreparedCorpus,
    ready_sql: &str,
    log_suffix: &str,
) -> Result<support::PipelineRun> {
    let root = repo_root();
    let PreparedCorpus {
        db,
        scratch,
        profile,
    } = prepared;
    let chain_rpc_urls = [(CHAIN, anvil.url.as_str())];
    let fixture_head = anvil.client().block_number().await?;
    let mut live_session = pipeline::IndexerRunSession::start_with_live_poll_adapter_sync(
        &root,
        &db.url,
        &profile.root,
        &chain_rpc_urls,
        log_suffix,
    )
    .await?;
    live_session
        .wait_for_checkpoint(&db.pool, fixture_head, Some(ready_sql))
        .await?;
    live_session.stop().await?;
    pipeline::worker_replay_all_current_projections(&root, &db.url).await?;
    support::serve_existing_db(db, scratch, anvil).await
}

async fn automatic_catchup(
    anvil: &Anvil,
    prepared: PreparedCorpus,
    ready_sql: &str,
    log_suffix: &str,
) -> Result<support::PipelineRun> {
    let root = repo_root();
    let PreparedCorpus {
        db,
        scratch,
        profile,
    } = prepared;
    // Keep the common derived baseline, but remove its live resume point so
    // automatic replay—not the post-handoff backlog—owns the fixture span.
    let deleted = sqlx::query("DELETE FROM chain_checkpoints WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(&db.pool)
        .await?;
    ensure!(
        deleted.rows_affected() == 1,
        "expected one {CHAIN} intake checkpoint before forcing automatic catch-up"
    );
    let mut session =
        pipeline::IndexerRunSession::start(&root, &db.url, &profile.root, &anvil.url, log_suffix)
            .await?;
    let fixture_head = anvil.client().block_number().await?;
    session
        .wait_for_checkpoint(&db.pool, fixture_head, Some(ready_sql))
        .await?;
    session.stop().await?;
    pipeline::worker_replay_all_current_projections(&root, &db.url).await?;
    support::serve_existing_db(db, scratch, anvil).await
}

#[tokio::test]
async fn automatic_catchup_matches_live_ingestion_outputs() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let chain = CatchupChain {
        resolver: deployment.public_resolver.address,
        deployment,
        owner: accounts[1],
        record_target: accounts[2],
        child_owner: accounts[3],
    };

    rpc.mine(FINALITY_MARGIN_BLOCKS).await?;
    let live_baseline = prepare_baseline(&anvil, &chain, "catchup-equivalence-live-base").await?;
    let catchup_baseline =
        prepare_baseline(&anvil, &chain, "catchup-equivalence-auto-base").await?;
    let fixture = add_rich_name_fixture(&anvil, &chain).await?;
    let live_ready = live_ready_sql(&chain);
    let catchup_ready = catchup_ready_sql(&chain, fixture.last_event_block);

    let live = live_ingest(
        &anvil,
        live_baseline,
        &live_ready,
        "catchup-equivalence-live-finalized",
    )
    .await?;
    let catchup = automatic_catchup(
        &anvil,
        catchup_baseline,
        &catchup_ready,
        "catchup-equivalence-auto",
    )
    .await?;
    let live_snapshots = support::route_snapshots(&live, &chain.subjects()).await?;
    let catchup_snapshots = support::route_snapshots(&catchup, &chain.subjects()).await?;
    perturb::assert_snapshots_equal(&live_snapshots, &catchup_snapshots)?;
    perturb::assert_catchup_normalized_event_parity(
        &live.db.pool,
        &catchup.db.pool,
        CATCHUP_EQUIVALENCE_CONTRACT,
        &fixture.expected_preimages,
    )
    .await?;

    live.db.cleanup().await?;
    catchup.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn automatic_catchup_matches_live_wrapper_reverse_outputs() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let chain = CatchupChain {
        resolver: deployment.public_resolver.address,
        deployment,
        owner: accounts[1],
        record_target: accounts[2],
        child_owner: accounts[3],
    };

    rpc.mine(FINALITY_MARGIN_BLOCKS).await?;
    let live_baseline =
        prepare_baseline(&anvil, &chain, "catchup-wrapper-reverse-live-base").await?;
    let catchup_baseline =
        prepare_baseline(&anvil, &chain, "catchup-wrapper-reverse-auto-base").await?;
    let fixture = add_wrapper_reverse_fixture(&anvil, &chain).await?;
    let live_ready = wrapper_reverse_live_ready_sql(&chain);
    let catchup_ready = wrapper_reverse_catchup_ready_sql(&chain, fixture.last_event_block);

    let live = live_ingest(
        &anvil,
        live_baseline,
        &live_ready,
        "catchup-wrapper-reverse-live-finalized",
    )
    .await?;
    let catchup = automatic_catchup(
        &anvil,
        catchup_baseline,
        &catchup_ready,
        "catchup-wrapper-reverse-auto",
    )
    .await?;
    let live_snapshots = wrapper_reverse_route_snapshots(&live, &chain).await?;
    let catchup_snapshots = wrapper_reverse_route_snapshots(&catchup, &chain).await?;
    perturb::assert_snapshots_equal(&live_snapshots, &catchup_snapshots)?;
    perturb::assert_catchup_normalized_event_parity(
        &live.db.pool,
        &catchup.db.pool,
        CATCHUP_EQUIVALENCE_CONTRACT,
        &fixture.expected_preimages,
    )
    .await?;

    live.db.cleanup().await?;
    catchup.db.cleanup().await?;
    Ok(())
}
