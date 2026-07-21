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
const TEXT_KEY: &str = "com.twitter";
const YEAR: u64 = 365 * 24 * 60 * 60;
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
    expected_preimage: perturb::StatelessLabelPreimage,
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
    let dns_encoded_name = format!("{:#x}", ens_v1::dns_encode_name(NAME)?);
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
            "decoded_name": NAME,
            "labelhashes": [
                format!("{:#x}", ens_v1::labelhash(LABEL)),
                format!("{:#x}", ens_v1::labelhash("eth")),
            ],
            "namehash": format!("{:#x}", ens_v1::namehash(NAME)),
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
    let expected_preimage = expected_preimage_from_registration(&rpc, chain, &registration).await?;
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
        expected_preimage,
    })
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

fn catchup_ready_sql(chain: &CatchupChain, last_fixture_event_block: u64) -> String {
    format!(
        "SELECT {} \
         AND EXISTS (SELECT 1 FROM normalized_replay_cursors \
         WHERE deployment_profile = '{DEPLOYMENT_PROFILE}' \
         AND chain_id = '{CHAIN}' \
         AND cursor_kind = 'raw_fact_normalized_events' \
         AND range_start_block_number <= {last_fixture_event_block} \
         AND target_block_number >= {last_fixture_event_block} \
         AND next_block_number > target_block_number \
         AND last_completed_block_number = target_block_number \
         AND last_replayed_at IS NOT NULL \
         AND last_failure_reason IS NULL)",
        derived_output_ready_expression(chain),
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
    chain: &CatchupChain,
    prepared: PreparedCorpus,
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
        "catchup-equivalence-live-finalized",
    )
    .await?;
    live_session
        .wait_for_checkpoint(&db.pool, fixture_head, Some(&live_ready_sql(chain)))
        .await?;
    live_session.stop().await?;
    pipeline::worker_replay_all_current_projections(&root, &db.url).await?;
    support::serve_existing_db(db, scratch, anvil).await
}

async fn automatic_catchup(
    anvil: &Anvil,
    chain: &CatchupChain,
    prepared: PreparedCorpus,
    last_fixture_event_block: u64,
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
    let mut session = pipeline::IndexerRunSession::start(
        &root,
        &db.url,
        &profile.root,
        &anvil.url,
        "catchup-equivalence-auto",
    )
    .await?;
    let fixture_head = anvil.client().block_number().await?;
    session
        .wait_for_checkpoint(
            &db.pool,
            fixture_head,
            Some(&catchup_ready_sql(chain, last_fixture_event_block)),
        )
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

    let live = live_ingest(&anvil, &chain, live_baseline).await?;
    let catchup =
        automatic_catchup(&anvil, &chain, catchup_baseline, fixture.last_event_block).await?;
    let live_snapshots = support::route_snapshots(&live, &chain.subjects()).await?;
    let catchup_snapshots = support::route_snapshots(&catchup, &chain.subjects()).await?;
    perturb::assert_snapshots_equal(&live_snapshots, &catchup_snapshots)?;
    perturb::assert_catchup_normalized_event_parity(
        &live.db.pool,
        &catchup.db.pool,
        CATCHUP_EQUIVALENCE_CONTRACT,
        &[fixture.expected_preimage],
    )
    .await?;

    live.db.cleanup().await?;
    catchup.db.cleanup().await?;
    Ok(())
}
