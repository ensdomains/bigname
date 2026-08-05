use std::collections::BTreeMap;

use alloy_primitives::Address;
use anyhow::{Result, anyhow, ensure};
use serde_json::{Value, json};

use super::support;
use crate::harness::{
    anvil::Anvil, db::HarnessDb, ens_v1, manifests, perturb, pipeline, repo_root,
};

const CHAIN: &str = "ethereum-e2e-rpc";
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
struct CatchupChain {
    deployment: ens_v1::EnsV1Deployment,
    owner: Address,
    record_target: Address,
    child_owner: Address,
    resolver: Address,
}

struct CatchupFixture {
    expected_preimage_names: Vec<String>,
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

async fn add_rich_name_fixture(anvil: &Anvil, chain: &CatchupChain) -> Result<CatchupFixture> {
    let rpc = anvil.client();
    ens_v1::register_eth_name(
        &rpc,
        &chain.deployment,
        LABEL,
        chain.owner,
        YEAR,
        chain.resolver,
    )
    .await?;
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
    rpc.mine(2).await?;
    Ok(CatchupFixture {
        expected_preimage_names: vec![NAME.to_owned()],
    })
}

async fn add_wrapper_reverse_fixture(
    anvil: &Anvil,
    chain: &CatchupChain,
) -> Result<CatchupFixture> {
    let rpc = anvil.client();
    ens_v1::register_eth_name(
        &rpc,
        &chain.deployment,
        WRAPPED_LABEL,
        chain.owner,
        YEAR,
        chain.resolver,
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

    ens_v1::register_eth_name(
        &rpc,
        &chain.deployment,
        RESTORED_LABEL,
        chain.owner,
        YEAR,
        chain.resolver,
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

    rpc.mine(2).await?;
    Ok(CatchupFixture {
        expected_preimage_names: vec![WRAPPED_NAME.to_owned(), RESTORED_NAME.to_owned()],
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
                } else if matches!(key.as_str(), "reverse_event_id" | "claim_event_id")
                    && !value.is_null()
                {
                    *value = Value::String("<normalized_event_id>".to_owned());
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
    if let Some(last_updated) = value.get_mut("last_updated") {
        ensure!(
            last_updated.is_string(),
            "primary-name route snapshot last_updated is not a string: {last_updated}"
        );
        *last_updated = Value::String("<last_updated>".to_owned());
    }
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
            .pointer("/declared_state/control/registrant")
            .and_then(Value::as_str)
            == Some(format!("{:#x}", chain.record_target).as_str())
            && wrapped
                .pointer("/declared_state/registration/authority_kind")
                .and_then(Value::as_str)
                == Some("wrapper"),
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
            == Some("not_found"),
        "{claimant} route snapshot must keep the generic resolver NameChanged claim raw-only: {primary}"
    );
    let contract_instances = perturb::contract_instance_stable_keys(&run.db.pool).await?;
    normalize_primary_route_snapshot(&mut primary, &contract_instances)?;
    snapshots.insert(format!("GET {primary_path}"), primary);
    Ok(snapshots)
}

fn derived_output_ready_expression(chain: &CatchupChain) -> String {
    let parent_node = format!("{:#x}", ens_v1::namehash(NAME));
    let sub_labelhash = format!("{:#x}", ens_v1::labelhash(SUB_LABEL));
    let logical_name_id = support::schema_v2_logical_name_id(&format!("ens:{NAME}"));
    format!(
        "EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{logical_name_id}' AND event_kind = 'ResolverChanged' \
         AND canonicality_state = 'canonical' \
         AND lower(after_state->>'resolver') = '{resolver:#x}') \
         AND (SELECT count(DISTINCT after_state->>'record_key') >= 2 FROM normalized_events \
         WHERE logical_name_id = '{logical_name_id}' AND event_kind = 'RecordChanged' \
         AND canonicality_state = 'canonical' \
         AND after_state->>'record_key' IN ('addr:60', 'text:{TEXT_KEY}')) \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
         AND canonicality_state = 'canonical' \
         AND lower(after_state->>'node') = '{parent_node}' \
         AND lower(after_state->>'labelhash') = '{sub_labelhash}' \
         AND lower(after_state->>'owner') = '{child_owner:#x}')",
        resolver = chain.resolver,
        child_owner = chain.child_owner,
    )
}

fn rich_ready_sql(chain: &CatchupChain) -> String {
    format!(
        "SELECT {} \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'PreimageObserved' \
         AND source_family = 'ens_v1_registrar_l1' \
         AND derivation_kind = 'raw_log_preimage_observation' \
         AND after_state->>'raw_name' = '{NAME}' \
         AND after_state->'raw_labels'->>0 = '{LABEL}' \
         AND canonicality_state = 'canonical')",
        derived_output_ready_expression(chain),
    )
}

fn wrapper_reverse_derived_output_ready_expression(chain: &CatchupChain) -> String {
    let wrapped_logical_id = support::schema_v2_logical_name_id(&format!("ens:{WRAPPED_NAME}"));
    let restored_logical_id = support::schema_v2_logical_name_id(&format!("ens:{RESTORED_NAME}"));
    format!(
        "EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{wrapped_logical_id}' \
         AND event_kind = 'AuthorityEpochChanged' \
         AND source_family = 'ens_v1_wrapper_l1' \
         AND canonicality_state = 'canonical' \
         AND before_state->>'authority_kind' = 'registrar' \
         AND after_state->>'authority_kind' = 'wrapper') \
         AND (SELECT count(*) >= 2 FROM normalized_events \
         WHERE logical_name_id = '{wrapped_logical_id}' \
         AND event_kind = 'PermissionScopeChanged' \
         AND source_family = 'ens_v1_wrapper_l1' \
         AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{wrapped_logical_id}' \
         AND event_kind = 'PermissionScopeChanged' \
         AND source_family = 'ens_v1_wrapper_l1' \
         AND canonicality_state = 'canonical' \
         AND ((after_state->>'fuses')::BIGINT & {FIXTURE_FUSE}) = {FIXTURE_FUSE}) \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{restored_logical_id}' \
         AND event_kind = 'AuthorityEpochChanged' \
         AND canonicality_state = 'canonical' \
         AND before_state->>'authority_kind' = 'wrapper' \
         AND after_state->>'authority_kind' = 'registrar') \
         AND EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'ReverseChanged' \
         AND source_family = 'ens_v1_reverse_l1' \
         AND canonicality_state = 'canonical' \
         AND lower(after_state->>'address') = '{claimant:#x}')",
        claimant = chain.child_owner,
    )
}

fn wrapper_reverse_ready_sql(chain: &CatchupChain) -> String {
    format!(
        "SELECT {} \
         AND (SELECT count(DISTINCT after_state->>'raw_name') = 2 \
         FROM normalized_events \
         WHERE event_kind = 'PreimageObserved' \
         AND source_family = 'ens_v1_registrar_l1' \
         AND derivation_kind = 'raw_log_preimage_observation' \
         AND after_state->>'raw_name' IN ('{WRAPPED_NAME}', '{RESTORED_NAME}') \
         AND canonicality_state = 'canonical')",
        wrapper_reverse_derived_output_ready_expression(chain),
    )
}

#[derive(Clone, Copy)]
enum RawFactPath {
    UpfrontFixture,
    RpcIngest,
}

async fn run_corpus(
    anvil: &Anvil,
    chain: &CatchupChain,
    ready_sql: &str,
    path: RawFactPath,
) -> Result<support::PipelineRun> {
    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &root,
        &chain.deployment.manifest_targets(),
    )?;
    profile.retarget_chain("ethereum-mainnet", CHAIN)?;
    let db = HarnessDb::create().await?;
    let head = anvil.client().block_number().await?;
    let chain_rpc_urls = [(CHAIN, anvil.url.as_str())];
    match path {
        RawFactPath::UpfrontFixture => {
            pipeline::run_fixture_spines_through_targets(
                &root,
                &db.url,
                &db.pool,
                &profile.root,
                &chain_rpc_urls,
                &[(CHAIN, head)],
                Some(ready_sql),
            )
            .await?;
        }
        RawFactPath::RpcIngest => {
            pipeline::run_rpc_ingest_redo(
                &root,
                &db.url,
                &db.pool,
                &profile.root,
                CHAIN,
                &anvil.url,
                0,
                head,
            )
            .await?;
            pipeline::run_existing_raw_spine(
                &root,
                &db.url,
                &db.pool,
                &profile.root,
                CHAIN,
                &anvil.url,
                head,
            )
            .await?;
            let ready: bool = sqlx::query_scalar(ready_sql).fetch_one(&db.pool).await?;
            ensure!(
                ready,
                "RPC-ingest corpus did not satisfy semantic readiness: {ready_sql}"
            );
        }
    }
    support::serve_existing_db(db, scratch, anvil).await
}

#[tokio::test]
async fn upfront_facts_match_rpc_ingest_outputs() -> Result<()> {
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

    let fixture = add_rich_name_fixture(&anvil, &chain).await?;
    let ready_sql = rich_ready_sql(&chain);
    let upfront = run_corpus(&anvil, &chain, &ready_sql, RawFactPath::UpfrontFixture).await?;
    let ingested = run_corpus(&anvil, &chain, &ready_sql, RawFactPath::RpcIngest).await?;
    let upfront_snapshots = support::route_snapshots(&upfront, &chain.subjects()).await?;
    let ingested_snapshots = support::route_snapshots(&ingested, &chain.subjects()).await?;
    perturb::assert_snapshots_equal(&upfront_snapshots, &ingested_snapshots)?;
    perturb::assert_ingest_path_normalized_event_parity(
        &upfront.db.pool,
        &ingested.db.pool,
        &fixture.expected_preimage_names,
    )
    .await?;

    upfront.db.cleanup().await?;
    ingested.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn upfront_facts_match_rpc_ingest_wrapper_reverse_outputs() -> Result<()> {
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

    let fixture = add_wrapper_reverse_fixture(&anvil, &chain).await?;
    let ready_sql = wrapper_reverse_ready_sql(&chain);
    let upfront = run_corpus(&anvil, &chain, &ready_sql, RawFactPath::UpfrontFixture).await?;
    let ingested = run_corpus(&anvil, &chain, &ready_sql, RawFactPath::RpcIngest).await?;
    let upfront_snapshots = wrapper_reverse_route_snapshots(&upfront, &chain).await?;
    let ingested_snapshots = wrapper_reverse_route_snapshots(&ingested, &chain).await?;
    perturb::assert_snapshots_equal(&upfront_snapshots, &ingested_snapshots)?;
    perturb::assert_ingest_path_normalized_event_parity(
        &upfront.db.pool,
        &ingested.db.pool,
        &fixture.expected_preimage_names,
    )
    .await?;

    upfront.db.cleanup().await?;
    ingested.db.cleanup().await?;
    Ok(())
}
