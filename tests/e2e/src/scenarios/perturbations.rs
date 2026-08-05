use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{
    anvil::Anvil, db::HarnessDb, ens_v1, manifests, perturb, pipeline, repo_root,
};

const NAME: &str = "perturb.eth";
const LABEL: &str = "perturb";
const SUB_LABEL: &str = "sub";
const TEXT_KEY: &str = "com.twitter";
const YEAR: u64 = 365 * 24 * 60 * 60;
const REORG_CHAIN: &str = "ethereum-e2e-reorg";

struct PerturbationChain {
    deployment: ens_v1::EnsV1Deployment,
    owner: Address,
    record_target: Address,
    child_owner: Address,
    resolver: Address,
}

impl PerturbationChain {
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

async fn deploy_registered_name(anvil: &Anvil) -> Result<PerturbationChain> {
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let chain = PerturbationChain {
        resolver: deployment.public_resolver.address,
        deployment,
        owner: accounts[1],
        record_target: accounts[2],
        child_owner: accounts[3],
    };
    ens_v1::register_eth_name(
        &rpc,
        &chain.deployment,
        LABEL,
        chain.owner,
        YEAR,
        chain.resolver,
    )
    .await?;
    Ok(chain)
}

async fn add_records_and_subname(anvil: &Anvil, chain: &PerturbationChain) -> Result<()> {
    let rpc = anvil.client();
    ens_v1::set_addr_record(&rpc, chain.resolver, chain.owner, NAME, chain.record_target).await?;
    ens_v1::set_text_record(&rpc, chain.resolver, chain.owner, NAME, TEXT_KEY, "perturb").await?;
    ens_v1::create_subname(
        &rpc,
        &chain.deployment,
        chain.owner,
        NAME,
        SUB_LABEL,
        chain.child_owner,
    )
    .await?;
    Ok(())
}

async fn build_rich_chain(anvil: &Anvil) -> Result<PerturbationChain> {
    let chain = deploy_registered_name(anvil).await?;
    add_records_and_subname(anvil, &chain).await?;
    Ok(chain)
}

fn rich_ready_sql(resolver: Address, child_owner: Address) -> String {
    let parent_node = format!("{:#x}", ens_v1::namehash(NAME));
    let sub_labelhash = format!("{:#x}", ens_v1::labelhash(SUB_LABEL));
    let logical_name_id = support::schema_v2_logical_name_id(&format!("ens:{NAME}"));
    format!(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events event \
            JOIN chain_lineage lineage USING (chain_id, block_hash) \
            WHERE event.logical_name_id = '{logical_name_id}' \
            AND event.event_kind = 'ResolverChanged' \
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized') \
            AND lower(event.after_state->>'resolver') = '{resolver:#x}') \
         AND \
           (SELECT count(DISTINCT event.after_state->>'record_key') >= 2 \
            FROM normalized_events event \
            JOIN chain_lineage lineage USING (chain_id, block_hash) \
            WHERE event.logical_name_id = '{logical_name_id}' \
            AND event.event_kind = 'RecordChanged' \
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized') \
            AND event.after_state->>'record_key' IN ('addr:60', 'text:{TEXT_KEY}')) \
         AND \
           EXISTS (SELECT 1 FROM normalized_events event \
            JOIN chain_lineage lineage USING (chain_id, block_hash) \
            WHERE event.event_kind = 'SubregistryChanged' \
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized') \
            AND lower(event.after_state->>'node') = '{parent_node}' \
            AND lower(event.after_state->>'labelhash') = '{sub_labelhash}' \
            AND lower(event.after_state->>'owner') = '{child_owner:#x}')"
    )
}

async fn chain_snapshots(
    run: &support::PipelineRun,
    chain: &PerturbationChain,
) -> Result<perturb::RouteSnapshots> {
    support::route_snapshots(run, &chain.subjects()).await
}

async fn assert_exact_resolver(run: &support::PipelineRun, resolver: Address) -> Result<()> {
    let (status, body) = run.api.get_json("/v1/names/ens/perturb.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    assert_eq!(
        body.pointer("/declared_state/resolver/address")
            .cloned()
            .unwrap_or(Value::Null),
        format!("{resolver:#x}"),
        "winning resolver should serve in exact-name output; body: {body}"
    );
    Ok(())
}

#[tokio::test]
async fn rich_chain_projection_and_normalized_event_replay_are_route_stable() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let chain = build_rich_chain(&anvil).await?;
    let ready_sql = rich_ready_sql(chain.resolver, chain.child_owner);
    let run = support::ingest_and_serve(&anvil, &chain.deployment, Some(&ready_sql)).await?;

    let before = chain_snapshots(&run, &chain).await?;
    let head = anvil.client().block_number().await?;
    pipeline::phase_runner_replay_current_projections(
        &repo_root(),
        &run.db.url,
        &run.manifests_root,
        &anvil.url,
        head,
    )
    .await?;
    let after_projection_replay = chain_snapshots(&run, &chain).await?;
    perturb::assert_snapshots_equal(&before, &after_projection_replay)?;

    pipeline::phase_runner_replay_normalized_events(
        &repo_root(),
        &run.db.url,
        &run.manifests_root,
        &anvil.url,
        head,
    )
    .await?;
    let after_normalized_replay = chain_snapshots(&run, &chain).await?;
    perturb::assert_snapshots_equal(&before, &after_normalized_replay)?;

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rich_chain_successive_fixture_replays_match_single_pass() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let chain = deploy_registered_name(&anvil).await?;
    let ready_sql = rich_ready_sql(chain.resolver, chain.child_owner);

    let successive =
        support::ingest_with_successive_replay_and_serve(&anvil, &chain.deployment, || async {
            add_records_and_subname(&anvil, &chain).await?;
            let rpc = anvil.client();
            rpc.mine(2).await?;
            Ok(pipeline::ReplayCompletion {
                target_block: rpc.block_number().await?,
                extra_ready_sql: Some(ready_sql.clone()),
            })
        })
        .await?;
    let successive_snapshots = chain_snapshots(&successive, &chain).await?;

    let control =
        support::ingest_at_current_head(&anvil, &chain.deployment, Some(&ready_sql)).await?;
    let control_snapshots = chain_snapshots(&control, &chain).await?;
    perturb::assert_snapshots_equal(&control_snapshots, &successive_snapshots)?;

    successive.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rich_chain_rpc_ingest_normalized_events_match_upfront_facts() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let chain = build_rich_chain(&anvil).await?;
    let upfront = support::derive_ens_v1_from_upfront_facts(&anvil, &chain.deployment).await?;
    let ingested = support::derive_ens_v1_from_rpc_ingest(&anvil, &chain.deployment).await?;
    perturb::assert_ingest_path_normalized_event_parity(
        &upfront.db.pool,
        &ingested.db.pool,
        &[NAME.to_owned()],
    )
    .await?;

    upfront.db.cleanup().await?;
    ingested.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rich_chain_live_reorg_converges_to_winning_branch() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let chain = build_rich_chain(&anvil).await?;
    let replacement_resolver =
        ens_v1::deploy_extra_public_resolver(&rpc, &repo_root(), &chain.deployment).await?;
    rpc.mine(2).await?;
    let pre_reorg_head = rpc.block_number().await?;
    let pre_reorg_hash = rpc.block_hash(pre_reorg_head).await?;
    let snapshot_id = rpc.evm_snapshot().await?;

    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &root,
        &chain.deployment.manifest_targets(),
    )?;
    profile.retarget_chain("ethereum-mainnet", REORG_CHAIN)?;
    let db = HarnessDb::create().await?;
    pipeline::run_rpc_ingest_redo(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        REORG_CHAIN,
        &anvil.url,
        0,
        pre_reorg_head,
    )
    .await?;
    pipeline::run_existing_raw_spine(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        REORG_CHAIN,
        &anvil.url,
        pre_reorg_head,
    )
    .await?;

    ens_v1::set_text_record(&rpc, chain.resolver, chain.owner, NAME, TEXT_KEY, "losing").await?;
    let losing_event_block = rpc.block_number().await?;
    let losing_hash = rpc.block_hash(losing_event_block).await?;
    rpc.mine(3).await?;
    let losing_head = rpc.block_number().await?;
    let losing_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events event \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE event.chain_id = '{REORG_CHAIN}' AND event.block_hash = '{losing_hash}' \
         AND event.event_kind = 'RecordChanged' \
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))"
    );
    pipeline::run_rpc_ingest_redo(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        REORG_CHAIN,
        &anvil.url,
        pre_reorg_head + 1,
        losing_head,
    )
    .await?;
    pipeline::run_existing_raw_spine(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        REORG_CHAIN,
        &anvil.url,
        losing_head,
    )
    .await?;
    let losing_ready: bool = sqlx::query_scalar(&losing_ready_sql)
        .fetch_one(&db.pool)
        .await?;
    assert!(
        losing_ready,
        "losing branch was not interpreted before rewind"
    );

    rpc.evm_revert(&snapshot_id).await?;
    ens_v1::set_resolver(
        &rpc,
        &chain.deployment,
        chain.owner,
        NAME,
        replacement_resolver.address,
    )
    .await?;
    let winning_event_block = rpc.block_number().await?;
    let winning_hash = rpc.block_hash(winning_event_block).await?;
    rpc.mine(3).await?;
    let post_reorg_head = rpc.block_number().await?;
    assert_eq!(
        post_reorg_head, losing_head,
        "the test keeps both forks at one height so the stamped redo covers the complete winner"
    );
    let winning_ready_sql = rich_ready_sql(replacement_resolver.address, chain.child_owner);
    pipeline::rewind_to_ancestor(&root, &db.url, REORG_CHAIN, pre_reorg_head, &pre_reorg_hash)
        .await?;
    let losing_rows_before_redo: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        losing_rows_before_redo > 0,
        "head publication must retain losing normalized rows until stamped redo starts"
    );
    let readable_losing_rows_before_redo: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events event
         JOIN chain_lineage lineage USING (chain_id, block_hash)
         WHERE event.chain_id = $1 AND event.block_hash = $2
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        readable_losing_rows_before_redo, 0,
        "the lineage join must exclude losing normalized rows before stamped redo"
    );
    let logical_name_id = support::schema_v2_logical_name_id(&format!("ens:{NAME}"));
    let production_history = bigname_storage::load_event_history(
        &db.pool,
        bigname_storage::EventHistoryFilter {
            namespace: Some("ens".to_owned()),
            logical_name_id: Some(logical_name_id),
            event_kinds: vec!["RecordChanged".to_owned()],
            from_block: Some(i64::try_from(losing_event_block)?),
            to_block: Some(i64::try_from(losing_event_block)?),
            ..bigname_storage::EventHistoryFilter::default()
        },
        true,
    )
    .await?;
    assert!(
        production_history
            .iter()
            .all(|event| event.block_hash.as_deref() != Some(losing_hash.as_str())),
        "the production canonical-history reader exposed a losing event through row-local canonicality after lineage orphaning"
    );
    pipeline::run_rpc_ingest_redo(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        REORG_CHAIN,
        &anvil.url,
        pre_reorg_head + 1,
        post_reorg_head,
    )
    .await?;
    pipeline::run_required_reorg_spine(
        &root,
        &db.url,
        &db.pool,
        &profile.root,
        REORG_CHAIN,
        &anvil.url,
    )
    .await?;
    let winning_ready: bool = sqlx::query_scalar(&winning_ready_sql)
        .fetch_one(&db.pool)
        .await?;
    let post_rewind_events: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'event_kind', event_kind, 'block_number', block_number,
             'block_hash', block_hash,
             'after_state', after_state)
           ORDER BY block_number, log_index), '[]'::jsonb)
         FROM normalized_events WHERE chain_id = $1 AND block_number >= $2",
    )
    .bind(REORG_CHAIN)
    .bind(i64::try_from(pre_reorg_head)?)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        winning_ready,
        "winning branch was not projected after rewind; post-rewind events: {post_rewind_events}"
    );

    let losing_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await
    .context("count superseded losing normalized events")?;
    assert_eq!(
        losing_event_count, 0,
        "completed interpret redo must remove normalized events from losing block {losing_hash}"
    );
    let winning_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events event
         JOIN chain_lineage lineage USING (chain_id, block_hash)
         WHERE event.chain_id = $1 AND event.block_hash = $2
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(REORG_CHAIN)
    .bind(&winning_hash)
    .fetch_one(&db.pool)
    .await
    .context("count winning normalized events")?;
    assert!(
        winning_event_count > 0,
        "completed interpret redo must derive readable events from winning block {winning_hash}"
    );
    let retained_raw_logs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE chain_id = $1 AND block_hash = $2")
            .bind(REORG_CHAIN)
            .bind(&losing_hash)
            .fetch_one(&db.pool)
            .await
            .context("count retained losing raw logs")?;
    assert!(
        retained_raw_logs > 0,
        "losing block {losing_hash} must retain its immutable raw logs"
    );
    let losing_lineage_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM chain_lineage
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(REORG_CHAIN)
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await
    .context("load losing lineage canonicality")?;
    assert_eq!(
        losing_lineage_state, "orphaned",
        "losing block {losing_hash} must remain in permanent orphaned lineage"
    );

    let reorg_run = support::serve_existing_db(db, scratch, &anvil).await?;
    assert_exact_resolver(&reorg_run, replacement_resolver.address).await?;
    let reorg_snapshots = chain_snapshots(&reorg_run, &chain).await?;

    let control_scratch = support::TempDir::create()?;
    let control_profile = manifests::generate_local_profile(
        control_scratch.path(),
        &root,
        &chain.deployment.manifest_targets(),
    )?;
    control_profile.retarget_chain("ethereum-mainnet", REORG_CHAIN)?;
    let control_db = HarnessDb::create().await?;
    pipeline::run_rpc_ingest_redo(
        &root,
        &control_db.url,
        &control_db.pool,
        &control_profile.root,
        REORG_CHAIN,
        &anvil.url,
        0,
        post_reorg_head,
    )
    .await?;
    pipeline::run_existing_raw_spine(
        &root,
        &control_db.url,
        &control_db.pool,
        &control_profile.root,
        REORG_CHAIN,
        &anvil.url,
        post_reorg_head,
    )
    .await?;
    let control = support::serve_existing_db(control_db, control_scratch, &anvil).await?;
    let control_snapshots = chain_snapshots(&control, &chain).await?;
    perturb::assert_snapshots_equal(&control_snapshots, &reorg_snapshots)?;

    reorg_run.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}
