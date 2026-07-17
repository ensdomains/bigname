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

fn rich_ready_sql(
    resolver: Address,
    profile_seed_resolver: Address,
    child_owner: Address,
) -> String {
    let parent_node = format!("{:#x}", ens_v1::namehash(NAME));
    let sub_labelhash = format!("{:#x}", ens_v1::labelhash(SUB_LABEL));
    let resolver_profile_ready =
        support::resolver_code_hash_comparison_sql(resolver, profile_seed_resolver, true);
    format!(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:{NAME}' AND event_kind = 'ResolverChanged' \
            AND canonicality_state = 'canonical' \
            AND lower(after_state->>'resolver') = '{resolver:#x}') \
         AND \
           (SELECT count(DISTINCT after_state->>'record_key') >= 2 FROM normalized_events \
            WHERE logical_name_id = 'ens:{NAME}' AND event_kind = 'RecordChanged' \
            AND canonicality_state = 'canonical' \
            AND after_state->>'record_key' IN ('addr:60', 'text:{TEXT_KEY}')) \
         AND \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE event_kind = 'SubregistryChanged' AND canonicality_state = 'canonical' \
            AND lower(after_state->>'parent_node') = '{parent_node}' \
            AND lower(after_state->>'labelhash') = '{sub_labelhash}' \
            AND lower(after_state->>'owner') = '{child_owner:#x}') \
         AND {resolver_profile_ready}"
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
    let ready_sql = rich_ready_sql(chain.resolver, chain.resolver, chain.child_owner);
    let run = support::ingest_and_serve(&anvil, &chain.deployment, Some(&ready_sql)).await?;

    let before = chain_snapshots(&run, &chain).await?;
    pipeline::worker_replay_all_current_projections(&repo_root(), &run.db.url).await?;
    let after_worker_replay = chain_snapshots(&run, &chain).await?;
    perturb::assert_snapshots_equal(&before, &after_worker_replay)?;

    let head = anvil.client().block_number().await?;
    pipeline::indexer_replay_normalized_events(&repo_root(), &run.db.url, head).await?;
    pipeline::worker_replay_all_current_projections(&repo_root(), &run.db.url).await?;
    let after_normalized_replay = chain_snapshots(&run, &chain).await?;
    perturb::assert_snapshots_equal(&before, &after_normalized_replay)?;

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rich_chain_indexer_restart_mid_scenario_matches_control() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let chain = deploy_registered_name(&anvil).await?;
    let ready_sql = rich_ready_sql(chain.resolver, chain.resolver, chain.child_owner);

    let restarted = support::ingest_with_restart_and_serve(&anvil, &chain.deployment, || async {
        add_records_and_subname(&anvil, &chain).await?;
        let rpc = anvil.client();
        rpc.mine(2).await?;
        Ok(pipeline::RestartCompletion {
            target_block: rpc.block_number().await?,
            extra_ready_sql: Some(ready_sql.clone()),
        })
    })
    .await?;
    let restarted_snapshots = chain_snapshots(&restarted, &chain).await?;

    let control =
        support::ingest_at_current_head(&anvil, &chain.deployment, Some(&ready_sql)).await?;
    let control_snapshots = chain_snapshots(&control, &chain).await?;
    perturb::assert_snapshots_equal(&control_snapshots, &restarted_snapshots)?;

    restarted.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rich_chain_backfill_normalized_events_match_live_ingest() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let chain = build_rich_chain(&anvil).await?;
    let ready_sql = rich_ready_sql(chain.resolver, chain.resolver, chain.child_owner);
    let live = support::ingest_and_serve(&anvil, &chain.deployment, Some(&ready_sql)).await?;
    let _live_route_snapshots = chain_snapshots(&live, &chain).await?;

    let backfill =
        support::backfill_normalized_events(&anvil, &chain.deployment, "rich-chain-backfill")
            .await?;
    perturb::assert_backfill_normalized_event_parity(
        &live.db.pool,
        &backfill.db.pool,
        &[&format!("ens:{NAME}")],
    )
    .await?;

    live.db.cleanup().await?;
    backfill.db.cleanup().await?;
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
    let snapshot_id = rpc.evm_snapshot().await?;

    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &root,
        &chain.deployment.manifest_targets(),
    )?;
    let db = HarnessDb::create().await?;
    let mut session =
        pipeline::IndexerRunSession::start(&root, &db.url, &profile.root, &anvil.url, "reorg-live")
            .await?;
    session
        .wait_for_checkpoint(
            &db.pool,
            pre_reorg_head,
            Some(&rich_ready_sql(
                chain.resolver,
                chain.resolver,
                chain.child_owner,
            )),
        )
        .await?;

    ens_v1::set_text_record(&rpc, chain.resolver, chain.owner, NAME, TEXT_KEY, "losing").await?;
    let losing_head = rpc.block_number().await?;
    let losing_hash = rpc.block_hash(losing_head).await?;
    let losing_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE block_hash = '{losing_hash}' AND event_kind = 'RecordChanged' \
         AND canonicality_state = 'canonical')"
    );
    session
        .wait_for_checkpoint(&db.pool, losing_head, Some(&losing_ready_sql))
        .await?;

    rpc.evm_revert(&snapshot_id).await?;
    ens_v1::set_resolver(
        &rpc,
        &chain.deployment,
        chain.owner,
        NAME,
        replacement_resolver.address,
    )
    .await?;
    rpc.mine(3).await?;
    let post_reorg_head = rpc.block_number().await?;
    let winning_ready_sql = rich_ready_sql(
        replacement_resolver.address,
        chain.resolver,
        chain.child_owner,
    );
    session
        .wait_for_checkpoint(&db.pool, post_reorg_head, Some(&winning_ready_sql))
        .await?;
    session.stop().await?;
    pipeline::worker_replay_all_current_projections(&root, &db.url).await?;

    let orphaned_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE block_hash = $1 AND canonicality_state = 'orphaned'",
    )
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await
    .context("count orphaned normalized events")?;
    assert!(
        orphaned_events > 0,
        "losing block {losing_hash} should retain orphaned normalized events"
    );
    let orphaned_raw_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs WHERE block_hash = $1 AND canonicality_state = 'orphaned'",
    )
    .bind(&losing_hash)
    .fetch_one(&db.pool)
    .await
    .context("count orphaned raw logs")?;
    assert!(
        orphaned_raw_logs > 0,
        "losing block {losing_hash} should retain orphaned raw logs"
    );

    let reorg_run = support::serve_existing_db(db, scratch).await?;
    assert_exact_resolver(&reorg_run, replacement_resolver.address).await?;
    let reorg_snapshots = chain_snapshots(&reorg_run, &chain).await?;

    let control =
        support::ingest_at_current_head(&anvil, &chain.deployment, Some(&winning_ready_sql))
            .await?;
    let control_snapshots = chain_snapshots(&control, &chain).await?;
    perturb::assert_snapshots_equal(&control_snapshots, &reorg_snapshots)?;

    reorg_run.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}
