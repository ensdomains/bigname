use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde_json::Value;

use super::support;
use crate::harness::{
    anvil::Anvil,
    db::HarnessDb,
    ens_v1::{self, EnsV1Deployment},
    fault_proxy::{FaultKind, FaultProxy, FaultSpec},
    manifests::{self, LocalProfile},
    pipeline, repo_root,
    rpc::TxReceipt,
};

const INGEST_CHAIN: &str = "ethereum-e2e-rpc";
const TEXT_KEY: &str = "com.twitter";
const YEAR: u64 = 365 * 24 * 60 * 60;

struct Corpus {
    db: HarnessDb,
    _scratch: support::TempDir,
    profile: LocalProfile,
}

struct TextFixture {
    deployment: EnsV1Deployment,
    name: String,
    receipt: TxReceipt,
}

async fn deploy_text_fixture(anvil: &Anvil, label: &str, value: &str) -> Result<TextFixture> {
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let owner = accounts[1];
    let name = format!("{label}.eth");
    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        label,
        owner,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    let receipt = ens_v1::set_text_record_with_receipt(
        &rpc,
        deployment.public_resolver.address,
        owner,
        &name,
        TEXT_KEY,
        value,
    )
    .await?;
    rpc.mine(2).await?;
    Ok(TextFixture {
        deployment,
        name,
        receipt,
    })
}

async fn prepare_corpus(deployment: &EnsV1Deployment) -> Result<Corpus> {
    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    profile.retarget_chain("ethereum-mainnet", INGEST_CHAIN)?;
    let db = HarnessDb::create().await?;
    Ok(Corpus {
        db,
        _scratch: scratch,
        profile,
    })
}

async fn rpc_ingest(corpus: &Corpus, rpc_url: &str, head: u64) -> Result<String> {
    pipeline::run_rpc_ingest_redo(
        &repo_root(),
        &corpus.db.url,
        &corpus.db.pool,
        &corpus.profile.root,
        INGEST_CHAIN,
        rpc_url,
        0,
        head,
    )
    .await
}

async fn finish_spine(corpus: &Corpus, rpc_url: &str, head: u64) -> Result<()> {
    pipeline::run_existing_raw_spine(
        &repo_root(),
        &corpus.db.url,
        &corpus.db.pool,
        &corpus.profile.root,
        INGEST_CHAIN,
        rpc_url,
        head,
    )
    .await
}

async fn raw_log_count(pool: &sqlx::PgPool, transaction_hash: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs WHERE chain_id = $1 AND transaction_hash = $2",
    )
    .bind(INGEST_CHAIN)
    .bind(transaction_hash.to_ascii_lowercase())
    .fetch_one(pool)
    .await?)
}

async fn raw_receipt_count(pool: &sqlx::PgPool, transaction_hash: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM raw_receipts WHERE chain_id = $1 AND transaction_hash = $2",
    )
    .bind(INGEST_CHAIN)
    .bind(transaction_hash.to_ascii_lowercase())
    .fetch_one(pool)
    .await?)
}

async fn projected_text(pool: &sqlx::PgPool, name: &str) -> Result<Value> {
    let entries: Value = sqlx::query_scalar(
        "SELECT inventory.entries FROM name_current name \
         JOIN record_inventory_current inventory USING (resource_id) \
         WHERE name.namespace = 'ens' AND name.raw_name = $1 \
         ORDER BY inventory.inserted_at DESC LIMIT 1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .with_context(|| format!("load projected records for {name}"))?;
    entries
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry.get("record_key").and_then(Value::as_str) == Some("text:com.twitter"))
        .cloned()
        .with_context(|| format!("projected records for {name} omit text:{TEXT_KEY}: {entries}"))
}

async fn normalized_text(pool: &sqlx::PgPool, receipt: &TxReceipt) -> Result<Value> {
    sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE chain_id = $1 AND transaction_hash = $2 \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:com.twitter' \
           AND canonicality_state = 'canonical'",
    )
    .bind(INGEST_CHAIN)
    .bind(receipt.tx_hash.to_ascii_lowercase())
    .fetch_one(pool)
    .await
    .context("load normalized text event")
}

async fn ingest_clean_control(fixture: &TextFixture, anvil: &Anvil, head: u64) -> Result<Corpus> {
    let control = prepare_corpus(&fixture.deployment).await?;
    rpc_ingest(&control, &anvil.url, head).await?;
    finish_spine(&control, &anvil.url, head).await?;
    Ok(control)
}

#[tokio::test]
async fn silently_short_logs_are_accepted_until_explicit_refetch_matches_control() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let fixture = deploy_text_fixture(&anvil, "fault-short", "short").await?;
    let head = anvil.client().block_number().await?;
    let proxy = FaultProxy::spawn(&anvil.url).await?;
    proxy.add_fault(FaultSpec::drop_logs_once(&fixture.receipt.tx_hash, 1));

    let faulted = prepare_corpus(&fixture.deployment).await?;
    rpc_ingest(&faulted, &proxy.url, head).await?;
    ensure!(
        proxy.hit_count(FaultKind::DropLogs) == 1,
        "phase-runner ingest did not traverse the injected short-log response"
    );
    ensure!(
        raw_log_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 0,
        "the silently omitted log was unexpectedly materialized"
    );
    let published_head: i64 =
        sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = $1")
            .bind(INGEST_CHAIN)
            .fetch_one(&faulted.db.pool)
            .await?;
    ensure!(
        published_head == i64::try_from(head)?,
        "known defect #154 must remain explicit: the incomplete ingest was not published"
    );

    // Known defect #154: a valid but silently incomplete log array is accepted
    // and the harness's live-head boundary publishes the range without a
    // durable missing-fact signal. This clean ingest redo is an explicit test
    // action, not automatic repair triggered by the first command.
    rpc_ingest(&faulted, &anvil.url, head).await?;
    ensure!(
        raw_log_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1,
        "clean phase-runner refetch did not repair the omitted raw log"
    );
    finish_spine(&faulted, &anvil.url, head).await?;

    let control = ingest_clean_control(&fixture, &anvil, head).await?;
    assert_eq!(
        normalized_text(&faulted.db.pool, &fixture.receipt).await?,
        normalized_text(&control.db.pool, &fixture.receipt).await?
    );
    assert_eq!(
        projected_text(&faulted.db.pool, &fixture.name).await?,
        projected_text(&control.db.pool, &fixture.name).await?
    );
    proxy.assert_healthy()?;
    faulted.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn transient_provider_faults_and_partial_receipts_recover_to_control() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let fixture = deploy_text_fixture(&anvil, "fault-retry", "recovered").await?;
    let head = anvil.client().block_number().await?;
    let proxy = FaultProxy::spawn(&anvil.url).await?;
    proxy.add_faults([
        FaultSpec::error_once(&fixture.receipt.tx_hash, -32005, "injected capacity limit"),
        FaultSpec::delay_timeout_once(&fixture.receipt.tx_hash, Duration::from_millis(20)),
        FaultSpec::truncate_once(&fixture.receipt.tx_hash, 8),
    ]);

    let faulted = prepare_corpus(&fixture.deployment).await?;
    let first_attempt = rpc_ingest(&faulted, &proxy.url, head).await;
    for kind in [
        FaultKind::ErrorOnce,
        FaultKind::DelayTimeout,
        FaultKind::Truncate,
    ] {
        ensure!(
            proxy.hit_count(kind) == 1,
            "phase-runner ingest observed {} {kind:?} hits instead of one",
            proxy.hit_count(kind)
        );
    }
    ensure!(
        first_attempt.is_err(),
        "the truncated JSON response should terminate the first bounded redo"
    );

    // The structurally malformed response ends that command before it reaches
    // receipt hydration. A second command traverses the remaining partial-
    // receipt injection; the final clean redo is the explicit repair boundary.
    proxy.add_fault(FaultSpec::drop_receipts_once(&fixture.receipt.tx_hash, 1));
    let receipt_attempt = rpc_ingest(&faulted, &proxy.url, head).await;
    ensure!(
        proxy.hit_count(FaultKind::DropReceipts) == 1,
        "phase-runner ingest observed {} DropReceipts hits instead of one; second attempt: {:?}",
        proxy.hit_count(FaultKind::DropReceipts),
        receipt_attempt.as_ref().err()
    );
    ensure!(
        receipt_attempt.is_err()
            || raw_receipt_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 0,
        "partial receipt injection unexpectedly produced a complete target receipt"
    );
    rpc_ingest(&faulted, &anvil.url, head).await?;
    ensure!(
        raw_log_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1
            && raw_receipt_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1,
        "clean recovery did not retain the target log and receipt"
    );
    finish_spine(&faulted, &anvil.url, head).await?;

    let control = ingest_clean_control(&fixture, &anvil, head).await?;
    assert_eq!(
        normalized_text(&faulted.db.pool, &fixture.receipt).await?,
        normalized_text(&control.db.pool, &fixture.receipt).await?
    );
    assert_eq!(
        projected_text(&faulted.db.pool, &fixture.name).await?,
        projected_text(&control.db.pool, &fixture.name).await?
    );
    proxy.assert_healthy()?;
    faulted.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "retired: runtime bytecode-hash resolver admission and its eth_getCode retry path were deleted in Stage B"]
async fn transient_get_code_retries_primary_without_using_configured_fallback() -> Result<()> {
    Ok(())
}

#[tokio::test]
#[ignore = "retired: runtime bytecode-hash resolver admission and its archive eth_getCode fallback were deleted in Stage B"]
async fn pruned_get_code_fails_closed_then_uses_configured_fallback() -> Result<()> {
    Ok(())
}
