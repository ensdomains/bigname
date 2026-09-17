use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;

use super::support;
use crate::harness::{
    anvil::Anvil,
    db::HarnessDb,
    ens_v1::{self, EnsV1Deployment},
    facts,
    fault_proxy::{FaultKind, FaultProxy, FaultSpec},
    manifests::{self, LocalProfile},
    pipeline, repo_root,
    rpc::TxReceipt,
};

const INGEST_CHAIN: &str = "ethereum-e2e-rpc";
const TEXT_KEY: &str = "com.twitter";
const YEAR: u64 = 365 * 24 * 60 * 60;
/// RPC ingest reads a window once and re-fetches it at most twice after a rejected
/// provider response; a response still rejected on the last read is terminal.
const WINDOW_READ_ATTEMPTS: usize = 3;

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

/// Progress an ingest redo command is able to move: the redo's own position,
/// the ordinary ingest position the harness seeds before the command, the
/// per-source cursor, and the published chain head. Timestamps and error text
/// are left out so a failed command that only records its failure compares equal.
#[derive(Debug, PartialEq)]
struct IngestProgress {
    redo_in_progress: bool,
    redo_current_block_number: Option<i64>,
    ordinary: Value,
    source_cursor: Value,
    published_head: Option<Value>,
}

async fn ingest_progress(pool: &sqlx::PgPool) -> Result<IngestProgress> {
    let (redo_in_progress, redo_current_block_number, ordinary): (bool, Option<i64>, Value) =
        sqlx::query_as(
            "SELECT redo_in_progress, redo_current_block_number,
                    jsonb_build_object(
                        'current_block_number', current_block_number,
                        'current_block_hash', current_block_hash,
                        'target_block_number', target_block_number,
                        'target_block_hash', target_block_hash,
                        'live_handoff_block_number', live_handoff_block_number,
                        'live_handoff_block_hash', live_handoff_block_hash)
             FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
        )
        .bind(INGEST_CHAIN)
        .fetch_one(pool)
        .await
        .context("load ingest phase progress")?;
    let source_cursor = sqlx::query_scalar(
        "SELECT jsonb_build_object(
                    'next_block_number', next_block_number,
                    'target_block_number', target_block_number,
                    'last_processed_block_number', last_processed_block_number,
                    'last_processed_block_hash', last_processed_block_hash)
         FROM ingest_cursors WHERE chain_id = $1 AND source_key = 'e2e-rpc'",
    )
    .bind(INGEST_CHAIN)
    .fetch_one(pool)
    .await
    .context("load ingest source cursor")?;
    let published_head = sqlx::query_scalar(
        "SELECT jsonb_build_object(
                    'latest_block_number', latest_block_number,
                    'latest_block_hash', latest_block_hash)
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(INGEST_CHAIN)
    .fetch_optional(pool)
    .await
    .context("load published chain head")?;
    Ok(IngestProgress {
        redo_in_progress,
        redo_current_block_number,
        ordinary,
        source_cursor,
        published_head,
    })
}

/// Every stored transaction, receipt, and log on a canonical block, identified
/// by chain data only: block hash and number, transaction hash and index, the
/// log's own `log_index`, emitter, topics, and data. Generated ids and
/// observation timestamps are left out so two corpora can be compared.
async fn canonical_raw_rows(pool: &sqlx::PgPool) -> Result<Value> {
    sqlx::query_scalar(
        "WITH canonical AS (
             SELECT block_hash FROM chain_lineage
             WHERE chain_id = $1
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
         )
         SELECT jsonb_build_object(
             'transactions', (
                 SELECT coalesce(jsonb_agg(jsonb_build_array(
                            block_number, block_hash, transaction_index, transaction_hash,
                            from_address, to_address, encode(input, 'hex'))
                        ORDER BY block_number, transaction_index), '[]'::jsonb)
                 FROM raw_transactions JOIN canonical USING (block_hash)
                 WHERE chain_id = $1),
             'receipts', (
                 SELECT coalesce(jsonb_agg(jsonb_build_array(
                            block_number, block_hash, transaction_index, transaction_hash,
                            status, contract_address)
                        ORDER BY block_number, transaction_index), '[]'::jsonb)
                 FROM raw_receipts JOIN canonical USING (block_hash)
                 WHERE chain_id = $1),
             'logs', (
                 SELECT coalesce(jsonb_agg(jsonb_build_array(
                            block_number, block_hash, transaction_index, transaction_hash,
                            log_index, emitting_address, to_jsonb(topics), encode(data, 'hex'))
                        ORDER BY block_number, log_index), '[]'::jsonb)
                 FROM raw_logs JOIN canonical USING (block_hash)
                 WHERE chain_id = $1))",
    )
    .bind(INGEST_CHAIN)
    .fetch_one(pool)
    .await
    .context("load canonical raw rows")
}

fn ensure_raw_rows_match_control(faulted: &Value, control: &Value, stage: &str) -> Result<()> {
    ensure!(
        control["logs"]
            .as_array()
            .is_some_and(|logs| !logs.is_empty()),
        "the clean control stored no canonical raw logs to compare against"
    );
    ensure!(
        faulted == control,
        "canonical raw rows {stage} differ from the clean control\nfaulted: {faulted}\ncontrol: {control}"
    );
    Ok(())
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
        FaultSpec::truncate_times(&fixture.receipt.tx_hash, 8, WINDOW_READ_ATTEMPTS),
    ]);

    // Leg 1: truncation that outlasts ingest's bounded window re-fetch is terminal.
    let faulted = prepare_corpus(&fixture.deployment).await?;
    // The redo helper records ordinary ingest progress at `head` before it runs
    // the command, so that seeded state is the baseline, not "below the fault".
    // Seeding here first writes the same rows the helper writes again.
    facts::seed_anvil_rpc_redo_extent(&faulted.db.pool, INGEST_CHAIN, &anvil.url, head).await?;
    let baseline = ingest_progress(&faulted.db.pool).await?;
    let first_attempt = rpc_ingest(&faulted, &proxy.url, head).await;
    for (kind, expected) in [
        (FaultKind::ErrorOnce, 1),
        (FaultKind::DelayTimeout, 1),
        (FaultKind::Truncate, WINDOW_READ_ATTEMPTS),
    ] {
        ensure!(
            proxy.hit_count(kind) == expected,
            "phase-runner ingest observed {} {kind:?} hits instead of {expected}",
            proxy.hit_count(kind)
        );
    }
    let Err(first_error) = first_attempt else {
        bail!("truncated JSON on every read of the window should terminate the first bounded redo");
    };
    ensure!(
        format!("{first_error:#}").contains("JSON-RPC response"),
        "the first redo failed for a reason other than the truncated response: {first_error:#}"
    );
    ensure!(
        raw_log_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 0
            && raw_receipt_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 0,
        "the persistently truncated window unexpectedly retained target facts"
    );
    let failed = ingest_progress(&faulted.db.pool).await?;
    let fault_block = i64::try_from(fixture.receipt.block_number)?;
    ensure!(
        failed.redo_in_progress,
        "the failed redo reported completion instead of staying open: {failed:?}"
    );
    ensure!(
        failed
            .redo_current_block_number
            .is_none_or(|block| block < fault_block),
        "redo progress advanced through the failed window at block {fault_block}: {failed:?}"
    );
    ensure!(
        failed.ordinary == baseline.ordinary && failed.source_cursor == baseline.source_cursor,
        "the failed redo moved ordinary ingest progress\nbefore: {baseline:?}\nafter: {failed:?}"
    );
    ensure!(
        failed.published_head == baseline.published_head,
        "the failed redo published a chain head\nbefore: {baseline:?}\nafter: {failed:?}"
    );

    // Leg 2: a one-off truncated response is rejected, the window is read again by
    // the same command, and the re-fetch is visible to the operator.
    proxy.add_fault(FaultSpec::truncate_once(&fixture.receipt.tx_hash, 8));
    let recovered_output = rpc_ingest(&faulted, &proxy.url, head)
        .await
        .context("a one-off truncated response should be re-fetched by the same redo")?;
    ensure!(
        proxy.hit_count(FaultKind::Truncate) == WINDOW_READ_ATTEMPTS + 1,
        "phase-runner ingest observed {} Truncate hits instead of {}",
        proxy.hit_count(FaultKind::Truncate),
        WINDOW_READ_ATTEMPTS + 1
    );
    ensure!(
        recovered_output.contains("re-fetching ingest window")
            && recovered_output.contains("JSON-RPC response"),
        "the in-place window re-fetch was not logged with its cause: {recovered_output}"
    );
    ensure!(
        raw_log_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1
            && raw_receipt_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1,
        "the re-fetched window did not retain the target log and receipt"
    );
    // What the recovered redo stored, and what the rest of the pipeline derives
    // from it, must equal a corpus that never saw a fault. Compare now, before
    // any later redo could repair a difference.
    let control = ingest_clean_control(&fixture, &anvil, head).await?;
    let control_raw_rows = canonical_raw_rows(&control.db.pool).await?;
    ensure_raw_rows_match_control(
        &canonical_raw_rows(&faulted.db.pool).await?,
        &control_raw_rows,
        "after the in-place window re-fetch",
    )?;
    finish_spine(&faulted, &anvil.url, head).await?;
    assert_eq!(
        normalized_text(&faulted.db.pool, &fixture.receipt).await?,
        normalized_text(&control.db.pool, &fixture.receipt).await?
    );
    assert_eq!(
        projected_text(&faulted.db.pool, &fixture.name).await?,
        projected_text(&control.db.pool, &fixture.name).await?
    );

    // Leg 3: a temporarily missing selected receipt is retryable: the same
    // command must refetch it and finish without an explicit repair.
    let receipt_requests = proxy.transaction_receipt_request_count(&fixture.receipt.tx_hash);
    proxy.add_fault(FaultSpec::drop_receipts_once(&fixture.receipt.tx_hash, 1));
    rpc_ingest(&faulted, &proxy.url, head).await?;
    ensure!(
        proxy.hit_count(FaultKind::DropReceipts) == 1,
        "phase-runner ingest observed {} DropReceipts hits instead of one",
        proxy.hit_count(FaultKind::DropReceipts)
    );
    ensure!(
        proxy.transaction_receipt_request_count(&fixture.receipt.tx_hash) >= receipt_requests + 2
            && raw_receipt_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1,
        "the missing receipt was not refetched and retained by the same redo"
    );
    ensure_raw_rows_match_control(
        &canonical_raw_rows(&faulted.db.pool).await?,
        &control_raw_rows,
        "after the missing receipt was refetched",
    )?;
    // Idempotence, checked separately from recovery: a clean redo over the
    // recovered range changes no stored raw row. Interpret and Project already
    // ran above, on exactly the rows the recovery left.
    rpc_ingest(&faulted, &anvil.url, head).await?;
    ensure!(
        raw_log_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1
            && raw_receipt_count(&faulted.db.pool, &fixture.receipt.tx_hash).await? == 1,
        "clean recovery did not retain the target log and receipt"
    );
    ensure_raw_rows_match_control(
        &canonical_raw_rows(&faulted.db.pool).await?,
        &control_raw_rows,
        "after the clean redo",
    )?;
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
