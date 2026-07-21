use std::time::Duration;

use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result, bail, ensure};

use super::support;
use crate::harness::{
    anvil::Anvil,
    db::HarnessDb,
    ens_v1::{self, EnsV1Deployment},
    fault_proxy::{FaultKind, FaultProxy, FaultSpec},
    manifests::{self, LocalProfile},
    perturb,
    pipeline::{self, ChainBackfillTarget, IndexerRunSession},
    repo_root,
    rpc::TxReceipt,
};

const CHAIN: &str = "ethereum-mainnet";
const TEXT_KEY: &str = "com.twitter";
const YEAR: u64 = 365 * 24 * 60 * 60;

// #154 containment seam: switch this one line to `Strict` when a silently
// short log response is rejected before durable fetch coverage or the chain
// checkpoint can advance.
const SHORT_LOG_CONTRACT: ShortLogContract = ShortLogContract::KnownShortResponseOverclaim;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShortLogContract {
    #[allow(dead_code)]
    Strict,
    KnownShortResponseOverclaim,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShortLogObservation {
    RetryStarted,
    CheckpointAdvanced,
}

struct Corpus {
    db: HarnessDb,
    scratch: support::TempDir,
    profile: LocalProfile,
}

struct TextFixture {
    deployment: EnsV1Deployment,
    name: String,
    owner: Address,
    record_target: Address,
    text_receipt: TxReceipt,
}

impl TextFixture {
    fn logical_name_id(&self) -> String {
        format!("ens:{}", self.name)
    }

    fn ready_sql(&self) -> String {
        support::canonical_event_ready_sql(
            &self.logical_name_id(),
            "RecordChanged",
            Some(&format!("text:{TEXT_KEY}")),
        )
    }

    fn subjects(&self) -> perturb::RouteSnapshotSubjects {
        perturb::RouteSnapshotSubjects::new(
            [self.name.as_str()],
            [
                format!("{:#x}", self.owner),
                format!("{:#x}", self.record_target),
            ],
        )
    }
}

async fn deploy_text_fixture(anvil: &Anvil, label: &str, value: &str) -> Result<TextFixture> {
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let owner = accounts[1];
    let record_target = accounts[2];
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
    ens_v1::set_addr_record(
        &rpc,
        deployment.public_resolver.address,
        owner,
        &name,
        record_target,
    )
    .await?;
    let text_receipt = ens_v1::set_text_record_with_receipt(
        &rpc,
        deployment.public_resolver.address,
        owner,
        &name,
        TEXT_KEY,
        value,
    )
    .await?;
    Ok(TextFixture {
        deployment,
        name,
        owner,
        record_target,
        text_receipt,
    })
}

async fn prepare_corpus(deployment: &EnsV1Deployment) -> Result<Corpus> {
    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    let db = HarnessDb::create().await?;
    Ok(Corpus {
        db,
        scratch,
        profile,
    })
}

async fn canonical_checkpoint(pool: &sqlx::PgPool) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar::<_, Option<i64>>(
        "SELECT canonical_block_number FROM chain_checkpoints WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_optional(pool)
    .await
    .context("load e2e canonical checkpoint")?
    .flatten())
}

async fn raw_log_count(pool: &sqlx::PgPool, transaction_hash: &str) -> Result<i64> {
    sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs WHERE chain_id = $1 AND transaction_hash = $2",
    )
    .bind(CHAIN)
    .bind(transaction_hash.to_ascii_lowercase())
    .fetch_one(pool)
    .await
    .context("count transaction raw logs")
}

async fn resolver_coverage_covers(
    pool: &sqlx::PgPool,
    resolver: Address,
    block_number: u64,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS ( \
           SELECT 1 FROM backfill_coverage_facts fact \
           JOIN backfill_jobs job USING (backfill_job_id) \
           WHERE job.status = 'completed' \
             AND fact.chain_id = $1 \
             AND fact.source_family = 'ens_v1_resolver_l1' \
             AND fact.covered_from_block <= $2 \
             AND fact.covered_to_block >= $2 \
             AND (fact.scope = 'family' OR lower(fact.address) = lower($3)) \
         )",
    )
    .bind(CHAIN)
    .bind(block_number as i64)
    .bind(format!("{resolver:#x}"))
    .fetch_one(pool)
    .await
    .context("check resolver fetch coverage")
}

async fn coverage_rows_at_block(
    pool: &sqlx::PgPool,
    block_number: u64,
) -> Result<Vec<(String, String, Option<String>, i64, i64, String)>> {
    sqlx::query_as(
        "SELECT fact.source_family, fact.scope, fact.address, \
                fact.covered_from_block, fact.covered_to_block, job.status::TEXT \
         FROM backfill_coverage_facts fact \
         JOIN backfill_jobs job USING (backfill_job_id) \
         WHERE fact.chain_id = $1 \
           AND fact.covered_from_block <= $2 \
           AND fact.covered_to_block >= $2 \
         ORDER BY fact.source_family, fact.scope, fact.address",
    )
    .bind(CHAIN)
    .bind(block_number as i64)
    .fetch_all(pool)
    .await
    .context("load fetch coverage rows at target block")
}

async fn await_short_log_observation(
    session: &mut IndexerRunSession,
    pool: &sqlx::PgPool,
    proxy: &FaultProxy,
    target_block: u64,
    delay_hits_before: usize,
) -> Result<ShortLogObservation> {
    let ready_timeout_secs = pipeline::ready_timeout_secs()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(ready_timeout_secs);
    loop {
        session.assert_running()?;
        if canonical_checkpoint(pool)
            .await?
            .is_some_and(|checkpoint| checkpoint >= target_block as i64)
        {
            return Ok(ShortLogObservation::CheckpointAdvanced);
        }
        if proxy.hit_count(FaultKind::DelayTimeout) > delay_hits_before {
            return Ok(ShortLogObservation::RetryStarted);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "short-log run neither retried the affected fetch nor advanced its checkpoint within the configured {ready_timeout_secs}s readiness deadline"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_short_log_pre_refetch_contract(
    pool: &sqlx::PgPool,
    fixture: &TextFixture,
    observation: ShortLogObservation,
) -> Result<()> {
    let block = fixture.text_receipt.block_number;
    let checkpoint = canonical_checkpoint(pool).await?;
    let raw_logs = raw_log_count(pool, &fixture.text_receipt.tx_hash).await?;
    let covered =
        resolver_coverage_covers(pool, fixture.deployment.public_resolver.address, block).await?;

    match SHORT_LOG_CONTRACT {
        ShortLogContract::Strict => {
            ensure!(
                observation == ShortLogObservation::RetryStarted,
                "strict short-log safety requires a refetch before checkpoint advancement"
            );
            ensure!(
                raw_logs == 0,
                "the delayed refetch must not have admitted the omitted log yet"
            );
            ensure!(
                !covered,
                "the omitted resolver span must not have durable fetch coverage before refetch"
            );
            ensure!(
                checkpoint.is_none_or(|checkpoint| checkpoint < block as i64),
                "the canonical checkpoint advanced to {checkpoint:?} before block {block} was refetched"
            );
        }
        ShortLogContract::KnownShortResponseOverclaim => {
            ensure!(
                observation == ShortLogObservation::RetryStarted,
                "#154 containment expected startup to refetch the overclaimed span before checkpoint advancement"
            );
            ensure!(
                raw_logs == 0,
                "the supposedly omitted transaction was nevertheless retained"
            );
            if !covered {
                bail!(
                    "#154 repro did not mint the expected overclaimed resolver coverage fact; spanning facts: {:#?}",
                    coverage_rows_at_block(pool, block).await?
                );
            }
            ensure!(
                checkpoint.is_none_or(|checkpoint| checkpoint < block as i64),
                "startup advanced the canonical checkpoint to {checkpoint:?} before refetching block {block}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn silently_short_logs_are_contained_until_refetch_then_match_control() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let fixture = deploy_text_fixture(&anvil, "fault-short", "short").await?;
    anvil.client().mine(2).await?;
    let head = anvil.client().block_number().await?;

    let proxy = FaultProxy::spawn(&anvil.url).await?;
    proxy.add_fault(FaultSpec::drop_logs_once(&fixture.text_receipt.tx_hash, 1));
    let Corpus {
        db,
        scratch,
        profile,
    } = prepare_corpus(&fixture.deployment).await?;
    let short_backfill = pipeline::indexer_backfill(
        &repo_root(),
        &db.url,
        &profile.root,
        &proxy.url,
        0,
        head,
        "provider-fault-short-log",
    )
    .await;
    let short_backfill_drop_hits = proxy.hit_count(FaultKind::DropLogs);
    ensure!(
        short_backfill_drop_hits == 1,
        "completed short-log backfill observed {short_backfill_drop_hits} drop-log hits instead of exactly one"
    );
    let ready_sql = fixture.ready_sql();
    match SHORT_LOG_CONTRACT {
        ShortLogContract::Strict => {
            ensure!(
                short_backfill.is_err(),
                "strict short-log safety requires the incomplete backfill to fail"
            );
            ensure!(
                !resolver_coverage_covers(
                    &db.pool,
                    fixture.deployment.public_resolver.address,
                    fixture.text_receipt.block_number,
                )
                .await?,
                "a rejected short backfill must not mint resolver fetch coverage"
            );
            ensure!(
                canonical_checkpoint(&db.pool).await?.is_none(),
                "a rejected bounded backfill must not write a chain checkpoint"
            );
            pipeline::indexer_backfill(
                &repo_root(),
                &db.url,
                &profile.root,
                &anvil.url,
                0,
                head,
                "provider-fault-short-log-strict-refetch",
            )
            .await?;
        }
        ShortLogContract::KnownShortResponseOverclaim => {
            short_backfill.context(
                "#154 containment expected the silently short bounded backfill to complete",
            )?;
            ensure!(
                raw_log_count(&db.pool, &fixture.text_receipt.tx_hash).await? == 0,
                "the short backfill unexpectedly retained the omitted transaction log"
            );
            ensure!(
                resolver_coverage_covers(
                    &db.pool,
                    fixture.deployment.public_resolver.address,
                    fixture.text_receipt.block_number,
                )
                .await?,
                "#154 repro did not mint the expected overclaimed resolver coverage fact"
            );

            proxy.add_fault(FaultSpec::delay_timeout_until_cleared(
                &fixture.text_receipt.tx_hash,
                Duration::from_secs(5),
            ));
            let delay_hits_before = proxy.hit_count(FaultKind::DelayTimeout);
            let mut session = IndexerRunSession::start(
                &repo_root(),
                &db.url,
                &profile.root,
                &proxy.url,
                "provider-fault-short-logs",
            )
            .await?;
            let observation = await_short_log_observation(
                &mut session,
                &db.pool,
                &proxy,
                fixture.text_receipt.block_number,
                delay_hits_before,
            )
            .await?;
            assert_short_log_pre_refetch_contract(&db.pool, &fixture, observation).await?;
            session.stop().await?;
            proxy.clear_faults(FaultKind::DelayTimeout);

            pipeline::indexer_backfill(
                &repo_root(),
                &db.url,
                &profile.root,
                &anvil.url,
                fixture.text_receipt.block_number,
                fixture.text_receipt.block_number,
                "provider-fault-short-log-repair",
            )
            .await?;
        }
    }
    pipeline::indexer_run_until_checkpoint(
        &repo_root(),
        &db.url,
        &db.pool,
        &profile.root,
        &anvil.url,
        head,
        Some(&ready_sql),
    )
    .await?;

    let live_short_receipt = ens_v1::set_text_record_with_receipt(
        &anvil.client(),
        fixture.deployment.public_resolver.address,
        fixture.owner,
        &fixture.name,
        TEXT_KEY,
        "live-refetched",
    )
    .await?;
    anvil.client().mine(2).await?;
    let live_head = anvil.client().block_number().await?;
    let drop_hits_before = proxy.hit_count(FaultKind::DropLogs);
    let delay_hits_before = proxy.hit_count(FaultKind::DelayTimeout);
    proxy.add_faults([
        FaultSpec::drop_logs_once(&live_short_receipt.tx_hash, 1),
        FaultSpec::delay_timeout_until_cleared(&live_short_receipt.tx_hash, Duration::from_secs(5)),
    ]);
    let mut live_short_session = IndexerRunSession::start(
        &repo_root(),
        &db.url,
        &profile.root,
        &proxy.url,
        "provider-fault-short-live-poll",
    )
    .await?;
    proxy
        .wait_for_hits(
            FaultKind::DropLogs,
            drop_hits_before + 1,
            Duration::from_secs(pipeline::ready_timeout_secs()?),
        )
        .await?;
    let live_observation = await_short_log_observation(
        &mut live_short_session,
        &db.pool,
        &proxy,
        live_short_receipt.block_number,
        delay_hits_before,
    )
    .await?;
    match SHORT_LOG_CONTRACT {
        ShortLogContract::Strict => {
            ensure!(
                live_observation == ShortLogObservation::RetryStarted,
                "strict short-log safety requires live poll to refetch before checkpoint advancement"
            );
            ensure!(
                canonical_checkpoint(&db.pool)
                    .await?
                    .is_none_or(|checkpoint| checkpoint < live_short_receipt.block_number as i64),
                "live canonical checkpoint advanced before the omitted block was refetched"
            );
        }
        ShortLogContract::KnownShortResponseOverclaim => {
            ensure!(
                live_observation == ShortLogObservation::CheckpointAdvanced,
                "live poll safely retried the silently short response before checkpoint advancement; switch the short-response contract to strict"
            );
            ensure!(
                canonical_checkpoint(&db.pool)
                    .await?
                    .is_some_and(|checkpoint| checkpoint >= live_short_receipt.block_number as i64),
                "live canonical checkpoint did not reproduce advancement across the omitted block"
            );
        }
    }
    ensure!(
        raw_log_count(&db.pool, &live_short_receipt.tx_hash).await? == 0,
        "the held live refetch nevertheless admitted the omitted log"
    );
    live_short_session.stop().await?;
    proxy.clear_faults(FaultKind::DelayTimeout);
    pipeline::indexer_backfill(
        &repo_root(),
        &db.url,
        &profile.root,
        &anvil.url,
        live_short_receipt.block_number,
        live_short_receipt.block_number,
        "provider-fault-short-live-repair",
    )
    .await?;
    let live_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE chain_id = '{CHAIN}' \
           AND transaction_hash = '{}' \
           AND event_kind = 'RecordChanged' \
           AND canonicality_state = 'canonical')",
        live_short_receipt.tx_hash.to_ascii_lowercase()
    );
    pipeline::indexer_run_until_checkpoint(
        &repo_root(),
        &db.url,
        &db.pool,
        &profile.root,
        &anvil.url,
        live_head,
        Some(&live_ready_sql),
    )
    .await?;
    ensure!(
        raw_log_count(&db.pool, &fixture.text_receipt.tx_hash).await? > 0
            && raw_log_count(&db.pool, &live_short_receipt.tx_hash).await? > 0,
        "the correct refetch did not retain both formerly omitted logs"
    );
    pipeline::worker_replay_all_current_projections(&repo_root(), &db.url).await?;
    let faulted = support::serve_existing_db(db, scratch).await?;
    let faulted_snapshots = support::route_snapshots(&faulted, &fixture.subjects()).await?;

    let control =
        support::ingest_at_current_head(&anvil, &fixture.deployment, Some(&live_ready_sql)).await?;
    let control_snapshots = support::route_snapshots(&control, &fixture.subjects()).await?;
    perturb::assert_snapshots_equal(&control_snapshots, &faulted_snapshots)?;
    proxy.assert_healthy()?;

    faulted.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn transient_provider_faults_and_partial_receipts_recover_to_control() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let owner = accounts[1];
    let record_target = accounts[2];
    let name = "fault-retry.eth";
    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "fault-retry",
        owner,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::set_addr_record(
        &rpc,
        deployment.public_resolver.address,
        owner,
        name,
        record_target,
    )
    .await?;
    rpc.mine(2).await?;

    let proxy = FaultProxy::spawn(&anvil.url).await?;
    let Corpus {
        db,
        scratch,
        profile,
    } = prepare_corpus(&deployment).await?;
    let baseline_head = rpc.block_number().await?;
    let mut baseline = IndexerRunSession::start(
        &repo_root(),
        &db.url,
        &profile.root,
        &proxy.url,
        "provider-fault-retry-baseline",
    )
    .await?;
    baseline
        .wait_for_checkpoint(&db.pool, baseline_head, None)
        .await?;
    baseline.stop().await?;

    let text_receipt = ens_v1::set_text_record_with_receipt(
        &rpc,
        deployment.public_resolver.address,
        owner,
        name,
        TEXT_KEY,
        "recovered",
    )
    .await?;
    rpc.mine(2).await?;
    let target_head = rpc.block_number().await?;
    proxy.add_faults([
        FaultSpec::error_once(&text_receipt.tx_hash, -32005, "injected capacity limit"),
        // The production JSON-RPC request deadline is 45 seconds. Holding the
        // connection longer makes this a transport timeout, not an HTTP 504.
        FaultSpec::delay_timeout_once(&text_receipt.tx_hash, Duration::from_secs(90)),
        FaultSpec::truncate_once(&text_receipt.tx_hash, 8),
        FaultSpec::drop_receipts_once(&text_receipt.tx_hash, 1),
    ]);

    let ready_sql = support::canonical_event_ready_sql(
        &format!("ens:{name}"),
        "RecordChanged",
        Some(&format!("text:{TEXT_KEY}")),
    );
    let mut faulted_session = IndexerRunSession::start(
        &repo_root(),
        &db.url,
        &profile.root,
        &proxy.url,
        "provider-fault-retry-live",
    )
    .await?;
    faulted_session
        .wait_for_checkpoint(&db.pool, target_head, Some(&ready_sql))
        .await?;
    faulted_session.stop().await?;

    for kind in [
        FaultKind::ErrorOnce,
        FaultKind::DelayTimeout,
        FaultKind::Truncate,
        FaultKind::DropReceipts,
    ] {
        ensure!(
            proxy.hit_count(kind) == 1,
            "live pipeline observed {} {kind:?} hits instead of exactly one",
            proxy.hit_count(kind)
        );
    }
    ensure!(
        raw_log_count(&db.pool, &text_receipt.tx_hash).await? > 0,
        "recovered live poll did not retain the target log"
    );
    pipeline::worker_replay_all_current_projections(&repo_root(), &db.url).await?;
    let faulted = support::serve_existing_db(db, scratch).await?;
    let subjects = perturb::RouteSnapshotSubjects::new(
        [name],
        [format!("{owner:#x}"), format!("{record_target:#x}")],
    );
    let faulted_snapshots = support::route_snapshots(&faulted, &subjects).await?;

    let control = support::ingest_at_current_head(&anvil, &deployment, Some(&ready_sql)).await?;
    let control_snapshots = support::route_snapshots(&control, &subjects).await?;
    perturb::assert_snapshots_equal(&control_snapshots, &faulted_snapshots)?;
    proxy.assert_healthy()?;

    faulted.db.cleanup().await?;
    control.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn transient_get_code_retries_primary_without_using_configured_fallback() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let fixture = deploy_text_fixture(&anvil, "fault-code-retry", "healthy").await?;
    anvil.client().mine(2).await?;
    let head = anvil.client().block_number().await?;
    let block_hash = anvil
        .client()
        .block_hash(fixture.text_receipt.block_number)
        .await?;
    let expected_code = anvil
        .client()
        .get_code_at_block_hash(fixture.deployment.public_resolver.address, &block_hash)
        .await?;
    let expected_code_hash = format!("{:#x}", keccak256(&expected_code));
    let resolver = format!("{:#x}", fixture.deployment.public_resolver.address);

    let primary = FaultProxy::spawn(&anvil.url).await?;
    primary.add_fault(FaultSpec::get_code_error_once(
        &resolver,
        &block_hash,
        -32005,
        "injected healthy-block capacity limit",
    ));
    let fallback = FaultProxy::spawn(&anvil.url).await?;
    let Corpus {
        db,
        scratch: _scratch,
        profile,
    } = prepare_corpus(&fixture.deployment).await?;
    let primary_urls = [(CHAIN, primary.url.as_str())];
    let fallback_urls = [(CHAIN, fallback.url.as_str())];
    pipeline::indexer_backfill_with_chain_rpc_urls_and_code_fallbacks(
        &repo_root(),
        &db.url,
        &profile.root,
        ChainBackfillTarget {
            chain_rpc_urls: &primary_urls,
            chain: CHAIN,
            block_range: 0..=head,
            idempotency_key: "provider-fault-transient-code",
        },
        &fallback_urls,
    )
    .await?;

    ensure!(
        primary.hit_count(FaultKind::ErrorOnce) == 1,
        "primary did not inject exactly one targeted transient eth_getCode error"
    );
    let targeted_primary_calls = primary.get_code_request_count(&resolver, &block_hash);
    ensure!(
        targeted_primary_calls >= 2,
        "primary received {targeted_primary_calls} targeted eth_getCode calls; the transient error was not retried"
    );
    ensure!(
        fallback.total_request_count() == 0,
        "configured fallback received {} requests for a transient primary error",
        fallback.total_request_count()
    );
    ensure!(
        resolver_coverage_covers(
            &db.pool,
            fixture.deployment.public_resolver.address,
            fixture.text_receipt.block_number,
        )
        .await?,
        "primary retry recovery did not record resolver fetch coverage"
    );
    let code_observation: Option<(String, i64)> = sqlx::query_as(
        "SELECT code_hash, code_byte_length FROM raw_code_hashes \
         WHERE chain_id = $1 AND block_hash = $2 AND lower(contract_address) = lower($3)",
    )
    .bind(CHAIN)
    .bind(&block_hash)
    .bind(&resolver)
    .fetch_optional(&db.pool)
    .await?;
    let (stored_code_hash, stored_code_byte_length) = code_observation
        .context("primary retry did not persist a code observation at the healthy block")?;
    ensure!(
        stored_code_hash.eq_ignore_ascii_case(&expected_code_hash)
            && stored_code_byte_length == expected_code.len() as i64,
        "retried primary code observation differed from direct Anvil state: expected hash {expected_code_hash} and length {}, stored hash {stored_code_hash} and length {stored_code_byte_length}",
        expected_code.len()
    );
    ensure!(
        raw_log_count(&db.pool, &fixture.text_receipt.tx_hash).await? > 0,
        "primary retry recovery did not complete target raw-log materialization"
    );
    primary.assert_healthy()?;
    fallback.assert_healthy()?;

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pruned_get_code_fails_closed_then_uses_configured_fallback() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let fixture = deploy_text_fixture(&anvil, "fault-pruned", "archive").await?;
    anvil.client().mine(2).await?;
    let head = anvil.client().block_number().await?;
    let block_hash = anvil
        .client()
        .block_hash(fixture.text_receipt.block_number)
        .await?;
    let expected_code = anvil
        .client()
        .get_code_at_block_hash(fixture.deployment.public_resolver.address, &block_hash)
        .await?;
    let expected_code_hash = format!("{:#x}", keccak256(&expected_code));
    let resolver = format!("{:#x}", fixture.deployment.public_resolver.address);

    let primary = FaultProxy::spawn(&anvil.url).await?;
    primary.add_fault(FaultSpec::pruned_get_code(
        &resolver,
        &block_hash,
        fixture.text_receipt.block_number,
    ));
    let fallback = FaultProxy::spawn(&anvil.url).await?;
    let Corpus {
        db,
        scratch: _scratch,
        profile,
    } = prepare_corpus(&fixture.deployment).await?;
    let primary_urls = [(CHAIN, primary.url.as_str())];
    let target = || ChainBackfillTarget {
        chain_rpc_urls: &primary_urls,
        chain: CHAIN,
        block_range: 0..=head,
        idempotency_key: "provider-fault-pruned-code",
    };

    let error = pipeline::indexer_backfill_with_chain_rpc_urls(
        &repo_root(),
        &db.url,
        &profile.root,
        target(),
    )
    .await
    .expect_err("pruned historical code without a fallback must fail closed");
    ensure!(
        format!("{error:#}").contains(&format!(
            "state at block #{} is pruned",
            fixture.text_receipt.block_number + 1
        )),
        "unexpected no-fallback failure: {error:#}"
    );
    ensure!(
        primary.hit_count(FaultKind::PrunedState) > 0,
        "primary proxy never injected the targeted pruned-state response"
    );
    let coverage_before_fallback: i64 =
        sqlx::query_scalar("SELECT count(*) FROM backfill_coverage_facts WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&db.pool)
            .await?;
    ensure!(
        coverage_before_fallback == 0,
        "failed pruned-state backfill minted {coverage_before_fallback} coverage facts"
    );
    ensure!(
        canonical_checkpoint(&db.pool).await?.is_none(),
        "bounded backfill unexpectedly wrote a chain checkpoint"
    );

    let primary_hits_before_fallback = primary.hit_count(FaultKind::PrunedState);
    let fallback_urls = [(CHAIN, fallback.url.as_str())];
    pipeline::indexer_backfill_with_chain_rpc_urls_and_code_fallbacks(
        &repo_root(),
        &db.url,
        &profile.root,
        target(),
        &fallback_urls,
    )
    .await?;
    ensure!(
        primary.hit_count(FaultKind::PrunedState) > primary_hits_before_fallback,
        "configured fallback run did not try the primary provider first"
    );
    let targeted_fallback_calls = fallback.get_code_request_count(&resolver, &block_hash);
    ensure!(
        targeted_fallback_calls > 0,
        "configured fallback never received the targeted historical eth_getCode"
    );
    ensure!(
        fallback.total_request_count() == targeted_fallback_calls,
        "historical-code fallback received {} total requests but only {targeted_fallback_calls} targeted the pruned resolver block",
        fallback.total_request_count()
    );
    ensure!(
        resolver_coverage_covers(
            &db.pool,
            fixture.deployment.public_resolver.address,
            fixture.text_receipt.block_number,
        )
        .await?,
        "successful fallback backfill did not record resolver fetch coverage"
    );
    let code_observation: Option<(String, i64)> = sqlx::query_as(
        "SELECT code_hash, code_byte_length FROM raw_code_hashes \
         WHERE chain_id = $1 AND block_hash = $2 AND lower(contract_address) = lower($3)",
    )
    .bind(CHAIN)
    .bind(&block_hash)
    .bind(&resolver)
    .fetch_optional(&db.pool)
    .await?;
    let (stored_code_hash, stored_code_byte_length) = code_observation
        .context("fallback did not persist a code observation at the pruned block")?;
    ensure!(
        stored_code_hash.eq_ignore_ascii_case(&expected_code_hash)
            && stored_code_byte_length == expected_code.len() as i64,
        "fallback code observation differed from direct Anvil state: expected hash {expected_code_hash} and length {}, stored hash {stored_code_hash} and length {stored_code_byte_length}",
        expected_code.len()
    );
    ensure!(
        raw_log_count(&db.pool, &fixture.text_receipt.tx_hash).await? > 0,
        "fallback recovery did not complete the target raw-log materialization"
    );
    primary.assert_healthy()?;
    fallback.assert_healthy()?;

    db.cleanup().await?;
    Ok(())
}
