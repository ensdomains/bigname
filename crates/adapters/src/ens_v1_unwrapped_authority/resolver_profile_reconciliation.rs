use super::*;
use alloy_primitives::keccak256;
use anyhow::ensure;
use sqlx::{Postgres, QueryBuilder};

const DEFAULT_REPLAY_MAX_RAW_LOGS_PER_PAGE: usize = 100_000;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolverProfileEventReconciliationSummary {
    pub resolver_address_count: usize,
    pub block_hash_count: usize,
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub normalized_event_count: usize,
    pub normalized_event_inserted_count: usize,
    pub orphaned_normalized_event_count: usize,
    pub replay_page_count: usize,
    pub max_replay_page_log_count: usize,
    pub max_live_state_item_count: usize,
    pub max_live_state_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolverEmitterReplayRange {
    first_block_number: i64,
    last_block_number: i64,
    resolver_block_count: usize,
}

pub(super) struct ResolverProfileReplayContext {
    pub(super) run_id: Uuid,
    pub(super) first_block_number: i64,
    pub(super) last_block_number: i64,
    pub(super) max_raw_logs_per_page: usize,
    pub(super) page_count: usize,
    pub(super) max_page_log_count: usize,
    pub(super) max_live_state_item_count: usize,
    pub(super) max_live_state_payload_bytes: usize,
}

impl ResolverProfileReplayContext {
    pub(super) fn record_page(
        &mut self,
        page_log_count: usize,
        live_state_item_count: usize,
        live_state_payload_bytes: usize,
    ) {
        self.page_count += 1;
        self.max_page_log_count = self.max_page_log_count.max(page_log_count);
        self.max_live_state_item_count = self.max_live_state_item_count.max(live_state_item_count);
        self.max_live_state_payload_bytes = self
            .max_live_state_payload_bytes
            .max(live_state_payload_bytes);
    }
}

/// Re-derive resolver-local events after resolver profile inputs change.
///
/// The replay admits only the requested ENSv1 or Basenames resolver emitters.
/// Registry, registrar, and wrapper logs across the inclusive resolver-fact
/// range remain chronological normalization context, but their events are
/// never candidates for profile orphaning.
pub async fn reconcile_resolver_profile_events(
    pool: &PgPool,
    chain: &str,
    resolver_addresses: &[String],
) -> Result<ResolverProfileEventReconciliationSummary> {
    reconcile_resolver_profile_events_with_log_limit(
        pool,
        chain,
        resolver_addresses,
        DEFAULT_REPLAY_MAX_RAW_LOGS_PER_PAGE,
    )
    .await
}

pub(super) async fn reconcile_resolver_profile_events_with_log_limit(
    pool: &PgPool,
    chain: &str,
    resolver_addresses: &[String],
    max_raw_logs_per_page: usize,
) -> Result<ResolverProfileEventReconciliationSummary> {
    ensure!(
        max_raw_logs_per_page > 0,
        "resolver profile reconciliation max logs per page must be positive"
    );
    let resolver_addresses = normalized_resolver_addresses(resolver_addresses)?;
    if resolver_addresses.is_empty() {
        return Ok(ResolverProfileEventReconciliationSummary::default());
    }

    let mut raw_log_guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let retention_generation = raw_log_guard.version().retention_generation;
    ensure!(
        retention_generation == 0,
        "resolver-profile reconciliation cannot establish complete stateful history from raw-log retention generation {retention_generation} on chain {chain}; fully rebootstrap the database into a new generation-zero corpus before retrying"
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("resolver_profile_reconciliation:{chain}"))
        .execute(raw_log_guard.connection_mut())
        .await
        .with_context(|| format!("failed to lock resolver-profile reconciliation for {chain}"))?;
    let replay_range = load_resolver_emitter_replay_range(pool, chain, &resolver_addresses).await?;
    let mut summary = ResolverProfileEventReconciliationSummary {
        resolver_address_count: resolver_addresses.len(),
        block_hash_count: replay_range.map_or(0, |range| range.resolver_block_count),
        ..ResolverProfileEventReconciliationSummary::default()
    };
    let Some(replay_range) = replay_range else {
        raw_log_guard.release().await?;
        return Ok(summary);
    };
    let resolver_address_set_digest = resolver_address_set_digest(&resolver_addresses);
    let run_id = start_reconciliation_run(
        pool,
        chain,
        replay_range,
        &resolver_addresses,
        &resolver_address_set_digest,
    )
    .await?;
    let mut replay_context = ResolverProfileReplayContext {
        run_id,
        first_block_number: replay_range.first_block_number,
        last_block_number: replay_range.last_block_number,
        max_raw_logs_per_page,
        page_count: 0,
        max_page_log_count: 0,
        max_live_state_item_count: 0,
        max_live_state_payload_bytes: 0,
    };

    // Derive one chronological history under the current profile. Resolver
    // facts define the inclusive repair range; registry, registrar, and
    // wrapper facts inside that range remain required authority context.
    let replay = pipeline::sync_ens_v1_unwrapped_authority_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        None,
        None,
        None,
        Some(&mut replay_context),
    )
    .await?;
    summary.scanned_log_count = replay.scanned_log_count;
    summary.matched_log_count = replay.matched_log_count;
    summary.normalized_event_count = replay.total_normalized_event_count;
    summary.normalized_event_inserted_count = replay.total_normalized_event_inserted_count;
    summary.replay_page_count = replay_context.page_count;
    summary.max_replay_page_log_count = replay_context.max_page_log_count;
    summary.max_live_state_item_count = replay_context.max_live_state_item_count;
    summary.max_live_state_payload_bytes = replay_context.max_live_state_payload_bytes;

    mark_reconciliation_replay_complete(pool, chain, run_id).await?;
    let (inserted_count, orphaned_count) = publish_resolver_profile_events(
        raw_log_guard.transaction_mut(),
        chain,
        replay_range,
        run_id,
        &resolver_address_set_digest,
        resolver_addresses.len(),
    )
    .await?;
    summary.normalized_event_inserted_count = inserted_count;
    summary.orphaned_normalized_event_count = usize::try_from(orphaned_count)
        .context("orphaned resolver-profile normalized-event count does not fit usize")?;
    raw_log_guard.release().await?;
    Ok(summary)
}

async fn start_reconciliation_run(
    pool: &PgPool,
    chain: &str,
    replay_range: ResolverEmitterReplayRange,
    resolver_addresses: &[String],
    resolver_address_set_digest: &str,
) -> Result<Uuid> {
    let run_id = Uuid::new_v4();
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start resolver-profile reconciliation run transaction")?;
    sqlx::query("DELETE FROM resolver_profile_reconciliation_runs WHERE chain_id = $1")
        .bind(chain)
        .execute(transaction.as_mut())
        .await
        .with_context(|| {
            format!("failed to clean an incomplete resolver-profile run for chain {chain}")
        })?;
    sqlx::query(
        r#"
        INSERT INTO resolver_profile_reconciliation_runs (
            run_id,
            chain_id,
            first_block_number,
            last_block_number,
            resolver_address_count,
            resolver_address_set_digest
        ) VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(run_id)
    .bind(chain)
    .bind(replay_range.first_block_number)
    .bind(replay_range.last_block_number)
    .bind(i64::try_from(resolver_addresses.len()).context("resolver address count overflowed i64")?)
    .bind(resolver_address_set_digest)
    .execute(transaction.as_mut())
    .await
    .with_context(|| format!("failed to create resolver-profile run for chain {chain}"))?;
    for addresses in resolver_addresses.chunks(1_000) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO resolver_profile_reconciliation_targets (run_id, resolver_address) ",
        );
        builder.push_values(addresses, |mut row, address| {
            row.push_bind(run_id).push_bind(address);
        });
        builder
            .build()
            .execute(transaction.as_mut())
            .await
            .with_context(|| {
                format!("failed to stage resolver-profile targets for chain {chain}")
            })?;
    }
    transaction
        .commit()
        .await
        .context("failed to commit resolver-profile reconciliation run")?;
    Ok(run_id)
}

async fn mark_reconciliation_replay_complete(
    pool: &PgPool,
    chain: &str,
    run_id: Uuid,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        UPDATE resolver_profile_reconciliation_runs
        SET status = 'replay_complete', updated_at = now()
        WHERE run_id = $1
          AND chain_id = $2
          AND status = 'running'
        "#,
    )
    .bind(run_id)
    .bind(chain)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to mark resolver-profile replay complete for chain {chain}")
    })?;
    ensure!(
        result.rows_affected() == 1,
        "resolver-profile replay run disappeared before completion for chain {chain}"
    );
    Ok(())
}

fn normalized_resolver_addresses(resolver_addresses: &[String]) -> Result<Vec<String>> {
    let mut normalized = BTreeSet::new();
    for address in resolver_addresses {
        let address = address.trim().to_ascii_lowercase();
        if address.is_empty() {
            bail!("resolver profile reconciliation address must not be empty");
        }
        normalized.insert(address);
    }
    Ok(normalized.into_iter().collect())
}

fn resolver_address_set_digest(resolver_addresses: &[String]) -> String {
    format!("{:#x}", keccak256(resolver_addresses.join("\n").as_bytes()))
}

async fn load_resolver_emitter_replay_range(
    pool: &PgPool,
    chain: &str,
    resolver_addresses: &[String],
) -> Result<Option<ResolverEmitterReplayRange>> {
    let (first_block_number, last_block_number, resolver_block_count, invalid_lineage_count) =
        sqlx::query_as::<_, (Option<i64>, Option<i64>, i64, i64)>(
            r#"
        SELECT
            MIN(raw_log.block_number),
            MAX(raw_log.block_number),
            COUNT(DISTINCT raw_log.block_hash)::BIGINT,
            COUNT(*) FILTER (
                WHERE lineage.block_hash IS NULL
                   OR lineage.block_number <> raw_log.block_number
                   OR lineage.canonicality_state NOT IN (
                       'canonical'::canonicality_state,
                       'safe'::canonicality_state,
                       'finalized'::canonicality_state
                   )
            )::BIGINT AS invalid_lineage_count
        FROM raw_logs raw_log
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = raw_log.chain_id
         AND lineage.block_hash = raw_log.block_hash
        WHERE raw_log.chain_id = $1
          AND lower(raw_log.emitting_address) = ANY($2::TEXT[])
          AND raw_log.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
        )
        .bind(chain)
        .bind(resolver_addresses)
        .fetch_one(pool)
        .await
        .with_context(|| {
            format!("failed to load retained resolver-emitter replay range for chain {chain}")
        })?;

    if invalid_lineage_count > 0 {
        bail!(
            "{invalid_lineage_count} canonical resolver-emitter raw logs lack matching canonical lineage on chain {chain}"
        );
    }
    let Some(first_block_number) = first_block_number else {
        ensure!(
            last_block_number.is_none() && resolver_block_count == 0,
            "empty resolver-emitter replay range has inconsistent aggregate values on chain {chain}"
        );
        return Ok(None);
    };
    let last_block_number = last_block_number
        .context("non-empty resolver-emitter replay range must have a last block")?;
    Ok(Some(ResolverEmitterReplayRange {
        first_block_number,
        last_block_number,
        resolver_block_count: usize::try_from(resolver_block_count)
            .context("resolver-emitter block count does not fit usize")?,
    }))
}

async fn publish_resolver_profile_events(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chain: &str,
    replay_range: ResolverEmitterReplayRange,
    run_id: Uuid,
    resolver_address_set_digest: &str,
    resolver_address_count: usize,
) -> Result<(usize, u64)> {
    let run = sqlx::query_as::<_, (i64, i64, i64, String, String)>(
        r#"
        SELECT
            first_block_number,
            last_block_number,
            resolver_address_count,
            resolver_address_set_digest,
            status
        FROM resolver_profile_reconciliation_runs
        WHERE run_id = $1 AND chain_id = $2
        FOR UPDATE
        "#,
    )
    .bind(run_id)
    .bind(chain)
    .fetch_optional(transaction.as_mut())
    .await
    .with_context(|| format!("failed to lock resolver-profile publication for {chain}"))?
    .context("resolver-profile run disappeared before publication")?;
    ensure!(
        run.0 == replay_range.first_block_number
            && run.1 == replay_range.last_block_number
            && run.2
                == i64::try_from(resolver_address_count)
                    .context("resolver address count overflowed i64")?
            && run.3 == resolver_address_set_digest
            && run.4 == "replay_complete",
        "resolver-profile run metadata changed before publication"
    );

    let mut inserted_count = 0usize;
    let mut after_identity = None::<String>;
    loop {
        let rows = sqlx::query_as::<_, (String, Value)>(
            r#"
            SELECT item_key, item_payload
            FROM resolver_profile_reconciliation_state_items
            WHERE run_id = $1
              AND item_kind = 'normalized_event'
              AND ($2::TEXT IS NULL OR item_key > $2)
            ORDER BY item_key
            LIMIT 1_000
            "#,
        )
        .bind(run_id)
        .bind(after_identity.as_deref())
        .fetch_all(transaction.as_mut())
        .await
        .context("failed to load staged resolver-profile event page")?;
        if rows.is_empty() {
            break;
        }
        after_identity = rows.last().map(|(identity, _)| identity.clone());
        let mut events = rows
            .into_iter()
            .map(|(_, payload)| decode_item(payload, "normalized_event"))
            .collect::<Result<Vec<NormalizedEvent>>>()?;
        event_persistence::pin_existing_event_manifest_provenance(transaction, &mut events).await?;
        inserted_count += bigname_storage::upsert_normalized_events_count_only_in_transaction(
            transaction,
            &events,
        )
        .await?;
    }

    let orphaned = sqlx::query(
        r#"
        WITH stale AS (
            SELECT DISTINCT event.normalized_event_id
            FROM normalized_events event
            JOIN raw_logs raw_log
              ON raw_log.chain_id = event.chain_id
             AND raw_log.block_hash = event.block_hash
             AND raw_log.transaction_hash = event.transaction_hash
             AND raw_log.log_index = event.log_index
            JOIN chain_lineage lineage
              ON lineage.chain_id = raw_log.chain_id
             AND lineage.block_hash = raw_log.block_hash
            JOIN resolver_profile_reconciliation_targets target
              ON target.run_id = $4
             AND target.resolver_address = LOWER(raw_log.emitting_address)
            WHERE event.chain_id = $1
              AND event.derivation_kind = 'ens_v1_unwrapped_authority'
              AND event.source_family IN ('ens_v1_resolver_l1', 'basenames_base_resolver')
              AND event.raw_fact_ref->>'kind' = 'raw_log'
              AND raw_log.block_number BETWEEN $2::BIGINT AND $3::BIGINT
              AND NOT EXISTS (
                  SELECT 1
                  FROM resolver_profile_reconciliation_state_items expected
                  WHERE expected.run_id = $4
                    AND expected.item_kind = 'normalized_event'
                    AND expected.item_key = event.event_identity
              )
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND raw_log.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        UPDATE normalized_events event
        SET canonicality_state = 'orphaned'::canonicality_state, observed_at = now()
        FROM stale
        WHERE event.normalized_event_id = stale.normalized_event_id
        "#,
    )
    .bind(chain)
    .bind(replay_range.first_block_number)
    .bind(replay_range.last_block_number)
    .bind(run_id)
    .execute(transaction.as_mut())
    .await
    .with_context(|| format!("failed to orphan stale resolver-profile events for {chain}"))?
    .rows_affected();
    sqlx::query("DELETE FROM resolver_profile_reconciliation_runs WHERE run_id = $1")
        .bind(run_id)
        .execute(transaction.as_mut())
        .await
        .with_context(|| format!("failed to clean resolver-profile run for {chain}"))?;
    Ok((inserted_count, orphaned))
}
