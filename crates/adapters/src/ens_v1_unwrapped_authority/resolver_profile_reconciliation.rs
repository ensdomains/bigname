use super::*;
use crate::checkpoint_context::StartupAdapterProgress;
use anyhow::ensure;
use sqlx::Postgres;

mod targets;

pub use targets::{
    ResolverProfileEventReconciliation, ResolverProfileEventReconciliationPublication,
};

const DEFAULT_REPLAY_MAX_RAW_LOGS_PER_PAGE: usize = 1_000;

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
pub(super) struct ResolverEmitterReplayRange {
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

/// Re-derive resolver-local events after
/// [resolver-profile](../../../docs/glossary.md) inputs change.
///
/// The replay admits only the requested ENSv1 or Basenames resolver emitters.
/// Registry, registrar, and wrapper logs across the inclusive resolver-fact
/// range remain chronological normalization context, but their events are
/// never candidates for resolver-profile orphaning.
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
    if resolver_addresses.is_empty() {
        return Ok(ResolverProfileEventReconciliationSummary::default());
    }
    let mut reconciliation = begin_resolver_profile_event_reconciliation(pool, chain).await?;
    reconciliation.stage_addresses(resolver_addresses).await?;
    reconciliation
        .reconcile_with_log_limit(max_raw_logs_per_page, None)
        .await?
        .finish()
        .await
}

pub async fn begin_resolver_profile_event_reconciliation(
    pool: &PgPool,
    chain: &str,
) -> Result<ResolverProfileEventReconciliation> {
    targets::begin_reconciliation(pool, chain).await
}

impl ResolverProfileEventReconciliation {
    /// Replay one chronological chain context after every target page has been
    /// staged. The returned publication retains the exact target set until the
    /// indexer durably publishes its projection invalidations.
    pub async fn reconcile(self) -> Result<ResolverProfileEventReconciliationPublication> {
        self.reconcile_with_log_limit(DEFAULT_REPLAY_MAX_RAW_LOGS_PER_PAGE, None)
            .await
    }

    pub async fn reconcile_with_progress(
        self,
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<ResolverProfileEventReconciliationPublication> {
        self.reconcile_with_log_limit(DEFAULT_REPLAY_MAX_RAW_LOGS_PER_PAGE, Some(progress))
            .await
    }

    async fn reconcile_with_log_limit(
        mut self,
        max_raw_logs_per_page: usize,
        mut progress: Option<&mut dyn StartupAdapterProgress>,
    ) -> Result<ResolverProfileEventReconciliationPublication> {
        ensure!(
            max_raw_logs_per_page > 0,
            "resolver profile reconciliation max logs per page must be positive"
        );
        let prepared = self.prepare(&mut progress).await?;
        let targets::ResolverProfileEventReconciliation {
            pool,
            chain,
            mut raw_log_guard,
            run_id,
        } = self;
        let replay_range = prepared.replay_range;
        let mut summary = ResolverProfileEventReconciliationSummary {
            resolver_address_count: prepared.resolver_address_count,
            block_hash_count: replay_range.map_or(0, |range| range.resolver_block_count),
            ..ResolverProfileEventReconciliationSummary::default()
        };
        let Some(replay_range) = replay_range else {
            return Ok(ResolverProfileEventReconciliationPublication::new(
                chain,
                run_id,
                raw_log_guard,
                summary,
            ));
        };
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

        // Derive one chronological history under the current resolver profile.
        // Resolver facts define the inclusive repair range; registry, registrar,
        // and wrapper facts inside that range remain required authority context.
        let replay = match progress.as_mut() {
            Some(progress) => {
                pipeline::sync_ens_v1_unwrapped_authority_with_scope(
                    &pool,
                    &chain,
                    false,
                    &[],
                    None,
                    None,
                    None,
                    None,
                    Some(&mut replay_context),
                    Some(&mut **progress),
                )
                .await?
            }
            None => {
                pipeline::sync_ens_v1_unwrapped_authority_with_scope(
                    &pool,
                    &chain,
                    false,
                    &[],
                    None,
                    None,
                    None,
                    None,
                    Some(&mut replay_context),
                    None,
                )
                .await?
            }
        };
        summary.scanned_log_count = replay.scanned_log_count;
        summary.matched_log_count = replay.matched_log_count;
        summary.normalized_event_count = replay.total_normalized_event_count;
        summary.normalized_event_inserted_count = replay.total_normalized_event_inserted_count;
        summary.replay_page_count = replay_context.page_count;
        summary.max_replay_page_log_count = replay_context.max_page_log_count;
        summary.max_live_state_item_count = replay_context.max_live_state_item_count;
        summary.max_live_state_payload_bytes = replay_context.max_live_state_payload_bytes;

        mark_reconciliation_replay_complete(&pool, &chain, run_id).await?;
        let (inserted_count, orphaned_count) = publish_resolver_profile_events(
            &pool,
            raw_log_guard.transaction_mut(),
            &chain,
            replay_range,
            run_id,
            &prepared.resolver_address_set_digest,
            prepared.resolver_address_count,
            &mut progress,
        )
        .await?;
        summary.normalized_event_inserted_count = inserted_count;
        summary.orphaned_normalized_event_count = usize::try_from(orphaned_count)
            .context("orphaned resolver-profile normalized-event count does not fit usize")?;
        Ok(ResolverProfileEventReconciliationPublication::new(
            chain,
            run_id,
            raw_log_guard,
            summary,
        ))
    }
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

async fn publish_resolver_profile_events(
    pool: &PgPool,
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chain: &str,
    replay_range: ResolverEmitterReplayRange,
    run_id: Uuid,
    resolver_address_set_digest: &str,
    resolver_address_count: usize,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
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
            LIMIT 1000
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
        record_progress(pool, progress).await?;
    }

    let event_watermark = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(normalized_event_id), 0)::BIGINT FROM normalized_events",
    )
    .fetch_one(transaction.as_mut())
    .await
    .context("failed to load resolver-profile orphan scan watermark")?;
    let mut after_event_id = 0i64;
    let mut orphaned = 0u64;
    while after_event_id < event_watermark {
        let (last_event_id, page_orphaned_count) = sqlx::query_as::<_, (Option<i64>, i64)>(
            r#"
        WITH source_page AS MATERIALIZED (
            SELECT event.normalized_event_id
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
            WHERE event.normalized_event_id > $5
              AND event.normalized_event_id <= $6
              AND event.chain_id = $1
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
            ORDER BY event.normalized_event_id
            LIMIT 1000
        ),
        page_end AS (
            SELECT MAX(normalized_event_id) AS last_event_id
            FROM source_page
        ),
        updated AS (
            UPDATE normalized_events event
            SET canonicality_state = 'orphaned'::canonicality_state, observed_at = now()
            FROM source_page
            WHERE event.normalized_event_id = source_page.normalized_event_id
            RETURNING 1
        )
        SELECT
            page_end.last_event_id,
            (SELECT COUNT(*)::BIGINT FROM updated)
        FROM page_end
        "#,
        )
        .bind(chain)
        .bind(replay_range.first_block_number)
        .bind(replay_range.last_block_number)
        .bind(run_id)
        .bind(after_event_id)
        .bind(event_watermark)
        .fetch_one(transaction.as_mut())
        .await
        .with_context(|| {
            format!("failed to orphan stale resolver-profile event page for {chain}")
        })?;
        let Some(last_event_id) = last_event_id else {
            break;
        };
        ensure!(
            last_event_id > after_event_id,
            "resolver-profile orphan scan did not advance"
        );
        after_event_id = last_event_id;
        orphaned = orphaned
            .checked_add(u64::try_from(page_orphaned_count)?)
            .context("resolver-profile orphan count overflowed u64")?;
        record_progress(pool, progress).await?;
    }
    Ok((inserted_count, orphaned))
}

async fn record_progress(
    pool: &PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "resolver_profile_reconciliation/progress_tests.rs"]
mod progress_tests;
