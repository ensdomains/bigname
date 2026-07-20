use std::collections::BTreeSet;

use alloy_primitives::Keccak256;
use anyhow::{Context, Result, bail, ensure};
use bigname_storage::{RawLogStagingReadGuard, acquire_raw_log_staging_read_guard};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder, types::Uuid};

use super::{ResolverEmitterReplayRange, ResolverProfileEventReconciliationSummary};

const TARGET_BATCH_SIZE: usize = 1_000;

pub struct ResolverProfileEventReconciliation {
    pub(super) pool: PgPool,
    pub(super) chain: String,
    pub(super) raw_log_guard: RawLogStagingReadGuard,
    pub(super) run_id: Uuid,
}

pub struct ResolverProfileEventReconciliationPublication {
    chain: String,
    run_id: Uuid,
    raw_log_guard: RawLogStagingReadGuard,
    summary: ResolverProfileEventReconciliationSummary,
}

pub(super) struct PreparedResolverProfileEventReconciliation {
    pub(super) replay_range: Option<ResolverEmitterReplayRange>,
    pub(super) resolver_address_count: usize,
    pub(super) resolver_address_set_digest: String,
}

pub(super) async fn begin_reconciliation(
    pool: &PgPool,
    chain: &str,
) -> Result<ResolverProfileEventReconciliation> {
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

    let run_id = Uuid::new_v4();
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start resolver-profile target-staging transaction")?;
    sqlx::query("DELETE FROM resolver_profile_reconciliation_runs WHERE chain_id = $1")
        .bind(chain)
        .execute(transaction.as_mut())
        .await
        .with_context(|| {
            format!("failed to clean an incomplete resolver-profile run for chain {chain}")
        })?;
    // Valid placeholders make a partially staged run ordinary crash residue;
    // prepare() replaces every value before replay can consume the row.
    sqlx::query(
        r#"
        INSERT INTO resolver_profile_reconciliation_runs (
            run_id,
            chain_id,
            first_block_number,
            last_block_number,
            resolver_address_count,
            resolver_address_set_digest
        ) VALUES ($1, $2, 0, 0, 1, 'target-staging')
        "#,
    )
    .bind(run_id)
    .bind(chain)
    .execute(transaction.as_mut())
    .await
    .with_context(|| format!("failed to create resolver-profile target staging for {chain}"))?;
    transaction
        .commit()
        .await
        .context("failed to commit resolver-profile target staging")?;

    Ok(ResolverProfileEventReconciliation {
        pool: pool.clone(),
        chain: chain.to_owned(),
        raw_log_guard,
        run_id,
    })
}

impl ResolverProfileEventReconciliation {
    pub const fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub async fn stage_addresses(&mut self, resolver_addresses: &[String]) -> Result<()> {
        let addresses = normalized_resolver_addresses(resolver_addresses)?;
        for addresses in addresses.chunks(TARGET_BATCH_SIZE) {
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO resolver_profile_reconciliation_targets \
                 (run_id, resolver_address) ",
            );
            builder.push_values(addresses, |mut row, address| {
                row.push_bind(self.run_id).push_bind(address);
            });
            builder.push(" ON CONFLICT (run_id, resolver_address) DO NOTHING");
            builder.build().execute(&self.pool).await.with_context(|| {
                format!(
                    "failed to stage resolver-profile target page for {}",
                    self.chain
                )
            })?;
        }
        Ok(())
    }

    pub(super) async fn prepare(&mut self) -> Result<PreparedResolverProfileEventReconciliation> {
        let (resolver_address_count, resolver_address_set_digest) =
            load_target_metadata(&self.pool, self.run_id).await?;
        ensure!(
            resolver_address_count > 0,
            "resolver-profile reconciliation must stage at least one target"
        );
        let replay_range =
            load_resolver_emitter_replay_range(&self.pool, &self.chain, self.run_id).await?;
        let (first_block_number, last_block_number, status) = replay_range
            .map_or((0, 0, "replay_complete"), |range| {
                (range.first_block_number, range.last_block_number, "running")
            });
        let result = sqlx::query(
            r#"
            UPDATE resolver_profile_reconciliation_runs
            SET
                first_block_number = $3,
                last_block_number = $4,
                resolver_address_count = $5,
                resolver_address_set_digest = $6,
                status = $7,
                updated_at = now()
            WHERE run_id = $1
              AND chain_id = $2
              AND status = 'running'
            "#,
        )
        .bind(self.run_id)
        .bind(&self.chain)
        .bind(first_block_number)
        .bind(last_block_number)
        .bind(i64::try_from(resolver_address_count)?)
        .bind(&resolver_address_set_digest)
        .bind(status)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "failed to prepare resolver-profile target staging for {}",
                self.chain
            )
        })?;
        ensure!(
            result.rows_affected() == 1,
            "resolver-profile target staging disappeared before replay for {}",
            self.chain
        );
        Ok(PreparedResolverProfileEventReconciliation {
            replay_range,
            resolver_address_count,
            resolver_address_set_digest,
        })
    }
}

impl ResolverProfileEventReconciliationPublication {
    pub(super) fn new(
        chain: String,
        run_id: Uuid,
        raw_log_guard: RawLogStagingReadGuard,
        summary: ResolverProfileEventReconciliationSummary,
    ) -> Self {
        Self {
            chain,
            run_id,
            raw_log_guard,
            summary,
        }
    }

    pub const fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Borrow the fenced publication connection so indexer-owned projection
    /// invalidations commit atomically with the normalized-event repair.
    pub fn connection_mut(&mut self) -> &mut PgConnection {
        self.raw_log_guard.connection_mut()
    }

    pub async fn finish(self) -> Result<ResolverProfileEventReconciliationSummary> {
        let Self {
            chain,
            run_id,
            mut raw_log_guard,
            summary,
        } = self;
        let result = sqlx::query(
            "DELETE FROM resolver_profile_reconciliation_runs \
             WHERE run_id = $1 AND chain_id = $2",
        )
        .bind(run_id)
        .bind(&chain)
        .execute(raw_log_guard.connection_mut())
        .await
        .with_context(|| format!("failed to clean resolver-profile run for {chain}"))?;
        ensure!(
            result.rows_affected() == 1,
            "resolver-profile run disappeared before cleanup for {chain}"
        );
        raw_log_guard.release().await?;
        Ok(summary)
    }
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

async fn load_target_metadata(pool: &PgPool, run_id: Uuid) -> Result<(usize, String)> {
    let mut after = None::<String>;
    let mut count = 0usize;
    let mut digest = Keccak256::new();
    loop {
        let addresses = sqlx::query_scalar::<_, String>(
            r#"
            SELECT resolver_address
            FROM resolver_profile_reconciliation_targets
            WHERE run_id = $1
              AND ($2::TEXT IS NULL OR resolver_address > $2)
            ORDER BY resolver_address
            LIMIT $3
            "#,
        )
        .bind(run_id)
        .bind(after.as_deref())
        .bind(i64::try_from(TARGET_BATCH_SIZE)?)
        .fetch_all(pool)
        .await
        .context("failed to load staged resolver-profile target metadata page")?;
        let Some(last) = addresses.last() else {
            break;
        };
        after = Some(last.clone());
        for address in addresses {
            if count > 0 {
                digest.update(b"\n");
            }
            digest.update(address.as_bytes());
            count += 1;
        }
    }
    Ok((count, format!("{:#x}", digest.finalize())))
}

async fn load_resolver_emitter_replay_range(
    pool: &PgPool,
    chain: &str,
    run_id: Uuid,
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
            JOIN resolver_profile_reconciliation_targets target
              ON target.run_id = $2
             AND target.resolver_address = LOWER(raw_log.emitting_address)
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = raw_log.chain_id
             AND lineage.block_hash = raw_log.block_hash
            WHERE raw_log.chain_id = $1
              AND raw_log.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            "#,
        )
        .bind(chain)
        .bind(run_id)
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
