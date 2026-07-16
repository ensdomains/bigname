use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};

/// Commit-ordered version of one chain's retained raw-log staging corpus.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawLogStagingInputVersion {
    pub retention_generation: i64,
    pub revision: i64,
}

/// Compatibility of a previously consumed inclusive raw-log boundary with the
/// currently fenced staging corpus.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawLogStagingBoundaryStatus {
    Accepted(RawLogStagingInputVersion),
    RetentionGenerationChanged {
        observed: RawLogStagingInputVersion,
    },
    ChangedAtOrBefore {
        observed: RawLogStagingInputVersion,
        earliest_block: i64,
    },
}

/// A long-lived same-chain semantic-mutation fence plus a raw-log truncation
/// lock. Ordinary writes on other chains remain live while the guard is held.
pub struct RawLogStagingReadGuard {
    transaction: Transaction<'static, Postgres>,
    chain: String,
    version: RawLogStagingInputVersion,
}

impl RawLogStagingReadGuard {
    pub fn version(&self) -> RawLogStagingInputVersion {
        self.version
    }

    /// Borrow the fenced transaction's connection for an atomic caller-owned
    /// validation/publication statement before releasing the guard.
    pub fn connection_mut(&mut self) -> &mut PgConnection {
        self.transaction.as_mut()
    }

    /// Borrow the fenced transaction for a caller-owned atomic publication
    /// that spans more than one statement.
    pub fn transaction_mut(&mut self) -> &mut Transaction<'static, Postgres> {
        &mut self.transaction
    }

    /// Accepts the fenced corpus version when every semantic mutation newer
    /// than `expected` is strictly after the inclusive consumed boundary.
    ///
    /// The caller may atomically publish the returned version through
    /// `connection_mut`. Retention rotation, missing revision evidence, or a
    /// mutation at/before the boundary fails closed.
    pub async fn accept_newer_revisions_after(
        &mut self,
        expected: RawLogStagingInputVersion,
        consumed_through_block: i64,
    ) -> Result<RawLogStagingInputVersion> {
        match self
            .classify_newer_revisions_after(expected, consumed_through_block)
            .await?
        {
            RawLogStagingBoundaryStatus::Accepted(observed) => Ok(observed),
            RawLogStagingBoundaryStatus::RetentionGenerationChanged { observed } => {
                anyhow::bail!(
                    "raw-log staging retention generation changed for {}: expected {}, observed {}",
                    self.chain,
                    expected.retention_generation,
                    observed.retention_generation
                )
            }
            RawLogStagingBoundaryStatus::ChangedAtOrBefore { earliest_block, .. } => {
                anyhow::bail!(
                    "raw-log staging input changed for {} at block {} at or before consumed block {} after revision {}",
                    self.chain,
                    earliest_block,
                    consumed_through_block,
                    expected.revision
                )
            }
        }
    }

    pub async fn classify_newer_revisions_after(
        &mut self,
        expected: RawLogStagingInputVersion,
        consumed_through_block: i64,
    ) -> Result<RawLogStagingBoundaryStatus> {
        classify_newer_revisions_after(
            self.transaction.as_mut(),
            &self.chain,
            self.version,
            expected,
            consumed_through_block,
        )
        .await
    }

    pub async fn ensure_current(&mut self) -> Result<()> {
        let observed =
            load_raw_log_staging_input_version_in_transaction(&mut self.transaction, &self.chain)
                .await?;
        ensure!(
            observed == self.version,
            "raw-log staging input changed for {} while its replay read fence was held: expected generation {} revision {}, observed generation {} revision {}",
            self.chain,
            self.version.retention_generation,
            self.version.revision,
            observed.retention_generation,
            observed.revision
        );
        Ok(())
    }

    pub async fn release(mut self) -> Result<()> {
        self.ensure_current().await?;
        self.transaction.commit().await.with_context(|| {
            format!(
                "failed to release raw-log staging read fence for {}",
                self.chain
            )
        })
    }
}

/// One-transaction read fence for a sorted set of chains. Raw writers for
/// other chains remain live, and the guarded set consumes only one pool
/// connection regardless of chain count.
pub struct RawLogStagingReadSetGuard {
    transaction: Transaction<'static, Postgres>,
    versions: BTreeMap<String, RawLogStagingInputVersion>,
}

impl RawLogStagingReadSetGuard {
    pub fn version(&self, chain: &str) -> Option<RawLogStagingInputVersion> {
        self.versions.get(chain).copied()
    }

    pub fn connection_mut(&mut self) -> &mut PgConnection {
        self.transaction.as_mut()
    }

    pub async fn classify_newer_revisions_after(
        &mut self,
        chain: &str,
        expected: RawLogStagingInputVersion,
        consumed_through_block: i64,
    ) -> Result<RawLogStagingBoundaryStatus> {
        let observed =
            self.versions.get(chain).copied().with_context(|| {
                format!("raw-log staging read set does not guard chain {chain}")
            })?;
        classify_newer_revisions_after(
            self.transaction.as_mut(),
            chain,
            observed,
            expected,
            consumed_through_block,
        )
        .await
    }

    pub async fn ensure_current(&mut self) -> Result<()> {
        for (chain, expected) in &self.versions {
            let observed =
                load_raw_log_staging_input_version_in_transaction(&mut self.transaction, chain)
                    .await?;
            ensure!(
                observed == *expected,
                "raw-log staging input changed for {} while its read-set fence was held: expected generation {} revision {}, observed generation {} revision {}",
                chain,
                expected.retention_generation,
                expected.revision,
                observed.retention_generation,
                observed.revision
            );
        }
        Ok(())
    }

    pub async fn release(mut self) -> Result<()> {
        self.ensure_current().await?;
        self.transaction
            .commit()
            .await
            .context("failed to release raw-log staging read-set fence")
    }
}

pub async fn acquire_raw_log_staging_read_guard(
    pool: &PgPool,
    chain: &str,
) -> Result<RawLogStagingReadGuard> {
    ensure!(
        !chain.trim().is_empty(),
        "raw-log staging chain must not be empty"
    );
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start raw-log staging read fence")?;
    // Raw-log semantic-mutation triggers acquire this chain-scoped key before
    // publishing their revision. This lock is acquired before the table lock,
    // matching the ENSv2 full-source guard's order.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{chain}"))
        .execute(transaction.as_mut())
        .await
        .with_context(|| format!("failed to fence raw-log staging mutation for {chain}"))?;
    // ACCESS SHARE permits INSERT/UPDATE/DELETE but blocks global TRUNCATE.
    sqlx::query("LOCK TABLE raw_logs IN ACCESS SHARE MODE")
        .execute(transaction.as_mut())
        .await
        .with_context(|| format!("failed to fence raw-log staging truncation for {chain}"))?;
    let version =
        load_raw_log_staging_input_version_in_transaction(&mut transaction, chain).await?;
    Ok(RawLogStagingReadGuard {
        transaction,
        chain: chain.to_owned(),
        version,
    })
}

pub async fn acquire_raw_log_staging_read_set_guard(
    pool: &PgPool,
    chains: &[String],
) -> Result<RawLogStagingReadSetGuard> {
    ensure!(
        !chains.is_empty(),
        "raw-log staging read set must contain at least one chain"
    );
    for chain in chains {
        ensure!(
            !chain.trim().is_empty(),
            "raw-log staging read-set chain must not be empty"
        );
    }
    let chains = chains.iter().collect::<BTreeSet<_>>();
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start raw-log staging read-set fence")?;
    for chain in &chains {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("raw_log_staging:{chain}"))
            .execute(transaction.as_mut())
            .await
            .with_context(|| format!("failed to fence raw-log staging mutation for {chain}"))?;
    }
    sqlx::query("LOCK TABLE raw_logs IN ACCESS SHARE MODE")
        .execute(transaction.as_mut())
        .await
        .context("failed to fence raw-log staging truncation for read set")?;

    let mut versions = BTreeMap::new();
    for chain in chains {
        let version =
            load_raw_log_staging_input_version_in_transaction(&mut transaction, chain).await?;
        versions.insert((*chain).clone(), version);
    }
    Ok(RawLogStagingReadSetGuard {
        transaction,
        versions,
    })
}

async fn classify_newer_revisions_after(
    connection: &mut PgConnection,
    chain: &str,
    observed: RawLogStagingInputVersion,
    expected: RawLogStagingInputVersion,
    consumed_through_block: i64,
) -> Result<RawLogStagingBoundaryStatus> {
    ensure!(
        expected.retention_generation >= 0 && expected.revision >= 0,
        "expected raw-log staging input version must not be negative"
    );
    ensure!(
        consumed_through_block >= 0,
        "raw-log staging consumed boundary must not be negative"
    );
    if observed.retention_generation != expected.retention_generation {
        return Ok(RawLogStagingBoundaryStatus::RetentionGenerationChanged { observed });
    }
    ensure!(
        observed.revision >= expected.revision,
        "raw-log staging revision moved backwards for {chain}: expected at least {}, observed {}",
        expected.revision,
        observed.revision
    );
    if observed.revision == expected.revision {
        return Ok(RawLogStagingBoundaryStatus::Accepted(observed));
    }

    let earliest_changed_block = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MIN(block_number)
        FROM raw_log_staging_block_revisions
        WHERE chain_id = $1
          AND revision > $2
        "#,
    )
    .bind(chain)
    .bind(expected.revision)
    .fetch_one(connection)
    .await
    .with_context(|| {
        format!(
            "failed to inspect fenced raw-log staging changes for {chain} after revision {}",
            expected.revision
        )
    })?;
    let earliest_changed_block = earliest_changed_block.with_context(|| {
        format!(
            "raw-log staging revision advanced for {chain} from {} to {} without per-block revision evidence",
            expected.revision, observed.revision
        )
    })?;
    if earliest_changed_block <= consumed_through_block {
        return Ok(RawLogStagingBoundaryStatus::ChangedAtOrBefore {
            observed,
            earliest_block: earliest_changed_block,
        });
    }

    Ok(RawLogStagingBoundaryStatus::Accepted(observed))
}

pub async fn load_raw_log_staging_input_version(
    pool: &PgPool,
    chain: &str,
) -> Result<RawLogStagingInputVersion> {
    ensure!(
        !chain.trim().is_empty(),
        "raw-log staging chain must not be empty"
    );
    let row = sqlx::query(
        r#"
        SELECT retention_generation, revision
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load raw-log staging input version for {chain}"))?;
    raw_log_staging_input_version_from_row(row)
}

/// Reports whether a committed semantic raw-log mutation after `revision`
/// touched any block in the inclusive range.
pub async fn raw_log_staging_block_range_changed_since(
    pool: &PgPool,
    chain: &str,
    revision: i64,
    from_block: i64,
    through_block: i64,
) -> Result<bool> {
    ensure!(
        !chain.trim().is_empty(),
        "raw-log staging chain must not be empty"
    );
    ensure!(
        revision >= 0,
        "raw-log staging revision must not be negative"
    );
    ensure!(
        from_block >= 0,
        "raw-log staging range start must not be negative"
    );
    ensure!(
        through_block >= from_block,
        "raw-log staging range end must not precede its start"
    );
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM raw_log_staging_block_revisions
            WHERE chain_id = $1
              AND revision > $2
              AND block_number BETWEEN $3 AND $4
        )
        "#,
    )
    .bind(chain)
    .bind(revision)
    .bind(from_block)
    .bind(through_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to inspect raw-log staging changes for {chain} after revision {revision} in {from_block}..={through_block}"
        )
    })
}

/// Returns the earliest block at or below `through_block` touched by a
/// semantic raw-log mutation after `revision`.
pub async fn earliest_raw_log_staging_block_changed_since(
    pool: &PgPool,
    chain: &str,
    revision: i64,
    through_block: i64,
) -> Result<Option<i64>> {
    ensure!(
        !chain.trim().is_empty(),
        "raw-log staging chain must not be empty"
    );
    ensure!(
        revision >= 0,
        "raw-log staging revision must not be negative"
    );
    ensure!(
        through_block >= 0,
        "raw-log staging changed-block boundary must not be negative"
    );
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MIN(block_number)
        FROM raw_log_staging_block_revisions
        WHERE chain_id = $1
          AND revision > $2
          AND block_number <= $3
        "#,
    )
    .bind(chain)
    .bind(revision)
    .bind(through_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load earliest raw-log staging change for {chain} after revision {revision} through block {through_block}"
        )
    })
}

async fn load_raw_log_staging_input_version_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
) -> Result<RawLogStagingInputVersion> {
    let row = sqlx::query(
        r#"
        SELECT retention_generation, revision
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(transaction.as_mut())
    .await
    .with_context(|| format!("failed to load fenced raw-log staging input version for {chain}"))?;
    raw_log_staging_input_version_from_row(row)
}

fn raw_log_staging_input_version_from_row(
    row: Option<sqlx::postgres::PgRow>,
) -> Result<RawLogStagingInputVersion> {
    let Some(row) = row else {
        return Ok(RawLogStagingInputVersion::default());
    };
    Ok(RawLogStagingInputVersion {
        retention_generation: row.try_get("retention_generation")?,
        revision: row.try_get("revision")?,
    })
}
