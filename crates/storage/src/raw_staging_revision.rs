use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};

/// Commit-ordered version of one chain's retained raw-log staging corpus.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawLogStagingInputVersion {
    pub retention_generation: i64,
    pub revision: i64,
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
        ensure!(
            expected.retention_generation >= 0 && expected.revision >= 0,
            "expected raw-log staging input version must not be negative"
        );
        ensure!(
            consumed_through_block >= 0,
            "raw-log staging consumed boundary must not be negative"
        );
        let observed = self.version;
        ensure!(
            observed.retention_generation == expected.retention_generation,
            "raw-log staging retention generation changed for {}: expected {}, observed {}",
            self.chain,
            expected.retention_generation,
            observed.retention_generation
        );
        ensure!(
            observed.revision >= expected.revision,
            "raw-log staging revision moved backwards for {}: expected at least {}, observed {}",
            self.chain,
            expected.revision,
            observed.revision
        );
        if observed.revision == expected.revision {
            return Ok(observed);
        }

        let earliest_changed_block = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MIN(block_number)
            FROM raw_log_staging_block_revisions
            WHERE chain_id = $1
              AND revision > $2
            "#,
        )
        .bind(&self.chain)
        .bind(expected.revision)
        .fetch_one(self.transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to inspect fenced raw-log staging changes for {} after revision {}",
                self.chain, expected.revision
            )
        })?;
        let earliest_changed_block = earliest_changed_block.with_context(|| {
            format!(
                "raw-log staging revision advanced for {} from {} to {} without per-block revision evidence",
                self.chain, expected.revision, observed.revision
            )
        })?;
        ensure!(
            earliest_changed_block > consumed_through_block,
            "raw-log staging input changed for {} at block {} at or before consumed block {} after revision {}",
            self.chain,
            earliest_changed_block,
            consumed_through_block,
            expected.revision
        );

        Ok(observed)
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
