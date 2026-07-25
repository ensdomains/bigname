use anyhow::{Context, Result};
use sqlx::{PgConnection, Row};

use super::{BackfillBlockRange, ExactStoredSelector, STORED_VERIFICATION_BUCKET_BLOCKS};

pub(super) const LOCAL_IDENTITY_BUCKET_SCAN_SQL: &str = r#"
    WITH selected_raw AS (
        SELECT
            ((logs.block_number - $2) / $6)::BIGINT AS bucket,
            lineage.block_hash IS NOT NULL AS usable,
            md5(
                LOWER(logs.block_hash)
                || LOWER(logs.transaction_hash)
                || logs.log_index::TEXT
            ) AS row_hash
        FROM raw_logs logs
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
         AND lineage.block_number = logs.block_number
         AND lineage.canonicality_state IN (
             'canonical'::canonicality_state,
             'safe'::canonicality_state,
             'finalized'::canonicality_state
         )
        WHERE logs.chain_id = $1
          AND logs.block_number BETWEEN $2 AND $3
          AND LOWER(logs.emitting_address) = $4
          AND LOWER(logs.topics[1]) = ANY($5::TEXT[])
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )

        UNION ALL

        SELECT
            ((logs.block_number - $2) / $6)::BIGINT AS bucket,
            FALSE AS usable,
            NULL::TEXT AS row_hash
        FROM raw_logs logs
        WHERE logs.chain_id = $1
          AND logs.block_number BETWEEN $2 AND $3
          AND LOWER(logs.emitting_address) = $4
          AND LOWER(logs.topics[1]) = ANY($5::TEXT[])
          AND logs.canonicality_state = 'observed'::canonicality_state
    )
    SELECT
        bucket,
        COUNT(*) FILTER (WHERE usable)::BIGINT AS selected_log_count,
        COUNT(*) FILTER (WHERE NOT usable)::BIGINT AS invalid_count,
        COALESCE(
            bit_xor(
                ('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT
            ) FILTER (WHERE usable),
            0
        ) AS digest_left,
        COALESCE(
            bit_xor(
                ('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT
            ) FILTER (WHERE usable),
            0
        ) AS digest_right
    FROM selected_raw
    GROUP BY bucket
    ORDER BY bucket
"#;

pub(super) const FINAL_PAYLOAD_DIGEST_SCAN_SQL: &str = r#"
    WITH matching AS (
        SELECT
            lineage.block_hash IS NOT NULL AS usable,
            md5(
                jsonb_build_array(
                    LOWER(logs.block_hash),
                    logs.block_number,
                    LOWER(logs.transaction_hash),
                    logs.transaction_index,
                    logs.log_index,
                    LOWER(logs.emitting_address),
                    logs.topics,
                    encode(logs.data, 'hex')
                )::TEXT
            ) AS row_hash
        FROM raw_logs logs
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
         AND lineage.block_number = logs.block_number
         AND lineage.canonicality_state IN (
             'canonical'::canonicality_state,
             'safe'::canonicality_state,
             'finalized'::canonicality_state
         )
        WHERE logs.chain_id = $1
          AND logs.block_number BETWEEN $2 AND $3
          AND LOWER(logs.emitting_address) = $4
          AND LOWER(logs.topics[1]) = ANY($5::TEXT[])
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )

        UNION ALL

        SELECT FALSE AS usable, NULL::TEXT AS row_hash
        FROM raw_logs logs
        WHERE logs.chain_id = $1
          AND logs.block_number BETWEEN $2 AND $3
          AND LOWER(logs.emitting_address) = $4
          AND LOWER(logs.topics[1]) = ANY($5::TEXT[])
          AND logs.canonicality_state = 'observed'::canonicality_state
    )
    SELECT
        COUNT(*) FILTER (WHERE usable)::BIGINT AS selected_log_count,
        COUNT(*) FILTER (WHERE NOT usable)::BIGINT AS invalid_count,
        LPAD(
            to_hex(
                COALESCE(
                    bit_xor(
                        ('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT
                    ) FILTER (WHERE usable),
                    0
                )
            ),
            16,
            '0'
        ) || LPAD(
            to_hex(
                COALESCE(
                    bit_xor(
                        ('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT
                    ) FILTER (WHERE usable),
                    0
                )
            ),
            16,
            '0'
        ) AS selected_log_digest
    FROM matching
"#;

pub(super) struct LocalIdentityBucketScanRow {
    pub(super) bucket: i64,
    pub(super) selected_log_count: i64,
    pub(super) invalid_count: i64,
    pub(super) digest_left: i64,
    pub(super) digest_right: i64,
}

pub(super) struct FinalPayloadDigestScan {
    pub(super) selected_log_count: i64,
    pub(super) invalid_count: i64,
    pub(super) selected_log_digest: String,
}

pub(super) async fn scan_local_identity_buckets(
    pool: &sqlx::PgPool,
    selector: &ExactStoredSelector,
    range: BackfillBlockRange,
) -> Result<Vec<LocalIdentityBucketScanRow>> {
    let rows = sqlx::query(LOCAL_IDENTITY_BUCKET_SCAN_SQL)
        .bind(&selector.chain)
        .bind(range.from_block)
        .bind(range.to_block)
        .bind(&selector.address)
        .bind(&selector.topic0s)
        .bind(STORED_VERIFICATION_BUCKET_BLOCKS)
        .fetch_all(pool)
        .await
        .with_context(|| scan_context("plan", selector, range))?;
    let rows = rows
        .into_iter()
        .map(|row| {
            Ok(LocalIdentityBucketScanRow {
                bucket: row.try_get("bucket")?,
                selected_log_count: row.try_get("selected_log_count")?,
                invalid_count: row.try_get("invalid_count")?,
                digest_left: row.try_get("digest_left")?,
                digest_right: row.try_get("digest_right")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    #[cfg(test)]
    test_hook::pause_after_scan(pool, "identity").await;
    Ok(rows)
}

pub(super) async fn scan_final_payload_digest(
    pool: &sqlx::PgPool,
    selector: &ExactStoredSelector,
    range: BackfillBlockRange,
) -> Result<FinalPayloadDigestScan> {
    let row = sqlx::query(FINAL_PAYLOAD_DIGEST_SCAN_SQL)
        .bind(&selector.chain)
        .bind(range.from_block)
        .bind(range.to_block)
        .bind(&selector.address)
        .bind(&selector.topic0s)
        .fetch_one(pool)
        .await
        .with_context(|| scan_context("finalize", selector, range))?;
    let scan = FinalPayloadDigestScan {
        selected_log_count: row.try_get("selected_log_count")?,
        invalid_count: row.try_get("invalid_count")?,
        selected_log_digest: row.try_get("selected_log_digest")?,
    };
    #[cfg(test)]
    test_hook::pause_after_scan(pool, "payload").await;
    Ok(scan)
}

pub(super) async fn range_changed_since(
    connection: &mut PgConnection,
    chain: &str,
    revision: i64,
    range: BackfillBlockRange,
) -> Result<bool> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM raw_log_staging_block_revisions changed
            WHERE changed.chain_id = $1
              AND changed.block_number BETWEEN $3 AND $4
              AND changed.revision > $2
        )
        "#,
    )
    .bind(chain)
    .bind(revision)
    .bind(range.from_block)
    .bind(range.to_block)
    .fetch_one(connection)
    .await
    .context("failed to fence stored verification range against concurrent raw-log changes")
}

pub(super) async fn segments_changed_since(
    connection: &mut PgConnection,
    chain: &str,
    revision: i64,
    from_blocks: &[i64],
    to_blocks: &[i64],
) -> Result<bool> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM UNNEST($3::BIGINT[], $4::BIGINT[])
                AS segment(from_block, to_block)
            JOIN raw_log_staging_block_revisions changed
              ON changed.chain_id = $1
             AND changed.block_number BETWEEN
                 segment.from_block AND segment.to_block
             AND changed.revision > $2
        )
        "#,
    )
    .bind(chain)
    .bind(revision)
    .bind(from_blocks)
    .bind(to_blocks)
    .fetch_one(connection)
    .await
    .context("failed to fence stored verification segments against later raw-log changes")
}

fn scan_context(phase: &str, selector: &ExactStoredSelector, range: BackfillBlockRange) -> String {
    format!(
        "failed to {phase} stored raw-log verification for {} {} on {} over {}..={}",
        selector.source_family, selector.address, selector.chain, range.from_block, range.to_block
    )
}

#[cfg(test)]
pub(super) use test_hook::install_after_scan;

#[cfg(test)]
mod test_hook {
    use std::sync::Arc;

    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use tokio::sync::Notify;

    type HookKey = (String, &'static str);

    #[derive(Clone)]
    struct HookState {
        after_scan: Arc<Notify>,
        resume: Arc<Notify>,
    }

    static HOOKS: ScopedTestHookRegistry<HookKey, HookState> = ScopedTestHookRegistry::new();

    pub(crate) struct AfterScanHook {
        state: HookState,
        _registration: ScopedTestHookGuard<HookKey, HookState>,
    }

    impl AfterScanHook {
        pub(crate) async fn wait(&self) {
            self.state.after_scan.notified().await;
        }

        pub(crate) fn resume(&self) {
            self.state.resume.notify_one();
        }
    }

    impl Drop for AfterScanHook {
        fn drop(&mut self) {
            self.state.resume.notify_one();
        }
    }

    pub(crate) async fn install_after_scan(
        pool: &sqlx::PgPool,
        phase: &'static str,
    ) -> AfterScanHook {
        let database = current_test_database(pool)
            .await
            .expect("stored verification test hook must identify its database");
        let state = HookState {
            after_scan: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        let registration = HOOKS.install((database, phase), state.clone());
        AfterScanHook {
            state,
            _registration: registration,
        }
    }

    pub(super) async fn pause_after_scan(pool: &sqlx::PgPool, phase: &'static str) {
        let database = current_test_database(pool)
            .await
            .expect("stored verification test hook must identify its database");
        if let Some(hook) = HOOKS.take(&(database, phase)) {
            hook.after_scan.notify_one();
            hook.resume.notified().await;
        }
    }
}
