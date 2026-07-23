use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, PgPool, Row};

use super::{StoredLineageCoverageFrontierHeader, StoredLineageCoverageProgress};

const INTEGRITY_PAGE_ROWS: i64 = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RequirementSnapshotIntegrity {
    pub(super) row_count: i64,
    pub(super) digest: String,
}

pub(super) fn digest_is_well_formed(digest: &str) -> bool {
    digest.len() == 32
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) async fn validate_candidate_and_load_integrity(
    connection: &mut PgConnection,
    chain: &str,
    verified_from_block: i64,
    verified_through_block: i64,
) -> Result<RequirementSnapshotIntegrity> {
    let row = sqlx::query(
        r#"
        WITH candidate AS (
            SELECT
                *,
                md5(
                    jsonb_build_array(
                        source_family,
                        address,
                        required_intervals::TEXT
                    )::TEXT
                ) AS row_hash
            FROM pg_temp.stored_lineage_coverage_frontier_candidate_requirements
        )
        SELECT
            COUNT(*) FILTER (
                WHERE source_family = ''
                   OR address <> lower(address)
                   OR required_intervals = '{}'::INT8MULTIRANGE
                   OR lower(required_intervals) IS NULL
                   OR upper(required_intervals) IS NULL
                   OR lower(required_intervals) < $1
                   OR upper(required_intervals) > $2 + 1
            )::BIGINT AS invalid_count,
            COUNT(*)::BIGINT AS requirement_row_count,
            LPAD(
                to_hex(
                    COALESCE(
                        bit_xor(('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT),
                        0
                    )
                ),
                16,
                '0'
            ) || LPAD(
                to_hex(
                    COALESCE(
                        bit_xor(('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT),
                        0
                    )
                ),
                16,
                '0'
            ) AS requirement_digest
        FROM candidate
        "#,
    )
    .bind(verified_from_block)
    .bind(verified_through_block)
    .fetch_one(connection)
    .await
    .with_context(|| format!("failed to validate coverage candidate rows for {chain}"))?;
    let invalid_count: i64 = row.try_get("invalid_count")?;
    ensure!(
        invalid_count == 0,
        "stored-lineage coverage candidate for {chain} has {invalid_count} invalid rows"
    );
    Ok(RequirementSnapshotIntegrity {
        row_count: row.try_get("requirement_row_count")?,
        digest: row.try_get("requirement_digest")?,
    })
}

pub(super) async fn validate_candidate_and_load_integrity_with_progress(
    connection: &mut PgConnection,
    chain: &str,
    verified_from_block: i64,
    verified_through_block: i64,
    progress: &mut dyn StoredLineageCoverageProgress,
) -> Result<RequirementSnapshotIntegrity> {
    let mut cursor = None::<(String, String)>;
    let mut row_count = 0i64;
    let mut invalid_count = 0i64;
    let mut digest_left = 0u64;
    let mut digest_right = 0u64;
    loop {
        let rows = sqlx::query(
            r#"
            SELECT
                source_family,
                address,
                (
                    source_family = ''
                    OR address <> lower(address)
                    OR required_intervals = '{}'::INT8MULTIRANGE
                    OR lower(required_intervals) IS NULL
                    OR upper(required_intervals) IS NULL
                    OR lower(required_intervals) < $3
                    OR upper(required_intervals) > $4 + 1
                ) AS invalid,
                md5(
                    jsonb_build_array(
                        source_family,
                        address,
                        required_intervals::TEXT
                    )::TEXT
                ) AS row_hash
            FROM pg_temp.stored_lineage_coverage_frontier_candidate_requirements
            WHERE $1::TEXT IS NULL OR (source_family, address) > ($1, $2)
            ORDER BY source_family, address
            LIMIT $5
            "#,
        )
        .bind(cursor.as_ref().map(|(family, _)| family))
        .bind(cursor.as_ref().map(|(_, address)| address))
        .bind(verified_from_block)
        .bind(verified_through_block)
        .bind(INTEGRITY_PAGE_ROWS)
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("failed to validate a coverage candidate page for {chain}"))?;
        let Some(last) = rows.last() else {
            break;
        };
        cursor = Some((last.try_get("source_family")?, last.try_get("address")?));
        accumulate_integrity_page(
            &rows,
            &mut row_count,
            &mut invalid_count,
            &mut digest_left,
            &mut digest_right,
        )?;
        progress.record().await?;
    }
    ensure!(
        invalid_count == 0,
        "stored-lineage coverage candidate for {chain} has {invalid_count} invalid rows"
    );
    Ok(RequirementSnapshotIntegrity {
        row_count,
        digest: format!("{digest_left:016x}{digest_right:016x}"),
    })
}

pub(super) async fn saved_snapshot_is_valid(
    pool: &PgPool,
    header: &StoredLineageCoverageFrontierHeader,
) -> Result<bool> {
    if !header.is_well_formed {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        WITH requirements AS (
            SELECT
                *,
                md5(
                    jsonb_build_array(
                        source_family,
                        address,
                        required_intervals::TEXT
                    )::TEXT
                ) AS row_hash
            FROM stored_lineage_coverage_frontier_requirements
            WHERE chain_id = $1
        )
        SELECT
            COUNT(*) FILTER (
                WHERE required_intervals = '{}'::INT8MULTIRANGE
                   OR lower(required_intervals) IS NULL
                   OR upper(required_intervals) IS NULL
                   OR lower(required_intervals) < $2
                   OR upper(required_intervals) > $3 + 1
            )::BIGINT AS invalid_count,
            COUNT(*)::BIGINT AS requirement_row_count,
            LPAD(
                to_hex(
                    COALESCE(
                        bit_xor(('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT),
                        0
                    )
                ),
                16,
                '0'
            ) || LPAD(
                to_hex(
                    COALESCE(
                        bit_xor(('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT),
                        0
                    )
                ),
                16,
                '0'
            ) AS requirement_digest
        FROM requirements
        "#,
    )
    .bind(&header.chain_id)
    .bind(header.verified_from_block)
    .bind(header.verified_through_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to validate stored-lineage coverage requirements for {} revision {}",
            header.chain_id, header.snapshot_revision
        )
    })?;
    let invalid_count: i64 = row.try_get("invalid_count")?;
    let row_count: i64 = row.try_get("requirement_row_count")?;
    let digest: String = row.try_get("requirement_digest")?;
    Ok(invalid_count == 0
        && row_count == header.requirement_row_count
        && digest == header.requirement_digest)
}

pub(super) async fn saved_snapshot_is_valid_with_progress(
    pool: &PgPool,
    header: &StoredLineageCoverageFrontierHeader,
    progress: &mut dyn StoredLineageCoverageProgress,
) -> Result<bool> {
    if !header.is_well_formed {
        return Ok(false);
    }
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin stored-lineage integrity snapshot")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(transaction.as_mut())
        .await
        .context("failed to pin stored-lineage integrity snapshot")?;
    let mut cursor = None::<(String, String)>;
    let mut row_count = 0i64;
    let mut invalid_count = 0i64;
    let mut digest_left = 0u64;
    let mut digest_right = 0u64;
    loop {
        let rows = sqlx::query(
            r#"
            SELECT
                source_family,
                address,
                (
                    required_intervals = '{}'::INT8MULTIRANGE
                    OR lower(required_intervals) IS NULL
                    OR upper(required_intervals) IS NULL
                    OR lower(required_intervals) < $4
                    OR upper(required_intervals) > $5 + 1
                ) AS invalid,
                md5(
                    jsonb_build_array(
                        source_family,
                        address,
                        required_intervals::TEXT
                    )::TEXT
                ) AS row_hash
            FROM stored_lineage_coverage_frontier_requirements
            WHERE chain_id = $1
              AND ($2::TEXT IS NULL OR (source_family, address) > ($2, $3))
            ORDER BY source_family, address
            LIMIT $6
            "#,
        )
        .bind(&header.chain_id)
        .bind(cursor.as_ref().map(|(family, _)| family))
        .bind(cursor.as_ref().map(|(_, address)| address))
        .bind(header.verified_from_block)
        .bind(header.verified_through_block)
        .bind(INTEGRITY_PAGE_ROWS)
        .fetch_all(transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to validate a stored-lineage coverage page for {} revision {}",
                header.chain_id, header.snapshot_revision
            )
        })?;
        let Some(last) = rows.last() else {
            break;
        };
        cursor = Some((last.try_get("source_family")?, last.try_get("address")?));
        accumulate_integrity_page(
            &rows,
            &mut row_count,
            &mut invalid_count,
            &mut digest_left,
            &mut digest_right,
        )?;
        progress.record().await?;
    }
    transaction
        .commit()
        .await
        .context("failed to close stored-lineage integrity snapshot")?;
    Ok(invalid_count == 0
        && row_count == header.requirement_row_count
        && format!("{digest_left:016x}{digest_right:016x}") == header.requirement_digest)
}

fn accumulate_integrity_page(
    rows: &[sqlx::postgres::PgRow],
    row_count: &mut i64,
    invalid_count: &mut i64,
    digest_left: &mut u64,
    digest_right: &mut u64,
) -> Result<()> {
    for row in rows {
        *row_count += 1;
        if row.try_get::<bool, _>("invalid")? {
            *invalid_count += 1;
        }
        let row_hash: String = row.try_get("row_hash")?;
        ensure!(row_hash.len() == 32, "coverage row hash is malformed");
        *digest_left ^= u64::from_str_radix(&row_hash[..16], 16)
            .context("failed to decode coverage row hash prefix")?;
        *digest_right ^= u64::from_str_radix(&row_hash[16..], 16)
            .context("failed to decode coverage row hash suffix")?;
    }
    Ok(())
}
