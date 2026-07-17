use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, PgPool, Row};

use super::StoredLineageCoverageFrontierHeader;

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
