use anyhow::{Context, Result};
use sqlx::{Executor, PgPool, Postgres};

use super::decode::decode_lineage_block;
use super::types::ChainLineageBlock;

/// Load one lineage snapshot by hash-first identity.
pub async fn load_chain_lineage_block(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<ChainLineageBlock>> {
    load_chain_lineage_block_internal(pool, chain_id, block_hash).await
}

/// Load the highest canonical-marked (canonical/safe/finalized) stored lineage
/// row for a chain — the stored frontier that deep-gap promotion anchors on.
pub async fn load_highest_canonical_chain_lineage_block(
    pool: &PgPool,
    chain_id: &str,
) -> Result<Option<ChainLineageBlock>> {
    let row = sqlx::query(
        r#"
        SELECT
            lineage.chain_id,
            lineage.block_hash,
            lineage.parent_hash,
            lineage.block_number,
            lineage.block_timestamp,
            audit.logs_bloom,
            audit.transactions_root,
            audit.receipts_root,
            audit.state_root,
            lineage.canonicality_state::TEXT AS canonicality_state
        FROM bigname_phase.chain_lineage AS lineage
        LEFT JOIN chain_header_audit AS audit
          ON audit.chain_id = lineage.chain_id
         AND audit.block_hash = lineage.block_hash
        WHERE lineage.chain_id = $1
          AND lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
        ORDER BY lineage.block_number DESC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load highest canonical lineage row for chain {chain_id}")
    })?;

    row.map(decode_lineage_block).transpose()
}

pub(crate) async fn load_chain_lineage_block_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<ChainLineageBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            lineage.chain_id,
            lineage.block_hash,
            lineage.parent_hash,
            lineage.block_number,
            lineage.block_timestamp,
            audit.logs_bloom,
            audit.transactions_root,
            audit.receipts_root,
            audit.state_root,
            lineage.canonicality_state::TEXT AS canonicality_state
        FROM bigname_phase.chain_lineage AS lineage
        LEFT JOIN chain_header_audit AS audit
          ON audit.chain_id = lineage.chain_id
         AND audit.block_hash = lineage.block_hash
        WHERE lineage.chain_id = $1
          AND lineage.block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to load lineage row for chain {chain_id} block {block_hash}")
    })?;

    row.map(decode_lineage_block).transpose()
}

pub async fn chain_lineage_contains_ancestor(
    pool: &PgPool,
    chain_id: &str,
    descendant_hash: &str,
    ancestor_hash: &str,
) -> Result<bool> {
    chain_lineage_contains_ancestor_internal(pool, chain_id, descendant_hash, ancestor_hash).await
}

/// Prove parent-hash ancestry using the caller's already-known ancestor height
/// as the recursive walk floor.
pub async fn chain_lineage_contains_ancestor_at_block(
    pool: &PgPool,
    chain_id: &str,
    descendant_hash: &str,
    ancestor_hash: &str,
    ancestor_block_number: i64,
) -> Result<bool> {
    chain_lineage_contains_ancestor_with_floor(
        pool,
        chain_id,
        descendant_hash,
        ancestor_hash,
        Some(ancestor_block_number),
    )
    .await
}

pub(crate) async fn chain_lineage_contains_ancestor_internal<'e, E>(
    executor: E,
    chain_id: &str,
    descendant_hash: &str,
    ancestor_hash: &str,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    chain_lineage_contains_ancestor_with_floor(
        executor,
        chain_id,
        descendant_hash,
        ancestor_hash,
        None,
    )
    .await
}

async fn chain_lineage_contains_ancestor_with_floor<'e, E>(
    executor: E,
    chain_id: &str,
    descendant_hash: &str,
    ancestor_hash: &str,
    known_ancestor_block_number: Option<i64>,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let contains = sqlx::query_scalar::<_, bool>(
        r#"
        WITH RECURSIVE ancestor AS (
            SELECT block_number
            FROM bigname_phase.chain_lineage
            WHERE chain_id = $1
              AND block_hash = $3
              AND ($4::BIGINT IS NULL OR block_number = $4)
              AND canonicality_state <> 'orphaned'::bigname_phase.canonicality_state
        ),
        lineage_path AS (
            SELECT
                descendant.chain_id,
                descendant.block_hash,
                descendant.parent_hash,
                descendant.block_number,
                0::BIGINT AS depth,
                ancestor.block_number AS floor_block_number,
                descendant.block_number - ancestor.block_number AS max_depth
            FROM bigname_phase.chain_lineage AS descendant
            CROSS JOIN ancestor
            WHERE descendant.chain_id = $1
              AND descendant.block_hash = $2
              AND descendant.block_number >= ancestor.block_number
              AND descendant.canonicality_state <> 'orphaned'::bigname_phase.canonicality_state

            UNION ALL

            SELECT
                parent.chain_id,
                parent.block_hash,
                parent.parent_hash,
                parent.block_number,
                lineage_path.depth + 1,
                lineage_path.floor_block_number,
                lineage_path.max_depth
            FROM bigname_phase.chain_lineage AS parent
            JOIN lineage_path
              ON parent.chain_id = lineage_path.chain_id
             AND parent.block_hash = lineage_path.parent_hash
            WHERE lineage_path.block_hash <> $3
              AND lineage_path.block_number > lineage_path.floor_block_number
              AND lineage_path.depth < lineage_path.max_depth
              AND parent.block_number >= lineage_path.floor_block_number
              AND parent.block_number < lineage_path.block_number
              AND parent.canonicality_state <> 'orphaned'::bigname_phase.canonicality_state
        )
        SELECT EXISTS (
            SELECT 1
            FROM lineage_path
            WHERE block_hash = $3
              AND block_number = floor_block_number
        )
        "#,
    )
    .bind(chain_id)
    .bind(descendant_hash)
    .bind(ancestor_hash)
    .bind(known_ancestor_block_number)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to prove lineage ancestry for chain {chain_id} descendant {descendant_hash} ancestor {ancestor_hash}"
        )
    })?;

    Ok(contains)
}

/// Check whether one stored `(chain_id, block_number, block_hash)` is eligible
/// as an older canonical ancestor of a selected canonical descendant block.
///
/// This intentionally avoids walking parent links. `chain_lineage` is
/// append-only, and reorg repair flips whole losing branches to `orphaned`.
/// If the selected block is canonical-marked, the candidate is canonical-marked,
/// and the candidate is the unique canonical/safe/finalized row at that height,
/// both rows are on the same canonical chain and block-number ordering implies
/// ancestry. During a mid-reorg window where two rows at the candidate height
/// are still canonical-marked, uniqueness fails and the caller skips the
/// candidate conservatively.
pub async fn chain_lineage_contains_canonical_ancestor_position<'e, E>(
    executor: E,
    chain_id: &str,
    descendant_hash: &str,
    descendant_block_number: i64,
    ancestor_block_number: i64,
    ancestor_hash: &str,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let contains = sqlx::query_scalar::<_, bool>(
        r#"
        WITH canonical_at_candidate_height AS (
            SELECT block_hash
            FROM bigname_phase.chain_lineage
            WHERE chain_id = $1
              AND block_number = $4
              AND canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
            LIMIT 2
        )
        SELECT
            EXISTS (
                SELECT 1
                FROM bigname_phase.chain_lineage
                WHERE chain_id = $1
                  AND block_hash = $2
                  AND block_number = $3
                  AND canonicality_state IN (
                      'canonical'::bigname_phase.canonicality_state,
                      'safe'::bigname_phase.canonicality_state,
                      'finalized'::bigname_phase.canonicality_state
                  )
            )
            AND (
                SELECT COUNT(*) = 1
                FROM canonical_at_candidate_height
            )
            AND EXISTS (
                SELECT 1
                FROM canonical_at_candidate_height
                WHERE block_hash = $5
            )
        "#,
    )
    .bind(chain_id)
    .bind(descendant_hash)
    .bind(descendant_block_number)
    .bind(ancestor_block_number)
    .bind(ancestor_hash)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to check canonical lineage uniqueness for chain {chain_id} descendant {descendant_hash} ancestor {ancestor_hash} at block {ancestor_block_number}"
        )
    })?;

    Ok(contains)
}

pub async fn load_chain_lineage_canonical_child_path(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    from_number: i64,
    max_blocks: usize,
) -> Result<Vec<ChainLineageBlock>> {
    let mut path = Vec::with_capacity(max_blocks);
    let mut cursor_hash = from_hash.to_owned();
    let mut cursor_number = from_number;

    for _ in 0..max_blocks {
        let next_number = cursor_number
            .checked_add(1)
            .context("stored lineage child block number overflowed while walking path")?;
        let rows = sqlx::query(
            r#"
            SELECT
                lineage.chain_id,
                lineage.block_hash,
                lineage.parent_hash,
                lineage.block_number,
                lineage.block_timestamp,
                audit.logs_bloom,
                audit.transactions_root,
                audit.receipts_root,
                audit.state_root,
                lineage.canonicality_state::TEXT AS canonicality_state
            FROM bigname_phase.chain_lineage AS lineage
            LEFT JOIN chain_header_audit AS audit
              ON audit.chain_id = lineage.chain_id
             AND audit.block_hash = lineage.block_hash
            WHERE lineage.chain_id = $1
              AND lineage.parent_hash = $2
              AND lineage.block_number = $3
              AND lineage.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
            ORDER BY lineage.block_hash
            LIMIT 2
            "#,
        )
        .bind(chain_id)
        .bind(&cursor_hash)
        .bind(next_number)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load stored canonical lineage child for chain {chain_id} parent {cursor_hash} at block {next_number}"
            )
        })?;

        if rows.is_empty() {
            break;
        }
        if rows.len() > 1 {
            break;
        }

        let block = decode_lineage_block(rows.into_iter().next().expect("checked above"))?;
        cursor_hash = block.block_hash.clone();
        cursor_number = block.block_number;
        path.push(block);
    }

    Ok(path)
}
