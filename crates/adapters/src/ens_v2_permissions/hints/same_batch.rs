use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{PermissionsRawLogRow, ResolverResourceHint};

pub(in crate::ens_v2_permissions) async fn load_same_batch_resolver_resource_hint(
    pool: &PgPool,
    raw_log: &PermissionsRawLogRow,
    candidates: &[ResolverResourceHint],
) -> Result<Option<ResolverResourceHint>> {
    let mut candidate_hashes = Vec::new();
    let mut candidate_block_numbers = Vec::new();
    let mut candidate_indexes = Vec::new();
    let mut same_block_hint = None;
    for (index, hint) in candidates.iter().enumerate() {
        let first_ref = &hint.first_ref;
        if first_ref.chain_id != raw_log.chain_id
            || !first_ref
                .emitting_address
                .eq_ignore_ascii_case(&raw_log.emitting_address)
            || first_ref.emitting_contract_instance_id != raw_log.emitting_contract_instance_id
            || first_ref.source_family != raw_log.source_family
            || (
                first_ref.block_number,
                first_ref.transaction_index,
                first_ref.log_index,
            ) >= (
                raw_log.block_number,
                raw_log.transaction_index,
                raw_log.log_index,
            )
        {
            continue;
        }

        if raw_log.block_number == first_ref.block_number
            && raw_log.block_hash == first_ref.block_hash
        {
            same_block_hint = Some(hint.clone());
            continue;
        }
        candidate_hashes.push(first_ref.block_hash.clone());
        candidate_block_numbers.push(first_ref.block_number);
        candidate_indexes.push(i64::try_from(index).context("too many same-batch resolver hints")?);
    }
    if same_block_hint.is_some() || candidate_hashes.is_empty() {
        return Ok(same_block_hint);
    }

    let selected_index = sqlx::query_scalar::<_, i64>(
        r#"
        WITH RECURSIVE candidates AS (
            SELECT *
            FROM unnest($4::TEXT[], $5::BIGINT[], $6::BIGINT[]) AS candidate(
                block_hash,
                block_number,
                candidate_index
            )
        ),
        candidate_floor AS (
            SELECT MIN(block_number) AS block_number
            FROM candidates
        ),
        selected_path AS (
            SELECT
                descendant.chain_id,
                descendant.block_hash,
                descendant.parent_hash,
                descendant.block_number,
                0::BIGINT AS depth,
                candidate_floor.block_number AS floor_block_number,
                descendant.block_number - candidate_floor.block_number AS max_depth
            FROM chain_lineage descendant
            CROSS JOIN candidate_floor
            WHERE descendant.chain_id = $1
              AND descendant.block_hash = $3
              AND descendant.block_number = $2
              AND descendant.block_number >= candidate_floor.block_number
              AND descendant.canonicality_state <> 'orphaned'::canonicality_state

            UNION ALL

            SELECT
                parent.chain_id,
                parent.block_hash,
                parent.parent_hash,
                parent.block_number,
                selected_path.depth + 1,
                selected_path.floor_block_number,
                selected_path.max_depth
            FROM chain_lineage parent
            JOIN selected_path
              ON parent.chain_id = selected_path.chain_id
             AND parent.block_hash = selected_path.parent_hash
            WHERE selected_path.block_number > selected_path.floor_block_number
              AND selected_path.depth < selected_path.max_depth
              AND parent.block_number >= selected_path.floor_block_number
              AND parent.block_number < selected_path.block_number
              AND parent.canonicality_state <> 'orphaned'::canonicality_state
        )
        SELECT candidate.candidate_index
        FROM candidates candidate
        JOIN selected_path
          ON selected_path.block_number = candidate.block_number
         AND selected_path.block_hash = candidate.block_hash
        ORDER BY candidate.candidate_index DESC
        LIMIT 1
        "#,
    )
    .bind(&raw_log.chain_id)
    .bind(raw_log.block_number)
    .bind(&raw_log.block_hash)
    .bind(&candidate_hashes)
    .bind(&candidate_block_numbers)
    .bind(&candidate_indexes)
    .fetch_optional(pool)
    .await
    .context("failed to select a same-batch ENSv2 resolver hint on the role-log ancestry")?;

    selected_index
        .map(|index| {
            usize::try_from(index)
                .ok()
                .and_then(|index| candidates.get(index))
                .cloned()
                .context("selected same-batch resolver hint index is out of range")
        })
        .transpose()
}
