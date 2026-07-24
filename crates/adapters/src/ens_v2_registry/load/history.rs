use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use bigname_storage::sql_row;
use sqlx::PgPool;

use super::super::{
    emitters::emitter_for_block_and_scope,
    types::{ActiveEmitter, RegistryRawLogRow},
    util::normalize_address,
};
use crate::{
    checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress},
    startup_progress::{
        RawLogPagePosition, STARTUP_ADAPTER_PROGRESS_PAGE_ROWS,
        STARTUP_ADAPTER_PROGRESS_PAGE_ROWS_I64,
    },
};

pub(in crate::ens_v2_registry) async fn load_registry_raw_log_prefix(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    before: &RegistryRawLogRow,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<Vec<RegistryRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }
    ensure_selected_path_reaches_stable_boundary(pool, chain, before).await?;

    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let addresses = emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let from_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let to_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();
    let paged = progress.is_some();
    let page_limit = if paged {
        STARTUP_ADAPTER_PROGRESS_PAGE_ROWS_I64
    } else {
        i64::MAX
    };
    let mut start_after = None::<RawLogPagePosition>;
    let mut output = Vec::new();

    loop {
        let rows = sqlx::query(
            r#"
            WITH RECURSIVE selected_tail AS (
                SELECT
                    chain_id,
                    block_hash,
                    block_number,
                    parent_hash,
                    canonicality_state
                FROM chain_lineage
                WHERE chain_id = $1
                  AND block_number = $2
                  AND block_hash = $3
                  AND canonicality_state <> 'orphaned'::canonicality_state

                UNION ALL

                SELECT
                    parent.chain_id,
                    parent.block_hash,
                    parent.block_number,
                    parent.parent_hash,
                    parent.canonicality_state
                FROM selected_tail child
                JOIN chain_lineage parent
                  ON parent.chain_id = child.chain_id
                 AND parent.block_hash = child.parent_hash
                 AND parent.block_number = child.block_number - 1
                 AND parent.canonicality_state <> 'orphaned'::canonicality_state
                WHERE child.canonicality_state = 'observed'::canonicality_state
            ),
            selected_boundary AS (
                SELECT COALESCE(
                    MAX(block_number) FILTER (
                        WHERE canonicality_state <> 'observed'::canonicality_state
                    ),
                    -1
                ) AS stable_through_block
                FROM selected_tail
            )
            SELECT
                raw.chain_id,
                raw.block_hash,
                raw.block_number,
                lineage.block_timestamp,
                raw.transaction_hash,
                raw.transaction_index,
                raw.log_index,
                raw.emitting_address,
                raw.topics,
                raw.data,
                raw.canonicality_state::TEXT AS canonicality_state
            FROM raw_logs raw
            JOIN chain_lineage lineage
              ON lineage.chain_id = raw.chain_id
             AND lineage.block_hash = raw.block_hash
             AND lineage.block_number = raw.block_number
            CROSS JOIN selected_boundary
            WHERE raw.chain_id = $1
              AND lower(raw.emitting_address) = ANY($4::TEXT[])
              AND EXISTS (
                  SELECT 1
                  FROM unnest($4::TEXT[], $5::BIGINT[], $6::BIGINT[]) AS watched(
                      address,
                      active_from_block,
                      active_to_block
                  )
                  WHERE watched.address = lower(raw.emitting_address)
                    AND raw.block_number BETWEEN watched.active_from_block
                        AND watched.active_to_block
              )
              AND raw.canonicality_state <> 'orphaned'::canonicality_state
              AND lineage.canonicality_state <> 'orphaned'::canonicality_state
              AND (
                  EXISTS (
                      SELECT 1
                      FROM selected_tail
                      WHERE selected_tail.block_number = raw.block_number
                        AND selected_tail.block_hash = raw.block_hash
                  )
                  OR (
                      raw.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                      AND lineage.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                      AND raw.block_number <= selected_boundary.stable_through_block
                  )
              )
              AND (
                  raw.block_number,
                  raw.transaction_index,
                  raw.log_index
              ) < ($2, $7, $8)
              AND (
                  $9::BIGINT IS NULL
                  OR (
                      raw.block_number,
                      raw.transaction_index,
                      raw.log_index,
                      lower(raw.emitting_address),
                      raw.block_hash
                  ) > ($9, $10, $11, $12, $13)
              )
            ORDER BY
                raw.block_number,
                raw.transaction_index,
                raw.log_index,
                lower(raw.emitting_address),
                raw.block_hash
            LIMIT $14
            "#,
        )
        .bind(chain)
        .bind(before.block_number)
        .bind(&before.block_hash)
        .bind(&addresses)
        .bind(&from_blocks)
        .bind(&to_blocks)
        .bind(before.transaction_index)
        .bind(before.log_index)
        .bind(start_after.as_ref().map(|position| position.block_number))
        .bind(
            start_after
                .as_ref()
                .map(|position| position.transaction_index),
        )
        .bind(start_after.as_ref().map(|position| position.log_index))
        .bind(
            start_after
                .as_ref()
                .map(|position| position.emitting_address.as_str()),
        )
        .bind(
            start_after
                .as_ref()
                .map(|position| position.block_hash.as_str()),
        )
        .bind(page_limit)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load retained ENSv2 registry history before block {} log {} on {chain}",
                before.block_number, before.log_index
            )
        })?;
        if rows.is_empty() {
            break;
        }

        let page_len = rows.len();
        let last_position =
            RawLogPagePosition::from_row(rows.last().expect("non-empty registry history page"))?;
        for row in rows {
            let emitting_address =
                normalize_address(&sql_row::get::<String>(&row, "emitting_address")?);
            let block_number = sql_row::get(&row, "block_number")?;
            let emitter = emitters_by_address
                .get(&emitting_address)
                .and_then(|emitters| emitter_for_block_and_scope(emitters, block_number, None))
                .with_context(|| {
                    format!(
                        "retained ENSv2 registry history has no emitter attribution for {emitting_address} at block {block_number}"
                    )
                })?;
            output.push(RegistryRawLogRow {
                chain_id: sql_row::get(&row, "chain_id")?,
                block_hash: sql_row::get(&row, "block_hash")?,
                block_number,
                block_timestamp: sql_row::get(&row, "block_timestamp")?,
                transaction_hash: sql_row::get(&row, "transaction_hash")?,
                transaction_index: sql_row::get(&row, "transaction_index")?,
                log_index: sql_row::get(&row, "log_index")?,
                emitting_address,
                topics: sql_row::get(&row, "topics")?,
                data: sql_row::get(&row, "data")?,
                canonicality_state: sql_row::get(&row, "canonicality_state")?,
                emitting_contract_instance_id: emitter.contract_instance_id,
                source_manifest_id: emitter.source_manifest_id,
                namespace: emitter.namespace.clone(),
                source_family: emitter.source_family.clone(),
                manifest_version: emitter.manifest_version,
                normalizer_version: emitter.normalizer_version.clone(),
            });
        }
        if paged {
            record_startup_adapter_progress(pool, progress).await?;
        }
        if !paged || page_len < STARTUP_ADAPTER_PROGRESS_PAGE_ROWS {
            break;
        }
        start_after = Some(last_position);
    }

    Ok(output)
}

async fn ensure_selected_path_reaches_stable_boundary(
    pool: &PgPool,
    chain: &str,
    before: &RegistryRawLogRow,
) -> Result<()> {
    let reaches_stable_boundary = sqlx::query_scalar::<_, bool>(
        r#"
        WITH RECURSIVE selected_tail AS (
            SELECT
                chain_id,
                block_hash,
                block_number,
                parent_hash,
                canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_number = $2
              AND block_hash = $3
              AND canonicality_state <> 'orphaned'::canonicality_state

            UNION ALL

            SELECT
                parent.chain_id,
                parent.block_hash,
                parent.block_number,
                parent.parent_hash,
                parent.canonicality_state
            FROM selected_tail child
            JOIN chain_lineage parent
              ON parent.chain_id = child.chain_id
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
             AND parent.canonicality_state <> 'orphaned'::canonicality_state
            WHERE child.canonicality_state = 'observed'::canonicality_state
        )
        SELECT EXISTS (
            SELECT 1
            FROM selected_tail
            WHERE canonicality_state <> 'observed'::canonicality_state
        )
        "#,
    )
    .bind(chain)
    .bind(before.block_number)
    .bind(&before.block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to validate selected ENSv2 ancestry before block {} log {} on {chain}",
            before.block_number, before.log_index
        )
    })?;
    ensure!(
        reaches_stable_boundary,
        "ENSv2 incremental prior-state reconstruction cannot prove selected-path ancestry from \
         block {} ({}) to a stable canonical boundary on {chain}",
        before.block_number,
        before.block_hash
    );
    Ok(())
}
