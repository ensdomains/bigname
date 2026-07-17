//! Companion-row gate for stored-lineage promotion, scoped to the logs the
//! backfill write side actually observes companions for: family-selected logs
//! (emitter watched under a source family, block inside that watched entry's
//! active window, topic0 in the family's current manifest ABI topic0 set).
//! Sibling-retained foreign-topic logs from watched addresses land in
//! `raw_logs` via raw-completeness retention but never receive code
//! observations, so they must not demand companions.

use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::ChainLineageBlock;
use sqlx::Row;

use super::{path_end_number, path_start_number};

/// Every family-selected stored canonical log inside the path must carry its
/// raw code-hash, transaction, and receipt companions. Candidate logs are
/// bounded by the path's block hashes and the manifest topic0 sets (both
/// small binds); watchedness is resolved server-side by narrowing the watched
/// address table to the candidates' emitters in one pass and joining through
/// the manifest/discovery watched selection, because the watched surface itself
/// is millions of (family, address, window) rows and must never be
/// materialized client-side.
pub(super) async fn ensure_selected_logs_have_raw_companions(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
) -> std::result::Result<(), String> {
    if current_topic0s_by_family.is_empty() {
        return Ok(());
    }
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let mut topic_source_families = Vec::new();
    let mut topic0s = Vec::new();
    for (source_family, family_topic0s) in current_topic0s_by_family {
        for topic0 in family_topic0s {
            topic_source_families.push(source_family.clone());
            topic0s.push(topic0.to_ascii_lowercase());
        }
    }

    let row = sqlx::query(
        r#"
        WITH family_topic0s AS (
            SELECT source_family, topic0
            FROM UNNEST($3::TEXT[], $4::TEXT[]) AS family_topic(source_family, topic0)
        ),
        candidate_logs AS MATERIALIZED (
            SELECT
                raw_logs.block_hash,
                raw_logs.block_number,
                LOWER(raw_logs.emitting_address) AS emitting_address,
                family_topic0s.source_family,
                raw_logs.transaction_hash,
                raw_logs.transaction_index
            FROM raw_logs
            JOIN family_topic0s
              ON family_topic0s.topic0 = LOWER(raw_logs.topics[1])
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_hash = ANY($2::TEXT[])
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ),
        candidate_watched_addresses AS MATERIALIZED (
            SELECT DISTINCT
                cia.contract_instance_id,
                LOWER(cia.address) AS address,
                cia.active_from_block_number,
                cia.active_to_block_number
            FROM contract_instance_addresses cia
            WHERE cia.chain_id = $1
              AND cia.deactivated_at IS NULL
              AND LOWER(cia.address) IN (
                  SELECT DISTINCT emitting_address FROM candidate_logs
              )
            UNION
            SELECT DISTINCT
                cia.contract_instance_id,
                LOWER(cia.address) AS address,
                cia.active_from_block_number,
                cia.active_to_block_number
            FROM contract_instance_addresses cia
            WHERE cia.chain_id = $1
              AND cia.deactivated_at IS NOT NULL
              AND cia.active_to_block_number IS NOT NULL
              AND LOWER(cia.address) IN (
                  SELECT DISTINCT emitting_address FROM candidate_logs
              )
        ),
        selected_logs AS MATERIALIZED (
            SELECT DISTINCT
                selected.block_hash,
                selected.emitting_address,
                selected.transaction_hash,
                selected.transaction_index
            FROM (
                SELECT
                    candidate_logs.block_hash,
                    candidate_logs.emitting_address,
                    candidate_logs.transaction_hash,
                    candidate_logs.transaction_index
                FROM candidate_logs
                JOIN candidate_watched_addresses cia
                  ON cia.address = candidate_logs.emitting_address
                 AND (
                     cia.active_from_block_number IS NULL
                     OR cia.active_from_block_number <= candidate_logs.block_number
                 )
                 AND (
                     cia.active_to_block_number IS NULL
                     OR cia.active_to_block_number >= candidate_logs.block_number
                 )
                JOIN LATERAL (
                    SELECT 1
                    FROM manifest_contract_instances mci
                    JOIN manifest_versions mv
                      ON mv.manifest_id = mci.manifest_id
                     AND mv.chain = $1
                     AND mv.rollout_status = 'active'
                     AND mv.source_family = candidate_logs.source_family
                    LEFT JOIN LATERAL (
                        SELECT (entry ->> 'start_block')::BIGINT AS start_block
                        FROM jsonb_array_elements(
                            CASE
                                WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                                ELSE mv.manifest_payload -> 'contracts'
                            END
                        ) entry
                        WHERE (
                                mci.declaration_kind = 'root'
                                AND entry ->> 'name' = mci.declaration_name
                            )
                           OR (
                                mci.declaration_kind = 'contract'
                                AND entry ->> 'role' = mci.declaration_name
                            )
                        ORDER BY start_block NULLS LAST
                        LIMIT 1
                    ) manifest_range ON TRUE
                    WHERE mci.contract_instance_id = cia.contract_instance_id
                      AND (
                          manifest_range.start_block IS NULL
                          OR manifest_range.start_block <= candidate_logs.block_number
                      )
                    LIMIT 1
                ) manifest_admitted ON TRUE
                UNION ALL
                SELECT
                    candidate_logs.block_hash,
                    candidate_logs.emitting_address,
                    candidate_logs.transaction_hash,
                    candidate_logs.transaction_index
                FROM candidate_logs
                JOIN candidate_watched_addresses cia
                  ON cia.address = candidate_logs.emitting_address
                 AND (
                     cia.active_from_block_number IS NULL
                     OR cia.active_from_block_number <= candidate_logs.block_number
                 )
                 AND (
                     cia.active_to_block_number IS NULL
                     OR cia.active_to_block_number >= candidate_logs.block_number
                 )
                JOIN LATERAL (
                    SELECT 1
                    FROM discovery_edges de
                    JOIN manifest_versions mv
                      ON mv.manifest_id = de.source_manifest_id
                     AND mv.rollout_status = 'active'
                    LEFT JOIN manifest_versions target_mv
                      ON target_mv.rollout_status = 'active'
                     AND target_mv.namespace = mv.namespace
                     AND target_mv.chain = de.chain_id
                     AND target_mv.deployment_epoch = mv.deployment_epoch
                     AND target_mv.source_family = CASE
                         WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
                             THEN 'ens_v1_resolver_l1'
                         WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                             THEN 'ens_v2_resolver_l1'
                         WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                             THEN 'basenames_base_resolver'
                         ELSE NULL
                     END
                    WHERE de.chain_id = $1
                      AND de.to_contract_instance_id = cia.contract_instance_id
                      AND (
                          de.deactivated_at IS NULL
                          OR de.active_to_block_number IS NOT NULL
                      )
                      AND de.edge_kind <> 'migration'
                      AND (
                          de.active_from_block_number IS NULL
                          OR de.active_from_block_number <= candidate_logs.block_number
                      )
                      AND (
                          de.active_to_block_number IS NULL
                          OR de.active_to_block_number >= candidate_logs.block_number
                      )
                      AND COALESCE(target_mv.source_family, mv.source_family)
                          = candidate_logs.source_family
                      AND (
                          de.edge_kind <> 'resolver'
                          OR mv.source_family NOT IN (
                              'ens_v1_registry_l1',
                              'ens_v2_registry_l1',
                              'basenames_base_registry'
                          )
                          OR target_mv.manifest_id IS NOT NULL
                      )
                    LIMIT 1
                ) discovery_admitted ON TRUE
            ) selected
        ),
        selected_log_emitters AS (
            SELECT DISTINCT block_hash, emitting_address
            FROM selected_logs
        ),
        selected_log_transactions AS (
            SELECT DISTINCT block_hash, transaction_hash, transaction_index
            FROM selected_logs
        )
        SELECT
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_emitters
                LEFT JOIN LATERAL (
                    SELECT 1 AS present
                    FROM raw_code_hashes
                    WHERE raw_code_hashes.chain_id = $1
                      AND raw_code_hashes.block_hash = selected_log_emitters.block_hash
                      AND LOWER(raw_code_hashes.contract_address) = selected_log_emitters.emitting_address
                      AND raw_code_hashes.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                    LIMIT 1
                ) code_companion ON TRUE
                WHERE code_companion.present IS NULL
            ) AS selected_log_emitter_missing_code_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                LEFT JOIN LATERAL (
                    SELECT 1 AS present
                    FROM raw_transactions
                    WHERE raw_transactions.chain_id = $1
                      AND raw_transactions.block_hash = selected_log_transactions.block_hash
                      AND raw_transactions.transaction_hash = selected_log_transactions.transaction_hash
                      AND raw_transactions.transaction_index = selected_log_transactions.transaction_index
                      AND raw_transactions.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                    LIMIT 1
                ) transaction_companion ON TRUE
                WHERE transaction_companion.present IS NULL
            ) AS selected_log_transaction_missing_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                LEFT JOIN LATERAL (
                    SELECT 1 AS present
                    FROM raw_receipts
                    WHERE raw_receipts.chain_id = $1
                      AND raw_receipts.block_hash = selected_log_transactions.block_hash
                      AND raw_receipts.transaction_hash = selected_log_transactions.transaction_hash
                      AND raw_receipts.transaction_index = selected_log_transactions.transaction_index
                      AND raw_receipts.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                    LIMIT 1
                ) receipt_companion ON TRUE
                WHERE receipt_companion.present IS NULL
            ) AS selected_log_receipt_missing_count
        "#,
    )
    .bind(chain)
    .bind(&block_hashes)
    .bind(&topic_source_families)
    .bind(&topic0s)
    .fetch_one(pool)
    .await
    .map_err(|error| error.to_string())?;

    let missing_code_hashes: i64 = row
        .try_get("selected_log_emitter_missing_code_count")
        .map_err(|error| error.to_string())?;
    let missing_transactions: i64 = row
        .try_get("selected_log_transaction_missing_count")
        .map_err(|error| error.to_string())?;
    let missing_receipts: i64 = row
        .try_get("selected_log_receipt_missing_count")
        .map_err(|error| error.to_string())?;
    if missing_code_hashes != 0 || missing_transactions != 0 || missing_receipts != 0 {
        return Err(format!(
            "stored lineage selected logs over {}..={} are missing raw code/transaction/receipt companion rows (missing code: {missing_code_hashes}, transactions: {missing_transactions}, receipts: {missing_receipts}); rerun hash-pinned backfill for the selected range before retrying",
            path_start_number(path),
            path_end_number(path)
        ));
    }

    Ok(())
}
