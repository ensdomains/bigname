use std::collections::BTreeSet;

use bigname_manifests::{
    load_active_manifest_abi_events_by_chain_and_source_families,
    load_log_producing_source_families, load_required_watched_tuples,
};
use bigname_storage::ChainLineageBlock;
use sqlx::Row;

use super::{path_end_number, path_start_number};

/// Every selected canonical log must carry its raw code-hash, transaction, and
/// receipt companions. Address-scoped facts select every log from the exact
/// watched tuple; family-scope topic scans select only logs whose topic0 is in
/// that family's current manifest ABI. Same-transaction sibling logs remain
/// replay context and do not independently require an emitter code observation.
/// Historical active intervals still prevent a retired selected emitter from
/// escaping validation and avoid widening requirements beyond its watch life.
pub(super) async fn ensure_selected_logs_have_raw_companions(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
) -> std::result::Result<(), String> {
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let emitting_addresses = sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT LOWER(emitting_address)
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(&block_hashes)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    if emitting_addresses.is_empty() {
        return Ok(());
    }

    let emitting_addresses = emitting_addresses.into_iter().collect::<BTreeSet<_>>();
    let log_producing_source_families = load_log_producing_source_families(pool, chain)
        .await
        .map_err(|error| error.to_string())?;
    let required_intervals = load_required_watched_tuples(
        pool,
        chain,
        path_start_number(path),
        path_end_number(path),
        &log_producing_source_families,
    )
    .await
    .map_err(|error| error.to_string())?
    .into_iter()
    .filter(|tuple| emitting_addresses.contains(&tuple.address))
    .map(|tuple| {
        (
            tuple.source_family,
            tuple.address,
            tuple.required_from_block,
            tuple.required_to_block,
        )
    })
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect::<Vec<_>>();
    if required_intervals.is_empty() {
        return Ok(());
    }
    let selected_family_topics = load_active_manifest_abi_events_by_chain_and_source_families(
        pool,
        chain,
        &log_producing_source_families,
    )
    .await
    .map_err(|error| error.to_string())?
    .into_iter()
    .filter_map(|event| {
        event
            .topic0
            .map(|topic0| (event.source_family, topic0.to_ascii_lowercase()))
    })
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect::<Vec<_>>();
    let required_source_families = required_intervals
        .iter()
        .map(|(source_family, _, _, _)| source_family.clone())
        .collect::<Vec<_>>();
    let required_addresses = required_intervals
        .iter()
        .map(|(_, address, _, _)| address.clone())
        .collect::<Vec<_>>();
    let required_from_blocks = required_intervals
        .iter()
        .map(|(_, _, required_from, _)| *required_from)
        .collect::<Vec<_>>();
    let required_to_blocks = required_intervals
        .iter()
        .map(|(_, _, _, required_to)| *required_to)
        .collect::<Vec<_>>();
    let selected_topic_source_families = selected_family_topics
        .iter()
        .map(|(source_family, _)| source_family.clone())
        .collect::<Vec<_>>();
    let selected_topic0s = selected_family_topics
        .iter()
        .map(|(_, topic0)| topic0.clone())
        .collect::<Vec<_>>();

    let row = sqlx::query(
        r#"
        WITH required_watched_intervals AS (
            SELECT source_family, address, required_from_block, required_to_block
            FROM UNNEST($3::TEXT[], $4::TEXT[], $5::BIGINT[], $6::BIGINT[])
                AS required(
                    source_family,
                    address,
                    required_from_block,
                    required_to_block
                )
        ),
        selected_family_topics AS (
            SELECT source_family, topic0
            FROM UNNEST($7::TEXT[], $8::TEXT[])
                AS selected_topic(source_family, topic0)
        ),
        selected_logs AS (
            SELECT DISTINCT
                raw_logs.block_hash,
                raw_logs.transaction_hash,
                raw_logs.transaction_index,
                LOWER(raw_logs.emitting_address) AS emitting_address
            FROM raw_logs
            JOIN required_watched_intervals required
              ON required.address = LOWER(raw_logs.emitting_address)
             AND raw_logs.block_number BETWEEN required.required_from_block
                                           AND required.required_to_block
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_hash = ANY($2::TEXT[])
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND (
                  EXISTS (
                      SELECT 1
                      FROM backfill_coverage_facts fact
                      WHERE fact.chain_id = $1
                        AND fact.source_family = required.source_family
                        AND fact.scope = 'address'
                        AND fact.address = required.address
                        AND fact.covered_from_block <= raw_logs.block_number
                        AND fact.covered_to_block >= raw_logs.block_number
                  )
                  OR (
                      EXISTS (
                          SELECT 1
                          FROM backfill_coverage_facts fact
                          WHERE fact.chain_id = $1
                            AND fact.source_family = required.source_family
                            AND fact.scope = 'family'
                            AND fact.address IS NULL
                            AND fact.covered_from_block <= raw_logs.block_number
                            AND fact.covered_to_block >= raw_logs.block_number
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM selected_family_topics topic
                          WHERE topic.source_family = required.source_family
                            AND topic.topic0 = LOWER(raw_logs.topics[1])
                      )
                  )
              )
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
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_code_hashes
                    WHERE raw_code_hashes.chain_id = $1
                      AND raw_code_hashes.block_hash = selected_log_emitters.block_hash
                      AND LOWER(raw_code_hashes.contract_address) = selected_log_emitters.emitting_address
                      AND raw_code_hashes.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_emitter_missing_code_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                WHERE NOT EXISTS (
                    SELECT 1
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
                )
            ) AS selected_log_transaction_missing_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                WHERE NOT EXISTS (
                    SELECT 1
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
                )
            ) AS selected_log_receipt_missing_count
        "#,
    )
    .bind(chain)
    .bind(&block_hashes)
    .bind(&required_source_families)
    .bind(&required_addresses)
    .bind(&required_from_blocks)
    .bind(&required_to_blocks)
    .bind(&selected_topic_source_families)
    .bind(&selected_topic0s)
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
