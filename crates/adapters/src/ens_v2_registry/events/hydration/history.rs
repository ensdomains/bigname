use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::Uuid};

use super::{
    LinkedTransferState, TransferHydrationKey, TransferRequest, linked_transfer_state_from_rows,
    registry_event_topics,
};
use crate::{
    ens_v2_registry::constants::{
        ABI_EVENT_LABEL_REGISTERED_SIGNATURE, ABI_EVENT_LABEL_RESERVED_SIGNATURE,
        ABI_EVENT_LABEL_UNREGISTERED_SIGNATURE, ABI_EVENT_TOKEN_REGENERATED_SIGNATURE,
        ABI_EVENT_TOKEN_RESOURCE_SIGNATURE,
    },
    evm_abi::keccak_signature_hex,
};

const POSTGRES_BIND_PARAMETER_LIMIT: usize = 65_535;
const TRANSFER_REQUEST_BIND_COUNT: usize = 8;
const TRANSFER_QUERY_FIXED_BIND_COUNT: usize = 1;
const TRANSFER_REQUEST_CHUNK_SIZE: usize = 8_000;
const REGISTRY_AUTHORITY_REQUEST_BIND_COUNT: usize = 2;
const REGISTRY_AUTHORITY_REQUEST_CHUNK_SIZE: usize = 30_000;

mod subregistry;

pub(super) use subregistry::load_subregistry_target_rows;
use subregistry::{RegistryAuthorityKey, load_registry_suffixes};

pub(super) async fn load_linked_transfer_states(
    pool: &PgPool,
    requests: &[TransferRequest],
) -> Result<HashMap<TransferHydrationKey, LinkedTransferState>> {
    let unique_requests = unique_transfer_requests(requests);
    if unique_requests.is_empty() {
        return Ok(HashMap::new());
    }

    load_unique_linked_transfer_states(pool, unique_requests).await
}

fn unique_transfer_requests(
    requests: &[TransferRequest],
) -> Vec<(TransferHydrationKey, &TransferRequest)> {
    let mut seen = HashSet::new();
    requests
        .iter()
        .filter_map(|request| {
            let key = TransferHydrationKey::from(request);
            seen.insert(key.clone()).then_some((key, request))
        })
        .collect()
}

async fn load_declared_registry_authorities(
    pool: &PgPool,
    authorities: &[RegistryAuthorityKey],
) -> Result<HashSet<RegistryAuthorityKey>> {
    let mut declared = HashSet::new();
    for authorities in authorities.chunks(REGISTRY_AUTHORITY_REQUEST_CHUNK_SIZE) {
        debug_assert!(
            registry_authority_query_bind_count(authorities.len()) < POSTGRES_BIND_PARAMETER_LIMIT
        );
        let mut query = build_declared_registry_authorities_query(authorities);
        for (source_manifest_id, registry_contract_instance_id, is_declared) in query
            .build_query_as::<(i64, Uuid, bool)>()
            .fetch_all(pool)
            .await
            .context("failed to classify batched ENSv2 transfer registry emitters")?
        {
            if is_declared {
                declared.insert(RegistryAuthorityKey {
                    source_manifest_id,
                    registry_contract_instance_id,
                });
            }
        }
    }
    Ok(declared)
}

const fn registry_authority_query_bind_count(request_count: usize) -> usize {
    request_count * REGISTRY_AUTHORITY_REQUEST_BIND_COUNT
}

fn build_declared_registry_authorities_query(
    authorities: &[RegistryAuthorityKey],
) -> QueryBuilder<'_, Postgres> {
    let mut query = QueryBuilder::<Postgres>::new(
        "WITH requested (source_manifest_id, registry_contract_instance_id) AS (",
    );
    query.push_values(authorities, |mut row, authority| {
        row.push_bind(authority.source_manifest_id)
            .push_bind(authority.registry_contract_instance_id);
    });
    query.push(
        r#") SELECT requested.source_manifest_id,
                   requested.registry_contract_instance_id,
                   EXISTS (
                       SELECT 1 FROM manifest_contract_instances manifest
                       WHERE manifest.manifest_id = requested.source_manifest_id
                         AND manifest.contract_instance_id =
                             requested.registry_contract_instance_id
                   ) AS declared
            FROM requested"#,
    );
    query
}

async fn load_unique_linked_transfer_states(
    pool: &PgPool,
    unique_requests: Vec<(TransferHydrationKey, &TransferRequest)>,
) -> Result<HashMap<TransferHydrationKey, LinkedTransferState>> {
    let predecessor_topics = [
        ABI_EVENT_LABEL_REGISTERED_SIGNATURE,
        ABI_EVENT_LABEL_RESERVED_SIGNATURE,
        ABI_EVENT_LABEL_UNREGISTERED_SIGNATURE,
        ABI_EVENT_TOKEN_RESOURCE_SIGNATURE,
        ABI_EVENT_TOKEN_REGENERATED_SIGNATURE,
    ]
    .into_iter()
    .map(keccak_signature_hex)
    .collect::<Vec<_>>();
    let suffixes = load_registry_suffixes(pool, &unique_requests).await?;
    let event_topics = registry_event_topics();
    let mut hydrated = HashMap::with_capacity(unique_requests.len());
    for unique_requests in unique_requests.chunks(TRANSFER_REQUEST_CHUNK_SIZE) {
        debug_assert!(
            transfer_query_bind_count(unique_requests.len()) < POSTGRES_BIND_PARAMETER_LIMIT
        );
        let mut query = build_transfer_predecessor_query(unique_requests, &predecessor_topics);
        let mut rows_by_request = HashMap::<usize, Vec<PgRow>>::new();
        for row in query
            .build()
            .fetch_all(pool)
            .await
            .context("failed to load selected-path ENSv2 transfer predecessors")?
        {
            let request_index = usize::try_from(row.try_get::<i64, _>("request_index")?)
                .context("negative ENSv2 transfer hydration request index")?;
            rows_by_request.entry(request_index).or_default().push(row);
        }

        for (request_index, (key, request)) in unique_requests.iter().enumerate() {
            let suffix = suffixes
                .get(key)
                .context("batched ENSv2 transfer registry suffix is absent")?;
            let state = linked_transfer_state_from_rows(
                request,
                &event_topics,
                rows_by_request.remove(&request_index).unwrap_or_default(),
                suffix,
            )?;
            let previous = hydrated.insert(key.clone(), state);
            debug_assert!(previous.is_none(), "transfer hydration key was not unique");
        }
    }
    Ok(hydrated)
}

const fn transfer_query_bind_count(request_count: usize) -> usize {
    request_count * TRANSFER_REQUEST_BIND_COUNT + TRANSFER_QUERY_FIXED_BIND_COUNT
}

fn build_transfer_predecessor_query<'args>(
    unique_requests: &'args [(TransferHydrationKey, &'args TransferRequest)],
    predecessor_topics: &'args [String],
) -> QueryBuilder<'args, Postgres> {
    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        WITH RECURSIVE requested (
            request_index,
            chain_id,
            registry_address,
            token_id,
            block_number,
            block_hash,
            transaction_index,
            log_index
        ) AS (
        "#,
    );
    query.push_values(
        unique_requests.iter().enumerate(),
        |mut row, (request_index, (_, request))| {
            row.push_bind(i64::try_from(request_index).expect("request index must fit i64"))
                .push_bind(&request.chain_id)
                .push_bind(&request.registry_address)
                .push_bind(&request.token_id)
                .push_bind(request.block_number)
                .push_bind(&request.block_hash)
                .push_bind(request.transaction_index)
                .push_bind(request.log_index);
        },
    );
    query.push(
        r#"
        ), registry_scopes AS (
            SELECT
                chain_id,
                registry_address,
                MAX(block_number)::BIGINT AS max_block_number
            FROM requested
            GROUP BY chain_id, registry_address
        ), candidate_logs AS MATERIALIZED (
            SELECT
                raw.raw_log_id,
                raw.chain_id,
                raw.block_hash,
                raw.block_number,
                lineage.block_timestamp,
                raw.transaction_hash,
                raw.transaction_index,
                raw.log_index,
                lower(raw.emitting_address) AS registry_address,
                raw.topics,
                raw.data,
                raw.canonicality_state AS raw_canonicality_state,
                lineage.canonicality_state AS lineage_canonicality_state,
                left(lower(raw.topics[2]), 58) AS token_identity
            FROM registry_scopes scope
            JOIN raw_logs raw
              ON raw.chain_id = scope.chain_id
             AND lower(raw.emitting_address) = scope.registry_address
             AND raw.block_number <= scope.max_block_number
            JOIN chain_lineage lineage
              ON lineage.chain_id = raw.chain_id
             AND lineage.block_hash = raw.block_hash
             AND lineage.block_number = raw.block_number
            WHERE raw.topics[1] = ANY(
        "#,
    );
    query.push_bind(predecessor_topics);
    query.push(
        r#"::TEXT[])
              AND raw.canonicality_state <> 'orphaned'::canonicality_state
              AND lineage.canonicality_state <> 'orphaned'::canonicality_state
        ), selected_tail AS (
            SELECT
                requested.request_index,
                lineage.chain_id,
                lineage.block_hash,
                lineage.block_number,
                lineage.parent_hash,
                lineage.canonicality_state
            FROM requested
            JOIN chain_lineage lineage
              ON lineage.chain_id = requested.chain_id
             AND lineage.block_number = requested.block_number
             AND lineage.block_hash = requested.block_hash
             AND lineage.canonicality_state <> 'orphaned'::canonicality_state

            UNION ALL

            SELECT
                child.request_index,
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
        SELECT
            requested.request_index,
            raw.chain_id,
            raw.block_hash,
            raw.block_number,
            raw.block_timestamp,
            raw.transaction_hash,
            raw.transaction_index,
            raw.log_index,
            raw.registry_address AS emitting_address,
            raw.topics,
            raw.data,
            raw.raw_canonicality_state::TEXT AS canonicality_state
        FROM requested
        JOIN candidate_logs raw
          ON raw.chain_id = requested.chain_id
         AND raw.registry_address = requested.registry_address
         AND raw.token_identity = left(lower(requested.token_id), 58)
        LEFT JOIN selected_tail
          ON selected_tail.request_index = requested.request_index
         AND selected_tail.block_hash = raw.block_hash
         AND selected_tail.block_number = raw.block_number
        WHERE (
              selected_tail.block_hash IS NOT NULL
              OR (
                  raw.raw_canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND raw.lineage_canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND raw.block_number <= COALESCE((
                      SELECT MAX(tail.block_number)
                      FROM selected_tail tail
                      WHERE tail.request_index = requested.request_index
                        AND tail.canonicality_state <> 'observed'::canonicality_state
                  ), -1)
              )
          )
          AND (raw.block_number, raw.transaction_index, raw.log_index)
              < (
                  requested.block_number,
                  requested.transaction_index,
                  requested.log_index
              )
        ORDER BY
            requested.request_index,
            raw.block_number,
            raw.transaction_index,
            raw.log_index,
            raw.raw_log_id
        "#,
    );
    query
}

#[cfg(test)]
mod tests {
    use sqlx::types::Uuid;

    use super::*;

    fn test_transfer_request(index: usize) -> TransferRequest {
        TransferRequest {
            event_index: index,
            chain_id: "ethereum-sepolia".to_owned(),
            namespace: "ens".to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            source_manifest_id: 7,
            manifest_version: 1,
            registry_contract_instance_id: Uuid::from_u128(9),
            registry_address: "0x0000000000000000000000000000000000000001".to_owned(),
            token_id: format!("0x{:064x}", index + 1),
            block_number: 12,
            block_hash: format!("0x{:064x}", 12),
            transaction_index: 1,
            log_index: 2,
        }
    }

    #[test]
    fn transfer_hydration_deduplicates_event_positions_with_the_same_history_query() {
        let request = test_transfer_request(3);
        let mut duplicate = request.clone();
        duplicate.event_index = 4;

        assert_eq!(unique_transfer_requests(&[request, duplicate]).len(), 1);
    }

    #[test]
    fn transfer_hydration_chunks_queries_below_the_postgres_bind_limit() {
        let former_single_query_limit = (POSTGRES_BIND_PARAMETER_LIMIT
            - TRANSFER_QUERY_FIXED_BIND_COUNT)
            / TRANSFER_REQUEST_BIND_COUNT;
        let requests = (0..=former_single_query_limit)
            .map(test_transfer_request)
            .collect::<Vec<_>>();
        let unique_requests = unique_transfer_requests(&requests);
        let predecessor_topics = vec!["topic".to_owned()];
        let chunks = unique_requests
            .chunks(TRANSFER_REQUEST_CHUNK_SIZE)
            .collect::<Vec<_>>();

        assert_eq!(unique_requests.len(), former_single_query_limit + 1);
        assert_eq!(
            chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
            vec![8_000, 192]
        );
        for chunk in chunks {
            let query = build_transfer_predecessor_query(chunk, &predecessor_topics);
            let bind_count = transfer_query_bind_count(chunk.len());
            assert!(bind_count < POSTGRES_BIND_PARAMETER_LIMIT);
            assert!(query.sql().contains(&format!("${bind_count}")));
            assert!(!query.sql().contains(&format!("${}", bind_count + 1)));
        }
    }

    #[test]
    fn registry_authority_queries_chunk_below_the_postgres_bind_limit() {
        let former_single_query_limit =
            POSTGRES_BIND_PARAMETER_LIMIT / REGISTRY_AUTHORITY_REQUEST_BIND_COUNT;
        let authorities = (0..=former_single_query_limit)
            .map(|index| RegistryAuthorityKey {
                source_manifest_id: i64::try_from(index).expect("test index must fit i64"),
                registry_contract_instance_id: Uuid::from_u128(
                    u128::try_from(index + 1).expect("test index must fit u128"),
                ),
            })
            .collect::<Vec<_>>();
        let chunks = authorities
            .chunks(REGISTRY_AUTHORITY_REQUEST_CHUNK_SIZE)
            .collect::<Vec<_>>();

        assert_eq!(authorities.len(), former_single_query_limit + 1);
        assert_eq!(
            chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
            vec![30_000, 2_768]
        );
        for chunk in chunks {
            let query = build_declared_registry_authorities_query(chunk);
            let bind_count = registry_authority_query_bind_count(chunk.len());
            assert!(bind_count < POSTGRES_BIND_PARAMETER_LIMIT);
            assert!(query.sql().contains(&format!("${bind_count}")));
            assert!(!query.sql().contains(&format!("${}", bind_count + 1)));
        }
    }
}
