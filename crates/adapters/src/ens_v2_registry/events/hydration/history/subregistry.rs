use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, types::Uuid};

use super::{
    super::{TargetRequest, TransferHydrationKey, TransferRequest},
    POSTGRES_BIND_PARAMETER_LIMIT, load_declared_registry_authorities,
};
use crate::ens_v2_registry::constants::SOURCE_FAMILY_ENS_V2_ROOT_L1;

const SUBREGISTRY_REQUEST_BIND_COUNT: usize = 6;
const SUBREGISTRY_REQUEST_CHUNK_SIZE: usize = 10_000;
const REGISTRY_SUFFIX_REQUEST_BIND_COUNT: usize = 5;
const REGISTRY_SUFFIX_REQUEST_CHUNK_SIZE: usize = 12_000;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct RegistryAuthorityKey {
    pub(super) source_manifest_id: i64,
    pub(super) registry_contract_instance_id: Uuid,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RegistrySuffixPosition {
    chain_id: String,
    namespace: String,
    source_family: String,
    authority: RegistryAuthorityKey,
    block_number: i64,
    block_hash: String,
}

impl From<&TransferRequest> for RegistrySuffixPosition {
    fn from(request: &TransferRequest) -> Self {
        Self {
            chain_id: request.chain_id.clone(),
            namespace: request.namespace.clone(),
            source_family: request.source_family.clone(),
            authority: RegistryAuthorityKey {
                source_manifest_id: request.source_manifest_id,
                registry_contract_instance_id: request.registry_contract_instance_id,
            },
            block_number: request.block_number,
            block_hash: request.block_hash.clone(),
        }
    }
}

pub(super) async fn load_registry_suffixes(
    pool: &PgPool,
    unique_requests: &[(TransferHydrationKey, &TransferRequest)],
) -> Result<HashMap<TransferHydrationKey, String>> {
    let positions = unique_registry_suffix_positions(unique_requests);
    let authorities = unique_registry_authorities(&positions);
    let declared = load_declared_registry_authorities(pool, &authorities).await?;
    let mut suffix_by_position = HashMap::with_capacity(positions.len());
    let mut discovered = Vec::new();
    for (position, request) in positions {
        if position.source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1 {
            suffix_by_position.insert(position, String::new());
        } else if declared.contains(&position.authority) {
            suffix_by_position.insert(position, "eth".to_owned());
        } else {
            discovered.push((position, request));
        }
    }
    load_discovered_registry_suffixes(pool, &discovered, &mut suffix_by_position).await?;

    unique_requests
        .iter()
        .map(|(key, request)| {
            let position = RegistrySuffixPosition::from(*request);
            let suffix = suffix_by_position
                .get(&position)
                .cloned()
                .context("batched ENSv2 registry suffix position is absent")?;
            Ok((key.clone(), suffix))
        })
        .collect()
}

fn unique_registry_suffix_positions<'request>(
    requests: &[(TransferHydrationKey, &'request TransferRequest)],
) -> Vec<(RegistrySuffixPosition, &'request TransferRequest)> {
    let mut seen = HashSet::new();
    requests
        .iter()
        .filter_map(|(_, request)| {
            let position = RegistrySuffixPosition::from(*request);
            seen.insert(position.clone())
                .then_some((position, *request))
        })
        .collect()
}

fn unique_registry_authorities(
    positions: &[(RegistrySuffixPosition, &TransferRequest)],
) -> Vec<RegistryAuthorityKey> {
    let mut seen = HashSet::new();
    positions
        .iter()
        .filter(|(position, _)| position.source_family != SOURCE_FAMILY_ENS_V2_ROOT_L1)
        .filter_map(|(position, _)| {
            seen.insert(position.authority)
                .then_some(position.authority)
        })
        .collect()
}

async fn load_discovered_registry_suffixes(
    pool: &PgPool,
    requests: &[(RegistrySuffixPosition, &TransferRequest)],
    suffixes: &mut HashMap<RegistrySuffixPosition, String>,
) -> Result<()> {
    for requests in requests.chunks(REGISTRY_SUFFIX_REQUEST_CHUNK_SIZE) {
        debug_assert!(
            registry_suffix_query_bind_count(requests.len()) < POSTGRES_BIND_PARAMETER_LIMIT
        );
        let mut query = build_registry_suffix_history_query(requests);
        let mut names = query
            .build_query_as::<(i64, Option<String>)>()
            .fetch_all(pool)
            .await
            .context("failed to load batched discovered ENSv2 registry suffixes")?
            .into_iter()
            .map(|(index, name)| {
                Ok((
                    usize::try_from(index).context("negative registry suffix request index")?,
                    name,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        for (request_index, (position, request)) in requests.iter().enumerate() {
            let logical_name_id = names
                .remove(&request_index)
                .flatten()
                .with_context(|| {
                    format!(
                        "ENSv2 TokenControlTransferred {} {} has no retained selected-path registry-parent discovery edge",
                        request.registry_address, request.token_id
                    )
                })?;
            let suffix = logical_name_id
                .strip_prefix(&format!("{}:", position.namespace))
                .map(str::to_owned)
                .with_context(|| {
                    format!("ENSv2 registry parent {logical_name_id} has the wrong namespace")
                })?;
            let previous = suffixes.insert(position.clone(), suffix);
            debug_assert!(
                previous.is_none(),
                "registry suffix position was not unique"
            );
        }
    }
    Ok(())
}

const fn registry_suffix_query_bind_count(request_count: usize) -> usize {
    request_count * REGISTRY_SUFFIX_REQUEST_BIND_COUNT
}

fn build_registry_suffix_history_query<'args>(
    requests: &'args [(RegistrySuffixPosition, &'args TransferRequest)],
) -> QueryBuilder<'args, Postgres> {
    let mut query = QueryBuilder::<Postgres>::new(
        "WITH RECURSIVE requested (request_index, chain_id, registry_id, block_number, block_hash) AS (",
    );
    query.push_values(
        requests.iter().enumerate(),
        |mut row, (index, (position, _))| {
            row.push_bind(i64::try_from(index).expect("request index must fit i64"))
                .push_bind(&position.chain_id)
                .push_bind(position.authority.registry_contract_instance_id)
                .push_bind(position.block_number)
                .push_bind(&position.block_hash);
        },
    );
    query.push(
        r#"), selected_tail AS (
            SELECT requested.request_index, lineage.chain_id, lineage.block_number,
                   lineage.block_hash, lineage.parent_hash, lineage.canonicality_state
            FROM requested JOIN chain_lineage lineage
              ON lineage.chain_id = requested.chain_id
             AND lineage.block_number = requested.block_number
             AND lineage.block_hash = requested.block_hash
             AND lineage.canonicality_state <> 'orphaned'::canonicality_state
            UNION ALL
            SELECT child.request_index, parent.chain_id, parent.block_number,
                   parent.block_hash, parent.parent_hash, parent.canonicality_state
            FROM selected_tail child JOIN chain_lineage parent
              ON parent.chain_id = child.chain_id
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
             AND parent.canonicality_state <> 'orphaned'::canonicality_state
            WHERE child.canonicality_state = 'observed'::canonicality_state
        ), selected_boundaries AS (
            SELECT requested.request_index,
                   COALESCE(MAX(tail.block_number) FILTER (
                       WHERE tail.canonicality_state <> 'observed'::canonicality_state
                   ), -1) AS stable_through_block
            FROM requested LEFT JOIN selected_tail tail
              ON tail.request_index = requested.request_index
            GROUP BY requested.request_index
        )
        SELECT DISTINCT ON (requested.request_index)
               requested.request_index, edge.provenance ->> 'logical_name_id'
        FROM requested
        JOIN selected_boundaries USING (request_index)
        JOIN discovery_edges edge
          ON edge.chain_id = requested.chain_id
         AND edge.edge_kind = 'subregistry'
         AND edge.to_contract_instance_id = requested.registry_id
         AND (edge.deactivated_at IS NULL OR edge.active_to_block_number IS NOT NULL)
         AND edge.active_from_block_number <= requested.block_number
        JOIN chain_lineage edge_start
          ON edge_start.chain_id = edge.chain_id
         AND edge_start.block_number = edge.active_from_block_number
         AND edge_start.block_hash = edge.active_from_block_hash
         AND edge_start.canonicality_state <> 'orphaned'::canonicality_state
        LEFT JOIN selected_tail selected_start
          ON selected_start.request_index = requested.request_index
         AND selected_start.block_number = edge.active_from_block_number
         AND selected_start.block_hash = edge.active_from_block_hash
        WHERE (selected_start.block_hash IS NOT NULL OR (
            edge_start.canonicality_state IN ('canonical', 'safe', 'finalized')
            AND edge_start.block_number <= selected_boundaries.stable_through_block
        )) AND (edge.active_to_block_number IS NULL
            OR edge.active_to_block_number >= requested.block_number
            OR NOT EXISTS (
                SELECT 1 FROM chain_lineage edge_close
                LEFT JOIN selected_tail selected_close
                  ON selected_close.request_index = requested.request_index
                 AND selected_close.block_number = edge_close.block_number
                 AND selected_close.block_hash = edge_close.block_hash
                WHERE edge_close.chain_id = edge.chain_id
                  AND edge_close.block_number = edge.active_to_block_number
                  AND edge_close.block_hash = edge.active_to_block_hash
                  AND edge_close.canonicality_state <> 'orphaned'::canonicality_state
                  AND (selected_close.block_hash IS NOT NULL OR (
                      edge_close.canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND edge_close.block_number <= selected_boundaries.stable_through_block
                  ))
            ))
        ORDER BY requested.request_index, edge.active_from_block_number DESC,
                 edge.discovery_edge_id DESC
        "#,
    );
    query
}

/// Load discovery intervals from the selected canonical/safe/finalized prefix
/// plus the exact parent chain of an `Observed` event block. A bounded interval
/// stays eligible after deactivation, but an `Observed` sibling cannot supply
/// either its start or its closing boundary to the selected branch.
pub(in crate::ens_v2_registry::events::hydration) async fn load_subregistry_target_rows(
    pool: &PgPool,
    requests: &[TargetRequest],
) -> Result<Vec<(i64, Uuid)>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    for requests in requests.chunks(SUBREGISTRY_REQUEST_CHUNK_SIZE) {
        debug_assert!(subregistry_query_bind_count(requests.len()) < POSTGRES_BIND_PARAMETER_LIMIT);
        let mut query = build_subregistry_target_query(requests);
        rows.extend(
            query
                .build_query_as::<(i64, Uuid)>()
                .fetch_all(pool)
                .await
                .context("failed to load selected-path ENSv2 subregistry discovery targets")?,
        );
    }
    rows.sort_unstable();
    Ok(rows)
}

const fn subregistry_query_bind_count(request_count: usize) -> usize {
    request_count * SUBREGISTRY_REQUEST_BIND_COUNT
}

fn build_subregistry_target_query(requests: &[TargetRequest]) -> QueryBuilder<'_, Postgres> {
    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        WITH RECURSIVE requested (
            event_index,
            chain_id,
            from_contract_instance_id,
            target_address,
            block_number,
            block_hash
        ) AS (
        "#,
    );
    query.push_values(requests, |mut row, request| {
        row.push_bind(request.event_index)
            .push_bind(&request.chain_id)
            .push_bind(request.from_contract_instance_id)
            .push_bind(&request.target_address)
            .push_bind(request.block_number)
            .push_bind(&request.block_hash);
    });
    query.push(
        r#"
        ), selected_tail AS (
            SELECT
                requested.event_index,
                lineage.chain_id,
                lineage.block_number,
                lineage.block_hash,
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
                child.event_index,
                parent.chain_id,
                parent.block_number,
                parent.block_hash,
                parent.parent_hash,
                parent.canonicality_state
            FROM selected_tail child
            JOIN chain_lineage parent
              ON parent.chain_id = child.chain_id
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
             AND parent.canonicality_state <> 'orphaned'::canonicality_state
            WHERE child.canonicality_state = 'observed'::canonicality_state
        ), selected_boundaries AS (
            SELECT
                requested.event_index,
                COALESCE(
                    MAX(selected_tail.block_number) FILTER (
                        WHERE selected_tail.canonicality_state
                            <> 'observed'::canonicality_state
                    ),
                    -1
                ) AS stable_through_block
            FROM requested
            LEFT JOIN selected_tail
              ON selected_tail.event_index = requested.event_index
            GROUP BY requested.event_index
        )
        SELECT
            requested.event_index,
            edge.to_contract_instance_id
        FROM requested
        JOIN selected_boundaries
          ON selected_boundaries.event_index = requested.event_index
        JOIN discovery_edges edge
          ON edge.chain_id = requested.chain_id
         AND edge.edge_kind = 'subregistry'
         AND edge.from_contract_instance_id = requested.from_contract_instance_id
         AND lower(edge.provenance ->> 'to_address') = requested.target_address
         AND (edge.deactivated_at IS NULL OR edge.active_to_block_number IS NOT NULL)
         AND edge.active_from_block_number <= requested.block_number
        JOIN chain_lineage edge_start
          ON edge_start.chain_id = edge.chain_id
         AND edge_start.block_number = edge.active_from_block_number
         AND edge_start.block_hash = edge.active_from_block_hash
         AND edge_start.canonicality_state <> 'orphaned'::canonicality_state
        LEFT JOIN selected_tail selected_start
          ON selected_start.event_index = requested.event_index
         AND selected_start.block_number = edge.active_from_block_number
         AND selected_start.block_hash = edge.active_from_block_hash
        WHERE (
            selected_start.block_hash IS NOT NULL
            OR (
                edge_start.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
                )
                AND edge_start.block_number <= selected_boundaries.stable_through_block
            )
        )
          AND (
              edge.active_to_block_number IS NULL
              OR edge.active_to_block_number >= requested.block_number
              OR NOT EXISTS (
                  SELECT 1
                  FROM chain_lineage edge_close
                  LEFT JOIN selected_tail selected_close
                    ON selected_close.event_index = requested.event_index
                   AND selected_close.block_number = edge_close.block_number
                   AND selected_close.block_hash = edge_close.block_hash
                  WHERE edge_close.chain_id = edge.chain_id
                    AND edge_close.block_number = edge.active_to_block_number
                    AND edge_close.block_hash = edge.active_to_block_hash
                    AND edge_close.canonicality_state <> 'orphaned'::canonicality_state
                    AND (
                        selected_close.block_hash IS NOT NULL
                        OR (
                            edge_close.canonicality_state IN (
                                'canonical'::canonicality_state,
                                'safe'::canonicality_state,
                                'finalized'::canonicality_state
                            )
                            AND edge_close.block_number
                                <= selected_boundaries.stable_through_block
                        )
                    )
              )
          )
        ORDER BY requested.event_index, edge.discovery_edge_id
        "#,
    );
    query
}

#[cfg(test)]
mod tests;
