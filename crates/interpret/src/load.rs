use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput,
    RawLogInput,
};
use sqlx::{PgPool, types::Uuid};

use crate::{InterpretError, Result};

mod prior;

type ManifestRow = (i64, i64, String, String, String, String, String, String);
type RawLogRow = (
    String,
    String,
    i64,
    time::OffsetDateTime,
    String,
    String,
    i64,
    i64,
    String,
    Vec<String>,
    Vec<u8>,
);

pub(crate) async fn batch_input(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<BatchInput> {
    let manifests = load_manifests(pool, chain_id).await?;
    if manifests.is_empty() {
        return Err(InterpretError::configuration(format!(
            "chain {chain_id} has no active manifests for interpretation"
        )));
    }
    Ok(BatchInput {
        chain_id: chain_id.to_owned(),
        discovery_rules: load_discovery_rules(pool, chain_id).await?,
        admissions: load_admissions(pool, chain_id, from_block).await?,
        prior_events: prior::events(pool, chain_id, from_block).await?,
        blocks: load_blocks(pool, chain_id, from_block, to_block).await?,
        raw_logs: load_raw_logs(pool, chain_id, from_block, to_block).await?,
        manifests,
    })
}

async fn load_blocks(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<RawBlockInput>> {
    let rows: Vec<(String, String, i64, time::OffsetDateTime, String)> = sqlx::query_as(
        "
        SELECT chain_id, block_hash, block_number, block_timestamp,
               canonicality_state::text
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY block_number, block_hash
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical block facts", error))?;
    Ok(rows
        .into_iter()
        .map(
            |(chain_id, block_hash, block_number, block_timestamp, canonicality_state)| {
                RawBlockInput {
                    chain_id,
                    block_hash,
                    block_number,
                    block_timestamp,
                    canonicality_state,
                }
            },
        )
        .collect())
}

pub(crate) async fn canonical_markers(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    limit: i64,
) -> Result<Vec<(i64, String)>> {
    sqlx::query_as(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY block_number, block_hash
        LIMIT $4
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical batch markers", error))
}

pub(crate) async fn marker(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
) -> Result<Option<(i64, String)>> {
    sqlx::query_as(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_optional(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical target marker", error))
}

async fn load_manifests(pool: &PgPool, chain_id: &str) -> Result<Vec<ManifestInput>> {
    let rows: Vec<ManifestRow> = sqlx::query_as(
        "
        SELECT manifest_id,
               manifest_version,
               namespace,
               source_family,
               chain_id,
               deployment_label,
               normalizer_version,
               manifest_payload::text
        FROM manifest_versions
        WHERE chain_id = $1
          AND rollout_status = 'active'
        ORDER BY namespace, source_family, manifest_version, manifest_id
        ",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load active manifests", error))?;
    Ok(rows
        .into_iter()
        .map(
            |(
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain_id,
                deployment_label,
                normalizer_version,
                payload_json,
            )| ManifestInput {
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain_id,
                deployment_label,
                normalizer_version,
                payload_json,
            },
        )
        .collect())
}

async fn load_discovery_rules(pool: &PgPool, chain_id: &str) -> Result<Vec<DiscoveryRuleInput>> {
    let rows: Vec<(i64, String, Option<String>, String)> = sqlx::query_as(
        "
        SELECT rule.manifest_id, rule.edge_kind, rule.from_role, rule.admission
        FROM manifest_discovery_rules rule
        JOIN manifest_versions manifest
          ON manifest.manifest_id = rule.manifest_id
        WHERE manifest.chain_id = $1
          AND manifest.rollout_status = 'active'
        ORDER BY rule.manifest_id, rule.manifest_discovery_rule_id
        ",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load manifest discovery rules", error))?;
    Ok(rows
        .into_iter()
        .map(
            |(manifest_id, edge_kind, from_role, admission)| DiscoveryRuleInput {
                manifest_id,
                edge_kind,
                from_role,
                admission,
            },
        )
        .collect())
}

#[allow(clippy::type_complexity)]
async fn load_admissions(
    pool: &PgPool,
    chain_id: &str,
    before_block: i64,
) -> Result<Vec<AddressAdmissionInput>> {
    let rows: Vec<(
        String,
        Uuid,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<Uuid>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    )> = sqlx::query_as(
        "
        WITH declared AS (
            SELECT lower(address.address) AS address,
                   address.contract_instance_id,
                   manifest.manifest_id AS source_manifest_id,
                   COALESCE(declaration.role, declaration.declaration_name) AS role,
                   NULL::text AS edge_kind,
                   NULL::uuid AS from_contract_instance_id,
                   NULL::text AS observation_key,
                   GREATEST(
                       COALESCE(declaration.start_block_number, 0),
                       COALESCE(address.active_from_block_number, 0)
                   ) AS active_from,
                   address.active_to_block_number AS active_to
            FROM manifest_versions manifest
            JOIN manifest_contract_instances declaration
              ON declaration.manifest_id = manifest.manifest_id
             AND declaration.chain_id = manifest.chain_id
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = declaration.contract_instance_id
             AND address.chain_id = declaration.chain_id
            WHERE manifest.chain_id = $1
              AND manifest.rollout_status = 'active'
              AND (
                  address.deactivated_at IS NULL
                  OR address.active_to_block_number IS NOT NULL
              )
        ),
        discovered AS (
            SELECT lower(address.address) AS address,
                   address.contract_instance_id,
                   edge.source_manifest_id,
                   NULL::text AS role,
                   edge.edge_kind,
                   edge.from_contract_instance_id,
                   edge.provenance ->> 'observation_key' AS observation_key,
                   GREATEST(
                       COALESCE(edge.active_from_block_number, 0),
                       COALESCE(address.active_from_block_number, 0)
                   ) AS active_from,
                   CASE
                       WHEN edge.active_to_block_number IS NULL
                           THEN address.active_to_block_number
                       WHEN address.active_to_block_number IS NULL
                           THEN edge.active_to_block_number
                       ELSE LEAST(
                           edge.active_to_block_number,
                           address.active_to_block_number
                       )
                   END AS active_to
            FROM discovery_edges edge
            JOIN manifest_versions manifest
              ON manifest.manifest_id = edge.source_manifest_id
             AND manifest.chain_id = edge.chain_id
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = edge.to_contract_instance_id
             AND address.chain_id = edge.chain_id
            WHERE edge.chain_id = $1
              AND manifest.rollout_status = 'active'
              AND edge.edge_kind IN ('resolver', 'registry_announcement')
              AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND COALESCE(edge.active_from_block_number, 0) < $2
              AND (
                  edge.deactivated_at IS NULL
                  OR edge.active_to_block_number IS NOT NULL
              )
        )
        SELECT address,
               contract_instance_id,
               source_manifest_id,
               role,
               edge_kind,
               from_contract_instance_id,
               observation_key,
               active_from,
               active_to
        FROM declared
        UNION ALL
        SELECT address,
               contract_instance_id,
               source_manifest_id,
               role,
               edge_kind,
               from_contract_instance_id,
               observation_key,
               active_from,
               active_to
        FROM discovered
        ORDER BY address, contract_instance_id, edge_kind NULLS FIRST
        ",
    )
    .bind(chain_id)
    .bind(before_block)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load admitted contract ranges", error))?;
    Ok(rows
        .into_iter()
        .map(
            |(
                address,
                contract_instance_id,
                source_manifest_id,
                role,
                discovery_edge_kind,
                discovery_from_contract_instance_id,
                discovery_observation_key,
                active_from_block,
                active_to_block,
            )| AddressAdmissionInput {
                address,
                contract_instance_id,
                source_manifest_id,
                role,
                discovery_edge_kind,
                discovery_from_contract_instance_id,
                discovery_observation_key,
                active_from_block,
                active_to_block,
            },
        )
        .collect())
}

async fn load_raw_logs(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<RawLogInput>> {
    let rows: Vec<RawLogRow> = sqlx::query_as(
        "
        SELECT raw.chain_id,
               raw.block_hash,
               raw.block_number,
               lineage.block_timestamp,
               lineage.canonicality_state::text,
               raw.transaction_hash,
               raw.transaction_index,
               raw.log_index,
               lower(raw.emitting_address),
               raw.topics,
               raw.data
        FROM raw_logs raw
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw.chain_id
         AND lineage.block_hash = raw.block_hash
         AND lineage.block_number = raw.block_number
        WHERE raw.chain_id = $1
          AND raw.block_number BETWEEN $2 AND $3
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY raw.block_number,
                 raw.transaction_index,
                 raw.log_index,
                 raw.block_hash
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical raw logs", error))?;
    Ok(rows
        .into_iter()
        .map(
            |(
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
            )| RawLogInput {
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
            },
        )
        .collect())
}
