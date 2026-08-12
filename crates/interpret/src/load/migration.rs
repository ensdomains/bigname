use bigname_adapters::schema_v2::AddressAdmissionInput;
use bigname_adapters::schema_v2::seam::{
    LOG_INDEX_KEY, MIGRATION_REGISTRY_ASSOCIATION_KIND, REGISTRY_ANNOUNCEMENT_EDGE_KIND,
    TRANSACTION_INDEX_KEY,
};
use sqlx::{PgConnection, types::Uuid};

use crate::{InterpretError, Result};

type AdmissionRow = (
    String,
    Uuid,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<Uuid>,
    Option<String>,
    Option<i64>,
    Option<i64>,
);

pub(super) async fn admissions(
    connection: &mut PgConnection,
    chain_id: &str,
    before_block: i64,
) -> Result<Vec<AddressAdmissionInput>> {
    let rows: Vec<AdmissionRow> = sqlx::query_as(
        "
        SELECT lower(address.address),
               association.registry_contract_instance_id,
               association.source_manifest_id,
               NULL::text AS role,
               $3::text AS edge_kind,
               edge.from_contract_instance_id,
               jsonb_build_object(
                   'id', association.migration_correlation_id,
                   'evidence', association.evidence_refs
               )::text AS observation_key,
               association.block_number AS active_from,
               edge.active_to_block_number AS active_to
        FROM migration_discovery_associations association
        JOIN discovery_edges edge
          ON edge.chain_id = association.chain_id
         AND edge.edge_kind = $4
         AND edge.to_contract_instance_id = association.registry_contract_instance_id
         AND edge.source_manifest_id = association.source_manifest_id
         AND edge.active_from_block_number = association.block_number
         AND edge.active_from_block_hash = association.block_hash
         AND (edge.provenance ->> $5)::bigint = association.transaction_index
         AND (edge.provenance ->> $6)::bigint = association.log_index
        JOIN contract_instance_addresses address
          ON address.chain_id = association.chain_id
         AND address.contract_instance_id = association.registry_contract_instance_id
         AND lower(address.address) = lower(association.registry_address)
        JOIN chain_lineage association_lineage
          ON association_lineage.chain_id = association.chain_id
         AND association_lineage.block_number = association.block_number
         AND association_lineage.block_hash = association.block_hash
        WHERE association.chain_id = $1
          AND association.consumer_visibility IN ('candidate', 'activated')
          AND association.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND association_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND association.block_number < $2
          AND edge.deactivated_at IS NULL
          AND address.deactivated_at IS NULL
        ORDER BY address, association.registry_contract_instance_id
        ",
    )
    .bind(chain_id)
    .bind(before_block)
    .bind(MIGRATION_REGISTRY_ASSOCIATION_KIND)
    .bind(REGISTRY_ANNOUNCEMENT_EDGE_KIND)
    .bind(TRANSACTION_INDEX_KEY)
    .bind(LOG_INDEX_KEY)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load migration registry associations", error)
    })?;
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
