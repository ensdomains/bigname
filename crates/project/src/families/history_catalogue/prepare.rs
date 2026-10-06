//! Complete own-chain source and membership work keys for one block or rebuild range.
use std::collections::BTreeSet;

use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{ProjectError, Result};

#[derive(Default)]
pub(crate) struct Work {
    pub names: Vec<String>,
    pub resources: Vec<Uuid>,
    pub edge_resources: Vec<Uuid>,
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    number: i64,
    after: i64,
    summary_names: &[String],
) -> Result<Work> {
    // Keep discovery conservative across every canonicality state. The keyed probe's
    // OFFSET 0 keeps unrelated unpositioned manifest history out of its access path.
    // Applying the publication bound outside it reads a touched key's retained future
    // events too; this is intentional work-key discovery, not serving eligibility.
    let resources: Vec<Uuid> = sqlx::query_scalar(
        "/* project:history.work_resources */
WITH wanted_names AS MATERIALIZED (SELECT DISTINCT name FROM unnest($4::text[]) wanted(name))
SELECT DISTINCT resource FROM (
 SELECT ne.resource_id AS resource FROM normalized_events ne
 WHERE ne.chain_id=$1 AND ne.block_number>$2 AND ne.block_number<=$3
 UNION ALL
 SELECT resource.value FROM wanted_names wanted
 CROSS JOIN LATERAL (SELECT candidate.* FROM project_binding_candidate candidate
  WHERE candidate.chain_id=$1 AND candidate.logical_name_id=wanted.name OFFSET 0) candidate
 CROSS JOIN LATERAL (VALUES(candidate.resource_id),(candidate.wrapped_registrar_resource_id),
  (candidate.predecessor_resource_id),(candidate.lease_resource_id)) resource(value)
 UNION ALL
 SELECT anchor.current_resource_id FROM wanted_names wanted
 CROSS JOIN LATERAL (SELECT anchor.current_resource_id FROM project_address_history_anchor anchor
  WHERE anchor.chain_id=$1 AND anchor.anchor_kind=0 AND anchor.anchor_id=wanted.name OFFSET 0) anchor
 UNION ALL
 SELECT ne.resource_id FROM wanted_names wanted CROSS JOIN LATERAL (
  SELECT ne.resource_id, ne.block_number FROM normalized_events ne
  WHERE ne.logical_name_id=wanted.name AND ne.chain_id=$1 AND ne.resource_id IS NOT NULL
   AND ne.canonicality_state IN ('canonical'::canonicality_state,'safe'::canonicality_state,'finalized'::canonicality_state)
  UNION ALL
  SELECT ne.resource_id, ne.block_number FROM normalized_events ne
  WHERE ne.logical_name_id=wanted.name AND ne.chain_id=$1 AND ne.resource_id IS NOT NULL
   AND ne.canonicality_state NOT IN ('canonical'::canonicality_state,'safe'::canonicality_state,'finalized'::canonicality_state)
  OFFSET 0) ne WHERE ne.block_number<=$3 OR ne.block_number IS NULL
) touched WHERE resource IS NOT NULL",
    )
    .bind(chain)
    .bind(after)
    .bind(number)
    .bind(summary_names)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to discover history resource keys", e))?;
    let mut names: BTreeSet<String> = summary_names.iter().cloned().collect();
    names.extend(
        sqlx::query_scalar::<_, String>(
            "/* project:history.work_names */
WITH wanted_resources AS MATERIALIZED (SELECT DISTINCT resource FROM unnest($4::uuid[]) wanted(resource))
SELECT DISTINCT name FROM (
 SELECT ne.logical_name_id AS name FROM normalized_events ne
 WHERE ne.chain_id=$1 AND ((ne.block_number>$2 AND ne.block_number<=$3) OR ($2=-1 AND ne.block_number IS NULL))
 UNION ALL
 SELECT ne.logical_name_id FROM wanted_resources wanted CROSS JOIN LATERAL (
  SELECT ne.logical_name_id, ne.block_number FROM normalized_events ne
  WHERE ne.resource_id=wanted.resource AND ne.chain_id=$1 AND ne.logical_name_id IS NOT NULL
   AND ne.canonicality_state IN ('canonical'::canonicality_state,'safe'::canonicality_state,'finalized'::canonicality_state)
  UNION ALL
  SELECT ne.logical_name_id, ne.block_number FROM normalized_events ne
  WHERE ne.resource_id=wanted.resource AND ne.chain_id=$1 AND ne.logical_name_id IS NOT NULL
   AND ne.canonicality_state NOT IN ('canonical'::canonicality_state,'safe'::canonicality_state,'finalized'::canonicality_state)
  OFFSET 0) ne WHERE ne.block_number<=$3 OR ne.block_number IS NULL
) touched WHERE name IS NOT NULL",
        )
        .bind(chain)
        .bind(after)
        .bind(number)
        .bind(&resources)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to discover history name keys", e))?,
    );
    // A link can change source discovery for every older pointer at its resolver. The node
    // edge is kept even before any link, so it supplies this reverse dependency from day one.
    let mut edge_resources: BTreeSet<Uuid> = resources.iter().copied().collect();
    edge_resources.extend(
        sqlx::query_scalar::<_, Uuid>(
            "/* project:history.link_resources */
        SELECT DISTINCT edge.resource_id FROM normalized_events link
        JOIN project_history_source_edge edge ON edge.chain_id=link.chain_id
          AND edge.pointer_resolver=lower(link.after_state->>'resolver')
          AND (edge.node=lower(link.after_state->>'node')
            OR lower(link.after_state->>'node')=
                '0x0000000000000000000000000000000000000000000000000000000000000000')
        WHERE link.chain_id=$1 AND link.block_number>$2 AND link.block_number<=$3
          AND link.event_kind='ResolverRecordLinked'
          AND link.after_state->>'storage_model'='resolver_record_id'",
        )
        .bind(chain)
        .bind(after)
        .bind(number)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to discover history link dependencies", e))?,
    );
    tracing::debug!(target: "bigname_project::families", chain_id=chain,
        through=number, names=names.len(), resources=resources.len(),
        edge_resources=edge_resources.len(), "prepared history catalogue work");
    Ok(Work {
        names: names.into_iter().collect(),
        resources,
        edge_resources: edge_resources.into_iter().collect(),
    })
}

pub(super) async fn dependent_resources(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    sources: Value,
) -> Result<Vec<Uuid>> {
    sqlx::query_scalar(
        "/* project:history.source_resources */
        SELECT DISTINCT edge.resource_id FROM jsonb_to_recordset($2)
          source(source_kind smallint, source_key text, resolver_address text)
        JOIN project_history_source_edge edge ON edge.chain_id=$1
          AND edge.source_kind=source.source_kind AND edge.source_key=source.source_key
          AND edge.source_resolver=source.resolver_address",
    )
    .bind(chain)
    .bind(sources)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to find history source resource envelopes", e))
}
