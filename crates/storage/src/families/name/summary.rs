//! The name summary (TYR-36 step 7b slice 2b, the stored `project_name_summary` family): the
//! per-name fields the child and label lists filter, sort and count by inside one statement,
//! which the lists cannot compose at read for every child of a parent. The family step writes
//! them for the names each block touches, from the same composition as the composed name row
//! (`batch.rs`, `load_chain`), so the rules have one copy. Each field but `zero_owner` is the one the served
//! lists read from the name's `name_current` row:
//!
//! - `authority_arm`: `provenance.authority_selection.authority_arm`, the child's selected arm
//!   (the served children builder's arm rule);
//! - `serving`: whether `provenance.read_reachability.serving_resource_id` is set, which admits
//!   an ownerless child;
//! - `registration_status`: `declared_summary.registration.status`, whose `released` the expiry
//!   fence drops;
//! - `expires_at` and `registered_at`: the timestamp reads of the subnames sorts and fence, the
//!   same SQL expressions (`address_names::query`) over the composed summary;
//! - `zero_owner`: whether the latest ENSv1 or Basenames registry Transfer attributed to the name
//!   names the zero owner, which zeroes a registry child's owner. The served child builder
//!   attributes a Transfer as `project_latest_registry_owner` does
//!   (crates/project/src/builders/name_authority/stage.rs): by the name it carries, else by the
//!   latest named event of its resource and family, else by an active, readable surface at its
//!   node. That is not the composed name row's rule (its node's latest Transfer), so this field
//!   is the child builder's attribution over the registry owner events
//!   (`project_registry_owner_event`), whose named events are the registry's `SubregistryChanged`
//!   and `AuthorityTransferred` rows: a resource named only by another registry event kind links
//!   no name here;
//! - `recompose_at`: the first second after the composition's block at which the composition
//!   can change with no fact changing (a binding interval opening or closing, a NameWrapper
//!   expiry or grace boundary); the writer composes the name again at the first block whose time
//!   reaches it.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{CoverageShape, FamilyPublication, batch::load_chain, loaders::surfaces};
use crate::address_names::{push_expires_at_timestamp_expr, push_registered_at_timestamp_expr};

/// The summary rows of `logical_name_ids` at `publication`, as `to_jsonb` of a
/// `project_name_summary` row renders them, keyed by name. Every name with a surface of the chain
/// at or below the block has one, since a Transfer is attributed to a name whatever its surface's
/// state; a name the composed reader serves no row for (no active, readable surface) has no arm,
/// no serving resource, no registration and no clock boundary. `conn` may be the family block's
/// own transaction, whose writes the composition then reads.
pub async fn compose_name_summaries(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, Value>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    // The block's own surfaces and every earlier one; the marker has not moved to it yet.
    let surfaces: Vec<_> = surfaces(&mut *conn, logical_name_ids)
        .await?
        .into_iter()
        .filter(|surface| {
            surface.chain_id == publication.chain_id
                && surface.block_number <= publication.block_number
        })
        .collect();
    let composed = load_chain(
        &mut *conn,
        publication,
        &surfaces,
        CoverageShape::Plain,
        false,
    )
    .await?;
    let source = Value::Array(
        composed
            .iter()
            .map(|(name, composed)| {
                json!({
                    "logical_name_id": name,
                    "namespace": composed.row.namespace,
                    "declared_summary": composed.row.declared_summary,
                    "provenance": composed.row.provenance,
                    "recompose_at": composed.recompose_at,
                })
            })
            .collect(),
    );
    // The chain and block bind first, as `$1` and `$2`, which ZERO_OWNER reads.
    let mut builder = QueryBuilder::<Postgres>::new(
        "/* storage:families.name.summaries */ SELECT named.logical_name_id, to_jsonb(summary)
         FROM (SELECT ",
    );
    builder.push_bind(&publication.chain_id);
    builder.push("::text AS chain_id, ");
    builder.push_bind(publication.block_number);
    builder.push(
        "::bigint AS block_number) params
         CROSS JOIN LATERAL (
             SELECT DISTINCT surface.logical_name_id, surface.namespace
             FROM bigname_phase.name_surfaces surface
             WHERE surface.chain_id = params.chain_id
               AND surface.block_number <= params.block_number
               AND surface.logical_name_id = ANY(",
    );
    builder.push_bind(logical_name_ids);
    builder.push("::text[])) named LEFT JOIN jsonb_to_recordset(");
    builder.push_bind(&source);
    builder.push(
        ") AS nc(logical_name_id text, namespace text, declared_summary jsonb, provenance jsonb,
                 recompose_at bigint)
           ON nc.logical_name_id = named.logical_name_id
         CROSS JOIN LATERAL (
             SELECT params.chain_id, named.logical_name_id, named.namespace,
                nc.provenance #>> '{authority_selection,authority_arm}' AS authority_arm,
                COALESCE(nc.provenance #>> '{read_reachability,serving_resource_id}' IS NOT NULL,
                         FALSE) AS serving,
                nc.declared_summary #>> '{registration,status}' AS registration_status, ",
    );
    push_expires_at_timestamp_expr(&mut builder);
    builder.push(" AS expires_at, ");
    push_registered_at_timestamp_expr(&mut builder);
    builder.push(format!(
        " AS registered_at, {ZERO_OWNER} AS zero_owner,
                to_timestamp(nc.recompose_at) AS recompose_at) summary"
    ));
    let rows: Vec<(String, Value)> = builder
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .context("failed to shape the name summaries")?;
    Ok(rows.into_iter().collect())
}

/// `zero_owner` of the name `named.logical_name_id` at the block `$2` of chain `$1` (the binds of
/// the summary statement), over the registry owner events: its candidate Transfers are those
/// naming it, the unnamed ones at its node, and the unnamed ones of a resource one of its named
/// registry events carries; each is attributed by its name, else the latest named event of its
/// resource and family, else an active, readable surface at its node; the latest one attributed
/// to the name decides, in the served order (block, transaction index, log index with nulls
/// lowest, then event identity).
const ZERO_OWNER: &str = "COALESCE((
    SELECT attributed.owner_getter = '0x0000000000000000000000000000000000000000'
    FROM (
        SELECT transfer.owner_getter, transfer.block_number, transfer.transaction_index,
               transfer.log_index, transfer.event_identity,
               COALESCE(transfer.logical_name_id, linked.logical_name_id,
                        at_node.logical_name_id) AS logical_name_id
        FROM (
            SELECT candidate.* FROM bigname_phase.project_registry_owner_event candidate
            WHERE candidate.chain_id = $1 AND candidate.logical_name_id = named.logical_name_id
            UNION
            SELECT candidate.* FROM bigname_phase.project_registry_owner_event candidate
            WHERE candidate.chain_id = $1 AND candidate.namespace = named.namespace
              AND candidate.node = (SELECT lower(surface.namehash)
                                    FROM bigname_phase.name_surfaces surface
                                    WHERE surface.logical_name_id = named.logical_name_id)
              AND candidate.logical_name_id IS NULL
            UNION
            SELECT candidate.* FROM bigname_phase.project_registry_owner_event candidate
            WHERE candidate.chain_id = $1 AND candidate.logical_name_id IS NULL
              AND candidate.resource_id IN (
                  SELECT named.resource_id FROM bigname_phase.project_registry_owner_event named
                  WHERE named.chain_id = $1 AND named.logical_name_id = named.logical_name_id
                    AND named.resource_id IS NOT NULL
                    AND named.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry'))
        ) transfer
        LEFT JOIN LATERAL (
            SELECT named.logical_name_id
            FROM bigname_phase.project_registry_owner_event named
            WHERE transfer.logical_name_id IS NULL AND named.chain_id = transfer.chain_id
              AND named.resource_id = transfer.resource_id
              AND named.source_family = transfer.source_family
              AND named.logical_name_id IS NOT NULL
            ORDER BY named.block_number DESC, named.transaction_index DESC NULLS LAST,
                     named.log_index DESC NULLS LAST, named.event_identity DESC
            LIMIT 1
        ) linked ON TRUE
        LEFT JOIN bigname_phase.name_surfaces at_node
          ON transfer.logical_name_id IS NULL AND linked.logical_name_id IS NULL
         AND at_node.chain_id = transfer.chain_id AND at_node.namespace = transfer.namespace
         AND lower(at_node.namehash) = transfer.node
         AND at_node.visibility_state = 'active' AND at_node.block_number <= $2
         AND at_node.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage at_node_lineage
                     WHERE at_node_lineage.chain_id = at_node.chain_id
                       AND at_node_lineage.block_hash = at_node.block_hash
                       AND at_node_lineage.block_number = at_node.block_number
                       AND at_node_lineage.canonicality_state
                           IN ('canonical', 'safe', 'finalized'))
        WHERE transfer.event_kind = 'AuthorityTransferred'
          AND transfer.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
    ) attributed
    WHERE attributed.logical_name_id = named.logical_name_id
    ORDER BY attributed.block_number DESC, attributed.transaction_index DESC NULLS LAST,
             attributed.log_index DESC NULLS LAST, attributed.event_identity DESC
    LIMIT 1
), FALSE)";
