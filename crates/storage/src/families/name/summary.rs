//! The name summary (the stored `project_name_summary` family): the
//! per-name fields the child and label lists filter, sort and count by inside one statement,
//! which the lists cannot compose at read for every child of a parent. The family step writes
//! them for the names each block touches, from the same composition as the composed name row
//! (`batch.rs`, `load_chain`), so the rules have one copy. Except for `authority_arm` and
//! `zero_owner`, these are fields of the composed name row:
//!
//! - `authority_arm`: the child's independently selected arm, which the children relation reads
//!   even when token readability withholds the name row;
//! - `serving`: whether `provenance.read_reachability.serving_resource_id` is set, which admits
//!   an ownerless child;
//! - `registration_status`: `declared_summary.registration.status`, whose `released` the expiry
//!   fence drops;
//! - `expires_at` and `registered_at`: the timestamp reads of the subnames sorts and fence, the
//!   same SQL expressions (`address_names::query`) over the composed summary;
//! - `zero_owner`: whether the latest ENSv1 or Basenames registry Transfer attributed to the name
//!   names the zero owner, which zeroes a registry child's owner. A Transfer is attributed by
//!   the name it carries, else by the latest named event of its resource and family, of any
//!   kind, else by an active, readable surface at its node. That is not the composed name row's
//!   rule (its node's latest Transfer), so this field keeps the child list's own attribution:
//!   the Transfers are the registry owner events (`project_registry_owner_event`), and the named
//!   events that link a resource are read from the readable interpreted events;
//! - `owner`: the owner the name row serves, `declared_summary.control.owner`, else
//!   `control.registry_owner`, lower-cased, null when the first present one is blank or the name
//!   composes no row (apps/api/src/v2/name_record/declared.rs, `declared_owner`); the registry
//!   labels' `owner` and `exclude_owner` filters read it;
//! - `recompose_at`: the first second after the composition's block at which the composition
//!   can change with no fact changing (a binding interval opening or closing, a NameWrapper
//!   expiry or grace boundary), in Unix seconds, since a NameWrapper expiry can lie past the last
//!   instant a timestamp holds; kept for a name that composes no row too. The writer composes
//!   the name again at the first block whose time reaches it.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{CoverageShape, FamilyPublication, batch::load_chain, loaders::surfaces};
use crate::address_names::{push_expires_at_timestamp_expr, push_registered_at_timestamp_expr};

/// The summary rows of `logical_name_ids` at `publication`, as `to_jsonb` of a
/// `project_name_summary` row renders them, keyed by name. Every name with a surface of the chain
/// at or below the block has one, since a Transfer is attributed to a name whatever its surface's
/// state; a bound name withheld for unreadable token lineage keeps its selected arm and clock
/// boundary, but has no serving resource or registration. `conn` may be the family block's own transaction, whose
/// writes the composition then reads.
/// Summary rows and exact null-resolver names from the same composition. The family writer
/// uses the latter to retire direct-resolution evidence in its publication transaction.
#[derive(Default)]
pub struct NameSummaryPublication {
    pub rows: BTreeMap<String, Value>,
    pub null_resolver_names: Vec<String>,
}

pub async fn compose_name_summaries(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, Value>> {
    Ok(
        compose_name_summary_publication(conn, publication, logical_name_ids)
            .await?
            .rows,
    )
}

/// Composition for a Project publication, including evidence-retirement inputs.
pub async fn compose_name_summary_publication(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    logical_name_ids: &[String],
) -> Result<NameSummaryPublication> {
    if logical_name_ids.is_empty() {
        return Ok(NameSummaryPublication::default());
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
    let null_resolver_names = if matches!(
        publication.chain_id.as_str(),
        "ethereum-mainnet" | "ethereum-sepolia"
    ) {
        composed
            .values()
            .filter_map(|composed| composed.row.as_ref())
            .filter(|row| {
                row.namespace == "ens"
                    && row.declared_summary.pointer("/resolver/chain_id") == Some(&Value::Null)
                    && row.declared_summary.pointer("/resolver/address") == Some(&Value::Null)
            })
            .map(|row| row.logical_name_id.clone())
            .collect()
    } else {
        Vec::new()
    };
    let source = Value::Array(
        composed
            .iter()
            .map(|(name, composed)| {
                json!({
                    "logical_name_id": name,
                    "authority_arm": composed.authority_arm,
                    "declared_summary": composed.row.as_ref().map(|row| &row.declared_summary),
                    "provenance": composed.row.as_ref().map(|row| &row.provenance),
                    "recompose_at": composed.recompose_at,
                })
            })
            .collect(),
    );
    // The chain and block bind first, as `$1` and `$2`, which `zero_owner` reads.
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
        ") AS nc(logical_name_id text, authority_arm text, declared_summary jsonb, provenance jsonb,
                 recompose_at bigint)
           ON nc.logical_name_id = named.logical_name_id
         CROSS JOIN LATERAL (
             SELECT params.chain_id, named.logical_name_id, named.namespace,
                nc.authority_arm,
                COALESCE(nc.provenance #>> '{read_reachability,serving_resource_id}' IS NOT NULL,
                         FALSE) AS serving,
                nc.declared_summary #>> '{registration,status}' AS registration_status, ",
    );
    push_expires_at_timestamp_expr(&mut builder);
    builder.push(" AS expires_at, ");
    push_registered_at_timestamp_expr(&mut builder);
    builder.push(format!(
        " AS registered_at, {} AS zero_owner,
                nc.recompose_at, {SERVED_OWNER} AS owner) summary",
        zero_owner()
    ));
    let rows: Vec<(String, Value)> = builder
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .context("failed to shape the name summaries")?;
    Ok(NameSummaryPublication {
        rows: rows.into_iter().collect(),
        null_resolver_names,
    })
}

/// The owner the composed name row serves: the first of `control.owner` and
/// `control.registry_owner` that is present, lower-cased; null when that one is blank, when
/// neither is present and when the name composes no row.
const SERVED_OWNER: &str = "(SELECT CASE WHEN btrim(served.owner) = '' THEN NULL
                 ELSE lower(served.owner) END
         FROM (SELECT COALESCE(nc.declared_summary #>> '{control,owner}',
                               nc.declared_summary #>> '{control,registry_owner}') AS owner)
              served)";

/// `zero_owner` of the name `named.logical_name_id` at the block `$2` of chain `$1` (the binds of
/// the summary statement): its candidate Transfers are those naming it, the unnamed ones at its
/// node, and the unnamed ones of a resource one of its named registry events carries; each is
/// attributed by its name, else the latest named event of its resource and family, else an
/// active, readable surface at its node; the latest one attributed to the name decides, in the
/// served order (block, transaction index, log index with nulls lowest, then event identity).
/// The named events are the activated, readable interpreted events at or below the block.
fn zero_owner() -> String {
    let own = readable_event("own");
    let latest = readable_event("latest");
    format!(
        "COALESCE((
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
                  SELECT own.resource_id FROM bigname_phase.normalized_events own
                  WHERE own.logical_name_id = named.logical_name_id AND {own}
                    AND own.resource_id IS NOT NULL
                    AND own.source_family IN ({REGISTRIES}))
        ) transfer
        LEFT JOIN LATERAL (
            SELECT latest.logical_name_id
            FROM bigname_phase.normalized_events latest
            WHERE transfer.logical_name_id IS NULL AND latest.resource_id = transfer.resource_id
              AND latest.source_family = transfer.source_family
              AND latest.logical_name_id IS NOT NULL AND {latest}
            ORDER BY latest.block_number DESC NULLS LAST,
                     latest.transaction_index DESC NULLS LAST,
                     latest.log_index DESC NULLS LAST, latest.event_identity DESC
            LIMIT 1
        ) linked ON TRUE
        LEFT JOIN bigname_phase.name_surfaces at_node
          ON transfer.logical_name_id IS NULL AND linked.logical_name_id IS NULL
         AND at_node.chain_id = transfer.chain_id AND at_node.namespace = transfer.namespace
         AND lower(at_node.namehash) = transfer.node
         AND at_node.visibility_state = 'active' AND at_node.block_number <= $2
         AND at_node.canonicality_state IN {READABLE}
         AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage at_node_lineage
                     WHERE at_node_lineage.chain_id = at_node.chain_id
                       AND at_node_lineage.block_hash = at_node.block_hash
                       AND at_node_lineage.block_number = at_node.block_number
                       AND at_node_lineage.canonicality_state IN {READABLE})
        WHERE transfer.event_kind = 'AuthorityTransferred'
          AND transfer.source_family IN ({REGISTRIES})
    ) attributed
    WHERE attributed.logical_name_id = named.logical_name_id
    ORDER BY attributed.block_number DESC, attributed.transaction_index DESC NULLS LAST,
             attributed.log_index DESC NULLS LAST, attributed.event_identity DESC
    LIMIT 1
), FALSE)"
    )
}

const REGISTRIES: &str = "'ens_v1_registry_l1', 'basenames_base_registry'";
const READABLE: &str = "('canonical', 'safe', 'finalized')";

/// An interpreted event `alias` of chain `$1` the served stage reads at block `$2`: activated,
/// readable, and on a readable block at or below it (or on none).
fn readable_event(alias: &str) -> String {
    format!(
        "{alias}.chain_id = $1 AND {alias}.consumer_visibility = 'activated'
         AND {alias}.canonicality_state IN {READABLE}
         AND (({alias}.block_number IS NULL AND {alias}.block_hash IS NULL)
              OR ({alias}.block_number <= $2
                  AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage {alias}_lineage
                              WHERE {alias}_lineage.chain_id = {alias}.chain_id
                                AND {alias}_lineage.block_hash = {alias}.block_hash
                                AND {alias}_lineage.block_number = {alias}.block_number
                                AND {alias}_lineage.canonicality_state IN {READABLE})))"
    )
}
