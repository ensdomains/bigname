//! The name summary (TYR-36 step 7b slice 2b, the stored `project_name_summary` family): the
//! per-name fields the child and label lists filter, sort and count by inside one statement,
//! which the lists cannot compose at read for every child of a parent. The family step writes
//! them for the names each block touches, from the same composition as the composed name row
//! (`batch.rs`), so the rules have one copy. Each field is the one the served lists read from the
//! name's `name_current` row:
//!
//! - `authority_arm`: `provenance.authority_selection.authority_arm`, the child's selected arm
//!   (the served children builder's arm rule);
//! - `serving`: whether `provenance.read_reachability.serving_resource_id` is set, which admits
//!   an ownerless child;
//! - `registration_status`: `declared_summary.registration.status`, whose `released` the expiry
//!   fence drops;
//! - `expires_at` and `registered_at`: the timestamp reads of the subnames sorts and fence, the
//!   same SQL expressions (`address_names::query`) over the composed summary;
//! - `zero_owner`: whether the node's latest registry transfer names the zero owner, which
//!   zeroes a registry child's owner;
//! - `recompose_at`: the first second after the composition's block at which the composition
//!   can change with no fact changing (a binding interval opening or closing, a NameWrapper
//!   expiry or grace boundary); the writer composes the name again at the first block whose time
//!   reaches it.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{CoverageShape, FamilyPublication, batch::compose_chain, loaders::surfaces};
use crate::address_names::{push_expires_at_timestamp_expr, push_registered_at_timestamp_expr};

/// The summary rows of `logical_name_ids` at `publication`, as `to_jsonb` of a
/// `project_name_summary` row renders them, keyed by name; a name the composed reader serves no
/// row for has none. `conn` may be the family block's own transaction, whose writes the
/// composition then reads.
pub async fn compose_name_summaries(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, Value>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let surfaces = surfaces(
        &mut *conn,
        logical_name_ids,
        Some((&publication.chain_id, publication.block_number)),
    )
    .await?;
    let composed = compose_chain(
        &mut *conn,
        publication,
        &surfaces,
        CoverageShape::Plain,
        false,
    )
    .await?;
    if composed.is_empty() {
        return Ok(BTreeMap::new());
    }
    let source = Value::Array(
        composed
            .iter()
            .map(|(name, composed)| {
                json!({
                    "logical_name_id": name,
                    "namespace": composed.row.namespace,
                    "declared_summary": composed.row.declared_summary,
                    "provenance": composed.row.provenance,
                    "zero_owner": composed.zero_owner,
                    "recompose_at": composed.recompose_at,
                })
            })
            .collect(),
    );
    let mut builder = QueryBuilder::<Postgres>::new(
        "/* storage:families.name.summaries */ SELECT nc.logical_name_id, to_jsonb(summary)
         FROM jsonb_to_recordset(",
    );
    builder.push_bind(&source);
    builder.push(
        ") AS nc(logical_name_id text, namespace text, declared_summary jsonb, provenance jsonb,
                 zero_owner boolean, recompose_at bigint)
         CROSS JOIN LATERAL (
             SELECT ",
    );
    builder.push_bind(&publication.chain_id);
    builder.push(
        "::text AS chain_id, nc.logical_name_id, nc.namespace,
                nc.provenance #>> '{authority_selection,authority_arm}' AS authority_arm,
                nc.provenance #>> '{read_reachability,serving_resource_id}' IS NOT NULL
                    AS serving,
                nc.declared_summary #>> '{registration,status}' AS registration_status, ",
    );
    push_expires_at_timestamp_expr(&mut builder);
    builder.push(" AS expires_at, ");
    push_registered_at_timestamp_expr(&mut builder);
    builder.push(
        " AS registered_at, nc.zero_owner, to_timestamp(nc.recompose_at) AS recompose_at) summary",
    );
    let rows: Vec<(String, Value)> = builder
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .context("failed to shape the name summaries")?;
    Ok(rows.into_iter().collect())
}
