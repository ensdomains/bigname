//! The rules of the resolver overview's bound-name list (`GET /v1/resolvers/{chain_id}/{address}`),
//! as SQL over a composed name row `nc`, kept in one place for the two sides that apply them:
//!
//! - The family step stores each name's per-name outcome on its name summary
//!   (docs/glossary.md, "Name summary") from the same composition (`summary.rs`): the served
//!   resolver, whether the name passes every rule except the resolver match and the capability
//!   gate, whether it is exempt from that gate, and the name the page orders it by.
//! - The bound-name reader (`bound.rs`) selects a page from those stored values and runs the
//!   whole rule set again over the page's composed rows.
//!
//! The capability gate itself, the resolver family's binding-enumeration support, is not stored.
//! The reader takes it from the resolver's classification row at request time, because a
//! classification can change while no name is composed again.
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};

use super::{compose::Surface, rendered::placeholder_name};
use crate::NameCurrentRow;

/// The most bytes an order name built from label hashes keeps: the length bound of the page
/// order indexes (`project_name_summary_bound_order_idx` and
/// `project_name_summary_bound_exempt_order_idx`).
pub(crate) const ORDER_NAME_BYTES: usize = 2000;

/// The name the bound-name page orders `surface` by: the name rendered from chain data alone,
/// that is its raw name, else every label as `[<labelhash>]` (`rendered::placeholder_name`, the
/// renderer with no preimage source). An imported preimage moves no name.
///
/// A label-hash path longer than [`ORDER_NAME_BYTES`] keeps its first bytes up to a character
/// boundary, so every such name is listed. Names that share those bytes then order by namespace
/// and namehash. A raw name is kept whole: the listing leaves out a raw name over the bound, as
/// search does.
pub(super) fn order_name(surface: &Surface) -> Result<String> {
    if let Some(raw_name) = &surface.raw_name {
        return Ok(raw_name.clone());
    }
    let mut name = placeholder_name(&surface.labelhashes)
        .with_context(|| format!("{} has no placeholder name", surface.logical_name_id))?;
    if name.len() > ORDER_NAME_BYTES {
        let end = (0..=ORDER_NAME_BYTES)
            .rev()
            .find(|end| name.is_char_boundary(*end))
            .unwrap_or_default();
        name.truncate(end);
    }
    Ok(name)
}

/// The column list of a composed row set bound as `jsonb_to_recordset`: the rows
/// [`bound_inputs`] and the listings' `source_row` build.
pub(crate) const COMPOSED_NC_COLUMNS: &str =
    "nc(logical_name_id text, namespace text, raw_name text, display_name text,
    namehash text, surface_binding_id uuid, resource_id uuid, serving_resource_id uuid,
    token_lineage_id uuid, binding_kind text, declared_summary jsonb, provenance jsonb,
    support_status text, unsupported_reason text, listing_eligible boolean, chain_positions jsonb,
    canonicality_summary jsonb, manifest_version bigint, last_recomputed_at timestamptz)";

/// The fields of a composed row the bound-name rules read, under their `COMPOSED_NC_COLUMNS`
/// names.
pub(crate) fn bound_inputs(row: &NameCurrentRow) -> Map<String, Value> {
    let unsupported = super::list_keys::unsupported(&row.coverage);
    let Value::Object(inputs) = json!({
        "logical_name_id": row.logical_name_id,
        "namespace": row.namespace,
        "surface_binding_id": row.surface_binding_id,
        "resource_id": row.resource_id,
        "serving_resource_id": row.serving_resource_id,
        "binding_kind": row.binding_kind.map(|kind| kind.as_str()),
        "declared_summary": row.declared_summary,
        "provenance": row.provenance,
        "support_status": if unsupported { "unsupported" } else { "supported" },
        "unsupported_reason": row.coverage.get("unsupported_reason"),
    }) else {
        unreachable!("the bound inputs are an object")
    };
    inputs
}

/// The chain of the resolver the composed row serves.
pub(crate) const BOUND_RESOLVER_CHAIN_SQL: &str = "nc.declared_summary #>> '{resolver,chain_id}'";

/// The lower-cased address of the resolver the composed row serves.
pub(crate) const BOUND_RESOLVER_ADDRESS_SQL: &str =
    "lower(nc.declared_summary #>> '{resolver,address}')";

/// Every bound-name rule except the resolver match and the capability gate: listing eligibility
/// by support, binding, registration and control.
pub(crate) const BOUND_LISTING_SQL: &str = r#"
  nc.support_status IN ('supported', 'unsupported')
  -- Retained pointer evidence alone does not list a name whose authority is not projected.
  -- An ENSv2 TLD's current root-registry pointer is its serving resource and does.
  AND (
      nc.unsupported_reason IS DISTINCT FROM 'current_authority_not_projected'
      OR nc.provenance #>> '{read_reachability,basis}' =
          'root_registry_resolver_pointer'
  )
  AND (
      (
          nc.surface_binding_id IS NOT NULL
          AND nc.declared_summary #>> '{registration,status}' IS DISTINCT FROM 'released'
          AND NULLIF(btrim(COALESCE(
                  nc.declared_summary #>> '{registration,released_at}', ''
              )), '') IS NULL
          AND (
              nc.declared_summary #>> '{registration,authority_kind}' = 'registrar'
              OR (
                  nc.declared_summary #>> '{registration,authority_kind}'
                      IN ('registry_only', 'ens_v2_registry')
                  AND NULLIF(btrim(COALESCE(
                      nc.declared_summary #>> '{control,owner}',
                      nc.declared_summary #>> '{control,registry_owner}', ''
                  )), '') IS NOT NULL
              )
              OR (
                  nc.declared_summary #>> '{registration,authority_kind}' = 'wrapper'
                  AND nc.namespace <> 'basenames'
              )
          )
      )
      OR (
          nc.surface_binding_id IS NULL
          AND nc.resource_id IS NULL
          AND nc.serving_resource_id IS NOT NULL
          AND nc.binding_kind IS NULL
          AND nc.namespace IN ('ens', 'basenames')
          AND nc.provenance #>> '{read_reachability,basis}' IN (
              'retained_registry_resolver_pointer', 'root_registry_resolver_pointer'
          )
      )
  )
"#;

/// The per-name part of the capability gate: a name with a selected resource, or with no serving
/// resource, is listed whatever the resolver family's binding-enumeration support.
pub(crate) const BOUND_CAPABILITY_EXEMPT_SQL: &str =
    "(nc.resource_id IS NOT NULL OR nc.serving_resource_id IS NULL)";

/// The stored bound-name values of one composed row `nc`: the summary's `resolver_chain_id`,
/// `resolver_address`, `resolver_listing_eligible`, `resolver_support_exempt` and
/// `order_name`, as a select list over a `COMPOSED_NC_COLUMNS` recordset whose `raw_name` is the
/// row's [`order_name`].
pub(crate) fn stored_bound_columns_sql() -> String {
    format!(
        "nc.logical_name_id, nc.raw_name AS order_name,
         {BOUND_RESOLVER_CHAIN_SQL} AS resolver_chain_id,
         {BOUND_RESOLVER_ADDRESS_SQL} AS resolver_address,
         COALESCE(({BOUND_LISTING_SQL}), FALSE) AS resolver_listing_eligible,
         COALESCE({BOUND_CAPABILITY_EXEMPT_SQL}, FALSE) AS resolver_support_exempt"
    )
}
