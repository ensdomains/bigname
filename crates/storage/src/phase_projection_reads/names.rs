use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::{IdentityNameRecordRow, NameCurrentRow};

pub async fn load_phase_identity_records_by_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityNameRecordRow>> {
    load_phase_identity_records(pool, logical_name_ids, true).await
}

pub async fn load_phase_identity_name_feed_records_by_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityNameRecordRow>> {
    load_phase_identity_records(pool, logical_name_ids, false).await
}

async fn load_phase_identity_records(
    pool: &PgPool,
    logical_name_ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    let requested = dedupe(logical_name_ids);
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    super::family_identity::load(pool, &requested, include_inventory).await
}

pub async fn load_phase_name_current_rows_by_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    crate::families::name::load_family_names_by_logical_name_ids(pool, logical_name_ids).await
}

/// The bound-name predicates of `load_phase_resolver_bound_name_rows` over a name row `nc`, with
/// the resolver's served row joined as `resolver_capability`, `$1` the chain and `$2` the
/// resolver address: listing eligibility, the resolver match and the serving-only capability
/// gate. The composed bound-name reader (`families::name::bound`) applies the same text to the
/// composed rows.
pub(crate) const BOUND_NAME_PREDICATES: &str = r#"
  nc.support_status IN ('supported', 'unsupported')
  -- Retained pointer evidence alone does not list a name whose authority is not projected;
  -- an ENSv2 TLD's current root-registry pointer is its serving resource and does.
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
  AND nc.declared_summary #>> '{resolver,chain_id}' = $1
  AND lower(nc.declared_summary #>> '{resolver,address}') = lower($2)
  AND (
      nc.resource_id IS NOT NULL
      OR nc.serving_resource_id IS NULL
      OR resolver_capability.declared_summary #>> '{bindings,status}' = 'supported'
  )
"#;

pub(super) fn normalize_phase_name(
    logical_name_id: &str,
    raw_name: &str,
) -> Result<bigname_domain::normalization::NormalizedEnsName> {
    bigname_domain::normalization::normalize_name(raw_name).with_context(|| {
        format!("phase name row {logical_name_id} has an unreadable active raw_name")
    })
}

pub(super) fn phase_labelhash(
    normalized: &bigname_domain::normalization::NormalizedEnsName,
) -> Option<String> {
    normalized.normalized_labels.first().map(|label| {
        format!(
            "0x{}",
            alloy_primitives::hex::encode(alloy_primitives::keccak256(label.as_bytes()))
        )
    })
}

fn dedupe(values: &[String]) -> Vec<String> {
    values
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
