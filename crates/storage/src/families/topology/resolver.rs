//! The resolver overview's shadow reads: the classification row
//! (`project_resolver_classification`, which holds what the overview serves from
//! `resolver_current` without the sampled sections) and `bound_names` over the resolver index of
//! `project_resource_pointer` joined to name eligibility.
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::PgPool;

use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentRow,
    load_phase_name_current_rows_by_ids,
    name_current::{DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER},
};

/// Where a shadow classification came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassificationSource {
    /// The `project_resolver_classification` row.
    Family,
    /// No row for the address (step 2 fills the table block by block, so this covers a resolver
    /// the families have not classified yet): the latest active declaration manifest that names
    /// the address as a contract. This is a partial stand-in, not the resolver builder's classification: it
    /// gives the source family, role and mirror only, and does not reproduce declaration
    /// precedence, discovery admission, start blocks, upgrade implementations, proxy kinds or
    /// support status.
    DeclarationManifest,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FamilyResolverClassification {
    pub source: ClassificationSource,
    pub chain_id: String,
    pub resolver_address: String,
    /// The classification object, or `{source_family, role}` from the declaration.
    pub classification: Value,
    pub support_status: Option<String>,
    pub unsupported_reason: Option<String>,
    pub manifest_id: Option<i64>,
    pub manifest_event_id: Option<i64>,
    pub admission_namespace: Option<String>,
    pub summary_version: Option<String>,
}

impl FamilyResolverClassification {
    /// The ENSv1 registry a declared mirror resolver reads, as the overview serves it.
    pub fn mirrored_registry_address(&self) -> Option<String> {
        self.classification
            .get("mirror")?
            .get("mirrored_registry_address")?
            .as_str()
            .map(str::to_ascii_lowercase)
    }
}

/// The overview's classification: the `project_resolver_classification` row, else the
/// declaration manifest.
pub async fn load_resolver_shadow(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<FamilyResolverClassification>> {
    let address = resolver_address.to_ascii_lowercase();
    type FamilyRow = (
        Option<Value>,
        String,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
    );
    let row: Option<FamilyRow> = sqlx::query_as(
        "SELECT classification, support_status, unsupported_reason, manifest_id,
                manifest_event_id, admission_namespace, summary_version
         FROM bigname_phase.project_resolver_classification
         WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(chain_id)
    .bind(&address)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the classification row of {chain_id}:{address}"))?;
    if let Some((
        classification,
        support_status,
        unsupported_reason,
        manifest_id,
        manifest_event_id,
        namespace,
        summary_version,
    )) = row
    {
        return Ok(Some(FamilyResolverClassification {
            source: ClassificationSource::Family,
            chain_id: chain_id.to_owned(),
            resolver_address: address,
            classification: classification.unwrap_or(Value::Null),
            support_status: Some(support_status),
            unsupported_reason,
            manifest_id,
            manifest_event_id,
            admission_namespace: namespace,
            summary_version,
        }));
    }
    // The latest readable update of each manifest at or below the family marker's block, in
    // the order today's manifest staging uses (crates/project/src/stage.rs,
    // `create_manifests`: latest written first); only then is it asked whether it is active.
    let declared: Option<(Value, Option<i64>, String)> = sqlx::query_as(
        "WITH clock AS (
             SELECT current_block_number AS block_number
             FROM bigname_phase.project_family_marker WHERE chain_id = $1
         ), latest AS (
             SELECT DISTINCT ON (event.source_manifest_id)
                    event.source_manifest_id, event.normalized_event_id, event.namespace,
                    event.source_family, event.after_state
             FROM bigname_phase.normalized_events event
             LEFT JOIN bigname_phase.chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             WHERE event.chain_id = $1
               AND event.event_kind = 'SourceManifestUpdated'
               AND event.source_manifest_id IS NOT NULL
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (event.block_hash IS NULL
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
               AND (event.block_number IS NULL
                    OR event.block_number <= (SELECT block_number FROM clock))
             ORDER BY event.source_manifest_id, event.normalized_event_id DESC
         )
         SELECT jsonb_strip_nulls(jsonb_build_object(
                    'source_family', manifest.source_family,
                    'role', declaration ->> 'role',
                    'mirror', CASE WHEN declaration ->> 'role' = 'ensv1_mirror_resolver'
                        THEN jsonb_strip_nulls(jsonb_build_object(
                            'mirrored_source_family', 'ens_v1_resolver_l1',
                            'mirrored_registry_source_family', 'ens_v1_registry_l1',
                            'mirrored_registry_address', lower(manifest.after_state
                                #>> '{manifest_payload,correlation_addresses,ens_v1_registry}')))
                        END)),
                manifest.source_manifest_id, manifest.namespace
         FROM latest manifest
         CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
             manifest.after_state #> '{manifest_payload,contracts}', '[]'::jsonb)) declaration
         WHERE manifest.after_state ->> 'rollout_status' = 'active'
           AND lower(declaration ->> 'address') = $2
         ORDER BY manifest.normalized_event_id DESC
         LIMIT 1",
    )
    .bind(chain_id)
    .bind(&address)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the declaration of {chain_id}:{address}"))?;
    Ok(declared.map(
        |(classification, manifest_id, namespace)| FamilyResolverClassification {
            source: ClassificationSource::DeclarationManifest,
            chain_id: chain_id.to_owned(),
            resolver_address: address,
            classification,
            support_status: None,
            unsupported_reason: None,
            manifest_id,
            manifest_event_id: None,
            admission_namespace: Some(namespace),
            summary_version: None,
        },
    ))
}

/// The names bound to a resolver: resources whose current pointer names it, through
/// `project_resource_pointer_resolver_idx`, joined to name eligibility. Until name eligibility
/// and the serving selection are read from the family tables, eligibility is `name_current`'s
/// predicate block of `load_phase_resolver_bound_name_rows`; only the resolver match moves to
/// `project_resource_pointer`. A name's pointer is the one on its selected resource
/// (interim: `name_current.resource_id`, else its serving resource), as today's name row takes its
/// resolver from the selected authority's events; a pointer on another resource of the name does
/// not move it. Same keyset and order as the served reader.
pub async fn load_bound_names_shadow(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    namespace: Option<&str>,
    cursor: Option<&NameCurrentListCursor>,
    limit: i64,
) -> Result<Vec<NameCurrentRow>> {
    let cursor_values = cursor
        .map(|cursor| match &cursor.sort_value {
            NameCurrentListCursorValue::Name(_) => Ok((
                cursor.normalized_name.as_str(),
                cursor.namespace.as_str(),
                cursor.namehash.as_str(),
            )),
            _ => bail!("bound-name shadow cursor must use name ordering"),
        })
        .transpose()?;
    let query = format!(
        r#"
        WITH pointed AS (
            -- Only the pointer of the name's selected resource counts, as today's name row takes
            -- its resolver from the selected authority's events. Interim: the selected resource is
            -- `name_current.resource_id`, else its serving resource, until the selection is
            -- computed from the binding candidates.
            SELECT DISTINCT pointer.namespace || ':' || pointer.namehash AS logical_name_id
            FROM bigname_phase.project_resource_pointer pointer
            JOIN bigname_phase.name_current selected
              ON selected.logical_name_id = pointer.namespace || ':' || pointer.namehash
             AND pointer.resource_id = COALESCE(selected.resource_id, selected.serving_resource_id)
            WHERE pointer.chain_id = $1 AND pointer.resolver_address = lower($2)
              AND pointer.namehash IS NOT NULL
        )
        SELECT nc.logical_name_id
        FROM pointed
        JOIN bigname_phase.name_current nc ON nc.logical_name_id = pointed.logical_name_id
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN bigname_phase.resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN bigname_phase.surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN bigname_phase.token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        LEFT JOIN bigname_phase.resolver_current resolver_capability
          ON resolver_capability.chain_id = $1
         AND lower(resolver_capability.resolver_address) = lower($2)
        {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
        WHERE nc.support_status IN ('supported', 'unsupported')
          AND (
              nc.unsupported_reason IS DISTINCT FROM 'current_authority_not_projected'
              OR nc.provenance #>> '{{read_reachability,basis}}' =
                  'root_registry_resolver_pointer'
          )
          {DEFAULT_NAME_CURRENT_READ_FILTER}
          AND (
              (
                  nc.surface_binding_id IS NOT NULL
                  AND nc.declared_summary #>> '{{registration,status}}' IS DISTINCT FROM 'released'
                  AND NULLIF(btrim(COALESCE(
                          nc.declared_summary #>> '{{registration,released_at}}', ''
                      )), '') IS NULL
                  AND (
                      nc.declared_summary #>> '{{registration,authority_kind}}' = 'registrar'
                      OR (
                          nc.declared_summary #>> '{{registration,authority_kind}}'
                              IN ('registry_only', 'ens_v2_registry')
                          AND NULLIF(btrim(COALESCE(
                              nc.declared_summary #>> '{{control,owner}}',
                              nc.declared_summary #>> '{{control,registry_owner}}', ''
                          )), '') IS NOT NULL
                      )
                      OR (
                          nc.declared_summary #>> '{{registration,authority_kind}}' = 'wrapper'
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
                  AND nc.provenance #>> '{{read_reachability,basis}}' IN (
                      'retained_registry_resolver_pointer', 'root_registry_resolver_pointer'
                  )
              )
          )
          AND (
              nc.resource_id IS NOT NULL
              OR nc.serving_resource_id IS NULL
              OR resolver_capability.declared_summary #>> '{{bindings,status}}' = 'supported'
          )
          AND ($3::TEXT IS NULL OR nc.namespace = $3)
          AND (
              $4::TEXT IS NULL
              OR (nc.raw_name, nc.namespace, nc.namehash) > ($4, $5, $6)
          )
        ORDER BY nc.raw_name, nc.namespace, nc.namehash
        LIMIT $7
        "#
    );
    let ids: Vec<String> = sqlx::query_scalar(&query)
        .bind(chain_id)
        .bind(resolver_address)
        .bind(namespace)
        .bind(cursor_values.map(|values| values.0))
        .bind(cursor_values.map(|values| values.1))
        .bind(cursor_values.map(|values| values.2))
        .bind(limit)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load the bound-name shadow of {chain_id}:{resolver_address}")
        })?;
    let mut rows: BTreeMap<String, NameCurrentRow> =
        load_phase_name_current_rows_by_ids(pool, &ids).await?;
    ids.iter()
        .map(|id| {
            rows.remove(id)
                .with_context(|| format!("bound-name shadow row {id} vanished"))
        })
        .collect()
}
