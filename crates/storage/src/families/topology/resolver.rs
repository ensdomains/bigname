//! The resolver overview's shadow reads: the classification row
//! (`project_resolver_classification`, which holds what the overview serves from
//! `resolver_current` without the sampled sections) and `bound_names` over the resolver index of
//! `project_resource_pointer` joined to name eligibility.

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;

/// Where a shadow classification came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassificationSource {
    /// The `project_resolver_classification` row.
    Family,
    /// The former fallback for a resolver with no row: the latest active declaration manifest
    /// naming the address. No reader produces it since TYR-36 step 7b slice 4 deleted the
    /// fallback; the variant stays for the harness's source count until step 7c.
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

/// The overview's classification: the `project_resolver_classification` row, none without one.
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
    // No classification row: the resolver is not classified at this publication. The former
    // declaration-manifest fallback is gone (TYR-36 step 7b slice 4): F3 is complete once the
    // families have caught up, which the harness requires (`classification_sources`).
    Ok(None)
}
