use anyhow::Result;
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::SurfaceBindingKind;

/// Persisted current exact-name projection row served by API reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub surface_binding_id: Option<Uuid>,
    pub resource_id: Option<Uuid>,
    pub serving_resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub binding_kind: Option<SurfaceBindingKind>,
    pub declared_summary: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

impl NameCurrentRow {
    pub fn record_serving_resource_id(&self) -> Option<Uuid> {
        self.serving_resource_id.or(self.resource_id)
    }

    /// Load current exact-name projection rows keyed by logical name identity.
    ///
    /// Missing rows are omitted. Duplicate requested ids collapse into one map entry, and map
    /// iteration is sorted by `logical_name_id`; callers that need page order should iterate the
    /// original page and look up rows in the returned map.
    pub async fn load_by_logical_name_ids(
        pool: &sqlx::PgPool,
        logical_name_ids: &[String],
    ) -> Result<std::collections::BTreeMap<String, NameCurrentRow>> {
        super::load_name_current_by_logical_name_ids(pool, logical_name_ids).await
    }
}

pub(super) fn decode_name_current_row(row: PgRow) -> Result<NameCurrentRow> {
    let binding_kind = crate::sql_row::get(&row, "binding_kind")?;

    Ok(NameCurrentRow {
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        surface_binding_id: crate::sql_row::get(&row, "surface_binding_id")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        serving_resource_id: crate::sql_row::get(&row, "serving_resource_id")?,
        token_lineage_id: crate::sql_row::get(&row, "token_lineage_id")?,
        binding_kind,
        declared_summary: crate::sql_row::get(&row, "declared_summary")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}
