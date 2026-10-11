use anyhow::Result;
use bigname_domain::vocabulary::Namespace;
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
    /// The resource the name serves records through: its serving resource, else its bound
    /// resource; none when the current resolution path is absent or its retained target cannot
    /// be proved ([`Self::unresolvable_reason`], [`Self::resolution_unsupported_reason`]), and
    /// none for a released ENSv2 registration ([`Self::released_ens_v2_registration`]).
    pub fn record_serving_resource_id(&self) -> Option<Uuid> {
        if self.released_ens_v2_registration() {
            return None;
        }
        self.audit_record_resource_id()
    }

    /// [`Self::record_serving_resource_id`] without the release gate: the resource the records
    /// diagnostic audits, which a released ENSv2 registration keeps.
    pub fn audit_record_resource_id(&self) -> Option<Uuid> {
        if self.unresolvable_reason().is_some() || self.resolution_unsupported_reason().is_some() {
            return None;
        }
        self.serving_resource_id.or(self.resource_id)
    }

    /// Whether the row serves a released registration under the public `ens_v2` authority and
    /// no event-linked registry serving resource. Such a row serves no records. A released
    /// ENSv1 lease is not included.
    pub fn released_ens_v2_registration(&self) -> bool {
        crate::name_current_public_authority(&self.provenance) == Some("ens_v2")
            && crate::public_name_fields::registration_released(&self.declared_summary)
            && !name_current_has_event_linked_registry_serving(self)
    }

    /// Why the name serves no resolver or records although its authority has them: its
    /// post-cutover ENSv2 path has no resolver, including a missing live second-level entry.
    pub fn unresolvable_reason(&self) -> Option<&str> {
        self.declared_summary
            .get("unresolvable_reason")
            .and_then(Value::as_str)
    }

    /// Why retained resolution data cannot be attributed to the current ENSv2 path.
    pub fn resolution_unsupported_reason(&self) -> Option<&str> {
        self.declared_summary
            .get("resolution_unsupported_reason")
            .and_then(Value::as_str)
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

/// `name_current.provenance.read_reachability.basis` values that select a serving resource from
/// an event-linked registry resolver pointer: the retained pointer of an ownerless ENSv1 or
/// Basenames registry name, and the root-registry pointer of an ENSv2 TLD with no projected
/// authority (`docs/projections.md`).
pub const EVENT_LINKED_REGISTRY_SERVING_BASES: [&str; 2] = [
    "retained_registry_resolver_pointer",
    "root_registry_resolver_pointer",
];

pub fn name_current_has_event_linked_registry_serving(row: &NameCurrentRow) -> bool {
    row.resource_id.is_none()
        && row.surface_binding_id.is_none()
        && row.binding_kind.is_none()
        && row.serving_resource_id.is_some()
        && has_event_linked_registry_serving_provenance(&row.namespace, &row.provenance)
}

pub(crate) fn has_event_linked_registry_serving_provenance(
    namespace: &str,
    provenance: &Value,
) -> bool {
    (namespace == Namespace::Ens.as_str() || namespace == Namespace::Basenames.as_str())
        && provenance
            .pointer("/read_reachability/basis")
            .and_then(Value::as_str)
            .is_some_and(|basis| EVENT_LINKED_REGISTRY_SERVING_BASES.contains(&basis))
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
