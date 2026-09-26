//! F5, the resource resolver pointer (`project_resource_pointer`): one row per resource with three
//! column groups, the current pointer with clears, the latest non-zero pointer and the record
//! version boundary (docs/projections.md, "Owned key families").
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::FamilyPosition;

/// One `project_resource_pointer` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyResourcePointer {
    pub chain_id: String,
    pub resource_id: Uuid,
    /// The event that last wrote the row, a pointer or a version change.
    pub position: FamilyPosition,
    pub normalized_event_id: Option<i64>,
    /// The latest named `ResolverChanged` on the resource, clears included: the zero address or
    /// null for a clear, null when the resource only has version changes.
    pub resolver_address: Option<String>,
    pub pointer_position: Option<FamilyPosition>,
    pub namespace: Option<String>,
    pub source_family: Option<String>,
    /// The lower-cased namehash of the pointer event's name.
    pub namehash: Option<String>,
    /// The latest `ResolverChanged` whose resolver is not the zero address.
    pub nonzero_resolver_address: Option<String>,
    pub nonzero_position: Option<FamilyPosition>,
    /// The latest `RecordVersionChanged` or `ResolverChanged` on the resource, clears included.
    pub boundary_kind: Option<String>,
    pub boundary_position: Option<FamilyPosition>,
    pub boundary_block_timestamp: Option<OffsetDateTime>,
}

impl FamilyResourcePointer {
    /// The pointer event's normalized event id when the pointer event is the row's last writer.
    /// Otherwise a later version change owns the row and the id has to be read from
    /// `normalized_events` by the pointer's event identity.
    pub fn pointer_event_id_if_last(&self) -> Option<i64> {
        let pointer = self.pointer_position.as_ref()?;
        (pointer.event_identity == self.position.event_identity)
            .then_some(self.normalized_event_id)
            .flatten()
    }
}

/// The F5 row of `resource_id`.
pub async fn load_family_resource_pointer(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
) -> Result<Option<FamilyResourcePointer>> {
    let row = sqlx::query(
        "SELECT chain_id, resource_id, block_number, transaction_index, log_index,
                event_identity, normalized_event_id, resolver_address, pointer_position,
                namespace, source_family, namehash, nonzero_resolver_address, nonzero_position,
                boundary_kind, boundary_position, boundary_block_timestamp
         FROM bigname_phase.project_resource_pointer
         WHERE chain_id = $1 AND resource_id = $2",
    )
    .bind(chain_id)
    .bind(resource_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the family resource pointer of {resource_id}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let position = |column: &str| -> Result<Option<FamilyPosition>> {
        Ok(row
            .try_get::<Option<Value>, _>(column)?
            .as_ref()
            .and_then(FamilyPosition::from_json))
    };
    Ok(Some(FamilyResourcePointer {
        chain_id: row.try_get("chain_id")?,
        resource_id: row.try_get("resource_id")?,
        position: FamilyPosition::from_row(&row)?,
        normalized_event_id: row.try_get("normalized_event_id")?,
        resolver_address: row.try_get("resolver_address")?,
        pointer_position: position("pointer_position")?,
        namespace: row.try_get("namespace")?,
        source_family: row.try_get("source_family")?,
        namehash: row.try_get("namehash")?,
        nonzero_resolver_address: row.try_get("nonzero_resolver_address")?,
        nonzero_position: position("nonzero_position")?,
        boundary_kind: row.try_get("boundary_kind")?,
        boundary_position: position("boundary_position")?,
        boundary_block_timestamp: row.try_get("boundary_block_timestamp")?,
    }))
}
