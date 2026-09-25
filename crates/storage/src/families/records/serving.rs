//! The pointer a resource serves records through, and the outer gate over it: the current F5
//! pointer when it is not a clear (linked_records.rs, `project_record_pointers`), its
//! support from the resolver classification (record_inventory.rs, `pointer_eligibility`).
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    FamilyResourcePointer,
    facts::{ResolverClassification, probe_events},
    is_cleared,
};

/// One resource's serving pointer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServingPointer {
    pub(crate) resource_id: Uuid,
    pub(crate) logical_name_id: String,
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) namehash: String,
    pub(crate) resolver_address: String,
    pub(crate) pointer_event_id: Option<i64>,
    pub(crate) block_number: i64,
}

/// The serving pointer of an F5 row, `None` when the current pointer is a clear or the resource
/// has only version changes. The row keeps neither the pointer event's normalized event id (when
/// a later version change owns the row) nor its logical name, so both are read back from
/// `normalized_events` by the pointer's event identity.
pub(crate) async fn serving_pointer(
    pool: &PgPool,
    pointer: &FamilyResourcePointer,
) -> Result<Option<ServingPointer>> {
    let (Some(resolver_address), Some(position)) =
        (&pointer.resolver_address, &pointer.pointer_position)
    else {
        return Ok(None);
    };
    if is_cleared(Some(resolver_address)) {
        return Ok(None);
    }
    let probed = probe_events(pool, std::slice::from_ref(&position.event_identity)).await?;
    let event = probed.get(&position.event_identity);
    let Some(logical_name_id) = event.and_then(|event| event.logical_name_id.clone()) else {
        anyhow::bail!(
            "the family pointer of {} names event {} with no logical name",
            pointer.resource_id,
            position.event_identity
        );
    };
    Ok(Some(ServingPointer {
        resource_id: pointer.resource_id,
        logical_name_id,
        namespace: pointer.namespace.clone().unwrap_or_default(),
        source_family: pointer.source_family.clone().unwrap_or_default(),
        namehash: pointer.namehash.clone().unwrap_or_default(),
        resolver_address: resolver_address.clone(),
        pointer_event_id: pointer
            .pointer_event_id_if_last()
            .or(event.map(|event| event.normalized_event_id)),
        block_number: position.block_number,
    }))
}

/// Whether the resource's records are served, and the reason when not: the resolver must be
/// classified and supported, and a manifest-declared resolver needs its declaration manifest in
/// the pointer's namespace.
pub(crate) fn family_pointer_eligibility(
    pointer_namespace: &str,
    classification: Option<&ResolverClassification>,
) -> (bool, Option<String>) {
    let Some(classification) = classification else {
        return (false, Some("resolver_classification_missing".to_owned()));
    };
    if !classification.supported() {
        return (
            false,
            Some(
                classification
                    .unsupported_reason
                    .clone()
                    .unwrap_or_else(|| "resolver_classification_missing".to_owned()),
            ),
        );
    }
    if classification.field("basis") == Some("manifest_declared_address")
        && !classification.declared_in(pointer_namespace)
    {
        return (false, Some("resolver_classification_missing".to_owned()));
    }
    (true, None)
}
