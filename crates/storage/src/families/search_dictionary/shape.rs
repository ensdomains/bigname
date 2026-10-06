//! Project search fields from the existing authoritative composition. No new authority selection.
use crate::{
    NameCurrentRow,
    public_name_fields::{SearchFields, ens_v1, registration_fields},
};
use anyhow::Result;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct PublicationFields {
    pub search_supported: bool,
    pub owner: Option<String>,
    pub public_authority: Option<String>,
    pub search_fields: Option<SearchFields>,
    pub search_creation_transport_resource_id: Option<Uuid>,
    pub display_name_override: Option<String>,
}

pub fn from_composed(row: Option<&NameCurrentRow>, raw_backed: bool) -> Result<PublicationFields> {
    let supported = row.is_some_and(|row| {
        row.coverage
            .get("status")
            .and_then(serde_json::Value::as_str)
            != Some("unsupported")
    });
    let mut fields = PublicationFields {
        search_supported: supported,
        owner: None,
        public_authority: None,
        search_fields: None,
        search_creation_transport_resource_id: None,
        display_name_override: None,
    };
    if !supported {
        return Ok(fields);
    }
    let row = row.expect("supported row must exist");
    let registration = registration_fields(
        &row.namespace,
        &row.declared_summary,
        row.surface_binding_id.is_some() || row.resource_id.is_some() || row.binding_kind.is_some(),
    );
    let authority = crate::name_current_public_authority(&row.provenance);
    if registration.created_at_declared.is_none()
        && row.namespace == "basenames"
        && row.provenance["chain_id"].as_str() == Some("base-mainnet")
        && (row
            .binding_kind
            .is_some_and(|kind| kind.as_str() == "declared_registry_path")
            || row.surface_binding_id.is_none())
    {
        fields.search_creation_transport_resource_id = row.serving_resource_id.or(row.resource_id);
    }
    fields.owner = registration.owner.clone();
    fields.public_authority = authority.map(str::to_owned);
    fields.search_fields = Some(SearchFields {
        registration,
        ens_v1: ens_v1(authority, &row.declared_summary)?,
    });
    if raw_backed && row.canonical_display_name != row.normalized_name {
        fields.display_name_override = Some(row.canonical_display_name.clone());
    }
    Ok(fields)
}
