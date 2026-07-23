mod invalidations;
mod reads;
mod source_decode;
mod source_stream;
mod sources;
mod types;
mod writes;

pub(crate) use invalidations::enqueue_children_current_invalidations_for_parent_surfaces;
pub use reads::{
    load_children_current, load_children_current_including_noncanonical,
    load_children_current_page, load_children_current_summaries,
};
pub use source_stream::{
    stream_canonical_declared_child_sources, stream_canonical_declared_child_sources_after,
};
pub use sources::{
    load_canonical_declared_child_sources, load_canonical_ens_v1_declared_child_sources,
};
pub use types::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentRow, ChildrenCurrentSummary,
    DeclaredChildEventSource,
};
pub use writes::{clear_children_current, delete_children_current, upsert_children_current_rows};

const DECLARED_SURFACE_CLASS: &str = "declared";
const SUBREGISTRY_EVENT_KIND: &str = "SubregistryChanged";
const PARENT_EVENT_KIND: &str = "ParentChanged";
const REGISTRATION_GRANTED_EVENT_KIND: &str = "RegistrationGranted";
const REGISTRATION_RENEWED_EVENT_KIND: &str = "RegistrationRenewed";
const REGISTRATION_RELEASED_EVENT_KIND: &str = "RegistrationReleased";
const SUBREGISTRY_DERIVATION_KIND: &str = "ens_v1_subregistry_changed";
const ENSV2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ENSV1_SUBREGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
const BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
const ENSV2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENSV2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
const DEFAULT_CHILDREN_CURRENT_READ_FILTER: &str = r#"
  AND parent.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND (
      child.logical_name_id IS NULL
      OR cc.provenance #>> '{label,source}' = 'label_preimage'
      OR child.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
  )
"#;

#[cfg(test)]
mod tests;
