//! Schema-v2 interpretation orchestration and its plain derived-data write layer.

mod engine;
mod error;
mod load;
mod write;

pub use engine::{BatchOutcome, BatchRequest, Engine, Marker, RunMode};
pub use error::{ErrorKind, InterpretError, Result};

pub const RECOMPUTE_FLAGS_UNAVAILABLE_REASON: &str = "interpret recompute-flags is unavailable: label flags cannot be published until name-surface visibility and active binding reconciliation are implemented";

#[cfg(test)]
mod tests {
    use bigname_adapters::schema_v2::seam::{
        ADMISSION_DISCOVERY_EDGE_KINDS, INTERPRETER_STATE_KEY, LOG_INDEX_KEY, OBSERVATION_KEY,
        PROVENANCE_KIND_KEY, RAW_BLOCK_PROVENANCE_KIND, STATE_SCOPE_KEY, SURFACE_BINDING_ID_KEY,
        SURFACE_BOUND_EVENT_KIND, SURFACE_UNBOUND_EVENT_KIND, TOKEN_LINEAGE_ID_KEY,
        TRANSACTION_INDEX_KEY,
    };

    #[test]
    fn adapter_seam_vocabulary_has_no_interpret_side_literal_copy() {
        let transport_sources = [
            include_str!("engine.rs"),
            include_str!("load.rs"),
            include_str!("load/prior.rs"),
            include_str!("write.rs"),
            include_str!("write/discovery.rs"),
            include_str!("write/identity.rs"),
            include_str!("write/identity_names.rs"),
            include_str!("write/normalized.rs"),
        ]
        .join("\n");
        let mut vocabulary = vec![
            SURFACE_BOUND_EVENT_KIND,
            SURFACE_UNBOUND_EVENT_KIND,
            SURFACE_BINDING_ID_KEY,
            TOKEN_LINEAGE_ID_KEY,
            INTERPRETER_STATE_KEY,
            STATE_SCOPE_KEY,
            OBSERVATION_KEY,
            TRANSACTION_INDEX_KEY,
            LOG_INDEX_KEY,
            PROVENANCE_KIND_KEY,
            RAW_BLOCK_PROVENANCE_KIND,
        ];
        vocabulary.extend_from_slice(ADMISSION_DISCOVERY_EDGE_KINDS);
        for value in vocabulary {
            assert!(
                !transport_sources.contains(&format!("\"{value}\""))
                    && !transport_sources.contains(&format!("'{value}'")),
                "adapter seam vocabulary {value:?} was copied into the persistence transport"
            );
        }
    }
}
