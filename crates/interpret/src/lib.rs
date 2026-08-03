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
        ADMISSION_DISCOVERY_EDGE_KINDS, BINDING_CLOSE_CLAMP_SQL, EVENT_CLOSE_TIME_SQL,
        INTERPRETER_STATE_KEY, LOG_INDEX_KEY, OBSERVATION_KEY, PROVENANCE_KIND_KEY,
        RAW_BLOCK_PROVENANCE_KIND, REDO_BINDING_CLOSE_CLAMP_SQL, STATE_SCOPE_KEY,
        SURFACE_BINDING_ID_KEY, SURFACE_BOUND_EVENT_KIND, SURFACE_UNBOUND_EVENT_KIND,
        TOKEN_LINEAGE_ID_KEY, TRANSACTION_INDEX_KEY,
    };

    fn interpret_rust_sources() -> Vec<std::path::PathBuf> {
        fn visit(directory: &std::path::Path, output: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(directory).expect("read interpret source directory") {
                let path = entry.expect("read interpret source entry").path();
                if path.is_dir() {
                    visit(&path, output);
                } else if path.extension().and_then(std::ffi::OsStr::to_str) == Some("rs") {
                    output.push(path);
                }
            }
        }

        let mut sources = Vec::new();
        visit(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut sources,
        );
        sources.sort();
        sources
    }

    #[test]
    fn adapter_seam_vocabulary_has_no_interpret_side_literal_copy() {
        let transport_sources = interpret_rust_sources()
            .into_iter()
            .map(|path| std::fs::read_to_string(path).expect("read interpret Rust source"))
            .collect::<Vec<_>>()
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
            EVENT_CLOSE_TIME_SQL,
            BINDING_CLOSE_CLAMP_SQL,
            REDO_BINDING_CLOSE_CLAMP_SQL,
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
