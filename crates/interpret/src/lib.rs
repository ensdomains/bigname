//! Schema-v2 interpretation orchestration and its plain derived-data write layer.

mod engine;
mod error;
mod load;
mod recompute;
mod write;

pub use engine::{
    BatchOutcome, BatchRequest, DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES, Engine, Marker, RunMode,
};
pub use error::{ErrorKind, InterpretError, Result};
pub use recompute::{RecomputeSummary, finalize_recompute_flags};

pub const NORMALIZATION_STATE_REPAIR_REASON: &str =
    "run interpret recompute-flags before retrying ordinary interpretation";

#[cfg(test)]
mod tests {
    use bigname_adapters::schema_v2::seam::{
        ADMISSION_DISCOVERY_EDGE_KINDS, BINDING_CLOSE_CLAMP_SQL, EVENT_CLOSE_TIME_SQL,
        INTERPRETER_STATE_KEY, LOG_INDEX_KEY, OBSERVATION_KEY, PREIMAGE_OBSERVATION_EVENT_KIND,
        PROVENANCE_KIND_KEY, RAW_BLOCK_PROVENANCE_KIND, REDO_BINDING_CLOSE_CLAMP_SQL,
        STATE_SCOPE_KEY, SURFACE_BINDING_ID_KEY, SURFACE_BOUND_EVENT_KIND,
        SURFACE_UNBOUND_EVENT_KIND, TOKEN_LINEAGE_ID_KEY, TRANSACTION_INDEX_KEY,
    };

    fn is_scanned_rust_source(path: &std::path::Path) -> bool {
        path.extension().and_then(std::ffi::OsStr::to_str) == Some("rs")
    }

    fn interpret_rust_sources() -> Vec<std::path::PathBuf> {
        fn visit(directory: &std::path::Path, output: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(directory).expect("read interpret source directory") {
                let path = entry.expect("read interpret source entry").path();
                if path.is_dir() {
                    visit(&path, output);
                } else if is_scanned_rust_source(&path) {
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

    fn source_copies_literal_vocabulary(source: &str, value: &str) -> bool {
        source.contains(&format!("\"{value}\"")) || source.contains(&format!("'{value}'"))
    }

    fn normalize_formula(value: &str) -> String {
        value
            .chars()
            .filter(|character| !character.is_whitespace() && *character != '\\')
            .collect()
    }

    fn source_copies_formula(source: &str, formula: &str) -> bool {
        normalize_formula(source).contains(&normalize_formula(formula))
    }

    #[test]
    fn adapter_seam_vocabulary_has_no_interpret_side_literal_copy() {
        let transport_sources = interpret_rust_sources()
            .into_iter()
            .map(|path| std::fs::read_to_string(path).expect("read interpret Rust source"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut literal_vocabulary = vec![
            PREIMAGE_OBSERVATION_EVENT_KIND,
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
        literal_vocabulary.extend_from_slice(ADMISSION_DISCOVERY_EDGE_KINDS);
        for value in literal_vocabulary {
            assert!(
                !source_copies_literal_vocabulary(&transport_sources, value),
                "adapter seam vocabulary {value:?} was copied into the persistence transport"
            );
        }
        for formula in [
            EVENT_CLOSE_TIME_SQL,
            BINDING_CLOSE_CLAMP_SQL,
            REDO_BINDING_CLOSE_CLAMP_SQL,
        ] {
            assert!(
                !source_copies_formula(&transport_sources, formula),
                "adapter seam formula {formula:?} was copied into the persistence transport"
            );
        }
    }

    #[test]
    fn seam_guard_rejects_formula_copied_inside_larger_sql() {
        let planted = [
            "SELECT ",
            "lineage.block_timestamp + make_",
            "interval(secs => COALESCE(event.log_index, 0)::double precision / 1000000.0)",
            " AS closed_at",
        ]
        .concat();
        assert!(source_copies_formula(&planted, EVENT_CLOSE_TIME_SQL));
    }

    #[test]
    fn seam_guard_rejects_rust_line_continued_formula_copy() {
        let planted = [
            "\"lineage.block_timestamp + make_",
            "interval(\\\n",
            "    secs => COALESCE(event.log_index, 0)::double precision / 1000000.0\\\n",
            ")\"",
        ]
        .concat();
        assert!(source_copies_formula(&planted, EVENT_CLOSE_TIME_SQL));
    }

    #[test]
    fn seam_guard_scans_interpret_files_named_seam() {
        let planted = [
            "lineage.block_timestamp + make_",
            "interval(secs => COALESCE(event.log_index, 0)::double precision / 1000000.0)",
        ]
        .concat();
        assert!(is_scanned_rust_source(std::path::Path::new(
            "nested/seam.rs"
        )));
        assert!(source_copies_formula(&planted, EVENT_CLOSE_TIME_SQL));
    }
}
