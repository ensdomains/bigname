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
        ADMISSION_DISCOVERY_EDGE_KINDS, BINDING_CLOSE_CLAMP_SQL, CHILD_CLEANUP_EVENT_KINDS,
        EVENT_CLOSE_TIME_SQL, INTERPRETER_STATE_KEY, LOG_INDEX_KEY, MIGRATION_APPLIED_EVENT_KIND,
        OBSERVATION_KEY, PREIMAGE_OBSERVATION_EVENT_KIND, PROVENANCE_KIND_KEY,
        RAW_BLOCK_PROVENANCE_KIND, REDO_BINDING_CLOSE_CLAMP_SQL, REDO_CLOSED_ARM_SQL,
        REDO_RESOLVER_EVIDENCE_SELECT_SQL, STATE_SCOPE_KEY, SURFACE_BINDING_ID_KEY,
        SURFACE_BOUND_EVENT_KIND, SURFACE_UNBOUND_EVENT_KIND, TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
        TOKEN_LINEAGE_ID_KEY, TRANSACTION_INDEX_KEY,
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
            TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
        ];
        literal_vocabulary.extend_from_slice(ADMISSION_DISCOVERY_EDGE_KINDS);
        literal_vocabulary.extend_from_slice(CHILD_CLEANUP_EVENT_KINDS);
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
            REDO_CLOSED_ARM_SQL,
            REDO_RESOLVER_EVIDENCE_SELECT_SQL,
        ] {
            assert!(
                !source_copies_formula(&transport_sources, formula),
                "adapter seam formula {formula:?} was copied into the persistence transport"
            );
        }
    }

    // The planted copies are built from the constant rather than spelled out, so this file never
    // contains a literal the guard would flag, and a formula edit cannot quietly stop these from
    // testing the formula they name.
    #[test]
    fn seam_guard_rejects_formula_copied_inside_larger_sql() {
        let planted = format!("SELECT {EVENT_CLOSE_TIME_SQL} AS closed_at");
        assert!(source_copies_formula(&planted, EVENT_CLOSE_TIME_SQL));
    }

    #[test]
    fn seam_guard_rejects_rust_line_continued_formula_copy() {
        let planted = format!("\"{}\"", EVENT_CLOSE_TIME_SQL.replace(' ', "\\\n    "));
        assert!(source_copies_formula(&planted, EVENT_CLOSE_TIME_SQL));
    }

    // A migration boundary's successor binding is on the opposite arm from the close it made, so a
    // boundary missing its recorded predecessor arm must resolve to no arm at all.
    #[test]
    fn redo_closed_arm_never_falls_back_to_a_migration_successor_binding() {
        assert!(!REDO_CLOSED_ARM_SQL.contains("successor_binding"));
        assert!(REDO_CLOSED_ARM_SQL.contains(&format!(
            "WHEN event.event_kind = '{MIGRATION_APPLIED_EVENT_KIND}'"
        )));
    }

    #[test]
    fn seam_guard_scans_interpret_files_named_seam() {
        assert!(is_scanned_rust_source(std::path::Path::new(
            "nested/seam.rs"
        )));
        assert!(source_copies_formula(
            EVENT_CLOSE_TIME_SQL,
            EVENT_CLOSE_TIME_SQL
        ));
    }
}
