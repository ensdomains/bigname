use super::*;

pub(super) fn completion_reports_advance(token: &ProgressToken, progress: &PhaseProgress) -> bool {
    progress.current != token.starting_cursor.current
        || token.key.phase == PhaseName::Ingest
            && progress.source_progress.iter().any(|source| {
                token.starting_cursor.ingest.iter().all(|cursor| {
                    cursor.source_key != source.source_key
                        || cursor.last_processed != marker_tuple(source.current.as_ref())
                        || cursor.redo_loaded_boundary
                            != marker_tuple(source.redo_loaded_boundary.as_ref())
                })
            })
}
