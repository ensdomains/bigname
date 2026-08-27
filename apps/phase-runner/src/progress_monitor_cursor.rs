use super::*;

pub(super) fn completion_reports_advance(token: &ProgressToken, progress: &PhaseProgress) -> bool {
    progress.current != token.starting_cursor.current
        || token.key.phase == PhaseName::Ingest
            && progress.source_progress.iter().any(|source| {
                token.starting_cursor.ingest.iter().all(|cursor| {
                    cursor.source_key != source.source_key
                        || source
                            .current
                            .as_ref()
                            .and_then(|marker| marker.number.checked_add(1))
                            .is_some_and(|next| next > cursor.next_block_number)
                        || cursor.last_processed != marker_tuple(source.current.as_ref())
                        || cursor.redo_loaded_boundary
                            != marker_tuple(source.redo_loaded_boundary.as_ref())
                })
            })
}
pub(super) fn elapsed_seconds(since: Option<Instant>, now: Instant) -> i64 {
    since
        .map(|since| {
            i64::try_from(now.saturating_duration_since(since).as_secs()).unwrap_or(i64::MAX)
        })
        .unwrap_or(0)
}
