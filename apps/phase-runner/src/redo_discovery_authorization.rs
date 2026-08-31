use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
    transitions::PhaseStateRow,
};

/// Revalidates automatic discovery routing from the phase-state rows locked by redo begin.
pub(crate) fn require_locked(
    chain_id: &str,
    phase: PhaseName,
    requested: BlockRange,
    state: &PhaseStateRow,
) -> RunnerResult<()> {
    require(
        chain_id,
        phase,
        requested,
        state.redo_in_progress,
        state.redo_from_block_number,
        state.redo_to_block_number,
        state.last_error.as_deref(),
    )
}

fn require(
    chain_id: &str,
    phase: PhaseName,
    requested: BlockRange,
    redo_in_progress: bool,
    redo_from: Option<i64>,
    redo_to: Option<i64>,
    last_error: Option<&str>,
) -> RunnerResult<()> {
    if phase != PhaseName::Ingest {
        return Err(RunnerError::data_integrity(
            "automatic discovery repair can target only Ingest",
        ));
    }
    let persisted = redo_from
        .zip(redo_to)
        .map(|(from, to)| BlockRange::new(from, to))
        .transpose()?;
    let discovery_owned =
        redo_in_progress && last_error.is_some_and(bigname_manifests::is_discovery_required_ingest);
    if persisted != Some(requested) || !discovery_owned {
        return Err(crate::transitions::required_ingest_redo_error(
            chain_id,
            persisted.unwrap_or(requested),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const DISCOVERY_REASON: &str = "required downstream redo: discovery watch admission added coverage over already-ingested blocks [required-ingest-cause:discovery]";

    #[test]
    fn locked_begin_accepts_only_the_selected_discovery_range() {
        let range = BlockRange::new(5, 10).unwrap();
        require(
            "chain",
            PhaseName::Ingest,
            range,
            true,
            Some(5),
            Some(10),
            Some(DISCOVERY_REASON),
        )
        .unwrap();

        let changed = require(
            "chain",
            PhaseName::Ingest,
            range,
            true,
            Some(4),
            Some(10),
            Some(DISCOVERY_REASON),
        )
        .expect_err("a range changed after selection must return to routing");
        assert!(changed.to_string().contains("operator decision"));
    }

    #[test]
    fn locked_begin_rejects_manifest_ownership_merged_after_selection() {
        let error = require(
            "chain",
            PhaseName::Ingest,
            BlockRange::new(5, 10).unwrap(),
            true,
            Some(5),
            Some(10),
            Some(
                "required downstream redo: manifest watch plan widened over an already-ingested range",
            ),
        )
        .expect_err("manifest ownership must retain the operator gate");
        assert!(error.to_string().contains("operator decision"));
    }
}
