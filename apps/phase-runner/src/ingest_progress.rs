use std::collections::{BTreeMap, BTreeSet};

use crate::{
    config::SourceConfig,
    error::{RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::PhaseProgress,
};

pub(crate) fn validate(
    sources: &[SourceConfig],
    progress: &PhaseProgress,
    completing: bool,
) -> RunnerResult<()> {
    if sources.len() > 1 && progress.source_progress.is_empty() {
        return Err(RunnerError::data_integrity(format!(
            "ingest batch for chain {} has {} configured sources but reported no per-source \
             progress; refusing to advance every source cursor",
            sources[0].chain_id,
            sources.len()
        )));
    }
    let configured = sources
        .iter()
        .map(|source| source.source_key.as_str())
        .collect::<BTreeSet<_>>();
    let mut reported = BTreeSet::new();
    for source_progress in &progress.source_progress {
        if !configured.contains(source_progress.source_key.as_str()) {
            return Err(RunnerError::data_integrity(format!(
                "ingest phase reported unconfigured source {}",
                source_progress.source_key
            )));
        }
        if !reported.insert(source_progress.source_key.as_str()) {
            return Err(RunnerError::data_integrity(format!(
                "ingest phase reported source {} more than once in one batch",
                source_progress.source_key
            )));
        }
    }
    if completing && sources.len() > 1 {
        let missing = configured
            .difference(&reported)
            .copied()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(RunnerError::data_integrity(format!(
                "multi-source ingest cannot complete with missing source progress for {}",
                missing.join(", ")
            )));
        }
    }
    if completing {
        if progress.source_progress.is_empty() {
            if let Some(source) = sources.first() {
                require_source_completion(
                    source,
                    progress.current.as_ref(),
                    progress.target.as_ref(),
                )?;
            }
        } else {
            let sources_by_key = sources
                .iter()
                .map(|source| (source.source_key.as_str(), source))
                .collect::<BTreeMap<_, _>>();
            for source_progress in &progress.source_progress {
                let source = sources_by_key
                    .get(source_progress.source_key.as_str())
                    .ok_or_else(|| {
                        RunnerError::data_integrity(format!(
                            "ingest phase reported unconfigured source {}",
                            source_progress.source_key
                        ))
                    })?;
                require_source_completion(
                    source,
                    source_progress.current.as_ref(),
                    source_progress.target.as_ref(),
                )?;
            }
        }
        require_summary_completion(progress)?;
    }
    Ok(())
}

fn require_source_completion(
    source: &SourceConfig,
    current: Option<&BlockMarker>,
    target: Option<&BlockMarker>,
) -> RunnerResult<()> {
    let current = current.filter(|marker| marker.number >= source.start_block_number);
    let Some(target) = target else {
        return Err(RunnerError::data_integrity(format!(
            "ingest source {} cannot complete without a target block",
            source.source_key
        )));
    };
    if target.number < source.start_block_number {
        return Ok(());
    }
    let Some(current) = current else {
        return Err(RunnerError::data_integrity(format!(
            "ingest source {} cannot complete without reaching target block {}",
            source.source_key, target.number
        )));
    };
    if current.number < target.number {
        return Err(RunnerError::data_integrity(format!(
            "ingest source {} cannot complete at block {} before target block {}",
            source.source_key, current.number, target.number
        )));
    }
    if current.number > target.number {
        return Err(RunnerError::data_integrity(format!(
            "ingest source {} cannot complete at block {} after target block {}",
            source.source_key, current.number, target.number
        )));
    }
    if current.hash != target.hash {
        return Err(RunnerError::data_integrity(format!(
            "ingest source {} cannot complete with different current and target hashes at block {}",
            source.source_key, current.number
        )));
    }
    Ok(())
}

fn require_summary_completion(progress: &PhaseProgress) -> RunnerResult<()> {
    let target = progress.target.as_ref().ok_or_else(|| {
        RunnerError::data_integrity("ingest cannot complete without a target block")
    })?;
    let current = progress.current.as_ref().ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "ingest cannot complete without reaching target block {}",
            target.number
        ))
    })?;
    if current.number != target.number || current.hash != target.hash {
        return Err(RunnerError::data_integrity(format!(
            "ingest cannot complete unless its current block matches target block {}",
            target.number
        )));
    }
    let live_handoff = progress.live_handoff.as_ref().ok_or_else(|| {
        RunnerError::data_integrity("ingest cannot complete without a live handoff")
    })?;
    if live_handoff != target {
        return Err(RunnerError::data_integrity(format!(
            "ingest live handoff must match target block {} at completion",
            target.number
        )));
    }
    Ok(())
}
