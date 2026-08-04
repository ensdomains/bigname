use std::{collections::HashMap, sync::Mutex};

use sqlx::PgPool;

use crate::{InterpretError, Result, load, recompute, write};

const CANONICAL_BLOCKS_PER_BATCH: i64 = 500;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Marker {
    pub number: i64,
    pub hash: String,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RunMode {
    Normal,
    Redo,
    RecomputeFlags,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchRequest {
    pub chain_id: String,
    pub from_block: i64,
    pub to_block: i64,
    pub resume_current: Option<Marker>,
    pub mode: RunMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchOutcome {
    pub current: Marker,
    pub target: Marker,
    pub complete: bool,
    pub estimated_write_bytes: u64,
}

pub struct Engine {
    pool: PgPool,
    prior_sessions: Mutex<HashMap<SessionKey, PriorSession>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SessionKey {
    chain_id: String,
    from_block: i64,
    mode: RunMode,
}

struct PriorSession {
    next_block: i64,
    snapshot: load::PriorSnapshot,
}

impl Engine {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            prior_sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run_batch(&self, request: BatchRequest) -> Result<BatchOutcome> {
        validate_request(&request)?;
        let target = load::marker(&self.pool, &request.chain_id, request.to_block)
            .await?
            .map(|(number, hash)| Marker { number, hash })
            .ok_or_else(|| {
                InterpretError::data_integrity(format!(
                    "interpret target block {} for chain {} is not canonical",
                    request.to_block, request.chain_id
                ))
            })?;
        validate_resume(&self.pool, &request, &target).await?;
        let next_block = request
            .resume_current
            .as_ref()
            .map_or(request.from_block, |marker| marker.number.saturating_add(1));
        if next_block > target.number {
            return Ok(BatchOutcome {
                current: target.clone(),
                target,
                complete: true,
                estimated_write_bytes: 0,
            });
        }
        if matches!(request.mode, RunMode::RecomputeFlags) {
            let estimated_write_bytes = recompute::run(
                &self.pool,
                &request.chain_id,
                request.from_block,
                request.to_block,
            )
            .await?;
            return Ok(BatchOutcome {
                current: target.clone(),
                target,
                complete: true,
                estimated_write_bytes,
            });
        }
        let markers = load::canonical_markers(
            &self.pool,
            &request.chain_id,
            next_block,
            target.number,
            CANONICAL_BLOCKS_PER_BATCH,
        )
        .await?;
        validate_contiguous_markers(&request.chain_id, next_block, &markers)?;
        let Some((batch_from, _)) = markers.first() else {
            return Err(InterpretError::data_integrity(format!(
                "interpret range {next_block}..={} for chain {} has no canonical lineage",
                target.number, request.chain_id
            )));
        };
        let (batch_to, batch_hash) = markers.last().expect("non-empty markers");
        let session_key = SessionKey {
            chain_id: request.chain_id.clone(),
            from_block: request.from_block,
            mode: request.mode,
        };
        let cached_prior_snapshot = if request.resume_current.is_some() {
            self.cached_prior_snapshot(&session_key, *batch_from)?
        } else {
            None
        };
        let loaded = load::batch_input(
            &self.pool,
            &request.chain_id,
            *batch_from,
            *batch_to,
            request
                .resume_current
                .as_ref()
                .map(|marker| (marker.number, marker.hash.as_str())),
            cached_prior_snapshot,
        )
        .await?;
        let prior_snapshot = loaded.prior_snapshot;
        let input = loaded.input;
        let prior_events = input.prior_events.clone();
        let batch_blocks = input.blocks.clone();
        let loaded_markers = batch_blocks
            .iter()
            .map(|block| (block.block_number, block.block_hash.clone()))
            .collect::<Vec<_>>();
        validate_loaded_lineage(&request.chain_id, &markers, &loaded_markers)?;
        let mut write_lineage = Vec::with_capacity(
            loaded_markers.len() + usize::from(request.resume_current.is_some()),
        );
        if let Some(resume) = &request.resume_current {
            write_lineage.push((resume.number, resume.hash.clone()));
        }
        write_lineage.extend(loaded_markers.iter().cloned());
        let output =
            bigname_adapters::schema_v2::interpret_schema_v2_batch(input).map_err(|error| {
                InterpretError::data_integrity(format!(
                    "hash-covered adapter interpretation failed: {error:#}"
                ))
            })?;
        let next_prior_events = bigname_adapters::schema_v2::seam::fold_prior_events(
            prior_events,
            &output.normalized_events,
            &batch_blocks,
        )
        .map_err(|error| {
            InterpretError::data_integrity(format!(
                "hash-covered adapter prior-state fold failed: {error:#}"
            ))
        })?;
        let next_prior_snapshot =
            load::fold_prior_snapshot(prior_snapshot, next_prior_events, &output.normalized_events);
        let complete = *batch_to == target.number;
        let redo_range =
            matches!(request.mode, RunMode::Redo).then_some((request.from_block, request.to_block));
        let prepare_redo_range = redo_range.is_some() && request.resume_current.is_none();
        let estimated_write_bytes = write::batch(
            &self.pool,
            &request.chain_id,
            redo_range,
            prepare_redo_range,
            complete,
            &write_lineage,
            &output,
        )
        .await?;
        self.store_prior_snapshot(
            session_key,
            batch_to.saturating_add(1),
            next_prior_snapshot,
            complete,
        )?;
        let current = Marker {
            number: *batch_to,
            hash: batch_hash.clone(),
        };
        Ok(BatchOutcome {
            complete,
            current,
            target,
            estimated_write_bytes,
        })
    }

    fn cached_prior_snapshot(
        &self,
        key: &SessionKey,
        next_block: i64,
    ) -> Result<Option<load::PriorSnapshot>> {
        let sessions = self.prior_sessions.lock().map_err(|_| {
            InterpretError::transient("interpret prior-state session lock was poisoned")
        })?;
        Ok(sessions
            .get(key)
            .filter(|session| session.next_block == next_block)
            .map(|session| session.snapshot.clone()))
    }

    fn store_prior_snapshot(
        &self,
        key: SessionKey,
        next_block: i64,
        snapshot: load::PriorSnapshot,
        complete: bool,
    ) -> Result<()> {
        let mut sessions = self.prior_sessions.lock().map_err(|_| {
            InterpretError::transient("interpret prior-state session lock was poisoned")
        })?;
        update_prior_sessions(&mut sessions, key, next_block, snapshot, complete);
        Ok(())
    }
}

fn update_prior_sessions(
    sessions: &mut HashMap<SessionKey, PriorSession>,
    key: SessionKey,
    next_block: i64,
    snapshot: load::PriorSnapshot,
    complete: bool,
) {
    if matches!(key.mode, RunMode::Redo) && complete {
        sessions.retain(|candidate, _| candidate.chain_id != key.chain_id);
    } else {
        sessions.insert(
            key,
            PriorSession {
                next_block,
                snapshot,
            },
        );
    }
}

fn validate_loaded_lineage(
    chain_id: &str,
    selected: &[(i64, String)],
    loaded: &[(i64, String)],
) -> Result<()> {
    if loaded != selected {
        return Err(InterpretError::transient(format!(
            "interpret batch lineage changed while loading raw facts for chain {chain_id}; retry with a fresh canonical batch"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "engine/tests.rs"]
mod tests;

fn validate_contiguous_markers(
    chain_id: &str,
    expected_from: i64,
    markers: &[(i64, String)],
) -> Result<()> {
    for (offset, (actual, _)) in markers.iter().enumerate() {
        let expected = expected_from.saturating_add(i64::try_from(offset).unwrap_or(i64::MAX));
        if *actual != expected {
            return Err(InterpretError::data_integrity(format!(
                "interpret canonical lineage for chain {chain_id} has a gap at block {expected}"
            )));
        }
    }
    Ok(())
}

fn validate_request(request: &BatchRequest) -> Result<()> {
    if request.chain_id.trim().is_empty() {
        return Err(InterpretError::configuration(
            "interpret chain ID must not be empty",
        ));
    }
    if request.from_block < 0 || request.to_block < request.from_block {
        return Err(InterpretError::configuration(format!(
            "invalid interpret range {}..={}",
            request.from_block, request.to_block
        )));
    }
    Ok(())
}

async fn validate_resume(pool: &PgPool, request: &BatchRequest, target: &Marker) -> Result<()> {
    let Some(resume) = &request.resume_current else {
        return Ok(());
    };
    if resume.number > target.number {
        return Err(InterpretError::data_integrity(format!(
            "interpret resume block {} is above target block {}",
            resume.number, target.number
        )));
    }
    let canonical = load::marker(pool, &request.chain_id, resume.number).await?;
    if canonical.as_ref().map(|(_, hash)| hash) != Some(&resume.hash) {
        return Err(InterpretError::data_integrity(format!(
            "interpret resume marker {} {} is no longer canonical",
            resume.number, resume.hash
        )));
    }
    Ok(())
}
