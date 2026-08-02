use sqlx::PgPool;

use crate::{InterpretError, RECOMPUTE_FLAGS_UNAVAILABLE_REASON, Result, load, write};

const CANONICAL_BLOCKS_PER_BATCH: i64 = 500;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Marker {
    pub number: i64,
    pub hash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
}

impl Engine {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn run_batch(&self, request: BatchRequest) -> Result<BatchOutcome> {
        validate_request(&request)?;
        if matches!(request.mode, RunMode::RecomputeFlags) {
            return Err(InterpretError::configuration(
                RECOMPUTE_FLAGS_UNAVAILABLE_REASON,
            ));
        }
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
        let input =
            load::batch_input(&self.pool, &request.chain_id, *batch_from, *batch_to).await?;
        let output =
            bigname_adapters::schema_v2::interpret_schema_v2_batch(input).map_err(|error| {
                InterpretError::data_integrity(format!(
                    "hash-covered adapter interpretation failed: {error:#}"
                ))
            })?;
        let redo_range = (matches!(request.mode, RunMode::Redo)
            && request.resume_current.is_none())
        .then_some((request.from_block, request.to_block));
        let estimated_write_bytes =
            write::batch(&self.pool, &request.chain_id, redo_range, &output).await?;
        let current = Marker {
            number: *batch_to,
            hash: batch_hash.clone(),
        };
        Ok(BatchOutcome {
            complete: current == target,
            current,
            target,
            estimated_write_bytes,
        })
    }
}

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
