use std::sync::Arc;

use sqlx::PgPool;

use crate::{
    ProjectError, Result, StepObserver, builders, integrity, publish, scope, stage, steps::Steps,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Marker {
    pub number: i64,
    pub hash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunMode {
    Normal,
    Redo,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchRequest {
    pub chain_id: String,
    pub target_block: i64,
    pub affected_from_block: i64,
    pub affected_to_block: i64,
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
    step_observer: Option<Arc<dyn StepObserver>>,
}

impl Engine {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            step_observer: None,
        }
    }

    pub fn with_step_observer(mut self, observer: Arc<dyn StepObserver>) -> Self {
        self.step_observer = Some(observer);
        self
    }

    pub async fn run_batch(&self, request: BatchRequest) -> Result<BatchOutcome> {
        validate_request(&request)?;
        let target = load_marker(&self.pool, &request.chain_id, request.target_block).await?;
        validate_resume(&self.pool, &request, &target).await?;

        let mut transaction = self.pool.begin().await.map_err(|error| {
            ProjectError::database("failed to begin project transaction", error)
        })?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to configure project snapshot", error)
            })?;
        revalidate_target(&mut transaction, &request.chain_id, &target).await?;

        let long_run = request.mode == RunMode::Redo || request.resume_current.is_none();
        let steps = Steps::new(
            self.step_observer.as_deref().filter(|_| long_run),
            &request.chain_id,
        );
        let row_count = derive(&mut transaction, &request, &target, &steps).await?;
        steps.enter("commit");
        transaction.commit().await.map_err(|error| {
            ProjectError::database("failed to commit atomic project publication", error)
        })?;

        Ok(BatchOutcome {
            current: target.clone(),
            target,
            complete: true,
            estimated_write_bytes: row_count.saturating_mul(1_024),
        })
    }
}

async fn derive(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &BatchRequest,
    target: &Marker,
    steps: &Steps<'_>,
) -> Result<u64> {
    let mut stage_start = std::time::Instant::now();
    let full_rebuild = matches!(request.mode, RunMode::Normal) && request.resume_current.is_none();
    steps.enter("prepare");
    stage::prepare(transaction, &request.chain_id, target).await?;
    tracing::debug!(
        stage = "prepare",
        elapsed_ms = stage_start.elapsed().as_millis() as u64,
        "Project stage completed"
    );
    stage_start = std::time::Instant::now();
    steps.enter("scope");
    scope::initialize(
        transaction,
        &request.chain_id,
        target,
        scope::Window {
            previous: request.resume_current.as_ref(),
            from_block: request.affected_from_block,
            to_block: request.affected_to_block,
            full_rebuild,
            retain_retracted: matches!(request.mode, RunMode::Redo),
        },
    )
    .await?;
    tracing::debug!(
        stage = "scope",
        elapsed_ms = stage_start.elapsed().as_millis() as u64,
        "Project stage completed"
    );
    stage_start = std::time::Instant::now();
    steps.enter("inputs");
    stage::inputs(transaction, &request.chain_id, target, full_rebuild).await?;
    tracing::debug!(
        stage = "inputs",
        elapsed_ms = stage_start.elapsed().as_millis() as u64,
        "Project stage completed"
    );
    stage_start = std::time::Instant::now();
    builders::build_all(transaction, &request.chain_id, target, full_rebuild, steps).await?;
    tracing::debug!(
        stage = "builders",
        elapsed_ms = stage_start.elapsed().as_millis() as u64,
        "Project stage completed"
    );
    stage_start = std::time::Instant::now();
    steps.enter("integrity");
    integrity::assert_publishable(transaction, &request.chain_id, target).await?;
    tracing::debug!(
        stage = "integrity",
        elapsed_ms = stage_start.elapsed().as_millis() as u64,
        "Project stage completed"
    );
    stage_start = std::time::Instant::now();
    steps.enter("publish");
    let row_count = publish::swap(transaction, &request.chain_id, full_rebuild).await?
        + builders::child_registrations::publish(
            transaction,
            &request.chain_id,
            full_rebuild,
            request.affected_from_block,
            request.affected_to_block,
        )
        .await?;
    tracing::debug!(
        stage = "publish",
        elapsed_ms = stage_start.elapsed().as_millis() as u64,
        "Project stage completed"
    );
    Ok(row_count)
}

fn validate_request(request: &BatchRequest) -> Result<()> {
    if request.chain_id.trim().is_empty() {
        return Err(ProjectError::configuration(
            "project chain ID must not be empty",
        ));
    }
    if request.target_block < 0
        || request.affected_from_block < 0
        || request.affected_to_block < request.affected_from_block
        || request.affected_to_block > request.target_block
    {
        return Err(ProjectError::configuration(format!(
            "invalid project target {} and affected range {}..={}",
            request.target_block, request.affected_from_block, request.affected_to_block
        )));
    }
    Ok(())
}

async fn load_marker(pool: &PgPool, chain_id: &str, number: i64) -> Result<Marker> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(number)
    .fetch_all(pool)
    .await
    .map_err(|error| ProjectError::database("failed to load project target", error))?;
    match rows.as_slice() {
        [hash] => Ok(Marker {
            number,
            hash: hash.clone(),
        }),
        [] => Err(ProjectError::data_integrity(format!(
            "project target block {number} for chain {chain_id} is not canonical"
        ))),
        _ => Err(ProjectError::data_integrity(format!(
            "project target block {number} for chain {chain_id} has multiple canonical hashes"
        ))),
    }
}

async fn validate_resume(pool: &PgPool, request: &BatchRequest, target: &Marker) -> Result<()> {
    let Some(resume) = &request.resume_current else {
        return Ok(());
    };
    if resume.number > target.number {
        return Err(ProjectError::data_integrity(format!(
            "project resume block {} is above target block {}",
            resume.number, target.number
        )));
    }
    let actual = load_marker(pool, &request.chain_id, resume.number).await?;
    if actual.hash != resume.hash {
        return Err(ProjectError::transient(format!(
            "project resume marker {} {} changed before projection",
            resume.number, resume.hash
        )));
    }
    Ok(())
}

async fn revalidate_target(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    let live: Option<String> = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
         FOR SHARE",
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to revalidate project target", error))?;
    if live.is_none() {
        return Err(ProjectError::transient(format!(
            "project target {} {} changed before derivation",
            target.number, target.hash
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "engine_benchmark.rs"]
mod benchmark;

#[cfg(test)]
#[path = "engine_contract_compare.rs"]
pub(crate) mod contract_compare;
