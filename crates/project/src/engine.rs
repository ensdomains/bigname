use sqlx::PgPool;

use crate::{ProjectError, Result, builders, publish, scope, stage};

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
}

impl Engine {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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

        let full_rebuild =
            matches!(request.mode, RunMode::Normal) && request.resume_current.is_none();
        stage::prepare(&mut transaction, &request.chain_id, &target).await?;
        scope::initialize(
            &mut transaction,
            &request.chain_id,
            &target,
            scope::Window {
                previous: request.resume_current.as_ref(),
                from_block: request.affected_from_block,
                to_block: request.affected_to_block,
                full_rebuild,
                retain_retracted: matches!(request.mode, RunMode::Redo),
            },
        )
        .await?;
        stage::inputs(&mut transaction, &request.chain_id, &target, full_rebuild).await?;
        builders::build_all(&mut transaction, &request.chain_id, &target, full_rebuild).await?;
        let row_count = publish::swap(&mut transaction, &request.chain_id, full_rebuild).await?;
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
