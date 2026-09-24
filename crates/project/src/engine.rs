use std::{collections::BTreeMap, time::Instant};

use sqlx::PgPool;

use crate::{ProjectError, Result, builders, integrity, publish, scope, stage};

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
    pub write_summary: WriteSummary,
}

/// What one batch read and wrote, counted inside its transaction. Keys are table and stage names.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteSummary {
    /// Blocks in the affected range.
    pub blocks: u64,
    /// Events in the affected range that changed the scope; zero for a full rebuild, which
    /// derives the whole chain without one.
    pub changed_events: u64,
    /// Events staged for the builders: the history the rebuilt keys read.
    pub staged_events: u64,
    /// Keys in each scope when publication starts, by scope (`names` for `project_scope_names`).
    pub scope_keys: BTreeMap<&'static str, u64>,
    /// Rows each served table lost when publication cleared the scope.
    pub deleted: BTreeMap<&'static str, u64>,
    /// Rows each served table received; for child registrations, rows inserted or updated.
    pub inserted: BTreeMap<&'static str, u64>,
    /// Elapsed milliseconds of each derivation stage.
    pub stage_elapsed_ms: BTreeMap<&'static str, u64>,
}

/// The scopes publication replaces, each named by its table without the `project_scope_` prefix.
const SCOPES: [&str; 6] = [
    "names",
    "children",
    "resources",
    "account_permissions",
    "resolvers",
    "primary",
];

impl WriteSummary {
    pub fn inserted_rows(&self) -> u64 {
        self.inserted.values().copied().fold(0, u64::saturating_add)
    }

    fn finish_stage(&mut self, stage: &'static str, started: &mut Instant) {
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        tracing::debug!(stage, elapsed_ms, "Project stage completed");
        self.stage_elapsed_ms.insert(stage, elapsed_ms);
        *started = Instant::now();
    }
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
        sqlx::query(
            "/* project:engine.isolation */ SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| ProjectError::database("failed to configure project snapshot", error))?;
        revalidate_target(&mut transaction, &request.chain_id, &target).await?;

        let write_summary = derive(&mut transaction, &request, &target).await?;
        transaction.commit().await.map_err(|error| {
            ProjectError::database("failed to commit atomic project publication", error)
        })?;
        tracing::info!(
            target: "bigname_project::batch",
            chain_id = request.chain_id,
            target_block = target.number,
            blocks = write_summary.blocks,
            changed_events = write_summary.changed_events,
            staged_events = write_summary.staged_events,
            scope_keys = ?write_summary.scope_keys,
            deleted = ?write_summary.deleted,
            inserted = ?write_summary.inserted,
            stage_elapsed_ms = ?write_summary.stage_elapsed_ms,
            "Project batch committed"
        );

        Ok(BatchOutcome {
            current: target.clone(),
            target,
            complete: true,
            estimated_write_bytes: write_summary.inserted_rows().saturating_mul(1_024),
            write_summary,
        })
    }
}

async fn derive(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &BatchRequest,
    target: &Marker,
) -> Result<WriteSummary> {
    let mut summary = WriteSummary {
        blocks: u64::try_from(request.affected_to_block - request.affected_from_block + 1)
            .unwrap_or(0),
        ..WriteSummary::default()
    };
    let mut stage_start = Instant::now();
    let full_rebuild = matches!(request.mode, RunMode::Normal) && request.resume_current.is_none();
    stage::prepare(transaction, &request.chain_id, target).await?;
    summary.finish_stage("prepare", &mut stage_start);
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
    summary.finish_stage("scope", &mut stage_start);
    stage::inputs(transaction, &request.chain_id, target, full_rebuild).await?;
    summary.finish_stage("inputs", &mut stage_start);
    builders::build_all(transaction, &request.chain_id, target, full_rebuild).await?;
    summary.finish_stage("builders", &mut stage_start);
    integrity::assert_publishable(transaction, &request.chain_id, target).await?;
    summary.finish_stage("integrity", &mut stage_start);
    // Counting the scope belongs to `publish`, so the six stage durations add up to the whole
    // derivation.
    count_inputs(transaction, full_rebuild, &mut summary).await?;
    publish::swap(transaction, &request.chain_id, full_rebuild, &mut summary).await?;
    builders::child_registrations::publish(
        transaction,
        &request.chain_id,
        full_rebuild,
        request.affected_from_block,
        request.affected_to_block,
        &mut summary,
    )
    .await?;
    summary.finish_stage("publish", &mut stage_start);
    Ok(summary)
}

/// Staged events and scope keys, read in one statement before publication. A full rebuild
/// stages no changed events.
async fn count_inputs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    full_rebuild: bool,
    summary: &mut WriteSummary,
) -> Result<()> {
    let changed = if full_rebuild {
        "0::bigint"
    } else {
        "(SELECT count(*) FROM project_changed_events)"
    };
    let counts: Vec<i64> = sqlx::query_scalar(&format!(
        "/* project:engine.count_inputs */ SELECT unnest(ARRAY[{changed}, \
         (SELECT count(*) FROM project_events), {}])",
        SCOPES
            .map(|scope| format!("(SELECT count(*) FROM project_scope_{scope})"))
            .join(", ")
    ))
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to count project batch inputs", error))?;
    let mut counts = counts
        .into_iter()
        .map(|count| u64::try_from(count).unwrap_or(0));
    summary.changed_events = counts.next().unwrap_or(0);
    summary.staged_events = counts.next().unwrap_or(0);
    for (scope, count) in SCOPES.into_iter().zip(counts) {
        summary.scope_keys.insert(scope, count);
    }
    Ok(())
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
        "/* project:engine.load_marker */ SELECT block_hash FROM chain_lineage
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
        "/* project:engine.revalidate_target */ SELECT block_hash FROM chain_lineage
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
