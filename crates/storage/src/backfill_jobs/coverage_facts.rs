use anyhow::{Context, Result, bail};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder};

use crate::projection_helpers::POSTGRES_MAX_BIND_PARAMETERS;

const COVERAGE_FACT_INSERT_BIND_COLUMNS: usize = 8;
pub(super) const COVERAGE_FACT_INSERT_CHUNK_ROWS: usize =
    POSTGRES_MAX_BIND_PARAMETERS / COVERAGE_FACT_INSERT_BIND_COLUMNS;

/// Scope of a durable backfill coverage fact. `Family` rows mean every address
/// of the source family is covered by a topics-complete fetch over the range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackfillCoverageFactScope {
    Address,
    Family,
}

impl BackfillCoverageFactScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Address => "address",
            Self::Family => "family",
        }
    }
}

/// How a coverage fact was derived. `JobCompletion` rows are written from the
/// completing job's own in-memory selector plan; `LegacyFullPayloadIdentity`
/// rows are re-derived by ops tooling from a persisted full-payload
/// `source_identity` of an already-completed job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackfillCoverageFactDerivation {
    JobCompletion,
    LegacyFullPayloadIdentity,
}

impl BackfillCoverageFactDerivation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JobCompletion => "job_completion",
            Self::LegacyFullPayloadIdentity => "legacy_full_payload_identity",
        }
    }
}

/// One coverage fact row to append for a backfill job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillCoverageFactWrite {
    pub source_family: String,
    pub scope: BackfillCoverageFactScope,
    pub address: Option<String>,
    pub covered_from_block: i64,
    pub covered_to_block: i64,
}

struct BackfillCoverageAuthority {
    chain_id: String,
    status: String,
    range_start_block_number: i64,
    range_end_block_number: i64,
}

/// Append coverage facts for a backfill job inside the caller's transaction.
/// The fact rows' chain_id is read from the job row itself, so it can never
/// disagree with the job the facts describe. The job must already be completed
/// and every fact interval must be contained by its declared range. Inserts are
/// chunked below the PostgreSQL bind limit and idempotent via ON CONFLICT DO
/// NOTHING against the tuple key; returns the inserted count.
pub async fn write_backfill_coverage_facts(
    conn: &mut PgConnection,
    backfill_job_id: i64,
    derivation: BackfillCoverageFactDerivation,
    facts: &[BackfillCoverageFactWrite],
) -> Result<u64> {
    if facts.is_empty() {
        return Ok(0);
    }
    // Validate the whole batch before the first insert: on a bare connection a
    // mid-batch validation failure would otherwise leave earlier chunks
    // persisted behind an error return.
    for fact in facts {
        validate_backfill_coverage_fact_write(fact)?;
    }
    let authority = load_backfill_job_coverage_authority(conn, backfill_job_id).await?;
    validate_backfill_job_coverage_authority(backfill_job_id, &authority, facts.iter())?;
    let mut inserted = 0_u64;
    for chunk in facts.chunks(COVERAGE_FACT_INSERT_CHUNK_ROWS) {
        inserted += insert_backfill_coverage_fact_chunk(
            conn,
            backfill_job_id,
            &authority.chain_id,
            derivation,
            chunk,
        )
        .await?;
    }
    Ok(inserted)
}

/// Streaming variant of [`write_backfill_coverage_facts`] for whole-active
/// plans (millions of targets): buffers at most one insert chunk at a time so
/// per-chunk allocations stay bounded regardless of plan size.
pub(super) async fn write_backfill_coverage_facts_from_iter(
    conn: &mut PgConnection,
    backfill_job_id: i64,
    derivation: BackfillCoverageFactDerivation,
    facts: impl Iterator<Item = BackfillCoverageFactWrite>,
) -> Result<u64> {
    let mut inserted = 0_u64;
    let mut chunk = Vec::new();
    let authority = load_backfill_job_coverage_authority(conn, backfill_job_id).await?;
    for fact in facts {
        chunk.push(fact);
        if chunk.len() == COVERAGE_FACT_INSERT_CHUNK_ROWS {
            validate_backfill_job_coverage_authority(backfill_job_id, &authority, chunk.iter())?;
            inserted += insert_backfill_coverage_fact_chunk(
                conn,
                backfill_job_id,
                &authority.chain_id,
                derivation,
                &chunk,
            )
            .await?;
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        validate_backfill_job_coverage_authority(backfill_job_id, &authority, chunk.iter())?;
        inserted += insert_backfill_coverage_fact_chunk(
            conn,
            backfill_job_id,
            &authority.chain_id,
            derivation,
            &chunk,
        )
        .await?;
    }
    Ok(inserted)
}

async fn load_backfill_job_coverage_authority(
    conn: &mut PgConnection,
    backfill_job_id: i64,
) -> Result<BackfillCoverageAuthority> {
    sqlx::query_as::<_, (String, String, i64, i64)>(
        r#"
        SELECT
            chain_id,
            status::TEXT,
            range_start_block_number,
            range_end_block_number
        FROM backfill_jobs
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .fetch_optional(&mut *conn)
    .await
    .with_context(|| {
        format!("failed to load coverage authority for backfill job {backfill_job_id}")
    })?
    .map(
        |(chain_id, status, range_start_block_number, range_end_block_number)| {
            BackfillCoverageAuthority {
                chain_id,
                status,
                range_start_block_number,
                range_end_block_number,
            }
        },
    )
    .with_context(|| format!("missing backfill job {backfill_job_id} for coverage fact write"))
}

fn validate_backfill_job_coverage_authority<'a>(
    backfill_job_id: i64,
    authority: &BackfillCoverageAuthority,
    facts: impl Iterator<Item = &'a BackfillCoverageFactWrite>,
) -> Result<()> {
    if authority.status != "completed" {
        bail!(
            "backfill job {backfill_job_id} is {}, not completed; it cannot authorize coverage facts",
            authority.status
        );
    }
    for fact in facts {
        if fact.covered_from_block < authority.range_start_block_number
            || fact.covered_to_block > authority.range_end_block_number
        {
            bail!(
                "backfill coverage fact interval {}..={} is not contained by job range {}..={} for backfill job {backfill_job_id}",
                fact.covered_from_block,
                fact.covered_to_block,
                authority.range_start_block_number,
                authority.range_end_block_number
            );
        }
    }
    Ok(())
}

/// Count persisted coverage facts for a backfill job (tests/ops introspection).
pub async fn load_backfill_coverage_fact_counts(
    pool: &PgPool,
    backfill_job_id: i64,
) -> Result<u64> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM backfill_coverage_facts WHERE backfill_job_id = $1",
    )
    .bind(backfill_job_id)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to count coverage facts for backfill job {backfill_job_id}")
    })?;
    u64::try_from(count).context("backfill coverage fact count must not be negative")
}

async fn insert_backfill_coverage_fact_chunk(
    conn: &mut PgConnection,
    backfill_job_id: i64,
    chain_id: &str,
    derivation: BackfillCoverageFactDerivation,
    facts: &[BackfillCoverageFactWrite],
) -> Result<u64> {
    for fact in facts {
        validate_backfill_coverage_fact_write(fact)?;
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO backfill_coverage_facts (
            backfill_job_id,
            chain_id,
            source_family,
            scope,
            address,
            covered_from_block,
            covered_to_block,
            derivation
        )
        "#,
    );
    builder.push_values(facts, |mut row, fact| {
        row.push_bind(backfill_job_id)
            .push_bind(chain_id)
            .push_bind(&fact.source_family)
            .push_bind(fact.scope.as_str())
            .push_bind(fact.address.as_deref())
            .push_bind(fact.covered_from_block)
            .push_bind(fact.covered_to_block)
            .push_bind(derivation.as_str());
    });
    builder.push(" ON CONFLICT ON CONSTRAINT backfill_coverage_facts_tuple_key DO NOTHING");

    let result = builder.build().execute(conn).await.with_context(|| {
        format!("failed to insert coverage facts for backfill job {backfill_job_id}")
    })?;
    Ok(result.rows_affected())
}

fn validate_backfill_coverage_fact_write(fact: &BackfillCoverageFactWrite) -> Result<()> {
    if fact.source_family.trim().is_empty() {
        bail!("backfill coverage fact source_family must not be empty");
    }
    match (fact.scope, fact.address.as_deref()) {
        (BackfillCoverageFactScope::Address, None) => {
            bail!("address-scoped backfill coverage fact must carry an address");
        }
        (BackfillCoverageFactScope::Address, Some(address)) if address.trim().is_empty() => {
            bail!("address-scoped backfill coverage fact address must not be empty");
        }
        (BackfillCoverageFactScope::Family, Some(_)) => {
            bail!("family-scoped backfill coverage fact must not carry an address");
        }
        _ => {}
    }
    if fact.covered_from_block > fact.covered_to_block {
        bail!(
            "backfill coverage fact covered_from_block {} is after covered_to_block {}",
            fact.covered_from_block,
            fact.covered_to_block
        );
    }
    Ok(())
}
