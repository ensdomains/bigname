use std::{collections::BTreeMap, future::Future, pin::Pin};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction, types::time::OffsetDateTime};

#[path = "stored_lineage_coverage/integrity.rs"]
mod integrity;

/// Candidate-table name shared with the manifest authority query helpers.
/// The table is transaction-local and never contains durable authority.
pub const STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE: &str =
    "stored_lineage_coverage_frontier_candidate_requirements";
pub const STORED_LINEAGE_COVERAGE_PROOF_FORMAT_VERSION: &str = "stored_lineage_coverage_v1";
const COVERAGE_PUBLICATION_PAGE_ROWS: i64 = 1_000;

pub type StoredLineageCoverageProgressFuture<'a> =
    Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub trait StoredLineageCoverageProgress: Send {
    fn record<'a>(&'a mut self) -> StoredLineageCoverageProgressFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredLineageCoverageFrontierHeader {
    pub chain_id: String,
    pub snapshot_revision: i64,
    pub proof_format_version: String,
    pub discovery_admission_epoch: i64,
    pub verified_from_block: i64,
    pub verified_through_block: i64,
    pub topic0s_by_family: BTreeMap<String, Vec<String>>,
    pub requirement_row_count: i64,
    pub requirement_digest: String,
    pub updated_at: OffsetDateTime,
    /// False when a current-format row can be identified and CAS-replaced but
    /// its durable metadata must not participate in proof reuse.
    pub is_well_formed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredLineageCoverageFrontierPublication {
    pub discovery_admission_epoch: i64,
    pub verified_from_block: i64,
    pub verified_through_block: i64,
    pub topic0s_by_family: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoredLineageCoveragePublicationOutcome {
    Published { snapshot_revision: i64 },
    Conflict,
}

/// Owns the optimistic transaction and its server-side candidate. Publication
/// consumes the guard, briefly locks and rechecks the discovery epoch, then
/// atomically replaces the durable requirement snapshot.
pub struct StoredLineageCoveragePublicationGuard {
    transaction: Transaction<'static, Postgres>,
    chain: String,
    expected_snapshot_revision: Option<i64>,
    expected_discovery_admission_epoch: i64,
}

impl StoredLineageCoveragePublicationGuard {
    pub fn connection_mut(&mut self) -> &mut PgConnection {
        self.transaction.as_mut()
    }

    pub async fn publish(
        self,
        publication: &StoredLineageCoverageFrontierPublication,
    ) -> Result<StoredLineageCoveragePublicationOutcome> {
        self.publish_inner(publication, &mut None).await
    }

    pub async fn publish_with_progress(
        self,
        publication: &StoredLineageCoverageFrontierPublication,
        progress: &mut dyn StoredLineageCoverageProgress,
    ) -> Result<StoredLineageCoveragePublicationOutcome> {
        self.publish_inner(publication, &mut Some(progress)).await
    }

    async fn publish_inner(
        mut self,
        publication: &StoredLineageCoverageFrontierPublication,
        progress: &mut Option<&mut dyn StoredLineageCoverageProgress>,
    ) -> Result<StoredLineageCoveragePublicationOutcome> {
        validate_publication(publication)?;
        ensure!(
            publication.discovery_admission_epoch == self.expected_discovery_admission_epoch,
            "stored-lineage coverage publication epoch {} does not match fenced epoch {}",
            publication.discovery_admission_epoch,
            self.expected_discovery_admission_epoch
        );

        let candidate_integrity = match progress.as_deref_mut() {
            Some(progress) => {
                integrity::validate_candidate_and_load_integrity_with_progress(
                    self.transaction.as_mut(),
                    &self.chain,
                    publication.verified_from_block,
                    publication.verified_through_block,
                    progress,
                )
                .await?
            }
            None => {
                integrity::validate_candidate_and_load_integrity(
                    self.transaction.as_mut(),
                    &self.chain,
                    publication.verified_from_block,
                    publication.verified_through_block,
                )
                .await?
            }
        };

        // Candidate derivation and immutable coverage-fact verification are
        // deliberately optimistic. Take the shared admission fence only for
        // the final epoch recheck and atomic durable replacement.
        sqlx::query(
            r#"
            INSERT INTO discovery_admission_epochs (chain_id, epoch)
            VALUES ($1, 0)
            ON CONFLICT (chain_id) DO NOTHING
            "#,
        )
        .bind(&self.chain)
        .execute(self.transaction.as_mut())
        .await
        .with_context(|| format!("failed to ensure discovery epoch row for {}", self.chain))?;
        let observed_epoch = sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1 FOR SHARE",
        )
        .bind(&self.chain)
        .fetch_one(self.transaction.as_mut())
        .await
        .with_context(|| format!("failed to fence discovery epoch for {}", self.chain))?;
        ensure!(
            observed_epoch == self.expected_discovery_admission_epoch,
            "discovery admission epoch for chain {} changed from {} to {} after coverage candidate verification",
            self.chain,
            self.expected_discovery_admission_epoch,
            observed_epoch
        );

        let topics = serde_json::to_value(&publication.topic0s_by_family)
            .context("failed to encode stored-lineage coverage topic sets")?;
        let snapshot_revision_result = match self.expected_snapshot_revision {
            Some(expected_revision) => {
                sqlx::query_scalar::<_, i64>(
                    r#"
                    UPDATE stored_lineage_coverage_frontiers
                    SET snapshot_revision = snapshot_revision + 1,
                        proof_format_version = $3,
                        discovery_admission_epoch = $4,
                        verified_from_block = $5,
                        verified_through_block = $6,
                        topic0s_by_family = $7,
                        requirement_row_count = $8,
                        requirement_digest = $9,
                        updated_at = now()
                    WHERE chain_id = $1
                      AND snapshot_revision = $2
                    RETURNING snapshot_revision
                    "#,
                )
                .bind(&self.chain)
                .bind(expected_revision)
                .bind(STORED_LINEAGE_COVERAGE_PROOF_FORMAT_VERSION)
                .bind(publication.discovery_admission_epoch)
                .bind(publication.verified_from_block)
                .bind(publication.verified_through_block)
                .bind(&topics)
                .bind(candidate_integrity.row_count)
                .bind(&candidate_integrity.digest)
                .fetch_optional(self.transaction.as_mut())
                .await
            }
            None => {
                sqlx::query_scalar::<_, i64>(
                    r#"
                    INSERT INTO stored_lineage_coverage_frontiers (
                        chain_id,
                        snapshot_revision,
                        proof_format_version,
                        discovery_admission_epoch,
                        verified_from_block,
                        verified_through_block,
                        topic0s_by_family,
                        requirement_row_count,
                        requirement_digest
                    )
                    VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (chain_id) DO NOTHING
                    RETURNING snapshot_revision
                    "#,
                )
                .bind(&self.chain)
                .bind(STORED_LINEAGE_COVERAGE_PROOF_FORMAT_VERSION)
                .bind(publication.discovery_admission_epoch)
                .bind(publication.verified_from_block)
                .bind(publication.verified_through_block)
                .bind(&topics)
                .bind(candidate_integrity.row_count)
                .bind(&candidate_integrity.digest)
                .fetch_optional(self.transaction.as_mut())
                .await
            }
        };
        let snapshot_revision = match snapshot_revision_result {
            Ok(revision) => revision,
            Err(error) if is_serialization_failure(&error) => {
                self.transaction.rollback().await.with_context(|| {
                    format!(
                        "failed to roll back serialization-conflicted stored-lineage coverage publication for {}",
                        self.chain
                    )
                })?;
                return Ok(StoredLineageCoveragePublicationOutcome::Conflict);
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to compare-and-set stored-lineage coverage frontier for {}",
                        self.chain
                    )
                });
            }
        };

        let Some(snapshot_revision) = snapshot_revision else {
            self.transaction.rollback().await.with_context(|| {
                format!(
                    "failed to roll back conflicting stored-lineage coverage publication for {}",
                    self.chain
                )
            })?;
            return Ok(StoredLineageCoveragePublicationOutcome::Conflict);
        };

        replace_requirements_with_progress(self.transaction.as_mut(), &self.chain, progress)
            .await?;

        self.transaction.commit().await.with_context(|| {
            format!(
                "failed to commit stored-lineage coverage publication for {}",
                self.chain
            )
        })?;
        Ok(StoredLineageCoveragePublicationOutcome::Published { snapshot_revision })
    }
}

fn is_serialization_failure(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database) if database.code().as_deref() == Some("40001")
    )
}

pub async fn load_stored_lineage_coverage_frontier_header(
    pool: &PgPool,
    chain: &str,
) -> Result<Option<StoredLineageCoverageFrontierHeader>> {
    ensure!(
        !chain.trim().is_empty(),
        "coverage frontier chain must not be empty"
    );
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            snapshot_revision,
            proof_format_version,
            discovery_admission_epoch,
            verified_from_block,
            verified_through_block,
            topic0s_by_family,
            requirement_row_count,
            requirement_digest,
            updated_at
        FROM stored_lineage_coverage_frontiers
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load stored-lineage coverage frontier for {chain}"))?;
    row.map(header_from_row).transpose()
}

/// Validate the durable child snapshot before its intervals participate in a
/// server-side subtraction. Callers may cache this result only by immutable
/// `(chain_id, snapshot_revision)` identity.
pub async fn stored_lineage_coverage_frontier_requirements_are_valid(
    pool: &PgPool,
    header: &StoredLineageCoverageFrontierHeader,
) -> Result<bool> {
    integrity::saved_snapshot_is_valid(pool, header).await
}

pub async fn stored_lineage_coverage_frontier_requirements_are_valid_with_progress(
    pool: &PgPool,
    header: &StoredLineageCoverageFrontierHeader,
    progress: &mut dyn StoredLineageCoverageProgress,
) -> Result<bool> {
    integrity::saved_snapshot_is_valid_with_progress(pool, header, progress).await
}

async fn replace_requirements_with_progress(
    connection: &mut PgConnection,
    chain: &str,
    progress: &mut Option<&mut dyn StoredLineageCoverageProgress>,
) -> Result<()> {
    loop {
        let deleted = sqlx::query(
            r#"
            DELETE FROM stored_lineage_coverage_frontier_requirements target
            WHERE target.ctid IN (
                SELECT candidate.ctid
                FROM stored_lineage_coverage_frontier_requirements candidate
                WHERE candidate.chain_id = $1
                ORDER BY candidate.source_family, candidate.address
                LIMIT $2
            )
            "#,
        )
        .bind(chain)
        .bind(COVERAGE_PUBLICATION_PAGE_ROWS)
        .execute(&mut *connection)
        .await
        .with_context(|| {
            format!("failed to delete a prior stored-lineage coverage page for {chain}")
        })?
        .rows_affected();
        if deleted == 0 {
            break;
        }
        record_publication_progress(progress).await?;
    }

    let mut cursor = None::<(String, String)>;
    loop {
        let rows = sqlx::query(
            r#"
            SELECT source_family, address
            FROM pg_temp.stored_lineage_coverage_frontier_candidate_requirements
            WHERE $1::TEXT IS NULL OR (source_family, address) > ($1, $2)
            ORDER BY source_family, address
            LIMIT $3
            "#,
        )
        .bind(cursor.as_ref().map(|(family, _)| family))
        .bind(cursor.as_ref().map(|(_, address)| address))
        .bind(COVERAGE_PUBLICATION_PAGE_ROWS)
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("failed to page coverage publication candidate for {chain}"))?;
        let Some(last) = rows.last() else {
            break;
        };
        cursor = Some((last.try_get("source_family")?, last.try_get("address")?));
        let families = rows
            .iter()
            .map(|row| row.try_get::<String, _>("source_family"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let addresses = rows
            .iter()
            .map(|row| row.try_get::<String, _>("address"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        sqlx::query(
            r#"
            WITH page_keys AS (
                SELECT * FROM UNNEST($2::TEXT[], $3::TEXT[]) key(source_family, address)
            )
            INSERT INTO stored_lineage_coverage_frontier_requirements (
                chain_id, source_family, address, required_intervals
            )
            SELECT $1, candidate.source_family, candidate.address, candidate.required_intervals
            FROM page_keys key
            JOIN pg_temp.stored_lineage_coverage_frontier_candidate_requirements candidate
              USING (source_family, address)
            "#,
        )
        .bind(chain)
        .bind(&families)
        .bind(&addresses)
        .execute(&mut *connection)
        .await
        .with_context(|| format!("failed to insert a stored-lineage coverage page for {chain}"))?;
        record_publication_progress(progress).await?;
    }
    Ok(())
}

async fn record_publication_progress(
    progress: &mut Option<&mut dyn StoredLineageCoverageProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record().await?;
    }
    Ok(())
}

pub async fn begin_stored_lineage_coverage_frontier_publication(
    pool: &PgPool,
    chain: &str,
    expected_snapshot_revision: Option<i64>,
    expected_discovery_admission_epoch: i64,
) -> Result<StoredLineageCoveragePublicationGuard> {
    ensure!(
        !chain.trim().is_empty(),
        "coverage frontier chain must not be empty"
    );
    ensure!(
        expected_snapshot_revision.is_none_or(|revision| revision > 0),
        "expected coverage frontier revision must be positive"
    );
    ensure!(
        expected_discovery_admission_epoch >= 0,
        "expected discovery admission epoch must not be negative"
    );

    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin stored-lineage coverage publication")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(transaction.as_mut())
        .await
        .context("failed to establish repeatable-read coverage proof snapshot")?;
    sqlx::query(
        r#"
        CREATE TEMP TABLE stored_lineage_coverage_frontier_candidate_requirements (
            source_family TEXT NOT NULL,
            address TEXT NOT NULL,
            required_intervals INT8MULTIRANGE NOT NULL,
            PRIMARY KEY (source_family, address),
            CHECK (address = lower(address)),
            CHECK (required_intervals <> '{}'::INT8MULTIRANGE)
        ) ON COMMIT DROP
        "#,
    )
    .execute(transaction.as_mut())
    .await
    .context("failed to create stored-lineage coverage candidate table")?;

    Ok(StoredLineageCoveragePublicationGuard {
        transaction,
        chain: chain.to_owned(),
        expected_snapshot_revision,
        expected_discovery_admission_epoch,
    })
}

fn header_from_row(row: sqlx::postgres::PgRow) -> Result<StoredLineageCoverageFrontierHeader> {
    let topics: Value = row.try_get("topic0s_by_family")?;
    let (topic0s_by_family, topics_decoded) =
        match serde_json::from_value::<BTreeMap<String, Vec<String>>>(topics) {
            Ok(topics) => (topics, true),
            Err(_) => (BTreeMap::new(), false),
        };
    let snapshot_revision = row.try_get("snapshot_revision")?;
    let discovery_admission_epoch = row.try_get("discovery_admission_epoch")?;
    let verified_from_block = row.try_get("verified_from_block")?;
    let verified_through_block = row.try_get("verified_through_block")?;
    let requirement_row_count = row.try_get("requirement_row_count")?;
    let requirement_digest: String = row.try_get("requirement_digest")?;
    let is_well_formed = snapshot_revision > 0
        && discovery_admission_epoch >= 0
        && verified_from_block >= 0
        && verified_from_block <= verified_through_block
        && verified_through_block < i64::MAX
        && topics_decoded
        && topic_sets_are_valid(&topic0s_by_family)
        && requirement_row_count >= 0
        && integrity::digest_is_well_formed(&requirement_digest);
    Ok(StoredLineageCoverageFrontierHeader {
        chain_id: row.try_get("chain_id")?,
        snapshot_revision,
        proof_format_version: row.try_get("proof_format_version")?,
        discovery_admission_epoch,
        verified_from_block,
        verified_through_block,
        topic0s_by_family,
        requirement_row_count,
        requirement_digest,
        updated_at: row.try_get("updated_at")?,
        is_well_formed,
    })
}

fn validate_publication(publication: &StoredLineageCoverageFrontierPublication) -> Result<()> {
    ensure!(
        publication.discovery_admission_epoch >= 0,
        "coverage publication discovery epoch must not be negative"
    );
    ensure!(
        publication.verified_from_block >= 0,
        "coverage publication lower bound must not be negative"
    );
    ensure!(
        publication.verified_through_block >= publication.verified_from_block,
        "coverage publication bounds must not be inverted"
    );
    ensure!(
        publication.verified_through_block < i64::MAX,
        "inclusive coverage publication upper bound cannot be represented as an INT8MULTIRANGE"
    );
    validate_topic_sets(&publication.topic0s_by_family)
}

fn validate_topic_sets(topic0s_by_family: &BTreeMap<String, Vec<String>>) -> Result<()> {
    ensure!(
        topic_sets_are_valid(topic0s_by_family),
        "coverage topic sets must use nonempty families with sorted, deduplicated lowercase 32-byte hex topic0 values"
    );
    Ok(())
}

fn topic_sets_are_valid(topic0s_by_family: &BTreeMap<String, Vec<String>>) -> bool {
    topic0s_by_family.iter().all(|(family, topics)| {
        !family.is_empty()
            && !topics.is_empty()
            && topics.windows(2).all(|pair| pair[0] < pair[1])
            && topics.iter().all(|topic| {
                topic.len() == 66
                    && topic.starts_with("0x")
                    && topic[2..]
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
    })
}

#[cfg(test)]
#[path = "stored_lineage_coverage/tests.rs"]
mod tests;
