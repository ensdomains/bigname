//! Temp-table DDL, observation staging, and staged-row reads for the
//! streamed full-source reconcile.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{Postgres, QueryBuilder, Row, postgres::PgConnection};

use super::super::super::types::{DiscoveryObservation, ExistingReconciledDiscoveryEdge};
use super::{DiscoveryObservationPageSource, StreamedDiscoveryReconciliationOptions};
use crate::normalize_address;

pub(super) async fn create_streamed_reconcile_temp_tables(
    executor: &mut PgConnection,
) -> Result<()> {
    // One row per staged observation key (the source is latest-per-key):
    // exactly the inputs the terminal states, the admission walk, and the
    // cascade need. `active_to_*` observation fields are not staged because
    // no full-reconciliation consumer reads them. Text keys use the "C"
    // collation so SQL ordering matches Rust byte order.
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_observations (
            observation_key TEXT COLLATE "C" PRIMARY KEY,
            chain_id TEXT NOT NULL,
            from_address TEXT NOT NULL,
            normalized_from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            edge_kind TEXT NOT NULL,
            discovery_source TEXT NOT NULL,
            active_from_block_number BIGINT,
            active_from_block_hash TEXT,
            provenance JSONB NOT NULL
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile observation temp table")?;

    // Address keys whose newly admitted contracts require another
    // fixed-point walk pass. One set is bulk-filled before its keyset walk,
    // then reused by every page in that pass.
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_derived_contract_keys (
            chain_id TEXT NOT NULL,
            address TEXT NOT NULL
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile derived-contract-key temp table")?;

    // Full `ReconciledDiscoveryEdgeSpec` rows. The unique constraint spans
    // the complete spec identity so `ON CONFLICT DO NOTHING` deduplicates
    // exactly like `HashSet<ReconciledDiscoveryEdgeSpec>` insertion
    // (provenance_json is compared as text, matching the spec's string
    // equality; the event-position columns are derived from it and stored
    // for SQL chronology comparisons).
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_desired_edges (
            desired_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            observation_key TEXT COLLATE "C" NOT NULL,
            chain_id TEXT NOT NULL,
            edge_kind TEXT NOT NULL,
            from_contract_instance_id UUID NOT NULL,
            to_contract_instance_id UUID NOT NULL,
            discovery_source TEXT NOT NULL,
            source_manifest_id BIGINT NOT NULL,
            admission TEXT NOT NULL,
            active_from_block_number BIGINT,
            active_from_block_hash TEXT,
            active_from_transaction_index BIGINT,
            active_from_log_index BIGINT,
            provenance_json TEXT COLLATE "C" NOT NULL,
            UNIQUE NULLS NOT DISTINCT (
                observation_key,
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                active_from_transaction_index,
                active_from_log_index,
                provenance_json
            )
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile desired-edge temp table")?;

    // Admitted-edge identities for the summary's exact admitted count
    // without holding the observation-scale admitted set in memory.
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_admitted_edges (
            source_manifest_id BIGINT NOT NULL,
            chain_id TEXT NOT NULL,
            from_contract_instance_id UUID NOT NULL,
            to_contract_instance_id UUID NOT NULL,
            from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            edge_kind TEXT NOT NULL,
            discovery_source TEXT NOT NULL,
            admission TEXT NOT NULL,
            from_role TEXT NOT NULL,
            UNIQUE (
                source_manifest_id,
                chain_id,
                from_contract_instance_id,
                to_contract_instance_id,
                from_address,
                to_address,
                edge_kind,
                discovery_source,
                admission,
                from_role
            )
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile admitted-edge temp table")?;

    Ok(())
}

pub(super) struct StagedStreamedObservations {
    pub(super) staged_observation_count: usize,
    pub(super) observation_chains: BTreeSet<String>,
}

pub(super) async fn stage_streamed_observations(
    executor: &mut PgConnection,
    source: &impl DiscoveryObservationPageSource,
    options: &StreamedDiscoveryReconciliationOptions,
) -> Result<StagedStreamedObservations> {
    let mut staged_observation_count = 0usize;
    let mut observation_chains = BTreeSet::new();
    let mut after_key = None::<String>;
    loop {
        let page = source
            .load_page(after_key.as_deref(), options.observation_page_limit)
            .await?;
        let Some((last_key, _)) = page.last() else {
            break;
        };
        after_key = Some(last_key.clone());
        staged_observation_count += page.len();

        let mut rows = Vec::with_capacity(page.len());
        for (_, observation) in &page {
            observation_chains.insert(observation.chain.clone());
            rows.push((
                super::super::super::provenance::observation_key(observation)?,
                normalize_address(&observation.from_address),
                observation,
            ));
        }
        // Chunk below the bind-parameter protocol limit regardless of the
        // source's page size.
        for chunk in rows.chunks(options.mutation_batch_size.max(1)) {
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                INSERT INTO pg_temp.reconcile_observations (
                    observation_key,
                    chain_id,
                    from_address,
                    normalized_from_address,
                    to_address,
                    edge_kind,
                    discovery_source,
                    active_from_block_number,
                    active_from_block_hash,
                    provenance
                )
                "#,
            );
            builder.push_values(
                chunk.iter(),
                |mut row, (observation_key, normalized_from_address, observation)| {
                    row.push_bind(observation_key)
                        .push_bind(&observation.chain)
                        .push_bind(&observation.from_address)
                        .push_bind(normalized_from_address)
                        .push_bind(&observation.to_address)
                        .push_bind(&observation.edge_kind)
                        .push_bind(&observation.discovery_source)
                        .push_bind(observation.active_from_block_number)
                        .push_bind(observation.active_from_block_hash.as_deref())
                        .push_bind(&observation.provenance);
                },
            );
            builder.build().execute(&mut *executor).await.context(
                "failed to stage streamed discovery observations (the page source must yield \
                 latest-per-key observations with unique observation keys)",
            )?;
            source.record_progress().await?;
        }
    }

    sqlx::query(
        r#"
        CREATE INDEX reconcile_observations_from_address_idx
        ON pg_temp.reconcile_observations (chain_id, normalized_from_address)
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to index the streamed reconcile observation temp table")?;
    analyze_temp_table(&mut *executor, "reconcile_observations").await?;
    source.record_progress().await?;

    Ok(StagedStreamedObservations {
        staged_observation_count,
        observation_chains,
    })
}

/// Replace the current fixed-point round's derived contract keys with one
/// bulk array bind. The keys stay constant while that round's observation
/// pages advance, so the page query reads this indexed set without
/// re-materializing it.
pub(super) async fn stage_streamed_derived_contract_keys(
    executor: &mut PgConnection,
    keys: BTreeSet<(String, String)>,
) -> Result<()> {
    sqlx::query("TRUNCATE pg_temp.reconcile_derived_contract_keys")
        .execute(&mut *executor)
        .await
        .context("failed to clear the streamed reconcile derived-contract-key temp table")?;

    let (chains, addresses): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
    sqlx::query(
        r#"
        INSERT INTO pg_temp.reconcile_derived_contract_keys (chain_id, address)
        SELECT derived.chain_id, derived.address
        FROM UNNEST($1::TEXT[], $2::TEXT[]) AS derived(chain_id, address)
        "#,
    )
    .bind(&chains)
    .bind(&addresses)
    .execute(&mut *executor)
    .await
    .context("failed to stage streamed reconcile derived contract keys")?;
    // Build after the first bulk fill, like the observation index. Later
    // fixed-point rounds retain the same index across TRUNCATE.
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS reconcile_derived_contract_keys_chain_address_idx
        ON pg_temp.reconcile_derived_contract_keys (chain_id, address)
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to index the streamed reconcile derived-contract-key temp table")?;
    analyze_temp_table(&mut *executor, "reconcile_derived_contract_keys").await?;

    Ok(())
}

pub(super) struct StreamedObservationRow {
    pub(super) observation_key: String,
    pub(super) normalized_from_address: String,
    pub(super) observation: DiscoveryObservation,
}

pub(super) fn streamed_observation_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<StreamedObservationRow> {
    Ok(StreamedObservationRow {
        observation_key: row
            .try_get("observation_key")
            .context("failed to read staged observation_key")?,
        normalized_from_address: row
            .try_get("normalized_from_address")
            .context("failed to read staged normalized_from_address")?,
        observation: DiscoveryObservation {
            chain: row
                .try_get("chain_id")
                .context("failed to read staged observation chain_id")?,
            from_address: row
                .try_get("from_address")
                .context("failed to read staged observation from_address")?,
            to_address: row
                .try_get("to_address")
                .context("failed to read staged observation to_address")?,
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read staged observation edge_kind")?,
            discovery_source: row
                .try_get("discovery_source")
                .context("failed to read staged observation discovery_source")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("failed to read staged observation active_from_block_number")?,
            active_from_block_hash: row
                .try_get("active_from_block_hash")
                .context("failed to read staged observation active_from_block_hash")?,
            // Not staged: no full-reconciliation consumer reads the
            // active_to window of an observation.
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: row
                .try_get("provenance")
                .context("failed to read staged observation provenance")?,
        },
    })
}

pub(super) const STREAMED_OBSERVATION_COLUMNS: &str = r#"
    observation_key,
    chain_id,
    from_address,
    normalized_from_address,
    to_address,
    edge_kind,
    discovery_source,
    active_from_block_number,
    active_from_block_hash,
    provenance
"#;

pub(super) const STREAMED_OBSERVATION_COLUMNS_QUALIFIED: &str = r#"
    obs.observation_key,
    obs.chain_id,
    obs.from_address,
    obs.normalized_from_address,
    obs.to_address,
    obs.edge_kind,
    obs.discovery_source,
    obs.active_from_block_number,
    obs.active_from_block_hash,
    obs.provenance
"#;

pub(super) async fn load_streamed_observations_for_keys(
    executor: &mut PgConnection,
    candidates: &[ExistingReconciledDiscoveryEdge],
) -> Result<Vec<DiscoveryObservation>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let observation_keys = candidates
        .iter()
        .map(|candidate| candidate.spec.observation_key.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rows = sqlx::query(&format!(
        r#"
        SELECT {STREAMED_OBSERVATION_COLUMNS}
        FROM pg_temp.reconcile_observations
        WHERE observation_key = ANY($1::TEXT[])
        ORDER BY observation_key
        "#
    ))
    .bind(&observation_keys)
    .fetch_all(executor)
    .await
    .context("failed to load staged observations for deactivation candidates")?;

    rows.into_iter()
        .map(|row| Ok(streamed_observation_from_row(row)?.observation))
        .collect()
}

/// Temp tables are never autoanalyzed; without stats the diff's correlated
/// NOT EXISTS probes can plan as unindexed nested loops at full-closure
/// scale, so every temp table is analyzed right after its bulk fill.
pub(super) async fn analyze_temp_table(executor: &mut PgConnection, table: &str) -> Result<()> {
    sqlx::query(&format!("ANALYZE pg_temp.{table}"))
        .execute(executor)
        .await
        .with_context(|| format!("failed to analyze streamed reconcile temp table {table}"))?;
    Ok(())
}

pub(super) async fn count_temp_rows(executor: &mut PgConnection, table: &str) -> Result<usize> {
    let count =
        sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*)::BIGINT FROM pg_temp.{table}"))
            .fetch_one(executor)
            .await
            .with_context(|| format!("failed to count streamed reconcile rows in {table}"))?;
    usize::try_from(count).with_context(|| format!("streamed {table} count overflowed usize"))
}
