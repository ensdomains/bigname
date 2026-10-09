//! Revalidate every input of a prepared lookahead batch before its ordered write.
use std::{collections::BTreeSet, num::NonZeroU32};

use bigname_adapters::schema_v2::{PriorEventInput, StateCacheCapacity, V1BatchDependencies};
use sqlx::{PgConnection, PgPool};
use time::OffsetDateTime;

use super::super::{
    LoadedBatch, cache, load_admissions, load_blocks, load_discovery_rules, load_raw_logs,
    lookahead_query, manifests, migration, resume, validate_snapshot_resume_marker,
};
use super::{
    Attempt, Fetched, batch_input_inner, full_state_reason, load_closure, retained_family_reason,
};
use crate::{InterpretError, Result};

/// Inputs not already retained in `LoadedBatch`. Complete query results also certify absence:
/// a new due name, admission or member of a queried registry must reject the preparation.
pub(crate) struct SpeculativeCertificate {
    pub(super) from_block: i64,
    pub(super) to_block: i64,
    pub(super) other_families: Vec<(String, String)>,
    pub(super) predecessor: Option<OffsetDateTime>,
    pub(super) has_v2_manifest: bool,
    pub(super) latest_v2_topology: Option<OffsetDateTime>,
    pub(super) due_names: Vec<String>,
    pub(super) due_v2_keys: BTreeSet<String>,
    pub(super) dependencies: V1BatchDependencies,
    pub(super) prior_events: Vec<PriorEventInput>,
}

pub(crate) async fn batch_input(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    resume_marker: Option<(i64, &str)>,
    state_cache_capacity: StateCacheCapacity,
    statement_timeout: Option<NonZeroU32>,
) -> Result<Attempt> {
    batch_input_inner(
        pool,
        chain_id,
        from_block,
        to_block,
        resume_marker,
        state_cache_capacity,
        statement_timeout,
        false,
    )
    .await
}

/// Prepare without publishing any rows. Unsupported families keep the ordinary full-state
/// result so the coordinator can retry them serially at their turn.
pub(crate) async fn speculative_batch_input(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    resume_marker: Option<(i64, &str)>,
    state_cache_capacity: StateCacheCapacity,
    statement_timeout: Option<NonZeroU32>,
) -> Result<Attempt> {
    batch_input_inner(
        pool,
        chain_id,
        from_block,
        to_block,
        resume_marker,
        state_cache_capacity,
        statement_timeout,
        true,
    )
    .await
}

/// Call after every preceding batch committed, while retaining Interpret's exclusive writer
/// ownership. No adapter code executes here. The existing writer still revalidates lineage.
pub(crate) async fn validate_speculative(
    pool: &PgPool,
    loaded: &LoadedBatch,
    resume_marker: Option<(i64, &str)>,
    statement_timeout: Option<NonZeroU32>,
) -> Result<bool> {
    let Some(certificate) = &loaded.speculative_certificate else {
        return Ok(false);
    };
    let mut tx = pool.begin().await.map_err(|error| {
        InterpretError::database("failed to begin speculative validation snapshot", error)
    })?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            InterpretError::database("failed to configure speculative validation snapshot", error)
        })?;
    if let Some(timeout) = statement_timeout {
        sqlx::query(&format!(
            "SET LOCAL statement_timeout = '{}s'",
            timeout.get()
        ))
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            InterpretError::database("failed to bound speculative validation reads", error)
        })?;
    }
    validate_snapshot_resume_marker(&mut tx, &loaded.input.chain_id, resume_marker).await?;
    let valid = matches_snapshot(&mut tx, loaded, certificate).await?;
    tx.commit().await.map_err(|error| {
        InterpretError::database("failed to commit speculative validation snapshot", error)
    })?;
    Ok(valid)
}

async fn matches_snapshot(
    connection: &mut PgConnection,
    loaded: &LoadedBatch,
    certificate: &SpeculativeCertificate,
) -> Result<bool> {
    let input = &loaded.input;
    let chain = input.chain_id.as_str();
    let before = certificate.from_block;
    if cache::orphaning_epoch(connection, chain).await?
        != loaded.prior_cache.validated_orphaning_epoch
    {
        return Ok(false);
    }
    let (active, provenance) = manifests::load(connection, chain).await?;
    if active != input.manifests || provenance != loaded.provenance_manifests {
        return Ok(false);
    }
    let other_families = lookahead_query::other_manifest_families(connection, chain).await?;
    if other_families != certificate.other_families
        || full_state_reason(&active, &provenance).is_some()
        || retained_family_reason(connection, chain, before, &other_families)
            .await?
            .is_some()
    {
        return Ok(false);
    }
    if load_discovery_rules(connection, chain).await? != input.discovery_rules {
        return Ok(false);
    }
    let mut admissions = load_admissions(connection, chain, before).await?;
    admissions.extend(migration::admissions(connection, chain, before).await?);
    if admissions != input.admissions
        || load_blocks(connection, chain, before, certificate.to_block).await? != input.blocks
        || load_raw_logs(connection, chain, before, certificate.to_block).await? != input.raw_logs
    {
        return Ok(false);
    }
    let predecessor = resume::predecessor_timestamp(connection, chain, before).await?;
    let has_v2_manifest = provenance
        .iter()
        .map(|manifest| manifest.source_family.as_str())
        .chain(other_families.iter().map(|(family, _)| family.as_str()))
        .any(|family| family.starts_with("ens_v2_"));
    let latest_v2_topology = if has_v2_manifest {
        lookahead_query::v2_latest_topology(connection, chain, before).await?
    } else {
        None
    };
    if predecessor != certificate.predecessor
        || has_v2_manifest != certificate.has_v2_manifest
        || latest_v2_topology != certificate.latest_v2_topology
    {
        return Ok(false);
    }
    let Some(last) = input.blocks.last() else {
        return Ok(false);
    };
    if lookahead_query::due_names(connection, chain, before, predecessor, last.block_timestamp)
        .await?
        != certificate.due_names
    {
        return Ok(false);
    }
    let window_start = match (latest_v2_topology, predecessor) {
        (Some(_), Some(predecessor)) => predecessor.unix_timestamp(),
        _ => i64::MIN,
    };
    let window = (window_start, last.block_timestamp.unix_timestamp());
    if certificate.dependencies.v2_due_window != Some(window) {
        return Ok(false);
    }
    let due_v2_keys = if has_v2_manifest {
        lookahead_query::v2_due_keys(connection, chain, before, window).await?
    } else {
        Vec::new()
    };
    if due_v2_keys.into_iter().collect::<BTreeSet<_>>() != certificate.due_v2_keys {
        return Ok(false);
    }
    // Starting from the complete successful request set avoids rerunning interpretation to
    // rediscover dynamic dependencies. Fresh queries catch additions, deletions and new links.
    let mut dependencies = certificate.dependencies.clone();
    let prior = load_closure(
        connection,
        chain,
        before,
        &mut dependencies,
        &mut Fetched::default(),
    )
    .await?;
    Ok(dependencies == certificate.dependencies && prior == certificate.prior_events)
}
