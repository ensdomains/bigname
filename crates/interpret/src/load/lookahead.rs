//! Experimental V1 batch state loading. Canonical history remains the sole durable state.
use bigname_adapters::schema_v2::{
    BatchInput, StateCacheCapacity, V1BatchDependencies, V1NodeRequest,
    begin_schema_v2_adapter_restore_with_provenance, collect_v1_batch_dependencies,
};
use sqlx::PgPool;

use super::{LoadedBatch, cache, lookahead_query, manifests, migration, resume};
use crate::{InterpretError, Result};

const MAX_REQUESTS: usize = 100_000;
const MAX_PRIOR_EVENTS: usize = 100_000;
const MAX_DEPENDENCY_ROUNDS: usize = 16;

pub(crate) async fn batch_input(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    resume_marker: Option<(i64, &str)>,
    state_cache_capacity: StateCacheCapacity,
) -> Result<LoadedBatch> {
    let mut tx = pool.begin().await.map_err(|error| {
        InterpretError::database("failed to begin lookahead input snapshot", error)
    })?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            InterpretError::database("failed to configure lookahead input snapshot", error)
        })?;
    sqlx::query("SET LOCAL statement_timeout = '60s'")
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            InterpretError::database("failed to bound lookahead database reads", error)
        })?;
    super::validate_snapshot_resume_marker(&mut tx, chain_id, resume_marker).await?;
    let orphaning_epoch = cache::orphaning_epoch(&mut tx, chain_id).await?;
    let (manifests, provenance) = manifests::load(&mut tx, chain_id).await?;
    if manifests.is_empty() {
        return Err(InterpretError::configuration(
            "lookahead requires active manifests",
        ));
    }
    let discovery_rules = super::load_discovery_rules(&mut tx, chain_id).await?;
    let mut admissions = super::load_admissions(&mut tx, chain_id, from_block).await?;
    admissions.extend(migration::admissions(&mut tx, chain_id, from_block).await?);
    let blocks = super::load_blocks(&mut tx, chain_id, from_block, to_block).await?;
    let raw_logs = super::load_raw_logs(&mut tx, chain_id, from_block, to_block).await?;
    let predecessor = resume::predecessor_timestamp(&mut tx, chain_id, from_block).await?;
    let last_timestamp = blocks
        .last()
        .ok_or_else(|| InterpretError::data_integrity("lookahead batch has no canonical blocks"))?
        .block_timestamp;
    let input = BatchInput {
        chain_id: chain_id.to_owned(),
        manifests,
        discovery_rules,
        admissions,
        blocks,
        raw_logs,
        prior_events: Vec::new(),
    };
    let mut dependencies = collect_v1_batch_dependencies(&input, &provenance)
        .map_err(|error| invalid_dependencies("decode", error))?;
    validate_dependencies(&dependencies)?;
    for name in lookahead_query::due_names(
        &mut tx,
        chain_id,
        from_block,
        predecessor,
        last_timestamp,
        MAX_REQUESTS,
    )
    .await?
    {
        let (namespace, node) = name.split_once(':').ok_or_else(|| {
            InterpretError::data_integrity("lookahead expiry candidate has no namespace")
        })?;
        dependencies.nodes.insert(V1NodeRequest {
            namespace: namespace.to_owned(),
            node: node.to_owned(),
        });
    }
    let mut prior = None;
    for _ in 0..MAX_DEPENDENCY_ROUNDS {
        validate_dependencies(&dependencies)?;
        let previous = (dependencies.nodes.len(), dependencies.resource_ids.len());
        let names: Vec<_> = dependencies
            .nodes
            .iter()
            .map(|request| format!("{}:{}", request.namespace, request.node))
            .collect();
        let resources: Vec<_> = dependencies.resource_ids.iter().copied().collect();
        let events = lookahead_query::events(
            &mut tx,
            chain_id,
            from_block,
            &names,
            &resources,
            MAX_PRIOR_EVENTS,
        )
        .await?;
        dependencies
            .include_prior_events(&events)
            .map_err(|error| invalid_dependencies("expand prior links", error))?;
        validate_dependencies(&dependencies)?;
        if previous == (dependencies.nodes.len(), dependencies.resource_ids.len()) {
            prior = Some(events);
            break;
        }
        // Discard this partial fetch before querying the expanded set. Never retain the
        // previous round's full payload while loading the next one.
    }
    let prior = prior.ok_or_else(|| {
        InterpretError::data_integrity(
            "lookahead dependency closure exceeds 16 rounds; refusing incomplete state",
        )
    })?;
    let restored_event_count = prior.len();
    let mut restore = begin_schema_v2_adapter_restore_with_provenance(
        chain_id.to_owned(),
        input.manifests.clone(),
        provenance.clone(),
        input.discovery_rules.clone(),
        input.admissions.clone(),
        state_cache_capacity,
    )
    .map_err(|error| invalid_dependencies("begin restore", error))?;
    restore
        .apply_prior_events(prior)
        .map_err(|error| invalid_dependencies("restore", error))?;
    let adapter_session = restore.finish(predecessor);
    tx.commit().await.map_err(|error| {
        InterpretError::database("failed to commit lookahead input snapshot", error)
    })?;
    Ok(LoadedBatch {
        input,
        provenance_manifests: provenance,
        prior_cache: cache::freshly_loaded(orphaning_epoch),
        adapter_session: Some(adapter_session),
        restored_event_count,
        lookahead_nodes: Some(dependencies.nodes),
    })
}

fn validate_dependencies(dependencies: &V1BatchDependencies) -> Result<()> {
    if !dependencies.unsupported.is_empty() {
        return Err(InterpretError::configuration(format!(
            "experimental V1 lookahead does not cover: {:?}",
            dependencies.unsupported,
        )));
    }
    if dependencies
        .nodes
        .len()
        .saturating_add(dependencies.resource_ids.len())
        > MAX_REQUESTS
    {
        return Err(InterpretError::data_integrity(
            "lookahead exceeds 100000 dependency requests; refusing incomplete state",
        ));
    }
    Ok(())
}

fn invalid_dependencies(operation: &str, error: anyhow::Error) -> InterpretError {
    InterpretError::data_integrity(format!("lookahead {operation} failed: {error:#}"))
}

#[cfg(test)]
#[path = "lookahead_tests.rs"]
mod tests;
