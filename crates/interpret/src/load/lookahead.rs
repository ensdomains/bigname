//! Per-batch ENSv1 state loading: restore only the history of names the batch touches.
//! Canonical history remains the sole durable state.
use bigname_adapters::schema_v2::{
    BatchInput, ManifestInput, StateCacheCapacity, V1BatchDependencies, V1NodeRequest,
    begin_schema_v2_adapter_restore_with_provenance, collect_v1_batch_dependencies,
    restore_schema_v2_lookahead_session, v1_lookahead_supports_family,
};
use sqlx::PgPool;

use super::{LoadedBatch, cache, lookahead_query, manifests, migration, resume};
use crate::{FullStateReason, InterpretError, Result, StateLoader};

/// Either the batch restored by lookahead, or the reason this chain needs the full-state loader.
pub(crate) enum Attempt {
    Loaded(Box<LoadedBatch>),
    FullStateRequired(StateLoader),
}

// A termination guard, not a size limit. Each round either adds a name or resource from the
// chain's finite history or ends the loop, so the closure always terminates; sixteen rounds
// of links between names is far beyond any ENSv1 shape the adapter emits.
const MAX_DEPENDENCY_ROUNDS: usize = 16;

pub(crate) async fn batch_input(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    resume_marker: Option<(i64, &str)>,
    state_cache_capacity: StateCacheCapacity,
) -> Result<Attempt> {
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
        return Err(InterpretError::configuration(format!(
            "chain {chain_id} has no active manifests for interpretation"
        )));
    }
    // Decided inside this snapshot, so the choice matches the manifests the batch would use.
    // Deprecated manifests count too: the full-state loader restores their retained events,
    // and lookahead reads none from a family it does not cover.
    if let Some(reason) = full_state_reason(&manifests, &provenance) {
        return Ok(Attempt::FullStateRequired(StateLoader::FullState {
            reason,
        }));
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
    for name in
        lookahead_query::due_names(&mut tx, chain_id, from_block, predecessor, last_timestamp)
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
        let events =
            lookahead_query::events(&mut tx, chain_id, from_block, &names, &resources).await?;
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
    let restore = begin_schema_v2_adapter_restore_with_provenance(
        chain_id.to_owned(),
        input.manifests.clone(),
        provenance.clone(),
        input.discovery_rules.clone(),
        input.admissions.clone(),
        state_cache_capacity,
    )
    .map_err(|error| invalid_dependencies("begin restore", error))?;
    // Restore runs under the same loaded-names check as interpretation, so an event that
    // reaches a name whose history was not loaded fails the batch.
    let adapter_session =
        restore_schema_v2_lookahead_session(restore, prior, predecessor, &dependencies.nodes)
            .map_err(|error| invalid_dependencies("restore", error))?;
    tx.commit().await.map_err(|error| {
        InterpretError::database("failed to commit lookahead input snapshot", error)
    })?;
    Ok(Attempt::Loaded(Box::new(LoadedBatch {
        input,
        provenance_manifests: provenance,
        prior_cache: cache::freshly_loaded(orphaning_epoch),
        adapter_session: Some(adapter_session),
        restored_event_count,
        lookahead_nodes: Some(dependencies.nodes),
    })))
}

/// The first manifest, in the loader's stable order, whose source family lookahead does not
/// cover. `provenance` holds the active and the deprecated manifests.
fn full_state_reason(
    active: &[ManifestInput],
    provenance: &[ManifestInput],
) -> Option<FullStateReason> {
    let unsupported = |manifests: &[ManifestInput], rollout_status| {
        manifests
            .iter()
            .find(|manifest| !v1_lookahead_supports_family(&manifest.source_family))
            .map(|manifest| FullStateReason::UnsupportedSourceFamily {
                source_family: manifest.source_family.clone(),
                rollout_status,
            })
    };
    unsupported(active, "active").or_else(|| unsupported(provenance, "deprecated"))
}

fn validate_dependencies(dependencies: &V1BatchDependencies) -> Result<()> {
    if !dependencies.unsupported.is_empty() {
        // The loader was chosen because every manifest family is covered, and the prior-event
        // query reads only ENSv1 families, so this is a broken invariant, not configuration.
        return Err(InterpretError::data_integrity(format!(
            "lookahead was chosen but does not cover: {:?}",
            dependencies.unsupported,
        )));
    }
    Ok(())
}

fn invalid_dependencies(operation: &str, error: anyhow::Error) -> InterpretError {
    InterpretError::data_integrity(format!("lookahead {operation} failed: {error:#}"))
}

#[cfg(test)]
#[path = "lookahead_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lookahead_equivalence_tests.rs"]
mod equivalence_tests;
