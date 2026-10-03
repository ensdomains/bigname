#[path = "session_interpret.rs"]
mod interpret;
use interpret::interpret_loaded;
#[path = "session_transaction.rs"]
mod transaction;
use std::collections::{BTreeMap, BTreeSet};
use transaction::TransactionIndex;

use anyhow::{Context, bail};
use serde_json::{Value, json};

use super::{
    BatchInput, BatchOutput, RawLogInput, catalog::Catalog, seam::INTERPRETER_STATE_KEY,
    sourced_events::prepare_v1_state_derived_events as prepare_v1, state::State,
    state_residency::StateCacheCapacity,
};

/// Opaque retained adapter state that can be moved into the next batch for the same chain.
#[derive(Debug, Eq, PartialEq)]
pub struct AdapterSession {
    chain_id: String,
    state: State,
}

#[cfg(test)]
#[path = "session_test_access.rs"]
mod test_access;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterpreterStateValue {
    pub state_key: String,
    pub after_state: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterpreterStateRequest {
    pub state_key: String,
}

pub struct PreparedAdapterBatch {
    chain_id: String,
    blocks: Vec<super::RawBlockInput>,
    output: BatchOutput,
    committed_state: State,
    prior_tails: BTreeMap<String, Value>,
    state_value_requests: Vec<InterpreterStateRequest>,
}

pub struct AdapterSessionRestore {
    chain_id: String,
    state: State,
}

impl AdapterSessionRestore {
    pub fn apply_prior_events(
        &mut self,
        events: Vec<super::PriorEventInput>,
    ) -> anyhow::Result<()> {
        if let Some(event) = events.iter().find(|event| event.chain_id != self.chain_id) {
            bail!(
                "restored event for chain {} cannot enter adapter session for chain {}",
                event.chain_id,
                self.chain_id
            );
        }
        self.state.restore_prior_event_chunk(events);
        self.state.ensure_restore_succeeded()?;
        Ok(())
    }

    pub(super) fn include_v2_topology_timestamp(&mut self, at: Option<time::OffsetDateTime>) {
        if let Some(at) = at {
            self.state
                .include_v2_topology_timestamp(at.unix_timestamp());
        }
    }

    pub fn finish(
        mut self,
        resume_predecessor_timestamp: Option<time::OffsetDateTime>,
    ) -> AdapterSession {
        self.state.finish_prior_event_restore(
            resume_predecessor_timestamp.map(time::OffsetDateTime::unix_timestamp),
        );
        AdapterSession {
            chain_id: self.chain_id,
            state: self.state,
        }
    }
}

pub fn begin_schema_v2_adapter_restore(
    chain_id: String,
    manifests: Vec<super::ManifestInput>,
    discovery_rules: Vec<super::DiscoveryRuleInput>,
    admissions: Vec<super::AddressAdmissionInput>,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<AdapterSessionRestore> {
    let provenance_manifests = manifests.clone();
    begin_schema_v2_adapter_restore_with_provenance(
        chain_id,
        manifests,
        provenance_manifests,
        discovery_rules,
        admissions,
        cache_capacity,
    )
}

pub fn begin_schema_v2_adapter_restore_with_provenance(
    chain_id: String,
    manifests: Vec<super::ManifestInput>,
    provenance_manifests: Vec<super::ManifestInput>,
    discovery_rules: Vec<super::DiscoveryRuleInput>,
    admissions: Vec<super::AddressAdmissionInput>,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<AdapterSessionRestore> {
    let catalog =
        Catalog::new_with_provenance(manifests, provenance_manifests, discovery_rules, admissions)?;
    Ok(AdapterSessionRestore {
        chain_id,
        state: State::with_cache_capacity_and_manifest_ids(
            Vec::new(),
            catalog.v2_suffix_anchors(),
            cache_capacity,
            Some(catalog.provenance_ids()),
        ),
    })
}

impl PreparedAdapterBatch {
    pub fn state_value_requests(&self) -> &[InterpreterStateRequest] {
        &self.state_value_requests
    }

    pub fn finish(
        mut self,
        loaded: Vec<InterpreterStateValue>,
    ) -> anyhow::Result<(BatchOutput, AdapterSession)> {
        let expected = self
            .state_value_requests
            .iter()
            .map(|request| request.state_key.as_str())
            .collect::<BTreeSet<_>>();
        let mut supplied = BTreeSet::new();
        for value in loaded {
            if !expected.contains(value.state_key.as_str()) {
                bail!(
                    "loaded interpreter state key {} was not requested by the adapter",
                    value.state_key
                );
            }
            if !supplied.insert(value.state_key.clone()) {
                bail!(
                    "loaded interpreter state key {} more than once",
                    value.state_key
                );
            }
            self.prior_tails.insert(value.state_key, value.after_state);
        }
        rethread_before_states(&mut self.output, self.prior_tails);
        let delta = super::seam::fold_prior_events(
            Vec::new(),
            &self.output.normalized_events,
            &self.blocks,
        )?;
        self.committed_state.apply_prior_event_delta(delta);
        self.committed_state.ensure_restore_succeeded()?;
        if let Some(block) = self.blocks.last() {
            self.committed_state
                .commit_v2_batch_boundary(block.block_timestamp.unix_timestamp());
        }
        Ok((
            self.output,
            AdapterSession {
                chain_id: self.chain_id,
                state: self.committed_state,
            },
        ))
    }
}

pub fn interpret_schema_v2_batch(input: BatchInput) -> anyhow::Result<BatchOutput> {
    #[cfg(test)]
    {
        let incremental_input = input.clone();
        let restore_input = input.clone();
        let expected = interpret_fresh(input)?;
        let (actual, session) = prepare_schema_v2_batch_incremental(
            incremental_input,
            None,
            StateCacheCapacity::Unlimited,
        )?
        .finish(Vec::new())?;
        anyhow::ensure!(
            actual == expected,
            "incremental adapter output differs from fresh one-shot interpretation"
        );
        assert_restores_exactly(restore_input, &actual, &session)?;
        Ok(actual)
    }
    #[cfg(not(test))]
    {
        interpret_fresh(input)
    }
}

fn interpret_fresh(input: BatchInput) -> anyhow::Result<BatchOutput> {
    super::validate_order(&input)?;
    let BatchInput {
        manifests,
        discovery_rules,
        admissions,
        prior_events,
        blocks,
        raw_logs,
        ..
    } = input;
    let mut catalog = Catalog::new(manifests, discovery_rules, admissions)?;
    let mut state = State::with_cache_capacity_and_manifest_ids(
        prior_events,
        catalog.v2_suffix_anchors(),
        StateCacheCapacity::Unlimited,
        Some(catalog.provenance_ids()),
    );
    state.ensure_restore_succeeded()?;
    let mut output = interpret_loaded(&mut catalog, &blocks, raw_logs, &mut state)?;
    let (prior_tails, missing) = required_prior_tails(&mut state, &output);
    debug_assert!(
        missing
            .iter()
            .all(|request| !prior_tails.contains_key(&request.state_key))
    );
    rethread_before_states(&mut output, prior_tails);
    Ok(output)
}

pub fn prepare_schema_v2_batch_incremental(
    input: BatchInput,
    session: Option<AdapterSession>,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<PreparedAdapterBatch> {
    let provenance_manifests = input.manifests.clone();
    prepare_schema_v2_batch_incremental_with_provenance(
        input,
        provenance_manifests,
        session,
        cache_capacity,
    )
}

pub fn prepare_schema_v2_batch_incremental_with_provenance(
    input: BatchInput,
    provenance_manifests: Vec<super::ManifestInput>,
    session: Option<AdapterSession>,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<PreparedAdapterBatch> {
    super::validate_order(&input)?;
    let BatchInput {
        chain_id,
        manifests,
        discovery_rules,
        admissions,
        prior_events,
        blocks,
        raw_logs,
    } = input;
    let mut catalog =
        Catalog::new_with_provenance(manifests, provenance_manifests, discovery_rules, admissions)?;
    let suffix_anchors = catalog.v2_suffix_anchors();
    let mut committed_state = match session {
        Some(session) => {
            if session.chain_id != chain_id {
                bail!(
                    "adapter session for chain {} cannot interpret chain {chain_id}",
                    session.chain_id
                );
            }
            if !prior_events.is_empty() {
                bail!("a resumed adapter session cannot also receive restored prior events");
            }
            let mut state = session.state;
            state.replace_known_source_manifest_ids(catalog.provenance_ids());
            state.replace_v2_suffix_anchors(suffix_anchors);
            state
        }
        None => State::with_cache_capacity_and_manifest_ids(
            prior_events,
            suffix_anchors,
            cache_capacity,
            Some(catalog.provenance_ids()),
        ),
    };
    committed_state.ensure_restore_succeeded()?;
    // Reconciliation can discard provisional transitions. Interpret on a structurally shared
    // branch, then advance the retained state with only the normalized events that survived.
    let mut state = committed_state.clone();
    let output = interpret_loaded(&mut catalog, &blocks, raw_logs, &mut state)?;
    let (prior_tails, state_value_requests) = required_prior_tails(&mut committed_state, &output);
    Ok(PreparedAdapterBatch {
        chain_id,
        blocks,
        output,
        committed_state,
        prior_tails,
        state_value_requests,
    })
}

fn append_output(into: &mut BatchOutput, from: BatchOutput) {
    let BatchOutput {
        decode_skips,
        normalized_events,
        label_preimages,
        name_surfaces,
        token_lineages,
        resources,
        surface_bindings,
        binding_closures,
        contract_instances,
        contract_addresses,
        discovery_edges,
        discovery_edge_closures,
        migration_event_associations,
        migration_discovery_associations,
        migration_candidate_identity_effects,
        migration_candidate_discovery_effects,
        migration_authority_transitions,
    } = from;
    into.decode_skips.extend(decode_skips);
    into.normalized_events.extend(normalized_events);
    into.label_preimages.extend(label_preimages);
    into.name_surfaces.extend(name_surfaces);
    into.token_lineages.extend(token_lineages);
    into.resources.extend(resources);
    into.surface_bindings.extend(surface_bindings);
    into.binding_closures.extend(binding_closures);
    into.contract_instances.extend(contract_instances);
    into.contract_addresses.extend(contract_addresses);
    into.discovery_edges.extend(discovery_edges);
    into.discovery_edge_closures.extend(discovery_edge_closures);
    into.migration_event_associations
        .extend(migration_event_associations);
    into.migration_discovery_associations
        .extend(migration_discovery_associations);
    into.migration_candidate_identity_effects
        .extend(migration_candidate_identity_effects);
    into.migration_candidate_discovery_effects
        .extend(migration_candidate_discovery_effects);
    into.migration_authority_transitions
        .extend(migration_authority_transitions);
}

fn required_prior_tails(
    state: &mut State,
    output: &BatchOutput,
) -> (BTreeMap<String, Value>, Vec<InterpreterStateRequest>) {
    let mut seen = BTreeSet::new();
    let mut tails = BTreeMap::new();
    let mut requests = Vec::new();
    for event in &output.normalized_events {
        let Some(state_key) = event
            .raw_fact_ref
            .get(INTERPRETER_STATE_KEY)
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !seen.insert(state_key.to_owned()) || event.before_state_explicit {
            continue;
        }
        if let Some(value) = state.value_tail(state_key) {
            tails.insert(state_key.to_owned(), value);
        } else {
            requests.push(InterpreterStateRequest {
                state_key: state_key.to_owned(),
            });
        }
    }
    (tails, requests)
}

// Reconciliation can drop events or rewrite their interpreter state keys after in-memory
// transitions have already read state written under the pre-reconciliation keys. Re-derive every
// stream-chained before_state from the surviving rows so the emitted stream — and nothing else —
// determines what each event observes, keeping the output independent of where batch boundaries
// fall.
fn rethread_before_states(output: &mut BatchOutput, prior_tails: BTreeMap<String, Value>) {
    let mut tails = prior_tails;
    for event in &mut output.normalized_events {
        let Some(state_key) = event
            .raw_fact_ref
            .get(INTERPRETER_STATE_KEY)
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !event.before_state_explicit {
            event.before_state = tails.get(state_key).cloned().unwrap_or_else(|| json!({}));
        }
        tails.insert(state_key.to_owned(), event.after_state.clone());
    }
}

fn interpret_raw(
    catalog: &mut Catalog,
    raw: &RawLogInput,
    state: &mut State,
    output: &mut BatchOutput,
    migration_observations: &mut Vec<super::protocol::MigrationObservation>,
    transactions: &TransactionIndex,
) -> anyhow::Result<bool> {
    let Some(selected) = catalog.select(raw)? else {
        return Ok(false);
    };
    let mut registrar_context = super::migration::registrar_context(catalog, &selected, raw)?;
    registrar_context.registry_graveyard =
        super::protocol::v1::registry_graveyard(catalog, &selected, raw)?;
    registrar_context.wrapper_custody.name_wrapper =
        super::protocol::v1::registry_name_wrapper(catalog, &selected)?;
    transactions.apply(&selected, raw, &mut registrar_context)?;
    let registrar_migration_source = registrar_context
        .migration_enabled
        .then(|| catalog.source_for_family("ens_v2_migration_l1").cloned())
        .flatten();
    // Some protocol paths advance time-derived state before reaching their event-specific
    // decoder. Interpret each log on a structurally shared candidate and commit it only after the
    // whole protocol dispatch succeeds, so a non-fatal malformed log cannot change retained state.
    let mut candidate_state = state.clone();
    let mut interpreted =
        match super::protocol::interpret(&selected, raw, &mut candidate_state, registrar_context) {
            Ok(interpreted) => interpreted,
            Err(error)
                if crate::evm_abi::is_malformed_event_log(&error)
                    && !selected.manifest_declared_emitter =>
            {
                output.decode_skips.push(super::DecodeSkip {
                    chain_id: raw.chain_id.clone(),
                    block_hash: raw.block_hash.clone(),
                    block_number: raw.block_number,
                    transaction_hash: raw.transaction_hash.clone(),
                    log_index: raw.log_index,
                    emitting_address: raw.emitting_address.clone(),
                    source_family: selected.source.source_family.clone(),
                    selection_topic0: selected.event.topic0.clone(),
                    match_all: selected.match_all,
                    decode_context: error.to_string(),
                });
                return Ok(true);
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "{} adapter failed for raw log {}:{}",
                        selected.source.source_family, raw.block_hash, raw.log_index
                    )
                });
            }
        };
    prepare_v1(
        catalog,
        &selected,
        raw,
        &mut interpreted,
        &mut candidate_state,
    )?;
    *state = candidate_state;
    super::normalized::materialize(&selected, raw, interpreted.events.clone(), state, output);
    super::sourced_events::materialize(
        catalog,
        &selected.source.namespace,
        raw,
        interpreted.sourced_events.clone(),
        state,
        output,
    )?;
    super::normalized::materialize_boundary(
        &selected.source,
        &super::model::RawBlockInput {
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            block_timestamp: raw.block_timestamp,
            canonicality_state: raw.canonicality_state.clone(),
        },
        interpreted.boundary_events.clone(),
        state,
        output,
    );
    super::identity::materialize(&selected, raw, &interpreted, state, output)?;
    super::discovery::materialize(
        catalog,
        &selected,
        raw,
        interpreted.discovery,
        state,
        output,
    )?;
    if let Some(migration_source) = registrar_migration_source {
        migration_observations.extend(interpreted.migration_observations);
        super::normalized::materialize_for_source(
            &migration_source,
            raw,
            interpreted.migration_events,
            state,
            output,
        );
    } else {
        debug_assert!(interpreted.migration_events.is_empty());
        migration_observations.extend(interpreted.migration_observations);
    }
    Ok(true)
}

#[cfg(test)]
fn assert_restores_exactly(
    input: BatchInput,
    output: &BatchOutput,
    session: &AdapterSession,
) -> anyhow::Result<()> {
    let catalog = Catalog::new(input.manifests, input.discovery_rules, input.admissions)?;
    let prior = super::seam::fold_prior_events(
        input.prior_events,
        &output.normalized_events,
        &input.blocks,
    )?;
    let restored_state = State::with_cache_capacity_and_manifest_ids(
        prior,
        catalog.v2_suffix_anchors(),
        StateCacheCapacity::Unlimited,
        Some(catalog.provenance_ids()),
    );
    restored_state.ensure_restore_succeeded()?;
    let mut restored = AdapterSession {
        chain_id: input.chain_id,
        state: restored_state,
    };
    if let Some(block) = input.blocks.last() {
        restored
            .state
            .commit_v2_batch_boundary(block.block_timestamp.unix_timestamp());
    }
    anyhow::ensure!(
        session == &restored,
        "live adapter session state differs from a fresh retained-event restore:\n\
         live={session:#?}\nrestored={restored:#?}"
    );
    Ok(())
}
