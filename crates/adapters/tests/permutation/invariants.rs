use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{
    BatchInput, BatchOutput, interpret_schema_v2_batch, interpret_schema_v2_batch_incremental, seam,
};
use serde_json::Value;
use uuid::Uuid;

use super::{
    convergence::{KnownDivergence, assert_converged},
    rng::Rng,
};

/// Cumulative identity rows already visible to the persistence transport, so a later batch may
/// reference what an earlier one materialized without re-emitting it.
pub struct Ledger {
    resources: BTreeSet<(String, Uuid)>,
    lineages: BTreeSet<(String, Uuid)>,
    surfaces: BTreeSet<String>,
    bindings: BTreeSet<Uuid>,
    instances: BTreeSet<Uuid>,
    positions: BTreeMap<String, (i64, i64, i64)>,
}

impl Ledger {
    pub fn new(declared_instances: &[Uuid]) -> Self {
        Self {
            resources: BTreeSet::new(),
            lineages: BTreeSet::new(),
            surfaces: BTreeSet::new(),
            bindings: BTreeSet::new(),
            instances: declared_instances.iter().copied().collect(),
            positions: BTreeMap::new(),
        }
    }

    pub fn absorb(&mut self, context: &str, blocks: &[i64], output: &BatchOutput) -> Result<()> {
        let live = blocks.iter().copied().collect::<BTreeSet<_>>();
        for lineage in &output.token_lineages {
            self.lineages
                .insert((lineage.chain_id.clone(), lineage.token_lineage_id));
        }
        for surface in &output.name_surfaces {
            self.surfaces.insert(surface.logical_name_id.clone());
        }
        for instance in &output.contract_instances {
            self.instances.insert(instance.contract_instance_id);
        }
        for resource in &output.resources {
            if let Some(lineage) = resource.token_lineage_id
                && !self
                    .lineages
                    .contains(&(resource.chain_id.clone(), lineage))
            {
                bail!(
                    "{context}: resource {} references unknown token lineage {lineage}",
                    resource.resource_id
                );
            }
            self.resources
                .insert((resource.chain_id.clone(), resource.resource_id));
        }
        // normalized_events carries foreign keys to both name_surfaces and resources
        // (schema-v2/baseline/05_normalized_events.sql), which is the constraint the production
        // lease-release crash violated.
        for event in &output.normalized_events {
            if let Some(resource) = event.resource_id
                && !self.resources.contains(&(event.chain_id.clone(), resource))
            {
                bail!(
                    "{context}: event {} references unknown resource {resource}",
                    event.event_identity
                );
            }
            if let Some(logical) = event.logical_name_id.as_ref()
                && !self.surfaces.contains(logical)
            {
                bail!(
                    "{context}: event {} references unknown name surface {logical}",
                    event.event_identity
                );
            }
        }
        for binding in &output.surface_bindings {
            if !self
                .resources
                .contains(&(binding.chain_id.clone(), binding.resource_id))
            {
                bail!(
                    "{context}: binding {} references unknown resource {}",
                    binding.surface_binding_id,
                    binding.resource_id
                );
            }
            if !self.surfaces.contains(&binding.logical_name_id) {
                bail!(
                    "{context}: binding {} references unknown name surface {}",
                    binding.surface_binding_id,
                    binding.logical_name_id
                );
            }
            self.bindings.insert(binding.surface_binding_id);
        }
        for closure in &output.binding_closures {
            if let Some(binding) = closure.except_surface_binding_id
                && !self.bindings.contains(&binding)
            {
                bail!(
                    "{context}: binding closure for {} exempts unknown binding {binding}",
                    closure.logical_name_id
                );
            }
            if !self.surfaces.contains(&closure.logical_name_id) {
                bail!(
                    "{context}: binding closure references unknown name surface {}",
                    closure.logical_name_id
                );
            }
        }
        for address in &output.contract_addresses {
            if !self.instances.contains(&address.contract_instance_id) {
                bail!(
                    "{context}: contract address {} references unknown contract instance {}",
                    address.address,
                    address.contract_instance_id
                );
            }
        }
        for edge in &output.discovery_edges {
            for (side, instance) in [
                ("from", edge.from_contract_instance_id),
                ("to", edge.to_contract_instance_id),
            ] {
                if !self.instances.contains(&instance) {
                    bail!(
                        "{context}: discovery edge {} references unknown {side} contract instance {instance}",
                        edge.edge_kind
                    );
                }
            }
        }
        for closure in &output.discovery_edge_closures {
            if !self.instances.contains(&closure.from_contract_instance_id) {
                bail!(
                    "{context}: discovery closure {} references unknown from contract instance {}",
                    closure.edge_kind,
                    closure.from_contract_instance_id
                );
            }
        }
        self.check_canonicality(context, &live, output)
    }

    fn check_canonicality(
        &mut self,
        context: &str,
        live: &BTreeSet<i64>,
        output: &BatchOutput,
    ) -> Result<()> {
        for (kind, state, block) in row_canonicality(output) {
            if state != "canonical" {
                bail!("{context}: {kind} row derived from canonical raw facts is {state}");
            }
            if let Some(block) = block
                && !live.contains(&block)
            {
                bail!("{context}: {kind} row is anchored to block {block} outside its batch");
            }
        }
        for event in &output.normalized_events {
            let key = seam::retained_prior_state_key(
                event
                    .raw_fact_ref
                    .get(seam::INTERPRETER_STATE_KEY)
                    .and_then(Value::as_str),
                &event.event_identity,
            );
            let position = (
                event.block_number.unwrap_or(i64::MIN),
                event.transaction_index.unwrap_or(-1),
                event.log_index.unwrap_or(-1),
            );
            if let Some(previous) = self.positions.get(&key)
                && position < *previous
            {
                bail!(
                    "{context}: derived state for {key} moved backward from {previous:?} to {position:?}",
                );
            }
            self.positions.insert(key, position);
        }
        // A closure applies to the bindings at or before its own position, so exempting a binding
        // the sequence has not opened yet would leave the name with no active binding at all.
        for closure in &output.binding_closures {
            let Some(exempt) = closure.except_surface_binding_id else {
                continue;
            };
            let Some(binding) = output
                .surface_bindings
                .iter()
                .find(|binding| binding.surface_binding_id == exempt)
            else {
                continue;
            };
            if binding_position(binding) > closure_position(closure) {
                bail!(
                    "{context}: binding closure at {:?} exempts binding {exempt} opened later at {:?}",
                    closure_position(closure),
                    binding_position(binding)
                );
            }
        }
        Ok(())
    }
}

fn binding_position(binding: &bigname_adapters::schema_v2::SurfaceBinding) -> (i64, i64, i64) {
    (
        binding.block_number,
        binding
            .provenance
            .get(seam::TRANSACTION_INDEX_KEY)
            .and_then(Value::as_i64)
            .unwrap_or(-1),
        binding
            .provenance
            .get(seam::LOG_INDEX_KEY)
            .and_then(Value::as_i64)
            .unwrap_or(-1),
    )
}

fn closure_position(closure: &bigname_adapters::schema_v2::BindingClosure) -> (i64, i64, i64) {
    (
        closure.block_number,
        closure.transaction_index,
        closure.log_index,
    )
}

fn row_canonicality(output: &BatchOutput) -> Vec<(&'static str, &str, Option<i64>)> {
    let mut rows = Vec::new();
    rows.extend(output.normalized_events.iter().map(|row| {
        (
            "normalized event",
            row.canonicality_state.as_str(),
            row.block_number,
        )
    }));
    rows.extend(output.name_surfaces.iter().map(|row| {
        (
            "name surface",
            row.canonicality_state.as_str(),
            Some(row.block_number),
        )
    }));
    rows.extend(output.resources.iter().map(|row| {
        (
            "resource",
            row.canonicality_state.as_str(),
            Some(row.block_number),
        )
    }));
    rows.extend(output.token_lineages.iter().map(|row| {
        (
            "token lineage",
            row.canonicality_state.as_str(),
            Some(row.block_number),
        )
    }));
    rows.extend(output.surface_bindings.iter().map(|row| {
        (
            "surface binding",
            row.canonicality_state.as_str(),
            Some(row.block_number),
        )
    }));
    rows.extend(output.discovery_edges.iter().map(|row| {
        (
            "discovery edge",
            row.canonicality_state.as_str(),
            Some(row.active_from_block_number),
        )
    }));
    rows
}

pub struct Replayed {
    pub blocks: Vec<i64>,
    pub output: BatchOutput,
}

pub struct Converged {
    pub batches: Vec<Replayed>,
    pub known: KnownDivergence,
}

/// Runs the same sequence three ways — one fresh pass, one incremental pass, and one pass split
/// into resumed batches — and requires all three to derive identical state.
pub fn converge(context: &str, input: BatchInput, split_seed: u64) -> Result<Converged> {
    let fresh = interpret_schema_v2_batch(input.clone())
        .with_context(|| format!("{context}: fresh interpretation failed"))?;
    let (incremental, live) = interpret_schema_v2_batch_incremental(input.clone(), None)
        .with_context(|| format!("{context}: incremental interpretation failed"))?;
    if incremental != fresh {
        bail!("{context}: incremental output differs from the fresh pass");
    }
    let retained = seam::fold_prior_events(Vec::new(), &fresh.normalized_events, &input.blocks)?;
    let (_, restored) = interpret_schema_v2_batch_incremental(
        BatchInput {
            prior_events: retained,
            blocks: Vec::new(),
            raw_logs: Vec::new(),
            ..input.clone()
        },
        None,
    )?;
    if live != restored {
        bail!("{context}: live adapter state differs from a compacted restore");
    }

    let mut session = None;
    let mut prior = Vec::new();
    let mut replayed = BatchOutput::default();
    let mut outputs = Vec::new();
    for (index, range) in split(input.blocks.len(), split_seed)
        .into_iter()
        .enumerate()
    {
        let blocks = input.blocks[range.clone()].to_vec();
        let hashes = blocks
            .iter()
            .map(|block| block.block_hash.as_str())
            .collect::<BTreeSet<_>>();
        let raw_logs = input
            .raw_logs
            .iter()
            .filter(|log| hashes.contains(log.block_hash.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let restored_input = BatchInput {
            prior_events: prior.clone(),
            blocks: blocks.clone(),
            raw_logs,
            ..input.clone()
        };
        let restored_output = interpret_schema_v2_batch(restored_input.clone())
            .with_context(|| format!("{context}: split batch {index} failed a restored pass"))?;
        let (resumed_output, next) = interpret_schema_v2_batch_incremental(
            BatchInput {
                prior_events: if session.is_none() {
                    prior.clone()
                } else {
                    Vec::new()
                },
                ..restored_input.clone()
            },
            session,
        )
        .with_context(|| format!("{context}: split batch {index} failed a resumed pass"))?;
        if resumed_output != restored_output {
            bail!("{context}: split batch {index} resumed output differs from a restored pass");
        }
        prior = seam::fold_prior_events(prior, &resumed_output.normalized_events, &blocks)?;
        let (_, restored_session) = interpret_schema_v2_batch_incremental(
            BatchInput {
                prior_events: prior.clone(),
                blocks: Vec::new(),
                raw_logs: Vec::new(),
                ..input.clone()
            },
            None,
        )?;
        if next != restored_session {
            bail!("{context}: split batch {index} live state differs from a compacted restore");
        }
        session = Some(next);
        absorb_rows(&mut replayed, resumed_output.clone());
        outputs.push(Replayed {
            blocks: blocks.iter().map(|block| block.block_number).collect(),
            output: resumed_output,
        });
    }
    let known = assert_converged(context, &fresh, &replayed)?;
    Ok(Converged {
        batches: outputs,
        known,
    })
}

fn absorb_rows(into: &mut BatchOutput, from: BatchOutput) {
    into.normalized_events.extend(from.normalized_events);
    into.label_preimages.extend(from.label_preimages);
    into.name_surfaces.extend(from.name_surfaces);
    into.token_lineages.extend(from.token_lineages);
    into.resources.extend(from.resources);
    into.surface_bindings.extend(from.surface_bindings);
    into.binding_closures.extend(from.binding_closures);
    into.contract_instances.extend(from.contract_instances);
    into.contract_addresses.extend(from.contract_addresses);
    into.discovery_edges.extend(from.discovery_edges);
    into.discovery_edge_closures
        .extend(from.discovery_edge_closures);
}

pub fn split(len: usize, seed: u64) -> Vec<std::ops::Range<usize>> {
    if len == 0 {
        return Vec::new();
    }
    let mut rng = Rng::new(seed);
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < len {
        let end = (start + rng.between(1, 2)).min(len);
        ranges.push(start..end);
        start = end;
    }
    ranges
}
