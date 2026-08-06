use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{
    BatchInput, BatchOutput, interpret_schema_v2_batch, interpret_schema_v2_batch_incremental, seam,
};
use serde_json::Value;
use uuid::Uuid;

use super::{
    convergence::{BatchBoundaryArtifacts, assert_converged, assert_lineage_integrity},
    rng::Rng,
};

/// A chain position: block number, transaction index, log index.
type Position = (i64, i64, i64);

/// Models the identity rows a batch may reference: everything the persistence transport has
/// already committed, accumulated in commit order. `crates/interpret/src/write.rs` writes identity
/// rows, then discovery rows, then normalized events, one transaction per batch — so a row may
/// reference anything its own batch emits or anything an earlier batch emitted, and nothing else.
pub struct IdentityReferences {
    chain_id: String,
    resources: BTreeSet<(String, Uuid)>,
    lineages: BTreeSet<(String, Uuid)>,
    surfaces: BTreeSet<(String, String)>,
    bindings: BTreeMap<Uuid, Position>,
    instances: BTreeSet<(String, Uuid)>,
    positions: BTreeMap<String, Position>,
}

impl IdentityReferences {
    pub fn new(chain_id: &str, declared_instances: &[Uuid]) -> Self {
        Self {
            chain_id: chain_id.to_owned(),
            resources: BTreeSet::new(),
            lineages: BTreeSet::new(),
            surfaces: BTreeSet::new(),
            bindings: BTreeMap::new(),
            instances: declared_instances
                .iter()
                .map(|instance| (chain_id.to_owned(), *instance))
                .collect(),
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
            self.surfaces
                .insert((surface.chain_id.clone(), surface.logical_name_id.clone()));
        }
        for instance in &output.contract_instances {
            self.instances
                .insert((instance.chain_id.clone(), instance.contract_instance_id));
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
                && !self
                    .surfaces
                    .contains(&(event.chain_id.clone(), logical.clone()))
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
            if !self
                .surfaces
                .contains(&(binding.chain_id.clone(), binding.logical_name_id.clone()))
            {
                bail!(
                    "{context}: binding {} references unknown name surface {}",
                    binding.surface_binding_id,
                    binding.logical_name_id
                );
            }
            self.bindings
                .entry(binding.surface_binding_id)
                .or_insert(binding_position(binding)?);
        }
        for closure in &output.binding_closures {
            if let Some(binding) = closure.except_surface_binding_id
                && !self.bindings.contains_key(&binding)
            {
                bail!(
                    "{context}: binding closure for {} exempts unknown binding {binding}",
                    closure.logical_name_id
                );
            }
            if !self
                .surfaces
                .contains(&(self.chain_id.clone(), closure.logical_name_id.clone()))
            {
                bail!(
                    "{context}: binding closure references unknown name surface {}",
                    closure.logical_name_id
                );
            }
        }
        for address in &output.contract_addresses {
            if !self
                .instances
                .contains(&(address.chain_id.clone(), address.contract_instance_id))
            {
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
                if !self.instances.contains(&(edge.chain_id.clone(), instance)) {
                    bail!(
                        "{context}: discovery edge {} references unknown {side} contract instance {instance}",
                        edge.edge_kind
                    );
                }
            }
        }
        for closure in &output.discovery_edge_closures {
            if !self
                .instances
                .contains(&(closure.chain_id.clone(), closure.from_contract_instance_id))
            {
                bail!(
                    "{context}: discovery closure {} references unknown from contract instance {}",
                    closure.edge_kind,
                    closure.from_contract_instance_id
                );
            }
        }
        self.check_canonicality(context, &live, output)?;
        self.check_binding_exclusivity(context, output)
    }

    /// `surface_bindings_no_overlap` excludes overlapping `[active_from, active_to)` ranges per
    /// name. A binding that opens without a preceding closure is not an overlap — the writer caps
    /// the predecessor's `active_to` on every open, and `seam::binding_open_time` pushes the new
    /// start past it. Two distinct bindings for one name at the *same* chain position are, because
    /// they share an `active_from` no clamp can separate.
    fn check_binding_exclusivity(&self, context: &str, output: &BatchOutput) -> Result<()> {
        let mut by_position: BTreeMap<(&str, Position), BTreeSet<Uuid>> = BTreeMap::new();
        for binding in &output.surface_bindings {
            by_position
                .entry((binding.logical_name_id.as_str(), binding_position(binding)?))
                .or_default()
                .insert(binding.surface_binding_id);
        }
        for ((logical_name_id, position), bindings) in by_position {
            if bindings.len() > 1 {
                bail!(
                    "{context}: name {logical_name_id} opens {} surface bindings at one position {position:?}: {:?}",
                    bindings.len(),
                    bindings
                );
            }
        }
        Ok(())
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
            let Some(opened) = self.bindings.get(&exempt) else {
                bail!(
                    "{context}: binding closure at {:?} exempts binding {exempt}, which no batch opened",
                    closure_position(closure)
                );
            };
            if *opened > closure_position(closure) {
                bail!(
                    "{context}: binding closure at {:?} exempts binding {exempt} opened later at {opened:?}",
                    closure_position(closure)
                );
            }
        }
        Ok(())
    }
}

/// The writer fails a batch whose binding carries only half a position, a negative index, or no
/// position without raw-block provenance (`crates/interpret/src/write/identity.rs`), so a lane that
/// silently defaulted those to -1 would model a batch the database rejects.
fn binding_position(binding: &bigname_adapters::schema_v2::SurfaceBinding) -> Result<Position> {
    let index = |key| binding.provenance.get(key).and_then(Value::as_i64);
    match (
        index(seam::TRANSACTION_INDEX_KEY),
        index(seam::LOG_INDEX_KEY),
    ) {
        (Some(transaction_index), Some(log_index)) if transaction_index >= 0 && log_index >= 0 => {
            Ok((binding.block_number, transaction_index, log_index))
        }
        (None, None) if seam::is_raw_block_provenance(&binding.provenance) => {
            Ok((binding.block_number, -1, -1))
        }
        _ => bail!(
            "surface binding {} has a position the writer would reject: {}",
            binding.surface_binding_id,
            binding.provenance
        ),
    }
}

fn closure_position(closure: &bigname_adapters::schema_v2::BindingClosure) -> Position {
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
    /// The split replay, batch by batch, in commit order.
    pub batches: Vec<Replayed>,
    /// The same sequence interpreted as one batch — the shape a backfill runs.
    pub whole: Replayed,
    pub artifacts: BatchBoundaryArtifacts,
}

/// Runs the same sequence three ways — one fresh pass, one incremental pass, and one pass split
/// into resumed batches — and requires all three to derive identical state.
pub fn converge(context: &str, input: BatchInput, split: Vec<Range<usize>>) -> Result<Converged> {
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
    for (index, range) in split.into_iter().enumerate() {
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
                prior_events: Vec::new(),
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
    let artifacts = assert_converged(context, &fresh, &replayed)?;
    assert_lineage_integrity(context, "the whole-sequence pass", &fresh)?;
    assert_lineage_integrity(context, "the split replay", &replayed)?;
    Ok(Converged {
        batches: outputs,
        whole: Replayed {
            blocks: input
                .blocks
                .iter()
                .map(|block| block.block_number)
                .collect(),
            output: fresh,
        },
        artifacts,
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

pub fn split(len: usize, seed: u64) -> Vec<Range<usize>> {
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
