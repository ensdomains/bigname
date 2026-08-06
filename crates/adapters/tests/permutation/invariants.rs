use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, interpret_schema_v2_batch,
    interpret_schema_v2_batch_incremental, seam,
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
    manifests: BTreeSet<i64>,
    resources: BTreeSet<(String, Uuid)>,
    lineages: BTreeSet<(String, Uuid)>,
    surfaces: BTreeSet<(String, String)>,
    bindings: BTreeMap<Uuid, Position>,
    instances: BTreeSet<(String, Uuid)>,
    positions: BTreeMap<String, Position>,
}

impl IdentityReferences {
    pub fn new(chain_id: &str, declared_instances: &[Uuid]) -> Self {
        Self::with_manifests(chain_id, declared_instances, &[])
    }

    /// `source_manifest_id` is a foreign key on normalized events, contract addresses and discovery
    /// edges. A stale or unknown id converges fine between the two passes and fails only at the
    /// writer, so the batch's own manifest ids are carried here and every reference checked.
    pub fn with_manifests(chain_id: &str, declared_instances: &[Uuid], manifests: &[i64]) -> Self {
        Self {
            chain_id: chain_id.to_owned(),
            manifests: manifests.iter().copied().collect(),
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
        // An exemption naming a binding no batch opened is `check_canonicality`'s, which can also
        // report where the closure sits; checking it here too would only shadow that message.
        for closure in &output.binding_closures {
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
        if !self.manifests.is_empty() {
            let mut unknown = BTreeSet::new();
            for id in output
                .normalized_events
                .iter()
                .filter_map(|row| row.source_manifest_id)
                .chain(
                    output
                        .contract_addresses
                        .iter()
                        .map(|row| row.source_manifest_id),
                )
                .chain(
                    output
                        .discovery_edges
                        .iter()
                        .map(|row| row.source_manifest_id),
                )
            {
                if !self.manifests.contains(&id) {
                    unknown.insert(id);
                }
            }
            if !unknown.is_empty() {
                bail!(
                    "{context}: rows reference manifest ids the batch does not admit: {unknown:?}"
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

/// The convergence check keeps one row per key per family, modelling the upsert as "one emission
/// wins and the rest are harmless replays". That is only half the writer: each upsert carries a
/// `WHERE` guard, and a re-emission that disagrees with the stored row on a guarded column matches
/// no row, returns nothing, and fails the batch (`crates/interpret/src/write/identity.rs`,
/// `identity_names.rs`). Repeats are the norm rather than an edge case — the adapter emits a name
/// surface per interpreted name per log — so without this the most common shape in the output is
/// unchecked, and a sequence that agrees on the surviving row while disagreeing on a dropped one
/// looks convergent here and aborts in production.
///
/// The guard compares against the row already in the table, not against the batch, so this holds
/// over any set of rows the writer would apply in sequence — one batch, or a whole split replay
/// concatenated. Comparing against the first emission is right because the guarded columns are
/// frozen at insert: `name_surfaces`, `label_preimages`, and `normalized_events` never rewrite one,
/// and the two that can — `token_lineages` rewriting its anchor, `surface_bindings` rewriting
/// `active_from` — only do so when the stored row is orphaned, which cannot occur here (below).
/// `deactivated_at` is deliberately not compared: it is absent from the writer's guard, which is
/// the divergence tracked by issue #336.
///
/// Only `name_surfaces` and `label_preimages` actually repeat a key in this corpus. The other three
/// are covered on the same rule rather than assumed unique — `normalized_events` in particular
/// keys on an `event_identity` the adapter builds by hand, so a repeat there is reachable by
/// construction even though nothing generates one today.
pub fn assert_upsert_guards_agree(context: &str, output: &BatchOutput) -> Result<()> {
    // One report per key: a key emitted N times conflicting is one rejected batch, not N.
    let mut conflicts = BTreeMap::new();
    let mut guarded = BTreeMap::new();
    let mut check = |family: &'static str, key: String, columns: String| match guarded
        .entry((family, key.clone()))
    {
        std::collections::btree_map::Entry::Vacant(slot) => {
            slot.insert(columns);
        }
        std::collections::btree_map::Entry::Occupied(slot) => {
            if slot.get() != &columns {
                conflicts.entry((family, key)).or_insert_with(|| {
                    format!(
                        "{family} is emitted twice with different guarded columns\n      \
                             stored={}\n    incoming={columns}",
                        slot.get()
                    )
                });
            }
        }
    };
    for row in &output.name_surfaces {
        check(
            "name_surfaces",
            format!("{}:{}", row.chain_id, row.logical_name_id),
            format!(
                "{}:{}:{:?}:{:?}:{:?}:{}:{}:{}:{}:{:?}",
                row.namespace,
                row.raw_name,
                row.raw_labels,
                row.labelhashes,
                row.dns_encoded_name,
                row.namehash,
                row.normalizer_version,
                row.visibility_state,
                row.normalization_errors,
                row.deactivation_reason
            ),
        );
    }
    for row in &output.label_preimages {
        check(
            "label_preimages",
            row.labelhash.clone(),
            format!(
                "{:?}:{:?}:{}:{}:{:?}",
                row.raw_label,
                row.decoded_label,
                row.normalizer_version,
                row.normalized_under_version,
                row.normalization_error
            ),
        );
    }
    // The lineage guard also passes when the *stored* row is orphaned. That escape is not modelled
    // here because nothing in the corpus emits a non-canonical row — `check_canonicality` rejects
    // one outright at the lane's own call sites — so modelling it would be an untested branch. On
    // the concatenated call below, which runs before that check, an orphaned row would be reported
    // here as a conflict the writer would in fact accept.
    for row in &output.token_lineages {
        check(
            "token_lineages",
            format!("{}:{}", row.chain_id, row.token_lineage_id),
            format!("{}:{}:{}", row.block_hash, row.block_number, row.provenance),
        );
    }
    // The strictest guard in the schema: every column but `canonicality_state` must match, and the
    // conflict target is `event_identity`, which the adapter builds by hand rather than from a
    // sequence — so two emissions that build the same identity from different data fail the batch.
    for row in &output.normalized_events {
        check(
            "normalized_events",
            row.event_identity.clone(),
            format!(
                "{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{}:{}",
                row.namespace,
                row.logical_name_id,
                row.resource_id,
                row.event_kind,
                row.source_family,
                row.manifest_version,
                row.source_manifest_id,
                row.chain_id,
                row.block_number,
                row.block_hash,
                row.transaction_hash,
                row.transaction_index,
                row.log_index,
                row.raw_fact_ref,
                row.derivation_kind,
                row.before_state,
                row.after_state,
            ),
        );
    }
    // `active_from` is guarded too. The writer substitutes a start it looks up from the binding's
    // predecessor, so the emitted value is not literally what the guard compares — but two
    // emissions of one binding id carrying *different* starts cannot both survive that lookup, and
    // convergence keeps only one of them, so compare it here rather than drop it.
    for row in &output.surface_bindings {
        check(
            "surface_bindings",
            row.surface_binding_id.to_string(),
            format!(
                "{}:{}:{}:{}:{}",
                row.logical_name_id,
                row.resource_id,
                row.binding_kind,
                row.chain_id,
                row.active_from
            ),
        );
    }
    if !conflicts.is_empty() {
        bail!(
            "{context}: the writer would reject this batch — a repeated identity row disagrees \
             with the emission the upsert keeps:\n  {}",
            conflicts
                .into_iter()
                .map(|((_, key), report)| format!("{key} {report}"))
                .collect::<Vec<_>>()
                .join("\n  ")
        );
    }
    Ok(())
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
    // Production persists a discovery edge and its contract address, then loads them as admissions
    // for every later batch (`crates/interpret/src/load.rs`). Replaying each batch from the original
    // input instead would drop any later-batch log from a resolver or registry an earlier batch
    // discovered — the split would silently interpret less than the whole pass, and the convergence
    // comparison would read that as agreement.
    let mut admitted = input.admissions.clone();
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
            admissions: admitted.clone(),
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
        absorb_discovered_admissions(&mut admitted, &resumed_output);
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
    assert_upsert_guards_agree(
        &format!("{context}: split replay across batches"),
        &replayed,
    )?;
    // The guard compares against whatever is already in the table, so a row first written by batch 0
    // still guards batch 2's re-emission. Checking each batch alone would miss exactly that.

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

/// Turns the contract addresses a batch admitted into the admissions the next batch starts from,
/// carrying the discovery edge that justified each one where there is one. Mirrors the join
/// `crates/interpret/src/load.rs` runs against the persisted rows.
fn absorb_discovered_admissions(admitted: &mut Vec<AddressAdmissionInput>, from: &BatchOutput) {
    let known = admitted
        .iter()
        .map(|entry| (entry.address.clone(), entry.contract_instance_id))
        .collect::<BTreeSet<_>>();
    for address in &from.contract_addresses {
        if known.contains(&(address.address.clone(), address.contract_instance_id)) {
            continue;
        }
        let edge = from.discovery_edges.iter().find(|edge| {
            edge.to_contract_instance_id == address.contract_instance_id
                && edge.chain_id == address.chain_id
        });
        admitted.push(AddressAdmissionInput {
            address: address.address.clone(),
            contract_instance_id: address.contract_instance_id,
            source_manifest_id: Some(address.source_manifest_id),
            role: None,
            discovery_edge_kind: edge.map(|edge| edge.edge_kind.clone()),
            discovery_from_contract_instance_id: edge.map(|edge| edge.from_contract_instance_id),
            discovery_observation_key: edge.map(|edge| edge.observation_key.clone()),
            active_from_block: Some(address.active_from_block_number),
            active_to_block: None,
        });
    }
}

/// Destructured so that a row family added to `BatchOutput` stops compiling here. Missing one is
/// silent and total: the split replay would carry none of that family while the whole pass carried
/// all of it, and nothing downstream — convergence, foreign keys, upsert guards — would compare
/// them, because each of those enumerates the families too.
fn absorb_rows(into: &mut BatchOutput, from: BatchOutput) {
    let BatchOutput {
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
    } = from;
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
