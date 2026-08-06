use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use bigname_adapters::schema_v2::{BatchOutput, NormalizedEvent, seam::INTERPRETER_STATE_KEY};
use serde_json::Value;
use uuid::Uuid;

/// Differences between a whole-sequence pass and a split replay that are artifacts of where the
/// batch boundaries fell rather than of the chain data, with the shape each one is pinned to.
/// Anything outside these shapes fails the lane.
#[derive(Default)]
pub struct BatchBoundaryArtifacts {
    /// Two batch-local reconciliation behaviours put `before_state` out of step, one per direction.
    /// Rewriting an *earlier* event's interpreter state key on the emitted row without rewriting the
    /// live state map leaves that event's value reachable under the pre-reconciliation key for the
    /// rest of the batch, but absent from the retained state a later batch restores — so the whole
    /// pass carries a value the split replay leaves empty. Re-threading a resolver scope blanks the
    /// `before_state` of that scope's first event *in the batch*, and a split gives each batch its
    /// own first event — so the split replay blanks values the whole pass carries. Neither leaves a
    /// trace on the row itself, so this cannot be pinned tighter than "one side is empty"; the
    /// counter is split by direction to keep both visible. Only the whole-pass direction is
    /// witnessed by the default corpus and pinned in `EXPECTED_ARTIFACTS` — the resolver-scope
    /// direction is a mechanism read out of the interpreter, not something the lane has reproduced,
    /// so it would arrive as a new key and fail the pin rather than pass unnoticed.
    pub carried_before_states: BTreeMap<&'static str, usize>,
    /// Reconciliation keeps a superseded identity row only while a retained row in the same batch
    /// output still references it. Where the referencing row lands in a later batch the earlier
    /// emission is dropped, so the two passes anchor the same identity to different blocks. Only
    /// `ANCHOR_REBASE_FAMILIES` may do this; any other family must anchor identically.
    pub rebased_anchors: BTreeMap<&'static str, usize>,
    /// Whether an event carries a logical name or a resource depends on identity state the batch
    /// happens to hold: a whole-sequence pass sees registrations from later blocks, and a restored
    /// pass re-derives name state from retained events. Either side may therefore attribute an
    /// event the other leaves unattributed. The two never disagree on *which* identity.
    pub rebased_attributions: usize,
}

impl BatchBoundaryArtifacts {
    /// One flat count per artifact class the run actually produced, for pinning. A class that never
    /// fired is absent rather than zero, so a lane whose interpreter no longer diverges compares
    /// equal to an empty pin table. Destructured so that adding an artifact class to the struct
    /// stops compiling here until someone decides how it is counted.
    pub fn counts(&self) -> BTreeMap<String, usize> {
        let Self {
            carried_before_states,
            rebased_anchors,
            rebased_attributions,
        } = self;
        let mut counts = BTreeMap::new();
        for (direction, count) in carried_before_states {
            counts.insert(format!("carried_before_states:{direction}"), *count);
        }
        for (family, count) in rebased_anchors {
            counts.insert(format!("rebased_anchors:{family}"), *count);
        }
        if *rebased_attributions > 0 {
            counts.insert("rebased_attributions".to_owned(), *rebased_attributions);
        }
        counts
    }

    pub fn absorb(&mut self, other: Self) {
        self.rebased_attributions += other.rebased_attributions;
        for (direction, count) in other.carried_before_states {
            *self.carried_before_states.entry(direction).or_default() += count;
        }
        for (family, count) in other.rebased_anchors {
            *self.rebased_anchors.entry(family).or_default() += count;
        }
    }
}

impl std::fmt::Display for BatchBoundaryArtifacts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "carried_before_states={:?} rebased_attributions={} rebased_anchors={:?}",
            self.carried_before_states, self.rebased_attributions, self.rebased_anchors
        )
    }
}

struct Row {
    key: String,
    body: String,
    anchor: String,
}

/// Which emission of a repeated key survives the upsert. Most families keep the first writer;
/// `label_preimages` overwrites whenever the incoming source priority is at least the stored one
/// (`crates/interpret/src/write/identity_names.rs`), and the adapter writes one priority, so the
/// last emission wins there.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Keeps {
    First,
    Last,
}

/// Reconciliation can drop a superseded resource row whose only in-batch reference moved to a later
/// batch. No other identity family is allowed to land on a different block between the two passes.
const ANCHOR_REBASE_FAMILIES: &[&str] = &["resources"];

pub fn assert_converged(
    context: &str,
    fresh: &BatchOutput,
    replayed: &BatchOutput,
) -> Result<BatchBoundaryArtifacts> {
    let mut known = BatchBoundaryArtifacts::default();
    assert_event_stream(context, fresh, replayed, &mut known)?;
    for (family, keeps, whole, split) in families(fresh, replayed) {
        assert_identity_family(context, family, keeps, &whole, &split, &mut known)?;
    }
    assert_lineage_attachment(context, fresh, replayed)?;
    Ok(known)
}

fn assert_event_stream(
    context: &str,
    fresh: &BatchOutput,
    replayed: &BatchOutput,
    known: &mut BatchBoundaryArtifacts,
) -> Result<()> {
    if fresh.normalized_events.len() != replayed.normalized_events.len() {
        let whole = identities(&fresh.normalized_events);
        let split = identities(&replayed.normalized_events);
        bail!(
            "{context}: split replay derived a different normalized-event stream: {}",
            set_difference(&whole, &split)
        );
    }
    let empty = serde_json::json!({});
    for (whole, split) in fresh
        .normalized_events
        .iter()
        .zip(replayed.normalized_events.iter())
    {
        if whole == split {
            continue;
        }
        let mut rebased = split.clone();
        if whole.before_state != split.before_state
            && (split.before_state == empty || whole.before_state == empty)
        {
            let direction = if split.before_state == empty {
                "only-the-whole-pass"
            } else {
                "only-the-split-replay"
            };
            rebased.before_state = whole.before_state.clone();
            *known.carried_before_states.entry(direction).or_default() += 1;
        }
        let mut reattributed = false;
        if whole.logical_name_id != split.logical_name_id
            && (whole.logical_name_id.is_none() || split.logical_name_id.is_none())
        {
            rebased.logical_name_id = whole.logical_name_id.clone();
            reattributed = true;
        }
        if whole.resource_id != split.resource_id
            && (whole.resource_id.is_none() || split.resource_id.is_none())
        {
            rebased.resource_id = whole.resource_id;
            reattributed = true;
        }
        if reattributed {
            // The interpreter state key embeds both identities, so it moves with the attribution.
            if let (Some(fields), Some(state_key)) = (
                rebased.raw_fact_ref.as_object_mut(),
                whole.raw_fact_ref.get(INTERPRETER_STATE_KEY),
            ) {
                fields.insert(INTERPRETER_STATE_KEY.to_owned(), state_key.clone());
            }
            known.rebased_attributions += 1;
        }
        if rebased != *whole {
            bail!(
                "{context}: event {} diverges between the whole-sequence pass and the split replay:{}",
                whole.event_identity,
                event_field_difference(whole, &rebased),
            );
        }
    }
    Ok(())
}

/// Identity rows are upserts keyed by their primary key, so batching changes how many times a row
/// is replayed but never which rows exist. Only the emission the upsert keeps is compared.
fn assert_identity_family(
    context: &str,
    family: &'static str,
    keeps: Keeps,
    whole: &[Row],
    split: &[Row],
    known: &mut BatchBoundaryArtifacts,
) -> Result<()> {
    let whole_first = kept_by_key(whole, keeps);
    let split_first = kept_by_key(split, keeps);
    let whole_keys = whole_first.keys().cloned().collect::<BTreeSet<_>>();
    let split_keys = split_first.keys().cloned().collect::<BTreeSet<_>>();
    if whole_keys != split_keys {
        bail!(
            "{context}: split replay wrote a different {family} identity set: {}",
            set_difference(&whole_keys, &split_keys)
        );
    }
    for (key, whole) in &whole_first {
        let split = &split_first[key];
        if whole.body != split.body {
            bail!(
                "{context}: {family} {key} diverges between the whole-sequence pass and the split replay:\
                 \n      whole={}\n   replayed={}",
                whole.body,
                split.body
            );
        }
        if whole.anchor != split.anchor {
            if !ANCHOR_REBASE_FAMILIES.contains(&family) {
                bail!(
                    "{context}: {family} {key} is anchored differently by the split replay, which \
                     this family cannot do:\n      whole={}\n   replayed={}",
                    whole.anchor,
                    split.anchor
                );
            }
            *known.rebased_anchors.entry(family).or_default() += 1;
        }
    }
    Ok(())
}

/// The resource upsert refuses any re-emission whose lineage is not null-safe-equal to the stored
/// one (`crates/interpret/src/write/identity.rs`, `token_lineage_id IS NOT DISTINCT FROM EXCLUDED`)
/// — that predicate is symmetric, so attaching a lineage to a resource first written without one
/// fails just as hard as changing it. `resources.token_lineage_id` is also UNIQUE. Both are write
/// failures, so a pass that violates either aborts the batch rather than diverging.
pub fn assert_lineage_integrity(context: &str, pass: &str, output: &BatchOutput) -> Result<()> {
    let mut lineage_of_resource: BTreeMap<Uuid, Option<Uuid>> = BTreeMap::new();
    let mut owner_of_lineage: BTreeMap<Uuid, Uuid> = BTreeMap::new();
    for resource in &output.resources {
        let seen = lineage_of_resource
            .entry(resource.resource_id)
            .or_insert(resource.token_lineage_id);
        if *seen != resource.token_lineage_id {
            bail!(
                "{context}: {pass} re-emits resource {} with a different token lineage: {seen:?} then {:?}",
                resource.resource_id,
                resource.token_lineage_id
            );
        }
        if let Some(lineage) = resource.token_lineage_id
            && let Some(existing) = owner_of_lineage.insert(lineage, resource.resource_id)
            && existing != resource.resource_id
        {
            bail!(
                "{context}: {pass} attaches token lineage {lineage} to resource {existing} and to {}",
                resource.resource_id
            );
        }
    }
    Ok(())
}

/// The set of lineage attachments a sequence produces must not depend on batching.
fn assert_lineage_attachment(
    context: &str,
    fresh: &BatchOutput,
    replayed: &BatchOutput,
) -> Result<()> {
    let attachments = |output: &BatchOutput| {
        output
            .resources
            .iter()
            .filter_map(|resource| {
                resource
                    .token_lineage_id
                    .map(|lineage| format!("{} -> {lineage}", resource.resource_id))
            })
            .collect::<BTreeSet<_>>()
    };
    let whole = attachments(fresh);
    let split = attachments(replayed);
    if whole != split {
        bail!(
            "{context}: split replay attached different token lineages: {}",
            set_difference(&whole, &split)
        );
    }
    Ok(())
}

fn event_field_difference(whole: &NormalizedEvent, split: &NormalizedEvent) -> String {
    let fields: [(&str, String, String); 18] = [
        (
            "namespace",
            whole.namespace.clone(),
            split.namespace.clone(),
        ),
        (
            "source_family",
            whole.source_family.clone(),
            split.source_family.clone(),
        ),
        (
            "manifest_version",
            whole.manifest_version.to_string(),
            split.manifest_version.to_string(),
        ),
        ("chain_id", whole.chain_id.clone(), split.chain_id.clone()),
        (
            "block_number",
            format!("{:?}", whole.block_number),
            format!("{:?}", split.block_number),
        ),
        (
            "block_hash",
            format!("{:?}", whole.block_hash),
            format!("{:?}", split.block_hash),
        ),
        (
            "transaction_hash",
            format!("{:?}", whole.transaction_hash),
            format!("{:?}", split.transaction_hash),
        ),
        (
            "transaction_index",
            format!("{:?}", whole.transaction_index),
            format!("{:?}", split.transaction_index),
        ),
        (
            "log_index",
            format!("{:?}", whole.log_index),
            format!("{:?}", split.log_index),
        ),
        (
            "logical_name_id",
            format!("{:?}", whole.logical_name_id),
            format!("{:?}", split.logical_name_id),
        ),
        (
            "resource_id",
            format!("{:?}", whole.resource_id),
            format!("{:?}", split.resource_id),
        ),
        (
            "event_kind",
            whole.event_kind.clone(),
            split.event_kind.clone(),
        ),
        (
            "derivation_kind",
            whole.derivation_kind.clone(),
            split.derivation_kind.clone(),
        ),
        (
            "canonicality_state",
            whole.canonicality_state.clone(),
            split.canonicality_state.clone(),
        ),
        (
            "source_manifest_id",
            format!("{:?}", whole.source_manifest_id),
            format!("{:?}", split.source_manifest_id),
        ),
        (
            "raw_fact_ref",
            whole.raw_fact_ref.to_string(),
            split.raw_fact_ref.to_string(),
        ),
        (
            "before_state",
            whole.before_state.to_string(),
            split.before_state.to_string(),
        ),
        (
            "after_state",
            whole.after_state.to_string(),
            split.after_state.to_string(),
        ),
    ];
    fields
        .iter()
        .filter(|(_, whole, split)| whole != split)
        .map(|(field, whole, split)| {
            format!("\n  {field}\n      whole={whole}\n   replayed={split}")
        })
        .collect::<Vec<_>>()
        .join("")
}

fn kept_by_key(rows: &[Row], keeps: Keeps) -> BTreeMap<String, &Row> {
    let mut kept = BTreeMap::new();
    for row in rows {
        match keeps {
            Keeps::First => {
                kept.entry(row.key.clone()).or_insert(row);
            }
            Keeps::Last => {
                kept.insert(row.key.clone(), row);
            }
        }
    }
    kept
}

fn identities(events: &[NormalizedEvent]) -> BTreeSet<String> {
    events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect()
}

fn set_difference(whole: &BTreeSet<String>, split: &BTreeSet<String>) -> String {
    let sample = |values: &BTreeSet<String>, other: &BTreeSet<String>| {
        values
            .difference(other)
            .take(2)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n    ")
    };
    format!(
        "\n  only in the whole pass ({}): {}\n  only in the split replay ({}): {}",
        whole.difference(split).count(),
        sample(whole, split),
        split.difference(whole).count(),
        sample(split, whole),
    )
}

fn anchor(block_hash: &str, block_number: i64, provenance: &Value) -> String {
    format!("{block_hash}:{block_number}:{provenance}")
}

fn families(
    fresh: &BatchOutput,
    replayed: &BatchOutput,
) -> Vec<(&'static str, Keeps, Vec<Row>, Vec<Row>)> {
    vec![
        (
            "label_preimages",
            Keeps::Last,
            label_preimages(fresh),
            label_preimages(replayed),
        ),
        (
            "name_surfaces",
            Keeps::First,
            name_surfaces(fresh),
            name_surfaces(replayed),
        ),
        (
            "token_lineages",
            Keeps::First,
            token_lineages(fresh),
            token_lineages(replayed),
        ),
        (
            "resources",
            Keeps::First,
            resources(fresh),
            resources(replayed),
        ),
        (
            "surface_bindings",
            Keeps::First,
            surface_bindings(fresh),
            surface_bindings(replayed),
        ),
        (
            "binding_closures",
            Keeps::First,
            binding_closures(fresh),
            binding_closures(replayed),
        ),
        (
            "contract_instances",
            Keeps::First,
            contract_instances(fresh),
            contract_instances(replayed),
        ),
        (
            "contract_addresses",
            Keeps::First,
            contract_addresses(fresh),
            contract_addresses(replayed),
        ),
        (
            "discovery_edges",
            Keeps::First,
            discovery_edges(fresh),
            discovery_edges(replayed),
        ),
        (
            "discovery_edge_closures",
            Keeps::First,
            discovery_edge_closures(fresh),
            discovery_edge_closures(replayed),
        ),
    ]
}

fn label_preimages(output: &BatchOutput) -> Vec<Row> {
    output
        .label_preimages
        .iter()
        .map(|row| Row {
            key: row.labelhash.clone(),
            body: format!(
                "{:?}:{:?}:{}:{}:{}:{}:{:?}",
                row.raw_label,
                row.decoded_label,
                row.normalizer_version,
                row.normalized_under_version,
                row.source_kind,
                row.source_priority,
                row.normalization_error
            ),
            anchor: row.provenance.to_string(),
        })
        .collect()
}

fn name_surfaces(output: &BatchOutput) -> Vec<Row> {
    output
        .name_surfaces
        .iter()
        .map(|row| Row {
            key: format!("{}:{}", row.chain_id, row.logical_name_id),
            body: format!(
                "{}:{}:{:?}:{:?}:{:?}:{}:{}:{}:{}:{:?}:{:?}:{}",
                row.namespace,
                row.raw_name,
                row.raw_labels,
                row.labelhashes,
                row.dns_encoded_name,
                row.namehash,
                row.normalizer_version,
                row.visibility_state,
                row.normalization_errors,
                row.deactivation_reason,
                row.deactivated_at,
                row.canonicality_state
            ),
            anchor: anchor(&row.block_hash, row.block_number, &row.provenance),
        })
        .collect()
}

fn token_lineages(output: &BatchOutput) -> Vec<Row> {
    output
        .token_lineages
        .iter()
        .map(|row| Row {
            key: format!("{}:{}", row.chain_id, row.token_lineage_id),
            body: row.canonicality_state.clone(),
            anchor: anchor(&row.block_hash, row.block_number, &row.provenance),
        })
        .collect()
}

fn resources(output: &BatchOutput) -> Vec<Row> {
    output
        .resources
        .iter()
        .map(|row| Row {
            key: format!("{}:{}", row.chain_id, row.resource_id),
            body: format!("{:?}:{}", row.token_lineage_id, row.canonicality_state),
            anchor: anchor(&row.block_hash, row.block_number, &row.provenance),
        })
        .collect()
}

fn surface_bindings(output: &BatchOutput) -> Vec<Row> {
    output
        .surface_bindings
        .iter()
        .map(|row| Row {
            key: row.surface_binding_id.to_string(),
            body: format!(
                "{}:{}:{}:{}:{}",
                row.logical_name_id,
                row.resource_id,
                row.binding_kind,
                row.active_from,
                row.canonicality_state
            ),
            anchor: anchor(&row.block_hash, row.block_number, &row.provenance),
        })
        .collect()
}

fn binding_closures(output: &BatchOutput) -> Vec<Row> {
    output
        .binding_closures
        .iter()
        .map(|row| Row {
            // The exempted binding is part of the key, not just the body: two closures for one name
            // at one position differ only by which binding they spare, and keying without it would
            // compare one of them against the other and drop the rest.
            key: format!(
                "{}:{}:{}:{}:{:?}",
                row.logical_name_id,
                row.block_number,
                row.transaction_index,
                row.log_index,
                row.except_surface_binding_id
            ),
            body: row.active_to.to_string(),
            anchor: String::new(),
        })
        .collect()
}

fn contract_instances(output: &BatchOutput) -> Vec<Row> {
    output
        .contract_instances
        .iter()
        .map(|row| Row {
            key: format!("{}:{}", row.chain_id, row.contract_instance_id),
            body: row.contract_kind.clone(),
            anchor: row.provenance.to_string(),
        })
        .collect()
}

fn contract_addresses(output: &BatchOutput) -> Vec<Row> {
    output
        .contract_addresses
        .iter()
        .map(|row| Row {
            key: format!(
                "{}:{}:{}",
                row.chain_id, row.contract_instance_id, row.address
            ),
            body: row.source_manifest_id.to_string(),
            anchor: anchor(
                &row.active_from_block_hash,
                row.active_from_block_number,
                &row.provenance,
            ),
        })
        .collect()
}

fn discovery_edges(output: &BatchOutput) -> Vec<Row> {
    output
        .discovery_edges
        .iter()
        .map(|row| Row {
            key: format!(
                "{}:{}:{}:{}",
                row.chain_id, row.edge_kind, row.from_contract_instance_id, row.observation_key
            ),
            body: format!(
                "{}:{}:{}:{}:{}",
                row.to_contract_instance_id,
                row.discovery_source,
                row.admission_basis,
                row.source_manifest_id,
                row.canonicality_state
            ),
            anchor: anchor(
                &row.active_from_block_hash,
                row.active_from_block_number,
                &row.provenance,
            ),
        })
        .collect()
}

fn discovery_edge_closures(output: &BatchOutput) -> Vec<Row> {
    output
        .discovery_edge_closures
        .iter()
        .map(|row| Row {
            key: format!(
                "{}:{}:{}:{}:{}:{}",
                row.chain_id,
                row.edge_kind,
                row.from_contract_instance_id,
                row.observation_key,
                row.active_to_block_number,
                row.log_index
            ),
            body: format!(
                "{:?}:{}:{}",
                row.except_to_contract_instance_id, row.active_to_block_hash, row.transaction_index
            ),
            anchor: String::new(),
        })
        .collect()
}
