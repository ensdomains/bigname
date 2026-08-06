use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use bigname_adapters::schema_v2::{BatchOutput, NormalizedEvent, seam::INTERPRETER_STATE_KEY};
use serde_json::Value;

/// Divergences between a whole-sequence pass and a split replay that this lane accepts as known,
/// with the shape each one is pinned to. Anything outside these shapes fails the lane.
#[derive(Default)]
pub struct KnownDivergence {
    /// Same-transaction reconciliation rewrites an emitted event's interpreter state key without
    /// rewriting the live state map. A value written under the pre-reconciliation key stays visible
    /// to later events in the same batch but is absent from the retained state a later batch
    /// restores. Either pass can therefore carry a `before_state` the other leaves empty.
    pub carried_before_states: usize,
    /// Reconciliation keeps a superseded identity row only while a retained row in the same batch
    /// output still references it, and `resources` upserts keep the first writer's anchor. Where
    /// the referencing row lands in a later batch, the two passes anchor the same identity to
    /// different blocks.
    pub rebased_anchors: usize,
    /// Whether an event carries a logical name or a resource depends on identity state the batch
    /// happens to hold: a whole-sequence pass sees registrations from later blocks, and a restored
    /// pass re-derives name state from retained events. Either side may therefore attribute an
    /// event the other leaves unattributed. The two never disagree on *which* identity.
    pub rebased_attributions: usize,
}

struct Row {
    key: String,
    body: String,
    anchor: String,
}

pub fn assert_converged(
    context: &str,
    fresh: &BatchOutput,
    replayed: &BatchOutput,
) -> Result<KnownDivergence> {
    let mut known = KnownDivergence::default();
    assert_event_stream(context, fresh, replayed, &mut known)?;
    for (family, whole, split) in families(fresh, replayed) {
        assert_identity_family(context, family, &whole, &split, &mut known)?;
    }
    assert_lineage_attachment(context, fresh, replayed)?;
    Ok(known)
}

fn assert_event_stream(
    context: &str,
    fresh: &BatchOutput,
    replayed: &BatchOutput,
    known: &mut KnownDivergence,
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
            rebased.before_state = whole.before_state.clone();
            known.carried_before_states += 1;
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

/// Identity rows are upserts. Every identity a run writes must exist in the other run, and the
/// first writer decides the persisted anchor, so only the first emission per key is compared.
fn assert_identity_family(
    context: &str,
    family: &str,
    whole: &[Row],
    split: &[Row],
    known: &mut KnownDivergence,
) -> Result<()> {
    let whole_first = first_by_key(whole);
    let split_first = first_by_key(split);
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
            known.rebased_anchors += 1;
        }
    }
    Ok(())
}

/// `resources.token_lineage_id` is filled by the first non-null writer, so the set of attachments a
/// sequence produces must not depend on batching even though the row order does.
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
    let fields: [(&str, String, String); 9] = [
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

fn first_by_key(rows: &[Row]) -> BTreeMap<String, &Row> {
    let mut first = BTreeMap::new();
    for row in rows {
        first.entry(row.key.clone()).or_insert(row);
    }
    first
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
) -> Vec<(&'static str, Vec<Row>, Vec<Row>)> {
    vec![
        (
            "label_preimages",
            label_preimages(fresh),
            label_preimages(replayed),
        ),
        (
            "name_surfaces",
            name_surfaces(fresh),
            name_surfaces(replayed),
        ),
        (
            "token_lineages",
            token_lineages(fresh),
            token_lineages(replayed),
        ),
        ("resources", resources(fresh), resources(replayed)),
        (
            "surface_bindings",
            surface_bindings(fresh),
            surface_bindings(replayed),
        ),
        (
            "binding_closures",
            binding_closures(fresh),
            binding_closures(replayed),
        ),
        (
            "contract_instances",
            contract_instances(fresh),
            contract_instances(replayed),
        ),
        (
            "contract_addresses",
            contract_addresses(fresh),
            contract_addresses(replayed),
        ),
        (
            "discovery_edges",
            discovery_edges(fresh),
            discovery_edges(replayed),
        ),
        (
            "discovery_edge_closures",
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
            key: format!("{}:{}", row.labelhash, row.source_kind),
            body: format!(
                "{:?}:{:?}:{}:{}:{:?}",
                row.raw_label,
                row.decoded_label,
                row.normalizer_version,
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
                "{}:{}:{:?}:{}:{}:{}:{:?}:{}",
                row.namespace,
                row.raw_name,
                row.labelhashes,
                row.namehash,
                row.normalizer_version,
                row.visibility_state,
                row.deactivation_reason,
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
            body: row.canonicality_state.clone(),
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
            key: format!(
                "{}:{}:{}:{}",
                row.logical_name_id, row.block_number, row.transaction_index, row.log_index
            ),
            body: format!("{:?}:{}", row.except_surface_binding_id, row.active_to),
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
