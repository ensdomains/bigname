use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::hex;
use anyhow::bail;
use bigname_domain::normalization::ENS_NORMALIZER_VERSION;
use serde_json::{Value, json};

mod boundary;
pub(super) use boundary::materialize_v2_boundary;

use super::{
    catalog::Selected,
    common::{
        decoded_label, dns_encode, event_time, hash_hex, normalization_flag, provenance,
        stable_uuid,
    },
    model::{
        BatchOutput, BindingClosure, LabelPreimage, NameSurface, RawLogInput, Resource,
        SurfaceBinding, TokenLineage,
    },
    normalized::preimage_event,
    protocol::Interpreted,
    state::State,
};

pub(super) fn compact_reserved_label_preimages(output: &mut BatchOutput) -> anyhow::Result<()> {
    const RESERVED_LABEL_SOURCE_KIND: &str = "LabelReserved_label";

    // Every PreimageObserved event remains in normalized output. This vector is submitted to
    // persistent identity storage, so repeated suffixes from reservations need only submit the
    // same row once per batch. Keep the row that the sequential writer would leave as winner.
    let observations = std::mem::take(&mut output.label_preimages);
    let mut first_positions = BTreeMap::<String, usize>::new();
    let mut reserved_winners = BTreeMap::<String, usize>::new();
    for (position, candidate) in observations.iter().enumerate() {
        let Some(&first_position) = first_positions.get(&candidate.labelhash) else {
            first_positions.insert(candidate.labelhash.clone(), position);
            if candidate.source_kind == RESERVED_LABEL_SOURCE_KIND {
                reserved_winners.insert(candidate.labelhash.clone(), position);
            }
            continue;
        };
        let existing = &observations[first_position];
        if existing.raw_label != candidate.raw_label
            || existing.decoded_label != candidate.decoded_label
            || existing.normalizer_version != candidate.normalizer_version
            || existing.normalized_under_version != candidate.normalized_under_version
            || existing.normalization_error != candidate.normalization_error
        {
            bail!(
                "label hash {} has inconsistent preimage observations in one adapter batch",
                candidate.labelhash
            );
        }
        if candidate.source_kind == RESERVED_LABEL_SOURCE_KIND {
            match reserved_winners.get_mut(&candidate.labelhash) {
                Some(winner_position)
                    if candidate.source_priority
                        >= observations[*winner_position].source_priority =>
                {
                    *winner_position = position;
                }
                Some(_) => {}
                None => {
                    reserved_winners.insert(candidate.labelhash.clone(), position);
                }
            }
        }
    }
    output.label_preimages = observations
        .into_iter()
        .enumerate()
        .filter_map(|(position, observation)| {
            (observation.source_kind != RESERVED_LABEL_SOURCE_KIND
                || reserved_winners.get(&observation.labelhash) == Some(&position))
            .then_some(observation)
        })
        .collect();
    Ok(())
}

pub(super) fn materialize(
    selected: &Selected,
    raw: &RawLogInput,
    interpreted: &Interpreted,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let raw_provenance = provenance(raw, &selected.event.name, selected.source.manifest_id);
    let transition_time = event_time(raw);
    let v1_surface = selected.source.source_family.starts_with("ens_v1_")
        || selected.source.source_family.starts_with("basenames_");
    let mut shadow_names = interpreted
        .names
        .iter()
        .filter(|name| {
            name.labels
                .iter()
                .any(|label| !normalization_flag(Some(label)).normalized)
        })
        .map(|name| format!("{}:{}", selected.source.namespace, name.namehash))
        .collect::<BTreeSet<_>>();
    shadow_names.extend(
        interpreted
            .shadow_names
            .iter()
            .map(|name| format!("{}:{}", selected.source.namespace, name.namehash)),
    );
    let mut labels = BTreeMap::new();
    for label in &interpreted.labels {
        if label.raw_label.is_empty() {
            continue;
        }
        let decoded_label = decoded_label(&label.raw_label);
        let flag = normalization_flag(decoded_label.as_deref());
        let labelhash = hash_hex(&label.raw_label);
        labels.entry(labelhash.clone()).or_insert(LabelPreimage {
            labelhash,
            raw_label: label.raw_label.clone(),
            decoded_label,
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            normalized_under_version: flag.normalized,
            normalization_error: flag.error,
            source_kind: label.source_kind.clone(),
            source_priority: 100,
            provenance: raw_provenance.clone(),
        });
    }
    for name in &interpreted.names {
        for raw_label in &name.labels {
            if raw_label.is_empty() {
                continue;
            }
            let flag = normalization_flag(Some(raw_label));
            let labelhash = hash_hex(raw_label.as_bytes());
            labels.entry(labelhash.clone()).or_insert(LabelPreimage {
                labelhash,
                raw_label: raw_label.as_bytes().to_vec(),
                decoded_label: Some(raw_label.clone()),
                normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
                normalized_under_version: flag.normalized,
                normalization_error: flag.error,
                source_kind: name.source_kind.clone(),
                source_priority: 100,
                provenance: raw_provenance.clone(),
            });
        }
    }
    for name in &interpreted.shadow_names {
        for raw_label in &name.raw_labels {
            if raw_label.is_empty() {
                continue;
            }
            let decoded_label = decoded_label(raw_label);
            let flag = normalization_flag(decoded_label.as_deref());
            let labelhash = hash_hex(raw_label);
            labels.entry(labelhash.clone()).or_insert(LabelPreimage {
                labelhash,
                raw_label: raw_label.clone(),
                decoded_label,
                normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
                normalized_under_version: flag.normalized,
                normalization_error: flag.error,
                source_kind: name.source_kind.clone(),
                source_priority: 100,
                provenance: raw_provenance.clone(),
            });
        }
    }
    output.label_preimages.extend(labels.into_values());

    for closure in &interpreted.binding_closures {
        if shadow_names.contains(&closure.logical_name_id) {
            continue;
        }
        output.binding_closures.push(BindingClosure {
            logical_name_id: closure.logical_name_id.clone(),
            authority_arm: closure.authority_arm.clone(),
            chain_id: raw.chain_id.clone(),
            except_surface_binding_id: None,
            active_to: transition_time,
            block_number: raw.block_number,
            transaction_index: raw.transaction_index,
            log_index: raw.log_index,
        });
    }

    for resource in &interpreted.resources {
        if let Some(token_lineage_id) = resource.token_lineage_id
            && state.materialize_token_lineage(token_lineage_id)
        {
            output.token_lineages.push(TokenLineage {
                token_lineage_id,
                chain_id: raw.chain_id.clone(),
                block_hash: raw.block_hash.clone(),
                block_number: raw.block_number,
                provenance: raw_provenance.clone(),
                canonicality_state: raw.canonicality_state.clone(),
            });
        }
        output.resources.push(Resource {
            resource_id: resource.resource_id,
            token_lineage_id: resource.token_lineage_id,
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            provenance: raw_provenance.clone(),
            canonicality_state: raw.canonicality_state.clone(),
        });
    }

    for binding in &interpreted.bindings {
        if shadow_names.contains(&binding.logical_name_id) {
            continue;
        }
        let surface_binding_id = binding
            .surface_binding_id
            .unwrap_or_else(|| binding_id(&binding.logical_name_id, binding.resource_id, raw));
        output.binding_closures.push(BindingClosure {
            logical_name_id: binding.logical_name_id.clone(),
            authority_arm: binding.authority_arm.clone(),
            chain_id: raw.chain_id.clone(),
            except_surface_binding_id: Some(surface_binding_id),
            active_to: transition_time,
            block_number: raw.block_number,
            transaction_index: raw.transaction_index,
            log_index: raw.log_index,
        });
        output.surface_bindings.push(SurfaceBinding {
            surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: binding.binding_kind.clone(),
            authority_arm: binding.authority_arm.clone(),
            active_from: binding.active_from.unwrap_or(transition_time),
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            provenance: raw_provenance.clone(),
            canonicality_state: raw.canonicality_state.clone(),
        });
    }

    let mut represented = BTreeSet::<Vec<u8>>::new();
    for name in &interpreted.names {
        let logical_name_id = format!("{}:{}", selected.source.namespace, name.namehash);
        if v1_surface {
            state.observe_v1_surface(&selected.source.namespace, &name.namehash);
        }
        let flags = name
            .labels
            .iter()
            .map(|label| (label, normalization_flag(Some(label))))
            .collect::<Vec<_>>();
        let errors = flags
            .iter()
            .filter_map(|(label, flag)| {
                flag.error
                    .as_ref()
                    .map(|error| json!({ "raw_label": label, "error": error }))
            })
            .collect::<Vec<_>>();
        let active = errors.is_empty();
        let labelhashes = name
            .labels
            .iter()
            .map(|label| hash_hex(label.as_bytes()))
            .collect::<Vec<_>>();
        represented.extend(name.labels.iter().map(|label| label.as_bytes().to_vec()));
        output.name_surfaces.push(NameSurface {
            logical_name_id: logical_name_id.clone(),
            namespace: selected.source.namespace.clone(),
            raw_name: name.labels.join("."),
            raw_labels: name.labels.clone(),
            dns_encoded_name: dns_encode(&name.labels)?,
            namehash: name.namehash.clone(),
            labelhashes,
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            visibility_state: if active { "active" } else { "shadow" }.to_owned(),
            normalization_errors: Value::Array(errors),
            deactivation_reason: (!active).then(|| "normalization_gate".to_owned()),
            deactivated_at: (!active).then_some(raw.block_timestamp),
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            provenance: raw_provenance.clone(),
            canonicality_state: raw.canonicality_state.clone(),
        });
        if let Some(resource_id) = name.resource_id {
            output.resources.push(Resource {
                resource_id,
                token_lineage_id: name.token_lineage_id,
                chain_id: raw.chain_id.clone(),
                block_hash: raw.block_hash.clone(),
                block_number: raw.block_number,
                provenance: raw_provenance.clone(),
                canonicality_state: raw.canonicality_state.clone(),
            });
            if let Some(token_lineage_id) = name.token_lineage_id
                && state.materialize_token_lineage(token_lineage_id)
            {
                output.token_lineages.push(TokenLineage {
                    token_lineage_id,
                    chain_id: raw.chain_id.clone(),
                    block_hash: raw.block_hash.clone(),
                    block_number: raw.block_number,
                    provenance: raw_provenance.clone(),
                    canonicality_state: raw.canonicality_state.clone(),
                });
            }
            if active && name.bind {
                let surface_binding_id = name
                    .surface_binding_id
                    .unwrap_or_else(|| binding_id(&logical_name_id, resource_id, raw));
                output.binding_closures.push(BindingClosure {
                    logical_name_id: logical_name_id.clone(),
                    authority_arm: name.authority_arm.clone(),
                    chain_id: raw.chain_id.clone(),
                    except_surface_binding_id: Some(surface_binding_id),
                    active_to: transition_time,
                    block_number: raw.block_number,
                    transaction_index: raw.transaction_index,
                    log_index: raw.log_index,
                });
                output.surface_bindings.push(SurfaceBinding {
                    surface_binding_id,
                    logical_name_id: logical_name_id.clone(),
                    resource_id,
                    binding_kind: name.binding_kind.clone(),
                    authority_arm: name.authority_arm.clone(),
                    active_from: transition_time,
                    chain_id: raw.chain_id.clone(),
                    block_hash: raw.block_hash.clone(),
                    block_number: raw.block_number,
                    provenance: raw_provenance.clone(),
                    canonicality_state: raw.canonicality_state.clone(),
                });
            }
        }
        let mut after_state = json!({
            "source_event": selected.event.name,
            "raw_name": name.labels.join("."),
            "raw_labels": name.labels,
            "namehash": name.namehash,
        });
        if let (Some(after), Some(metadata)) =
            (after_state.as_object_mut(), name.preimage_metadata.as_ref())
            && let Some(metadata) = metadata.as_object()
        {
            after.extend(metadata.clone());
        }
        output.normalized_events.push(preimage_event(
            selected,
            raw,
            Some(logical_name_id),
            &name.namehash,
            after_state,
        ));
    }
    for name in &interpreted.shadow_names {
        represented.extend(name.raw_labels.iter().cloned());
        let logical_name_id = format!("{}:{}", selected.source.namespace, name.namehash);
        if v1_surface {
            state.observe_v1_surface(&selected.source.namespace, &name.namehash);
        }
        let decoded_labels = name
            .raw_labels
            .iter()
            .map(|label| decoded_label(label))
            .collect::<Option<Vec<_>>>();
        let postgres_text_labels =
            decoded_labels.filter(|labels| labels.iter().all(|label| !label.contains('\0')));
        let (raw_name, raw_labels, labelhashes) = postgres_text_labels
            .map(|labels| {
                let raw_name = labels.join(".");
                let labelhashes = name
                    .raw_labels
                    .iter()
                    .map(|label| hash_hex(label))
                    .collect();
                (raw_name, labels, labelhashes)
            })
            .unwrap_or_default();
        let normalization_errors = name
            .raw_labels
            .iter()
            .filter_map(|label| {
                let decoded_label = decoded_label(label);
                let flag = normalization_flag(decoded_label.as_deref());
                flag.error.map(|error| {
                    json!({
                        "raw_label_hex": hex::encode(label),
                        "decoded_label": decoded_label,
                        "error": error,
                    })
                })
            })
            .collect::<Vec<_>>();
        output.name_surfaces.push(NameSurface {
            logical_name_id: logical_name_id.clone(),
            namespace: selected.source.namespace.clone(),
            raw_name,
            raw_labels,
            dns_encoded_name: dns_encode_raw(&name.raw_labels).unwrap_or_default(),
            namehash: name.namehash.clone(),
            labelhashes,
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            visibility_state: "shadow".to_owned(),
            normalization_errors: Value::Array(normalization_errors),
            deactivation_reason: Some("normalization_gate".to_owned()),
            deactivated_at: Some(raw.block_timestamp),
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            provenance: raw_provenance.clone(),
            canonicality_state: raw.canonicality_state.clone(),
        });
        output.normalized_events.push(preimage_event(
            selected,
            raw,
            Some(logical_name_id.clone()),
            &name.namehash,
            json!({
                "source_event": selected.event.name,
                "logical_name_id": logical_name_id,
                "namehash": name.namehash,
                "visibility_state": "shadow",
                "deactivation_reason": "normalization_gate",
                "raw_labels_hex": name.raw_labels.iter().map(hex::encode).collect::<Vec<_>>(),
                "decoded_labels": name.raw_labels.iter().map(|label| decoded_label(label)).collect::<Vec<_>>(),
            }),
        ));
    }
    for label in &interpreted.labels {
        if label.skips_automatic_preimage(represented.contains(&label.raw_label)) {
            continue;
        }
        let labelhash = hash_hex(&label.raw_label);
        output.normalized_events.push(preimage_event(
            selected,
            raw,
            None,
            &labelhash,
            json!({
                "source_event": selected.event.name,
                "raw_label_hex": hex::encode(&label.raw_label),
                "decoded_label": decoded_label(&label.raw_label),
                "labelhash": labelhash,
            }),
        ));
    }
    Ok(())
}

fn binding_id(logical_name_id: &str, resource_id: uuid::Uuid, raw: &RawLogInput) -> uuid::Uuid {
    stable_uuid(&format!(
        "binding:{logical_name_id}:{resource_id}:{}:{}:{}:{}",
        raw.chain_id, raw.block_hash, raw.transaction_hash, raw.log_index
    ))
}

fn dns_encode_raw(labels: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut encoded = Vec::new();
    for label in labels {
        if label.is_empty() {
            return None;
        }
        encoded.push(u8::try_from(label.len()).ok()?);
        encoded.extend_from_slice(label);
    }
    encoded.push(0);
    Some(encoded)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn preimage(labelhash: &str, raw_label: &[u8], priority: i32, ordinal: i64) -> LabelPreimage {
        LabelPreimage {
            labelhash: labelhash.to_owned(),
            raw_label: raw_label.to_vec(),
            decoded_label: Some(String::from_utf8(raw_label.to_vec()).expect("UTF-8")),
            normalizer_version: "test-normalizer".to_owned(),
            normalized_under_version: true,
            normalization_error: None,
            source_kind: "LabelReserved_label".to_owned(),
            source_priority: priority,
            provenance: json!({"ordinal":ordinal}),
        }
    }

    fn other_preimage(hash: &str, label: &[u8], priority: i32, ordinal: i64) -> LabelPreimage {
        LabelPreimage {
            source_kind: "Resolver_name".to_owned(),
            ..preimage(hash, label, priority, ordinal)
        }
    }

    fn sequential_winners(observations: &[LabelPreimage]) -> BTreeMap<String, LabelPreimage> {
        let mut winners = BTreeMap::<String, LabelPreimage>::new();
        for candidate in observations {
            if winners
                .get(&candidate.labelhash)
                .is_none_or(|winner| candidate.source_priority >= winner.source_priority)
            {
                winners.insert(candidate.labelhash.clone(), candidate.clone());
            }
        }
        winners
    }

    #[test]
    fn reserved_label_preimage_compaction_preserves_writer_winner_and_position()
    -> anyhow::Result<()> {
        let mut output = BatchOutput {
            label_preimages: vec![
                preimage("hash-a", b"a", 100, 1),
                preimage("hash-b", b"b", 100, 2),
                preimage("hash-a", b"a", 90, 3),
                preimage("hash-a", b"a", 200, 4),
                preimage("hash-a", b"a", 200, 5),
            ],
            ..BatchOutput::default()
        };

        compact_reserved_label_preimages(&mut output)?;

        assert_eq!(output.label_preimages.len(), 2);
        assert_eq!(output.label_preimages[0].labelhash, "hash-b");
        assert_eq!(output.label_preimages[1].labelhash, "hash-a");
        assert_eq!(output.label_preimages[1].source_priority, 200);
        assert_eq!(output.label_preimages[1].provenance, json!({"ordinal":5}));
        Ok(())
    }

    #[test]
    fn label_preimage_compaction_rejects_inconsistent_same_hash_observations() {
        let mut output = BatchOutput {
            label_preimages: vec![
                preimage("hash-a", b"a", 100, 1),
                preimage("hash-a", b"different", 200, 2),
            ],
            ..BatchOutput::default()
        };

        let error =
            compact_reserved_label_preimages(&mut output).expect_err("conflict must be rejected");
        assert!(
            error
                .to_string()
                .contains("inconsistent preimage observations"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn interleaved_compaction_matches_writer() -> anyhow::Result<()> {
        let original = vec![
            preimage("hash-a", b"a", 100, 1),
            other_preimage("hash-a", b"a", 100, 2),
            preimage("hash-a", b"a", 100, 3),
            other_preimage("hash-a", b"a", 90, 4),
            preimage("hash-a", b"a", 200, 5),
            other_preimage("hash-a", b"a", 200, 6),
            preimage("hash-a", b"a", 200, 7),
            preimage("hash-b", b"b", 300, 8),
            other_preimage("hash-b", b"b", 300, 9),
            preimage("hash-b", b"b", 200, 10),
        ];
        let expected_winners = sequential_winners(&original);
        let expected_other_rows = original
            .iter()
            .filter(|observation| observation.source_kind != "LabelReserved_label")
            .count();
        let mut output = BatchOutput {
            label_preimages: original,
            ..BatchOutput::default()
        };

        compact_reserved_label_preimages(&mut output)?;

        assert_eq!(
            sequential_winners(&output.label_preimages),
            expected_winners
        );
        assert_eq!(
            output
                .label_preimages
                .iter()
                .filter(|observation| observation.source_kind != "LabelReserved_label")
                .count(),
            expected_other_rows,
            "compaction must retain every non-reservation submission"
        );
        Ok(())
    }
}
