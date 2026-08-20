use anyhow::{Context, ensure};
use bigname_domain::normalization::ENS_NORMALIZER_VERSION;
use serde_json::{Value, json};

use crate::schema_v2::{
    common::{dns_encode, hash_hex, normalization_flag},
    manifest::ManifestSource,
    model::{
        BatchOutput, BindingClosure, LabelPreimage, NameSurface, RawBlockInput, Resource,
        SurfaceBinding, TokenLineage,
    },
    normalized::boundary_preimage_event,
    protocol::Interpreted,
    seam::{ARM_WIDE_BINDING_CLOSE_KEY, CLOSED_AUTHORITY_ARM_KEY, SURFACE_BINDING_ID_KEY},
    state::State,
};

pub(in crate::schema_v2) fn materialize_v2_boundary(
    source: &ManifestSource,
    block: &RawBlockInput,
    mut interpreted: Interpreted,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    ensure!(
        interpreted.events.is_empty()
            && interpreted.labels.is_empty()
            && interpreted.shadow_names.is_empty()
            && interpreted.resources.is_empty()
            && interpreted.binding_closures.is_empty()
            && interpreted.bindings.is_empty()
            && interpreted.discovery.is_empty()
            && interpreted.migration_events.is_empty()
            && interpreted.migration_observations.is_empty()
            && interpreted.names.len() == 1,
        "ENSv2 boundary reassertion produced an unsupported draft shape"
    );
    let name = interpreted.names.pop().expect("checked one name draft");
    let resource_id = name
        .resource_id
        .context("ENSv2 boundary reassertion has no resource")?;
    let surface_binding_id = name
        .surface_binding_id
        .context("ENSv2 boundary reassertion has no binding identity")?;
    ensure!(name.bind, "ENSv2 boundary reassertion is not binding");
    let logical_name_id = format!("{}:{}", source.namespace, name.namehash);
    let provenance = json!({
        "kind":"raw_block",
        "chain_id":block.chain_id,
        "block_hash":block.block_hash,
        "block_number":block.block_number,
        "block_timestamp":block.block_timestamp.unix_timestamp(),
        "source_manifest_id":source.manifest_id,
    });
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
                .map(|error| json!({"raw_label":label,"error":error}))
        })
        .collect::<Vec<_>>();
    ensure!(errors.is_empty(), "ENSv2 boundary reassertion is shadowed");
    for label in &name.labels {
        let labelhash = hash_hex(label.as_bytes());
        output.label_preimages.push(LabelPreimage {
            labelhash,
            raw_label: label.as_bytes().to_vec(),
            decoded_label: Some(label.clone()),
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            normalized_under_version: true,
            normalization_error: None,
            source_kind: name.source_kind.clone(),
            source_priority: 100,
            provenance: provenance.clone(),
        });
    }
    output.name_surfaces.push(NameSurface {
        logical_name_id: logical_name_id.clone(),
        namespace: source.namespace.clone(),
        raw_name: name.labels.join("."),
        raw_labels: name.labels.clone(),
        dns_encoded_name: dns_encode(&name.labels)?,
        namehash: name.namehash.clone(),
        labelhashes: name
            .labels
            .iter()
            .map(|label| hash_hex(label.as_bytes()))
            .collect(),
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        visibility_state: "active".to_owned(),
        normalization_errors: Value::Array(errors),
        deactivation_reason: None,
        deactivated_at: None,
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        provenance: provenance.clone(),
        canonicality_state: block.canonicality_state.clone(),
    });
    output.resources.push(Resource {
        resource_id,
        token_lineage_id: name.token_lineage_id,
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        provenance: provenance.clone(),
        canonicality_state: block.canonicality_state.clone(),
    });
    if let Some(token_lineage_id) = name.token_lineage_id
        && state.materialize_token_lineage(token_lineage_id)
    {
        output.token_lineages.push(TokenLineage {
            token_lineage_id,
            chain_id: block.chain_id.clone(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            provenance: provenance.clone(),
            canonicality_state: block.canonicality_state.clone(),
        });
    }
    output.binding_closures.push(BindingClosure {
        logical_name_id: logical_name_id.clone(),
        authority_arm: name.authority_arm.clone(),
        chain_id: block.chain_id.clone(),
        except_surface_binding_id: Some(surface_binding_id),
        active_to: block.block_timestamp,
        block_number: block.block_number,
        transaction_index: -1,
        log_index: -1,
    });
    output.surface_bindings.push(SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.clone(),
        resource_id,
        binding_kind: name.binding_kind,
        authority_arm: name.authority_arm,
        active_from: block.block_timestamp,
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        provenance,
        canonicality_state: block.canonicality_state.clone(),
    });
    output.normalized_events.push(boundary_preimage_event(
        source,
        block,
        Some(logical_name_id),
        &name.namehash,
        json!({
            "source_event":"RegistryPathExpired",
            "raw_name":name.labels.join("."),
            "raw_labels":name.labels,
            "namehash":name.namehash,
            (ARM_WIDE_BINDING_CLOSE_KEY):true,
            (CLOSED_AUTHORITY_ARM_KEY):"ens_v2",
            (SURFACE_BINDING_ID_KEY):surface_binding_id.to_string(),
        }),
    ));
    Ok(())
}
