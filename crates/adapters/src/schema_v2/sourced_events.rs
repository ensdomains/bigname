use anyhow::Context;

use super::{
    catalog::{Catalog, Selected},
    model::{BatchOutput, RawLogInput},
    protocol::{Interpreted, SourcedEventBatch},
    state::State,
};

pub(super) fn prepare_v1_state_derived_events(
    catalog: &Catalog,
    selected: &Selected,
    raw: &RawLogInput,
    interpreted: &mut Interpreted,
    state: &mut State,
) -> anyhow::Result<()> {
    mark_registrar_retirement(catalog, selected, raw, interpreted);
    super::protocol::v1::materialize_registrar_surface(selected, raw, state, interpreted)?;
    super::protocol::v1::materialize_wrapper_surface(selected, raw, state, interpreted)?;
    for event in interpreted.events.iter().chain(
        interpreted
            .sourced_events
            .iter()
            .flat_map(|batch| batch.events.iter()),
    ) {
        let Some((node, resolver, resource_id)) = (event.event_kind == "ResolverChanged")
            .then(|| {
                Some((
                    super::seam::v1_event_node(&event.after_state)?,
                    event.after_state.get("resolver")?.as_str()?,
                    event.resource_id?,
                ))
            })
            .flatten()
        else {
            continue;
        };
        state.remember_v1_resolver_linked_resource(
            &selected.source.namespace,
            node,
            resolver,
            resource_id,
            event.logical_name_id.clone(),
        );
    }
    Ok(())
}

pub(super) fn materialize(
    catalog: &Catalog,
    namespace: &str,
    raw: &RawLogInput,
    batches: Vec<SourcedEventBatch>,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for batch in batches {
        let node = batch
            .events
            .first()
            .and_then(|event| event.after_state.get("node"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let source = catalog.provenance(batch.source_manifest_id).with_context(|| {
            format!(
                "state-derived source manifest is missing for namespace {namespace}, namehash {node}, manifest {}",
                batch.source_manifest_id
            )
        })?;
        super::normalized::materialize_for_provenance(source, raw, batch.events, state, output);
    }
    Ok(())
}

// Retain only the admitted launch-bounded Graveyard retirement, independently of readability.
fn mark_registrar_retirement(
    catalog: &Catalog,
    selected: &Selected,
    raw: &RawLogInput,
    output: &mut Interpreted,
) {
    let family = "ens_v2_migration_l1";
    let Some(migration) = catalog.source_for_family(family) else {
        return;
    };
    if selected.source.source_family != "ens_v1_registrar_l1"
        || selected.emitter_role.as_deref() != Some("registrar")
        || migration.namespace != selected.source.namespace
        || migration.chain_id != raw.chain_id
        || catalog
            .correlation_address(family, "ens_v1_base_registrar")
            .is_none_or(|address| !address.eq_ignore_ascii_case(&raw.emitting_address))
        || catalog
            .declared_start_block_for_role(family, "graveyard")
            .is_none_or(|start| raw.block_number < start)
    {
        return;
    }
    let Some(graveyard) = catalog.declared_address_for_role(family, "graveyard") else {
        return;
    };
    for event in output
        .events
        .iter_mut()
        .chain(output.migration_events.iter_mut())
    {
        if (event.event_kind == "TokenControlTransferred"
            && event.after_state["to"]
                .as_str()
                .is_some_and(|owner| owner.eq_ignore_ascii_case(graveyard)))
            || (event.event_kind == "RegistrationReleased"
                && event.after_state["owner"]
                    .as_str()
                    .is_some_and(|owner| owner.eq_ignore_ascii_case(graveyard)))
        {
            event.after_state["registrar_surface_retired"] = true.into();
        }
    }
}
