use super::*;

pub(super) fn interpret_raw(
    catalog: &mut Catalog,
    raw: &RawLogInput,
    state: &mut State,
    output: &mut BatchOutput,
    migration_observations: &mut Vec<crate::schema_v2::protocol::MigrationObservation>,
) -> anyhow::Result<()> {
    let Some(selected) = catalog.select(raw)? else {
        return Ok(());
    };
    let registrar_migration_source = if selected.source.source_family == "ens_v1_registrar_l1" {
        crate::schema_v2::migration::correlated_registrar_source(catalog, &selected, raw)?
    } else {
        None
    };
    // Interpret on a structurally shared candidate so a malformed log cannot retain partial state.
    let mut candidate_state = state.clone();
    let mut interpreted = match crate::schema_v2::protocol::interpret(
        &selected,
        raw,
        &mut candidate_state,
        registrar_migration_source.is_some(),
    ) {
        Ok(interpreted) => interpreted,
        Err(error)
            if crate::evm_abi::is_malformed_event_log(&error)
                && !selected.manifest_declared_emitter =>
        {
            output.decode_skips.push(crate::schema_v2::DecodeSkip {
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
            return Ok(());
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
    prepare_v1(&selected, raw, &mut interpreted, &mut candidate_state)?;
    *state = candidate_state;
    materialize_interpreted(catalog, &selected, raw, &interpreted, state, output)?;
    if let Some(migration_source) = registrar_migration_source {
        migration_observations.extend(interpreted.migration_observations);
        crate::schema_v2::normalized::materialize_for_source(
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
    Ok(())
}

pub(super) fn interpret_held_registrar(
    catalog: &mut Catalog,
    raw: &RawLogInput,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let Some(selected) = catalog.select(raw)? else {
        return Ok(());
    };
    let mut candidate_state = state.clone();
    let mut interpreted =
        crate::schema_v2::protocol::interpret_held_registrar(&selected, raw, &mut candidate_state)
            .with_context(|| {
                format!(
                    "{} registrar fallback failed for raw log {}:{}",
                    selected.source.source_family, raw.block_hash, raw.log_index
                )
            })?;
    prepare_v1(&selected, raw, &mut interpreted, &mut candidate_state)?;
    *state = candidate_state;
    debug_assert!(interpreted.migration_events.is_empty());
    materialize_interpreted(catalog, &selected, raw, &interpreted, state, output)
}

fn materialize_interpreted(
    catalog: &mut Catalog,
    selected: &crate::schema_v2::catalog::Selected,
    raw: &RawLogInput,
    interpreted: &crate::schema_v2::protocol::Interpreted,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    crate::schema_v2::normalized::materialize(
        selected,
        raw,
        interpreted.events.clone(),
        state,
        output,
    );
    crate::schema_v2::sourced_events::materialize(
        catalog,
        &selected.source.namespace,
        raw,
        interpreted.sourced_events.clone(),
        state,
        output,
    )?;
    crate::schema_v2::normalized::materialize_boundary(
        &selected.source,
        &crate::schema_v2::model::RawBlockInput {
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
    crate::schema_v2::identity::materialize(selected, raw, interpreted, state, output)?;
    crate::schema_v2::discovery::materialize(
        catalog,
        selected,
        raw,
        interpreted.discovery.clone(),
        output,
    )?;
    Ok(())
}
