use super::*;

use std::collections::BTreeSet;

fn interpret_raw(
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

fn interpret_held_registrar(
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

/// Interpret one transaction's logs in order. A registrar `NameRegistered` /
/// `NameRenewed` is the fallback source of a fact only when nothing later in
/// the same transaction claims it, and that is not known until the transaction
/// has been read; but the fact has to exist at the registrar's own position,
/// because a later log in the same transaction — the controller transferring
/// the token it registered to itself — reads the state it establishes. So a
/// transaction that holds a registrar log is interpreted twice: a trial decides
/// which held logs go unclaimed, then the transaction is interpreted for real
/// with those logs interpreted as the fallback where they sit. The trial only
/// answers that question: it runs on a copy of the state (structurally shared,
/// so cheap) with scratch output that is discarded, never on a copy of the
/// output accumulated so far, which would grow with every earlier transaction.
pub(super) fn interpret_transaction(
    catalog: &mut Catalog,
    transaction: &[RawLogInput],
    state: &mut State,
    output: &mut BatchOutput,
    migration_observations: &mut Vec<crate::schema_v2::protocol::MigrationObservation>,
) -> anyhow::Result<()> {
    let holds_registrar_log = transaction
        .iter()
        .map(|raw| is_fallback_registrar_log(catalog, raw))
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .any(|held| held);
    if !holds_registrar_log {
        for raw in transaction {
            interpret_raw(catalog, raw, state, output, migration_observations)?;
        }
        return Ok(());
    }
    let mut trial_state = state.clone();
    let mut scratch_output = BatchOutput::default();
    let mut scratch_observations = Vec::new();
    for raw in transaction {
        interpret_raw(
            catalog,
            raw,
            &mut trial_state,
            &mut scratch_output,
            &mut scratch_observations,
        )?;
    }
    let unclaimed = trial_state
        .take_v1_pending_registrar_logs()
        .into_iter()
        .map(|raw| raw.log_index)
        .collect::<BTreeSet<_>>();
    for raw in transaction {
        interpret_raw(catalog, raw, state, output, migration_observations)?;
        if unclaimed.contains(&raw.log_index) {
            interpret_held_registrar(catalog, raw, state, output)?;
        }
    }
    // The real pass held the same logs again; they are interpreted now.
    state.take_v1_pending_registrar_logs();
    Ok(())
}

/// A registrar lifecycle log that the manifest has opted into the fallback for,
/// by declaring `RegistrationGranted` on it. A manifest that declares only
/// migration output never holds the log, so its transactions need no trial.
fn is_fallback_registrar_log(catalog: &Catalog, raw: &RawLogInput) -> anyhow::Result<bool> {
    Ok(catalog.select(raw)?.is_some_and(|selected| {
        selected.source.source_family == "ens_v1_registrar_l1"
            && matches!(
                selected.event.signature.as_str(),
                "NameRegistered(uint256,address,uint256)" | "NameRenewed(uint256,uint256)"
            )
            && selected
                .event
                .normalized_events
                .iter()
                .any(|kind| kind == "RegistrationGranted")
    }))
}
