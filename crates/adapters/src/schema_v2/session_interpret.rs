use super::*;

pub(super) fn interpret_loaded(
    catalog: &mut Catalog,
    blocks: &[crate::schema_v2::RawBlockInput],
    raw_logs: Vec<crate::schema_v2::RawLogInput>,
    state: &mut State,
) -> anyhow::Result<BatchOutput> {
    let output = interpret_once(catalog, blocks, raw_logs.clone(), state)?;
    let crossed = output
        .migration_authority_transitions
        .iter()
        .filter(|transition| {
            output.normalized_events.iter().any(|event| {
                event.source_family == "ens_v1_registrar_l1"
                    && event.event_kind == "RegistrationReleased"
                    && event.raw_fact_ref["kind"] == "raw_block"
                    && event.logical_name_id.as_deref() == Some(&transition.logical_name_id)
                    && event
                        .block_number
                        .is_some_and(|block| block > transition.block_number)
            })
        })
        .collect::<Vec<_>>();
    if crossed.is_empty() {
        return Ok(output);
    }
    // Migration groups are confirmed after batch assembly. If this same batch
    // also crosses an old lease's expiry, interpret it once more with those
    // proven boundaries. The timestamp prevents a future proof affecting an
    // earlier block; raw logs and migration correlation remain unchanged.
    let mut reconciled = state.clone();
    for transition in crossed {
        let block = blocks
            .iter()
            .find(|block| block.block_number == transition.block_number)
            .expect("migration boundary belongs to a loaded block");
        reconciled.remember_v2_migration(
            &transition.logical_name_id,
            block.block_timestamp.unix_timestamp(),
        );
    }
    let corrected = interpret_once(catalog, blocks, raw_logs, &mut reconciled)?;
    anyhow::ensure!(
        corrected.migration_authority_transitions == output.migration_authority_transitions,
        "expiry reconciliation changed its proving migration boundaries"
    );
    Ok(corrected)
}

fn interpret_once(
    catalog: &mut Catalog,
    blocks: &[crate::schema_v2::RawBlockInput],
    raw_logs: Vec<RawLogInput>,
    state: &mut State,
) -> anyhow::Result<BatchOutput> {
    let mut output = BatchOutput::default();
    let mut migration_observations = Vec::new();
    let registrar_registry_setups = registrar_registry_setups(catalog, &raw_logs)?;
    let mut raw_logs = raw_logs.into_iter().peekable();
    let mut committed_state = state.clone();
    committed_state.begin_batch();
    for block in blocks {
        let mut block_output = BatchOutput::default();
        let mut block_raw_logs = Vec::new();
        let first_migration_observation = migration_observations.len();
        let mut block_state = committed_state.clone();
        crate::schema_v2::settle_block_boundary(
            catalog,
            block,
            &mut block_state,
            &mut block_output,
        )?;
        while raw_logs.peek().is_some_and(|raw| {
            raw.block_number == block.block_number && raw.block_hash == block.block_hash
        }) {
            block_raw_logs.push(raw_logs.next().expect("peeked raw log"));
            let raw = block_raw_logs.last().expect("collected raw log");
            interpret_raw(
                catalog,
                raw,
                &mut block_state,
                &mut block_output,
                &mut migration_observations,
                &registrar_registry_setups,
            )?;
        }
        crate::schema_v2::protocol::reconcile_block(
            catalog,
            block,
            &block_raw_logs,
            &migration_observations[first_migration_observation..],
            &committed_state,
            &mut block_state,
            &mut block_output,
        )?;
        committed_state = block_state;
        append_output(&mut output, block_output);
    }
    if let Some(raw) = raw_logs.next() {
        bail!(
            "raw log {}:{} at block {} {} has no matching loaded live-lineage block",
            raw.transaction_hash,
            raw.log_index,
            raw.block_number,
            raw.block_hash
        );
    }
    if let Some((logical_name_id, authority_arm)) =
        committed_state.pending_v2_terminal_closure_hit()
    {
        bail!(
            "terminal {authority_arm} binding closure for {logical_name_id} was not handled in its adapter batch"
        );
    }
    crate::schema_v2::identity::compact_reserved_label_preimages(&mut output)?;
    crate::schema_v2::migration::correlate(catalog, migration_observations, &mut output)?;
    Ok(output)
}
