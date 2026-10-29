use super::*;

pub(super) fn interpret_loaded(
    catalog: &mut Catalog,
    blocks: &[crate::schema_v2::RawBlockInput],
    raw_logs: Vec<crate::schema_v2::RawLogInput>,
    state: &mut State,
) -> anyhow::Result<BatchOutput> {
    // A pass admits and retires discovered contracts as it reads the batch.
    // Each re-run below starts from the admissions the batch began with, so
    // a later admission cannot hide an emitter's earlier logs.
    let initial_catalog = catalog.clone();
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
    // Locked boundaries are only activated after batch assembly, but an
    // original child unwrapped later in the same batch must already see
    // them. Re-run once with each activated locked boundary recorded at
    // its own position. The unwrap gate orders against that position, so
    // a boundary proved later in the batch changes nothing earlier.
    // Any activated boundary triggers the re-run. An unwrap the boundary
    // changes need not leave a first-pass event that shows it: one that
    // keeps an already-selected registry-only authority binds nothing.
    let locked_boundaries = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "MigrationApplied"
                && event.consumer_visibility == "activated"
                && matches!(
                    event.after_state["migration_path"].as_str(),
                    Some("locked_wrapped" | "locked_child")
                )
        })
        .filter_map(|event| {
            Some((
                event.logical_name_id.clone()?,
                crate::schema_v2::model::PriorWritePosition::from_parts(
                    event.block_number,
                    event.transaction_index,
                    event.log_index,
                )?,
            ))
        })
        .collect::<Vec<_>>();
    if !locked_boundaries.is_empty() {
        let mut seeded = state.clone();
        for (name, position) in &locked_boundaries {
            seeded.note_live_locked_migration(name, *position);
        }
        *catalog = initial_catalog.clone();
        let corrected = interpret_once(catalog, blocks, raw_logs.clone(), &mut seeded)?;
        anyhow::ensure!(
            corrected.migration_authority_transitions == output.migration_authority_transitions,
            "locked-boundary reconciliation changed its proving ENSv1→ENSv2 migration boundaries"
        );
        if crossed.is_empty() {
            return Ok(corrected);
        }
        // Both reconciliations apply. The locked re-run's output is only
        // checked above and then discarded. The expiry re-run starts again
        // from `state`, seeded with the same locked boundaries and with the
        // crossed ENSv1→ENSv2 migrations.
        let mut expiry_state = state.clone();
        for (name, position) in &locked_boundaries {
            expiry_state.note_live_locked_migration(name, *position);
        }
        let mut reconciled = expiry_state;
        for transition in crossed {
            let block = blocks
                .iter()
                .find(|block| block.block_number == transition.block_number)
                .expect("ENSv1→ENSv2 migration boundary belongs to a loaded block");
            reconciled.remember_v2_migration(
                &transition.logical_name_id,
                block.block_timestamp.unix_timestamp(),
            );
        }
        *catalog = initial_catalog;
        let corrected = interpret_once(catalog, blocks, raw_logs, &mut reconciled)?;
        anyhow::ensure!(
            corrected.migration_authority_transitions == output.migration_authority_transitions,
            "expiry reconciliation changed its proving ENSv1→ENSv2 migration boundaries"
        );
        return Ok(corrected);
    }
    if crossed.is_empty() {
        return Ok(output);
    }
    // No locked boundary is activated here, so the expiry re-run needs none.
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
    *catalog = initial_catalog;
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
    let transactions = TransactionIndex::build(catalog, &raw_logs)?;
    let mut raw_logs = raw_logs.into_iter().peekable();
    let mut committed_state = state.clone();
    committed_state.begin_batch();
    let mut unselected = Vec::new();
    for block in blocks {
        let mut block_output = BatchOutput::default();
        let mut block_raw_logs = Vec::new();
        let mut deferred = Vec::new();
        let mut recovered_events = false;
        let first_migration_observation = migration_observations.len();
        let mut block_state = committed_state.clone();
        crate::schema_v2::settle_block_boundary(
            catalog,
            block,
            &mut block_state,
            &mut block_output,
        )?;
        // A discovery observation admits its target from its own block, which includes the
        // target's earlier logs in that block (an ERC-1967 proxy announces its implementation
        // during construction, before any registry can point at it). Interpret those logs as
        // soon as a log records the admitting discovery edge, before any later log of the block,
        // so a later log of the same emitter reads the state they leave.
        while raw_logs.peek().is_some_and(|raw| {
            raw.block_number == block.block_number && raw.block_hash == block.block_hash
        }) {
            block_raw_logs.push(raw_logs.next().expect("peeked raw log"));
            let raw = block_raw_logs.last().expect("collected raw log");
            let edges = block_output.discovery_edges.len();
            if !interpret_raw(
                catalog,
                raw,
                &mut block_state,
                &mut block_output,
                &mut migration_observations,
                &transactions,
            )? {
                deferred.push(block_raw_logs.len() - 1);
            } else if block_output.discovery_edges.len() > edges && !deferred.is_empty() {
                recovered_events |= recover_admitted(
                    catalog,
                    &block_raw_logs,
                    &mut deferred,
                    &mut block_state,
                    &mut block_output,
                    &mut migration_observations,
                    &transactions,
                )?;
            }
        }
        if !deferred.is_empty() {
            recovered_events |= recover_admitted(
                catalog,
                &block_raw_logs,
                &mut deferred,
                &mut block_state,
                &mut block_output,
                &mut migration_observations,
                &transactions,
            )?;
            unselected.extend(deferred.iter().map(|&index| block_raw_logs[index].clone()));
        }
        // Restore chain order so stream-chained before-states follow log order rather than
        // interpretation order.
        if recovered_events {
            block_output
                .normalized_events
                .sort_by_key(|event| (event.transaction_index, event.log_index));
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
    record_pre_admission_skips(catalog, &unselected, &mut output);
    crate::schema_v2::identity::compact_reserved_label_preimages(&mut output)?;
    crate::schema_v2::migration::correlate(catalog, migration_observations, &mut output)?;
    Ok(output)
}

/// Interprets, in log order, the deferred logs of the block that the catalog now selects, and
/// again while those logs record discovery edges. Returns whether they wrote normalized events.
fn recover_admitted(
    catalog: &mut Catalog,
    block_raw_logs: &[RawLogInput],
    deferred: &mut Vec<usize>,
    state: &mut State,
    output: &mut BatchOutput,
    migration_observations: &mut Vec<crate::schema_v2::protocol::MigrationObservation>,
    transactions: &TransactionIndex,
) -> anyhow::Result<bool> {
    let first_event = output.normalized_events.len();
    loop {
        let edges = output.discovery_edges.len();
        let mut still_deferred = Vec::new();
        for index in std::mem::take(deferred) {
            if !interpret_raw(
                catalog,
                &block_raw_logs[index],
                state,
                output,
                migration_observations,
                transactions,
            )? {
                still_deferred.push(index);
            }
        }
        *deferred = still_deferred;
        if deferred.is_empty() || output.discovery_edges.len() == edges {
            return Ok(output.normalized_events.len() > first_event);
        }
    }
}

/// Discovery is forward-only and performs no lookback: a log from an address admitted later in
/// the same batch stays uninterpreted. Record it so the drop is visible rather than silent.
fn record_pre_admission_skips(
    catalog: &Catalog,
    unselected: &[RawLogInput],
    output: &mut BatchOutput,
) {
    for raw in unselected {
        let Some(topic0) = raw.topics.first() else {
            continue;
        };
        let Some((admitted_at, source_family)) = catalog.later_discovery_admission(raw) else {
            continue;
        };
        output.decode_skips.push(crate::schema_v2::DecodeSkip {
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            transaction_hash: raw.transaction_hash.clone(),
            log_index: raw.log_index,
            emitting_address: raw.emitting_address.clone(),
            source_family,
            selection_topic0: topic0.clone(),
            match_all: false,
            decode_context: format!(
                "log precedes its emitter's discovery admission at block {admitted_at}; \
                 interpretation is forward-only from the admitting observation and performs no \
                 lookback"
            ),
            inconsistency_site: None,
        });
    }
}
