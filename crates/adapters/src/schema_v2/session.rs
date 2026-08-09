use anyhow::{Context, bail};
use serde_json::{Value, json};

use super::{
    BatchInput, BatchOutput, RawLogInput, catalog::Catalog, seam::INTERPRETER_STATE_KEY,
    state::State,
};

/// Opaque retained adapter state that can be moved into the next batch for the same chain.
#[derive(Debug, Eq, PartialEq)]
pub struct AdapterSession {
    chain_id: String,
    state: State,
}

pub fn interpret_schema_v2_batch(input: BatchInput) -> anyhow::Result<BatchOutput> {
    #[cfg(test)]
    {
        let incremental_input = input.clone();
        let restore_input = input.clone();
        let expected = interpret_fresh(input)?;
        let (actual, session) = interpret_schema_v2_batch_incremental(incremental_input, None)?;
        anyhow::ensure!(
            actual == expected,
            "incremental adapter output differs from fresh one-shot interpretation"
        );
        assert_restores_exactly(restore_input, &actual, &session)?;
        Ok(actual)
    }
    #[cfg(not(test))]
    {
        interpret_fresh(input)
    }
}

fn interpret_fresh(input: BatchInput) -> anyhow::Result<BatchOutput> {
    super::validate_order(&input)?;
    let BatchInput {
        manifests,
        discovery_rules,
        admissions,
        prior_events,
        blocks,
        raw_logs,
        ..
    } = input;
    let mut catalog = Catalog::new(manifests, discovery_rules, admissions)?;
    let mut state = State::new(prior_events, catalog.v2_suffix_anchors());
    interpret_loaded(&mut catalog, &blocks, raw_logs, &mut state)
}

/// Interprets one batch and returns the retained state to move into the next batch.
///
/// A resumed session must belong to the same chain, and its input must not repeat restored prior
/// events. Passing no session performs the full restore from `input.prior_events`.
pub fn interpret_schema_v2_batch_incremental(
    input: BatchInput,
    session: Option<AdapterSession>,
) -> anyhow::Result<(BatchOutput, AdapterSession)> {
    super::validate_order(&input)?;
    let BatchInput {
        chain_id,
        manifests,
        discovery_rules,
        admissions,
        prior_events,
        blocks,
        raw_logs,
    } = input;
    let mut catalog = Catalog::new(manifests, discovery_rules, admissions)?;
    let suffix_anchors = catalog.v2_suffix_anchors();
    let mut committed_state = match session {
        Some(session) => {
            if session.chain_id != chain_id {
                bail!(
                    "adapter session for chain {} cannot interpret chain {chain_id}",
                    session.chain_id
                );
            }
            if !prior_events.is_empty() {
                bail!("a resumed adapter session cannot also receive restored prior events");
            }
            let mut state = session.state;
            state.replace_v2_suffix_anchors(suffix_anchors);
            state
        }
        None => State::new(prior_events, suffix_anchors),
    };
    // Reconciliation can discard provisional transitions. Interpret on a structurally shared
    // branch, then advance the retained state with only the normalized events that survived.
    let mut state = committed_state.clone();
    let output = interpret_loaded(&mut catalog, &blocks, raw_logs, &mut state)?;
    let delta = super::seam::fold_prior_events(Vec::new(), &output.normalized_events, &blocks)?;
    committed_state.apply_prior_event_delta(delta);
    Ok((
        output,
        AdapterSession {
            chain_id,
            state: committed_state,
        },
    ))
}

fn interpret_loaded(
    catalog: &mut Catalog,
    blocks: &[super::RawBlockInput],
    raw_logs: Vec<RawLogInput>,
    state: &mut State,
) -> anyhow::Result<BatchOutput> {
    let mut output = BatchOutput::default();
    let mut raw_logs = raw_logs.into_iter().peekable();
    let prior_tails = state.value_tails().clone();
    for block in blocks {
        super::settle_block_boundary(catalog, block, state, &mut output)?;
        while raw_logs.peek().is_some_and(|raw| {
            raw.block_number == block.block_number && raw.block_hash == block.block_hash
        }) {
            let raw = raw_logs.next().expect("peeked raw log");
            interpret_raw(catalog, &raw, state, &mut output)?;
        }
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
    super::protocol::reconcile_batch(&mut output);
    rethread_before_states(&mut output, prior_tails);
    Ok(output)
}

// Reconciliation can drop events or rewrite their interpreter state keys after in-memory
// transitions have already read state written under the pre-reconciliation keys. Re-derive every
// stream-chained before_state from the surviving rows so the emitted stream — and nothing else —
// determines what each event observes, keeping the output independent of where batch boundaries
// fall.
fn rethread_before_states(output: &mut BatchOutput, prior_tails: imbl::OrdMap<String, Value>) {
    let mut tails = prior_tails;
    for event in &mut output.normalized_events {
        let Some(state_key) = event
            .raw_fact_ref
            .get(INTERPRETER_STATE_KEY)
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !event.before_state_explicit {
            event.before_state = tails.get(state_key).cloned().unwrap_or_else(|| json!({}));
        }
        tails.insert(state_key.to_owned(), event.after_state.clone());
    }
}

fn interpret_raw(
    catalog: &mut Catalog,
    raw: &RawLogInput,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let Some(selected) = catalog.select(raw)? else {
        return Ok(());
    };
    let interpreted = match super::protocol::interpret(&selected, raw, state) {
        Ok(interpreted) => interpreted,
        Err(error) if selected.match_all && crate::evm_abi::is_malformed_event_log(&error) => {
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
    super::normalized::materialize(&selected, raw, interpreted.events.clone(), state, output);
    super::identity::materialize(&selected, raw, &interpreted, state, output)?;
    super::discovery::materialize(catalog, &selected, raw, interpreted.discovery, output)?;
    Ok(())
}

#[cfg(test)]
fn assert_restores_exactly(
    input: BatchInput,
    output: &BatchOutput,
    session: &AdapterSession,
) -> anyhow::Result<()> {
    let catalog = Catalog::new(input.manifests, input.discovery_rules, input.admissions)?;
    let prior = super::seam::fold_prior_events(
        input.prior_events,
        &output.normalized_events,
        &input.blocks,
    )?;
    let restored = AdapterSession {
        chain_id: input.chain_id,
        state: State::new(prior, catalog.v2_suffix_anchors()),
    };
    anyhow::ensure!(
        session == &restored,
        "live adapter session state differs from a fresh retained-event restore:\n\
         live={session:#?}\nrestored={restored:#?}"
    );
    Ok(())
}
