use anyhow::Context;

use super::{
    catalog::Catalog,
    model::{BatchOutput, RawLogInput},
    protocol::SourcedEventBatch,
    state::State,
};

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
        let source = catalog.source(batch.source_manifest_id).with_context(|| {
            format!(
                "state-derived source manifest is missing for namespace {namespace}, namehash {node}, manifest {}",
                batch.source_manifest_id
            )
        })?;
        super::normalized::materialize_for_source(source, raw, batch.events, state, output);
    }
    Ok(())
}
