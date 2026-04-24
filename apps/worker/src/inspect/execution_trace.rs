use anyhow::{Context, Result};
use bigname_storage::{ExecutionTraceInspection, ExecutionTraceStep};
use serde_json::{Value, json};

use super::InspectExecutionTraceArgs;
use super::formatting::format_timestamp;

pub(in crate::inspect) async fn inspect_execution_trace(
    args: InspectExecutionTraceArgs,
) -> Result<()> {
    let _emit_json = args.json;
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection =
        bigname_storage::load_execution_trace_inspection(&pool, args.execution_trace_id)
            .await?
            .with_context(|| format!("missing execution trace {}", args.execution_trace_id))?;

    println!("{}", render_execution_trace_inspection(&inspection));
    Ok(())
}

pub(in crate::inspect) fn render_execution_trace_inspection(
    inspection: &ExecutionTraceInspection,
) -> Value {
    let trace = &inspection.trace;
    json!({
        "command": "inspect execution-trace",
        "execution_trace_id": trace.execution_trace_id.to_string(),
        "request_type": trace.request_type.as_str(),
        "request_key": trace.request_key.as_str(),
        "namespace": trace.namespace.as_str(),
        "request": {
            "type": trace.request_type.as_str(),
            "key": trace.request_key.as_str(),
            "metadata": trace.request_metadata.clone(),
        },
        "request_metadata": trace.request_metadata.clone(),
        "chain_positions": persisted_context_array(&trace.chain_context, &[
            "chain_positions",
            "requested_positions",
        ]),
        "chain_context": trace.chain_context.clone(),
        "manifest_versions": persisted_context_array(&trace.manifest_context, &[
            "manifest_versions",
            "versions",
        ]),
        "manifest_context": trace.manifest_context.clone(),
        "contracts_called": trace.contracts_called.clone(),
        "gateway_digests": trace.gateway_digests.clone(),
        "status": execution_trace_status(inspection),
        "final_value_digest": persisted_digest_metadata(trace.final_payload.as_ref(), &[
            "final_value_digest",
            "value_digest",
            "digest",
        ]),
        "failure_reason": persisted_failure_reason(trace.failure_payload.as_ref()),
        "finished_at": trace.finished_at.map(format_timestamp),
        "steps": trace
            .steps
            .iter()
            .map(render_execution_trace_step)
            .collect::<Vec<_>>(),
    })
}

fn render_execution_trace_step(step: &ExecutionTraceStep) -> Value {
    json!({
        "step_index": step.step_index,
        "step_kind": step.step_kind.as_str(),
        "input_digest": step.input_digest.as_deref(),
        "output_digest": step.output_digest.as_deref(),
        "latency_ms": step.latency_ms,
        "canonicality_dependency": step.canonicality_dependency.clone(),
        "attachment_digest_metadata": persisted_digest_metadata(Some(&step.step_payload), &[
            "attachment_digest_metadata",
            "attachment_digests",
            "attachments",
        ]),
    })
}

fn persisted_context_array(context: &Value, keys: &[&str]) -> Value {
    keys.iter()
        .find_map(|key| context.get(*key).filter(|value| value.is_array()))
        .cloned()
        .unwrap_or(Value::Null)
}

fn persisted_digest_metadata(payload: Option<&Value>, keys: &[&str]) -> Option<Value> {
    let payload = payload?;
    keys.iter().find_map(|key| payload.get(*key).cloned())
}

fn persisted_failure_reason(payload: Option<&Value>) -> Option<String> {
    let payload = payload?;
    ["failure_reason", "reason", "message"]
        .iter()
        .find_map(|key| payload.get(*key)?.as_str().map(str::to_owned))
}

fn execution_trace_status(inspection: &ExecutionTraceInspection) -> &'static str {
    let trace = &inspection.trace;
    if trace.failure_payload.is_some() {
        "failed"
    } else if trace.final_payload.is_some() {
        "succeeded"
    } else {
        "unknown"
    }
}
