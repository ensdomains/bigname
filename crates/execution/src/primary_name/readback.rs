use anyhow::{Result, bail};
use bigname_storage::{ExecutionOutcome, ExecutionTrace};
use serde_json::Value;

use crate::json_helpers::required_array;
use crate::persistence::VerifiedPrimaryNameReadbackProvenance;

use super::context::verified_primary_context_label;

pub(crate) fn extract_verified_primary_readback_provenance(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<VerifiedPrimaryNameReadbackProvenance> {
    let context = verified_primary_context_label(&trace.namespace)?;
    let cache_manifest_versions = required_array(
        Some(&outcome.cache_key.manifest_versions),
        &format!("{context} cache_key.manifest_versions"),
    )?;
    if let Some(trace_manifest_versions) = trace.manifest_context.get("manifest_versions") {
        let trace_manifest_versions = required_array(
            Some(trace_manifest_versions),
            &format!("{context} trace.manifest_context.manifest_versions"),
        )?;
        if trace_manifest_versions != cache_manifest_versions {
            bail!(
                "{context} trace.manifest_context.manifest_versions must match cache_key.manifest_versions"
            );
        }
    }

    Ok(VerifiedPrimaryNameReadbackProvenance {
        execution_trace_id: trace.execution_trace_id,
        manifest_versions: Value::Array(cache_manifest_versions.clone()),
    })
}
