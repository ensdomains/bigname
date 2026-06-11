use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionTrace, SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey,
};
use serde_json::Value;

use super::selectors::extract_verified_queries_from_payload;
use crate::json_helpers::{
    optional_nonempty_string_field, required_coin_type_field, required_nonempty_string_field,
    required_object, required_string,
};
use crate::validation::{VerifiedQueryStatus, VerifiedQuerySummary};

pub(super) fn validate_trace_terminal_payloads(
    trace: &ExecutionTrace,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    let all_execution_failed = queries
        .iter()
        .all(|query| query.status == VerifiedQueryStatus::ExecutionFailed);

    if all_execution_failed {
        if trace.final_payload.is_some() {
            bail!(
                "ENS direct-path verified resolution execution_failed trace {} must not set final_payload",
                trace.execution_trace_id
            );
        }
        required_object(
            trace.failure_payload.as_ref(),
            "ENS direct-path verified resolution execution_failed trace.failure_payload",
        )?;
        return Ok(());
    }

    if trace.failure_payload.is_some() {
        bail!(
            "ENS direct-path verified resolution trace {} must not set failure_payload unless every selector status is execution_failed",
            trace.execution_trace_id
        );
    }

    let final_payload = trace.final_payload.as_ref().with_context(|| {
        format!(
            "ENS direct-path verified resolution trace {} must set final_payload when any selector resolves or returns not_found",
            trace.execution_trace_id
        )
    })?;
    if final_payload_contains_verified_queries(final_payload)? {
        let final_queries = extract_verified_queries_from_payload(
            final_payload,
            "ENS direct-path verified resolution trace.final_payload",
        )?;
        if final_queries != queries {
            bail!(
                "ENS direct-path verified resolution trace.final_payload.verified_queries must match outcome_payload.verified_queries"
            );
        }
        return Ok(());
    }

    if queries.len() != 1 {
        bail!(
            "ENS direct-path verified resolution multi-selector trace {} final_payload must include verified_queries",
            trace.execution_trace_id
        );
    }

    match queries[0].status {
        VerifiedQueryStatus::Success => validate_success_final_payload(final_payload, &queries[0]),
        VerifiedQueryStatus::NotFound => {
            validate_not_found_final_payload(final_payload, &queries[0])
        }
        VerifiedQueryStatus::Unsupported => bail!(
            "ENS direct-path verified resolution unsupported trace.final_payload must include verified_queries"
        ),
        VerifiedQueryStatus::ExecutionFailed => unreachable!("all execution_failed handled above"),
    }
}

fn validate_success_final_payload(
    final_payload: &Value,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    let object = required_object(
        Some(final_payload),
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    let record_kind = required_string(
        object,
        "record_kind",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    match &query.selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => {
            if record_kind != "addr" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be addr, found {}",
                    record_kind
                );
            }
            let payload_coin_type = required_coin_type_field(
                object,
                "coin_type",
                "ENS direct-path verified resolution success trace.final_payload",
            )?;
            if &payload_coin_type != coin_type {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.coin_type {} does not match outcome record_key {}",
                    payload_coin_type,
                    query.record_key
                );
            }
        }
        SupportedVerifiedRecordKey::Contenthash => {
            if record_kind != "contenthash" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be contenthash, found {}",
                    record_kind
                );
            }
        }
        SupportedVerifiedRecordKey::Avatar => {
            if record_kind != "avatar" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be avatar, found {}",
                    record_kind
                );
            }
        }
        SupportedVerifiedRecordKey::Text => {
            if record_kind != "text" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be text, found {}",
                    record_kind
                );
            }
        }
    }
    let value = required_nonempty_string_field(
        object,
        "value",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    if query
        .value
        .as_deref()
        .is_some_and(|expected_value| expected_value != value)
    {
        bail!(
            "ENS direct-path verified resolution success trace.final_payload.value {} does not match outcome query value {}",
            value,
            query.value.as_deref().unwrap_or_default()
        );
    }
    Ok(())
}

fn validate_not_found_final_payload(
    final_payload: &Value,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    let final_payload_object = required_object(
        Some(final_payload),
        "ENS direct-path verified resolution not_found trace.final_payload",
    )?;
    let failure_reason = optional_nonempty_string_field(
        final_payload_object,
        "failure_reason",
        "ENS direct-path verified resolution not_found trace.final_payload",
    )?;
    if failure_reason != query.failure_reason {
        bail!(
            "ENS direct-path verified resolution not_found trace.final_payload.failure_reason {:?} does not match outcome query failure_reason {:?}",
            failure_reason,
            query.failure_reason
        );
    }
    Ok(())
}

fn final_payload_contains_verified_queries(final_payload: &Value) -> Result<bool> {
    Ok(required_object(
        Some(final_payload),
        "ENS direct-path verified resolution trace.final_payload",
    )?
    .contains_key("verified_queries"))
}
