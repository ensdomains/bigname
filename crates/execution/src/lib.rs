//! ENS verified-resolution direct-path execution persistence bootstrap.

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, RawCallSnapshot, upsert_execution_outcome,
    upsert_execution_trace, upsert_raw_call_snapshots,
};
use serde_json::{Map, Value};
use sqlx::PgPool;
use uuid::Uuid;

pub use bigname_storage::{
    CanonicalityState, ExecutionTraceStep, load_execution_outcome, load_execution_trace,
    load_raw_call_snapshots_by_block_hash,
};

pub const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";
pub const ENS_NAMESPACE: &str = "ens";
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
pub const ENS_EXECUTION_SOURCE_FAMILY: &str = "ens_execution";
pub const ENS_UNIVERSAL_RESOLVER_ROLE: &str = "universal_resolver";
pub const ENS_UNIVERSAL_RESOLVER_ADDRESS: &str = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";

/// One narrow direct-path ENS verified-resolution persistence request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistEnsExactNameVerifiedResolutionRequest {
    pub raw_call_snapshots: Vec<RawCallSnapshot>,
    pub trace: ExecutionTrace,
    pub outcome: ExecutionOutcome,
}

/// Persisted identity the route layer can read back through storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedVerifiedResolutionIdentity {
    pub execution_trace_id: Uuid,
    pub cache_key: ExecutionCacheKey,
}

/// Current execution bootstrap status.
pub const fn bootstrap_status() -> &'static str {
    "ens-verified-resolution-direct-producer-ready"
}

/// Persist one exact-name ENS verified-resolution direct-path result and return
/// the storage identity the route layer can load back.
pub async fn persist_ens_exact_name_verified_resolution_direct(
    pool: &PgPool,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<PersistedVerifiedResolutionIdentity> {
    validate_direct_request(request)?;

    if !request.raw_call_snapshots.is_empty() {
        upsert_raw_call_snapshots(pool, &request.raw_call_snapshots).await?;
    }

    let trace = upsert_execution_trace(pool, &request.trace).await?;
    let outcome = upsert_execution_outcome(pool, &request.outcome).await?;

    if trace.execution_trace_id != outcome.execution_trace_id {
        bail!(
            "persisted ENS verified-resolution direct path trace {} does not match outcome trace {}",
            trace.execution_trace_id,
            outcome.execution_trace_id
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "persisted ENS verified-resolution direct path request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }
    Ok(PersistedVerifiedResolutionIdentity {
        execution_trace_id: trace.execution_trace_id,
        cache_key: outcome.cache_key,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerifiedQueryStatus {
    Success,
    NotFound,
    ExecutionFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedQuerySummary {
    record_key: String,
    coin_type: String,
    status: VerifiedQueryStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestedChainPosition {
    chain_id: String,
    block_number: i64,
    block_hash: String,
}

fn validate_direct_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<VerifiedQuerySummary> {
    let query = extract_supported_verified_query(&request.outcome)?;
    validate_trace(&request.trace, &request.outcome, &query)?;
    validate_outcome(&request.outcome, &request.trace, &query)?;
    validate_raw_call_snapshots(&request.raw_call_snapshots, &request.outcome, &query)?;
    Ok(query)
}

fn extract_supported_verified_query(outcome: &ExecutionOutcome) -> Result<VerifiedQuerySummary> {
    let outcome_payload = outcome
        .outcome_payload
        .as_ref()
        .context("ENS direct-path verified resolution outcome must set outcome_payload")?;
    let payload = required_object(
        Some(outcome_payload),
        "ENS direct-path verified resolution outcome_payload",
    )?;
    let verified_queries = required_array(
        payload.get("verified_queries"),
        "ENS direct-path verified resolution outcome_payload.verified_queries",
    )?;
    if verified_queries.len() != 1 {
        bail!(
            "ENS direct-path verified resolution outcome_payload must include exactly one verified query, found {}",
            verified_queries.len()
        );
    }

    let query = required_object(
        Some(&verified_queries[0]),
        "ENS direct-path verified resolution outcome_payload.verified_queries[0]",
    )?;
    if query.contains_key("unsupported_reason") {
        bail!("ENS direct-path verified resolution does not persist unsupported selectors");
    }

    let record_key = required_string(
        query,
        "record_key",
        "ENS direct-path verified resolution outcome query",
    )?
    .to_owned();
    let coin_type = parse_supported_addr_record_key(&record_key)?;
    let status = match required_string(
        query,
        "status",
        "ENS direct-path verified resolution outcome query",
    )? {
        "success" => {
            let value = required_object(
                query.get("value"),
                "ENS direct-path verified resolution outcome query.value",
            )?;
            let value_coin_type = required_string(
                value,
                "coin_type",
                "ENS direct-path verified resolution outcome query.value",
            )?;
            if value_coin_type != coin_type {
                bail!(
                    "ENS direct-path verified resolution outcome query value coin_type {} does not match record_key {}",
                    value_coin_type,
                    record_key
                );
            }
            required_nonempty_string_field(
                value,
                "value",
                "ENS direct-path verified resolution outcome query.value",
            )?;
            if query.contains_key("failure_reason") {
                bail!(
                    "ENS direct-path verified resolution success query must not set failure_reason"
                );
            }
            VerifiedQueryStatus::Success
        }
        "not_found" => {
            ensure_absent(
                query,
                "value",
                "ENS direct-path verified resolution not_found query",
            )?;
            optional_nonempty_string_field(
                query,
                "failure_reason",
                "ENS direct-path verified resolution not_found query",
            )?;
            VerifiedQueryStatus::NotFound
        }
        "execution_failed" => {
            ensure_absent(
                query,
                "value",
                "ENS direct-path verified resolution execution_failed query",
            )?;
            required_nonempty_string_field(
                query,
                "failure_reason",
                "ENS direct-path verified resolution execution_failed query",
            )?;
            VerifiedQueryStatus::ExecutionFailed
        }
        status => bail!(
            "ENS direct-path verified resolution only supports success, not_found, and execution_failed selector results; found {status}"
        ),
    };

    Ok(VerifiedQuerySummary {
        record_key,
        coin_type,
        status,
    })
}

fn validate_trace(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "ENS direct-path verified resolution trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if trace.namespace != ENS_NAMESPACE {
        bail!(
            "ENS direct-path verified resolution trace {} must use namespace {}",
            trace.execution_trace_id,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS direct-path verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let request_metadata = required_object(
        Some(&trace.request_metadata),
        "ENS direct-path verified resolution trace.request_metadata",
    )?;
    let surface = required_string(
        request_metadata,
        "surface",
        "ENS direct-path verified resolution trace.request_metadata",
    )?;
    let metadata_record_key = required_string(
        request_metadata,
        "record_key",
        "ENS direct-path verified resolution trace.request_metadata",
    )?;
    if metadata_record_key != query.record_key {
        bail!(
            "ENS direct-path verified resolution trace.request_metadata.record_key {} does not match outcome record_key {}",
            metadata_record_key,
            query.record_key
        );
    }

    let expected_request_key = format!("{ENS_NAMESPACE}:{surface}:{}", query.record_key);
    if trace.request_key != expected_request_key {
        bail!(
            "ENS direct-path verified resolution trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        "ENS direct-path verified resolution trace.gateway_digests",
    )?;
    if !gateway_digests.is_empty() {
        bail!("ENS direct-path verified resolution must keep gateway_digests empty");
    }

    if !manifest_versions_include_source_family(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
    )? {
        bail!(
            "ENS direct-path verified resolution must include source_family {} in manifest context or cache key",
            ENS_EXECUTION_SOURCE_FAMILY
        );
    }

    ensure_contains_universal_resolver_call(&trace.contracts_called, trace.execution_trace_id)?;
    ensure_steps_are_direct_path_only(&trace.steps, trace.execution_trace_id)?;

    match query.status {
        VerifiedQueryStatus::Success => {
            if trace.failure_payload.is_some() {
                bail!(
                    "ENS direct-path verified resolution success trace {} must not set failure_payload",
                    trace.execution_trace_id
                );
            }
            let final_payload = trace.final_payload.as_ref().with_context(|| {
                format!(
                    "ENS direct-path verified resolution success trace {} must set final_payload",
                    trace.execution_trace_id
                )
            })?;
            validate_success_final_payload(final_payload, query)?;
        }
        VerifiedQueryStatus::NotFound => {
            if trace.failure_payload.is_some() {
                bail!(
                    "ENS direct-path verified resolution not_found trace {} must not set failure_payload",
                    trace.execution_trace_id
                );
            }
            let final_payload = trace.final_payload.as_ref().with_context(|| {
                format!(
                    "ENS direct-path verified resolution not_found trace {} must set final_payload",
                    trace.execution_trace_id
                )
            })?;
            let final_payload_object = required_object(
                Some(final_payload),
                "ENS direct-path verified resolution not_found trace.final_payload",
            )?;
            optional_nonempty_string_field(
                final_payload_object,
                "failure_reason",
                "ENS direct-path verified resolution not_found trace.final_payload",
            )?;
        }
        VerifiedQueryStatus::ExecutionFailed => {
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
        }
    }

    Ok(())
}

fn validate_outcome(
    outcome: &ExecutionOutcome,
    trace: &ExecutionTrace,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    if outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if outcome.namespace != ENS_NAMESPACE {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS direct-path verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "ENS direct-path verified resolution trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "ENS direct-path verified resolution outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "ENS direct-path verified resolution outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;
    if trace_positions != requested_positions {
        bail!(
            "ENS direct-path verified resolution trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    match query.status {
        VerifiedQueryStatus::ExecutionFailed => {
            required_object(
                outcome.failure_payload.as_ref(),
                "ENS direct-path verified resolution execution_failed outcome.failure_payload",
            )?;
        }
        VerifiedQueryStatus::Success | VerifiedQueryStatus::NotFound => {
            if outcome.failure_payload.is_some() {
                bail!(
                    "ENS direct-path verified resolution non-failed outcome for request_key {} must not set failure_payload",
                    outcome.cache_key.request_key
                );
            }
        }
    }

    Ok(())
}

fn validate_raw_call_snapshots(
    raw_call_snapshots: &[RawCallSnapshot],
    outcome: &ExecutionOutcome,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    if raw_call_snapshots.is_empty() {
        return Ok(());
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;
    let requested_position = requested_positions
        .first()
        .context("ENS direct-path verified resolution must include one requested chain position")?;

    for snapshot in raw_call_snapshots {
        if snapshot.chain_id != requested_position.chain_id
            || snapshot.block_hash != requested_position.block_hash
            || snapshot.block_number != requested_position.block_number
        {
            bail!(
                "ENS direct-path verified resolution raw call snapshot for request {} must align with requested chain position {} {} {}",
                query.record_key,
                requested_position.chain_id,
                requested_position.block_number,
                requested_position.block_hash
            );
        }
    }

    Ok(())
}

fn parse_supported_addr_record_key(record_key: &str) -> Result<String> {
    let Some(coin_type) = record_key.strip_prefix("addr:") else {
        bail!(
            "ENS direct-path verified resolution only supports addr:<coin_type> selectors, found {}",
            record_key
        );
    };
    if coin_type.is_empty() || !coin_type.as_bytes().iter().all(u8::is_ascii_digit) {
        bail!(
            "ENS direct-path verified resolution only supports addr:<coin_type> selectors, found {}",
            record_key
        );
    }
    Ok(coin_type.to_owned())
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
    if record_kind != "addr" {
        bail!(
            "ENS direct-path verified resolution success trace.final_payload.record_kind must be addr, found {}",
            record_kind
        );
    }
    let coin_type = required_coin_type_field(
        object,
        "coin_type",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    if coin_type != query.coin_type {
        bail!(
            "ENS direct-path verified resolution success trace.final_payload.coin_type {} does not match outcome record_key {}",
            coin_type,
            query.record_key
        );
    }
    required_nonempty_string_field(
        object,
        "value",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    Ok(())
}

fn manifest_versions_include_source_family(
    manifest_context: Option<&Value>,
    cache_manifest_versions: Option<&Value>,
) -> Result<bool> {
    if let Some(context) = manifest_context {
        let object = required_object(
            Some(context),
            "ENS direct-path verified resolution trace.manifest_context",
        )?;
        if contains_source_family(object.get("manifest_versions"), ENS_EXECUTION_SOURCE_FAMILY)? {
            return Ok(true);
        }
    }

    contains_source_family(cache_manifest_versions, ENS_EXECUTION_SOURCE_FAMILY)
}

fn contains_source_family(value: Option<&Value>, expected_source_family: &str) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let items = required_array(
        Some(value),
        "ENS direct-path verified resolution manifest_versions",
    )?;
    for (index, item) in items.iter().enumerate() {
        let object = required_object(
            Some(item),
            &format!("ENS direct-path verified resolution manifest_versions[{index}]"),
        )?;
        if object
            .get("source_family")
            .and_then(Value::as_str)
            .is_some_and(|value| value == expected_source_family)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_contains_universal_resolver_call(
    contracts_called: &Value,
    execution_trace_id: Uuid,
) -> Result<()> {
    let calls = required_array(
        Some(contracts_called),
        "ENS direct-path verified resolution trace.contracts_called",
    )?;
    for (index, call) in calls.iter().enumerate() {
        let object = required_object(
            Some(call),
            &format!("ENS direct-path verified resolution trace.contracts_called[{index}]"),
        )?;
        let chain_id = required_string(
            object,
            "chain_id",
            "ENS direct-path verified resolution trace.contracts_called entry",
        )?;
        let contract_address = required_string(
            object,
            "contract_address",
            "ENS direct-path verified resolution trace.contracts_called entry",
        )?;
        let selector = required_string(
            object,
            "selector",
            "ENS direct-path verified resolution trace.contracts_called entry",
        )?;
        if chain_id == ETHEREUM_MAINNET_CHAIN_ID
            && contract_address.eq_ignore_ascii_case(ENS_UNIVERSAL_RESOLVER_ADDRESS)
            && !selector.is_empty()
        {
            return Ok(());
        }
    }

    bail!(
        "ENS direct-path verified resolution trace {} must include one {} contract call on {}",
        execution_trace_id,
        ENS_UNIVERSAL_RESOLVER_ROLE,
        ETHEREUM_MAINNET_CHAIN_ID
    )
}

fn ensure_steps_are_direct_path_only(
    steps: &[bigname_storage::ExecutionTraceStep],
    execution_trace_id: Uuid,
) -> Result<()> {
    let mut saw_universal_resolver_call = false;
    for step in steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("alias")
            || normalized.contains("wildcard")
            || normalized.contains("ccip")
        {
            bail!(
                "ENS direct-path verified resolution trace {} must not persist non-direct step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if step.step_kind == "call_universal_resolver" {
            saw_universal_resolver_call = true;
        }
    }

    if !saw_universal_resolver_call {
        bail!(
            "ENS direct-path verified resolution trace {} must include step_kind call_universal_resolver",
            execution_trace_id
        );
    }

    Ok(())
}

fn ensure_single_ethereum_mainnet_position(
    positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    if positions.len() != 1 {
        bail!(
            "{context} must include exactly one chain position, found {}",
            positions.len()
        );
    }
    let position = &positions[0];
    if position.chain_id != ETHEREUM_MAINNET_CHAIN_ID {
        bail!(
            "{context} must target chain_id {}, found {}",
            ETHEREUM_MAINNET_CHAIN_ID,
            position.chain_id
        );
    }
    Ok(())
}

fn required_chain_positions(
    value: Option<&Value>,
    context: &str,
) -> Result<Vec<RequestedChainPosition>> {
    let items = required_array(value, context)?;
    let mut positions = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = required_object(Some(item), &format!("{context}[{index}]"))?;
        let block_number = object
            .get("block_number")
            .and_then(Value::as_i64)
            .with_context(|| {
                format!("{context}[{index}] must include integer field block_number")
            })?;
        positions.push(RequestedChainPosition {
            chain_id: required_string(object, "chain_id", &format!("{context}[{index}]"))?
                .to_owned(),
            block_number,
            block_hash: required_string(object, "block_hash", &format!("{context}[{index}]"))?
                .to_owned(),
        });
    }
    Ok(positions)
}

fn required_object<'a>(value: Option<&'a Value>, context: &str) -> Result<&'a Map<String, Value>> {
    value
        .and_then(Value::as_object)
        .with_context(|| format!("{context} must be a JSON object"))
}

fn required_array<'a>(value: Option<&'a Value>, context: &str) -> Result<&'a Vec<Value>> {
    value
        .and_then(Value::as_array)
        .with_context(|| format!("{context} must be a JSON array"))
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{context} must include non-empty string field {field_name}"))
}

fn required_nonempty_string_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<String> {
    Ok(required_string(object, field_name, context)?.to_owned())
}

fn optional_nonempty_string_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<Option<String>> {
    match object.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => bail!("{context} field {field_name} must be null or a non-empty string"),
    }
}

fn required_coin_type_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<String> {
    match object.get(field_name) {
        Some(Value::String(value))
            if !value.is_empty() && value.as_bytes().iter().all(u8::is_ascii_digit) =>
        {
            Ok(value.clone())
        }
        Some(Value::Number(value)) if value.as_u64().is_some_and(|coin_type| coin_type > 0) => {
            Ok(value.to_string())
        }
        _ => bail!("{context} field {field_name} must be decimal coin_type text or number"),
    }
}

fn ensure_absent(object: &Map<String, Value>, field_name: &str, context: &str) -> Result<()> {
    if object.contains_key(field_name) {
        bail!("{context} must not set field {field_name}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };

    use super::*;
    use bigname_storage::{MIGRATOR, default_database_url};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for execution tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_execution_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for execution tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect execution test pool")?;

            MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for execution tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn requested_chain_positions() -> Value {
        json!([
            {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "block_number": 21_000_000,
                "block_hash": "0xabc123"
            }
        ])
    }

    fn version_boundary(resource_id: Uuid) -> Value {
        json!({
            "logical_name_id": "ens:alice.eth",
            "resource_id": resource_id.to_string(),
            "normalized_event_id": 1_200,
            "event_kind": "RecordsChanged",
            "chain_position": {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "block_number": 21_000_000,
                "block_hash": "0xabc123",
                "timestamp": "2024-06-01T00:00:17Z",
            }
        })
    }

    fn manifest_versions() -> Value {
        json!([
            {
                "source_family": ENS_EXECUTION_SOURCE_FAMILY,
                "manifest_version": 1
            },
            {
                "source_manifest_id": 7,
                "manifest_version": 3
            }
        ])
    }

    fn raw_call_snapshot() -> RawCallSnapshot {
        RawCallSnapshot {
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xabc123".to_owned(),
            block_number: 21_000_000,
            request_hash: "0xreq-a".to_owned(),
            request_payload: json!({
                "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                "data": "0x9061b923"
            }),
            response_hash: "0xresp-a".to_owned(),
            response_payload: json!({
                "result": "0x00000000000000000000000000000000000000aa"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }
    }

    fn success_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000011);
        let finished_at = timestamp(1_717_171_717);
        let request_key = "ens:alice.eth:addr:60".to_owned();
        PersistEnsExactNameVerifiedResolutionRequest {
            raw_call_snapshots: vec![raw_call_snapshot()],
            trace: ExecutionTrace {
                execution_trace_id,
                request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
                request_key: request_key.clone(),
                namespace: ENS_NAMESPACE.to_owned(),
                chain_context: json!({
                    "requested_positions": requested_chain_positions(),
                    "topology_version_boundary": {
                        ETHEREUM_MAINNET_CHAIN_ID: 21_000_000
                    }
                }),
                manifest_context: json!({
                    "manifest_versions": manifest_versions(),
                    "rollout_boundary": "shadow"
                }),
                contracts_called: json!([
                    {
                        "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                        "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                        "selector": "0x9061b923"
                    }
                ]),
                gateway_digests: json!([]),
                final_payload: Some(json!({
                    "record_kind": "addr",
                    "coin_type": 60,
                    "value": "0x00000000000000000000000000000000000000aa"
                })),
                failure_payload: None,
                request_metadata: json!({
                    "surface": "alice.eth",
                    "record_key": "addr:60",
                    "normalizer_version": "uts46-v1"
                }),
                finished_at: Some(finished_at),
                steps: vec![
                    ExecutionTraceStep {
                        step_index: 0,
                        step_kind: "load_declared_topology".to_owned(),
                        input_digest: Some("sha256:topology-input".to_owned()),
                        output_digest: Some("sha256:topology-output".to_owned()),
                        latency_ms: Some(4),
                        canonicality_dependency: json!({
                            ETHEREUM_MAINNET_CHAIN_ID: {
                                "block_hash": "0xabc123",
                                "block_number": 21_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "entrypoint": ENS_UNIVERSAL_RESOLVER_ROLE,
                            "resolver": ENS_UNIVERSAL_RESOLVER_ADDRESS
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 1,
                        step_kind: "call_universal_resolver".to_owned(),
                        input_digest: Some("sha256:resolver-input".to_owned()),
                        output_digest: Some("sha256:resolver-output".to_owned()),
                        latency_ms: Some(28),
                        canonicality_dependency: json!({
                            ETHEREUM_MAINNET_CHAIN_ID: {
                                "block_hash": "0xabc123",
                                "block_number": 21_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "coin_type": 60,
                            "name": "alice.eth",
                            "resolved_address": "0x00000000000000000000000000000000000000aa"
                        }),
                    },
                ],
            },
            outcome: ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key,
                    requested_chain_positions: requested_chain_positions(),
                    manifest_versions: manifest_versions(),
                    topology_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000aaa1,
                    )),
                    record_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000aaa2,
                    )),
                },
                execution_trace_id,
                request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
                namespace: ENS_NAMESPACE.to_owned(),
                outcome_payload: Some(json!({
                    "verified_queries": [
                        {
                            "record_key": "addr:60",
                            "status": "success",
                            "value": {
                                "coin_type": "60",
                                "value": "0x00000000000000000000000000000000000000aa"
                            }
                        }
                    ]
                })),
                failure_payload: None,
                finished_at,
            },
        }
    }

    fn execution_failed_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = success_request();
        request.raw_call_snapshots.clear();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000012);
        request.trace.request_key = "ens:alice.eth:addr:60".to_owned();
        request.trace.final_payload = None;
        request.trace.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "stage": "call_universal_resolver"
        }));
        request.trace.finished_at = Some(timestamp(1_717_171_800));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "execution_failed",
                    "failure_reason": "resolver_call_reverted"
                }
            ]
        }));
        request.outcome.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "reverted": true
        }));
        request.outcome.finished_at = request
            .trace
            .finished_at
            .expect("execution failed test trace must finish");
        request
    }

    #[tokio::test]
    async fn persists_successful_direct_path_and_reads_back_storage_identity() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = success_request();

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        let loaded_raw_calls = load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?;
        assert_eq!(loaded_raw_calls, request.raw_call_snapshots);

        let persisted_again =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(persisted_again, persisted);
        assert_eq!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?,
            request.raw_call_snapshots
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_execution_failed_direct_path_without_raw_call_snapshots() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = execution_failed_request();

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted.execution_trace_id,
            request.trace.execution_trace_id
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "execution failed direct path fixture should not persist raw call snapshots"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_non_addr_selector_before_writing_any_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = success_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000013);
        request.trace.request_key = "ens:alice.eth:text:com.twitter".to_owned();
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "text:com.twitter",
            "normalizer_version": "uts46-v1"
        });
        request.trace.final_payload = Some(json!({
            "record_kind": "text",
            "key": "com.twitter",
            "value": "@alice"
        }));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.cache_key.request_key = request.trace.request_key.clone();
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice"
                    }
                }
            ]
        }));

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("non-addr selector must be rejected");
        assert!(
            error
                .to_string()
                .contains("only supports addr:<coin_type> selectors"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected request must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected request must not persist outcome rows"
        );
        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "rejected request must not persist raw call snapshots"
        );

        database.cleanup().await
    }
}
