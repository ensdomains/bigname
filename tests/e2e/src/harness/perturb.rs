use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use serde_json::Value;

use super::pipeline::ProjectionReader;

pub type RouteSnapshots = BTreeMap<String, Value>;

const NORMALIZED_EVENT_ROWS_SQL: &str = "SELECT jsonb_build_object( \
        'event_identity', event_identity, \
        'namespace', namespace, \
        'logical_name_id', logical_name_id, \
        'resource_id', resource_id::TEXT, \
        'event_kind', event_kind, \
        'source_family', source_family, \
        'manifest_version', manifest_version, \
        'source_manifest_id', source_manifest_id, \
        'chain_id', chain_id, \
        'block_number', block_number, \
        'block_hash', block_hash, \
        'transaction_hash', transaction_hash, \
        'transaction_index', transaction_index, \
        'log_index', log_index, \
        'raw_fact_ref', raw_fact_ref, \
        'derivation_kind', derivation_kind, \
        'canonicality_state', canonicality_state::TEXT, \
        'before_state', before_state, \
        'after_state', after_state \
    )::TEXT \
    FROM normalized_events \
    WHERE $1::TEXT[] IS NULL OR logical_name_id = ANY($1)";

#[derive(Clone, Debug, Default)]
pub struct RouteSnapshotSubjects {
    names: BTreeSet<String>,
    addresses: BTreeSet<String>,
}

impl RouteSnapshotSubjects {
    pub fn new(
        names: impl IntoIterator<Item = impl Into<String>>,
        addresses: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            names: names.into_iter().map(Into::into).collect(),
            addresses: addresses.into_iter().map(Into::into).collect(),
        }
    }
}

pub async fn route_snapshots(
    api: &ProjectionReader,
    subjects: &RouteSnapshotSubjects,
) -> Result<RouteSnapshots> {
    let mut snapshots = RouteSnapshots::new();

    for name in &subjects.names {
        let name = path_name(name);
        for path in [
            format!("/v1/names/ens/{name}"),
            format!("/v1/names/ens/{name}/children?include=counts"),
            format!(
                "/v1/names/ens/{name}/records?include=resolver_address,known_text_keys,\
                 content_hash,coins&texts=com.twitter&known_text_keys=true&content_hash=true\
                 &coin_types=60&mode=declared&meta=full"
            ),
        ] {
            snapshots.insert(
                format!("GET {path}"),
                get_normalized_body(api, &path).await?,
            );
        }
    }

    for address in &subjects.addresses {
        let path = format!(
            "/v1/addresses/{}/names?namespace=ens&relation=registrant&include=role_summary&page_size=50",
            address.to_ascii_lowercase()
        );
        snapshots.insert(
            format!("GET {path}"),
            get_normalized_body(api, &path).await?,
        );
    }

    Ok(snapshots)
}

pub fn assert_snapshots_equal(expected: &RouteSnapshots, actual: &RouteSnapshots) -> Result<()> {
    if expected != actual {
        bail!(
            "route snapshots differed:\n{}",
            snapshot_diff(expected, actual)?
        );
    }
    Ok(())
}

/// Upfront-fixture versus RPC-ingest parity is full normalized-event row
/// equality after normalizing per-corpus contract-instance ids. Fixture names
/// are checked explicitly so equality cannot hide a label-preimage omission
/// from both paths.
pub async fn assert_ingest_path_normalized_event_parity(
    upfront: &sqlx::PgPool,
    ingested: &sqlx::PgPool,
    expected_preimage_names: &[String],
) -> Result<()> {
    let upfront_rows = normalized_event_rows(upfront, None).await?;
    let ingested_rows = normalized_event_rows(ingested, None).await?;
    ensure!(
        !expected_preimage_names.is_empty(),
        "ingest-path equivalence must name at least one expected label preimage"
    );
    if upfront_rows != ingested_rows {
        bail!(
            "upfront and RPC-ingested normalized_events differed: upfront {} rows, ingested {} rows\n{}",
            upfront_rows.len(),
            ingested_rows.len(),
            first_line_diff(&upfront_rows.join("\n"), &ingested_rows.join("\n"))
        );
    }
    for (corpus, pool) in [("upfront", upfront), ("RPC-ingested", ingested)] {
        for name in expected_preimage_names {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM normalized_events \
                 WHERE event_kind = 'PreimageObserved' \
                   AND after_state->>'raw_name' = $1 \
                   AND canonicality_state IN ('canonical', 'safe', 'finalized')",
            )
            .bind(name)
            .fetch_one(pool)
            .await?;
            ensure!(
                count > 0,
                "{corpus} corpus omitted the {name} label preimage"
            );
        }
    }
    Ok(())
}

async fn normalized_event_rows(
    pool: &sqlx::PgPool,
    logical_name_ids: Option<&[&str]>,
) -> Result<Vec<String>> {
    let ids = logical_name_ids.map(|ids| ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>());
    let rows: Vec<String> = sqlx::query_scalar(NORMALIZED_EVENT_ROWS_SQL)
        .bind(ids)
        .fetch_all(pool)
        .await?;

    // Manifest sync mints contract-instance UUIDs independently in each
    // corpus. Replace those UUIDs wherever they occur with the stable
    // chain/address identity; every other normalized-event field remains in
    // the comparison, including resource ids, manifest ids, raw-fact refs,
    // positions, before-state, and after-state.
    let contract_instances = contract_instance_stable_keys(pool).await?;
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row: Value = serde_json::from_str(&row)?;
        normalize_contract_instance_ids(&mut row, &contract_instances);
        normalized.push(serde_json::to_string(&row)?);
    }
    normalized.sort();
    Ok(normalized)
}

pub async fn contract_instance_stable_keys(
    pool: &sqlx::PgPool,
) -> Result<BTreeMap<String, String>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT ON (contract_instance_id) \
            contract_instance_id::TEXT, \
            chain_id || ':' || lower(address) AS stable_key \
        FROM contract_instance_addresses \
        ORDER BY contract_instance_id, (deactivated_at IS NULL) DESC, admitted_at DESC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

fn normalize_contract_instance_ids(
    value: &mut Value,
    contract_instances: &BTreeMap<String, String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_contract_instance_ids(value, contract_instances);
            }
        }
        Value::Object(fields) => {
            for value in fields.values_mut() {
                normalize_contract_instance_ids(value, contract_instances);
            }
        }
        Value::String(value) => {
            for (id, stable_key) in contract_instances {
                if value.contains(id) {
                    *value = value.replace(id, &format!("<contract:{stable_key}>"));
                }
            }
        }
        _ => {}
    }
}

async fn get_normalized_body(api: &ProjectionReader, path: &str) -> Result<Value> {
    let (status, mut body) = api.get_json(path).await?;
    if !status.is_success() {
        bail!("GET {path} returned {status}: {body}");
    }
    normalize_snapshot_body(&mut body);
    Ok(body)
}

fn normalize_snapshot_body(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_snapshot_body(value);
            }
        }
        Value::Object(fields) => {
            let empty_collection = fields
                .get("data")
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty);
            for (key, value) in fields {
                match key.as_str() {
                    // `normalized_event_id` is a database sequence value. A
                    // reorg-observed DB can assign different ids than a fresh
                    // winning-branch control DB for the same event identities.
                    "normalized_event_id" => normalize_present_id(value, "<normalized_event_id>"),
                    // Route provenance aggregates those same sequence values;
                    // preserve cardinality but not run-specific ids.
                    "normalized_event_ids" => normalize_id_array(value, "<normalized_event_id>"),
                    // Manifest and event row ids are corpus-local sequences.
                    // The stable source-family/chain/address fields beside
                    // them remain in the replay comparison.
                    "source_manifest_id" | "manifest_id" | "contract_instance_id" => {
                        normalize_present_id(value, "<corpus_local_id>")
                    }
                    "selected_event_ids" => normalize_id_array(value, "<normalized_event_id>"),
                    // Interpreter state keys embed the corpus-local contract
                    // instance UUID while their surrounding raw-fact identity
                    // remains fully compared.
                    "interpreter_state_key" => {
                        normalize_present_id(value, "<interpreter_state_key>")
                    }
                    // Only empty collection envelopes fall back to the
                    // read-time wall clock. Non-empty and exact-name
                    // timestamps remain part of replay equality.
                    "last_updated" if empty_collection => {
                        normalize_present_id(value, "<last_updated>")
                    }
                    _ => normalize_snapshot_body(value),
                }
            }
        }
        _ => {}
    }
}

fn normalize_present_id(value: &mut Value, placeholder: &str) {
    if !value.is_null() {
        *value = Value::String(placeholder.to_owned());
    }
}

fn normalize_id_array(value: &mut Value, placeholder: &str) {
    let Value::Array(values) = value else {
        return;
    };
    for value in values {
        normalize_present_id(value, placeholder);
    }
}

fn snapshot_diff(expected: &RouteSnapshots, actual: &RouteSnapshots) -> Result<String> {
    let expected = serde_json::to_string_pretty(expected)?;
    let actual = serde_json::to_string_pretty(actual)?;
    Ok(first_line_diff(&expected, &actual))
}

fn first_line_diff(expected: &str, actual: &str) -> String {
    let expected_lines: Vec<&str> = expected.lines().collect();
    let actual_lines: Vec<&str> = actual.lines().collect();
    let max = expected_lines.len().max(actual_lines.len());
    for index in 0..max {
        let left = expected_lines.get(index).copied().unwrap_or("<missing>");
        let right = actual_lines.get(index).copied().unwrap_or("<missing>");
        if left != right {
            let start = index.saturating_sub(4);
            let end = (index + 5).min(max);
            let mut diff = format!("first difference at pretty JSON line {}\n", index + 1);
            for line in start..end {
                let expected_line = expected_lines.get(line).copied().unwrap_or("<missing>");
                let actual_line = actual_lines.get(line).copied().unwrap_or("<missing>");
                if expected_line == actual_line {
                    diff.push_str(&format!("  {}\n", expected_line));
                } else {
                    diff.push_str(&format!("- {}\n+ {}\n", expected_line, actual_line));
                }
            }
            return diff;
        }
    }
    "snapshots differed but no line difference was found".to_owned()
}

fn path_name(name: &str) -> String {
    name.replace('%', "%25")
        .replace('[', "%5B")
        .replace(']', "%5D")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_event_parity_includes_transaction_index() {
        assert!(
            NORMALIZED_EVENT_ROWS_SQL.contains("'transaction_index', transaction_index"),
            "full normalized-event parity must compare transaction_index"
        );
    }
}
