use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use serde_json::Value;

use super::pipeline::ApiServer;

pub type RouteSnapshots = BTreeMap<String, Value>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatchupEquivalenceContract {
    Full,
    MissingStatelessLabelPreimages,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StatelessLabelPreimage(String);

impl StatelessLabelPreimage {
    pub fn from_expected_row(row: Value) -> Result<Self> {
        stateless_label_preimage(&serde_json::to_string(&row)?)
    }
}

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
    api: &ApiServer,
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
            format!("/v1/history/names/ens/{name}?scope=both&view=compact&meta=none&page_size=50"),
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

/// Automatic catch-up parity is full
/// [normalized-event](../../../../docs/glossary.md) row equality after
/// normalizing per-corpus [contract-instance](../../../../docs/glossary.md)
/// ids. The receipt-reconstructed [label-preimage](../../../../docs/glossary.md)
/// events authenticate both paths under the full contract. The temporary #157
/// containment contract permits only those exact events to be live-only;
/// catch-up-only or any other live-only row fails.
pub async fn assert_catchup_normalized_event_parity(
    live: &sqlx::PgPool,
    catchup: &sqlx::PgPool,
    contract: CatchupEquivalenceContract,
    expected_preimages: &[StatelessLabelPreimage],
) -> Result<()> {
    let live_rows = normalized_event_rows(live, None).await?;
    let catchup_rows = normalized_event_rows(catchup, None).await?;
    ensure!(
        !expected_preimages.is_empty(),
        "catch-up equivalence must name at least one receipt-reconstructed stateless label-preimage event"
    );
    if contract == CatchupEquivalenceContract::Full {
        if live_rows != catchup_rows {
            bail!(
                "live and automatic catch-up normalized_events differed: live {} rows, catch-up {} rows\n{}",
                live_rows.len(),
                catchup_rows.len(),
                first_line_diff(&live_rows.join("\n"), &catchup_rows.join("\n"))
            );
        }
        assert_expected_stateless_label_preimages(
            &live_rows,
            expected_preimages,
            "live ingestion",
        )?;
        assert_expected_stateless_label_preimages(
            &catchup_rows,
            expected_preimages,
            "automatic catch-up",
        )?;
        return Ok(());
    }

    let catchup_only = multiset_difference(&catchup_rows, &live_rows);
    if !catchup_only.is_empty() {
        bail!(
            "automatic catch-up derived {} unexpected normalized event(s):\n{}",
            catchup_only.len(),
            catchup_only.join("\n")
        );
    }

    let live_only = multiset_difference(&live_rows, &catchup_rows);
    let mut observed_missing_preimages = live_only
        .iter()
        .map(|row| stateless_label_preimage(row))
        .collect::<Result<Vec<_>>>()?;
    observed_missing_preimages.sort();
    let mut expected_missing_preimages = expected_preimages.to_vec();
    expected_missing_preimages.sort();
    if observed_missing_preimages != expected_missing_preimages {
        bail!(
            "automatic catch-up omitted a different stateless label-preimage set: expected {expected_missing_preimages:#?}, observed {observed_missing_preimages:#?}"
        );
    }
    Ok(())
}

fn assert_expected_stateless_label_preimages(
    rows: &[String],
    expected_preimages: &[StatelessLabelPreimage],
    corpus: &str,
) -> Result<()> {
    let expected_rows = expected_preimages
        .iter()
        .map(|preimage| preimage.0.clone())
        .collect::<Vec<_>>();
    let missing = multiset_difference(&expected_rows, rows);
    if !missing.is_empty() {
        bail!(
            "{corpus} omitted {} receipt-reconstructed stateless label-preimage event(s):\n{}",
            missing.len(),
            missing.join("\n")
        );
    }
    Ok(())
}

fn stateless_label_preimage(row: &str) -> Result<StatelessLabelPreimage> {
    let event: Value = serde_json::from_str(row)?;
    ensure!(
        event.get("event_kind").and_then(Value::as_str) == Some("PreimageObserved")
            && event.get("derivation_kind").and_then(Value::as_str)
                == Some("raw_log_preimage_observation")
            && event.get("logical_name_id").is_some_and(Value::is_null)
            && event.get("resource_id").is_some_and(Value::is_null)
            && event.get("canonicality_state").and_then(Value::as_str) == Some("finalized")
            && event
                .get("event_identity")
                .and_then(Value::as_str)
                .is_some_and(|identity| identity.starts_with("raw_log_preimage_observed:"))
            && event.pointer("/raw_fact_ref/kind").and_then(Value::as_str) == Some("raw_log"),
        "unexpected live-only normalized event outside the stateless label-preimage class: {row}"
    );
    Ok(StatelessLabelPreimage(serde_json::to_string(&event)?))
}

async fn normalized_event_rows(
    pool: &sqlx::PgPool,
    logical_name_ids: Option<&[&str]>,
) -> Result<Vec<String>> {
    let ids = logical_name_ids.map(|ids| ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>());
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT jsonb_build_object( \
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
            'log_index', log_index, \
            'raw_fact_ref', raw_fact_ref, \
            'derivation_kind', derivation_kind, \
            'canonicality_state', canonicality_state::TEXT, \
            'before_state', before_state, \
            'after_state', after_state \
        )::TEXT \
        FROM normalized_events \
        WHERE $1::TEXT[] IS NULL OR logical_name_id = ANY($1)",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;

    // Manifest sync mints contract-instance UUIDs independently in each
    // corpus. Replace those UUIDs wherever they occur with the stable
    // chain/address identity; every other normalized-event field remains in
    // the comparison, including resource ids, manifest ids, raw-fact refs,
    // positions, before-state, and after-state.
    let contract_instances = contract_instance_keys(pool).await?;
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row: Value = serde_json::from_str(&row)?;
        normalize_contract_instance_ids(&mut row, &contract_instances);
        normalized.push(serde_json::to_string(&row)?);
    }
    normalized.sort();
    Ok(normalized)
}

async fn contract_instance_keys(pool: &sqlx::PgPool) -> Result<BTreeMap<String, String>> {
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

/// Backfill parity contract, pinned at the layers the bounded backfill job
/// owns:
///
/// 1. For every surface the scenario touched, the live and backfill
///    databases derive byte-identical normalized events (the authority
///    closure — the events that carry a `logical_name_id`).
/// 2. After normalizing per-corpus contract-instance ids, every event the
///    readiness-stopped live session derived exists identically in the
///    backfill database (live ⊆ backfill, exactly). Backfill-only extras
///    are bounded to bookkeeping and late-round derivations
///    (`SourceManifestUpdated`/`CapabilityChanged`/`PreimageObserved`)
///    that a readiness-stopped live session may be killed before writing.
pub async fn assert_backfill_normalized_event_parity(
    live: &sqlx::PgPool,
    backfill: &sqlx::PgPool,
    scenario_logical_name_ids: &[&str],
) -> Result<()> {
    let scoped = normalized_event_rows(live, Some(scenario_logical_name_ids)).await?;
    let scoped_backfill = normalized_event_rows(backfill, Some(scenario_logical_name_ids)).await?;
    if scoped != scoped_backfill {
        bail!(
            "scenario-scoped normalized_events differed: live {} rows, backfill {} rows\n{}",
            scoped.len(),
            scoped_backfill.len(),
            first_line_diff(&scoped.join("\n"), &scoped_backfill.join("\n"))
        );
    }

    let live_rows = normalized_event_rows(live, None).await?;
    let backfill_rows = normalized_event_rows(backfill, None).await?;
    let missing = multiset_difference(&live_rows, &backfill_rows);
    if !missing.is_empty() {
        bail!(
            "{} live-derived events are missing from the backfill database:\n{}",
            missing.len(),
            missing
                .iter()
                .map(|row| row.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    assert_delta_bounded(
        multiset_difference(&backfill_rows, &live_rows).iter(),
        "backfill-only",
        &[
            "SourceManifestUpdated",
            "CapabilityChanged",
            "PreimageObserved",
        ],
    )?;
    Ok(())
}

fn multiset_difference(left: &[String], right: &[String]) -> Vec<String> {
    let mut right_counts = BTreeMap::<&str, usize>::new();
    for row in right {
        *right_counts.entry(row).or_default() += 1;
    }

    let mut difference = Vec::new();
    for row in left {
        match right_counts.get_mut(row.as_str()) {
            Some(count) if *count > 0 => *count -= 1,
            _ => difference.push(row.clone()),
        }
    }
    difference
}

fn assert_delta_bounded<'a>(
    rows: impl Iterator<Item = &'a String>,
    direction: &str,
    allowed_kinds: &[&str],
) -> Result<()> {
    for row in rows {
        let parsed: Value = serde_json::from_str(row)?;
        let kind = parsed
            .get("event_kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !allowed_kinds.contains(&kind) {
            bail!("unexpected {direction} normalized event kind {kind}: {row}");
        }
    }
    Ok(())
}

async fn get_normalized_body(api: &ApiServer, path: &str) -> Result<Value> {
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
