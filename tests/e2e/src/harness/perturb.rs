use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use serde_json::Value;

use super::pipeline::ApiServer;

pub type RouteSnapshots = BTreeMap<String, Value>;

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

async fn normalized_event_rows(pool: &sqlx::PgPool) -> Result<BTreeSet<String>> {
    // Contract-instance ids are minted per corpus (random UUIDs at manifest
    // sync), so identical discovery derivations differ bytewise across
    // databases; strip exactly those fields for cross-database comparison.
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT jsonb_build_array( \
            event_identity, \
            event_kind, \
            logical_name_id, \
            canonicality_state::TEXT, \
            after_state - 'to_contract_instance_id' - 'from_contract_instance_id' \
                #- '{claim_provenance,contract_instance_id}' \
        )::TEXT \
        FROM normalized_events",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
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
    let scoped: (i64, String) = scoped_digest(live, scenario_logical_name_ids).await?;
    let scoped_backfill: (i64, String) = scoped_digest(backfill, scenario_logical_name_ids).await?;
    if scoped != scoped_backfill {
        bail!(
            "scenario-scoped normalized_events digest differed: live {scoped:?}, backfill {scoped_backfill:?}"
        );
    }

    let live_rows = normalized_event_rows(live).await?;
    let backfill_rows = normalized_event_rows(backfill).await?;
    let missing: Vec<&String> = live_rows.difference(&backfill_rows).collect();
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
        backfill_rows.difference(&live_rows),
        "backfill-only",
        &[
            "SourceManifestUpdated",
            "CapabilityChanged",
            "PreimageObserved",
        ],
    )?;
    Ok(())
}

fn assert_delta_bounded<'a>(
    rows: impl Iterator<Item = &'a String>,
    direction: &str,
    allowed_kinds: &[&str],
) -> Result<()> {
    for row in rows {
        let parsed: Vec<Value> = serde_json::from_str(row)?;
        let kind = parsed.get(1).and_then(Value::as_str).unwrap_or_default();
        if !allowed_kinds.contains(&kind) {
            bail!("unexpected {direction} normalized event kind {kind}: {row}");
        }
    }
    Ok(())
}

async fn scoped_digest(pool: &sqlx::PgPool, logical_name_ids: &[&str]) -> Result<(i64, String)> {
    let ids: Vec<String> = logical_name_ids.iter().map(|id| id.to_string()).collect();
    Ok(sqlx::query_as(
        "\
        SELECT \
            COUNT(*)::BIGINT AS row_count, \
            COALESCE(md5(string_agg(row_text, E'\\n' ORDER BY row_text)), md5('')) AS digest \
        FROM ( \
            SELECT jsonb_build_array( \
                event_identity, \
                event_kind, \
                logical_name_id, \
                canonicality_state::TEXT, \
                after_state \
            )::TEXT AS row_text \
            FROM normalized_events \
            WHERE logical_name_id = ANY($1) \
        ) AS rows",
    )
    .bind(&ids)
    .fetch_one(pool)
    .await?)
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
            for (key, value) in fields {
                match key.as_str() {
                    // `normalized_event_id` is a database sequence value. A
                    // reorg-observed DB can assign different ids than a fresh
                    // winning-branch control DB for the same event identities.
                    "normalized_event_id" => normalize_present_id(value, "<normalized_event_id>"),
                    // Route provenance aggregates those same sequence values;
                    // preserve cardinality but not run-specific ids.
                    "normalized_event_ids" => normalize_id_array(value, "<normalized_event_id>"),
                    // On empty collections `last_updated` is the read-time
                    // wall clock rather than a chain-derived position, so it
                    // differs across runs of identical state.
                    "last_updated" => normalize_present_id(value, "<last_updated>"),
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
