use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, NameCurrentRow, SurfaceBindingKind, clear_name_current, delete_name_current,
    upsert_name_current_rows,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use uuid::Uuid;

const ENS_NAMESPACE: &str = "ens";
const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
const NAME_CURRENT_DERIVATION_KIND: &str = "name_current_rebuild";
const RESOLVER_UNSUPPORTED_REASON: &str =
    "declared resolver summary is not yet projected for the ENSv1 name_current rebuild";
const RECORD_INVENTORY_UNSUPPORTED_REASON: &str =
    "record_inventory remains unsupported in the ENSv1 name_current rebuild";
const HISTORY_UNSUPPORTED_REASON: &str =
    "declared history pointers are not yet projected for the ENSv1 name_current rebuild";
const COVERAGE_UNSUPPORTED_REASON: &str =
    "current_resolver and record_inventory remain unsupported in this projection slice";
const RELEVANT_EVENT_KINDS: &[&str] = &[
    "AuthorityEpochChanged",
    "AuthorityTransferred",
    "ExpiryChanged",
    "RegistrationGranted",
    "RegistrationReleased",
    "RegistrationRenewed",
    "SurfaceBound",
    "SurfaceUnbound",
    "TokenControlTransferred",
];
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameCurrentRebuildSummary {
    pub requested_name_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug)]
struct NameSurfaceSeed {
    logical_name_id: String,
    namespace: String,
    canonical_display_name: String,
    normalized_name: String,
    namehash: String,
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: Option<OffsetDateTime>,
    canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug)]
struct CurrentBindingContext {
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    binding_kind: SurfaceBindingKind,
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: Option<OffsetDateTime>,
    surface_binding_state: CanonicalityState,
    resource_state: CanonicalityState,
    token_lineage_state: Option<CanonicalityState>,
}

#[derive(Clone, Debug)]
struct RelevantEvent {
    normalized_event_id: i64,
    event_kind: String,
    source_family: String,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    chain_id: Option<String>,
    block_number: Option<i64>,
    block_hash: Option<String>,
    block_timestamp: Option<OffsetDateTime>,
    raw_fact_ref: Value,
    canonicality_state: CanonicalityState,
    after_state: Value,
}

#[derive(Clone, Debug, Default)]
struct ProjectedFacts {
    registration_status: Option<String>,
    authority_kind: Option<String>,
    authority_key: Option<String>,
    registrant: Option<String>,
    expiry: Option<i64>,
    released_at: Option<i64>,
    registry_owner: Option<String>,
    latest_registration_event_kind: Option<String>,
    latest_control_event_kind: Option<String>,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    slot: String,
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: OffsetDateTime,
}

pub async fn rebuild_name_current(
    pool: &PgPool,
    logical_name_id: Option<&str>,
) -> Result<NameCurrentRebuildSummary> {
    match logical_name_id {
        Some(logical_name_id) => rebuild_one_name_current(pool, logical_name_id).await,
        None => rebuild_all_name_current(pool).await,
    }
}

async fn rebuild_all_name_current(pool: &PgPool) -> Result<NameCurrentRebuildSummary> {
    let names = load_canonical_name_surfaces(pool).await?;
    let _ = clear_name_current(pool).await?;

    let mut rows = Vec::with_capacity(names.len());
    for name in &names {
        rows.push(build_name_current_row(pool, name).await?);
    }

    let upserted_row_count = upsert_name_current_rows(pool, &rows).await?.len();
    Ok(NameCurrentRebuildSummary {
        requested_name_count: names.len(),
        upserted_row_count,
        deleted_row_count: 0,
    })
}

async fn rebuild_one_name_current(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<NameCurrentRebuildSummary> {
    let deleted_row_count = delete_name_current(pool, logical_name_id).await?;
    let Some(name) = load_canonical_name_surface(pool, logical_name_id).await? else {
        return Ok(NameCurrentRebuildSummary {
            requested_name_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let row = build_name_current_row(pool, &name).await?;
    let upserted_row_count = upsert_name_current_rows(pool, &[row]).await?.len();
    Ok(NameCurrentRebuildSummary {
        requested_name_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_name_current_row(pool: &PgPool, name: &NameSurfaceSeed) -> Result<NameCurrentRow> {
    let current_binding = load_current_binding_context(pool, &name.logical_name_id).await?;
    let events = load_relevant_events(pool, &name.logical_name_id).await?;
    let facts = project_facts(&events, current_binding.as_ref());
    let chain_positions = build_chain_positions(name, current_binding.as_ref(), &events);
    let canonicality_summary = build_canonicality_summary(name, current_binding.as_ref(), &events);
    let provenance = build_provenance(&events)?;
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .max()
        .unwrap_or(1);
    let last_recomputed_at = max_timestamp(name, current_binding.as_ref(), &events)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);

    Ok(NameCurrentRow {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        namehash: name.namehash.clone(),
        surface_binding_id: current_binding
            .as_ref()
            .map(|binding| binding.surface_binding_id),
        resource_id: current_binding.as_ref().map(|binding| binding.resource_id),
        token_lineage_id: current_binding
            .as_ref()
            .and_then(|binding| binding.token_lineage_id),
        binding_kind: current_binding.as_ref().map(|binding| binding.binding_kind),
        declared_summary: build_declared_summary(facts),
        provenance,
        coverage: json!({
            "status": "partial",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": COVERAGE_UNSUPPORTED_REASON,
            "enumeration_basis": "exact_name",
        }),
        chain_positions,
        canonicality_summary,
        manifest_version,
        last_recomputed_at,
    })
}

fn build_declared_summary(facts: ProjectedFacts) -> Value {
    json!({
        "registration": {
            "status": facts.registration_status,
            "authority_kind": facts.authority_kind,
            "authority_key": facts.authority_key,
            "registrant": facts.registrant,
            "expiry": facts.expiry,
            "released_at": facts.released_at,
            "latest_event_kind": facts.latest_registration_event_kind,
        },
        "control": {
            "registrant": facts.registrant,
            "registry_owner": facts.registry_owner,
            "latest_event_kind": facts.latest_control_event_kind,
        },
        "resolver": {
            "status": "unsupported",
            "address": Value::Null,
            "unsupported_reason": RESOLVER_UNSUPPORTED_REASON,
        },
        "record_inventory": {
            "status": "unsupported",
            "unsupported_reason": RECORD_INVENTORY_UNSUPPORTED_REASON,
        },
        "history": {
            "status": "unsupported",
            "unsupported_reason": HISTORY_UNSUPPORTED_REASON,
        },
    })
}

fn build_provenance(events: &[RelevantEvent]) -> Result<Value> {
    let normalized_event_ids = events
        .iter()
        .map(|event| event.normalized_event_id)
        .collect::<Vec<_>>();
    let raw_fact_refs = dedupe_json_values(events.iter().map(|event| event.raw_fact_ref.clone()))?;
    let manifest_versions = dedupe_json_values(events.iter().map(|event| {
        json!({
            "source_manifest_id": event.source_manifest_id,
            "source_family": event.source_family,
            "manifest_version": event.manifest_version,
        })
    }))?;

    Ok(json!({
        "normalized_event_ids": normalized_event_ids,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": NAME_CURRENT_DERIVATION_KIND,
    }))
}

fn build_chain_positions(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
) -> Value {
    let mut latest_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    if let Some(timestamp) = name.block_timestamp {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&name.chain_id),
                chain_id: name.chain_id.clone(),
                block_number: name.block_number,
                block_hash: name.block_hash.clone(),
                timestamp,
            },
        );
    }

    if let Some(binding) = current_binding
        && let Some(timestamp) = binding.block_timestamp
    {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.chain_id),
                chain_id: binding.chain_id.clone(),
                block_number: binding.block_number,
                block_hash: binding.block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in events {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(chain_id),
                chain_id: chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    Value::Object(
        latest_positions
            .into_iter()
            .map(|(slot, position)| {
                (
                    slot,
                    json!({
                        "chain_id": position.chain_id,
                        "block_number": position.block_number,
                        "block_hash": position.block_hash,
                        "timestamp": format_timestamp(position.timestamp),
                    }),
                )
            })
            .collect(),
    )
}

fn push_chain_position(
    latest_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    let replace = latest_positions
        .get(&candidate.slot)
        .map(|current| {
            candidate.block_number > current.block_number
                || (candidate.block_number == current.block_number
                    && candidate.block_hash > current.block_hash)
        })
        .unwrap_or(true);
    if replace {
        latest_positions.insert(candidate.slot.clone(), candidate);
    }
}

fn build_canonicality_summary(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
) -> Value {
    let mut states = vec![name.canonicality_state];
    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    merge_chain_state(&mut chain_states, &name.chain_id, name.canonicality_state);

    if let Some(binding) = current_binding {
        states.push(binding.surface_binding_state);
        states.push(binding.resource_state);
        merge_chain_state(
            &mut chain_states,
            &binding.chain_id,
            binding.surface_binding_state,
        );
        merge_chain_state(&mut chain_states, &binding.chain_id, binding.resource_state);
        if let Some(token_lineage_state) = binding.token_lineage_state {
            states.push(token_lineage_state);
            merge_chain_state(&mut chain_states, &binding.chain_id, token_lineage_state);
        }
    }

    for event in events {
        states.push(event.canonicality_state);
        if let Some(chain_id) = event.chain_id.as_ref() {
            merge_chain_state(&mut chain_states, chain_id, event.canonicality_state);
        }
    }

    let status =
        weakest_canonicality(states.iter().copied()).unwrap_or(CanonicalityState::Canonical);
    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

fn merge_chain_state(
    chain_states: &mut BTreeMap<String, CanonicalityState>,
    chain_id: &str,
    state: CanonicalityState,
) {
    let replacement = chain_states
        .get(chain_id)
        .map(|current| canonicality_rank(state) < canonicality_rank(*current))
        .unwrap_or(true);
    if replacement {
        chain_states.insert(chain_id.to_owned(), state);
    }
}

fn project_facts(
    events: &[RelevantEvent],
    current_binding: Option<&CurrentBindingContext>,
) -> ProjectedFacts {
    let mut facts = ProjectedFacts::default();

    for event in events {
        match event.event_kind.as_str() {
            "RegistrationGranted" => {
                facts.registration_status = Some("active".to_owned());
                facts.authority_kind = json_str(&event.after_state, &["authority_kind"]);
                facts.authority_key = json_str(&event.after_state, &["authority_key"]);
                facts.registrant = json_str(&event.after_state, &["registrant"]);
                facts.expiry = json_i64(&event.after_state, &["expiry"]);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "RegistrationRenewed" => {
                if facts.registration_status.as_deref() != Some("released") {
                    facts.registration_status = Some("active".to_owned());
                }
                facts.expiry = json_i64(&event.after_state, &["expiry"]).or(facts.expiry);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "ExpiryChanged" => {
                facts.expiry = json_i64(&event.after_state, &["expiry"]).or(facts.expiry);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "RegistrationReleased" => {
                facts.registration_status = Some("released".to_owned());
                facts.released_at = json_i64(&event.after_state, &["released_at"]);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "TokenControlTransferred" => {
                facts.registrant = json_str(&event.after_state, &["to"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            "AuthorityTransferred" => {
                facts.registry_owner = json_str(&event.after_state, &["owner"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            "AuthorityEpochChanged" => {
                facts.authority_kind = json_str(&event.after_state, &["authority_kind"]);
                facts.authority_key = json_str(&event.after_state, &["authority_key"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            _ => {}
        }
    }

    if current_binding.is_some() && facts.registration_status.is_none() {
        facts.registration_status = Some("active".to_owned());
    }

    facts
}

fn max_timestamp(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
) -> Option<OffsetDateTime> {
    let mut timestamps = Vec::new();
    if let Some(timestamp) = name.block_timestamp {
        timestamps.push(timestamp);
    }
    if let Some(binding) = current_binding
        && let Some(timestamp) = binding.block_timestamp
    {
        timestamps.push(timestamp);
    }
    timestamps.extend(events.iter().filter_map(|event| event.block_timestamp));
    timestamps.into_iter().max()
}

async fn load_canonical_name_surfaces(pool: &PgPool) -> Result<Vec<NameSurfaceSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id,
            ns.block_hash,
            ns.block_number,
            rb.block_timestamp,
            ns.canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces ns
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ns.chain_id
         AND rb.block_hash = ns.block_hash
        WHERE ns.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY ns.logical_name_id
        "#
    ))
    .fetch_all(pool)
    .await
    .context("failed to load canonical name_surfaces for name_current rebuild")?;

    rows.into_iter().map(decode_name_surface_seed).collect()
}

async fn load_canonical_name_surface(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameSurfaceSeed>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id,
            ns.block_hash,
            ns.block_number,
            rb.block_timestamp,
            ns.canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces ns
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ns.chain_id
         AND rb.block_hash = ns.block_hash
        WHERE ns.logical_name_id = $1
          AND ns.canonicality_state {CANONICAL_STATE_FILTER}
        "#
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load canonical name_surface {logical_name_id} for name_current rebuild")
    })?;

    row.map(decode_name_surface_seed).transpose()
}

async fn load_current_binding_context(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<CurrentBindingContext>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            sb.surface_binding_id,
            sb.resource_id,
            r.token_lineage_id,
            sb.binding_kind::TEXT AS binding_kind,
            sb.chain_id,
            sb.block_hash,
            sb.block_number,
            rb.block_timestamp,
            sb.canonicality_state::TEXT AS surface_binding_state,
            r.canonicality_state::TEXT AS resource_state,
            tl.canonicality_state::TEXT AS token_lineage_state
        FROM surface_bindings sb
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN token_lineages tl
          ON tl.token_lineage_id = r.token_lineage_id
         AND tl.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = sb.chain_id
         AND rb.block_hash = sb.block_hash
        WHERE sb.logical_name_id = $1
          AND sb.active_to IS NULL
          AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY sb.active_from DESC, sb.surface_binding_id DESC
        LIMIT 1
        "#
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load current binding context for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_current_binding_context).transpose()
}

async fn load_relevant_events(pool: &PgPool, logical_name_id: &str) -> Result<Vec<RelevantEvent>> {
    let event_kinds = RELEVANT_EVENT_KINDS
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect::<Vec<_>>();
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.namespace = $1
          AND ne.logical_name_id = $2
          AND ne.derivation_kind = $3
          AND ne.event_kind = ANY($4::TEXT[])
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number NULLS FIRST,
            COALESCE(ne.log_index, 2147483647),
            ne.event_identity
        "#
    ))
    .bind(ENS_NAMESPACE)
    .bind(logical_name_id)
    .bind(ENS_V1_AUTHORITY_DERIVATION_KIND)
    .bind(&event_kinds)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv1 normalized events for {logical_name_id}"))?;

    rows.into_iter().map(decode_relevant_event).collect()
}

fn decode_name_surface_seed(row: sqlx::postgres::PgRow) -> Result<NameSurfaceSeed> {
    Ok(NameSurfaceSeed {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing name_surface logical_name_id")?,
        namespace: row
            .try_get("namespace")
            .context("missing name_surface namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing name_surface canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing name_surface normalized_name")?,
        namehash: row
            .try_get("namehash")
            .context("missing name_surface namehash")?,
        chain_id: row
            .try_get("chain_id")
            .context("missing name_surface chain_id")?,
        block_hash: row
            .try_get("block_hash")
            .context("missing name_surface block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing name_surface block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing raw_blocks.block_timestamp join for name_surface")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing name_surface canonicality_state")?,
        )?,
    })
}

fn decode_current_binding_context(row: sqlx::postgres::PgRow) -> Result<CurrentBindingContext> {
    Ok(CurrentBindingContext {
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id in current binding context")?,
        resource_id: row
            .try_get("resource_id")
            .context("missing resource_id in current binding context")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id in current binding context")?,
        binding_kind: parse_surface_binding_kind(
            &row.try_get::<String, _>("binding_kind")
                .context("missing binding_kind in current binding context")?,
        )?,
        chain_id: row
            .try_get("chain_id")
            .context("missing chain_id in current binding context")?,
        block_hash: row
            .try_get("block_hash")
            .context("missing block_hash in current binding context")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number in current binding context")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp in current binding context")?,
        surface_binding_state: parse_canonicality_state(
            &row.try_get::<String, _>("surface_binding_state")
                .context("missing surface_binding_state in current binding context")?,
        )?,
        resource_state: parse_canonicality_state(
            &row.try_get::<String, _>("resource_state")
                .context("missing resource_state in current binding context")?,
        )?,
        token_lineage_state: row
            .try_get::<Option<String>, _>("token_lineage_state")
            .context("missing token_lineage_state in current binding context")?
            .map(|value| parse_canonicality_state(&value))
            .transpose()?,
    })
}

fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_kind: row.try_get("event_kind").context("missing event_kind")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        after_state: row.try_get("after_state").context("missing after_state")?,
    })
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state {value}"),
    }
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface_binding kind {value}"),
    }
}

fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Observed => 0,
        CanonicalityState::Canonical => 1,
        CanonicalityState::Safe => 2,
        CanonicalityState::Finalized => 3,
        CanonicalityState::Orphaned => 4,
    }
}

fn weakest_canonicality(
    states: impl Iterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    states.min_by_key(|state| canonicality_rank(*state))
}

fn chain_slot(chain_id: &str) -> String {
    if chain_id.starts_with("ethereum") {
        "ethereum".to_owned()
    } else if chain_id.starts_with("base") {
        "base".to_owned()
    } else {
        chain_id.to_owned()
    }
}

fn format_timestamp(timestamp: OffsetDateTime) -> String {
    let timestamp = timestamp.to_offset(sqlx::types::time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
    )
}

fn json_str(value: &Value, path: &[&str]) -> Option<String> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn json_i64(value: &Value, path: &[&str]) -> Option<i64> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .and_then(Value::as_i64)
}

fn dedupe_json_values(values: impl IntoIterator<Item = Value>) -> Result<Vec<Value>> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();

    for value in values {
        let key = serde_json::to_string(&value).context("failed to serialize JSON for dedupe")?;
        if seen.insert(key) {
            unique.push(value);
        }
    }

    Ok(unique)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use bigname_storage::{
        NameSurface, NormalizedEvent, RawBlock, Resource, SurfaceBinding, TokenLineage,
        default_database_url, load_name_current, upsert_name_surfaces, upsert_normalized_events,
        upsert_raw_blocks, upsert_resources, upsert_surface_bindings, upsert_token_lineages,
    };

    use super::*;

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
                .context("failed to parse database URL for worker name_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_worker_name_current_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker name_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker name_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker name_current tests")?;

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

    #[tokio::test]
    async fn rebuilds_first_registration_into_name_current() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x1100, 0x2200, 0x3300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xname", 100, 1_717_171_700),
                raw_block("ethereum-mainnet", "0xgrant", 101, 1_717_171_701),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 101, 1_717_171_701).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-1",
                    "RegistrationGranted",
                    "0xgrant",
                    101,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "epoch-1",
                    "AuthorityEpochChanged",
                    "0xgrant",
                    101,
                    None,
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                    }),
                ),
                authority_event(
                    &binding,
                    "bound-1",
                    "SurfaceBound",
                    "0xgrant",
                    101,
                    None,
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "active_from": 1_717_171_701_i64,
                        "binding_kind": "declared_registry_path",
                    }),
                ),
            ],
        )
        .await?;

        let summary = rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
        assert_eq!(summary.requested_name_count, 1);
        assert_eq!(summary.upserted_row_count, 1);

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
        assert_eq!(row.resource_id, Some(binding.resource_id));
        assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
        assert_eq!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath)
        );
        assert_eq!(
            row.declared_summary["registration"]["status"],
            Value::String("active".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["authority_kind"],
            Value::String("registrar".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["registrant"],
            Value::String("0x0000000000000000000000000000000000000aaa".to_owned())
        );
        assert_eq!(
            row.declared_summary["resolver"]["status"],
            Value::String("unsupported".to_owned())
        );
        assert_eq!(
            row.declared_summary["record_inventory"]["status"],
            Value::String("unsupported".to_owned())
        );
        assert_eq!(
            row.declared_summary["history"]["status"],
            Value::String("unsupported".to_owned())
        );
        assert_eq!(row.coverage["status"], Value::String("partial".to_owned()));
        assert_eq!(row.manifest_version, 3);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_keeps_same_binding_for_renewal_and_transfer() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x4100, 0x4200, 0x4300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 201, 1_717_171_801),
                raw_block("ethereum-mainnet", "0xrenew", 202, 1_717_171_802),
                raw_block("ethereum-mainnet", "0xtransfer", 203, 1_717_171_803),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 201, 1_717_171_801).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-2",
                    "RegistrationGranted",
                    "0xgrant",
                    201,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "renew-2",
                    "RegistrationRenewed",
                    "0xrenew",
                    202,
                    Some(1),
                    json!({
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "expiry-2",
                    "ExpiryChanged",
                    "0xrenew",
                    202,
                    Some(2),
                    json!({
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "transfer-2",
                    "TokenControlTransferred",
                    "0xtransfer",
                    203,
                    Some(0),
                    json!({
                        "from": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "to": "0x0000000000000000000000000000000000000bbb",
                    }),
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
        assert_eq!(row.resource_id, Some(binding.resource_id));
        assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
        assert_eq!(
            row.declared_summary["registration"]["expiry"],
            Value::Number(1_900_000_000_i64.into())
        );
        assert_eq!(
            row.declared_summary["registration"]["registrant"],
            Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["registrant"],
            Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_switches_to_rebound_authority_epoch_binding() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x5100, 0x5200, 0x5300);
        let rebound = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x6100, 0x6200, 0x6300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 301, 1_717_171_901),
                raw_block("ethereum-mainnet", "0xrebind", 302, 1_717_171_902),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 301, 1_717_171_901).await?;
        seed_rebound_identity(
            database.pool(),
            &binding,
            &rebound,
            "0xrebind",
            302,
            1_717_171_902,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-3",
                    "RegistrationGranted",
                    "0xgrant",
                    301,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "release-3",
                    "RegistrationReleased",
                    "0xrebind",
                    302,
                    None,
                    json!({
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "released_at": 1_717_171_902_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "epoch-3",
                    "AuthorityEpochChanged",
                    "0xrebind",
                    302,
                    None,
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                    }),
                    json!({
                        "authority_kind": "registry_only",
                        "authority_key": "registry:ethereum-mainnet:alice",
                    }),
                ),
                authority_event(
                    &binding,
                    "transfer-3",
                    "AuthorityTransferred",
                    "0xrebind",
                    302,
                    Some(0),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000ccc",
                    }),
                ),
                authority_event(
                    &binding,
                    "unbound-3",
                    "SurfaceUnbound",
                    "0xrebind",
                    302,
                    None,
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                    }),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "active_to": 1_717_171_902_i64,
                    }),
                ),
                authority_event(
                    &rebound,
                    "bound-3",
                    "SurfaceBound",
                    "0xrebind",
                    302,
                    None,
                    json!({}),
                    json!({
                        "authority_kind": "registry_only",
                        "authority_key": "registry:ethereum-mainnet:alice",
                        "active_from": 1_717_171_902_i64,
                        "binding_kind": "declared_registry_path",
                    }),
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(row.surface_binding_id, Some(rebound.surface_binding_id));
        assert_eq!(row.resource_id, Some(rebound.resource_id));
        assert_eq!(row.token_lineage_id, Some(rebound.token_lineage_id));
        assert_eq!(
            row.declared_summary["registration"]["authority_kind"],
            Value::String("registry_only".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["status"],
            Value::String("released".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["registry_owner"],
            Value::String("0x0000000000000000000000000000000000000ccc".to_owned())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_is_idempotent() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x7100, 0x7200, 0x7300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 401, 1_717_172_001),
                raw_block("ethereum-mainnet", "0xrenew", 402, 1_717_172_002),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 401, 1_717_172_001).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-4",
                    "RegistrationGranted",
                    "0xgrant",
                    401,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "renew-4",
                    "RegistrationRenewed",
                    "0xrenew",
                    402,
                    Some(1),
                    json!({
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
            ],
        )
        .await?;

        let first = rebuild_name_current(database.pool(), None).await?;
        assert_eq!(first.upserted_row_count, 1);
        let first_row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("first rebuild row must exist")?;

        let second = rebuild_name_current(database.pool(), None).await?;
        assert_eq!(second.upserted_row_count, 1);
        let second_row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("second rebuild row must exist")?;

        assert_eq!(first_row, second_row);

        database.cleanup().await
    }

    #[derive(Clone, Debug)]
    struct IdentityBinding {
        logical_name_id: String,
        display_name: String,
        token_lineage_id: Uuid,
        resource_id: Uuid,
        surface_binding_id: Uuid,
    }

    impl IdentityBinding {
        fn new(
            logical_name_id: &str,
            display_name: &str,
            token_lineage: u128,
            resource: u128,
            binding: u128,
        ) -> Self {
            Self {
                logical_name_id: logical_name_id.to_owned(),
                display_name: display_name.to_owned(),
                token_lineage_id: Uuid::from_u128(token_lineage),
                resource_id: Uuid::from_u128(resource),
                surface_binding_id: Uuid::from_u128(binding),
            }
        }
    }

    async fn seed_identity(
        pool: &PgPool,
        binding: &IdentityBinding,
        block_hash: &str,
        block_number: i64,
        block_timestamp: i64,
    ) -> Result<()> {
        upsert_token_lineages(
            pool,
            &[token_lineage(
                binding.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_resources(
            pool,
            &[resource(
                binding.resource_id,
                binding.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_name_surfaces(
            pool,
            &[name_surface(
                &binding.logical_name_id,
                &binding.display_name,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_surface_bindings(
            pool,
            &[surface_binding(
                binding,
                block_timestamp,
                None,
                block_hash,
                block_number,
            )],
        )
        .await?;
        Ok(())
    }

    async fn seed_rebound_identity(
        pool: &PgPool,
        first: &IdentityBinding,
        rebound: &IdentityBinding,
        block_hash: &str,
        block_number: i64,
        block_timestamp: i64,
    ) -> Result<()> {
        upsert_token_lineages(
            pool,
            &[token_lineage(
                rebound.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_resources(
            pool,
            &[resource(
                rebound.resource_id,
                rebound.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_surface_bindings(
            pool,
            &[
                surface_binding(
                    first,
                    1_717_171_901,
                    Some(timestamp(block_timestamp)),
                    "0xgrant",
                    301,
                ),
                surface_binding(rebound, block_timestamp, None, block_hash, block_number),
            ],
        )
        .await?;
        Ok(())
    }

    async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
        upsert_raw_blocks(pool, blocks).await?;
        Ok(())
    }

    async fn seed_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        upsert_normalized_events(pool, events).await?;
        Ok(())
    }

    fn raw_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        unix_timestamp: i64,
    ) -> RawBlock {
        RawBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: timestamp(unix_timestamp),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn token_lineage(token_lineage_id: Uuid, block_hash: &str, block_number: i64) -> TokenLineage {
        TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn resource(
        resource_id: Uuid,
        token_lineage_id: Uuid,
        block_hash: &str,
        block_number: i64,
    ) -> Resource {
        Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn name_surface(
        logical_name_id: &str,
        display_name: &str,
        block_hash: &str,
        block_number: i64,
    ) -> NameSurface {
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{display_name}"),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "name_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn surface_binding(
        binding: &IdentityBinding,
        active_from_unix: i64,
        active_to: Option<OffsetDateTime>,
        block_hash: &str,
        block_number: i64,
    ) -> SurfaceBinding {
        SurfaceBinding {
            surface_binding_id: binding.surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(active_from_unix),
            active_to,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "surface_binding"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn authority_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        event_kind: &str,
        block_hash: &str,
        block_number: i64,
        log_index: Option<i64>,
        before_state: Value,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:{event_kind}:{identity_suffix}"),
            namespace: "ens".to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: event_kind.to_owned(),
            source_family: "ens_v1_registrar_l1".to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:{identity_suffix}")),
            log_index,
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state,
            after_state,
        }
    }

    fn timestamp(value: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
    }
}
