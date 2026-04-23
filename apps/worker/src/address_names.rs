use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    AddressNameCurrentRow, AddressNameRelation, CanonicalityState, SurfaceBindingKind,
    clear_address_names_current, delete_address_names_current, upsert_address_names_current_rows,
};
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{
    PgPool, Row,
    types::time::{OffsetDateTime, UtcOffset},
};
use uuid::Uuid;

const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ADDRESS_NAMES_CURRENT_DERIVATION_KIND: &str = "address_names_current_rebuild";
const ADDRESS_NAMES_ENUMERATION_BASIS: &str = "surface_current_relations";
const ENS_V1_REGISTRAR_SOURCE_FAMILY: &str = "ens_v1_registrar_l1";
const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
const ENS_V2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENS_V2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
const BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY: &str = "basenames_base_registrar";
const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const RELEVANT_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "TokenControlTransferred",
    "AuthorityTransferred",
    "AuthorityEpochChanged",
    "TokenRegenerated",
];
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AddressNamesCurrentRebuildSummary {
    pub requested_address_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug)]
struct CurrentBindingSeed {
    logical_name_id: String,
    namespace: String,
    canonical_display_name: String,
    normalized_name: String,
    namehash: String,
    surface_chain_id: String,
    surface_block_hash: String,
    surface_block_number: i64,
    surface_block_timestamp: Option<OffsetDateTime>,
    surface_state: CanonicalityState,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    binding_kind: SurfaceBindingKind,
    binding_chain_id: String,
    binding_block_hash: String,
    binding_block_number: i64,
    binding_block_timestamp: Option<OffsetDateTime>,
    binding_state: CanonicalityState,
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
struct ProjectedRelations {
    registrant: Option<String>,
    token_holder: Option<String>,
    effective_controller: Option<String>,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    slot: String,
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: OffsetDateTime,
}

pub async fn rebuild_address_names_current(
    pool: &PgPool,
    address: Option<&str>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    match address {
        Some(address) => rebuild_one_address(pool, address).await,
        None => rebuild_all_addresses(pool).await,
    }
}

async fn rebuild_all_addresses(pool: &PgPool) -> Result<AddressNamesCurrentRebuildSummary> {
    let bindings = load_current_bindings(pool).await?;
    let rows = build_rows(pool, &bindings, None).await?;
    let requested_address_count = rows
        .iter()
        .map(|row| row.address.clone())
        .collect::<BTreeSet<_>>()
        .len();
    let upserted_row_count = upsert_address_names_current_rows(pool, &rows).await?.len();
    let deleted_row_count = delete_stale_address_names_current_rows(pool, &rows).await?;

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_address(
    pool: &PgPool,
    address: &str,
) -> Result<AddressNamesCurrentRebuildSummary> {
    let normalized_address = normalize_address(address);
    let bindings = load_current_bindings(pool).await?;
    let rows = build_rows(pool, &bindings, Some(normalized_address.as_str())).await?;
    let upserted_row_count = upsert_address_names_current_rows(pool, &rows).await?.len();
    let deleted_row_count =
        delete_stale_address_names_current_rows_for_address(pool, &normalized_address, &rows)
            .await?;

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_rows(
    pool: &PgPool,
    bindings: &[CurrentBindingSeed],
    address_filter: Option<&str>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let mut rows = Vec::new();

    for binding in bindings {
        let events = load_relevant_events(
            pool,
            &binding.namespace,
            &binding.logical_name_id,
            &binding.surface_chain_id,
        )
        .await?;
        let relations = project_relations(binding, &events);
        rows.extend(build_relation_rows(
            binding,
            &events,
            relations,
            address_filter,
        )?);
    }

    Ok(rows)
}

async fn delete_stale_address_names_current_rows(
    pool: &PgPool,
    rows: &[AddressNameCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return clear_address_names_current(pool).await;
    }

    let addresses = rows
        .iter()
        .map(|row| row.address.clone())
        .collect::<Vec<_>>();
    let logical_name_ids = rows
        .iter()
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let relations = rows
        .iter()
        .map(|row| row.relation.as_str().to_owned())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM address_names_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::TEXT[], $2::TEXT[], $3::TEXT[]) AS replacement(
                address,
                logical_name_id,
                relation
            )
            WHERE replacement.address = current.address
              AND replacement.logical_name_id = current.logical_name_id
              AND replacement.relation = current.relation
        )
        "#,
    )
    .bind(&addresses)
    .bind(&logical_name_ids)
    .bind(&relations)
    .execute(pool)
    .await
    .context("failed to delete stale address_names_current rows after rebuild")
    .map(|result| result.rows_affected())
}

async fn delete_stale_address_names_current_rows_for_address(
    pool: &PgPool,
    address: &str,
    rows: &[AddressNameCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return delete_address_names_current(pool, address).await;
    }

    let logical_name_ids = rows
        .iter()
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let relations = rows
        .iter()
        .map(|row| row.relation.as_str().to_owned())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM address_names_current current
        WHERE current.address = $1
          AND NOT EXISTS (
            SELECT 1
            FROM UNNEST($2::TEXT[], $3::TEXT[]) AS replacement(logical_name_id, relation)
            WHERE replacement.logical_name_id = current.logical_name_id
              AND replacement.relation = current.relation
          )
        "#,
    )
    .bind(address)
    .bind(&logical_name_ids)
    .bind(&relations)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete stale address_names_current rows for address {address}")
    })
    .map(|result| result.rows_affected())
}

fn build_relation_rows(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
    relations: ProjectedRelations,
    address_filter: Option<&str>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .max()
        .unwrap_or(1);
    let last_recomputed_at = max_timestamp(binding, events).unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let provenance = build_provenance(events)?;
    let coverage = json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": address_names_source_classes(&binding.namespace, events),
        "unsupported_reason": Value::Null,
        "enumeration_basis": ADDRESS_NAMES_ENUMERATION_BASIS,
    });
    let chain_positions = build_chain_positions(binding, events);
    let canonicality_summary = build_canonicality_summary(binding, events);

    let mut rows = Vec::new();
    for (relation, address) in [
        (AddressNameRelation::Registrant, relations.registrant),
        (AddressNameRelation::TokenHolder, relations.token_holder),
        (
            AddressNameRelation::EffectiveController,
            relations.effective_controller,
        ),
    ] {
        let Some(address) = address else {
            continue;
        };
        if address_filter.is_some_and(|value| value != address) {
            continue;
        }

        rows.push(AddressNameCurrentRow {
            address,
            logical_name_id: binding.logical_name_id.clone(),
            relation,
            namespace: binding.namespace.clone(),
            canonical_display_name: binding.canonical_display_name.clone(),
            normalized_name: binding.normalized_name.clone(),
            namehash: binding.namehash.clone(),
            surface_binding_id: binding.surface_binding_id,
            resource_id: binding.resource_id,
            token_lineage_id: binding.token_lineage_id,
            binding_kind: binding.binding_kind,
            provenance: provenance.clone(),
            coverage: coverage.clone(),
            chain_positions: chain_positions.clone(),
            canonicality_summary: canonicality_summary.clone(),
            manifest_version,
            last_recomputed_at,
        });
    }

    Ok(rows)
}

fn project_relations(binding: &CurrentBindingSeed, events: &[RelevantEvent]) -> ProjectedRelations {
    let mut registrant = None;
    let mut token_holder = None;
    let mut registry_owner = None;

    for event in events {
        match event.event_kind.as_str() {
            "RegistrationGranted" => {
                registrant = json_str(&event.after_state, &["registrant"]).map(normalize_address);
            }
            "TokenControlTransferred" => {
                let transferred_to = json_str(&event.after_state, &["to"]).map(normalize_address);
                registrant = transferred_to.clone();
                token_holder = transferred_to;
            }
            "AuthorityTransferred" => {
                registry_owner = json_str(&event.after_state, &["owner"]).map(normalize_address);
            }
            "AuthorityEpochChanged" | "TokenRegenerated" => {}
            _ => {}
        }
    }

    if binding.token_lineage_id.is_some() {
        let token_holder = token_holder.or_else(|| registrant.clone());
        let effective_controller = registry_owner
            .or_else(|| token_holder.clone())
            .or_else(|| registrant.clone());
        ProjectedRelations {
            registrant,
            token_holder,
            effective_controller,
        }
    } else {
        ProjectedRelations {
            registrant: None,
            token_holder: None,
            effective_controller: registry_owner,
        }
    }
}

fn build_provenance(events: &[RelevantEvent]) -> Result<Value> {
    let normalized_event_ids = events
        .iter()
        .map(|event| Value::String(event.normalized_event_id.to_string()))
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
        "derivation_kind": ADDRESS_NAMES_CURRENT_DERIVATION_KIND,
    }))
}

fn build_chain_positions(binding: &CurrentBindingSeed, events: &[RelevantEvent]) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    if let Some(timestamp) = binding.surface_block_timestamp {
        merge_chain_position(
            &mut chain_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.surface_chain_id),
                chain_id: binding.surface_chain_id.clone(),
                block_number: binding.surface_block_number,
                block_hash: binding.surface_block_hash.clone(),
                timestamp,
            },
        );
    }
    if let Some(timestamp) = binding.binding_block_timestamp {
        merge_chain_position(
            &mut chain_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.binding_chain_id),
                chain_id: binding.binding_chain_id.clone(),
                block_number: binding.binding_block_number,
                block_hash: binding.binding_block_hash.clone(),
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

        merge_chain_position(
            &mut chain_positions,
            ChainPositionCandidate {
                slot: chain_slot(chain_id),
                chain_id: chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    json!(
        chain_positions
            .into_iter()
            .map(|(slot, candidate)| {
                (
                    slot,
                    json!({
                        "chain_id": candidate.chain_id,
                        "block_number": candidate.block_number,
                        "block_hash": candidate.block_hash,
                        "timestamp": format_timestamp(candidate.timestamp),
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

fn build_canonicality_summary(binding: &CurrentBindingSeed, events: &[RelevantEvent]) -> Value {
    let status = weakest_canonicality(
        std::iter::once(binding.surface_state)
            .chain(std::iter::once(binding.binding_state))
            .chain(std::iter::once(binding.resource_state))
            .chain(binding.token_lineage_state)
            .chain(events.iter().map(|event| event.canonicality_state)),
    )
    .unwrap_or(CanonicalityState::Canonical);

    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    merge_chain_state(
        &mut chain_states,
        &binding.surface_chain_id,
        binding.surface_state,
    );
    merge_chain_state(
        &mut chain_states,
        &binding.binding_chain_id,
        binding.binding_state,
    );
    for event in events {
        if let Some(chain_id) = event.chain_id.as_deref() {
            merge_chain_state(&mut chain_states, chain_id, event.canonicality_state);
        }
    }

    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

fn merge_chain_position(
    chain_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    match chain_positions.get(&candidate.slot) {
        Some(existing)
            if existing.block_number > candidate.block_number
                || (existing.block_number == candidate.block_number
                    && existing.block_hash >= candidate.block_hash) => {}
        _ => {
            chain_positions.insert(candidate.slot.clone(), candidate);
        }
    }
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

fn max_timestamp(binding: &CurrentBindingSeed, events: &[RelevantEvent]) -> Option<OffsetDateTime> {
    let mut timestamps = Vec::new();
    if let Some(timestamp) = binding.surface_block_timestamp {
        timestamps.push(timestamp);
    }
    if let Some(timestamp) = binding.binding_block_timestamp {
        timestamps.push(timestamp);
    }
    timestamps.extend(events.iter().filter_map(|event| event.block_timestamp));
    timestamps.into_iter().max()
}

async fn load_current_bindings(pool: &PgPool) -> Result<Vec<CurrentBindingSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id AS surface_chain_id,
            ns.block_hash AS surface_block_hash,
            ns.block_number AS surface_block_number,
            surface_block.block_timestamp AS surface_block_timestamp,
            ns.canonicality_state::TEXT AS surface_state,
            sb.surface_binding_id,
            sb.resource_id,
            r.token_lineage_id,
            sb.binding_kind::TEXT AS binding_kind,
            sb.chain_id AS binding_chain_id,
            sb.block_hash AS binding_block_hash,
            sb.block_number AS binding_block_number,
            binding_block.block_timestamp AS binding_block_timestamp,
            sb.canonicality_state::TEXT AS binding_state,
            r.canonicality_state::TEXT AS resource_state,
            tl.canonicality_state::TEXT AS token_lineage_state
        FROM surface_bindings sb
        JOIN name_surfaces ns
          ON ns.logical_name_id = sb.logical_name_id
         AND ns.canonicality_state {CANONICAL_STATE_FILTER}
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN token_lineages tl
          ON tl.token_lineage_id = r.token_lineage_id
         AND tl.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN raw_blocks surface_block
          ON surface_block.chain_id = ns.chain_id
         AND surface_block.block_hash = ns.block_hash
        LEFT JOIN raw_blocks binding_block
          ON binding_block.chain_id = sb.chain_id
         AND binding_block.block_hash = sb.block_hash
        WHERE sb.active_to IS NULL
          AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY ns.logical_name_id
        "#
    ))
    .fetch_all(pool)
    .await
    .context("failed to load current bindings for address_names_current rebuild")?;

    rows.into_iter().map(decode_current_binding_seed).collect()
}

async fn load_relevant_events(
    pool: &PgPool,
    namespace: &str,
    logical_name_id: &str,
    authority_chain_id: &str,
) -> Result<Vec<RelevantEvent>> {
    let event_kinds = RELEVANT_EVENT_KINDS
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect::<Vec<_>>();
    let derivation_kinds = authority_derivation_kinds(namespace)
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let source_families = authority_source_families(namespace);

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
          AND ne.derivation_kind = ANY($3::TEXT[])
          AND ne.event_kind = ANY($4::TEXT[])
          AND ne.source_family = ANY($5::TEXT[])
          AND ne.chain_id = $6
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number NULLS FIRST,
            COALESCE(ne.log_index, 2147483647),
            ne.event_identity
        "#
    ))
    .bind(namespace)
    .bind(logical_name_id)
    .bind(&derivation_kinds)
    .bind(&event_kinds)
    .bind(&source_families)
    .bind(authority_chain_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load address-name normalized events for {logical_name_id}")
    })?;

    rows.into_iter().map(decode_relevant_event).collect()
}

fn decode_current_binding_seed(row: sqlx::postgres::PgRow) -> Result<CurrentBindingSeed> {
    Ok(CurrentBindingSeed {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_chain_id: row
            .try_get("surface_chain_id")
            .context("missing surface_chain_id")?,
        surface_block_hash: row
            .try_get("surface_block_hash")
            .context("missing surface_block_hash")?,
        surface_block_number: row
            .try_get("surface_block_number")
            .context("missing surface_block_number")?,
        surface_block_timestamp: row
            .try_get("surface_block_timestamp")
            .context("missing surface_block_timestamp")?,
        surface_state: parse_canonicality_state(
            &row.try_get::<String, _>("surface_state")
                .context("missing surface_state")?,
        )?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind: parse_surface_binding_kind(
            &row.try_get::<String, _>("binding_kind")
                .context("missing binding_kind")?,
        )?,
        binding_chain_id: row
            .try_get("binding_chain_id")
            .context("missing binding_chain_id")?,
        binding_block_hash: row
            .try_get("binding_block_hash")
            .context("missing binding_block_hash")?,
        binding_block_number: row
            .try_get("binding_block_number")
            .context("missing binding_block_number")?,
        binding_block_timestamp: row
            .try_get("binding_block_timestamp")
            .context("missing binding_block_timestamp")?,
        binding_state: parse_canonicality_state(
            &row.try_get::<String, _>("binding_state")
                .context("missing binding_state")?,
        )?,
        resource_state: parse_canonicality_state(
            &row.try_get::<String, _>("resource_state")
                .context("missing resource_state")?,
        )?,
        token_lineage_state: row
            .try_get::<Option<String>, _>("token_lineage_state")
            .context("missing token_lineage_state")?
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

fn normalize_address(value: impl AsRef<str>) -> String {
    value.as_ref().to_ascii_lowercase()
}

fn authority_source_families(namespace: &str) -> Vec<&'static str> {
    match namespace {
        "basenames" => vec![
            BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        ],
        _ => vec![
            ENS_V1_REGISTRAR_SOURCE_FAMILY,
            ENS_V1_REGISTRY_SOURCE_FAMILY,
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            ENS_V2_ROOT_SOURCE_FAMILY,
            ENS_V2_REGISTRY_SOURCE_FAMILY,
        ],
    }
}

fn authority_derivation_kinds(namespace: &str) -> Vec<&'static str> {
    match namespace {
        "basenames" => vec![ENS_V1_AUTHORITY_DERIVATION_KIND],
        _ => vec![
            ENS_V1_AUTHORITY_DERIVATION_KIND,
            ENS_V2_REGISTRY_DERIVATION_KIND,
        ],
    }
}

fn address_names_source_classes(namespace: &str, events: &[RelevantEvent]) -> Vec<&'static str> {
    if namespace == "basenames" {
        return vec!["ensv1_registry_path"];
    }

    let has_ens_v1 = events
        .iter()
        .any(|event| event.source_family.starts_with("ens_v1_"));
    let has_ens_v2 = events
        .iter()
        .any(|event| event.source_family.starts_with("ens_v2_"));

    match (has_ens_v1, has_ens_v2) {
        (false, true) => vec!["ensv2_registry_resource_surface"],
        (true, true) => vec!["ensv1_registry_path", "ensv2_registry_resource_surface"],
        _ => vec!["ensv1_registry_path"],
    }
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
    let timestamp = timestamp.to_offset(UtcOffset::UTC);
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
        default_database_url, load_address_names_current, upsert_name_surfaces,
        upsert_normalized_events, upsert_raw_blocks, upsert_resources, upsert_surface_bindings,
        upsert_token_lineages,
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
                .context("failed to parse database URL for worker address_names tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_worker_address_names_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker address_names tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker address_names test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker address_names tests")?;

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
    async fn rebuilds_current_token_holder_and_registry_controller_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let tokenized =
            IdentityBinding::new("ens:alpha.eth", "alpha.eth", Some(0x1100), 0x2200, 0x3300);
        let registry_only = IdentityBinding::new("ens:beta.eth", "beta.eth", None, 0x4400, 0x5500);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xalpha-grant", 100, 1_717_180_100),
                raw_block("ethereum-mainnet", "0xalpha-transfer", 101, 1_717_180_101),
                raw_block("ethereum-mainnet", "0xbeta-control", 102, 1_717_180_102),
            ],
        )
        .await?;
        seed_identity(
            database.pool(),
            &tokenized,
            "0xalpha-grant",
            100,
            1_717_180_100,
        )
        .await?;
        seed_identity(
            database.pool(),
            &registry_only,
            "0xbeta-control",
            102,
            1_717_180_102,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &tokenized,
                    "grant",
                    "RegistrationGranted",
                    ENS_V1_REGISTRAR_SOURCE_FAMILY,
                    "0xalpha-grant",
                    100,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alpha",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                    }),
                ),
                authority_event(
                    &tokenized,
                    "transfer",
                    "TokenControlTransferred",
                    ENS_V1_REGISTRAR_SOURCE_FAMILY,
                    "0xalpha-transfer",
                    101,
                    Some(0),
                    json!({
                        "from": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "to": "0x0000000000000000000000000000000000000bbb",
                    }),
                ),
                authority_event(
                    &registry_only,
                    "epoch",
                    "AuthorityEpochChanged",
                    ENS_V1_REGISTRY_SOURCE_FAMILY,
                    "0xbeta-control",
                    102,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registry_only",
                        "authority_key": "registry:ethereum-mainnet:beta",
                    }),
                ),
                authority_event(
                    &registry_only,
                    "owner",
                    "AuthorityTransferred",
                    ENS_V1_REGISTRY_SOURCE_FAMILY,
                    "0xbeta-control",
                    102,
                    Some(1),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000ccc",
                    }),
                ),
            ],
        )
        .await?;

        let summary = rebuild_address_names_current(database.pool(), None).await?;
        assert_eq!(summary.requested_address_count, 2);
        assert_eq!(summary.upserted_row_count, 4);

        let token_rows = load_address_names_current(
            database.pool(),
            "0x0000000000000000000000000000000000000bbb",
            None,
            None,
        )
        .await?;
        assert_eq!(token_rows.len(), 3);
        assert_eq!(
            token_rows
                .iter()
                .map(|row| row.relation)
                .collect::<Vec<_>>(),
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder,
                AddressNameRelation::EffectiveController,
            ]
        );
        assert!(
            token_rows
                .iter()
                .all(|row| row.logical_name_id == "ens:alpha.eth")
        );
        assert!(
            token_rows
                .iter()
                .all(|row| row.token_lineage_id == tokenized.token_lineage_id)
        );
        assert!(
            token_rows
                .iter()
                .all(|row| row.provenance["derivation_kind"]
                    == Value::String(ADDRESS_NAMES_CURRENT_DERIVATION_KIND.to_owned()))
        );
        assert!(
            token_rows
                .iter()
                .all(|row| row.coverage["enumeration_basis"]
                    == Value::String(ADDRESS_NAMES_ENUMERATION_BASIS.to_owned()))
        );

        let controller_rows = load_address_names_current(
            database.pool(),
            "0x0000000000000000000000000000000000000ccc",
            None,
            None,
        )
        .await?;
        assert_eq!(controller_rows.len(), 1);
        assert_eq!(
            controller_rows[0].relation,
            AddressNameRelation::EffectiveController
        );
        assert_eq!(controller_rows[0].logical_name_id, "ens:beta.eth");
        assert_eq!(controller_rows[0].token_lineage_id, None);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_one_address_refreshes_deleted_and_new_relation_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding =
            IdentityBinding::new("ens:alpha.eth", "alpha.eth", Some(0x6100), 0x6200, 0x6300);
        let old_holder = "0x0000000000000000000000000000000000000aaa";
        let new_holder = "0x0000000000000000000000000000000000000bbb";

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 200, 1_717_180_200),
                raw_block("ethereum-mainnet", "0xtransfer", 201, 1_717_180_201),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 200, 1_717_180_200).await?;
        seed_events(
            database.pool(),
            &[authority_event(
                &binding,
                "grant",
                "RegistrationGranted",
                ENS_V1_REGISTRAR_SOURCE_FAMILY,
                "0xgrant",
                200,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alpha",
                    "registrant": old_holder,
                }),
            )],
        )
        .await?;

        rebuild_address_names_current(database.pool(), None).await?;
        assert_eq!(
            load_address_names_current(database.pool(), old_holder, None, None)
                .await?
                .len(),
            3
        );

        seed_events(
            database.pool(),
            &[authority_event(
                &binding,
                "transfer",
                "TokenControlTransferred",
                ENS_V1_REGISTRAR_SOURCE_FAMILY,
                "0xtransfer",
                201,
                Some(0),
                json!({
                    "from": old_holder,
                }),
                json!({
                    "to": new_holder,
                }),
            )],
        )
        .await?;

        let old_summary = rebuild_address_names_current(database.pool(), Some(old_holder)).await?;
        assert_eq!(old_summary.deleted_row_count, 3);
        assert_eq!(old_summary.upserted_row_count, 0);
        assert!(
            load_address_names_current(database.pool(), old_holder, None, None)
                .await?
                .is_empty()
        );

        let new_summary = rebuild_address_names_current(database.pool(), Some(new_holder)).await?;
        assert_eq!(new_summary.upserted_row_count, 3);
        let new_rows = load_address_names_current(database.pool(), new_holder, None, None).await?;
        assert_eq!(new_rows.len(), 3);
        assert!(
            new_rows
                .iter()
                .all(|row| row.logical_name_id == binding.logical_name_id)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuilds_basenames_base_authority_rows_without_leaking_ignored_state() -> Result<()> {
        let database = TestDatabase::new().await?;
        let tokenized = IdentityBinding::with_namespace_and_chain(
            "basenames",
            "base-mainnet",
            "basenames:alice.base.eth",
            "alice.base.eth",
            Some(0x7100),
            0x7200,
            0x7300,
        );
        let registry_only = IdentityBinding::with_namespace_and_chain(
            "basenames",
            "base-mainnet",
            "basenames:beta.base.eth",
            "beta.base.eth",
            None,
            0x7400,
            0x7500,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xbase-grant", 400, 1_717_180_400),
                raw_block("base-mainnet", "0xbase-transfer", 401, 1_717_180_401),
                raw_block("base-mainnet", "0xbase-registry", 402, 1_717_180_402),
                raw_block("base-mainnet", "0xbase-ignored", 403, 1_717_180_403),
            ],
        )
        .await?;
        seed_identity(
            database.pool(),
            &tokenized,
            "0xbase-grant",
            400,
            1_717_180_400,
        )
        .await?;
        seed_identity(
            database.pool(),
            &registry_only,
            "0xbase-registry",
            402,
            1_717_180_402,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &tokenized,
                    "grant",
                    "RegistrationGranted",
                    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                    "0xbase-grant",
                    400,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                    }),
                ),
                authority_event(
                    &tokenized,
                    "transfer",
                    "TokenControlTransferred",
                    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                    "0xbase-transfer",
                    401,
                    Some(0),
                    json!({
                        "from": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "to": "0x0000000000000000000000000000000000000bbb",
                    }),
                ),
                ignored_event(
                    &tokenized,
                    "reverse-claim",
                    "ens_v1_reverse_claim",
                    "ignored_projection_state",
                    "AuthorityTransferred",
                    "0xbase-ignored",
                    403,
                    Some(0),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000ddd",
                    }),
                ),
                ignored_event(
                    &tokenized,
                    "transport",
                    "basenames_l1_compat_projection",
                    "ignored_projection_state",
                    "AuthorityTransferred",
                    "0xbase-ignored",
                    403,
                    Some(1),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000eee",
                    }),
                ),
                authority_event(
                    &registry_only,
                    "epoch",
                    "AuthorityEpochChanged",
                    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                    "0xbase-registry",
                    402,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registry_only",
                        "authority_key": "registry:base-mainnet:beta",
                    }),
                ),
                authority_event(
                    &registry_only,
                    "owner",
                    "AuthorityTransferred",
                    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                    "0xbase-registry",
                    402,
                    Some(1),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000ccc",
                    }),
                ),
                ignored_event(
                    &registry_only,
                    "primary-family-reuse",
                    ENS_V1_AUTHORITY_DERIVATION_KIND,
                    "basenames_base_primary",
                    "AuthorityTransferred",
                    "0xbase-ignored",
                    403,
                    Some(2),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000ddd",
                    }),
                ),
            ],
        )
        .await?;

        let summary = rebuild_address_names_current(database.pool(), None).await?;
        assert_eq!(summary.requested_address_count, 2);
        assert_eq!(summary.upserted_row_count, 4);

        let token_rows = load_address_names_current(
            database.pool(),
            "0x0000000000000000000000000000000000000bbb",
            Some("basenames"),
            None,
        )
        .await?;
        assert_eq!(token_rows.len(), 3);
        assert!(
            token_rows
                .iter()
                .all(|row| row.logical_name_id == tokenized.logical_name_id)
        );
        assert!(
            token_rows
                .iter()
                .all(|row| row.chain_positions.get("base").is_some())
        );
        assert!(token_rows.iter().all(|row| {
            row.provenance["normalized_event_ids"]
                .as_array()
                .is_some_and(|values| values.len() == 2)
        }));
        assert!(token_rows.iter().all(|row| {
            row.provenance["raw_fact_refs"]
                .as_array()
                .is_some_and(|values| values.len() == 2)
        }));

        let controller_rows = load_address_names_current(
            database.pool(),
            "0x0000000000000000000000000000000000000ccc",
            Some("basenames"),
            None,
        )
        .await?;
        assert_eq!(controller_rows.len(), 1);
        assert_eq!(
            controller_rows[0].relation,
            AddressNameRelation::EffectiveController
        );
        assert_eq!(
            controller_rows[0].logical_name_id,
            registry_only.logical_name_id
        );
        assert_eq!(controller_rows[0].token_lineage_id, None);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuilds_basenames_base_authority_control_vector_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let nft_only = IdentityBinding::with_namespace_and_chain(
            "basenames",
            "base-mainnet",
            "basenames:nft-only.base.eth",
            "nft-only.base.eth",
            Some(0x7600),
            0x7601,
            0x7602,
        );
        let management_only = IdentityBinding::with_namespace_and_chain(
            "basenames",
            "base-mainnet",
            "basenames:management-only.base.eth",
            "management-only.base.eth",
            Some(0x7610),
            0x7611,
            0x7612,
        );
        let full_transfer = IdentityBinding::with_namespace_and_chain(
            "basenames",
            "base-mainnet",
            "basenames:full-transfer.base.eth",
            "full-transfer.base.eth",
            Some(0x7620),
            0x7621,
            0x7622,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xnft-grant", 500, 1_717_181_500),
                raw_block("base-mainnet", "0xnft-manager", 501, 1_717_181_501),
                raw_block("base-mainnet", "0xnft-transfer", 502, 1_717_181_502),
                raw_block("base-mainnet", "0xmgmt-grant", 510, 1_717_181_510),
                raw_block("base-mainnet", "0xmgmt-manager", 511, 1_717_181_511),
                raw_block("base-mainnet", "0xfull-grant", 520, 1_717_181_520),
                raw_block("base-mainnet", "0xfull-manager", 521, 1_717_181_521),
                raw_block("base-mainnet", "0xfull-transfer", 522, 1_717_181_522),
                raw_block("base-mainnet", "0xfull-manager-final", 523, 1_717_181_523),
            ],
        )
        .await?;
        seed_identity(
            database.pool(),
            &nft_only,
            "0xnft-grant",
            500,
            1_717_181_500,
        )
        .await?;
        seed_identity(
            database.pool(),
            &management_only,
            "0xmgmt-grant",
            510,
            1_717_181_510,
        )
        .await?;
        seed_identity(
            database.pool(),
            &full_transfer,
            "0xfull-grant",
            520,
            1_717_181_520,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &nft_only,
                    "nft-grant",
                    "RegistrationGranted",
                    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                    "0xnft-grant",
                    500,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:nft-only",
                        "registrant": "0x00000000000000000000000000000000000000a1",
                    }),
                ),
                authority_event(
                    &nft_only,
                    "nft-manager",
                    "AuthorityTransferred",
                    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                    "0xnft-manager",
                    501,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000a1",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b1",
                    }),
                ),
                authority_event(
                    &nft_only,
                    "nft-transfer",
                    "TokenControlTransferred",
                    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                    "0xnft-transfer",
                    502,
                    Some(0),
                    json!({
                        "from": "0x00000000000000000000000000000000000000a1",
                    }),
                    json!({
                        "to": "0x00000000000000000000000000000000000000c1",
                    }),
                ),
                authority_event(
                    &management_only,
                    "mgmt-grant",
                    "RegistrationGranted",
                    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                    "0xmgmt-grant",
                    510,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:management-only",
                        "registrant": "0x00000000000000000000000000000000000000a2",
                    }),
                ),
                authority_event(
                    &management_only,
                    "mgmt-manager",
                    "AuthorityTransferred",
                    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                    "0xmgmt-manager",
                    511,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000a2",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b2",
                    }),
                ),
                authority_event(
                    &full_transfer,
                    "full-grant",
                    "RegistrationGranted",
                    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                    "0xfull-grant",
                    520,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:full-transfer",
                        "registrant": "0x00000000000000000000000000000000000000a3",
                    }),
                ),
                authority_event(
                    &full_transfer,
                    "full-manager",
                    "AuthorityTransferred",
                    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                    "0xfull-manager",
                    521,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000a3",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b3",
                    }),
                ),
                authority_event(
                    &full_transfer,
                    "full-transfer",
                    "TokenControlTransferred",
                    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                    "0xfull-transfer",
                    522,
                    Some(0),
                    json!({
                        "from": "0x00000000000000000000000000000000000000a3",
                    }),
                    json!({
                        "to": "0x00000000000000000000000000000000000000c3",
                    }),
                ),
                authority_event(
                    &full_transfer,
                    "full-manager-final",
                    "AuthorityTransferred",
                    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                    "0xfull-manager-final",
                    523,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b3",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000c3",
                    }),
                ),
            ],
        )
        .await?;

        let summary = rebuild_address_names_current(database.pool(), None).await?;
        assert_eq!(summary.requested_address_count, 5);
        assert_eq!(summary.upserted_row_count, 9);

        let nft_holder_rows = load_address_names_current(
            database.pool(),
            "0x00000000000000000000000000000000000000c1",
            Some("basenames"),
            None,
        )
        .await?;
        assert_eq!(nft_holder_rows.len(), 2);
        assert_eq!(
            nft_holder_rows
                .iter()
                .map(|row| row.relation)
                .collect::<Vec<_>>(),
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder,
            ]
        );
        assert!(
            nft_holder_rows
                .iter()
                .all(|row| row.logical_name_id == nft_only.logical_name_id)
        );
        let nft_controller_rows = load_address_names_current(
            database.pool(),
            "0x00000000000000000000000000000000000000b1",
            Some("basenames"),
            None,
        )
        .await?;
        assert_eq!(nft_controller_rows.len(), 1);
        assert_eq!(
            nft_controller_rows[0].relation,
            AddressNameRelation::EffectiveController
        );
        assert_eq!(
            nft_controller_rows[0].logical_name_id,
            nft_only.logical_name_id
        );

        let management_holder_rows = load_address_names_current(
            database.pool(),
            "0x00000000000000000000000000000000000000a2",
            Some("basenames"),
            None,
        )
        .await?;
        assert_eq!(management_holder_rows.len(), 2);
        assert_eq!(
            management_holder_rows
                .iter()
                .map(|row| row.relation)
                .collect::<Vec<_>>(),
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder,
            ]
        );
        assert!(
            management_holder_rows
                .iter()
                .all(|row| row.logical_name_id == management_only.logical_name_id)
        );
        let management_controller_rows = load_address_names_current(
            database.pool(),
            "0x00000000000000000000000000000000000000b2",
            Some("basenames"),
            None,
        )
        .await?;
        assert_eq!(management_controller_rows.len(), 1);
        assert_eq!(
            management_controller_rows[0].relation,
            AddressNameRelation::EffectiveController
        );
        assert_eq!(
            management_controller_rows[0].logical_name_id,
            management_only.logical_name_id
        );

        let full_rows = load_address_names_current(
            database.pool(),
            "0x00000000000000000000000000000000000000c3",
            Some("basenames"),
            None,
        )
        .await?;
        assert_eq!(full_rows.len(), 3);
        assert_eq!(
            full_rows.iter().map(|row| row.relation).collect::<Vec<_>>(),
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder,
                AddressNameRelation::EffectiveController,
            ]
        );
        assert!(
            full_rows
                .iter()
                .all(|row| row.logical_name_id == full_transfer.logical_name_id)
        );
        assert!(
            load_address_names_current(
                database.pool(),
                "0x00000000000000000000000000000000000000b3",
                Some("basenames"),
                None,
            )
            .await?
            .is_empty()
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuilds_ensv2_address_names_from_registry_resource_surface_events() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::with_namespace_and_chain(
            "ens",
            "ethereum-sepolia",
            "ens:bob.alice.eth",
            "bob.alice.eth",
            Some(0x9600),
            0x9700,
            0x9800,
        );
        let holder = "0x0000000000000000000000000000000000000b0b";
        let controller = "0x0000000000000000000000000000000000000c0c";

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-sepolia", "0xensv2-link", 700, 1_717_182_700),
                raw_block("ethereum-sepolia", "0xensv2-regen", 701, 1_717_182_701),
            ],
        )
        .await?;
        seed_identity(
            database.pool(),
            &binding,
            "0xensv2-link",
            700,
            1_717_182_700,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                ensv2_registry_event(
                    &binding,
                    "grant",
                    "RegistrationGranted",
                    "0xensv2-link",
                    700,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "ens_v2_registry",
                        "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                        "registrant": holder,
                        "expiry": 1_900_000_000_i64,
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    }),
                ),
                ensv2_registry_event(
                    &binding,
                    "controller",
                    "AuthorityTransferred",
                    "0xensv2-link",
                    700,
                    Some(1),
                    json!({
                        "owner": holder,
                    }),
                    json!({
                        "owner": controller,
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    }),
                ),
                ensv2_registry_event(
                    &binding,
                    "regen",
                    "TokenRegenerated",
                    "0xensv2-regen",
                    701,
                    Some(0),
                    json!({
                        "token_id": "0x01",
                    }),
                    json!({
                        "old_token_id": "0x01",
                        "new_token_id": "0x02",
                        "resource_id": binding.resource_id.to_string(),
                    }),
                ),
            ],
        )
        .await?;

        let summary = rebuild_address_names_current(database.pool(), None).await?;
        assert_eq!(summary.requested_address_count, 2);
        assert_eq!(summary.upserted_row_count, 3);

        let holder_rows =
            load_address_names_current(database.pool(), holder, Some("ens"), None).await?;
        assert_eq!(holder_rows.len(), 2);
        assert_eq!(
            holder_rows
                .iter()
                .map(|row| row.relation)
                .collect::<Vec<_>>(),
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder,
            ]
        );
        assert!(
            holder_rows
                .iter()
                .all(|row| row.logical_name_id == binding.logical_name_id)
        );
        assert!(
            holder_rows
                .iter()
                .all(|row| row.resource_id == binding.resource_id)
        );
        assert!(holder_rows.iter().all(|row| {
            row.coverage["source_classes_considered"] == json!(["ensv2_registry_resource_surface"])
        }));
        assert!(holder_rows.iter().all(|row| {
            row.provenance["normalized_event_ids"]
                .as_array()
                .is_some_and(|values| values.len() == 3)
        }));
        assert!(
            holder_rows
                .iter()
                .all(|row| row.chain_positions["ethereum"]["chain_id"] == json!("ethereum-sepolia"))
        );

        let controller_rows =
            load_address_names_current(database.pool(), controller, Some("ens"), None).await?;
        assert_eq!(controller_rows.len(), 1);
        assert_eq!(
            controller_rows[0].relation,
            AddressNameRelation::EffectiveController
        );
        assert_eq!(controller_rows[0].logical_name_id, binding.logical_name_id);

        database.cleanup().await
    }

    #[derive(Clone, Debug)]
    struct IdentityBinding {
        namespace: String,
        chain_id: String,
        logical_name_id: String,
        display_name: String,
        token_lineage_id: Option<Uuid>,
        resource_id: Uuid,
        surface_binding_id: Uuid,
    }

    impl IdentityBinding {
        fn new(
            logical_name_id: &str,
            display_name: &str,
            token_lineage: Option<u128>,
            resource: u128,
            binding: u128,
        ) -> Self {
            Self::with_namespace_and_chain(
                "ens",
                "ethereum-mainnet",
                logical_name_id,
                display_name,
                token_lineage,
                resource,
                binding,
            )
        }

        fn with_namespace_and_chain(
            namespace: &str,
            chain_id: &str,
            logical_name_id: &str,
            display_name: &str,
            token_lineage: Option<u128>,
            resource: u128,
            binding: u128,
        ) -> Self {
            Self {
                namespace: namespace.to_owned(),
                chain_id: chain_id.to_owned(),
                logical_name_id: logical_name_id.to_owned(),
                display_name: display_name.to_owned(),
                token_lineage_id: token_lineage.map(Uuid::from_u128),
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
        if let Some(token_lineage_id) = binding.token_lineage_id {
            upsert_token_lineages(
                pool,
                &[token_lineage(
                    binding,
                    token_lineage_id,
                    block_hash,
                    block_number,
                )],
            )
            .await?;
        }
        upsert_resources(
            pool,
            &[resource(
                binding,
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
                binding,
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
                block_hash,
                block_number,
            )],
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

    fn token_lineage(
        binding: &IdentityBinding,
        token_lineage_id: Uuid,
        block_hash: &str,
        block_number: i64,
    ) -> TokenLineage {
        TokenLineage {
            token_lineage_id,
            chain_id: binding.chain_id.clone(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_address_names_test", "kind": "token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn resource(
        binding: &IdentityBinding,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
        block_hash: &str,
        block_number: i64,
    ) -> Resource {
        Resource {
            resource_id,
            token_lineage_id,
            chain_id: binding.chain_id.clone(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_address_names_test", "kind": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn name_surface(
        binding: &IdentityBinding,
        logical_name_id: &str,
        display_name: &str,
        block_hash: &str,
        block_number: i64,
    ) -> NameSurface {
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: binding.namespace.clone(),
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{display_name}"),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: binding.chain_id.clone(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_address_names_test", "kind": "name_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn surface_binding(
        binding: &IdentityBinding,
        active_from_unix: i64,
        block_hash: &str,
        block_number: i64,
    ) -> SurfaceBinding {
        SurfaceBinding {
            surface_binding_id: binding.surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(active_from_unix),
            active_to: None,
            chain_id: binding.chain_id.clone(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_address_names_test", "kind": "surface_binding"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn authority_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        event_kind: &str,
        source_family: &str,
        block_hash: &str,
        block_number: i64,
        log_index: Option<i64>,
        before_state: Value,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-address-names:{event_kind}:{identity_suffix}"),
            namespace: binding.namespace.clone(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: event_kind.to_owned(),
            source_family: source_family.to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some(binding.chain_id.clone()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:{identity_suffix}")),
            log_index,
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": binding.chain_id,
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

    #[allow(clippy::too_many_arguments)]
    fn ensv2_registry_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        event_kind: &str,
        block_hash: &str,
        block_number: i64,
        log_index: Option<i64>,
        before_state: Value,
        after_state: Value,
    ) -> NormalizedEvent {
        let mut event = authority_event(
            binding,
            identity_suffix,
            event_kind,
            ENS_V2_REGISTRY_SOURCE_FAMILY,
            block_hash,
            block_number,
            log_index,
            before_state,
            after_state,
        );
        event.derivation_kind = ENS_V2_REGISTRY_DERIVATION_KIND.to_owned();
        event
    }

    #[allow(clippy::too_many_arguments)]
    fn ignored_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        derivation_kind: &str,
        source_family: &str,
        event_kind: &str,
        block_hash: &str,
        block_number: i64,
        log_index: Option<i64>,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-address-names:ignored:{event_kind}:{identity_suffix}"),
            namespace: binding.namespace.clone(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: event_kind.to_owned(),
            source_family: source_family.to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some(binding.chain_id.clone()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:ignored:{identity_suffix}")),
            log_index,
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": binding.chain_id,
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:ignored:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: derivation_kind.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state,
        }
    }

    fn timestamp(value: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
    }
}
