use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, ResolverCurrentRow, SurfaceBindingKind, clear_resolver_current,
    delete_resolver_current, upsert_resolver_current_rows,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::{OffsetDateTime, UtcOffset},
};
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const RESOLVER_CURRENT_DERIVATION_KIND: &str = "resolver_current_rebuild";
const RESOLVER_CURRENT_ENUMERATION_BASIS: &str = "resolver_overview";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolverCurrentRebuildSummary {
    pub requested_resolver_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ResolverTarget {
    chain_id: String,
    resolver_address: String,
}

#[derive(Clone, Debug)]
struct CurrentBindingSeed {
    chain_id: String,
    logical_name_id: String,
    canonical_display_name: String,
    normalized_name: String,
    namehash: String,
    resource_id: Uuid,
    surface_binding_id: Uuid,
    binding_kind: SurfaceBindingKind,
    normalized_event_id: i64,
    source_family: String,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    block_number: i64,
    block_hash: String,
    block_timestamp: Option<OffsetDateTime>,
    raw_fact_ref: Value,
    canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug)]
struct ResolverPermissionSeed {
    resource_id: Uuid,
    subject: String,
    effective_powers: Value,
    grant_source: Value,
    revocation_source: Option<Value>,
    provenance: Value,
    coverage: Value,
    chain_positions: Value,
    canonicality_summary: Value,
    manifest_version: i64,
    last_recomputed_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

pub async fn rebuild_resolver_current(
    pool: &PgPool,
    chain_id: Option<&str>,
    resolver_address: Option<&str>,
) -> Result<ResolverCurrentRebuildSummary> {
    match (chain_id, resolver_address) {
        (Some(chain_id), Some(resolver_address)) => {
            rebuild_one_resolver(pool, chain_id, resolver_address).await
        }
        (None, None) => rebuild_all_resolvers(pool).await,
        _ => bail!(
            "resolver_current rebuild requires both chain_id and resolver_address when targeting one resolver"
        ),
    }
}

async fn rebuild_all_resolvers(pool: &PgPool) -> Result<ResolverCurrentRebuildSummary> {
    let targets = load_target_resolvers(pool).await?;
    let deleted_row_count = clear_resolver_current(pool).await?;

    let mut rows = Vec::with_capacity(targets.len());
    for target in &targets {
        if let Some(row) = build_resolver_current_row(pool, target).await? {
            rows.push(row);
        }
    }

    let upserted_row_count = upsert_resolver_current_rows(pool, &rows).await?.len();
    Ok(ResolverCurrentRebuildSummary {
        requested_resolver_count: targets.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_resolver(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<ResolverCurrentRebuildSummary> {
    let target = ResolverTarget {
        chain_id: chain_id.to_owned(),
        resolver_address: normalize_resolver_address(resolver_address),
    };
    let deleted_row_count =
        delete_resolver_current(pool, &target.chain_id, &target.resolver_address).await?;

    let Some(row) = build_resolver_current_row(pool, &target).await? else {
        return Ok(ResolverCurrentRebuildSummary {
            requested_resolver_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_resolver_current_rows(pool, &[row]).await?.len();
    Ok(ResolverCurrentRebuildSummary {
        requested_resolver_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_resolver_current_row(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Option<ResolverCurrentRow>> {
    let bindings = load_current_bindings(pool, target).await?;
    let permissions = load_resolver_permissions(pool, target).await?;
    if bindings.is_empty() && permissions.is_empty() {
        return Ok(None);
    }

    let declared_summary = build_declared_summary(&bindings, &permissions);
    let provenance = build_provenance(&bindings, &permissions)?;
    let coverage = build_coverage(&bindings, &permissions);
    let chain_positions = build_chain_positions(&bindings, &permissions);
    let canonicality_summary = build_canonicality_summary(&bindings, &permissions)?;
    let manifest_version = bindings
        .iter()
        .map(|binding| binding.manifest_version)
        .chain(
            permissions
                .iter()
                .map(|permission| permission.manifest_version),
        )
        .max()
        .unwrap_or(1);
    let last_recomputed_at = bindings
        .iter()
        .filter_map(|binding| binding.block_timestamp)
        .chain(
            permissions
                .iter()
                .map(|permission| permission.last_recomputed_at),
        )
        .max()
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);

    Ok(Some(ResolverCurrentRow {
        chain_id: target.chain_id.clone(),
        resolver_address: target.resolver_address.clone(),
        declared_summary,
        provenance,
        coverage,
        chain_positions,
        canonicality_summary,
        manifest_version,
        last_recomputed_at,
    }))
}

async fn load_target_resolvers(pool: &PgPool) -> Result<Vec<ResolverTarget>> {
    let rows = sqlx::query(&format!(
        r#"
        WITH current_bindings AS (
            SELECT logical_name_id, resource_id
            FROM surface_bindings
            WHERE active_to IS NULL
              AND canonicality_state {CANONICAL_STATE_FILTER}
        ),
        latest_resolver_events AS (
            SELECT DISTINCT ON (ne.logical_name_id, ne.resource_id)
                ne.logical_name_id,
                ne.resource_id,
                ne.chain_id,
                LOWER(ne.after_state->>'resolver') AS resolver_address
            FROM normalized_events ne
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id IS NOT NULL
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.logical_name_id,
                ne.resource_id,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        )
        SELECT DISTINCT chain_id, resolver_address
        FROM (
            SELECT
                lre.chain_id,
                lre.resolver_address
            FROM latest_resolver_events lre
            INNER JOIN current_bindings cb
              ON cb.logical_name_id = lre.logical_name_id
             AND cb.resource_id = lre.resource_id
            WHERE lre.resolver_address IS NOT NULL
              AND lre.resolver_address <> ''
              AND lre.resolver_address <> $2

            UNION

            SELECT
                scope_detail->>'chain_id' AS chain_id,
                LOWER(scope_detail->>'resolver_address') AS resolver_address
            FROM permissions_current
            WHERE scope_kind = 'resolver'
              AND COALESCE(scope_detail->>'chain_id', '') <> ''
              AND COALESCE(scope_detail->>'resolver_address', '') <> ''
        ) targets
        ORDER BY chain_id, resolver_address
        "#
    ))
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(ZERO_ADDRESS)
    .fetch_all(pool)
    .await
    .context("failed to load resolver_current rebuild targets")?;

    rows.into_iter()
        .map(|row| {
            Ok(ResolverTarget {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                resolver_address: normalize_resolver_address(
                    &row.try_get::<String, _>("resolver_address")
                        .context("missing resolver_address")?,
                ),
            })
        })
        .collect()
}

async fn load_current_bindings(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<CurrentBindingSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        WITH current_bindings AS (
            SELECT
                sb.logical_name_id,
                sb.resource_id,
                sb.surface_binding_id,
                sb.binding_kind,
                ns.canonical_display_name,
                ns.normalized_name,
                ns.namehash
            FROM surface_bindings sb
            INNER JOIN name_surfaces ns
              ON ns.logical_name_id = sb.logical_name_id
             AND ns.canonicality_state {CANONICAL_STATE_FILTER}
            WHERE sb.active_to IS NULL
              AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        ),
        latest_resolver_events AS (
            SELECT DISTINCT ON (ne.logical_name_id, ne.resource_id)
                ne.logical_name_id,
                ne.resource_id,
                ne.normalized_event_id,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                rb.block_timestamp,
                ne.raw_fact_ref,
                ne.canonicality_state::TEXT AS canonicality_state,
                LOWER(ne.after_state->>'resolver') AS resolver_address
            FROM normalized_events ne
            LEFT JOIN raw_blocks rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id = $2
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.logical_name_id,
                ne.resource_id,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        )
        SELECT
            cb.logical_name_id,
            cb.canonical_display_name,
            cb.normalized_name,
            cb.namehash,
            cb.resource_id,
            cb.surface_binding_id,
            cb.binding_kind,
            lre.normalized_event_id,
            lre.source_family,
            lre.manifest_version,
            lre.source_manifest_id,
            lre.chain_id,
            lre.block_number,
            lre.block_hash,
            lre.block_timestamp,
            lre.raw_fact_ref,
            lre.canonicality_state
        FROM current_bindings cb
        INNER JOIN latest_resolver_events lre
          ON lre.logical_name_id = cb.logical_name_id
         AND lre.resource_id = cb.resource_id
        WHERE lre.resolver_address = $3
        ORDER BY cb.canonical_display_name, cb.logical_name_id, cb.surface_binding_id
        "#
    ))
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load current bindings for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(CurrentBindingSeed {
                chain_id: row.try_get("chain_id")?,
                logical_name_id: row.try_get("logical_name_id")?,
                canonical_display_name: row.try_get("canonical_display_name")?,
                normalized_name: row.try_get("normalized_name")?,
                namehash: row.try_get("namehash")?,
                resource_id: row.try_get("resource_id")?,
                surface_binding_id: row.try_get("surface_binding_id")?,
                binding_kind: parse_surface_binding_kind(
                    &row.try_get::<String, _>("binding_kind")?,
                )?,
                normalized_event_id: row.try_get("normalized_event_id")?,
                source_family: row.try_get("source_family")?,
                manifest_version: row.try_get("manifest_version")?,
                source_manifest_id: row.try_get("source_manifest_id")?,
                block_number: row.try_get("block_number")?,
                block_hash: row.try_get("block_hash")?,
                block_timestamp: row.try_get("block_timestamp")?,
                raw_fact_ref: row.try_get("raw_fact_ref")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")?,
                )?,
            })
        })
        .collect()
}

async fn load_resolver_permissions(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<ResolverPermissionSeed>> {
    let rows = sqlx::query(
        r#"
        SELECT
            resource_id,
            subject,
            effective_powers,
            grant_source,
            revocation_source,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        FROM permissions_current
        WHERE scope_kind = 'resolver'
          AND scope_detail->>'chain_id' = $1
          AND LOWER(scope_detail->>'resolver_address') = $2
        ORDER BY subject, resource_id, manifest_version
        "#,
    )
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load resolver-scoped permissions for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(ResolverPermissionSeed {
                resource_id: row.try_get("resource_id")?,
                subject: row.try_get("subject")?,
                effective_powers: row.try_get("effective_powers")?,
                grant_source: row.try_get("grant_source")?,
                revocation_source: row.try_get("revocation_source")?,
                provenance: row.try_get("provenance")?,
                coverage: row.try_get("coverage")?,
                chain_positions: row.try_get("chain_positions")?,
                canonicality_summary: row.try_get("canonicality_summary")?,
                manifest_version: row.try_get("manifest_version")?,
                last_recomputed_at: row.try_get("last_recomputed_at")?,
            })
        })
        .collect()
}

fn build_declared_summary(
    bindings: &[CurrentBindingSeed],
    permissions: &[ResolverPermissionSeed],
) -> Value {
    json!({
        "bindings": build_binding_summary(bindings.iter()),
        "aliases": build_binding_summary(
            bindings
                .iter()
                .filter(|binding| binding.binding_kind == SurfaceBindingKind::ResolverAliasPath)
        ),
        "permissions": {
            "status": "supported",
            "count": permissions.len(),
            "items": permissions
                .iter()
                .map(|permission| {
                    json!({
                        "resource_id": permission.resource_id,
                        "subject": permission.subject,
                        "effective_powers": permission.effective_powers,
                        "grant_source": permission.grant_source,
                        "revocation_source": permission.revocation_source,
                    })
                })
                .collect::<Vec<_>>(),
        },
        "role_holders": build_role_holders_summary(permissions),
        "event_summary": build_event_summary(bindings, permissions),
    })
}

fn build_binding_summary<'a>(bindings: impl Iterator<Item = &'a CurrentBindingSeed>) -> Value {
    let items = bindings.map(build_binding_item).collect::<Vec<_>>();
    json!({
        "status": "supported",
        "count": items.len(),
        "items": items,
    })
}

fn build_binding_item(binding: &CurrentBindingSeed) -> Value {
    json!({
        "logical_name_id": binding.logical_name_id,
        "canonical_display_name": binding.canonical_display_name,
        "normalized_name": binding.normalized_name,
        "namehash": binding.namehash,
        "resource_id": binding.resource_id,
        "surface_binding_id": binding.surface_binding_id,
        "binding_kind": binding.binding_kind.as_str(),
    })
}

fn build_role_holders_summary(permissions: &[ResolverPermissionSeed]) -> Value {
    let mut holders = BTreeMap::<String, (BTreeSet<String>, BTreeSet<String>)>::new();

    for permission in permissions {
        let entry = holders
            .entry(permission.subject.clone())
            .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()));
        entry.0.insert(permission.resource_id.to_string());
        for power in json_string_array(&permission.effective_powers) {
            entry.1.insert(power);
        }
    }

    json!({
        "status": "supported",
        "count": holders.len(),
        "items": holders
            .into_iter()
            .map(|(subject, (resource_ids, powers))| {
                json!({
                    "subject": subject,
                    "resource_count": resource_ids.len(),
                    "permission_row_count": resource_ids.len(),
                    "effective_powers": powers.into_iter().collect::<Vec<_>>(),
                    "resource_ids": resource_ids.into_iter().collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn build_event_summary(
    bindings: &[CurrentBindingSeed],
    permissions: &[ResolverPermissionSeed],
) -> Value {
    let resolver_changed_count = bindings.len();
    let permission_changed_count = permissions
        .iter()
        .map(|permission| {
            permission
                .provenance
                .get("normalized_event_ids")
                .and_then(Value::as_array)
                .map(|ids| ids.len())
                .unwrap_or(0)
        })
        .sum::<usize>();
    let total_count = resolver_changed_count + permission_changed_count;

    json!({
        "status": "supported",
        "count": total_count,
        "by_kind": {
            EVENT_KIND_PERMISSION_CHANGED: permission_changed_count,
            EVENT_KIND_RESOLVER_CHANGED: resolver_changed_count,
        },
    })
}

fn build_provenance(
    bindings: &[CurrentBindingSeed],
    permissions: &[ResolverPermissionSeed],
) -> Result<Value> {
    let normalized_event_ids = bindings
        .iter()
        .map(|binding| Value::Number(binding.normalized_event_id.into()))
        .chain(permissions.iter().flat_map(|permission| {
            extract_json_array(&permission.provenance, "normalized_event_ids")
        }))
        .collect::<Vec<_>>();
    let raw_fact_refs = bindings
        .iter()
        .map(|binding| binding.raw_fact_ref.clone())
        .chain(
            permissions
                .iter()
                .flat_map(|permission| extract_json_array(&permission.provenance, "raw_fact_refs")),
        )
        .collect::<Vec<_>>();
    let manifest_versions =
        bindings
            .iter()
            .map(|binding| {
                json!({
                    "source_manifest_id": binding.source_manifest_id,
                    "source_family": binding.source_family,
                    "manifest_version": binding.manifest_version,
                })
            })
            .chain(permissions.iter().flat_map(|permission| {
                extract_json_array(&permission.provenance, "manifest_versions")
            }))
            .collect::<Vec<_>>();

    Ok(json!({
        "normalized_event_ids": dedupe_json_values(normalized_event_ids)?,
        "raw_fact_refs": dedupe_json_values(raw_fact_refs)?,
        "manifest_versions": dedupe_json_values(manifest_versions)?,
        "execution_trace_id": Value::Null,
        "derivation_kind": RESOLVER_CURRENT_DERIVATION_KIND,
    }))
}

fn build_coverage(
    bindings: &[CurrentBindingSeed],
    permissions: &[ResolverPermissionSeed],
) -> Value {
    let mut source_classes = bindings
        .iter()
        .map(|binding| binding.source_family.clone())
        .collect::<BTreeSet<_>>();

    for permission in permissions {
        for value in extract_json_string_array(&permission.coverage, "source_classes_considered") {
            source_classes.insert(value);
        }
    }

    json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": source_classes.into_iter().collect::<Vec<_>>(),
        "unsupported_reason": Value::Null,
        "enumeration_basis": RESOLVER_CURRENT_ENUMERATION_BASIS,
    })
}

fn build_chain_positions(
    bindings: &[CurrentBindingSeed],
    permissions: &[ResolverPermissionSeed],
) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    for binding in bindings {
        let Some(timestamp) = binding.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            chain_id: binding.chain_id.clone(),
            block_number: binding.block_number,
            block_hash: binding.block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };
        merge_chain_position(&mut chain_positions, candidate);
    }

    for permission in permissions {
        let Some(entries) = permission.chain_positions.as_object() else {
            continue;
        };
        for entry in entries.values() {
            let Some(candidate) = decode_chain_position(entry) else {
                continue;
            };
            merge_chain_position(&mut chain_positions, candidate);
        }
    }

    json!(
        chain_positions
            .into_iter()
            .map(|(chain_id, candidate)| {
                (
                    chain_id,
                    json!({
                        "chain_id": candidate.chain_id,
                        "block_number": candidate.block_number,
                        "block_hash": candidate.block_hash,
                        "timestamp": candidate.timestamp,
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

fn build_canonicality_summary(
    bindings: &[CurrentBindingSeed],
    permissions: &[ResolverPermissionSeed],
) -> Result<Value> {
    let mut statuses = bindings
        .iter()
        .map(|binding| binding.canonicality_state)
        .collect::<Vec<_>>();
    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();

    for binding in bindings {
        merge_chain_state(
            &mut chain_states,
            binding.chain_id.clone(),
            binding.canonicality_state,
        );
    }

    for permission in permissions {
        if let Some(status) = permission
            .canonicality_summary
            .get("status")
            .and_then(Value::as_str)
        {
            statuses.push(parse_canonicality_state(status)?);
        }
        if let Some(chains) = permission
            .canonicality_summary
            .get("chains")
            .and_then(Value::as_object)
        {
            for (chain_id, value) in chains {
                let Some(state) = value.as_str() else {
                    continue;
                };
                merge_chain_state(
                    &mut chain_states,
                    chain_id.clone(),
                    parse_canonicality_state(state)?,
                );
            }
        }
    }

    let status = weakest_canonicality(statuses.into_iter()).unwrap_or(CanonicalityState::Canonical);
    Ok(json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    }))
}

fn normalize_resolver_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface binding kind {value}"),
    }
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "observed" => Ok(CanonicalityState::Observed),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}

fn weakest_canonicality(
    states: impl IntoIterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    states
        .into_iter()
        .min_by_key(|state| canonicality_rank(*state))
}

fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Canonical => 0,
        CanonicalityState::Safe => 1,
        CanonicalityState::Finalized => 2,
        CanonicalityState::Observed => 3,
        CanonicalityState::Orphaned => 4,
    }
}

fn merge_chain_state(
    chain_states: &mut BTreeMap<String, CanonicalityState>,
    chain_id: String,
    state: CanonicalityState,
) {
    let replace = chain_states
        .get(&chain_id)
        .map(|current| canonicality_rank(state) < canonicality_rank(*current))
        .unwrap_or(true);
    if replace {
        chain_states.insert(chain_id, state);
    }
}

fn merge_chain_position(
    chain_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    match chain_positions.get(&candidate.chain_id) {
        Some(existing)
            if existing.block_number > candidate.block_number
                || (existing.block_number == candidate.block_number
                    && existing.block_hash >= candidate.block_hash) => {}
        _ => {
            chain_positions.insert(candidate.chain_id.clone(), candidate);
        }
    }
}

fn decode_chain_position(value: &Value) -> Option<ChainPositionCandidate> {
    let chain_id = value.get("chain_id")?.as_str()?.to_owned();
    let block_number = value.get("block_number")?.as_i64()?;
    let block_hash = value.get("block_hash")?.as_str()?.to_owned();
    let timestamp = value.get("timestamp")?.as_str()?.to_owned();

    Some(ChainPositionCandidate {
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn extract_json_array(value: &Value, field: &str) -> Vec<Value> {
    value
        .get(field)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn extract_json_string_array(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn json_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn dedupe_json_values(values: Vec<Value>) -> Result<Vec<Value>> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();

    for value in values {
        let key = serde_json::to_string(&value).context("failed to serialize JSON value")?;
        if seen.insert(key) {
            deduped.push(value);
        }
    }

    Ok(deduped)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use anyhow::Result;
    use bigname_storage::{
        NameSurface, NormalizedEvent, PermissionsCurrentRow, RawBlock, Resource, SurfaceBinding,
        default_database_url, load_resolver_current, upsert_name_surfaces,
        upsert_normalized_events, upsert_permissions_current_rows, upsert_raw_blocks,
        upsert_resolver_current_rows, upsert_resources, upsert_surface_bindings,
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
                .context("failed to parse database URL for worker resolver_current tests")?;
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bg_wr_{}_{}_{}",
                std::process::id(),
                sequence,
                &Uuid::new_v4().simple().to_string()[..8]
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker resolver_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker resolver_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker resolver_current tests")?;

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
    async fn resolver_current_keyed_rebuild_projects_bindings_permissions_and_unsupported_aliases()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x8100);
        let surface_binding_id = Uuid::from_u128(0x8200);
        let alias_resource_id = Uuid::from_u128(0x8101);
        let alias_surface_binding_id = Uuid::from_u128(0x8201);

        seed_identity(
            database.pool(),
            "ens:alpha.eth",
            resource_id,
            surface_binding_id,
            "alpha.eth",
            SurfaceBindingKind::DeclaredRegistryPath,
        )
        .await?;
        seed_identity(
            database.pool(),
            "ens:beta.eth",
            alias_resource_id,
            alias_surface_binding_id,
            "beta.eth",
            SurfaceBindingKind::ResolverAliasPath,
        )
        .await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xres0100", 100, 1_776_200_100),
                raw_block("ethereum-mainnet", "0xres0101", 101, 1_776_200_101),
            ],
        )
        .await?;
        seed_resolver_events(
            database.pool(),
            &[
                resolver_event(
                    "resolver-alpha",
                    "ens:alpha.eth",
                    resource_id,
                    "0x0000000000000000000000000000000000000aAa",
                    100,
                    0,
                ),
                resolver_event(
                    "resolver-beta",
                    "ens:beta.eth",
                    alias_resource_id,
                    "0x0000000000000000000000000000000000000AaA",
                    100,
                    1,
                ),
            ],
        )
        .await?;
        seed_permissions(
            database.pool(),
            &[
                resolver_permission_row(
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    "ethereum-mainnet",
                    "0x0000000000000000000000000000000000000aaa",
                    json!([1, 2]),
                    json!([{
                        "kind": "raw_log",
                        "chain_id": "ethereum-mainnet",
                        "block_number": 101,
                        "log_index": 0
                    }]),
                    json!([{
                        "source_family": "ens_v1_unwrapped_authority",
                        "source_manifest_id": null,
                        "manifest_version": 7
                    }]),
                    101,
                    1_776_200_101,
                ),
                resolver_permission_row(
                    database_resource_id(1),
                    "0x0000000000000000000000000000000000000def",
                    "ethereum-mainnet",
                    "0x0000000000000000000000000000000000000aaa",
                    json!([3]),
                    json!([{
                        "kind": "raw_log",
                        "chain_id": "ethereum-mainnet",
                        "block_number": 101,
                        "log_index": 1
                    }]),
                    json!([{
                        "source_family": "ens_v1_unwrapped_authority",
                        "source_manifest_id": null,
                        "manifest_version": 8
                    }]),
                    101,
                    1_776_200_101,
                ),
            ],
        )
        .await?;

        let summary = rebuild_resolver_current(
            database.pool(),
            Some("ethereum-mainnet"),
            Some("0x0000000000000000000000000000000000000aaa"),
        )
        .await?;
        assert_eq!(summary.requested_resolver_count, 1);
        assert_eq!(summary.upserted_row_count, 1);
        assert_eq!(summary.deleted_row_count, 0);

        let row = load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000AaA",
        )
        .await?
        .context("resolver_current row should exist")?;

        assert_eq!(row.declared_summary["bindings"]["count"], json!(2));
        assert_eq!(
            row.declared_summary["bindings"]["items"][0]["logical_name_id"],
            json!("ens:alpha.eth")
        );
        assert_eq!(
            row.declared_summary["bindings"]["items"][1]["logical_name_id"],
            json!("ens:beta.eth")
        );
        assert_eq!(
            row.declared_summary["aliases"]["status"],
            json!("supported")
        );
        assert_eq!(row.declared_summary["aliases"]["count"], json!(1));
        assert_eq!(
            row.declared_summary["aliases"]["items"][0]["logical_name_id"],
            json!("ens:beta.eth")
        );
        assert_eq!(
            row.declared_summary["aliases"]["items"][0]["binding_kind"],
            json!("resolver_alias_path")
        );
        assert_eq!(
            row.declared_summary["aliases"]["items"][0],
            row.declared_summary["bindings"]["items"][1]
        );
        assert_eq!(row.declared_summary["permissions"]["count"], json!(2));
        assert_eq!(row.declared_summary["role_holders"]["count"], json!(2));
        assert_eq!(
            row.declared_summary["event_summary"]["by_kind"][EVENT_KIND_RESOLVER_CHANGED],
            json!(2)
        );
        assert_eq!(
            row.declared_summary["event_summary"]["by_kind"][EVENT_KIND_PERMISSION_CHANGED],
            json!(3)
        );
        assert_eq!(row.provenance["normalized_event_ids"], json!([1, 2, 3]));
        assert_eq!(
            row.coverage["enumeration_basis"],
            json!(RESOLVER_CURRENT_ENUMERATION_BASIS)
        );
        assert_eq!(
            row.chain_positions["ethereum-mainnet"]["block_number"],
            json!(101)
        );
        assert_eq!(row.canonicality_summary["status"], json!("finalized"));

        database.cleanup().await
    }

    #[tokio::test]
    async fn resolver_current_full_rebuild_clears_stale_rows_and_rebuilds_all_targets() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let binding_resource_id = Uuid::from_u128(0x8300);
        let binding_surface_binding_id = Uuid::from_u128(0x8301);
        let permission_only_resource_id = Uuid::from_u128(0x8302);

        seed_identity(
            database.pool(),
            "ens:beta.eth",
            binding_resource_id,
            binding_surface_binding_id,
            "beta.eth",
            SurfaceBindingKind::DeclaredRegistryPath,
        )
        .await?;
        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "ethereum-mainnet",
                "0xres0200",
                200,
                1_776_200_200,
            )],
        )
        .await?;
        seed_resolver_events(
            database.pool(),
            &[resolver_event(
                "resolver-beta",
                "ens:beta.eth",
                binding_resource_id,
                "0x0000000000000000000000000000000000000bbb",
                200,
                0,
            )],
        )
        .await?;
        seed_permissions(
            database.pool(),
            &[resolver_permission_row(
                permission_only_resource_id,
                "0x0000000000000000000000000000000000000abc",
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000ccc",
                json!([11]),
                json!([{
                    "kind": "raw_log",
                    "chain_id": "ethereum-mainnet",
                    "block_number": 210,
                    "log_index": 0
                }]),
                json!([{
                    "source_family": "permissions_current",
                    "source_manifest_id": null,
                    "manifest_version": 5
                }]),
                210,
                1_776_200_210,
            )],
        )
        .await?;
        upsert_resolver_current_rows(
            database.pool(),
            &[ResolverCurrentRow {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000bad".to_owned(),
                declared_summary: json!({"stale": true}),
                provenance: json!({"derivation_kind": RESOLVER_CURRENT_DERIVATION_KIND}),
                coverage: json!({"enumeration_basis": RESOLVER_CURRENT_ENUMERATION_BASIS}),
                chain_positions: json!({}),
                canonicality_summary: json!({"status": "finalized", "chains": {}}),
                manifest_version: 1,
                last_recomputed_at: timestamp(1_776_200_001),
            }],
        )
        .await?;

        let summary = rebuild_resolver_current(database.pool(), None, None).await?;
        assert_eq!(summary.requested_resolver_count, 2);
        assert_eq!(summary.upserted_row_count, 2);
        assert_eq!(summary.deleted_row_count, 1);

        let binding_row = load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000bbb",
        )
        .await?;
        let permission_row = load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000ccc",
        )
        .await?;
        let stale_row = load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000bad",
        )
        .await?;

        assert!(binding_row.is_some());
        assert!(permission_row.is_some());
        assert!(stale_row.is_none());
        assert_eq!(
            permission_row
                .context("permission-only resolver row should exist")?
                .declared_summary["bindings"]["count"],
            json!(0)
        );

        database.cleanup().await
    }

    async fn seed_identity(
        pool: &PgPool,
        logical_name_id: &str,
        resource_id: Uuid,
        surface_binding_id: Uuid,
        display_name: &str,
        binding_kind: SurfaceBindingKind,
    ) -> Result<()> {
        upsert_name_surfaces(pool, &[name_surface(logical_name_id, display_name)]).await?;
        upsert_resources(pool, &[resource(resource_id)]).await?;
        upsert_surface_bindings(
            pool,
            &[surface_binding(
                surface_binding_id,
                logical_name_id,
                resource_id,
                binding_kind,
            )],
        )
        .await?;
        Ok(())
    }

    async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
        upsert_raw_blocks(pool, blocks).await?;
        Ok(())
    }

    async fn seed_resolver_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        upsert_normalized_events(pool, events).await?;
        Ok(())
    }

    async fn seed_permissions(pool: &PgPool, rows: &[PermissionsCurrentRow]) -> Result<()> {
        let mut resource_ids = rows.iter().map(|row| row.resource_id).collect::<Vec<_>>();
        resource_ids.sort();
        resource_ids.dedup();
        let resources = resource_ids.into_iter().map(resource).collect::<Vec<_>>();
        upsert_resources(pool, &resources).await?;
        upsert_permissions_current_rows(pool, rows).await?;
        Ok(())
    }

    fn name_surface(logical_name_id: &str, display_name: &str) -> NameSurface {
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{display_name}"),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xsurface".to_owned(),
            block_number: 1,
            provenance: json!({"source": "worker_resolver_current_test"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn resource(resource_id: Uuid) -> Resource {
        Resource {
            resource_id,
            token_lineage_id: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: format!("0xresource{}", &resource_id.simple().to_string()[..8]),
            block_number: 10,
            provenance: json!({"source": "worker_resolver_current_test"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn surface_binding(
        surface_binding_id: Uuid,
        logical_name_id: &str,
        resource_id: Uuid,
        binding_kind: SurfaceBindingKind,
    ) -> SurfaceBinding {
        SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind,
            active_from: timestamp(1_776_200_000),
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xbind".to_owned(),
            block_number: 11,
            provenance: json!({"source": "worker_resolver_current_test"}),
            canonicality_state: CanonicalityState::Finalized,
        }
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

    fn resolver_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        resolver_address: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            source_family: "ens_v1_unwrapped_authority".to_owned(),
            manifest_version: 4,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xres{block_number:04}")),
            transaction_hash: Some(format!("0xtx{block_number:04x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "log_index": log_index
            }),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "resolver": resolver_address,
                "namehash": format!("namehash:{logical_name_id}"),
            }),
        }
    }

    fn resolver_permission_row(
        resource_id: Uuid,
        subject: &str,
        chain_id: &str,
        resolver_address: &str,
        normalized_event_ids: Value,
        raw_fact_refs: Value,
        manifest_versions: Value,
        block_number: i64,
        unix_timestamp: i64,
    ) -> PermissionsCurrentRow {
        PermissionsCurrentRow {
            resource_id,
            subject: subject.to_owned(),
            scope: bigname_storage::PermissionScope::Resolver {
                chain_id: chain_id.to_owned(),
                resolver_address: resolver_address.to_ascii_lowercase(),
            },
            effective_powers: json!(["set_resolver"]),
            grant_source: json!({"kind": "normalized_event"}),
            revocation_source: None,
            inheritance_path: json!([]),
            transfer_behavior: json!({"inherits": false}),
            provenance: json!({
                "normalized_event_ids": normalized_event_ids,
                "raw_fact_refs": raw_fact_refs,
                "manifest_versions": manifest_versions,
                "execution_trace_id": Value::Null,
                "derivation_kind": "permissions_current_rebuild",
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["permissions_current"],
                "unsupported_reason": Value::Null,
                "enumeration_basis": "resource_permissions",
            }),
            chain_positions: json!({
                chain_id: {
                    "chain_id": chain_id,
                    "block_number": block_number,
                    "block_hash": format!("0xperm{block_number:04x}"),
                    "timestamp": format_timestamp(timestamp(unix_timestamp)),
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    chain_id: "finalized",
                }
            }),
            manifest_version: 9,
            last_recomputed_at: timestamp(unix_timestamp),
        }
    }

    fn database_resource_id(offset: u128) -> Uuid {
        Uuid::from_u128(0x8f00 + offset)
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }
}
