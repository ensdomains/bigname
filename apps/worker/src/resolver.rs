use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, PermissionsCurrentRow, ResolverCurrentRow, SurfaceBindingKind,
    clear_resolver_current, delete_resolver_current, load_permissions_current_for_resolver_scope,
    load_permissions_current_resolver_targets, upsert_resolver_current_rows,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::{OffsetDateTime, UtcOffset},
};
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const BASENAMES_NAMESPACE: &str = "basenames";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const RESOLVER_CURRENT_DERIVATION_KIND: &str = "resolver_current_rebuild";
const RESOLVER_CURRENT_ENUMERATION_BASIS: &str = "resolver_overview";
const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
const RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION: &str = "resolver_authorization";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD: &str = "resolver_record";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION: &str = "resolver_record_version";
const RESOLVER_FAMILY_PENDING_REASON: &str = "resolver_family_pending";
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
struct AliasSeed {
    chain_id: String,
    resolver_address: String,
    normalized_event_id: i64,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    source_family: String,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    block_number: i64,
    block_hash: String,
    block_timestamp: Option<OffsetDateTime>,
    raw_fact_ref: Value,
    canonicality_state: CanonicalityState,
    after_state: Value,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

#[derive(Clone, Debug)]
struct ResolverProfileGate {
    admissions: BTreeMap<(String, String, String, String), String>,
}

impl ResolverProfileGate {
    async fn load(pool: &PgPool) -> Result<Self> {
        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions(pool)
                .await
                .context("failed to load ENSv1 PublicResolver profile admissions")?
                .into_iter()
                .collect::<Vec<_>>();
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions(pool)
                .await
                .context("failed to load Basenames L2Resolver profile admissions")?,
        );

        let admissions = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_for_source_family(&admission.source_family)
                    .is_some_and(|profile| admission.profile == profile)
            })
            .map(|admission| {
                (
                    (
                        admission.chain,
                        admission.source_family,
                        normalize_resolver_address(&admission.address),
                        admission.fact_family,
                    ),
                    admission.status,
                )
            })
            .collect();

        Ok(Self { admissions })
    }

    fn target_status(&self, target: &ResolverTarget, source_family: &str) -> &str {
        for &fact_family in resolver_overview_fact_families(source_family) {
            let Some(status) = self.admissions.get(&(
                target.chain_id.clone(),
                source_family.to_owned(),
                target.resolver_address.clone(),
                fact_family.to_owned(),
            )) else {
                return RESOLVER_PROFILE_STATUS_PENDING;
            };
            if status != RESOLVER_PROFILE_STATUS_SUPPORTED {
                return status.as_str();
            }
        }

        RESOLVER_PROFILE_STATUS_SUPPORTED
    }
}

fn resolver_profile_for_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE),
        _ => None,
    }
}

fn resolver_source_family_for_binding(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

fn resolver_profile_source_family_for_bindings(
    bindings: &[CurrentBindingSeed],
) -> Option<&'static str> {
    let mut source_families = bindings
        .iter()
        .filter_map(|binding| resolver_source_family_for_binding(&binding.source_family))
        .collect::<BTreeSet<_>>();
    if source_families.len() == 1 {
        source_families.pop_first()
    } else {
        None
    }
}

fn resolver_overview_fact_families(source_family: &str) -> &'static [&'static str] {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => &[
            RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION,
            RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION,
        ],
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => &[
            RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION,
            RESOLVER_PROFILE_FACT_FAMILY_RECORD,
        ],
        _ => &[],
    }
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
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let targets = load_target_resolvers(pool).await?;
    let deleted_row_count = clear_resolver_current(pool).await?;

    let mut rows = Vec::with_capacity(targets.len());
    for target in &targets {
        if let Some(row) = build_resolver_current_row(pool, &profile_gate, target).await? {
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
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let target = ResolverTarget {
        chain_id: chain_id.to_owned(),
        resolver_address: normalize_resolver_address(resolver_address),
    };
    let deleted_row_count =
        delete_resolver_current(pool, &target.chain_id, &target.resolver_address).await?;

    let Some(row) = build_resolver_current_row(pool, &profile_gate, &target).await? else {
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
    profile_gate: &ResolverProfileGate,
    target: &ResolverTarget,
) -> Result<Option<ResolverCurrentRow>> {
    let bindings = load_current_bindings(pool, target).await?;
    let aliases = load_alias_events(pool, target).await?;
    let permissions = load_resolver_permissions(pool, target).await?;
    if bindings.is_empty() && aliases.is_empty() && permissions.is_empty() {
        return Ok(None);
    }

    let provenance = build_provenance(&bindings, &aliases, &permissions)?;
    let chain_positions = build_chain_positions(&bindings, &aliases, &permissions);
    let canonicality_summary = build_canonicality_summary(&bindings, &aliases, &permissions)?;
    let manifest_version = bindings
        .iter()
        .map(|binding| binding.manifest_version)
        .chain(aliases.iter().map(|alias| alias.manifest_version))
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
        .chain(aliases.iter().filter_map(|alias| alias.block_timestamp))
        .chain(
            permissions
                .iter()
                .map(|permission| permission.last_recomputed_at),
        )
        .max()
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let target_status = resolver_profile_source_family_for_bindings(&bindings)
        .map(|source_family| profile_gate.target_status(target, source_family))
        .unwrap_or(RESOLVER_PROFILE_STATUS_SUPPORTED);
    let (declared_summary, coverage) = if target_status != RESOLVER_PROFILE_STATUS_SUPPORTED {
        (
            build_unsupported_declared_summary(RESOLVER_FAMILY_PENDING_REASON),
            build_unsupported_coverage(&bindings, &aliases, &permissions),
        )
    } else {
        (
            build_declared_summary(&bindings, &aliases, &permissions),
            build_coverage(&bindings, &aliases, &permissions),
        )
    };

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
        ) targets
        ORDER BY chain_id, resolver_address
        "#
    ))
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(ZERO_ADDRESS)
    .fetch_all(pool)
    .await
    .context("failed to load resolver_current rebuild targets")?;

    let mut targets = rows
        .into_iter()
        .map(|row| {
            Ok(ResolverTarget {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                resolver_address: normalize_resolver_address(
                    &row.try_get::<String, _>("resolver_address")
                        .context("missing resolver_address")?,
                ),
            })
        })
        .collect::<Result<BTreeSet<_>>>()?;

    for (chain_id, resolver_address) in load_permissions_current_resolver_targets(pool).await? {
        targets.insert(ResolverTarget {
            chain_id,
            resolver_address,
        });
    }
    for target in load_alias_target_resolvers(pool).await? {
        targets.insert(target);
    }

    Ok(targets.into_iter().collect())
}

async fn load_alias_target_resolvers(pool: &PgPool) -> Result<Vec<ResolverTarget>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT
            chain_id,
            LOWER(after_state->>'resolver') AS resolver_address
        FROM normalized_events
        WHERE event_kind = $1
          AND chain_id IS NOT NULL
          AND after_state->>'resolver' IS NOT NULL
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY chain_id, resolver_address
        "#
    ))
    .bind(EVENT_KIND_ALIAS_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to load AliasChanged resolver_current rebuild targets")?;

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
) -> Result<Vec<PermissionsCurrentRow>> {
    let mut rows = load_permissions_current_for_resolver_scope(
        pool,
        &target.chain_id,
        &target.resolver_address,
    )
    .await?;
    rows.sort_by(|left, right| {
        left.subject
            .cmp(&right.subject)
            .then_with(|| left.resource_id.cmp(&right.resource_id))
            .then_with(|| left.manifest_version.cmp(&right.manifest_version))
    });
    Ok(rows)
}

async fn load_alias_events(pool: &PgPool, target: &ResolverTarget) -> Result<Vec<AliasSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (ne.after_state->>'from_dns_encoded_name')
            ne.normalized_event_id,
            ne.logical_name_id,
            ne.resource_id,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state,
            LOWER(ne.after_state->>'resolver') AS resolver_address
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.event_kind = $1
          AND ne.chain_id = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND LOWER(ne.after_state->>'resolver') = $3
        ORDER BY
            ne.after_state->>'from_dns_encoded_name',
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#
    ))
    .bind(EVENT_KIND_ALIAS_CHANGED)
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load AliasChanged events for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(AliasSeed {
                chain_id: row.try_get("chain_id")?,
                resolver_address: normalize_resolver_address(
                    &row.try_get::<String, _>("resolver_address")?,
                ),
                normalized_event_id: row.try_get("normalized_event_id")?,
                logical_name_id: row.try_get("logical_name_id")?,
                resource_id: row.try_get("resource_id")?,
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
                after_state: row.try_get("after_state")?,
            })
        })
        .collect()
}

fn build_declared_summary(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    json!({
        "bindings": build_binding_summary(bindings.iter()),
        "aliases": build_alias_summary(bindings, aliases),
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
        "event_summary": build_event_summary(bindings, aliases, permissions),
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

fn build_alias_summary(bindings: &[CurrentBindingSeed], aliases: &[AliasSeed]) -> Value {
    let mut items = bindings
        .iter()
        .filter(|binding| binding.binding_kind == SurfaceBindingKind::ResolverAliasPath)
        .map(build_binding_item)
        .collect::<Vec<_>>();
    items.extend(aliases.iter().map(build_alias_item));
    items.sort_by(|left, right| {
        left.get("logical_name_id")
            .and_then(Value::as_str)
            .cmp(&right.get("logical_name_id").and_then(Value::as_str))
            .then(
                left.get("from_dns_encoded_name")
                    .and_then(Value::as_str)
                    .cmp(&right.get("from_dns_encoded_name").and_then(Value::as_str)),
            )
    });
    json!({
        "status": "supported",
        "count": items.len(),
        "items": items,
    })
}

fn build_alias_item(alias: &AliasSeed) -> Value {
    json!({
        "logical_name_id": alias.logical_name_id,
        "resource_id": alias.resource_id,
        "binding_kind": "resolver_alias_path",
        "alias_state": alias.after_state.get("alias_state").cloned().unwrap_or_else(|| json!("active")),
        "active": alias.after_state.get("active").cloned().unwrap_or_else(|| json!(true)),
        "chain_id": alias.chain_id,
        "resolver_address": alias.resolver_address,
        "from_dns_encoded_name": alias.after_state.get("from_dns_encoded_name").cloned().unwrap_or(Value::Null),
        "to_dns_encoded_name": alias.after_state.get("to_dns_encoded_name").cloned().unwrap_or(Value::Null),
        "from_name": alias.after_state.get("from_name").cloned().unwrap_or(Value::Null),
        "to_name": alias.after_state.get("to_name").cloned().unwrap_or(Value::Null),
        "to_logical_name_id": alias.after_state.get("to_logical_name_id").cloned().unwrap_or(Value::Null),
        "to_resource_id": alias.after_state.get("to_resource_id").cloned().unwrap_or(Value::Null),
        "latest_event_kind": EVENT_KIND_ALIAS_CHANGED,
    })
}

fn build_role_holders_summary(permissions: &[PermissionsCurrentRow]) -> Value {
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
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let resolver_changed_count = bindings.len();
    let alias_changed_count = aliases.len();
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
    let total_count = resolver_changed_count + alias_changed_count + permission_changed_count;
    let mut by_kind = serde_json::Map::new();
    if alias_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_ALIAS_CHANGED.to_owned(),
            Value::Number(alias_changed_count.into()),
        );
    }
    if permission_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_PERMISSION_CHANGED.to_owned(),
            Value::Number(permission_changed_count.into()),
        );
    }
    if resolver_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            Value::Number(resolver_changed_count.into()),
        );
    }

    json!({
        "status": "supported",
        "count": total_count,
        "by_kind": by_kind,
    })
}

fn build_unsupported_declared_summary(unsupported_reason: &str) -> Value {
    json!({
        "bindings": unsupported_summary(unsupported_reason),
        "aliases": unsupported_summary(unsupported_reason),
        "permissions": unsupported_summary(unsupported_reason),
        "role_holders": unsupported_summary(unsupported_reason),
        "event_summary": unsupported_summary(unsupported_reason),
    })
}

fn unsupported_summary(unsupported_reason: &str) -> Value {
    json!({
        "status": "unsupported",
        "unsupported_reason": unsupported_reason,
    })
}

fn build_provenance(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Result<Value> {
    let normalized_event_ids = bindings
        .iter()
        .map(|binding| Value::Number(binding.normalized_event_id.into()))
        .chain(
            aliases
                .iter()
                .map(|alias| Value::Number(alias.normalized_event_id.into())),
        )
        .chain(permissions.iter().flat_map(|permission| {
            extract_json_array(&permission.provenance, "normalized_event_ids")
        }))
        .collect::<Vec<_>>();
    let raw_fact_refs = bindings
        .iter()
        .map(|binding| binding.raw_fact_ref.clone())
        .chain(aliases.iter().map(|alias| alias.raw_fact_ref.clone()))
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
            .chain(aliases.iter().map(|alias| {
                json!({
                    "source_manifest_id": alias.source_manifest_id,
                    "source_family": alias.source_family,
                    "manifest_version": alias.manifest_version,
                })
            }))
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
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let mut source_classes = bindings
        .iter()
        .map(|binding| binding.source_family.clone())
        .collect::<BTreeSet<_>>();

    source_classes.extend(aliases.iter().map(|alias| alias.source_family.clone()));

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

fn build_unsupported_coverage(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let mut coverage = build_coverage(bindings, aliases, permissions);
    coverage["status"] = json!("partial");
    coverage["exhaustiveness"] = json!("best_effort");
    coverage["unsupported_reason"] = json!(RESOLVER_FAMILY_PENDING_REASON);
    coverage
}

fn build_chain_positions(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
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

    for alias in aliases {
        let Some(timestamp) = alias.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            chain_id: alias.chain_id.clone(),
            block_number: alias.block_number,
            block_hash: alias.block_hash.clone(),
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
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
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

    for alias in aliases {
        statuses.push(alias.canonicality_state);
        merge_chain_state(
            &mut chain_states,
            alias.chain_id.clone(),
            alias.canonicality_state,
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
        NameSurface, NormalizedEvent, RawBlock, RawCodeHash, Resource, SurfaceBinding,
        default_database_url, load_resolver_current, upsert_name_surfaces,
        upsert_normalized_events, upsert_raw_blocks, upsert_raw_code_hashes,
        upsert_resolver_current_rows, upsert_resources, upsert_surface_bindings,
    };

    use super::*;
    use crate::permissions::rebuild_permissions_current;

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
        let resolver_contract_instance_id = Uuid::from_u128(0x8202);
        let resolver_address = "0x0000000000000000000000000000000000000aaa";

        let resolver_manifest_id = insert_manifest_version(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "manifests/ens/ens_v1_resolver_l1/v1.toml",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            resolver_contract_instance_id,
            resolver_address,
            resolver_manifest_id,
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            resolver_manifest_id,
            "public_resolver",
            resolver_contract_instance_id,
            resolver_address,
        )
        .await?;

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
                    resolver_address,
                    100,
                    0,
                ),
                resolver_event(
                    "resolver-beta",
                    "ens:beta.eth",
                    alias_resource_id,
                    resolver_address,
                    100,
                    1,
                ),
            ],
        )
        .await?;
        seed_permission_events(
            database.pool(),
            &[
                resolver_permission_event(
                    "permission-alpha-1",
                    Some("ens:alpha.eth"),
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    "ethereum-mainnet",
                    "0x0000000000000000000000000000000000000aaa",
                    json!(["set_resolver"]),
                    101,
                    0,
                ),
                resolver_permission_event(
                    "permission-alpha-2",
                    Some("ens:alpha.eth"),
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    "ethereum-mainnet",
                    "0x0000000000000000000000000000000000000aaa",
                    json!(["set_resolver", "set_records"]),
                    101,
                    1,
                ),
                resolver_permission_event(
                    "permission-only",
                    None,
                    database_resource_id(1),
                    "0x0000000000000000000000000000000000000def",
                    "ethereum-mainnet",
                    "0x0000000000000000000000000000000000000aaa",
                    json!(["set_resolver"]),
                    101,
                    2,
                ),
            ],
        )
        .await?;
        rebuild_permissions_current(database.pool(), None).await?;

        let summary = rebuild_resolver_current(
            database.pool(),
            Some("ethereum-mainnet"),
            Some(resolver_address),
        )
        .await?;
        assert_eq!(summary.requested_resolver_count, 1);
        assert_eq!(summary.upserted_row_count, 1);
        assert_eq!(summary.deleted_row_count, 0);

        let row = load_resolver_current(database.pool(), "ethereum-mainnet", resolver_address)
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
        assert_eq!(
            row.provenance["normalized_event_ids"],
            json!([1, 2, 3, 4, 5])
        );
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
    async fn resolver_current_projects_latest_ensv2_alias_tombstone() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resolver_address = "0x0000000000000000000000000000000000000aaa";
        let target_resource_id = Uuid::from_u128(0x8102);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xres0300", 300, 1_776_200_300),
                raw_block("ethereum-mainnet", "0xres0301", 301, 1_776_200_301),
            ],
        )
        .await?;
        upsert_resources(database.pool(), &[resource(target_resource_id)]).await?;
        upsert_normalized_events(
            database.pool(),
            &[
                alias_event(
                    "alias-active",
                    Some("ens:from.eth"),
                    Some(target_resource_id),
                    resolver_address,
                    "0x0466726f6d0365746800",
                    "0x02746f0365746800",
                    Some("from.eth"),
                    Some("to.eth"),
                    "active",
                    300,
                    0,
                ),
                alias_event(
                    "alias-removed",
                    Some("ens:from.eth"),
                    None,
                    resolver_address,
                    "0x0466726f6d0365746800",
                    "0x",
                    Some("from.eth"),
                    None,
                    "removed",
                    301,
                    0,
                ),
            ],
        )
        .await?;

        let summary = rebuild_resolver_current(
            database.pool(),
            Some("ethereum-mainnet"),
            Some(resolver_address),
        )
        .await?;
        assert_eq!(summary.upserted_row_count, 1);

        let row = load_resolver_current(database.pool(), "ethereum-mainnet", resolver_address)
            .await?
            .context("resolver_current row should exist")?;
        assert_eq!(row.declared_summary["aliases"]["count"], json!(1));
        assert_eq!(
            row.declared_summary["aliases"]["items"][0]["alias_state"],
            json!("removed")
        );
        assert_eq!(
            row.declared_summary["aliases"]["items"][0]["active"],
            json!(false)
        );
        assert_eq!(
            row.declared_summary["aliases"]["items"][0]["to_dns_encoded_name"],
            json!("0x")
        );
        assert_eq!(row.provenance["normalized_event_ids"], json!([2]));

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
            &[
                raw_block("ethereum-mainnet", "0xres0200", 200, 1_776_200_200),
                raw_block("ethereum-mainnet", "0xres0210", 210, 1_776_200_210),
            ],
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
        seed_permission_events(
            database.pool(),
            &[resolver_permission_event(
                "permission-only-target",
                None,
                permission_only_resource_id,
                "0x0000000000000000000000000000000000000abc",
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000ccc",
                json!(["set_resolver"]),
                210,
                0,
            )],
        )
        .await?;
        rebuild_permissions_current(database.pool(), None).await?;
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

    #[tokio::test]
    async fn resolver_current_keeps_pending_ensv1_dynamic_resolver_sections_unsupported()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x8400);
        let surface_binding_id = Uuid::from_u128(0x8401);
        let registry_contract_instance_id = Uuid::from_u128(0x8402);
        let public_resolver_contract_instance_id = Uuid::from_u128(0x8403);
        let registry_address = "0x0000000000000000000000000000000000008402";
        let public_resolver_address = "0x0000000000000000000000000000000000008403";
        let pending_resolver_address = "0x0000000000000000000000000000000000008404";

        let registry_manifest_id = insert_manifest_version(
            database.pool(),
            "ens_v1_registry_l1",
            "manifests/ens/ens_v1_registry_l1/v2.toml",
        )
        .await?;
        let resolver_manifest_id = insert_manifest_version(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "manifests/ens/ens_v1_resolver_l1/v1.toml",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            registry_contract_instance_id,
            registry_address,
            registry_manifest_id,
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            public_resolver_contract_instance_id,
            public_resolver_address,
            resolver_manifest_id,
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            resolver_manifest_id,
            "public_resolver",
            public_resolver_contract_instance_id,
            public_resolver_address,
        )
        .await?;

        seed_identity(
            database.pool(),
            "ens:pending.eth",
            resource_id,
            surface_binding_id,
            "pending.eth",
            SurfaceBindingKind::DeclaredRegistryPath,
        )
        .await?;
        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "ethereum-mainnet",
                "0xres0400",
                400,
                1_776_200_400,
            )],
        )
        .await?;
        seed_resolver_events(
            database.pool(),
            &[resolver_event_with_manifest(
                "pending-resolver",
                "ens:pending.eth",
                resource_id,
                pending_resolver_address,
                "ens_v1_registry_l1",
                registry_manifest_id,
                400,
                0,
            )],
        )
        .await?;

        let summary = rebuild_resolver_current(
            database.pool(),
            Some("ethereum-mainnet"),
            Some(pending_resolver_address),
        )
        .await?;
        assert_eq!(summary.requested_resolver_count, 1);
        assert_eq!(summary.upserted_row_count, 1);

        let row = load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            pending_resolver_address,
        )
        .await?
        .context("pending resolver_current row should exist")?;
        for section in [
            "bindings",
            "aliases",
            "permissions",
            "role_holders",
            "event_summary",
        ] {
            assert_eq!(
                row.declared_summary[section]["status"],
                json!("unsupported")
            );
            assert_eq!(
                row.declared_summary[section]["unsupported_reason"],
                json!(RESOLVER_FAMILY_PENDING_REASON)
            );
        }
        assert_eq!(
            row.coverage["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
        assert_eq!(row.provenance["normalized_event_ids"], json!([1]));

        database.cleanup().await
    }

    #[tokio::test]
    async fn resolver_current_keeps_unadmitted_basenames_dynamic_resolver_sections_unsupported()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x8500);
        let surface_binding_id = Uuid::from_u128(0x8501);
        let resolver_address = "0x0000000000000000000000000000000000008502";

        seed_basenames_identity(
            database.pool(),
            "basenames:pending.base.eth",
            resource_id,
            surface_binding_id,
            "pending.base.eth",
            SurfaceBindingKind::DeclaredRegistryPath,
        )
        .await?;
        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "base-mainnet",
                "0xbase-res0400",
                400,
                1_776_200_400,
            )],
        )
        .await?;
        seed_resolver_events(
            database.pool(),
            &[basenames_resolver_event(
                "base-pending-resolver",
                "basenames:pending.base.eth",
                resource_id,
                resolver_address,
                400,
                0,
            )],
        )
        .await?;

        let summary = rebuild_resolver_current(
            database.pool(),
            Some("base-mainnet"),
            Some(resolver_address),
        )
        .await?;
        assert_eq!(summary.requested_resolver_count, 1);
        assert_eq!(summary.upserted_row_count, 1);

        let row = load_resolver_current(database.pool(), "base-mainnet", resolver_address)
            .await?
            .context("unadmitted Basenames resolver_current row should exist")?;
        for section in [
            "bindings",
            "aliases",
            "permissions",
            "role_holders",
            "event_summary",
        ] {
            assert_eq!(
                row.declared_summary[section]["status"],
                json!("unsupported")
            );
            assert_eq!(
                row.declared_summary[section]["unsupported_reason"],
                json!(RESOLVER_FAMILY_PENDING_REASON)
            );
        }
        assert_eq!(
            row.coverage["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
        assert_eq!(row.provenance["normalized_event_ids"], json!([1]));

        database.cleanup().await
    }

    #[tokio::test]
    async fn resolver_current_basenames_dynamic_resolver_gates_supported_pending_and_unsupported_targets()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let supported_resource_id = Uuid::from_u128(0x8600);
        let pending_resource_id = Uuid::from_u128(0x8601);
        let unsupported_resource_id = Uuid::from_u128(0x8602);
        let supported_surface_binding_id = Uuid::from_u128(0x8610);
        let pending_surface_binding_id = Uuid::from_u128(0x8611);
        let unsupported_surface_binding_id = Uuid::from_u128(0x8612);
        let seed_resolver_contract_instance_id = Uuid::from_u128(0x8620);
        let supported_resolver_contract_instance_id = Uuid::from_u128(0x8621);
        let pending_resolver_contract_instance_id = Uuid::from_u128(0x8622);
        let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x8623);
        let seed_resolver_address = "0x0000000000000000000000000000000000008620";
        let supported_resolver_address = "0x0000000000000000000000000000000000008621";
        let pending_resolver_address = "0x0000000000000000000000000000000000008622";
        let unsupported_resolver_address = "0x0000000000000000000000000000000000008623";

        insert_basenames_dynamic_resolver_profile_fixture(
            database.pool(),
            seed_resolver_contract_instance_id,
            seed_resolver_address,
            &[
                (
                    supported_resolver_contract_instance_id,
                    supported_resolver_address,
                ),
                (
                    pending_resolver_contract_instance_id,
                    pending_resolver_address,
                ),
                (
                    unsupported_resolver_contract_instance_id,
                    unsupported_resolver_address,
                ),
            ],
            &[
                (supported_resolver_address, Some(BASENAMES_L2_CODE_HASH)),
                (pending_resolver_address, None),
                (unsupported_resolver_address, Some(UNSUPPORTED_CODE_HASH)),
            ],
        )
        .await?;
        seed_basenames_identity(
            database.pool(),
            "basenames:supported.base.eth",
            supported_resource_id,
            supported_surface_binding_id,
            "supported.base.eth",
            SurfaceBindingKind::DeclaredRegistryPath,
        )
        .await?;
        seed_basenames_identity(
            database.pool(),
            "basenames:pending.base.eth",
            pending_resource_id,
            pending_surface_binding_id,
            "pending.base.eth",
            SurfaceBindingKind::DeclaredRegistryPath,
        )
        .await?;
        seed_basenames_identity(
            database.pool(),
            "basenames:unsupported.base.eth",
            unsupported_resource_id,
            unsupported_surface_binding_id,
            "unsupported.base.eth",
            SurfaceBindingKind::DeclaredRegistryPath,
        )
        .await?;
        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "base-mainnet",
                "0xbase-res0500",
                500,
                1_776_200_500,
            )],
        )
        .await?;
        seed_resolver_events(
            database.pool(),
            &[
                basenames_resolver_event(
                    "base-supported-resolver",
                    "basenames:supported.base.eth",
                    supported_resource_id,
                    supported_resolver_address,
                    500,
                    0,
                ),
                basenames_resolver_event(
                    "base-pending-resolver",
                    "basenames:pending.base.eth",
                    pending_resource_id,
                    pending_resolver_address,
                    500,
                    1,
                ),
                basenames_resolver_event(
                    "base-unsupported-resolver",
                    "basenames:unsupported.base.eth",
                    unsupported_resource_id,
                    unsupported_resolver_address,
                    500,
                    2,
                ),
            ],
        )
        .await?;

        for resolver_address in [
            supported_resolver_address,
            pending_resolver_address,
            unsupported_resolver_address,
        ] {
            let summary = rebuild_resolver_current(
                database.pool(),
                Some("base-mainnet"),
                Some(resolver_address),
            )
            .await?;
            assert_eq!(summary.requested_resolver_count, 1);
            assert_eq!(summary.upserted_row_count, 1);
        }

        let supported_row =
            load_resolver_current(database.pool(), "base-mainnet", supported_resolver_address)
                .await?
                .context("supported Basenames resolver_current row should exist")?;
        assert_eq!(
            supported_row.declared_summary["bindings"]["status"],
            json!("supported")
        );
        assert_eq!(
            supported_row.declared_summary["bindings"]["count"],
            json!(1)
        );
        assert_eq!(
            supported_row.declared_summary["bindings"]["items"][0]["logical_name_id"],
            json!("basenames:supported.base.eth")
        );
        assert_eq!(supported_row.coverage["unsupported_reason"], Value::Null);

        for (resolver_address, logical_name_id) in [
            (pending_resolver_address, "basenames:pending.base.eth"),
            (
                unsupported_resolver_address,
                "basenames:unsupported.base.eth",
            ),
        ] {
            let row = load_resolver_current(database.pool(), "base-mainnet", resolver_address)
                .await?
                .with_context(|| format!("{logical_name_id} resolver_current row should exist"))?;
            for section in [
                "bindings",
                "aliases",
                "permissions",
                "role_holders",
                "event_summary",
            ] {
                assert_eq!(
                    row.declared_summary[section]["status"],
                    json!("unsupported")
                );
                assert_eq!(
                    row.declared_summary[section]["unsupported_reason"],
                    json!(RESOLVER_FAMILY_PENDING_REASON)
                );
            }
            assert_eq!(
                row.coverage["unsupported_reason"],
                json!(RESOLVER_FAMILY_PENDING_REASON)
            );
            assert_eq!(
                row.provenance["normalized_event_ids"]
                    .as_array()
                    .map(Vec::len),
                Some(1)
            );
        }

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

    async fn seed_basenames_identity(
        pool: &PgPool,
        logical_name_id: &str,
        resource_id: Uuid,
        surface_binding_id: Uuid,
        display_name: &str,
        binding_kind: SurfaceBindingKind,
    ) -> Result<()> {
        upsert_name_surfaces(
            pool,
            &[basenames_name_surface(logical_name_id, display_name)],
        )
        .await?;
        upsert_resources(pool, &[basenames_resource(resource_id)]).await?;
        upsert_surface_bindings(
            pool,
            &[basenames_surface_binding(
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

    async fn seed_permission_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        let mut resource_ids = events
            .iter()
            .filter_map(|event| event.resource_id)
            .collect::<Vec<_>>();
        resource_ids.sort();
        resource_ids.dedup();
        let resources = resource_ids.into_iter().map(resource).collect::<Vec<_>>();
        upsert_resources(pool, &resources).await?;
        upsert_normalized_events(pool, events).await?;
        Ok(())
    }

    const BASENAMES_L2_CODE_HASH: &str =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    const UNSUPPORTED_CODE_HASH: &str =
        "0x2222222222222222222222222222222222222222222222222222222222222222";

    async fn insert_basenames_dynamic_resolver_profile_fixture(
        pool: &PgPool,
        seed_contract_instance_id: Uuid,
        seed_address: &str,
        dynamic_resolvers: &[(Uuid, &str)],
        code_hashes: &[(&str, Option<&str>)],
    ) -> Result<()> {
        let resolver_manifest_id = insert_basenames_manifest_version(
            pool,
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
            "manifests/basenames/basenames_base_resolver/v1.toml",
        )
        .await?;
        let registry_manifest_id = insert_basenames_manifest_version(
            pool,
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "manifests/basenames/basenames_base_registry/v1.toml",
        )
        .await?;
        insert_basenames_contract_instance(
            pool,
            seed_contract_instance_id,
            seed_address,
            resolver_manifest_id,
            "contract",
        )
        .await?;
        insert_basenames_manifest_contract_instance(
            pool,
            resolver_manifest_id,
            "resolver",
            seed_contract_instance_id,
            seed_address,
        )
        .await?;

        let registry_contract_instance_id = Uuid::from_u128(0x86ff);
        insert_basenames_contract_instance(
            pool,
            registry_contract_instance_id,
            "0x00000000000000000000000000000000000086ff",
            registry_manifest_id,
            "root",
        )
        .await?;

        for (contract_instance_id, address) in dynamic_resolvers {
            insert_basenames_contract_instance(
                pool,
                *contract_instance_id,
                address,
                resolver_manifest_id,
                "contract",
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO discovery_edges (
                    chain_id,
                    edge_kind,
                    from_contract_instance_id,
                    to_contract_instance_id,
                    discovery_source,
                    source_manifest_id,
                    admission,
                    provenance
                )
                VALUES (
                    'base-mainnet',
                    'resolver',
                    $1,
                    $2,
                    $3,
                    $4,
                    'test',
                    '{}'::jsonb
                )
                "#,
            )
            .bind(registry_contract_instance_id)
            .bind(contract_instance_id)
            .bind(format!("test:basenames-dynamic-resolver:{address}"))
            .bind(registry_manifest_id)
            .execute(pool)
            .await
            .context("failed to insert Basenames dynamic resolver discovery_edge")?;
        }

        let mut raw_code_hashes = vec![basenames_raw_code_hash(
            seed_address,
            BASENAMES_L2_CODE_HASH,
        )];
        raw_code_hashes.extend(code_hashes.iter().filter_map(|(address, code_hash)| {
            code_hash.map(|code_hash| basenames_raw_code_hash(address, code_hash))
        }));
        upsert_raw_code_hashes(pool, &raw_code_hashes).await?;

        Ok(())
    }

    async fn insert_basenames_manifest_version(
        pool: &PgPool,
        source_family: &str,
        file_path: &str,
    ) -> Result<i64> {
        sqlx::query(
            r#"
            INSERT INTO manifest_versions (
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (1, 'basenames', $1, 'base-mainnet', 'basenames_v1', 'active', 'uts46-v1', $2, '{}'::jsonb)
            RETURNING manifest_id
            "#,
        )
        .bind(source_family)
        .bind(file_path)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
        .try_get::<i64, _>("manifest_id")
        .context("failed to read Basenames manifest_id")
    }

    async fn insert_basenames_contract_instance(
        pool: &PgPool,
        contract_instance_id: Uuid,
        address: &str,
        source_manifest_id: i64,
        contract_kind: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
            VALUES ($1, 'base-mainnet', $2, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(contract_kind)
        .execute(pool)
        .await
        .context("failed to insert Basenames contract_instance")?;
        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, 'base-mainnet', lower($2), $3, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(address)
        .bind(source_manifest_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames contract_instance_address")?;
        Ok(())
    }

    async fn insert_basenames_manifest_contract_instance(
        pool: &PgPool,
        manifest_id: i64,
        role: &str,
        contract_instance_id: Uuid,
        address: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                role,
                proxy_kind
            )
            VALUES ($1, 'contract', $2, $3, lower($4), $2, 'none')
            "#,
        )
        .bind(manifest_id)
        .bind(role)
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .context("failed to insert Basenames manifest_contract_instance")?;
        Ok(())
    }

    async fn insert_manifest_version(
        pool: &PgPool,
        source_family: &str,
        file_path: &str,
    ) -> Result<i64> {
        sqlx::query(
            r#"
            INSERT INTO manifest_versions (
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (1, 'ens', $1, 'ethereum-mainnet', 'ens_v1', 'active', 'uts46-v1', $2, '{}'::jsonb)
            RETURNING manifest_id
            "#,
        )
        .bind(source_family)
        .bind(file_path)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
        .try_get::<i64, _>("manifest_id")
        .context("failed to read manifest_id")
    }

    async fn insert_contract_instance(
        pool: &PgPool,
        contract_instance_id: Uuid,
        address: &str,
        source_manifest_id: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
            VALUES ($1, 'ethereum-mainnet', 'contract', '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert contract_instance")?;

        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, 'ethereum-mainnet', lower($2), $3, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(address)
        .bind(source_manifest_id)
        .execute(pool)
        .await
        .context("failed to insert contract_instance_address")?;

        Ok(())
    }

    async fn insert_manifest_contract_instance(
        pool: &PgPool,
        manifest_id: i64,
        role: &str,
        contract_instance_id: Uuid,
        address: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                role,
                proxy_kind
            )
            VALUES ($1, 'contract', $2, $3, lower($4), $2, 'none')
            "#,
        )
        .bind(manifest_id)
        .bind(role)
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .context("failed to insert manifest_contract_instance")?;
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

    fn basenames_name_surface(logical_name_id: &str, display_name: &str) -> NameSurface {
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{display_name}"),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-surface".to_owned(),
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

    fn basenames_resource(resource_id: Uuid) -> Resource {
        Resource {
            resource_id,
            token_lineage_id: None,
            chain_id: "base-mainnet".to_owned(),
            block_hash: format!("0xbase-resource{}", &resource_id.simple().to_string()[..8]),
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

    fn basenames_surface_binding(
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
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-bind".to_owned(),
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

    fn basenames_raw_code_hash(address: &str, code_hash: &str) -> RawCodeHash {
        RawCodeHash {
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-code-hash".to_owned(),
            block_number: 41,
            contract_address: address.to_owned(),
            code_hash: code_hash.to_owned(),
            code_byte_length: 5,
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

    fn resolver_event_with_manifest(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        resolver_address: &str,
        source_family: &str,
        source_manifest_id: i64,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        let mut event = resolver_event(
            event_identity,
            logical_name_id,
            resource_id,
            resolver_address,
            block_number,
            log_index,
        );
        event.source_family = source_family.to_owned();
        event.source_manifest_id = Some(source_manifest_id);
        event
    }

    fn basenames_resolver_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        resolver_address: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
            manifest_version: 4,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xbase-res{block_number:04}")),
            transaction_hash: Some(format!("0xbase-tx{block_number:04x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "base-mainnet",
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

    fn resolver_permission_event(
        event_identity: &str,
        logical_name_id: Option<&str>,
        resource_id: Uuid,
        subject: &str,
        chain_id: &str,
        resolver_address: &str,
        effective_powers: Value,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: logical_name_id.map(str::to_owned),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_PERMISSION_CHANGED.to_owned(),
            source_family: "ens_v1_unwrapped_authority".to_owned(),
            manifest_version: 9,
            source_manifest_id: None,
            chain_id: Some(chain_id.to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xres{block_number:04}")),
            transaction_hash: Some(format!("0xperm{block_number:04x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": chain_id,
                "block_number": block_number,
                "log_index": log_index,
            }),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "subject": subject,
                "scope": {
                    "kind": "resolver",
                    "chain_id": chain_id,
                    "resolver_address": resolver_address,
                },
                "effective_powers": effective_powers,
                "grant_source": {
                    "kind": "normalized_event",
                    "event_identity": event_identity,
                },
                "revocation_source": Value::Null,
                "inheritance_path": [],
                "transfer_behavior": {},
            }),
        }
    }

    fn alias_event(
        event_identity: &str,
        logical_name_id: Option<&str>,
        resource_id: Option<Uuid>,
        resolver_address: &str,
        from_dns_encoded_name: &str,
        to_dns_encoded_name: &str,
        from_name: Option<&str>,
        to_name: Option<&str>,
        alias_state: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: logical_name_id.map(str::to_owned),
            resource_id,
            event_kind: EVENT_KIND_ALIAS_CHANGED.to_owned(),
            source_family: "ens_v2_resolver_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xres{block_number:04}")),
            transaction_hash: Some(format!("0xalias{block_number:04x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "log_index": log_index,
            }),
            derivation_kind: "ens_v2_resolver".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "source_event": "AliasChanged",
                "resolver": resolver_address,
                "from_dns_encoded_name": from_dns_encoded_name,
                "to_dns_encoded_name": to_dns_encoded_name,
                "alias_state": alias_state,
                "active": alias_state == "active",
                "from_name": from_name,
                "to_name": to_name,
                "to_logical_name_id": to_name.map(|name| format!("ens:{name}")),
                "to_resource_id": resource_id.map(|value| value.to_string()),
            }),
        }
    }

    fn database_resource_id(offset: u128) -> Uuid {
        Uuid::from_u128(0x8f00 + offset)
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }
}
