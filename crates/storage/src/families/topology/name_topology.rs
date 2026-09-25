//! The alias and wildcard arms of a name's `declared_summary.topology`, read from the aliases
//! (`project_name_alias`) and the resource resolver pointers (`project_resource_pointer`), as
//! crates/project/src/builders/name_topology.rs builds them in `project_alias_topology` and
//! `project_wildcard_topology`. The direct, ownerless and Basenames transport arms are not read
//! here.
//!
//! The alias arm joins the name's current pointer (latest, then reject zero), never the
//! historical non-zero pointer, so a pointer clear with no alias event leaves no alias topology
//! and exposes no older pointer. The wildcard arm takes the longest ancestor by suffix with a
//! binding, its historical non-zero pointer and its independent version boundary.
use anyhow::{Context, Result};
use bigname_domain::resolution_topology::ResolutionTopology;
use serde_json::{Map, Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    pointers::{load_family_alias_source_pointer, load_family_wildcard_source},
    shims::{SelectedBinding, selected_binding},
};

const ALIAS_PATH: &str = "resolver_alias_path";
const WILDCARD_PATH: &str = "observed_wildcard_path";

struct Surface {
    logical_name_id: String,
    namespace: String,
    raw_name: String,
    namehash: String,
}

/// The name's alias or wildcard topology as the serializer stores it, `None` when the name's
/// selected binding is neither arm or the arm has no source.
pub async fn load_name_topology_shadow(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<Value>> {
    let Some(surface) = load_surface(pool, logical_name_id).await? else {
        return Ok(None);
    };
    let Some(binding) = selected_binding(pool, logical_name_id).await? else {
        return Ok(None);
    };
    let topology = match binding.binding_kind.as_str() {
        ALIAS_PATH => alias_topology(pool, &surface, &binding).await?,
        WILDCARD_PATH => wildcard_topology(pool, &surface, &binding).await?,
        _ => None,
    };
    topology
        .map(|topology| {
            let typed =
                serde_json::from_value::<ResolutionTopology>(topology).with_context(|| {
                    format!("shadow topology of {logical_name_id} is not a ResolutionTopology")
                })?;
            serde_json::to_value(typed).context("failed to serialize the shadow topology")
        })
        .transpose()
}

async fn load_surface(pool: &PgPool, logical_name_id: &str) -> Result<Option<Surface>> {
    let row: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT logical_name_id, namespace, raw_name, namehash
         FROM bigname_phase.name_surfaces WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the surface of {logical_name_id}"))?;
    Ok(
        row.map(|(logical_name_id, namespace, raw_name, namehash)| Surface {
            logical_name_id,
            namespace,
            raw_name,
            namehash,
        }),
    )
}

fn name_ref(surface: &Surface, resource_id: Uuid, binding_kind: &str) -> Value {
    json!({
        "logical_name_id": surface.logical_name_id,
        "namespace": surface.namespace,
        "normalized_name": surface.raw_name,
        "canonical_display_name": surface.raw_name,
        "namehash": surface.namehash,
        "resource_id": resource_id,
        "binding_kind": binding_kind,
    })
}

fn resolver_hop(surface: &Surface, resource_id: Uuid, chain_id: &str, address: Value) -> Value {
    json!({
        "logical_name_id": surface.logical_name_id,
        "namespace": surface.namespace,
        "normalized_name": surface.raw_name,
        "canonical_display_name": surface.raw_name,
        "resource_id": resource_id,
        "chain_id": chain_id,
        "address": address,
        "latest_event_kind": "ResolverChanged",
    })
}

fn topology(
    registry: Value,
    resolver: Value,
    wildcard: Value,
    alias: Value,
    boundary: Value,
) -> Value {
    json!({
        "registry_path": [registry],
        "subregistry_path": [],
        "resolver_path": [resolver],
        "wildcard": wildcard,
        "alias": alias,
        "version_boundaries": {
            "topology_version_boundary": boundary,
            "record_version_boundary": boundary,
        },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null,
        },
    })
}

/// The block's hash and timestamp on the readable lineage.
async fn block(pool: &PgPool, chain_id: &str, number: i64) -> Result<Option<(String, Value)>> {
    sqlx::query_as(
        "SELECT block_hash, to_jsonb(block_timestamp) FROM bigname_phase.chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(number)
    .fetch_optional(pool)
    .await
    .context("failed to load a lineage block")
}

async fn alias_topology(
    pool: &PgPool,
    surface: &Surface,
    binding: &SelectedBinding,
) -> Result<Option<Value>> {
    let Some(pointer) =
        load_family_alias_source_pointer(pool, &binding.chain_id, binding.resource_id).await?
    else {
        return Ok(None);
    };
    type AliasRow = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let alias: Option<AliasRow> = sqlx::query_as(
        "SELECT to_logical_name_id, to_normalized_name, to_name, to_canonical_display_name,
                to_namehash, to_resource_id
         FROM bigname_phase.project_name_alias
         WHERE chain_id = $1 AND logical_name_id = $2
           AND active AND to_logical_name_id IS NOT NULL",
    )
    .bind(&binding.chain_id)
    .bind(&surface.logical_name_id)
    .fetch_optional(pool)
    .await
    .context("failed to load the name alias")?;
    let Some((to_id, to_normalized, to_name, to_display, to_namehash, to_resource)) = alias else {
        return Ok(None);
    };
    let Some((block_hash, timestamp)) =
        block(pool, &binding.chain_id, binding.block_number).await?
    else {
        return Ok(None);
    };
    // The alias event's namespace is the source name's: the event names that name.
    let mut target = Map::new();
    for (key, value) in [
        ("logical_name_id", to_id),
        ("namespace", Some(surface.namespace.clone())),
        ("normalized_name", to_normalized.or_else(|| to_name.clone())),
        ("canonical_display_name", to_display.or(to_name)),
        ("namehash", to_namehash),
        ("resource_id", to_resource),
        ("binding_kind", Some(ALIAS_PATH.to_owned())),
    ] {
        if let Some(value) = value {
            target.insert(key.to_owned(), Value::String(value));
        }
    }
    let target = Value::Object(target);
    let boundary = json!({
        "logical_name_id": surface.logical_name_id,
        "resource_id": binding.resource_id,
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": {
            "chain_id": binding.chain_id,
            "block_number": binding.block_number,
            "block_hash": block_hash,
            "timestamp": timestamp,
        },
    });
    Ok(Some(topology(
        name_ref(surface, binding.resource_id, &binding.binding_kind),
        resolver_hop(
            surface,
            binding.resource_id,
            &pointer.chain_id,
            json!(pointer.resolver_address),
        ),
        json!({"source": null, "matched_labels": []}),
        json!({"final_target": target, "hops": [target]}),
        boundary,
    )))
}

async fn wildcard_topology(
    pool: &PgPool,
    surface: &Surface,
    binding: &SelectedBinding,
) -> Result<Option<Value>> {
    let labels: Vec<&str> = surface.raw_name.split('.').collect();
    let suffixes: Vec<String> = (1..labels.len())
        .map(|start| labels[start..].join("."))
        .filter(|suffix| !suffix.is_empty())
        .collect();
    let ancestors: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT logical_name_id, namespace, raw_name, namehash
         FROM bigname_phase.name_surfaces
         WHERE namespace = $1 AND raw_name = ANY($2) AND raw_name <> ''
         ORDER BY cardinality(string_to_array(raw_name, '.')) DESC, logical_name_id",
    )
    .bind(&surface.namespace)
    .bind(&suffixes)
    .fetch_all(pool)
    .await
    .context("failed to load wildcard ancestors")?;
    for (logical_name_id, namespace, raw_name, namehash) in ancestors {
        let ancestor = Surface {
            logical_name_id,
            namespace,
            raw_name,
            namehash,
        };
        let Some(ancestor_binding) = selected_binding(pool, &ancestor.logical_name_id).await?
        else {
            continue;
        };
        let Some(source) = load_family_wildcard_source(
            pool,
            &ancestor_binding.chain_id,
            ancestor_binding.resource_id,
        )
        .await?
        else {
            continue;
        };
        let identity = source.boundary_position["event_identity"].as_str();
        let event: Option<(i64, String)> = sqlx::query_as(
            "SELECT normalized_event_id, block_hash FROM bigname_phase.normalized_events
             WHERE event_identity = $1",
        )
        .bind(identity)
        .fetch_optional(pool)
        .await
        .context("failed to load the wildcard boundary event")?;
        let (event_id, block_hash) =
            event.map_or((None, None), |(id, hash)| (Some(id), Some(hash)));
        let version = source.boundary_kind == "RecordVersionChanged";
        let boundary = json!({
            "logical_name_id": ancestor.logical_name_id,
            "resource_id": ancestor_binding.resource_id,
            "normalized_event_id": if version { json!(event_id) } else { Value::Null },
            "event_kind": if version { json!(source.boundary_kind) } else { Value::Null },
            "chain_position": {
                "chain_id": ancestor_binding.chain_id,
                "block_number": source.boundary_position["block_number"],
                "block_hash": block_hash,
                "timestamp": source.boundary_block_timestamp,
            },
        });
        let ancestor_labels = ancestor.raw_name.split('.').count();
        let matched: Vec<&str> = labels[..labels.len().saturating_sub(ancestor_labels)].to_vec();
        return Ok(Some(topology(
            name_ref(surface, binding.resource_id, &binding.binding_kind),
            resolver_hop(
                &ancestor,
                ancestor_binding.resource_id,
                &ancestor_binding.chain_id,
                json!(source.nonzero_resolver_address),
            ),
            json!({
                "source": name_ref(&ancestor, ancestor_binding.resource_id, WILDCARD_PATH),
                "matched_labels": matched,
            }),
            json!({"final_target": null, "hops": []}),
            boundary,
        )));
    }
    Ok(None)
}
