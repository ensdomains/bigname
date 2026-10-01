//! The wildcard arm of a name's `declared_summary.topology`, read from the resource resolver
//! pointers (`project_resource_pointer`). The direct, ownerless and Basenames transport arms are
//! not read here.
//!
//! The name's selected binding, and each wildcard ancestor's, is the one the composed name reader
//! selects (`families::name`). The wildcard arm takes the longest ancestor by suffix with a
//! binding, its historical non-zero pointer and its independent version boundary.
use anyhow::{Context, Result};
use bigname_domain::resolution_topology::ResolutionTopology;
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::pointers::load_family_wildcard_source_on as load_family_wildcard_source;

/// A name's selected binding: its `project_binding_candidate` row.
#[derive(Clone, Debug, PartialEq)]
struct SelectedBinding {
    chain_id: String,
    resource_id: Uuid,
    binding_kind: String,
    block_number: i64,
}

/// The binding the composed name reader selects for `logical_name_id`
/// (`families::name`, the `surface_binding_id` of its row), read from its
/// `project_binding_candidate` row.
async fn selected_binding(
    conn: &mut PgConnection,
    logical_name_id: &str,
) -> Result<Option<SelectedBinding>> {
    let Some(binding_id) = crate::families::name::load_composed_base(
        conn,
        &[logical_name_id.to_owned()],
        crate::families::name::CoverageShape::Plain,
    )
    .await?
    .remove(logical_name_id)
    .and_then(|row| row.surface_binding_id) else {
        return Ok(None);
    };
    let row: Option<(String, Uuid, String, i64)> = sqlx::query_as(
        "SELECT chain_id, resource_id, binding_kind, block_number
         FROM bigname_phase.project_binding_candidate
         WHERE surface_binding_id = $1",
    )
    .bind(binding_id)
    .fetch_optional(&mut *conn)
    .await
    .with_context(|| format!("failed to load the selected binding of {logical_name_id}"))?;
    Ok(row.map(
        |(chain_id, resource_id, binding_kind, block_number)| SelectedBinding {
            chain_id,
            resource_id,
            binding_kind,
            block_number,
        },
    ))
}

const WILDCARD_PATH: &str = "observed_wildcard_path";

struct Surface {
    logical_name_id: String,
    namespace: String,
    raw_name: String,
    namehash: String,
}

/// The name's wildcard topology as the serializer stores it, `None` when the name's selected
/// binding is not a wildcard binding or the arm has no source.
pub async fn load_name_topology_shadow(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<Value>> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let topology = load_name_topology_on(&mut snapshot, logical_name_id).await?;
    snapshot.commit().await?;
    Ok(topology)
}

pub(crate) async fn load_name_topology_on(
    conn: &mut PgConnection,
    logical_name_id: &str,
) -> Result<Option<Value>> {
    let Some(surface) = load_surface(conn, logical_name_id).await? else {
        return Ok(None);
    };
    let Some(binding) = selected_binding(conn, logical_name_id).await? else {
        return Ok(None);
    };
    let topology = match binding.binding_kind.as_str() {
        WILDCARD_PATH => wildcard_topology(conn, &surface, &binding).await?,
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

async fn load_surface(conn: &mut PgConnection, logical_name_id: &str) -> Result<Option<Surface>> {
    let row: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT logical_name_id, namespace, raw_name, namehash
         FROM bigname_phase.name_surfaces WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_optional(&mut *conn)
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

fn topology(registry: Value, resolver: Value, wildcard: Value, boundary: Value) -> Value {
    json!({
        "registry_path": [registry],
        "subregistry_path": [],
        "resolver_path": [resolver],
        "wildcard": wildcard,
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

async fn wildcard_topology(
    conn: &mut PgConnection,
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
    .fetch_all(&mut *conn)
    .await
    .context("failed to load wildcard ancestors")?;
    for (logical_name_id, namespace, raw_name, namehash) in ancestors {
        let ancestor = Surface {
            logical_name_id,
            namespace,
            raw_name,
            namehash,
        };
        let Some(ancestor_binding) = selected_binding(conn, &ancestor.logical_name_id).await?
        else {
            continue;
        };
        let Some(source) = load_family_wildcard_source(
            conn,
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
        .fetch_optional(&mut *conn)
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
            boundary,
        )));
    }
    Ok(None)
}
