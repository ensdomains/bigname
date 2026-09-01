use bigname_domain::{
    resolution_topology::{ResolutionRoute, ResolutionTopology},
    vocabulary::Namespace,
};
use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, Transaction};

use crate::{
    ENS_EXECUTION_SOURCE_FAMILY, ENS_NAMESPACE, ENS_UNIVERSAL_RESOLVER_ROLE,
    ENS_V1_REGISTRY_SOURCE_FAMILY, ETHEREUM_MAINNET_CHAIN_ID, LookupError, LookupPosition,
    LookupRequest, RecordSelector, Result, abi::ResolutionResultAbi, call::ExecutionBlock,
    error::database,
};

mod indexed;
mod manifests;
mod persistence;
mod positions;
mod routes;
#[cfg(test)]
pub(crate) fn indexed_answer(entries: &Value, selector: &RecordSelector) -> Value {
    indexed::answer(
        entries,
        &serde_json::json!({}),
        &serde_json::json!({"status":"projected"}),
        selector,
    )
}
#[cfg(test)]
pub(crate) use persistence::divergence_write_error;
pub(crate) use persistence::{persist_comparisons, revalidate_primary_name_position};
pub(crate) use routes::LookupRoute;

#[derive(Clone, Debug)]
pub(crate) struct LookupSnapshot {
    pub logical_name_id: String,
    pub name: String,
    pub dns_name: Vec<u8>,
    pub node: [u8; 32],
    pub resolver_chain_id: String,
    pub resolver_address: String,
    pub entrypoint_chain_id: String,
    pub entrypoint_address: String,
    pub authoritative_position: LookupPosition,
    pub execution_position: LookupPosition,
    pub execution_block: ExecutionBlock,
    pub follow_ccip: bool,
    pub result_abi: ResolutionResultAbi,
    pub observed_positions: Value,
    pub revalidation_positions: Value,
    pub execution_authority: Value,
    pub route: LookupRoute,
    comparison: Option<IndexedComparison>,
}

#[derive(Clone, Debug)]
pub(super) struct IndexedComparison {
    pub resource_id: String,
    pub boundary_key: String,
    pub row_xmin: String,
    pub entries: Value,
    pub provenance: Value,
    pub coverage: Value,
}

#[derive(Clone, Debug)]
pub(crate) struct EnsPrimaryNameAuthority {
    pub registry_address: String,
    pub universal_resolver_address: String,
    pub position: LookupPosition,
    pub execution_authority: Value,
}

impl LookupSnapshot {
    pub fn indexed_answer(&self, selector: &RecordSelector) -> Option<Value> {
        self.comparison.as_ref().map(|comparison| {
            indexed::answer(
                &comparison.entries,
                &comparison.provenance,
                &comparison.coverage,
                selector,
            )
        })
    }
}

#[derive(FromRow)]
struct NameRow {
    logical_name_id: String,
    namespace: String,
    raw_name: String,
    namehash: String,
    dns_encoded_name: Vec<u8>,
    resource_chain_id: String,
    declared_summary: Value,
    chain_positions: Value,
    row_xmin: String,
}

#[derive(FromRow)]
struct InventoryRow {
    resource_id: String,
    record_version_boundary_key: String,
    entries: Value,
    provenance: Value,
    coverage: Value,
    chain_positions: Value,
    row_xmin: String,
}

#[derive(Clone, Debug, FromRow)]
struct HeadRow {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    timestamp: String,
}

pub(crate) async fn load_snapshot(
    pool: &PgPool,
    request: &LookupRequest,
) -> Result<LookupSnapshot> {
    let mut transaction = pool.begin().await.map_err(database("start lookup read"))?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(database("set lookup read isolation"))?;
    let name = load_name(&mut transaction, &request.logical_name_id).await?;
    let namespace = name.namespace.parse::<Namespace>().map_err(|error| {
        LookupError::unsupported(format!(
            "projected name has an unsupported namespace: {error}"
        ))
    })?;
    let exact_resolver_is_null = name
        .declared_summary
        .get("resolver")
        .filter(|resolver| resolver.is_object())
        .is_some_and(|resolver| {
            resolver.get("chain_id").is_some_and(Value::is_null)
                && resolver.get("address").is_some_and(Value::is_null)
                && resolver.get("status").and_then(Value::as_str) != Some("unsupported")
        });
    let (topology, route) = match name.declared_summary.get("topology") {
        Some(topology_value) if topology_value.is_object() => {
            let topology = serde_json::from_value::<ResolutionTopology>(topology_value.clone())
                .map_err(|error| {
                    LookupError::unsupported(format!(
                        "projected topology does not match ResolutionTopology: {error}"
                    ))
                })?;
            let preflight_path = topology
                .classify(
                    &name.logical_name_id,
                    routes::preflight_route_policy(namespace, &topology)?,
                )
                .map_err(|error| LookupError::unsupported(error.to_string()))?;
            let route = routes::classify_lookup_route(routes::DiscoveryRouteCandidate {
                namespace,
                resource_chain_id: &name.resource_chain_id,
                logical_name_id: &name.logical_name_id,
                namehash: &name.namehash,
                dns_name: &name.dns_encoded_name,
                exact_resolver_is_null,
                topology: &topology,
                path: preflight_path,
            });
            (topology, route)
        }
        None => {
            let topology = routes::classify_absent_topology_route(
                namespace,
                &name.resource_chain_id,
                &name.logical_name_id,
                &name.namehash,
                &name.dns_encoded_name,
                exact_resolver_is_null,
            )
            .ok_or_else(|| {
                LookupError::unsupported("verified lookup requires projected topology")
            })?;
            (topology, LookupRoute::EnsUniversalResolverDiscovery)
        }
        Some(_) => {
            return Err(LookupError::unsupported(
                "verified lookup requires projected topology",
            ));
        }
    };
    let (resolver_chain_id, resolver_address) = routes::selected_resolver(route, &topology)?;
    if resolver_chain_id.as_str() != name.resource_chain_id {
        return Err(LookupError::unsupported(
            "projected resolver and indexed authority object are on different chains",
        ));
    }
    let boundary = match route {
        LookupRoute::EnsUniversalResolverDiscovery => None,
        LookupRoute::Projected => Some(
            name.declared_summary
                .get("topology")
                .expect("projected lookup route has topology")
                .pointer("/version_boundaries/record_version_boundary")
                .filter(|value| value.is_object())
                .ok_or_else(|| {
                    LookupError::unsupported("verified lookup requires a projected record boundary")
                })?,
        ),
    };
    let resolver_head = load_head(&mut transaction, resolver_chain_id.as_str()).await?;
    let project_row_xmin =
        positions::ensure_project_at_head(&mut transaction, &resolver_head).await?;
    let resolver_position =
        positions::position_for_chain(&name.chain_positions, resolver_chain_id.as_str())?;
    positions::ensure_canonical(&mut transaction, &resolver_position).await?;

    let entrypoint = routes::entrypoint_authority(namespace, resolver_chain_id)?;
    let authoritative_position = LookupPosition {
        chain_id: resolver_head.chain_id,
        block_number: resolver_head.block_number,
        block_hash: resolver_head.block_hash,
        timestamp: resolver_head.timestamp,
    };
    let execution_position = if entrypoint.chain_id.as_str() == resolver_position.chain_id {
        resolver_position.clone()
    } else {
        let position =
            positions::position_for_chain(&name.chain_positions, entrypoint.chain_id.as_str())?;
        positions::ensure_canonical(&mut transaction, &position).await?;
        position
    };
    let execution_block = if entrypoint.chain_id.as_str() == resolver_position.chain_id {
        ExecutionBlock {
            chain_id: authoritative_position.chain_id.clone(),
            block_number: authoritative_position.block_number,
            block_hash: authoritative_position.block_hash.clone(),
        }
    } else {
        ExecutionBlock {
            chain_id: execution_position.chain_id.clone(),
            block_number: execution_position.block_number,
            block_hash: execution_position.block_hash.clone(),
        }
    };
    let live_execution_position = if entrypoint.chain_id.as_str() == resolver_position.chain_id {
        authoritative_position.clone()
    } else {
        execution_position.clone().into()
    };
    let entrypoint_manifest = manifests::load_entrypoint(
        &mut transaction,
        manifests::EntrypointQuery {
            namespace: namespace.as_str(),
            source_family: entrypoint.source_family.as_str(),
            chain_id: entrypoint.chain_id.as_str(),
            role: entrypoint.role,
            allow_shadow: entrypoint.allow_shadow,
            execution_block_number: execution_block.block_number,
            required_manifest_version: entrypoint.required_manifest_version,
            require_resolution_capability: true,
        },
    )
    .await?;
    let route_policy = routes::route_policy(namespace, &entrypoint_manifest)?;
    let path_class = topology
        .classify(&name.logical_name_id, route_policy)
        .map_err(|error| LookupError::unsupported(error.to_string()))?;
    if route == LookupRoute::EnsUniversalResolverDiscovery
        && !routes::is_ens_universal_resolver_discovery_topology(
            &topology,
            path_class,
            &name.logical_name_id,
        )
    {
        return Err(LookupError::unsupported(
            "projected topology is outside the ENS Universal Resolver discovery route",
        ));
    }
    let inventory = if route == LookupRoute::EnsUniversalResolverDiscovery
        || path_class == ResolutionRoute::WildcardDerived
    {
        None
    } else {
        let boundary = boundary.expect("projected lookup route has a record boundary");
        let boundary_resource_id = boundary
            .get("resource_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                LookupError::unsupported(
                    "verified lookup record boundary has no comparison resource",
                )
            })?;
        Some(load_inventory(&mut transaction, boundary_resource_id, boundary).await?)
    };
    let comparison_position = if let Some(inventory) = &inventory {
        positions::inventory_position(
            &mut transaction,
            "record_inventory_current",
            &inventory.chain_positions,
            resolver_chain_id.as_str(),
        )
        .await?
    } else {
        resolver_position.clone()
    };
    transaction
        .commit()
        .await
        .map_err(database("commit lookup read"))?;

    let observed_positions =
        positions::observed_positions(&comparison_position, &execution_position)?;
    let revalidation_positions =
        positions::comparison_and_live_positions(&comparison_position, &live_execution_position)?;
    let execution_authority = execution_authority(
        &project_row_xmin,
        Some((&name.logical_name_id, &name.row_xmin)),
        std::slice::from_ref(&entrypoint_manifest),
    )?;
    Ok(LookupSnapshot {
        logical_name_id: name.logical_name_id,
        name: name.raw_name,
        dns_name: name.dns_encoded_name,
        node: crate::abi::parse_node(&name.namehash).map_err(|error| {
            LookupError::unsupported(format!("indexed namehash is malformed: {error:#}"))
        })?,
        resolver_chain_id: resolver_chain_id.to_string(),
        resolver_address: resolver_address.to_string(),
        entrypoint_chain_id: entrypoint.chain_id.to_string(),
        entrypoint_address: entrypoint_manifest.declared_address.to_ascii_lowercase(),
        authoritative_position,
        execution_position: live_execution_position,
        execution_block,
        follow_ccip: entrypoint.follow_ccip,
        result_abi: entrypoint.result_abi,
        observed_positions,
        revalidation_positions,
        execution_authority,
        route,
        comparison: inventory.map(|inventory| IndexedComparison {
            resource_id: inventory.resource_id,
            boundary_key: inventory.record_version_boundary_key,
            row_xmin: inventory.row_xmin,
            entries: inventory.entries,
            provenance: inventory.provenance,
            coverage: inventory.coverage,
        }),
    })
}

pub(crate) async fn load_ens_primary_name_authority(
    pool: &PgPool,
) -> Result<EnsPrimaryNameAuthority> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(database("start primary-name authority read"))?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(database("set primary-name authority read isolation"))?;
    let head = load_head(&mut transaction, ETHEREUM_MAINNET_CHAIN_ID).await?;
    let project_row_xmin = positions::ensure_project_at_head(&mut transaction, &head).await?;
    let registry_manifest = manifests::load_entrypoint(
        &mut transaction,
        manifests::EntrypointQuery {
            namespace: ENS_NAMESPACE,
            source_family: ENS_V1_REGISTRY_SOURCE_FAMILY,
            chain_id: ETHEREUM_MAINNET_CHAIN_ID,
            role: crate::ENS_REGISTRY_ROLE,
            allow_shadow: false,
            execution_block_number: head.block_number,
            required_manifest_version: None,
            require_resolution_capability: false,
        },
    )
    .await?;
    let universal_resolver_manifest = manifests::load_entrypoint(
        &mut transaction,
        manifests::EntrypointQuery {
            namespace: ENS_NAMESPACE,
            source_family: ENS_EXECUTION_SOURCE_FAMILY,
            chain_id: ETHEREUM_MAINNET_CHAIN_ID,
            role: ENS_UNIVERSAL_RESOLVER_ROLE,
            allow_shadow: true,
            execution_block_number: head.block_number,
            required_manifest_version: None,
            require_resolution_capability: true,
        },
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(database("commit primary-name authority read"))?;
    Ok(EnsPrimaryNameAuthority {
        registry_address: registry_manifest.declared_address.to_ascii_lowercase(),
        universal_resolver_address: universal_resolver_manifest
            .declared_address
            .to_ascii_lowercase(),
        position: LookupPosition {
            chain_id: head.chain_id,
            block_number: head.block_number,
            block_hash: head.block_hash,
            timestamp: head.timestamp,
        },
        execution_authority: execution_authority(
            &project_row_xmin,
            None,
            &[registry_manifest, universal_resolver_manifest],
        )?,
    })
}

fn execution_authority(
    project_row_xmin: &str,
    name: Option<(&str, &str)>,
    manifests: &[manifests::ManifestEntry],
) -> Result<Value> {
    let (logical_name_id, name_row_xmin) = name.unzip();
    Ok(serde_json::json!({
        "project_row_xmin": project_row_xmin,
        "logical_name_id": logical_name_id,
        "name_row_xmin": name_row_xmin,
        "manifest_authorities": manifests,
    }))
}

async fn load_name(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<NameRow> {
    sqlx::query_as::<_, NameRow>(
        r#"
        SELECT name.logical_name_id, name.namespace, name.raw_name, name.namehash,
               surface.dns_encoded_name, resource.chain_id AS resource_chain_id,
               name.declared_summary, name.chain_positions,
               name.xmin::text AS row_xmin
        FROM name_current name
        JOIN name_surfaces surface
          ON surface.logical_name_id = name.logical_name_id
        JOIN resources resource ON resource.resource_id = name.resource_id
        JOIN surface_bindings binding
          ON binding.surface_binding_id = name.surface_binding_id
         AND binding.logical_name_id = name.logical_name_id
         AND binding.resource_id = name.resource_id
         AND binding.binding_kind = name.binding_kind
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = name.token_lineage_id
        LEFT JOIN chain_lineage token_lineage_lineage
          ON token_lineage_lineage.chain_id = token_lineage.chain_id
         AND token_lineage_lineage.block_hash = token_lineage.block_hash
        JOIN chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
         AND surface_lineage.block_number = surface.block_number
        JOIN chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
         AND resource_lineage.block_number = resource.block_number
        JOIN chain_lineage binding_lineage
          ON binding_lineage.chain_id = binding.chain_id
         AND binding_lineage.block_hash = binding.block_hash
         AND binding_lineage.block_number = binding.block_number
        WHERE name.logical_name_id = $1
          AND name.support_status = 'supported'
          AND surface.visibility_state = 'active'
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND binding.active_to IS NULL
          AND (
              name.token_lineage_id IS NULL
              OR (
                  token_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND token_lineage_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              )
          )
          AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND binding_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(logical_name_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load readable name projection"))?
    .ok_or_else(|| LookupError::unsupported("verified lookup name is not readable or supported"))
}

async fn load_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: &str,
    boundary: &Value,
) -> Result<InventoryRow> {
    sqlx::query_as::<_, InventoryRow>(
        r#"
        SELECT inventory.resource_id::text AS resource_id,
               inventory.record_version_boundary_key, inventory.entries,
               inventory.provenance,
               CASE WHEN inventory.support_status = 'supported'
                   THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                   ELSE jsonb_build_object(
                       'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                       'unsupported_reason', inventory.unsupported_reason
                   )
               END AS coverage,
               inventory.chain_positions, inventory.xmin::text AS row_xmin
        FROM record_inventory_current inventory
        JOIN resources resource
          ON resource.resource_id = inventory.resource_id
        JOIN chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
        WHERE inventory.resource_id = $1::uuid
          AND inventory.record_version_boundary = $2
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(resource_id)
    .bind(boundary)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load indexed record answer"))?
    .ok_or_else(|| LookupError::unsupported("verified lookup requires an indexed record boundary"))
}

async fn load_head(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<HeadRow> {
    sqlx::query_as::<_, HeadRow>(
        r#"
        SELECT head.chain_id, head.latest_block_hash AS block_hash,
               head.latest_block_number AS block_number,
               to_char(
                   lineage.block_timestamp AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS"Z"'
               ) AS timestamp
        FROM chain_heads head
        JOIN chain_lineage lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE head.chain_id = $1
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load lookup chain head"))?
    .ok_or_else(|| LookupError::stale(format!("chain {chain_id} has no readable latest head")))
}
