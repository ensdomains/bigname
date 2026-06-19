const ENSV2_ROOT_UPSTREAM_RESOURCE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
const ENSV2_RESOURCE_UUID_PREFIX: &str = "ens-v2-resource";
const ENSV2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENSV2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";

async fn load_ensv2_root_resource_id_for_name_resource(
    pool: &PgPool,
    resource_id: Uuid,
    route: &'static str,
) -> ApiResult<Option<Uuid>> {
    let Some(resource) = load_resource(pool, resource_id).await.map_err(|load_error| {
        error!(
            service = "api",
            route = route,
            resource_id = %resource_id,
            error = ?load_error,
            "failed to load resource provenance for ENSv2 root fallback roles"
        );
        ApiError::internal_error("failed to load resource provenance for roles")
    })?
    else {
        return Ok(None);
    };

    let root_resource_id = ensv2_root_resource_id_from_resource(&resource);
    if root_resource_id.is_none() && is_ensv2_registry_or_root_resource(&resource) {
        warn!(
            service = "api",
            route = route,
            resource_id = %resource_id,
            provenance = ?resource.provenance,
            "skipped ENSv2 root fallback roles because resource provenance was malformed"
        );
    }

    Ok(root_resource_id.filter(|root| *root != resource_id))
}

fn is_ensv2_registry_or_root_resource(resource: &bigname_storage::Resource) -> bool {
    resource
        .provenance
        .as_object()
        .and_then(|provenance| provenance.get("source_family"))
        .and_then(JsonValue::as_str)
        .is_some_and(|source_family| {
            matches!(
                source_family,
                ENSV2_ROOT_SOURCE_FAMILY | ENSV2_REGISTRY_SOURCE_FAMILY
            )
        })
}

fn ensv2_root_resource_id_from_resource(resource: &bigname_storage::Resource) -> Option<Uuid> {
    let provenance = resource.provenance.as_object()?;
    let source_family = provenance.get("source_family")?.as_str()?;
    if !matches!(
        source_family,
        ENSV2_ROOT_SOURCE_FAMILY | ENSV2_REGISTRY_SOURCE_FAMILY
    ) {
        return None;
    }

    let chain_id = provenance.get("chain_id")?.as_str()?;
    let registry_contract_instance_id = provenance
        .get("registry_contract_instance_id")?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())?;
    provenance.get("upstream_resource")?.as_str()?;

    Some(ensv2_registry_permission_resource_id(
        chain_id,
        registry_contract_instance_id,
        ENSV2_ROOT_UPSTREAM_RESOURCE,
    ))
}

fn ensv2_registry_permission_resource_id(
    chain_id: &str,
    registry_contract_instance_id: Uuid,
    upstream_resource: &str,
) -> Uuid {
    let seed =
        format!("{ENSV2_RESOURCE_UUID_PREFIX}:{chain_id}:{registry_contract_instance_id}:{upstream_resource}");
    let digest = alloy_primitives::keccak256(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
