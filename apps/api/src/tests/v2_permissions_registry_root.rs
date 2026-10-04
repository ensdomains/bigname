use super::*;

const ROOT_HOLDERS: [&str; 4] = [
    "0x00000000000000000000000000000000000000c1",
    "0x00000000000000000000000000000000000000c2",
    "0x00000000000000000000000000000000000000c3",
    "0x00000000000000000000000000000000000000c4",
];
const REVOKED_HOLDER: &str = "0x00000000000000000000000000000000000000c5";
const ZERO_RESOURCE: &str = bigname_storage::ENS_V2_ROOT_UPSTREAM_RESOURCE;

fn alpha_root() -> Uuid {
    bigname_storage::ens_v2_registry_root_resource_id("ethereum-mainnet", Uuid::from_u128(0xA190))
}

fn root_role_event(subject: &str, block: i64, powers: Value) -> NormalizedEvent {
    let source = json!({"kind":"raw_log", "source_event":"EACRolesChanged", "upstream_resource":ZERO_RESOURCE,
        "registry_contract_instance_id":Uuid::from_u128(0xA190), "root_resource":true, "changed_powers":powers});
    let revoked = powers.as_array().is_some_and(Vec::is_empty);
    let mut event = registry_event(&format!("registry-root-role-{subject}-{block}"), None,
        "RootPermissionChanged", block, ALPHA_REGISTRY,
        json!({"subject":subject, "scope":{"kind":"registry_root", "chain_id":"ethereum-mainnet",
                "registry_address":ALPHA_REGISTRY},
            "effective_powers":powers, "source_event":"EACRolesChanged", "upstream_resource":ZERO_RESOURCE,
            "resource":ZERO_RESOURCE, "registry_contract_instance_id":Uuid::from_u128(0xA190),
            "root_resource":true, "grant_source":if revoked { json!({}) } else { source.clone() },
            "revocation_source":if revoked { source } else { Value::Null },
            "inheritance_path":[{"kind":"registry_root_fallback", "chain_id":"ethereum-mainnet",
                "registry_address":ALPHA_REGISTRY, "upstream_resource":ZERO_RESOURCE}],
            "transfer_behavior":{}}));
    event.resource_id = Some(alpha_root());
    event
}

/// The alpha registry from the registry fixture, with four current root holders, one revoked
/// holder and a label-resource grant that a registry read must not return.
async fn seed_registry_root_holders(database: &TestDatabase) -> Result<()> {
    seed_registry_fixture(database).await?;
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, provenance, canonicality_state)
        VALUES ($1, 'ethereum-mainnet', 59, '0xregistry59', '{}', 'canonical')")
        .bind(alpha_root()).execute(&database.pool).await?;
    let mut events = ROOT_HOLDERS
        .iter()
        .rev()
        .enumerate()
        .map(|(index, holder)| root_role_event(holder, 70 + index as i64, json!(["registrar", "admin_registrar"])))
        .collect::<Vec<_>>();
    events.push(root_role_event(REVOKED_HOLDER, 75, json!(["renew"])));
    events.push(root_role_event(REVOKED_HOLDER, 76, json!([])));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    insert_registry_permission_roles(database, Uuid::from_u128(0xA100), false, 77, json!(["renew"])).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 83, "0xregistry83").await
}

fn root_scope() -> Value {
    json!({"kind":"root", "detail":{"registry":{"chain_id":1, "address":ALPHA_REGISTRY}}})
}

async fn permissions_error(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_permissions_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    let body: Value = read_json(response).await?;
    assert_eq!(body["error"]["code"], "invalid_input", "{uri}: {body}");
    Ok(body)
}

#[tokio::test]
async fn v2_get_permissions_lists_registry_root_holders_in_address_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_root_holders(&database).await?;
    let root = alpha_root().to_string();
    let selector = format!("1:{}", ALPHA_REGISTRY.to_ascii_uppercase().replacen("0X", "0x", 1));
    let mut uri = format!("/v1/permissions?registry={selector}&page_size=2");
    let mut holders = Vec::new();
    for page_number in 0..3 {
        let page = v2_permissions_payload_for_database(&database, &uri).await?;
        assert!(page.get("restrictions").is_none(), "{page}");
        assert!(page["meta"].get("completeness").is_none(), "{page}");
        assert!(page["meta"].get("unlisted_permission_surfaces").is_none(), "{page}");
        assert_eq!(page["page"]["total_count"], Value::Null);
        for row in page["data"].as_array().unwrap() {
            assert_eq!(row["grant_scope"], root_scope(), "{row}");
            assert_eq!(row["registration_id"], root, "{row}");
            assert_eq!(row["authority_context"], "resource_audit");
            assert_eq!(row["powers"], json!(["registrar", "admin_registrar"]));
            assert!(row.get("name").is_none() && row.get("grant_relation").is_none(), "{row}");
            holders.push(row["address"].as_str().unwrap().to_owned());
        }
        let Some(cursor) = page["page"]["next_cursor"].as_str() else {
            assert_eq!(page_number, 1, "two pages of two holders: {page}");
            break;
        };
        // The cursor binds the normalized selector, so the lowercase spelling continues it.
        uri = format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&page_size=2&cursor={cursor}");
    }
    assert_eq!(holders, ROOT_HOLDERS);

    let one = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&address={}&include=lineage", ROOT_HOLDERS[2])).await?;
    assert_eq!(one["data"].as_array().unwrap().len(), 1, "{one}");
    assert_eq!(one["data"][0]["address"], ROOT_HOLDERS[2]);
    assert_eq!(one["data"][0]["lineage"]["inheritance_path"], json!([{"kind":"registry_root_fallback"}]));
    let revoked = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&address={REVOKED_HOLDER}")).await?;
    assert_eq!(revoked["data"], json!([]), "{revoked}");

    // The same root rows on an address read now name their registry.
    let by_address = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?address={}", ROOT_HOLDERS[0])).await?;
    let rows = by_address["data"].as_array().unwrap();
    assert!(rows.iter().any(|row| row["registration_id"] == root && row["grant_scope"] == root_scope()), "{by_address}");
    let by_resource = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registration_id={root}")).await?;
    assert_eq!(by_resource["data"].as_array().unwrap().len(), ROOT_HOLDERS.len(), "{by_resource}");
    assert!(by_resource["data"].as_array().unwrap().iter().all(|row| row["grant_scope"] == root_scope()));

    for unknown in [
        "/v1/permissions?registry=1:0x00000000000000000000000000000000000000ff",
        "/v1/permissions?registry=8453:0x00000000000000000000000000000000000000a1",
    ] {
        let empty = v2_permissions_payload_for_database(&database, unknown).await?;
        assert_eq!(empty["data"], json!([]), "{empty}");
        assert_eq!(empty["page"]["next_cursor"], Value::Null);
        assert!(empty["meta"].get("completeness").is_none(), "{empty}");
        assert!(empty.get("restrictions").is_none(), "{empty}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_registry_cursor_binds_its_registry() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_root_holders(&database).await?;
    let first = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&page_size=1")).await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("more holders");
    for uri in [
        format!("/v1/permissions?registry=1:0x00000000000000000000000000000000000000ff&page_size=1&cursor={cursor}"),
        format!("/v1/permissions?address={}&page_size=1&cursor={cursor}", ROOT_HOLDERS[0]),
        format!("/v1/permissions?registration_id={}&page_size=1&cursor={cursor}", alpha_root()),
        format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&address={}&page_size=1&cursor={cursor}", ROOT_HOLDERS[0]),
        format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&namespace=ens&page_size=1&cursor={cursor}"),
    ] {
        permissions_error(&database, &uri).await?;
    }
    let namespaced = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&namespace=ens")).await?;
    assert_eq!(namespaced["data"].as_array().unwrap().len(), ROOT_HOLDERS.len(), "{namespaced}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_rejects_malformed_registry_selectors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let invalid = "registry must be <chain_id>:<address> with a supported numeric chain id";
    for registry in ["0x00000000000000000000000000000000000000a1", "1", "one:0x00000000000000000000000000000000000000a1",
        "10:0x00000000000000000000000000000000000000a1", "-1:0x00000000000000000000000000000000000000a1",
        "ethereum-mainnet:0x00000000000000000000000000000000000000a1"] {
        let body = permissions_error(&database, &format!("/v1/permissions?registry={registry}")).await?;
        assert_eq!(body["error"]["message"], invalid, "{registry}: {body}");
    }
    permissions_error(&database, "/v1/permissions?registry=1:0x1234").await?;
    for combination in ["name=alpha.eth", "registration_id=550e8400-e29b-41d4-a716-446655440000"] {
        let body = permissions_error(&database,
            &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&{combination}")).await?;
        assert_eq!(body["error"]["message"], "registry cannot be combined with name or registration_id");
    }
    permissions_error(&database, &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&at=2026-01-01T00:00:00Z")).await?;
    permissions_error(&database, &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&finality=safe")).await?;
    permissions_error(&database, &format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}&page_size=201")).await?;
    let body = permissions_error(&database, "/v1/permissions").await?;
    assert_eq!(body["error"]["message"], "at least one of name, registration_id, address, or registry is required");
    database.cleanup().await
}

// A migration WrapperRegistry gives the parent registry's root roles to the parent name's owner
// and that owner's operators on the parent registry, so its root holders are not all rows.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L273-L287 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn v2_get_permissions_marks_migration_registry_roots_partial() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_root_holders(&database).await?;
    let manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version, namespace, source_family,
            chain_id, deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES (1, 'ens', 'ens_v2_migration_l1', 'ethereum-mainnet', 'fixture', 'active', 'fixture',
            'fixture/migration.toml', '{}') RETURNING manifest_id")
        .fetch_one(&database.pool).await?;
    sqlx::query("INSERT INTO migration_discovery_associations (logical_edge_identity, migration_correlation_id,
            correlation_kind, registry_contract_instance_id, registry_address, source_manifest_id, evidence_refs,
            chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index,
            canonicality_state, consumer_visibility, interpreter_content_hash)
        VALUES ('alpha-edge', 'alpha-migration', 'migration_registry_creation', $1, $2, $3, '[{\"kind\":\"raw_log\"}]',
            'ethereum-mainnet', 60, '0xregistry60', '0xtx60', 0, 0, 'canonical', 'activated', 'fixture')")
        .bind(Uuid::from_u128(0xA190)).bind(ALPHA_REGISTRY).bind(manifest).execute(&database.pool).await?;
    let uri = format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}");
    // A creation record without its canonical announcement edge, such as one retained from a
    // losing fork, does not make the registry a migration registry.
    let unannounced = v2_permissions_payload_for_database(&database, &uri).await?;
    assert!(unannounced["meta"].get("completeness").is_none(), "{unannounced}");
    sqlx::query("INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
            discovery_source, admission_basis, source_manifest_id, active_from_block_number, active_from_block_hash,
            canonicality_state, provenance)
        VALUES ('ethereum-mainnet', 'registry_announcement', $1, $1, 'RegistryCreated', 'migration_registry_creation',
            $2, 60, '0xregistry60', 'canonical', '{\"transaction_index\":0,\"log_index\":0}')")
        .bind(Uuid::from_u128(0xA190)).bind(manifest).execute(&database.pool).await?;
    let page = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(page["data"].as_array().unwrap().len(), ROOT_HOLDERS.len(), "{page}");
    assert_eq!(page["meta"]["completeness"], "partial");
    assert_eq!(page["meta"]["unsupported_reason"], "permissions_partially_listed");
    assert_eq!(page["meta"]["unlisted_permission_surfaces"], json!(["ens_v2_registry_operators"]));
    database.cleanup().await
}

// The registry resolves at the captured publication: an instance admitted after it selects
// nothing, one retired after it still selects its published holders, and one dropped as if it
// never existed selects nothing.
#[tokio::test]
async fn v2_get_permissions_resolves_the_registry_at_the_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_root_holders(&database).await?;
    let uri = format!("/v1/permissions?registry=1:{ALPHA_REGISTRY}");
    let set = |sql: &'static str| sqlx::query(sql).bind(ALPHA_REGISTRY).execute(&database.pool);
    set("UPDATE contract_instance_addresses SET active_from_block_number = 90 WHERE address = $1").await?;
    let admitted_later = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(admitted_later["data"], json!([]), "{admitted_later}");
    set("UPDATE contract_instance_addresses SET active_from_block_number = 61, active_to_block_number = 85,
        deactivated_at = now() WHERE address = $1").await?;
    let retired_later = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(retired_later["data"].as_array().unwrap().len(), ROOT_HOLDERS.len(), "{retired_later}");
    set("UPDATE contract_instance_addresses SET active_to_block_number = 80 WHERE address = $1").await?;
    let retired = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(retired["data"], json!([]), "{retired}");
    set("UPDATE contract_instance_addresses SET active_to_block_number = NULL WHERE address = $1").await?;
    let dropped = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(dropped["data"], json!([]), "{dropped}");
    database.cleanup().await
}

#[path = "v2_permissions_registry_root_real.rs"]
mod real_logs;
