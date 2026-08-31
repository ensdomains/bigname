use super::*;
use anyhow::Result as AnyResult;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

#[test]
fn address_range_only_admits_topics_from_its_manifest() {
    let filter = WatchFilter {
        address_ranges: vec![AddressRange {
            address: "0x01".to_owned(),
            from_block: 10,
            to_block: 20,
            topic0s: vec!["0xaa".to_owned()],
        }],
        all_emitter_ranges: Vec::new(),
        registry_announcements: None,
    };

    assert!(filter.includes("0x01", "0xaa", 10));
    assert!(!filter.includes("0x01", "0xbb", 10));
    assert!(!filter.includes("0x01", "0xaa", 21));
}

#[test]
fn registry_announcements_are_collected_from_all_emitters() {
    let topic = registry_announcement_topic0();

    assert_eq!(
        all_emitter_topics(ENS_V2_REGISTRY_SOURCE_FAMILY, std::slice::from_ref(&topic)),
        vec![topic]
    );
}

#[test]
fn announced_registry_topics_are_address_scoped_forward_only() {
    let mut filter = WatchFilter {
        address_ranges: Vec::new(),
        all_emitter_ranges: Vec::new(),
        registry_announcements: Some(RegistryAnnouncementWatch {
            announcement_topic0: "0xaa".to_owned(),
            scoped_topic0s: vec!["0xbb".to_owned()],
        }),
    };

    let queries = filter.admit_registry_announcements([("0x01".to_owned(), 10)], 0, 20);

    assert!(!filter.includes("0x01", "0xbb", 9));
    assert!(filter.includes("0x01", "0xbb", 10));
    assert_eq!(
        queries,
        [WatchQuery {
            from_block: 10,
            to_block: 20,
            addresses: vec!["0x01".to_owned()],
            topic0s: vec!["0xbb".to_owned()],
        }]
    );
}

#[test]
fn ens_v2_resolver_signatures_remain_address_scoped() {
    let name_changed = format!(
        "{}",
        alloy_primitives::keccak256("NameChanged(bytes32,string)".as_bytes())
    );
    let upgraded = format!(
        "{}",
        alloy_primitives::keccak256("Upgraded(address)".as_bytes())
    );

    assert!(all_emitter_topics("ens_v2_resolver_l1", &[name_changed, upgraded]).is_empty());
}

#[test]
fn query_windows_do_not_cross_product_manifest_topics() {
    let filter = WatchFilter {
        address_ranges: vec![
            AddressRange {
                address: "0x01".to_owned(),
                from_block: 10,
                to_block: 20,
                topic0s: vec!["0xaa".to_owned()],
            },
            AddressRange {
                address: "0x02".to_owned(),
                from_block: 10,
                to_block: 20,
                topic0s: vec!["0xbb".to_owned()],
            },
        ],
        all_emitter_ranges: Vec::new(),
        registry_announcements: None,
    };

    assert_eq!(
        filter.queries(),
        vec![
            WatchQuery {
                from_block: 10,
                to_block: 20,
                addresses: vec!["0x01".to_owned()],
                topic0s: vec!["0xaa".to_owned()],
            },
            WatchQuery {
                from_block: 10,
                to_block: 20,
                addresses: vec!["0x02".to_owned()],
                topic0s: vec!["0xbb".to_owned()],
            },
        ]
    );
}

#[test]
fn generic_resolver_topics_scan_all_emitters() {
    let filter = WatchFilter {
        address_ranges: Vec::new(),
        all_emitter_ranges: vec![AllEmitterRange {
            from_block: 10,
            to_block: 20,
            topic0s: vec!["0xaa".to_owned()],
        }],
        registry_announcements: None,
    };

    assert!(filter.includes("0x-unlisted", "0xaa", 10));
    assert_eq!(
        filter.queries(),
        vec![WatchQuery {
            from_block: 10,
            to_block: 20,
            addresses: Vec::new(),
            topic0s: vec!["0xaa".to_owned()],
        }]
    );
}

#[test]
fn aliased_root_and_contract_ranges_form_one_provider_query() {
    let address = "0x00000000000000000000000000000000000000aa".to_owned();
    let generic = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let approval = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let filter = WatchFilter {
        address_ranges: vec![
            AddressRange {
                address: address.clone(),
                from_block: 10,
                to_block: 20,
                topic0s: vec![generic.to_owned()],
            },
            AddressRange {
                address: address.clone(),
                from_block: 10,
                to_block: 20,
                topic0s: vec![generic.to_owned(), approval.to_owned()],
            },
        ],
        all_emitter_ranges: Vec::new(),
        registry_announcements: None,
    };

    assert_eq!(
        filter.queries(),
        [WatchQuery {
            from_block: 10,
            to_block: 20,
            addresses: vec![address],
            topic0s: vec![generic.to_owned(), approval.to_owned()],
        }],
        "a root alias must not duplicate generic-topic provider traffic from its contract declaration"
    );
}

#[test]
fn only_existing_generic_resolver_topics_are_selected_without_addresses() {
    let generic = generic_resolver_topic0s()[0].clone();
    let shared = format!(
        "{}",
        alloy_primitives::keccak256("ApprovalForAll(address,address,bool)".as_bytes())
    );

    assert_eq!(
        all_emitter_topics(
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            &[generic.clone(), shared.clone()],
        ),
        vec![generic.clone()]
    );
    assert!(all_emitter_topics(BASENAMES_BASE_RESOLVER_SOURCE_FAMILY, &[shared]).is_empty());
    assert_eq!(
        all_emitter_topics(
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
            std::slice::from_ref(&generic),
        ),
        vec![generic]
    );
}

#[tokio::test]
async fn runtime_filter_keeps_approvals_role_and_interval_scoped() -> AnyResult<()> {
    let database = range_database("ingest_address_scoped_approvals").await?;
    let chain_id = "approval-watch-chain";
    let allowed = "0x00000000000000000000000000000000000000aa";
    let old_generation = "0x00000000000000000000000000000000000000bb";
    let foreign = "0x00000000000000000000000000000000000000cc";
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": ENS_V1_RESOLVER_SOURCE_FAMILY,
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "resolver_implementations": [],
        "capability_flags": {},
        "roots": [{"name":"ResolverRootAlias", "address":allowed, "start_block":0}],
        "contracts": [
            {"role":"public_resolver", "address":allowed, "proxy_kind":"none", "start_block":10},
            {"role":"public_resolver_old", "address":old_generation, "proxy_kind":"none", "start_block":0}
        ],
        "discovery_rules": [],
        "abi": {"events": [
            {
                "name":"NameChanged",
                "fragment":"event NameChanged(bytes32 indexed node, string name)",
                "emitter_roles":[],
                "normalized_events":["RecordChanged"]
            },
            {
                "name":"ApprovalForAll",
                "fragment":"event ApprovalForAll(address indexed owner, address indexed operator, bool approved)",
                "emitter_roles":["public_resolver"],
                "normalized_events":[]
            },
            {
                "name":"Approved",
                "fragment":"event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved)",
                "emitter_roles":["public_resolver"],
                "normalized_events":[]
            }
        ], "calls":[]}
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES (
             1, 'ens', $1, $2, 'fixture', 'active',
             'ensip15@ens-normalize-0.1.1', 'tests/approval-watch.toml', $3
         ) RETURNING manifest_id",
    )
    .bind(ENS_V1_RESOLVER_SOURCE_FAMILY)
    .bind(chain_id)
    .bind(payload)
    .fetch_one(database.pool())
    .await?;
    let allowed_instance = insert_contract_instance(database.pool(), chain_id).await?;
    let old_instance = insert_contract_instance(database.pool(), chain_id).await?;
    for (instance, address) in [(allowed_instance, allowed), (old_instance, old_generation)] {
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address, active_from_block_number,
                 source_manifest_id, provenance
             ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
        )
        .bind(instance)
        .bind(chain_id)
        .bind(address)
        .bind(manifest_id)
        .execute(database.pool())
        .await?;
    }
    for (kind, name, instance, address, role, start) in [
        (
            "root",
            "ResolverRootAlias",
            allowed_instance,
            allowed,
            None,
            0_i64,
        ),
        (
            "contract",
            "public_resolver",
            allowed_instance,
            allowed,
            Some("public_resolver"),
            10_i64,
        ),
        (
            "contract",
            "public_resolver_old",
            old_instance,
            old_generation,
            Some("public_resolver_old"),
            0_i64,
        ),
    ] {
        sqlx::query(
            "INSERT INTO manifest_contract_instances (
                 manifest_id, chain_id, declaration_kind, declaration_name,
                 contract_instance_id, declared_address, role, proxy_kind,
                 start_block_number
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'none', $8)",
        )
        .bind(manifest_id)
        .bind(chain_id)
        .bind(kind)
        .bind(name)
        .bind(instance)
        .bind(address)
        .bind(role)
        .bind(start)
        .execute(database.pool())
        .await?;
    }

    let filter = load_persisted_watch_filter(database.pool(), chain_id, 0, 20).await?;
    let approval_for_all = format!(
        "{}",
        alloy_primitives::keccak256("ApprovalForAll(address,address,bool)".as_bytes())
    );
    let approved = format!(
        "{}",
        alloy_primitives::keccak256("Approved(address,bytes32,address,bool)".as_bytes())
    );
    let name_changed = format!(
        "{}",
        alloy_primitives::keccak256("NameChanged(bytes32,string)".as_bytes())
    );
    assert!(!filter.includes(allowed, &approval_for_all, 9));
    assert!(filter.includes(allowed, &approval_for_all, 10));
    assert!(filter.includes(allowed, &approved, 10));
    assert!(!filter.includes(old_generation, &approval_for_all, 10));
    assert!(!filter.includes(foreign, &approved, 10));
    assert!(filter.includes(foreign, &name_changed, 10));
    for query in filter.queries().iter().filter(|query| {
        query.topic0s.contains(&approval_for_all) || query.topic0s.contains(&approved)
    }) {
        assert_eq!(query.addresses, [allowed]);
        assert_eq!(query.from_block, 10);
    }
    assert!(filter.queries().iter().all(|query| {
        !query.addresses.is_empty()
            || (!query.topic0s.contains(&approval_for_all) && !query.topic0s.contains(&approved))
    }));
    database.cleanup().await
}

#[tokio::test]
async fn persisted_registry_announcement_is_watched_before_window_supplement() -> AnyResult<()> {
    let database = range_database("ingest_persisted_migration_registry_restart").await?;
    let chain_id = "migration-registry-restart-chain";
    let proxy = "0x00000000000000000000000000000000000000cc";
    let from_id = insert_contract_instance(database.pool(), chain_id).await?;
    let proxy_id = insert_contract_instance(database.pool(), chain_id).await?;
    let manifest_id = insert_registry_watch_manifest(database.pool(), chain_id).await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 10, $4, '{}'::jsonb)",
    )
    .bind(proxy_id)
    .bind(chain_id)
    .bind(proxy)
    .bind(manifest_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (
             chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
             discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, canonicality_state,
             provenance
         ) VALUES (
             $1, 'registry_announcement', $2, $3, 'RegistryCreated',
             'reachable_from_root', $4, 10, $5, 'finalized',
             '{\"transaction_index\":0,\"log_index\":4}'::jsonb
         )",
    )
    .bind(chain_id)
    .bind(from_id)
    .bind(proxy_id)
    .bind(manifest_id)
    .bind(format!("0x{}", "33".repeat(32)))
    .execute(database.pool())
    .await?;

    // This is the filter loaded at restart, before Ingest sees the new window and before the
    // same-window announcement supplement can contribute an address.
    let filter = load_persisted_watch_filter(database.pool(), chain_id, 11, 20).await?;
    let parent_updated = format!(
        "{}",
        alloy_primitives::keccak256("ParentUpdated(address,string,address)".as_bytes())
    );
    assert!(filter.includes(proxy, &parent_updated, 11));
    assert!(filter.queries().iter().any(|query| {
        query.from_block == 11
            && query.to_block == 20
            && query.addresses == [proxy]
            && query.topic0s.contains(&parent_updated)
    }));
    database.cleanup().await
}

#[tokio::test]
async fn planner_rejects_inverted_declared_bounds_with_manifest_name() -> AnyResult<()> {
    let database = range_database("ingest_inverted_manifest_range").await?;
    let chain_id = "inverted-range-chain";
    let file_path = "tests/inverted-range.toml";
    let address = "0x00000000000000000000000000000000000000aa";
    let contract_id = insert_contract_instance(database.pool(), chain_id).await?;
    let manifest_id = insert_manifest(
        database.pool(),
        chain_id,
        file_path,
        json!([{
            "role": "registry",
            "address": address,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 20
        }]),
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4,
                'registry', 'none', 20)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_id)
    .bind(address)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, active_to_block_number,
            source_manifest_id, deactivated_at, provenance
        )
        VALUES ($1, $2, $3, 0, 10, $4, now(), '{}'::jsonb)
        ",
    )
    .bind(contract_id)
    .bind(chain_id)
    .bind(address)
    .bind(manifest_id)
    .execute(database.pool())
    .await?;

    let error = load_watch_filter(database.pool(), chain_id, 0, 30)
        .await
        .expect_err("an inverted declared interval must stop planning");
    let message = error.to_string();
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(message.contains(file_path), "unexpected error: {message}");
    assert!(
        message.contains("inverted watch bounds")
            && message.contains("start block 20")
            && message.contains("end block 10"),
        "unexpected error: {message}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn planner_omits_inverted_row_when_instance_has_valid_range() -> AnyResult<()> {
    let database = range_database("ingest_inverted_manifest_range_with_sibling").await?;
    let chain_id = "inverted-range-sibling-chain";
    let file_path = "tests/inverted-range-sibling.toml";
    let address = "0x00000000000000000000000000000000000000aa";
    let contract_id = insert_contract_instance(database.pool(), chain_id).await?;
    let manifest_id = insert_manifest(
        database.pool(),
        chain_id,
        file_path,
        json!([{
            "role": "registry",
            "address": address,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 20
        }]),
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4,
                'registry', 'none', 20)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_id)
    .bind(address)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, active_to_block_number,
            source_manifest_id, deactivated_at, provenance
        )
        VALUES
            ($1, $2, $3, 0, 10, $4, now(), '{}'::jsonb),
            ($1, $2, $3, 11, NULL, $4, NULL, '{}'::jsonb)
        ",
    )
    .bind(contract_id)
    .bind(chain_id)
    .bind(address)
    .bind(manifest_id)
    .execute(database.pool())
    .await?;

    let queries = load_watch_filter(database.pool(), chain_id, 0, 30)
        .await?
        .queries();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].from_block, 20);
    assert_eq!(queries[0].to_block, 30);
    assert_eq!(queries[0].addresses, [address]);
    database.cleanup().await
}

#[tokio::test]
async fn planner_rejects_non_overlapping_discovery_and_address_windows() -> AnyResult<()> {
    let database = range_database("ingest_disjoint_discovery_range").await?;
    let chain_id = "disjoint-range-chain";
    let file_path = "tests/disjoint-range.toml";
    let address = "0x00000000000000000000000000000000000000bb";
    let from_id = insert_contract_instance(database.pool(), chain_id).await?;
    let target_id = insert_contract_instance(database.pool(), chain_id).await?;
    let manifest_id = insert_manifest(database.pool(), chain_id, file_path, json!([])).await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 20, $4, '{}'::jsonb)
        ",
    )
    .bind(target_id)
    .bind(chain_id)
    .bind(address)
    .bind(manifest_id)
    .execute(database.pool())
    .await?;
    let boundary_hash = format!("0x{}", "11".repeat(32));
    sqlx::query(
        "
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id,
            to_contract_instance_id, discovery_source, admission_basis,
            source_manifest_id, active_from_block_number,
            active_from_block_hash, active_to_block_number,
            active_to_block_hash, canonicality_state, deactivated_at,
            provenance
        )
        VALUES ($1, 'registry_announcement', $2, $3, 'fixture',
                'reachable_from_root', $4, 0, $5, 10, $5,
                'finalized', now(), '{}'::jsonb)
        ",
    )
    .bind(chain_id)
    .bind(from_id)
    .bind(target_id)
    .bind(manifest_id)
    .bind(boundary_hash)
    .execute(database.pool())
    .await?;

    let error = load_watch_filter(database.pool(), chain_id, 0, 30)
        .await
        .expect_err("disjoint discovery and address windows must stop planning");
    let message = error.to_string();
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(message.contains(file_path), "unexpected error: {message}");
    assert!(
        message.contains("non-overlapping watch windows")
            && message.contains("edge 0..=10")
            && message.contains("address 20..=unbounded"),
        "unexpected error: {message}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn planner_ignores_orphaned_discovery_windows() -> AnyResult<()> {
    let database = range_database("ingest_orphaned_discovery_range").await?;
    let chain_id = "orphaned-range-chain";
    let file_path = "tests/orphaned-range.toml";
    let declared_address = "0x00000000000000000000000000000000000000aa";
    let orphaned_address = "0x00000000000000000000000000000000000000bb";
    let declared_id = insert_contract_instance(database.pool(), chain_id).await?;
    let target_id = insert_contract_instance(database.pool(), chain_id).await?;
    let manifest_id = insert_manifest(
        database.pool(),
        chain_id,
        file_path,
        json!([{
            "role": "registry",
            "address": declared_address,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }]),
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4,
                'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(declared_id)
    .bind(declared_address)
    .execute(database.pool())
    .await?;
    for (contract_id, address, active_from) in [
        (declared_id, declared_address, 0_i64),
        (target_id, orphaned_address, 20_i64),
    ] {
        sqlx::query(
            "
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address,
                active_from_block_number, source_manifest_id, provenance
            )
            VALUES ($1, $2, $3, $4, $5, '{}'::jsonb)
            ",
        )
        .bind(contract_id)
        .bind(chain_id)
        .bind(address)
        .bind(active_from)
        .bind(manifest_id)
        .execute(database.pool())
        .await?;
    }
    let boundary_hash = format!("0x{}", "22".repeat(32));
    sqlx::query(
        "
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id,
            to_contract_instance_id, discovery_source, admission_basis,
            source_manifest_id, active_from_block_number,
            active_from_block_hash, active_to_block_number,
            active_to_block_hash, canonicality_state, deactivated_at,
            provenance
        )
        VALUES ($1, 'registry_announcement', $2, $3, 'fixture',
                'reachable_from_root', $4, 0, $5, 10, $5,
                'orphaned', now(), '{}'::jsonb)
        ",
    )
    .bind(chain_id)
    .bind(declared_id)
    .bind(target_id)
    .bind(manifest_id)
    .bind(boundary_hash)
    .execute(database.pool())
    .await?;

    let filter = load_watch_filter(database.pool(), chain_id, 0, 30).await?;
    let watched_addresses = filter
        .queries()
        .into_iter()
        .flat_map(|query| query.addresses)
        .collect::<Vec<_>>();
    assert!(watched_addresses.contains(&declared_address.to_owned()));
    assert!(!watched_addresses.contains(&orphaned_address.to_owned()));
    database.cleanup().await
}

async fn range_database(name: &str) -> AnyResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for schema in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
    ] {
        sqlx::raw_sql(schema).execute(database.pool()).await?;
    }
    Ok(database)
}

async fn insert_contract_instance(pool: &PgPool, chain_id: &str) -> AnyResult<Uuid> {
    let contract_id = Uuid::new_v4();
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        ",
    )
    .bind(contract_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    Ok(contract_id)
}

async fn insert_manifest(
    pool: &PgPool,
    chain_id: &str,
    file_path: &str,
    contracts: Value,
) -> AnyResult<i64> {
    let payload = json!({
        "manifest_version": 1,
        "namespace": "test",
        "source_family": "test_ranges",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "resolver_implementations": [],
        "capability_flags": {},
        "roots": [],
        "contracts": contracts,
        "discovery_rules": [],
        "abi": {
            "events": [{
                "name": "Transfer",
                "fragment": "event Transfer(address indexed from, address indexed to, uint256 value)",
                "emitter_roles": [],
                "normalized_events": []
            }],
            "calls": []
        }
    });
    Ok(sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version,
            file_path, manifest_payload
        )
        VALUES (1, 'test', 'test_ranges', $1, 'fixture', 'active',
                'ensip15@ens-normalize-0.1.1', $2, $3)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(file_path)
    .bind(payload)
    .fetch_one(pool)
    .await?)
}

async fn insert_registry_watch_manifest(pool: &PgPool, chain_id: &str) -> AnyResult<i64> {
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": ENS_V2_REGISTRY_SOURCE_FAMILY,
        "chain": chain_id,
        "deployment_epoch": "migration-restart-fixture",
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "resolver_implementations": [],
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [{
            "edge_kind": "registry_announcement",
            "from_role": "registry",
            "event": "RegistryCreated",
            "admission": "reachable_from_root"
        }],
        "abi": {
            "events": [
                {
                    "name": "RegistryCreated",
                    "fragment": "event RegistryCreated()",
                    "emitter_roles": [],
                    "normalized_events": ["RegistryCreated"]
                },
                {
                    "name": "ParentUpdated",
                    "fragment": "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                    "emitter_roles": ["registry"],
                    "normalized_events": ["ParentChanged"]
                }
            ],
            "calls": []
        }
    });
    Ok(sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES (
             1, 'ens', $1, $2, 'migration-restart-fixture', 'active',
             'ensip15@ens-normalize-0.1.1', 'tests/migration-restart.toml', $3
         ) RETURNING manifest_id",
    )
    .bind(ENS_V2_REGISTRY_SOURCE_FAMILY)
    .bind(chain_id)
    .bind(payload)
    .fetch_one(pool)
    .await?)
}
