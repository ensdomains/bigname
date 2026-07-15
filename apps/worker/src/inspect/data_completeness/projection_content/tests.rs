use std::path::Path;

use anyhow::Result;
use bigname_manifests::{load_repository, sync_repository};
use bigname_storage::ActiveManifestEventSource;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::load_projection_content;

const CHAIN: &str = "ethereum-sepolia";
const NAMESPACE: &str = "ens";

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("worker_projection_content_inspection")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse projection content test database URL")
            .admin_connect_context("failed to connect projection content admin pool")
            .pool_connect_context("failed to connect projection content test pool"),
        &bigname_storage::MIGRATOR,
        "failed to migrate projection content test database",
    )
    .await
}

fn source(event_kinds: &[&str]) -> ActiveManifestEventSource {
    ActiveManifestEventSource {
        manifest_id: 1,
        manifest_version: 1,
        chain: CHAIN.to_owned(),
        namespace: NAMESPACE.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        normalized_event_kinds: event_kinds.iter().map(|kind| (*kind).to_owned()).collect(),
        normalized_event_count: 1,
        normalized_events_missing_canonical_lineage_count: 0,
        normalized_events_missing_canonical_raw_log_count: 0,
    }
}

async fn seed_all_current_projections(database: &TestDatabase) -> Result<()> {
    sqlx::raw_sql(
        r#"
        INSERT INTO name_surfaces
            (logical_name_id, namespace, input_name, canonical_display_name,
             normalized_name, dns_encoded_name, namehash, labelhashes,
             normalizer_version, chain_id, block_hash, block_number, canonicality_state)
        VALUES
            ('ens:eth', 'ens', 'eth', 'eth', 'eth', '\x'::bytea, '0xparent', '{}',
             'test', 'ethereum-sepolia', '0xblock', 1, 'canonical'),
            ('ens:alice.eth', 'ens', 'alice.eth', 'alice.eth', 'alice.eth', '\x'::bytea,
             '0xchild', '{}', 'test', 'ethereum-sepolia', '0xblock', 1, 'canonical');

        INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
        VALUES
            ('11111111-1111-1111-1111-111111111111', 'ethereum-sepolia',
             '0xblock', 1, 'canonical');

        INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, active_from,
             chain_id, block_hash, block_number, canonicality_state)
        VALUES
            ('22222222-2222-2222-2222-222222222222', 'ens:eth',
             '11111111-1111-1111-1111-111111111111', 'declared_registry_path', now(),
             'ethereum-sepolia', '0xblock', 1, 'canonical');

        INSERT INTO name_current
            (logical_name_id, namespace, canonical_display_name, normalized_name, namehash,
             manifest_version)
        VALUES ('ens:eth', 'ens', 'eth', 'eth', '0xparent', 1);

        INSERT INTO children_current
            (parent_logical_name_id, child_logical_name_id, namespace,
             canonical_display_name, normalized_name, namehash, manifest_version)
        VALUES ('ens:eth', 'ens:alice.eth', 'ens', 'alice.eth', 'alice.eth', '0xchild', 1);

        INSERT INTO permissions_current
            (resource_id, subject, scope, scope_kind, manifest_version)
        VALUES
            ('11111111-1111-1111-1111-111111111111', '0xsubject', 'resource', 'resource', 1);

        INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, manifest_version)
        VALUES ('11111111-1111-1111-1111-111111111111', 'boundary', 1);

        INSERT INTO resolver_current (chain_id, resolver_address, manifest_version)
        VALUES ('ethereum-sepolia', '0xresolver', 1);

        INSERT INTO address_names_current
            (address, logical_name_id, relation, namespace, canonical_display_name,
             normalized_name, namehash, surface_binding_id, resource_id, binding_kind,
             manifest_version)
        VALUES
            ('0xowner', 'ens:eth', 'effective_controller', 'ens', 'eth', 'eth', '0xparent',
             '22222222-2222-2222-2222-222222222222',
             '11111111-1111-1111-1111-111111111111', 'declared_registry_path', 1);

        INSERT INTO primary_names_current (address, coin_type, namespace)
        VALUES ('0xowner', '60', 'ens');
        "#,
    )
    .execute(database.pool())
    .await?;
    Ok(())
}

#[tokio::test]
async fn truncating_one_non_name_projection_is_reported_by_table_and_scope() -> Result<()> {
    let database = test_database().await?;
    seed_all_current_projections(&database).await?;
    let all_kinds = source(&[
        "SubregistryChanged",
        "PermissionChanged",
        "RecordChanged",
        "ResolverChanged",
        "RegistrationGranted",
        "ReverseChanged",
    ]);

    let complete =
        load_projection_content(database.pool(), std::slice::from_ref(&all_kinds)).await?;
    assert!(complete.complete());

    sqlx::query("TRUNCATE resolver_current")
        .execute(database.pool())
        .await?;
    let truncated = load_projection_content(database.pool(), &[all_kinds]).await?;
    assert!(!truncated.complete());
    let resolver = truncated
        .tables
        .iter()
        .find(|table| table.projection == "resolver_current")
        .expect("resolver_current table report");
    assert_eq!(resolver.raw_total_count, 0);
    assert_eq!(resolver.servable_total_count, 0);
    assert_eq!(resolver.missing_scopes, vec![CHAIN.to_owned()]);

    database.cleanup().await
}

#[tokio::test]
async fn projection_without_declared_input_kind_may_be_empty() -> Result<()> {
    let database = test_database().await?;
    seed_all_current_projections(&database).await?;
    sqlx::query("TRUNCATE permissions_current")
        .execute(database.pool())
        .await?;

    let inspection =
        load_projection_content(database.pool(), &[source(&["ResolverChanged"])]).await?;
    assert!(inspection.complete());
    let permissions = inspection
        .tables
        .iter()
        .find(|table| table.projection == "permissions_current")
        .expect("permissions_current table report");
    assert_eq!(permissions.raw_total_count, 0);
    assert_eq!(permissions.servable_total_count, 0);
    assert!(permissions.expected_scopes.is_empty());
    assert!(permissions.missing_scopes.is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn name_scope_requires_a_servable_surface_anchor() -> Result<()> {
    let database = test_database().await?;
    seed_all_current_projections(&database).await?;
    let sources = [source(&["ResolverChanged"])];

    sqlx::query(
        "UPDATE name_surfaces SET canonicality_state = 'orphaned' WHERE logical_name_id = 'ens:eth'",
    )
    .execute(database.pool())
    .await?;
    let orphaned = load_projection_content(database.pool(), &sources).await?;
    let names = orphaned
        .tables
        .iter()
        .find(|table| table.projection == "name_current")
        .expect("name_current table report");
    assert_eq!(names.raw_total_count, 1);
    assert_eq!(names.servable_total_count, 0);
    assert_eq!(names.missing_scopes, vec![NAMESPACE.to_owned()]);
    assert!(!orphaned.complete());

    sqlx::query(
        "UPDATE name_surfaces SET canonicality_state = 'canonical' WHERE logical_name_id = 'ens:eth'",
    )
    .execute(database.pool())
    .await?;
    let canonical = load_projection_content(database.pool(), &sources).await?;
    assert!(canonical.complete());

    database.cleanup().await
}

#[tokio::test]
async fn resource_projections_require_rows_on_each_expected_chain() -> Result<()> {
    let database = test_database().await?;
    seed_all_current_projections(&database).await?;
    sqlx::raw_sql(
        r#"
        TRUNCATE permissions_current, record_inventory_current;

        INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
        VALUES
            ('33333333-3333-3333-3333-333333333333', 'base-mainnet',
             '0xforeign', 1, 'canonical');

        INSERT INTO permissions_current
            (resource_id, subject, scope, scope_kind, manifest_version)
        VALUES
            ('33333333-3333-3333-3333-333333333333', '0xsubject', 'resource', 'resource', 1);

        INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, manifest_version)
        VALUES ('33333333-3333-3333-3333-333333333333', 'foreign-boundary', 1);
        "#,
    )
    .execute(database.pool())
    .await?;
    let sources = [source(&["PermissionChanged", "RecordChanged"])];

    let foreign_only = load_projection_content(database.pool(), &sources).await?;
    for projection in ["permissions_current", "record_inventory_current"] {
        let table = foreign_only
            .tables
            .iter()
            .find(|table| table.projection == projection)
            .expect("resource projection table report");
        assert_eq!(table.scope_kind, "chain");
        assert_eq!(table.missing_scopes, vec![CHAIN.to_owned()]);
        assert_eq!(table.raw_scoped_counts[0].scope, "base-mainnet");
        assert_eq!(table.servable_scoped_counts[0].scope, "base-mainnet");
    }
    assert!(!foreign_only.complete());

    sqlx::raw_sql(
        r#"
        INSERT INTO permissions_current
            (resource_id, subject, scope, scope_kind, manifest_version)
        VALUES
            ('11111111-1111-1111-1111-111111111111', '0xsubject', 'resource', 'resource', 1);

        INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, manifest_version)
        VALUES ('11111111-1111-1111-1111-111111111111', 'active-boundary', 1);
        "#,
    )
    .execute(database.pool())
    .await?;
    assert!(
        load_projection_content(database.pool(), &sources)
            .await?
            .complete()
    );

    database.cleanup().await
}

#[tokio::test]
async fn checked_in_mainnet_reverse_adapter_requires_primary_name_content() -> Result<()> {
    let database = test_database().await?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet");
    let repository = load_repository(&root)?;
    sync_repository(database.pool(), &repository).await?;
    let adapter_kinds = bigname_adapters::adapter_normalized_event_kind_declarations();
    let read = bigname_storage::load_data_completeness_with_adapter_event_kinds(
        database.pool(),
        &adapter_kinds,
    )
    .await?;
    let reverse_source = read
        .active_manifest_event_sources
        .iter()
        .find(|source| source.source_family == "ens_v1_reverse_l1")
        .expect("checked-in ENS reverse source");
    assert_eq!(
        reverse_source.normalized_event_kinds,
        vec!["ReverseChanged"]
    );
    for source_family in [
        "ens_v1_registrar_l1",
        "ens_v1_wrapper_l1",
        "basenames_base_registrar",
    ] {
        let source = read
            .active_manifest_event_sources
            .iter()
            .find(|source| source.source_family == source_family)
            .expect("checked-in block-derived source");
        assert!(
            source
                .normalized_event_kinds
                .iter()
                .any(|kind| kind == "PreimageObserved")
        );
    }

    let inspection =
        load_projection_content(database.pool(), &read.active_manifest_event_sources).await?;
    let primary_names = inspection
        .tables
        .iter()
        .find(|table| table.projection == "primary_names_current")
        .expect("primary_names_current table report");
    assert!(!primary_names.expected_scopes.is_empty());
    assert!(primary_names.expected_scopes.contains(&"ens".to_owned()));
    assert!(primary_names.missing_scopes.contains(&"ens".to_owned()));
    assert_eq!(primary_names.raw_total_count, 0);
    assert_eq!(primary_names.servable_total_count, 0);
    assert!(!inspection.complete());

    database.cleanup().await
}
