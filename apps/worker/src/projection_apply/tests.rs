use std::time::Duration;

use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    NORMALIZED_EVENT_DERIVE_BATCH_LIMIT, NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT,
    apply::apply_pending_invalidations,
    derive::{
        capture_normalized_event_change_watermark, derive_normalized_event_invalidations,
        derive_normalized_event_invalidations_through,
    },
    derive_once, has_primary_hydration_blocking_work,
};

const DIFFERENTIAL_CHANGE_COUNT: i64 = NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT * 2 + 2;
const ACROSS_UNITS_KEY: &str = "ens:differential-across-units.eth";
const FIRST_UNIT_ONLY_KEY: &str = "ens:differential-first-unit.eth";
const SECOND_UNIT_ONLY_KEY: &str = "ens:differential-second-unit.eth";
const FIRST_BOUNDARY_KEY: &str = "ens:differential-first-boundary.eth";
const SECOND_BOUNDARY_KEY: &str = "ens:differential-second-boundary.eth";

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_worker_projection_apply_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for worker projection apply tests")
            .admin_connect_context("failed to connect admin pool for worker projection apply tests")
            .pool_connect_context("failed to connect worker projection apply test pool"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for worker projection apply tests",
    )
    .await
}

#[tokio::test]
async fn stateless_replay_supersession_invalidates_and_rederives_projection() -> Result<()> {
    let database = test_database().await?;
    let address = "0x1111111111111111111111111111111111111111";
    let mut event = bigname_storage::NormalizedEvent {
        event_identity: "projection-apply:stateless-replay-supersession".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "ReverseChanged".to_owned(),
        source_family: "ens_v1_reverse_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(42),
        block_hash: Some(
            "0x4242424242424242424242424242424242424242424242424242424242424242".to_owned(),
        ),
        transaction_hash: Some(
            "0x4343434343434343434343434343434343434343434343434343434343434343".to_owned(),
        ),
        log_index: Some(1),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": 42,
            "block_hash": "0x4242424242424242424242424242424242424242424242424242424242424242",
            "transaction_hash": "0x4343434343434343434343434343434343434343434343434343434343434343",
            "log_index": 1
        }),
        derivation_kind: "ens_v1_reverse_claim".to_owned(),
        canonicality_state: bigname_storage::CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "address": address,
            "namespace": "ens",
            "coin_type": "60",
            "claim_provenance": {"derivation_vintage": "stale"}
        }),
    };
    bigname_storage::upsert_normalized_events(database.pool(), std::slice::from_ref(&event))
        .await?;
    assert_eq!(derive_once(database.pool()).await?.scanned_event_count, 1);
    assert_eq!(
        apply_pending_invalidations(database.pool(), 10, None)
            .await?
            .applied_invalidation_count,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT claim_provenance->>'derivation_vintage'
            FROM primary_names_current
            WHERE address = $1
              AND namespace = 'ens'
              AND coin_type = '60'
            "#,
        )
        .bind(address)
        .fetch_one(database.pool())
        .await?,
        "stale"
    );

    event.after_state["claim_provenance"]["derivation_vintage"] = json!("current");
    let authority = bigname_storage::upsert_normalized_events_with_stateless_replay_authority(
        database.pool(),
        std::slice::from_ref(&event),
    )
    .await?;
    assert_eq!(authority.identities_superseded, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM projection_normalized_event_changes change
            JOIN normalized_events event
              ON event.normalized_event_id = change.normalized_event_id
            WHERE event.event_identity = $1
              AND change.change_kind = 'canonicality_update'
            "#,
        )
        .bind(&event.event_identity)
        .fetch_one(database.pool())
        .await?,
        1
    );

    let derive = derive_once(database.pool()).await?;
    assert_eq!(derive.scanned_event_count, 1);
    assert_eq!(derive.enqueued_invalidation_count, 1);
    let apply = apply_pending_invalidations(database.pool(), 10, None).await?;
    assert_eq!(apply.applied_invalidation_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT claim_provenance->>'derivation_vintage'
            FROM primary_names_current
            WHERE address = $1
              AND namespace = 'ens'
              AND coin_type = '60'
            "#,
        )
        .bind(address)
        .fetch_one(database.pool())
        .await?,
        "current"
    );

    database.cleanup().await
}

#[tokio::test]
async fn derives_key_scoped_invalidations_from_normalized_event_changes() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:name-resolver",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(10),
            block_hash: Some("0xblock10"),
            before_state: json!({
                "resolver": "0x0000000000000000000000000000000000000bbb"
            }),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000aaa"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:permission",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(11),
            block_hash: Some("0xblock11"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000ccc"
                }
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:resource-permission",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(11),
            block_hash: Some("0xblock11"),
            before_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa1"
            }),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa2"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:address",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted",
            source_family: "ens_v1_registrar_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(12),
            block_hash: Some("0xblock12"),
            before_state: json!({}),
            after_state: json!({
                "registrant": "0x0000000000000000000000000000000000000ddd"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:primary",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ReverseChanged",
            source_family: "ens_v1_reverse_l1",
            derivation_kind: "ens_v1_reverse_claim",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(13),
            block_hash: Some("0xblock13"),
            before_state: json!({}),
            after_state: json!({
                "address": "0x0000000000000000000000000000000000000eee",
                "namespace": "ens",
                "coin_type": "60"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:primary-resolver",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(14),
            block_hash: Some("0xblock14"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000abc",
                "primary_claim_source": {
                    "address": "0x0000000000000000000000000000000000000fff",
                    "namespace": "ens",
                    "coin_type": "60",
                    "reverse_node": "0xreverse"
                }
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:children",
            namespace: "ens",
            logical_name_id: Some("ens:parent.eth"),
            resource_id: None,
            event_kind: "SubregistryChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_subregistry_changed",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(15),
            block_hash: Some("0xblock15"),
            before_state: json!({}),
            after_state: json!({}),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 7);
    assert!(summary.enqueued_invalidation_count >= 10);

    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(&invalidations, "name_current", "ens:alice.eth"));
    assert!(has_key(
        &invalidations,
        "children_current",
        "ens:parent.eth"
    ));
    assert!(has_key(
        &invalidations,
        "permissions_current",
        &resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000aaa"
    ));
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000bbb"
    ));
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000ccc"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000ddd:ens:alice.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000aa1:ens:alice.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000aa2:ens:alice.eth"
    ));
    assert_eq!(
        load_invalidation_payload(
            &database,
            "address_names_current",
            "0x0000000000000000000000000000000000000ddd:ens:alice.eth"
        )
        .await?,
        json!({
            "address": "0x0000000000000000000000000000000000000ddd",
            "logical_name_id": "ens:alice.eth"
        })
    );
    assert!(has_key(
        &invalidations,
        "primary_names_current",
        "0x0000000000000000000000000000000000000eee:ens:60"
    ));
    assert!(has_key(
        &invalidations,
        "primary_names_current",
        "0x0000000000000000000000000000000000000fff:ens:60"
    ));

    sqlx::query(
        r#"
        UPDATE normalized_events
        SET canonicality_state = 'orphaned'::canonicality_state,
            observed_at = $2
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:name-resolver")
    .bind(timestamp(1_800_000_100))
    .execute(database.pool())
    .await
    .context("failed to orphan normalized event")?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let generation = invalidation_generation(&database, "name_current", "ens:alice.eth").await?;
    assert_eq!(generation, 1);

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear generated projection invalidations")?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:resource-permission-revoked",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(15),
            block_hash: Some("0xblock15"),
            before_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa2",
                "effective_powers": ["resource_control"]
            }),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa2",
                "effective_powers": []
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000aa2:ens:alice.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000ddd:ens:alice.eth"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn split_derive_matches_single_batch_invalidation_end_state() -> Result<()> {
    let database = test_database().await?;
    seed_derive_differential_fixture(&database).await?;

    let across_units_range = (
        fixture_change_id(&database, 1).await?,
        fixture_change_id(&database, DIFFERENTIAL_CHANGE_COUNT).await?,
    );
    let first_unit_range = (
        fixture_change_id(&database, 50).await?,
        fixture_change_id(&database, 200).await?,
    );
    let second_unit_range = (
        fixture_change_id(&database, 300).await?,
        fixture_change_id(&database, 450).await?,
    );
    let first_boundary_range = (
        fixture_change_id(&database, NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT).await?,
        fixture_change_id(&database, NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT + 1).await?,
    );
    let second_boundary_range = (
        fixture_change_id(&database, NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT * 2).await?,
        fixture_change_id(&database, NORMALIZED_EVENT_DERIVE_PROGRESS_LIMIT * 2 + 1).await?,
    );

    let single_batch =
        derive_normalized_event_invalidations(database.pool(), NORMALIZED_EVENT_DERIVE_BATCH_LIMIT)
            .await?;
    assert_eq!(single_batch.scanned_event_count, DIFFERENTIAL_CHANGE_COUNT);
    assert_eq!(load_apply_cursor(&database).await?, across_units_range.1);
    let single_batch_rows = load_stable_invalidation_rows(&database).await?;
    assert_invalidation_range(&single_batch_rows, ACROSS_UNITS_KEY, across_units_range);
    assert_invalidation_range(&single_batch_rows, FIRST_UNIT_ONLY_KEY, first_unit_range);
    assert_invalidation_range(&single_batch_rows, SECOND_UNIT_ONLY_KEY, second_unit_range);
    assert_invalidation_range(&single_batch_rows, FIRST_BOUNDARY_KEY, first_boundary_range);
    assert_invalidation_range(
        &single_batch_rows,
        SECOND_BOUNDARY_KEY,
        second_boundary_range,
    );
    assert_eq!(
        invalidation_generation(&database, "name_current", ACROSS_UNITS_KEY).await?,
        0
    );

    reset_derive_output(&database).await?;

    let split = derive_once(database.pool()).await?;
    assert_eq!(split.scanned_event_count, DIFFERENTIAL_CHANGE_COUNT);
    assert_eq!(load_apply_cursor(&database).await?, across_units_range.1);
    let split_rows = load_stable_invalidation_rows(&database).await?;
    assert_stable_invalidation_rows_eq(&single_batch_rows, &split_rows);

    assert_eq!(
        invalidation_generation(&database, "name_current", ACROSS_UNITS_KEY).await?,
        2
    );
    assert_eq!(
        invalidation_generation(&database, "name_current", FIRST_BOUNDARY_KEY).await?,
        1
    );
    assert_eq!(
        invalidation_generation(&database, "name_current", SECOND_BOUNDARY_KEY).await?,
        1
    );
    assert_eq!(
        invalidation_generation(&database, "name_current", FIRST_UNIT_ONLY_KEY).await?,
        0
    );
    assert_eq!(
        invalidation_generation(&database, "name_current", SECOND_UNIT_ONLY_KEY).await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn derive_releases_complete_prefix_fence_before_processing_changes() -> Result<()> {
    let database = test_database().await?;
    let observed_at = timestamp(1_800_000_001);
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fenced-change",
            namespace: "ens",
            logical_name_id: Some("ens:fenced.eth"),
            resource_id: None,
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(1),
            block_hash: Some("0xfenced1"),
            before_state: json!({}),
            after_state: json!({}),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:post-bound-change",
            namespace: "ens",
            logical_name_id: Some("ens:post-bound.eth"),
            resource_id: None,
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(2),
            block_hash: Some("0xfenced2"),
            before_state: json!({}),
            after_state: json!({}),
            observed_at,
        },
    )
    .await?;
    sqlx::query("DELETE FROM projection_normalized_event_changes")
        .execute(database.pool())
        .await?;

    let first_change_id =
        insert_projection_change(&database, "projection-apply:fenced-change").await?;
    insert_invalidation(&database, "name_current", "ens:fenced.eth").await?;
    let mut invalidation_blocker = database.pool().begin().await?;
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET generation = generation
        WHERE projection = 'name_current'
          AND projection_key = 'ens:fenced.eth'
        "#,
    )
    .execute(&mut *invalidation_blocker)
    .await?;

    let derive_pool = database.pool().clone();
    let derive =
        tokio::spawn(async move { derive_normalized_event_invalidations(&derive_pool, 100).await });
    wait_for_transaction_lock(database.pool()).await?;

    let second_change_id = match tokio::time::timeout(
        Duration::from_secs(2),
        insert_projection_change(&database, "projection-apply:post-bound-change"),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            derive.abort();
            invalidation_blocker.rollback().await?;
            database.cleanup().await?;
            anyhow::bail!("change-log writer waited for the invalidation derive phase");
        }
    };
    assert!(second_change_id > first_change_id);

    invalidation_blocker.commit().await?;
    let first_summary = tokio::time::timeout(Duration::from_secs(2), derive)
        .await
        .context("derive remained blocked after invalidation row lock released")?
        .context("derive task failed")??;
    assert_eq!(first_summary.scanned_event_count, 1);
    assert_eq!(load_apply_cursor(&database).await?, first_change_id);

    let second_summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(second_summary.scanned_event_count, 1);
    assert_eq!(load_apply_cursor(&database).await?, second_change_id);

    database.cleanup().await
}

#[tokio::test]
async fn two_capturers_resume_from_the_serialized_apply_cursor() -> Result<()> {
    let database = test_database().await?;
    let observed_at = timestamp(1_800_000_002);
    for (event_identity, logical_name_id, block_number) in [
        ("projection-apply:capturer-one", "ens:capturer-one.eth", 1),
        ("projection-apply:capturer-two", "ens:capturer-two.eth", 2),
    ] {
        insert_event(
            &database,
            EventSeed {
                event_identity,
                namespace: "ens",
                logical_name_id: Some(logical_name_id),
                resource_id: None,
                event_kind: "ResolverChanged",
                source_family: "ens_v1_registry_l1",
                derivation_kind: "ens_v1_unwrapped_authority",
                chain_id: Some("ethereum-mainnet"),
                block_number: Some(block_number),
                block_hash: Some("0xtwo-capturers"),
                before_state: json!({}),
                after_state: json!({}),
                observed_at,
            },
        )
        .await?;
    }
    sqlx::query(
        r#"
        INSERT INTO projection_apply_cursors (cursor_name, last_change_id)
        VALUES ('normalized_events_to_projection_invalidations', 0)
        "#,
    )
    .execute(database.pool())
    .await?;

    let first_bound = capture_normalized_event_change_watermark(database.pool()).await?;
    let second_bound = capture_normalized_event_change_watermark(database.pool()).await?;
    assert_eq!(first_bound, second_bound);

    let first_pool = database.pool().clone();
    let first = tokio::spawn(async move {
        derive_normalized_event_invalidations_through(&first_pool, 1, first_bound).await
    });
    let second_pool = database.pool().clone();
    let second = tokio::spawn(async move {
        derive_normalized_event_invalidations_through(&second_pool, 1, second_bound).await
    });
    let (first_summary, second_summary) = tokio::try_join!(first, second)?;
    let first_summary = first_summary?;
    let second_summary = second_summary?;

    assert_eq!(first_summary.scanned_event_count, 1);
    assert_eq!(second_summary.scanned_event_count, 1);
    assert_eq!(load_apply_cursor(&database).await?, first_bound.change_id);

    database.cleanup().await
}

#[tokio::test]
async fn ensv2_parent_changed_invalidates_linked_children_parent() -> Result<()> {
    let database = test_database().await?;
    let parent_contract_instance_id = Uuid::from_u128(0x3801).to_string();
    let child_registry_contract_instance_id = Uuid::from_u128(0x3802).to_string();
    let observed_at = timestamp(1_800_000_010);

    insert_name_surface(&database, "ens:old.eth", "old.eth").await?;
    insert_name_surface(&database, "ens:alice.eth", "alice.eth").await?;
    insert_name_surface(&database, "ens:bob.alice.eth", "bob.alice.eth").await?;
    sqlx::query(
        r#"
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            surface_class,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            labelhash,
            owner,
            registrant,
            provenance,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            'ens:old.eth',
            'ens:alice.eth',
            'declared',
            'ens',
            'alice.eth',
            'alice.eth',
            'node:alice.eth',
            '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            NULL,
            NULL,
            '{"source":"projection_apply_test"}'::jsonb,
            '{}'::jsonb,
            '{"status":"finalized"}'::jsonb,
            1,
            $1
        ),
        (
            'ens:alice.eth',
            'ens:bob.alice.eth',
            'declared',
            'ens',
            'bob.alice.eth',
            'bob.alice.eth',
            'node:bob.alice.eth',
            '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            NULL,
            NULL,
            '{"source":"projection_apply_test"}'::jsonb,
            '{}'::jsonb,
            '{"status":"finalized"}'::jsonb,
            1,
            $1
        )
        "#,
    )
    .bind(observed_at)
    .execute(database.pool())
    .await
    .context("failed to seed stale old-parent children_current row")?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-subregistry-link",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: None,
            event_kind: "SubregistryChanged",
            source_family: "ens_v2_root_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(40),
            block_hash: Some("0xensv2subregistry40"),
            before_state: json!({}),
            after_state: json!({
                "source_event": "SubregistryUpdated",
                "from_contract_instance_id": parent_contract_instance_id,
                "to_contract_instance_id": child_registry_contract_instance_id
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear subregistry invalidation")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-parent-change",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ParentChanged",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(41),
            block_hash: Some("0xensv2parent41"),
            before_state: json!({}),
            after_state: json!({
                "source_event": "ParentUpdated",
                "registry_name": "alice.eth",
                "registry_contract_instance_id": child_registry_contract_instance_id,
                "parent_contract_instance_id": parent_contract_instance_id
            }),
            observed_at: timestamp(1_800_000_011),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(&invalidations, "children_current", "ens:alice.eth"));
    assert!(has_key(&invalidations, "children_current", "ens:old.eth"));
    assert_eq!(
        load_invalidation_payload(&database, "children_current", "ens:alice.eth").await?,
        json!({"parent_logical_name_id": "ens:alice.eth"})
    );
    assert_eq!(
        load_invalidation_payload(&database, "children_current", "ens:old.eth").await?,
        json!({"parent_logical_name_id": "ens:old.eth"})
    );

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear registry-source parent invalidation")?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-root-parent-change",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ParentChanged",
            source_family: "ens_v2_root_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(42),
            block_hash: Some("0xensv2parent42"),
            before_state: json!({}),
            after_state: json!({
                "source_event": "ParentUpdated",
                "registry_name": "alice.eth",
                "registry_contract_instance_id": child_registry_contract_instance_id,
                "parent_contract_instance_id": parent_contract_instance_id
            }),
            observed_at: timestamp(1_800_000_012),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(&invalidations, "children_current", "ens:alice.eth"));
    assert!(has_key(&invalidations, "children_current", "ens:old.eth"));

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear first parent invalidation")?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-parent-change-null-parent",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ParentChanged",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(43),
            block_hash: Some("0xensv2parent43"),
            before_state: json!({}),
            after_state: json!({
                "source_event": "ParentUpdated",
                "registry_name": "alice.eth",
                "registry_contract_instance_id": child_registry_contract_instance_id,
                "parent_contract_instance_id": null
            }),
            observed_at: timestamp(1_800_000_013),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(&invalidations, "children_current", "ens:alice.eth"));
    assert!(has_key(&invalidations, "children_current", "ens:old.eth"));
    assert_eq!(
        load_invalidation_payload(&database, "children_current", "ens:alice.eth").await?,
        json!({"parent_logical_name_id": "ens:alice.eth"})
    );
    assert_eq!(
        load_invalidation_payload(&database, "children_current", "ens:old.eth").await?,
        json!({"parent_logical_name_id": "ens:old.eth"})
    );

    database.cleanup().await
}

#[tokio::test]
async fn ensv2_terminal_role_events_invalidate_children_and_prior_resolver() -> Result<()> {
    let database = test_database().await?;
    let resolver = "0x0000000000000000000000000000000000000abc";
    let observed_at = timestamp(1_800_000_019);
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-terminal-subregistry",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: None,
            event_kind: "SubregistryChanged",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-sepolia"),
            block_number: Some(60),
            block_hash: Some("0xensv2terminal60"),
            before_state: json!({
                "subregistry": "0x00000000000000000000000000000000000000bb"
            }),
            after_state: json!({
                "source_event": "LabelUnregistered",
                "terminal_reason": "unregistered",
                "subregistry": null,
                "from_contract_instance_id": Uuid::from_u128(0x3811).to_string(),
                "to_contract_instance_id": null,
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-terminal-resolver",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: None,
            event_kind: "ResolverChanged",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-sepolia"),
            block_number: Some(60),
            block_hash: Some("0xensv2terminal60"),
            before_state: json!({"resolver": resolver}),
            after_state: json!({
                "source_event": "LabelUnregistered",
                "terminal_reason": "unregistered",
                "resolver": null,
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 2);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(&invalidations, "children_current", "ens:alice.eth"));
    assert!(has_key(
        &invalidations,
        "resolver_current",
        &format!("ethereum-sepolia:{resolver}")
    ));

    database.cleanup().await
}

#[tokio::test]
async fn ensv2_parent_changed_derives_moved_name_key_without_existing_children_rows() -> Result<()>
{
    let database = test_database().await?;
    let parent_contract_instance_id = Uuid::from_u128(0x3811).to_string();
    let child_registry_contract_instance_id = Uuid::from_u128(0x3812).to_string();
    let observed_at = timestamp(1_800_000_020);

    insert_name_surface(&database, "ens:alice.eth", "alice.eth").await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-subregistry-link-branch-3",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: None,
            event_kind: "SubregistryChanged",
            source_family: "ens_v2_root_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(50),
            block_hash: Some("0xensv2subregistry50"),
            before_state: json!({}),
            after_state: json!({
                "source_event": "SubregistryUpdated",
                "from_contract_instance_id": parent_contract_instance_id,
                "to_contract_instance_id": child_registry_contract_instance_id
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear subregistry bootstrap invalidation")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-parent-change-branch-3",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ParentChanged",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(51),
            block_hash: Some("0xensv2parent51"),
            before_state: json!({}),
            after_state: json!({
                "source_event": "ParentUpdated",
                "registry_name": "alice.eth",
                "registry_contract_instance_id": child_registry_contract_instance_id,
                "parent_contract_instance_id": parent_contract_instance_id
            }),
            observed_at: timestamp(1_800_000_021),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    let children_keys = invalidations
        .iter()
        .filter_map(|(projection, projection_key)| {
            (projection == "children_current").then_some(projection_key.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(children_keys, vec!["ens:alice.eth"]);
    assert_eq!(
        load_invalidation_payload(&database, "children_current", "ens:alice.eth").await?,
        json!({"parent_logical_name_id": "ens:alice.eth"})
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_names_permission_changes_invalidate_existing_authority_owner() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:authority-owner-before-permission",
            namespace: "ens",
            logical_name_id: Some("ens:controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "AuthorityTransferred",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(20),
            block_hash: Some("0xblock20"),
            before_state: json!({}),
            after_state: json!({
                "owner": "0x0000000000000000000000000000000000000a00"
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear authority-owner invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:permission-overrides-authority-owner",
            namespace: "ens",
            logical_name_id: Some("ens:controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(21),
            block_hash: Some("0xblock21"),
            before_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000b00",
                "effective_powers": []
            }),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000b00",
                "effective_powers": ["resource_control"]
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000a00:ens:controller.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000b00:ens:controller.eth"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn token_control_transfer_invalidates_seller_and_buyer_address_names() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);
    let seller = "0x0000000000000000000000000000000000000a00";
    let buyer = "0x0000000000000000000000000000000000000b00";

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-sale-transfer",
            namespace: "ens",
            logical_name_id: Some("ens:sale.eth"),
            resource_id: Some(resource_id),
            event_kind: "TokenControlTransferred",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-sepolia"),
            block_number: Some(21),
            block_hash: Some("0xensv2sale21"),
            before_state: json!({"from": seller}),
            after_state: json!({"to": buyer, "source_event": "TransferSingle"}),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(&invalidations, "name_current", "ens:sale.eth"));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        &format!("{seller}:ens:sale.eth")
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        &format!("{buyer}:ens:sale.eth")
    ));

    database.cleanup().await
}

#[tokio::test]
async fn address_names_permission_scope_changes_invalidate_controller_and_fallback_addresses()
-> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-address-registrant",
            namespace: "ens",
            logical_name_id: Some("ens:fused-controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted",
            source_family: "ens_v1_registrar_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(30),
            block_hash: Some("0xfuse30"),
            before_state: json!({}),
            after_state: json!({
                "registrant": "0x0000000000000000000000000000000000000bbb"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-address-controller",
            namespace: "ens",
            logical_name_id: Some("ens:fused-controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(31),
            block_hash: Some("0xfuse31"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000ccc",
                "effective_powers": ["resource_control"]
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear setup invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-address-scope",
            namespace: "ens",
            logical_name_id: Some("ens:fused-controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionScopeChanged",
            source_family: "ens_v1_wrapper_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(32),
            block_hash: Some("0xfuse32"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "fuses": 8
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000ccc:ens:fused-controller.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000bbb:ens:fused-controller.eth"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_resource_scope_fuse_changes_invalidate_resolver_permission_keys()
-> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-resolver-permission",
            namespace: "ens",
            logical_name_id: Some("ens:fused-resolver.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(40),
            block_hash: Some("0xfuse40"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000ccc"
                },
                "subject": "0x0000000000000000000000000000000000000aaa",
                "effective_powers": ["resolver_control"]
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear setup invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-resolver-scope",
            namespace: "ens",
            logical_name_id: Some("ens:fused-resolver.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionScopeChanged",
            source_family: "ens_v1_wrapper_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(41),
            block_hash: Some("0xfuse41"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "fuses": 8
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000ccc"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn manifest_events_enqueue_manifest_sensitive_projection_keys() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_name_surface(&database, "ens:manifest.eth", "manifest.eth").await?;
    insert_resource(&database, resource_id).await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:manifest-record",
            namespace: "ens",
            logical_name_id: Some("ens:manifest.eth"),
            resource_id: Some(resource_id),
            event_kind: "RecordChanged",
            source_family: "ens_v1_resolver_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(20),
            block_hash: Some("0xmanifest20"),
            before_state: json!({}),
            after_state: json!({
                "record_key": "text:email",
                "record_family": "text",
                "selector_key": "email"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:manifest-resolver",
            namespace: "ens",
            logical_name_id: Some("ens:manifest.eth"),
            resource_id: Some(resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(21),
            block_hash: Some("0xmanifest21"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000abc"
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear setup invalidations")?;

    for (index, event_kind) in [
        "SourceManifestUpdated",
        "CapabilityChanged",
        "ProxyImplementationChanged",
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query("DELETE FROM projection_invalidations")
            .execute(database.pool())
            .await
            .context("failed to clear manifest invalidations")?;
        let event_identity = format!("projection-apply:manifest-{event_kind}");
        insert_event(
            &database,
            EventSeed {
                event_identity: &event_identity,
                namespace: "ens",
                logical_name_id: None,
                resource_id: None,
                event_kind,
                source_family: "ens_v1_registry_l1",
                derivation_kind: "manifest_sync",
                chain_id: Some("ethereum-mainnet"),
                block_number: None,
                block_hash: None,
                before_state: json!({}),
                after_state: json!({
                    "manifest_version": 2 + index as i64,
                    "normalizer_version": "test"
                }),
                observed_at: timestamp(1_800_000_100 + index as i64),
            },
        )
        .await?;

        let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
        assert_eq!(summary.scanned_event_count, 1);
        let invalidations = load_invalidations(&database).await?;
        assert!(has_key(&invalidations, "name_current", "ens:manifest.eth"));
        assert!(has_key(
            &invalidations,
            "record_inventory_current",
            &resource_id.to_string()
        ));
        assert!(has_key(
            &invalidations,
            "resolver_current",
            "ethereum-mainnet:0x0000000000000000000000000000000000000abc"
        ));
    }

    database.cleanup().await
}

#[tokio::test]
async fn generation_bump_releases_in_flight_claim_for_serialized_reapply() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let claim_token = Uuid::new_v4();

    insert_resource(&database, resource_id).await?;
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            claim_token,
            claimed_at
        )
        VALUES (
            'permissions_current',
            $1,
            jsonb_build_object('resource_id', $1),
            $2,
            now()
        )
        "#,
    )
    .bind(resource_id.to_string())
    .bind(claim_token)
    .execute(database.pool())
    .await
    .context("failed to seed claimed projection invalidation")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:claimed-permission-change",
            namespace: "ens",
            logical_name_id: Some("ens:claimed.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(22),
            block_hash: Some("0xclaimed22"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000abc",
                "effective_powers": ["resource_control"]
            }),
            observed_at: timestamp(1_800_000_200),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);

    let (generation, retained_claim_token): (i64, Option<Uuid>) = sqlx::query_as(
        r#"
        SELECT generation, claim_token
        FROM projection_invalidations
        WHERE projection = 'permissions_current'
          AND projection_key = $1
        "#,
    )
    .bind(resource_id.to_string())
    .fetch_one(database.pool())
    .await
    .context("failed to load bumped projection invalidation")?;
    assert_eq!(generation, 1);
    assert_eq!(retained_claim_token, None);

    database.cleanup().await
}

#[tokio::test]
async fn root_permission_changes_invalidate_permissions_current() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:root-permission-change",
            namespace: "ens",
            logical_name_id: None,
            resource_id: Some(resource_id),
            event_kind: "RootPermissionChanged",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_permissions",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(23),
            block_hash: Some("0xroot23"),
            before_state: json!({
                "subject": "0x0000000000000000000000000000000000000bbb",
                "role_bitmap": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "effective_powers": []
            }),
            after_state: json!({
                "scope": {
                    "kind": "registry_root",
                    "chain_id": "ethereum-mainnet",
                    "registry_address": "0x0000000000000000000000000000000000000eee"
                },
                "subject": "0x0000000000000000000000000000000000000bbb",
                "role_bitmap": "0x0000000000000000000000000000000000000000000000000000000000000011",
                "effective_powers": ["registrar", "register_reserved"]
            }),
            observed_at: timestamp(1_800_000_210),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "permissions_current",
        &resource_id.to_string()
    ));

    database.cleanup().await
}

#[tokio::test]
async fn ensv2_registration_without_role_events_invalidates_permissions_summary() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    insert_resource(&database, resource_id).await?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-registration-without-role-events",
            namespace: "ens",
            logical_name_id: Some("ens:zero-role.eth"),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(24),
            block_hash: Some("0xensv2registration24"),
            before_state: json!({}),
            after_state: json!({
                "registrant": "0x0000000000000000000000000000000000000abc",
                "registry_contract_instance_id": Uuid::from_u128(0xe201),
                "upstream_resource": "0x00000000000000000000000000000000000000000000000000000000000073d0",
            }),
            observed_at: timestamp(1_800_000_220),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(
        has_key(
            &invalidations,
            "permissions_current",
            &resource_id.to_string()
        ),
        "ENSv2 RegistrationGranted must invalidate the zero-row permission summary for its resource"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ensv2_root_registration_and_reserved_resource_link_invalidate_permission_summaries()
-> Result<()> {
    let database = test_database().await?;
    let root_registration_resource_id = Uuid::new_v4();
    let reserved_resource_id = Uuid::new_v4();
    insert_resource(&database, root_registration_resource_id).await?;
    insert_resource(&database, reserved_resource_id).await?;

    for (resource_id, event_kind, source_family, event_identity, logical_name_id) in [
        (
            root_registration_resource_id,
            "RegistrationGranted",
            "ens_v2_root_l1",
            "projection-apply:ensv2-root-registration",
            "ens:eth",
        ),
        (
            reserved_resource_id,
            "TokenResourceLinked",
            "ens_v2_registry_l1",
            "projection-apply:ensv2-reserved-resource-link",
            "ens:reserved.eth",
        ),
    ] {
        insert_event(
            &database,
            EventSeed {
                event_identity,
                namespace: "ens",
                logical_name_id: Some(logical_name_id),
                resource_id: Some(resource_id),
                event_kind,
                source_family,
                derivation_kind: "ens_v2_registry_resource_surface",
                chain_id: Some("ethereum-mainnet"),
                block_number: Some(25),
                block_hash: Some("0xensv2permissionevidence25"),
                before_state: json!({}),
                after_state: json!({
                    "registry_contract_instance_id": Uuid::from_u128(0xe201),
                    "upstream_resource": resource_id.to_string(),
                }),
                observed_at: timestamp(1_800_000_225),
            },
        )
        .await?;
    }

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 2);
    let invalidations = load_invalidations(&database).await?;
    for resource_id in [root_registration_resource_id, reserved_resource_id] {
        assert!(has_key(
            &invalidations,
            "permissions_current",
            &resource_id.to_string()
        ));
    }

    database.cleanup().await
}

#[tokio::test]
async fn authority_epoch_changes_invalidate_zero_holder_permission_summaries() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:wrapper-authority-epoch",
            namespace: "ens",
            logical_name_id: Some("ens:wrapped.eth"),
            resource_id: Some(resource_id),
            event_kind: "AuthorityEpochChanged",
            source_family: "ens_v1_wrapper_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(24),
            block_hash: Some("0xwrapper24"),
            before_state: json!({"authority_kind": "registrar"}),
            after_state: json!({
                "authority_kind": "wrapper",
                "authority_key": "ens-v1-wrapper:wrapped.eth"
            }),
            observed_at: timestamp(1_800_000_220),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "permissions_current",
        &resource_id.to_string()
    ));

    database.cleanup().await
}

#[tokio::test]
async fn derives_record_inventory_cross_resource_invalidations_for_logical_name_dependencies()
-> Result<()> {
    let database = test_database().await?;
    let predecessor_resource_id = Uuid::new_v4();
    let current_resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_resource(&database, predecessor_resource_id).await?;
    insert_resource(&database, current_resource_id).await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:current-resolver",
            namespace: "ens",
            logical_name_id: Some("ens:carry.eth"),
            resource_id: Some(current_resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(21),
            block_hash: Some("0xcarry21"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000aaa"
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear initial projection invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:predecessor-resolver-boundary",
            namespace: "ens",
            logical_name_id: Some("ens:carry.eth"),
            resource_id: Some(predecessor_resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(19),
            block_hash: Some("0xcarry19"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000bbb"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:predecessor-record",
            namespace: "ens",
            logical_name_id: Some("ens:carry.eth"),
            resource_id: Some(predecessor_resource_id),
            event_kind: "RecordChanged",
            source_family: "ens_v1_resolver_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(20),
            block_hash: Some("0xcarry20"),
            before_state: json!({}),
            after_state: json!({
                "record_key": "text:email",
                "record_family": "text",
                "selector_key": "email"
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 2);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &predecessor_resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &current_resource_id.to_string()
    ));

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear cross-resource projection invalidations")?;
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET resource_id = NULL
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:predecessor-resolver-boundary")
    .execute(database.pool())
    .await
    .context("failed to unbind predecessor resolver event")?;
    sqlx::query(
        r#"
        INSERT INTO projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        SELECT
            normalized_event_id,
            $2,
            'canonicality_update',
            canonicality_state
        FROM normalized_events
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:predecessor-resolver-boundary")
    .bind(timestamp(1_800_000_100))
    .execute(database.pool())
    .await
    .context("failed to log predecessor resolver repair change")?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &current_resource_id.to_string()
    ));

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear repaired resolver invalidations")?;
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET canonicality_state = 'orphaned'::canonicality_state,
            observed_at = $2
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:predecessor-record")
    .bind(timestamp(1_800_000_200))
    .execute(database.pool())
    .await
    .context("failed to orphan predecessor record event")?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &current_resource_id.to_string()
    ));

    database.cleanup().await
}

#[tokio::test]
async fn ensv2_registry_resolver_changes_invalidate_current_and_successor_record_inventory()
-> Result<()> {
    let database = test_database().await?;
    let resolver_resource_id = Uuid::new_v4();
    let successor_resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_001_000);

    insert_resource(&database, resolver_resource_id).await?;
    insert_resource(&database, successor_resource_id).await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-successor-record",
            namespace: "ens",
            logical_name_id: Some("ens:resolver-switch.eth"),
            resource_id: Some(successor_resource_id),
            event_kind: "RecordChanged",
            source_family: "ens_v2_resolver_l1",
            derivation_kind: "ens_v2_resolver",
            chain_id: Some("ethereum-sepolia"),
            block_number: Some(22),
            block_hash: Some("0xensv2switch22"),
            before_state: json!({}),
            after_state: json!({
                "record_key": "text:avatar",
                "record_family": "text",
                "selector_key": "avatar"
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear successor setup invalidation")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:ensv2-registry-resolver-switch",
            namespace: "ens",
            logical_name_id: Some("ens:resolver-switch.eth"),
            resource_id: Some(resolver_resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v2_registry_l1",
            derivation_kind: "ens_v2_registry_resource_surface",
            chain_id: Some("ethereum-sepolia"),
            block_number: Some(21),
            block_hash: Some("0xensv2switch21"),
            before_state: json!({
                "resolver": "0x0000000000000000000000000000000000000200"
            }),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000201"
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &resolver_resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &successor_resource_id.to_string()
    ));

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear ENSv2 resolver-switch invalidations")?;
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET canonicality_state = 'orphaned'::canonicality_state,
            observed_at = $2
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:ensv2-registry-resolver-switch")
    .bind(timestamp(1_800_001_100))
    .execute(database.pool())
    .await
    .context("failed to orphan ENSv2 registry resolver switch")?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &resolver_resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &successor_resource_id.to_string()
    ));

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_ignores_unrelated_retry_delayed_failures() -> Result<()> {
    let database = test_database().await?;

    insert_failed_invalidation(
        &database,
        "name_current",
        "ens:retry-delayed.eth",
        "1 second",
    )
    .await?;
    assert!(
        !has_primary_hydration_blocking_work(database.pool()).await?,
        "recently failed unrelated invalidations must not starve primary hydration"
    );

    insert_failed_invalidation(&database, "name_current", "ens:claimable.eth", "2 minutes").await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "failed invalidations become pending once the retry delay expires"
    );

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_detects_primary_retry_delayed_failures() -> Result<()> {
    let database = test_database().await?;

    insert_failed_invalidation(
        &database,
        "primary_names_current",
        "0x0000000000000000000000000000000000000aaa:ens:60",
        "1 second",
    )
    .await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "recently failed primary_names_current invalidations can affect hydration inputs"
    );

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_ignores_dead_lettered_invalidations() -> Result<()> {
    let database = test_database().await?;

    sqlx::query(
        r#"
        INSERT INTO projection_invalidation_dead_letters (
            projection,
            projection_key,
            key_payload,
            attempt_count,
            generation,
            last_changed_at,
            invalidated_at,
            last_failure_reason,
            last_failure_at,
            dead_lettered_at
        )
        VALUES (
            'primary_names_current',
            '0x0000000000000000000000000000000000000aaa:ens:60',
            jsonb_build_object(
                'address', '0x0000000000000000000000000000000000000aaa',
                'namespace', 'ens',
                'coin_type', '60'
            ),
            5,
            0,
            now(),
            now(),
            'poisoned test invalidation',
            now(),
            now() - '10 minutes'::INTERVAL
        )
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to seed dead-lettered primary_names_current invalidation")?;

    assert!(
        !has_primary_hydration_blocking_work(database.pool()).await?,
        "dead-lettered invalidations are operator-visible terminal failures, not claimable blocking work"
    );

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_detects_claimable_invalidations_and_cursor_lag()
-> Result<()> {
    let database = test_database().await?;
    assert!(!has_primary_hydration_blocking_work(database.pool()).await?);

    insert_invalidation(&database, "name_current", "ens:pending.eth").await?;
    assert!(has_primary_hydration_blocking_work(database.pool()).await?);

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear pending invalidation")?;
    assert!(!has_primary_hydration_blocking_work(database.pool()).await?);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:pending-work-cursor-lag",
            namespace: "ens",
            logical_name_id: Some("ens:cursor-lag.eth"),
            resource_id: Some(Uuid::new_v4()),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(30),
            block_hash: Some("0xcursor30"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000aaa"
            }),
            observed_at: timestamp(1_800_000_300),
        },
    )
    .await?;
    assert!(has_primary_hydration_blocking_work(database.pool()).await?);

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_detects_active_claimed_invalidations() -> Result<()> {
    let database = test_database().await?;

    insert_claimed_invalidation(&database, "name_current", "ens:in-flight.eth", "1 minute").await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "freshly claimed invalidations are active apply work and must block hydration"
    );

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear active claimed invalidation")?;
    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:stale-claim.eth",
        "10 minutes",
    )
    .await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "expired claims are claimable apply work and must block hydration"
    );

    database.cleanup().await
}

async fn seed_derive_differential_fixture(database: &TestDatabase) -> Result<()> {
    let observed_at = timestamp(1_800_000_400);
    for position in 1..=DIFFERENTIAL_CHANGE_COUNT {
        let event_identity = fixture_event_identity(position);
        let logical_name_id = match position {
            1 | 275 | DIFFERENTIAL_CHANGE_COUNT => ACROSS_UNITS_KEY.to_owned(),
            50 | 200 => FIRST_UNIT_ONLY_KEY.to_owned(),
            250 | 251 => FIRST_BOUNDARY_KEY.to_owned(),
            300 | 450 => SECOND_UNIT_ONLY_KEY.to_owned(),
            500 | 501 => SECOND_BOUNDARY_KEY.to_owned(),
            _ => format!("ens:differential-{position}.eth"),
        };
        insert_event(
            database,
            EventSeed {
                event_identity: &event_identity,
                namespace: "ens",
                logical_name_id: Some(&logical_name_id),
                resource_id: None,
                event_kind: "DifferentialChange",
                source_family: "test",
                derivation_kind: "projection_apply_differential_test",
                chain_id: None,
                block_number: None,
                block_hash: None,
                before_state: json!({}),
                after_state: json!({}),
                observed_at,
            },
        )
        .await?;
    }
    Ok(())
}

fn fixture_event_identity(position: i64) -> String {
    format!("projection-apply:differential-{position}")
}

async fn fixture_change_id(database: &TestDatabase, position: i64) -> Result<i64> {
    let event_identity = fixture_event_identity(position);
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT change.change_id
        FROM projection_normalized_event_changes change
        JOIN normalized_events event
          ON event.normalized_event_id = change.normalized_event_id
        WHERE event.event_identity = $1
        "#,
    )
    .bind(&event_identity)
    .fetch_one(database.pool())
    .await
    .with_context(|| format!("failed to load projection change for {event_identity}"))
}

async fn reset_derive_output(database: &TestDatabase) -> Result<()> {
    let mut transaction = database.pool().begin().await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(&mut *transaction)
        .await
        .context("failed to reset differential projection invalidations")?;
    sqlx::query(
        r#"
        UPDATE projection_apply_cursors
        SET last_change_id = 0,
            updated_at = now()
        WHERE cursor_name = 'normalized_events_to_projection_invalidations'
        "#,
    )
    .execute(&mut *transaction)
    .await
    .context("failed to reset differential projection apply cursor")?;
    transaction
        .commit()
        .await
        .context("failed to commit differential derive reset")
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct StableProjectionInvalidationRow {
    projection: String,
    projection_key: String,
    key_payload: Value,
    first_change_id: Option<i64>,
    last_change_id: Option<i64>,
    first_normalized_event_id: Option<i64>,
    last_normalized_event_id: Option<i64>,
    last_changed_at: OffsetDateTime,
    claim_token: Option<Uuid>,
    claimed_at: Option<OffsetDateTime>,
    attempt_count: i64,
    last_failure_reason: Option<String>,
    last_failure_at: Option<OffsetDateTime>,
    state: String,
}

async fn load_stable_invalidation_rows(
    database: &TestDatabase,
) -> Result<Vec<StableProjectionInvalidationRow>> {
    sqlx::query_as::<_, StableProjectionInvalidationRow>(
        r#"
        SELECT
            projection,
            projection_key,
            key_payload,
            first_change_id,
            last_change_id,
            first_normalized_event_id,
            last_normalized_event_id,
            last_changed_at,
            claim_token,
            claimed_at,
            attempt_count,
            last_failure_reason,
            last_failure_at,
            state::TEXT AS state
        FROM projection_invalidations
        ORDER BY projection, projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to load stable projection invalidation rows")
}

fn assert_invalidation_range(
    invalidations: &[StableProjectionInvalidationRow],
    projection_key: &str,
    expected_range: (i64, i64),
) {
    let invalidation = invalidations
        .iter()
        .find(|row| row.projection == "name_current" && row.projection_key == projection_key)
        .unwrap_or_else(|| panic!("missing name_current invalidation for {projection_key}"));
    assert_eq!(
        (invalidation.first_change_id, invalidation.last_change_id),
        (Some(expected_range.0), Some(expected_range.1)),
        "unexpected merged change range for {projection_key}"
    );
}

fn assert_stable_invalidation_rows_eq(
    expected: &[StableProjectionInvalidationRow],
    actual: &[StableProjectionInvalidationRow],
) {
    assert_eq!(actual.len(), expected.len());
    for (expected, actual) in expected.iter().zip(actual) {
        assert_eq!(
            actual, expected,
            "stable invalidation row differs for {}:{}",
            expected.projection, expected.projection_key
        );
    }
}

fn has_key(invalidations: &[(String, String)], projection: &str, projection_key: &str) -> bool {
    invalidations
        .iter()
        .any(|(candidate_projection, candidate_key)| {
            candidate_projection == projection && candidate_key == projection_key
        })
}

async fn load_invalidations(database: &TestDatabase) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        ORDER BY projection, projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to load projection invalidations")?;

    rows.into_iter()
        .map(|row| Ok((row.try_get("projection")?, row.try_get("projection_key")?)))
        .collect()
}

async fn invalidation_generation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT generation
        FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .fetch_one(database.pool())
    .await
    .context("failed to load projection invalidation generation")
}

async fn load_invalidation_payload(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<Value> {
    sqlx::query_scalar::<_, Value>(
        r#"
        SELECT key_payload
        FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .fetch_one(database.pool())
    .await
    .context("failed to load projection invalidation payload")
}

async fn insert_resource(database: &TestDatabase, resource_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            $1,
            'ethereum-mainnet',
            $2,
            1,
            'finalized'
        )
        "#,
    )
    .bind(resource_id)
    .bind(format!("0x{}", resource_id.simple()))
    .execute(database.pool())
    .await
    .context("failed to insert projection apply test resource")?;
    Ok(())
}

async fn insert_name_surface(
    database: &TestDatabase,
    logical_name_id: &str,
    normalized_name: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            $1,
            'ens',
            $2,
            $2,
            $2,
            '\x00'::bytea,
            $3,
            ARRAY[]::TEXT[],
            'test',
            'ethereum-mainnet',
            '0xsurface',
            1,
            'finalized'::canonicality_state
        )
        "#,
    )
    .bind(logical_name_id)
    .bind(normalized_name)
    .bind(format!("0x{name}", name = normalized_name.replace('.', "")))
    .execute(database.pool())
    .await
    .context("failed to insert projection apply test name surface")?;
    Ok(())
}

async fn insert_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES ($1, $2, '{}'::jsonb)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .execute(database.pool())
    .await
    .context("failed to insert projection invalidation")?;

    Ok(())
}

async fn insert_failed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
    failure_age: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            last_failure_at
        )
        VALUES ($1, $2, '{}'::jsonb, now() - $3::INTERVAL)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .bind(failure_age)
    .execute(database.pool())
    .await
    .context("failed to insert failed projection invalidation")?;

    Ok(())
}

async fn insert_claimed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
    claim_age: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            claim_token,
            claimed_at
        )
        VALUES ($1, $2, '{}'::jsonb, $3, now() - $4::INTERVAL)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .bind(Uuid::new_v4())
    .bind(claim_age)
    .execute(database.pool())
    .await
    .context("failed to insert claimed projection invalidation")?;

    Ok(())
}

struct EventSeed<'a> {
    event_identity: &'a str,
    namespace: &'a str,
    logical_name_id: Option<&'a str>,
    resource_id: Option<Uuid>,
    event_kind: &'a str,
    source_family: &'a str,
    derivation_kind: &'a str,
    chain_id: Option<&'a str>,
    block_number: Option<i64>,
    block_hash: Option<&'a str>,
    before_state: Value,
    after_state: Value,
    observed_at: OffsetDateTime,
}

async fn insert_event(database: &TestDatabase, event: EventSeed<'_>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state,
            observed_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, 1, $7, $8, $9,
            '{}'::jsonb, $10, 'canonical'::canonicality_state, $11, $12, $13
        )
        "#,
    )
    .bind(event.event_identity)
    .bind(event.namespace)
    .bind(event.logical_name_id)
    .bind(event.resource_id)
    .bind(event.event_kind)
    .bind(event.source_family)
    .bind(event.chain_id)
    .bind(event.block_number)
    .bind(event.block_hash)
    .bind(event.derivation_kind)
    .bind(event.before_state)
    .bind(event.after_state)
    .bind(event.observed_at)
    .execute(database.pool())
    .await
    .with_context(|| format!("failed to insert normalized event {}", event.event_identity))?;

    Ok(())
}

async fn insert_projection_change(database: &TestDatabase, event_identity: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        SELECT
            normalized_event_id,
            now(),
            'canonicality_update',
            canonicality_state
        FROM normalized_events
        WHERE event_identity = $1
        RETURNING change_id
        "#,
    )
    .bind(event_identity)
    .fetch_one(database.pool())
    .await
    .with_context(|| format!("failed to insert projection change for {event_identity}"))
}

async fn load_apply_cursor(database: &TestDatabase) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_change_id
        FROM projection_apply_cursors
        WHERE cursor_name = 'normalized_events_to_projection_invalidations'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load normalized-event apply cursor")
}

async fn wait_for_transaction_lock(pool: &sqlx::PgPool) -> Result<()> {
    for _ in 0..500 {
        let waiting = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks waiting_lock
                JOIN pg_stat_activity activity
                  ON activity.pid = waiting_lock.pid
                WHERE activity.datname = current_database()
                  AND waiting_lock.locktype = 'transactionid'
                  AND NOT waiting_lock.granted
            )
            "#,
        )
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("derive did not wait on the held invalidation row")
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("fixed timestamp must be valid")
}
