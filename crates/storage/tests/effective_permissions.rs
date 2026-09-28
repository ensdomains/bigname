//! Effective permission contracts over publications produced by the family reducers.
#[path = "../../project/tests/families_support/mod.rs"]
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{FamilyMode, FamilyOptions};
use bigname_storage::{
    EffectivePermissionScope, PermissionGrantRelation, PermissionsCurrentAccountResourceCursor,
    load_bounded_effective_permissions_by_resource_ids, load_serving_effective_permissions_page,
};
use families_support::{CHAIN, CONTENT_HASH, Event, Fixture};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

const OWNER: &str = "0x0000000000000000000000000000000000000a11";
const SUBJECT: &str = "0x0000000000000000000000000000000000000b22";
const REGISTRY: &str = "0x0000000000000000000000000000000000000c33";
const OTHER: &str = "0x0000000000000000000000000000000000000d44";

async fn binding(
    fixture: &Fixture,
    resource: Uuid,
    owner: &str,
    contract: &str,
    log: i64,
) -> Result<()> {
    fixture
        .write(
            1,
            log,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            None,
            Some(&resource.to_string()),
            json!({"source_event":"Transfer", "owner":owner,
            "owner_getter":owner, "authority_kind":"registry_only"}),
            contract,
        )
        .await?;
    Ok(())
}

async fn approval(fixture: &Fixture, owner: &str, log: i64) -> Result<()> {
    fixture.write(2, log, "AccountPermissionChanged", "ens_v1_registry_l1", None, None,
        json!({"subject":SUBJECT,"relation_kind":"operator","approved":true,
            "scope":{"kind":"account","chain_id":CHAIN,"authority_kind":"registry",
                "authority_contract":REGISTRY,"owner":owner}, "effective_powers":["registry_control"],
            "grant_source":{"kind":"raw_log","source_event":"ApprovalForAll"},
            "revocation_source":null,"inheritance_path":[],"transfer_behavior":{}}), REGISTRY).await?;
    Ok(())
}

async fn grant(fixture: &Fixture, resource: Uuid, log: i64) -> Result<()> {
    fixture
        .write(
            2,
            log,
            "PermissionChanged",
            "ens_v2_resolver_l1",
            None,
            Some(&resource.to_string()),
            json!({"subject":SUBJECT,"scope":{"kind":"resolver","chain_id":CHAIN,"resolver_address":REGISTRY},
            "effective_powers":["set_text"], "grant_source":{"kind":"raw_log","source_event":"EACRolesChanged"},
            "revocation_source":null,"inheritance_path":[],"transfer_behavior":{}}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

async fn rebuild(fixture: &Fixture) -> Result<()> {
    let outcome = fixture.apply(2, FamilyMode::Rebuild).await?;
    ensure!(outcome.marker.as_ref().map(|marker| marker.number) == Some(2));
    Ok(())
}

async fn fixture() -> Result<(Fixture, Uuid)> {
    let fixture = Fixture::new("effective_permissions", 2).await?;
    let resource = Uuid::from_u128(0x60501);
    binding(&fixture, resource, OWNER, REGISTRY, 0).await?;
    approval(&fixture, OWNER, 0).await?;
    rebuild(&fixture).await?;
    Ok((fixture, resource))
}

async fn count(pool: &PgPool, resource: Uuid, namespace: Option<&str>) -> Result<usize> {
    Ok(load_serving_effective_permissions_page(
        pool,
        Some(SUBJECT),
        Some(resource),
        namespace,
        None,
        100,
    )
    .await?
    .rows
    .len())
}

#[tokio::test]
async fn effective_permissions_require_matching_chain_contract_and_owner() -> Result<()> {
    for (owner, contract) in [(OTHER, REGISTRY), (OWNER, OTHER)] {
        let (fixture, resource) = fixture().await?;
        assert_eq!(count(&fixture.pool, resource, None).await?, 1);
        binding(&fixture, resource, owner, contract, 1).await?;
        rebuild(&fixture).await?;
        assert_eq!(count(&fixture.pool, resource, None).await?, 0);
        fixture.cleanup().await?;
    }
    let (fixture, resource) = fixture().await?;
    fixture.lineage("other-chain", 2).await?;
    sqlx::query("UPDATE normalized_events SET chain_id='other-chain' WHERE event_kind='AccountPermissionChanged'")
        .execute(&fixture.pool).await?;
    rebuild(&fixture).await?;
    fixture.apply_on("other-chain", 2).await?;
    assert_eq!(count(&fixture.pool, resource, None).await?, 0);
    fixture.cleanup().await
}

#[tokio::test]
async fn effective_permissions_serve_only_approved_operator_rows() -> Result<()> {
    let (fixture, resource) = fixture().await?;
    sqlx::query("UPDATE normalized_events SET after_state = after_state || '{\"approved\":false,\"effective_powers\":[],\"revocation_source\":{}}' WHERE event_kind='AccountPermissionChanged'")
        .execute(&fixture.pool).await?;
    rebuild(&fixture).await?;
    assert_eq!(count(&fixture.pool, resource, None).await?, 0);
    fixture.cleanup().await
}

#[tokio::test]
async fn effective_permissions_require_a_current_registry_owner_binding() -> Result<()> {
    let (fixture, resource) = fixture().await?;
    fixture
        .write(
            2,
            1,
            "SurfaceUnbound",
            "ens_v1_registry_l1",
            None,
            Some(&resource.to_string()),
            json!({}),
            REGISTRY,
        )
        .await?;
    rebuild(&fixture).await?;
    assert_eq!(count(&fixture.pool, resource, None).await?, 0);
    fixture.cleanup().await
}

#[tokio::test]
async fn effective_permissions_rebuild_excludes_orphaned_authority_and_approval_inputs()
-> Result<()> {
    for kind in ["AuthorityTransferred", "AccountPermissionChanged"] {
        let (fixture, resource) = fixture().await?;
        sqlx::query(
            "UPDATE normalized_events SET canonicality_state='orphaned' WHERE event_kind=$1",
        )
        .bind(kind)
        .execute(&fixture.pool)
        .await?;
        rebuild(&fixture).await?;
        assert_eq!(count(&fixture.pool, resource, None).await?, 0, "{kind}");
        fixture.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn effective_permissions_namespace_filter_requires_retained_membership() -> Result<()> {
    let (fixture, resource) = fixture().await?;
    assert_eq!(count(&fixture.pool, resource, Some("ens")).await?, 1);
    assert_eq!(count(&fixture.pool, resource, Some("basenames")).await?, 0);
    sqlx::query("UPDATE normalized_events SET canonicality_state='orphaned' WHERE resource_id=$1")
        .bind(resource)
        .execute(&fixture.pool)
        .await?;
    rebuild(&fixture).await?;
    assert_eq!(count(&fixture.pool, resource, Some("ens")).await?, 0);
    fixture.cleanup().await
}

#[tokio::test]
async fn effective_permissions_page_direct_and_operator_rows_without_gaps() -> Result<()> {
    let (fixture, resource) = fixture().await?;
    grant(&fixture, resource, 1).await?;
    rebuild(&fixture).await?;
    let first = load_serving_effective_permissions_page(
        &fixture.pool,
        Some(SUBJECT),
        Some(resource),
        None,
        None,
        1,
    )
    .await?;
    assert!(
        first.summary.is_none(),
        "a page must not aggregate the entire relation"
    );
    let second = load_serving_effective_permissions_page(
        &fixture.pool,
        Some(SUBJECT),
        Some(resource),
        None,
        first.next_cursor.as_ref(),
        1,
    )
    .await?;
    assert_eq!((first.rows.len(), second.rows.len()), (1, 1));
    assert_ne!(first.rows[0].scope, second.rows[0].scope);
    assert!(second.next_cursor.is_none());
    fixture.cleanup().await
}

#[tokio::test]
async fn effective_permissions_bounded_batch_keeps_operator_scope_and_budget() -> Result<()> {
    let (fixture, resource) = fixture().await?;
    let rows =
        load_bounded_effective_permissions_by_resource_ids(&fixture.pool, &[resource], None, 10)
            .await?;
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].scope,
        EffectivePermissionScope::Account { .. }
    ));
    assert_eq!(
        rows[0].grant_relation,
        Some(PermissionGrantRelation::Operator)
    );
    grant(&fixture, resource, 1).await?;
    fixture.write(2, 2, "PermissionChanged", "ens_v2_resolver_l1", None,
        Some(&resource.to_string()), json!({"subject":OWNER,
            "scope":{"kind":"resolver","chain_id":CHAIN,"resolver_address":REGISTRY},
            "effective_powers":["set_text"],"grant_source":{"kind":"raw_log","source_event":"EACRolesChanged"},
            "inheritance_path":[],"transfer_behavior":{}}), REGISTRY).await?;
    rebuild(&fixture).await?;
    let bounded =
        load_bounded_effective_permissions_by_resource_ids(&fixture.pool, &[resource], None, 1)
            .await?;
    let both =
        load_bounded_effective_permissions_by_resource_ids(&fixture.pool, &[resource], None, 2)
            .await?;
    assert_eq!(
        bounded,
        both[..2],
        "one extra row is the truncation sentinel"
    );
    assert_eq!(both.len(), 3);
    fixture.cleanup().await
}

#[tokio::test]
async fn effective_permissions_require_an_account_or_resource_anchor() -> Result<()> {
    let (fixture, _) = fixture().await?;
    let error = load_serving_effective_permissions_page(&fixture.pool, None, None, None, None, 10)
        .await
        .expect_err("unanchored reads must be rejected");
    assert!(format!("{error:#}").contains("subject or resource_id"));
    fixture.cleanup().await
}

#[tokio::test]
async fn effective_permissions_refuse_a_partial_rebuild() -> Result<()> {
    let (fixture, resource) = fixture().await?;
    let outcome = fixture
        .apply_with(
            2,
            FamilyMode::Rebuild,
            &FamilyOptions::new(CONTENT_HASH).with_max_blocks_per_run(1),
        )
        .await?;
    assert!(outcome.reset);
    let error = count(&fixture.pool, resource, None)
        .await
        .expect_err("partial rebuild must not serve an empty page");
    assert!(
        error
            .downcast_ref::<bigname_storage::families::name::FamilyPublicationUnavailable>()
            .is_some()
    );
    fixture.cleanup().await
}

async fn assert_page_equivalence(
    pool: &PgPool,
    ids: &[Uuid],
    namespace: Option<&str>,
) -> Result<()> {
    let mut expected =
        load_bounded_effective_permissions_by_resource_ids(pool, ids, namespace, 1000).await?;
    expected.sort_by_key(|row| {
        (
            row.subject.clone(),
            row.resource_id,
            row.scope.storage_key(),
        )
    });
    for size in [1_u64, 2, 25] {
        let mut cursor = None;
        let mut seen = Vec::new();
        loop {
            let page = load_serving_effective_permissions_page(
                pool,
                Some(SUBJECT),
                None,
                namespace,
                cursor.as_ref(),
                size,
            )
            .await?;
            let end = (seen.len() + size as usize).min(expected.len());
            assert_eq!(page.rows, expected[seen.len()..end]);
            assert!(page.summary.is_none());
            let next = (end < expected.len())
                .then(|| PermissionsCurrentAccountResourceCursor::from(&expected[end - 1]));
            assert_eq!(page.next_cursor, next);
            seen.extend(page.rows);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, expected);
        if let Some(last) = expected.last() {
            let terminal = PermissionsCurrentAccountResourceCursor::from(last);
            assert!(
                load_serving_effective_permissions_page(
                    pool,
                    Some(SUBJECT),
                    None,
                    namespace,
                    Some(&terminal),
                    size
                )
                .await?
                .rows
                .is_empty()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn effective_permissions_grant_pages_match_bounded_relation() -> Result<()> {
    let fixture = Fixture::new("effective_permission_pages", 2).await?;
    let mut ids = Vec::new();
    for group in 1..=4_u128 {
        let owner = format!("0x{group:040x}");
        approval(&fixture, &owner, group as i64).await?;
        for number in 1..=12 {
            let resource = Uuid::from_u128(group * 100 + number);
            binding(
                &fixture,
                resource,
                &owner,
                REGISTRY,
                (group * 100 + number) as i64,
            )
            .await?;
            if number % 3 == 0 {
                grant(&fixture, resource, (group * 100 + number) as i64).await?;
            }
            ids.push(resource);
        }
    }
    rebuild(&fixture).await?;
    assert_page_equivalence(&fixture.pool, &ids, None).await?;
    assert_page_equivalence(&fixture.pool, &ids, Some("ens")).await?;
    sqlx::query("UPDATE normalized_events SET after_state=after_state || '{\"approved\":false,\"effective_powers\":[]}' WHERE event_kind='AccountPermissionChanged' AND after_state #>> '{scope,owner}'=$1")
        .bind(format!("0x{:040x}", 2)).execute(&fixture.pool).await?;
    for (index, resource) in ids.iter().enumerate().filter(|(index, _)| index % 2 == 0) {
        fixture
            .event(
                Event::new(
                    &format!("clear-{resource}"),
                    2,
                    1000 + index as i64,
                    "SurfaceUnbound",
                    "ens_v1_registry_l1",
                )
                .resource(&resource.to_string())
                .after(json!({})),
            )
            .await?;
    }
    rebuild(&fixture).await?;
    assert_page_equivalence(&fixture.pool, &ids, None).await?;
    assert_page_equivalence(&fixture.pool, &ids, Some("ens")).await?;
    fixture.cleanup().await
}
