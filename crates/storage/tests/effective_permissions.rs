//! Effective permission contracts over publications produced by the family reducers.
#[path = "../../project/tests/families_support/mod.rs"]
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{FamilyMode, FamilyOptions};
use bigname_storage::{
    EffectivePermissionRow, EffectivePermissionScope, PermissionGrantRelation,
    PermissionsCurrentAccountResourceCursor,
    families::{
        control::permissions::page::load_family_bounded_permissions_per_resource as per_resource_loop,
        name::FamilyPublicationUnavailable,
    },
    load_bounded_effective_permissions_by_resource_ids, load_serving_effective_permissions_page,
};
use bigname_test_support::{StatementCounter, StatementCounts};
use families_support::{CHAIN, CONTENT_HASH, Event, Fixture, hash};
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

// The bounded read of an address page's grants against the read it replaced, which took one
// resource at a time. A read with a budget of n returns up to n + 1 rows: the extra row tells
// the caller the budget was passed. Each test also counts the statements the bounded read sends: the count
// must not depend on how many resources the page has.

const BASE: &str = "base-mainnet";
const CANDIDATES: &str = "families.control.permissions.resource_candidate_page";
const PUBLICATION: &str = "families.name.publication";

fn subject(n: u128) -> String {
    format!("0x{:040x}", 0x5b00 + n)
}

/// What a bounded read answers: its rows, or the chain whose publication it refused on.
fn answer(
    read: Result<Vec<EffectivePermissionRow>>,
) -> Result<std::result::Result<Vec<EffectivePermissionRow>, String>> {
    match read {
        Ok(rows) => Ok(Ok(rows)),
        Err(error) => match error.downcast_ref::<FamilyPublicationUnavailable>() {
            Some(unavailable) => Ok(Err(unavailable.chain_id.clone())),
            None => Err(error),
        },
    }
}

/// The bounded read's answer at `budget`, which must be the per-resource loop's.
async fn matching_answer(
    pool: &PgPool,
    ids: &[Uuid],
    namespace: Option<&str>,
    budget: u64,
) -> Result<std::result::Result<Vec<EffectivePermissionRow>, String>> {
    let served = answer(
        load_bounded_effective_permissions_by_resource_ids(pool, ids, namespace, budget).await,
    )?;
    let looped = answer(per_resource_loop(pool, ids, namespace, budget).await)?;
    assert_eq!(
        served, looped,
        "budget {budget}, namespace {namespace:?}: the read differs from the per-resource loop"
    );
    Ok(served)
}

/// The statements one bounded read of `ids` sends, on connections of its own.
async fn statements(
    pool: &PgPool,
    ids: &[Uuid],
    namespace: Option<&str>,
    budget: u64,
) -> Result<StatementCounts> {
    let counter = StatementCounter::connect(pool).await?;
    let (_, sent) = counter
        .count(load_bounded_effective_permissions_by_resource_ids(
            counter.pool(),
            ids,
            namespace,
            budget,
        ))
        .await;
    counter.close().await;
    Ok(sent)
}

async fn grant_to(
    fixture: &Fixture,
    chain: &str,
    resource: Uuid,
    subject: &str,
    resolver: &str,
    log: i64,
) -> Result<()> {
    let identity = format!("grant:{chain}:{log}");
    let resource = resource.to_string();
    fixture
        .event(
            Event::new(&identity, 2, log, "PermissionChanged", "ens_v2_resolver_l1")
                .on(chain)
                .resource(&resource)
                .after(json!({"subject":subject,
                    "scope":{"kind":"resolver","chain_id":chain,"resolver_address":resolver},
                    "effective_powers":["set_text"],
                    "grant_source":{"kind":"raw_log","source_event":"EACRolesChanged"},
                    "revocation_source":null,"inheritance_path":[],"transfer_behavior":{}}))
                .raw(json!({"emitting_address": resolver})),
        )
        .await?;
    Ok(())
}

async fn approval_of(fixture: &Fixture, owner: &str, operator: &str, log: i64) -> Result<()> {
    let identity = format!("approval:{log}");
    fixture
        .event(
            Event::new(
                &identity,
                2,
                log,
                "AccountPermissionChanged",
                "ens_v1_registry_l1",
            )
            .after(
                json!({"subject":operator,"relation_kind":"operator","approved":true,
                    "scope":{"kind":"account","chain_id":CHAIN,"authority_kind":"registry",
                        "authority_contract":REGISTRY,"owner":owner},
                    "effective_powers":["registry_control"],
                    "grant_source":{"kind":"raw_log","source_event":"ApprovalForAll"},
                    "revocation_source":null,"inheritance_path":[],"transfer_behavior":{}}),
            )
            .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    Ok(())
}

/// Four registry owners, each with an approved operator and twelve bound resources, every third
/// of which also has a direct grant: 48 resources and 64 rows.
async fn page_fixture(prefix: &str) -> Result<(Fixture, Vec<Uuid>)> {
    let fixture = Fixture::new(prefix, 2).await?;
    let mut ids = Vec::new();
    for group in 1..=4_u128 {
        let owner = format!("0x{group:040x}");
        approval(&fixture, &owner, group as i64).await?;
        for number in 1..=12 {
            let resource = Uuid::from_u128(group * 100 + number);
            let log = (group * 100 + number) as i64;
            binding(&fixture, resource, &owner, REGISTRY, log).await?;
            if number % 3 == 0 {
                grant(&fixture, resource, log).await?;
            }
            ids.push(resource);
        }
    }
    rebuild(&fixture).await?;
    Ok((fixture, ids))
}

#[tokio::test]
async fn bounded_permissions_match_the_per_resource_loop() -> Result<()> {
    let (fixture, ids) = page_fixture("bounded_permissions_loop").await?;
    // The ids as a page gives them: not in id order, one repeated, one with no rows.
    let mut page: Vec<Uuid> = ids.iter().rev().copied().collect();
    page.extend([ids[7], Uuid::from_u128(0xdead)]);
    for namespace in [None, Some("ens"), Some("basenames")] {
        for budget in [1, 2, 10, 1001] {
            let rows = matching_answer(&fixture.pool, &page, namespace, budget)
                .await?
                .expect("the chain is published");
            let all = if namespace == Some("basenames") {
                0
            } else {
                64
            };
            assert_eq!(
                rows.len(),
                all.min(budget as usize + 1),
                "{namespace:?} {budget}"
            );
        }
    }
    // The count for five resources is the count for all of them.
    let few = statements(&fixture.pool, &ids[..5], None, 1001).await?;
    let many = statements(&fixture.pool, &page, None, 1001).await?;
    assert_eq!(
        few.without_transaction_control(),
        many.without_transaction_control(),
        "{:?}\n{:?}",
        few.by_tag(),
        many.by_tag()
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn bounded_permissions_read_each_publication_a_fixed_number_of_times() -> Result<()> {
    let (fixture, ids) = page_fixture("bounded_permissions_statements").await?;
    let sent = statements(&fixture.pool, &ids, Some("ens"), 1001).await?;
    // One read before the keys are sought and one when their rows are composed.
    for (tag, count) in [
        ("families.control.permissions.in_namespace", 1),
        ("families.control.permissions.readable_resources", 2),
        (PUBLICATION, 2),
        (CANDIDATES, 1),
        ("families.control.permissions.grants", 1),
        ("families.control.permissions.key_states", 1),
        ("families.control.permissions.wrapper_approvals", 1),
        ("families.control.permissions.binding_observations", 1),
        ("families.control.permissions.registry_approvals", 1),
    ] {
        assert_eq!(sent.tagged(tag), count, "{tag}: {:?}", sent.by_tag());
    }
    fixture.cleanup().await
}

#[tokio::test]
async fn bounded_permissions_cut_mid_resource_matches_the_loop() -> Result<()> {
    let fixture = Fixture::new("bounded_permissions_cut", 2).await?;
    let ids: Vec<Uuid> = (1..=3).map(|n| Uuid::from_u128(0x7100 + n)).collect();
    for (index, resource) in ids.iter().enumerate() {
        binding(&fixture, *resource, OWNER, REGISTRY, index as i64).await?;
    }
    // Three operators on every resource. The first resource also has two resolver scopes for
    // each of four subjects, one of them an operator as well, and the second one grant.
    for n in 1..=3 {
        approval_of(&fixture, OWNER, &subject(n), n as i64).await?;
    }
    for n in 1..=4 {
        for (scope, resolver) in [REGISTRY, OTHER].into_iter().enumerate() {
            let log = 100 + n as i64 * 2 + scope as i64;
            grant_to(&fixture, CHAIN, ids[0], &subject(n + 2), resolver, log).await?;
        }
    }
    grant_to(&fixture, CHAIN, ids[1], &subject(9), REGISTRY, 200).await?;
    rebuild(&fixture).await?;
    for budget in 1..=19 {
        let rows = matching_answer(&fixture.pool, &ids, None, budget)
            .await?
            .expect("the chain is published");
        assert_eq!(rows.len(), 18.min(budget as usize + 1), "budget {budget}");
    }
    let one = statements(&fixture.pool, &ids[..1], None, 1001).await?;
    let all = statements(&fixture.pool, &ids, None, 1001).await?;
    assert_eq!(
        one.without_transaction_control(),
        all.without_transaction_control(),
        "{:?}\n{:?}",
        one.by_tag(),
        all.by_tag()
    );
    fixture.cleanup().await
}

/// Two resources on each of two published chains, alternating in id order, each with two
/// grants. The other chain's events are in the `basenames` namespace.
async fn two_chain_fixture(prefix: &str) -> Result<(Fixture, Vec<Uuid>)> {
    let fixture = Fixture::new(prefix, 2).await?;
    fixture.lineage(BASE, 2).await?;
    let ids: Vec<Uuid> = (1..=4).map(|n| Uuid::from_u128(0x7200 + n)).collect();
    for (index, resource) in ids.iter().enumerate() {
        let chain = if index % 2 == 0 { CHAIN } else { BASE };
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ($1, $2, $3, 0, 'canonical')",
        )
        .bind(resource)
        .bind(chain)
        .bind(hash(0))
        .execute(&fixture.pool)
        .await?;
        for n in 1..=2 {
            let log = index as i64 * 10 + n as i64;
            grant_to(&fixture, chain, *resource, &subject(n), REGISTRY, log).await?;
        }
    }
    sqlx::query("UPDATE normalized_events SET namespace = 'basenames' WHERE chain_id = $1")
        .bind(BASE)
        .execute(&fixture.pool)
        .await?;
    rebuild(&fixture).await?;
    let outcome = fixture.apply_on(BASE, 2).await?;
    ensure!(outcome.marker.as_ref().map(|marker| marker.number) == Some(2));
    Ok((fixture, ids))
}

/// A rebuild in flight on the other chain: it has no servable publication.
async fn unpublish_base(fixture: &Fixture) -> Result<()> {
    sqlx::query("UPDATE project_family_marker SET state = 'bootstrap_pending' WHERE chain_id = $1")
        .bind(BASE)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn bounded_permissions_cut_inside_the_second_chain_matches_the_loop() -> Result<()> {
    let (fixture, ids) = two_chain_fixture("bounded_permissions_two_chains").await?;
    for budget in 1..=9 {
        let rows = matching_answer(&fixture.pool, &ids, None, budget)
            .await?
            .expect("both chains are published");
        assert_eq!(rows.len(), 8.min(budget as usize + 1), "budget {budget}");
        // Resources stay in id order across the chains.
        assert!(
            rows.is_sorted_by_key(|row| row.resource_id),
            "budget {budget}"
        );
    }
    // One resource of each chain costs what two of each cost.
    let two = statements(&fixture.pool, &ids[..2], None, 1001).await?;
    let four = statements(&fixture.pool, &ids, None, 1001).await?;
    assert_eq!(
        two.without_transaction_control(),
        four.without_transaction_control(),
        "{:?}\n{:?}",
        two.by_tag(),
        four.by_tag()
    );
    assert_eq!(four.tagged(CANDIDATES), 2, "{:?}", four.by_tag());
    fixture.cleanup().await
}

#[tokio::test]
async fn bounded_permissions_refuse_an_unpublished_chain_only_before_the_budget_fills() -> Result<()>
{
    let (fixture, ids) = two_chain_fixture("bounded_permissions_unpublished").await?;
    unpublish_base(&fixture).await?;
    // In id order: a published resource with two rows, then one on the unpublished chain. With
    // a budget of one the first resource's two rows end the read before the unpublished chain
    // is reached. A page with a name on an unpublished chain never gets this far through the
    // API: composing the page's names refuses it before the grant read runs.
    let rows = matching_answer(&fixture.pool, &ids, None, 1)
        .await?
        .expect("the rows before the unpublished chain fill the budget");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.resource_id == ids[0]));
    for budget in [2, 3, 1001] {
        assert_eq!(
            matching_answer(&fixture.pool, &ids, None, budget).await?,
            Err(BASE.to_owned()),
            "budget {budget}: the budget is not full when the unpublished chain is reached"
        );
    }
    // The unpublished chain's resource first: no budget is full before it.
    assert_eq!(
        matching_answer(&fixture.pool, &ids[1..], None, 1).await?,
        Err(BASE.to_owned())
    );
    // Two published resources, then the unpublished chain: its publication is read once, and
    // the published chain's as often as for one resource.
    let page = [ids[0], ids[2], ids[3]];
    assert_eq!(
        matching_answer(&fixture.pool, &page, None, 1001).await?,
        Err(BASE.to_owned())
    );
    let sent = statements(&fixture.pool, &page, None, 1001).await?;
    assert_eq!(sent.tagged(PUBLICATION), 3, "{:?}", sent.by_tag());
    fixture.cleanup().await
}

#[tokio::test]
async fn bounded_permissions_check_the_namespace_before_any_publication() -> Result<()> {
    let (fixture, ids) = two_chain_fixture("bounded_permissions_namespace").await?;
    unpublish_base(&fixture).await?;
    // The unpublished chain's resources are not in the namespace, so its publication is not
    // read and the read is not refused.
    for budget in [1, 3, 4, 1001] {
        let rows = matching_answer(&fixture.pool, &ids, Some("ens"), budget)
            .await?
            .expect("no resource of the namespace is on the unpublished chain");
        assert_eq!(rows.len(), 4.min(budget as usize + 1), "budget {budget}");
    }
    assert_eq!(
        matching_answer(&fixture.pool, &ids, Some("basenames"), 1001).await?,
        Err(BASE.to_owned())
    );
    let sent = statements(&fixture.pool, &ids, Some("ens"), 1001).await?;
    assert_eq!(
        sent.tagged("families.control.permissions.in_namespace"),
        1,
        "{:?}",
        sent.by_tag()
    );
    assert_eq!(sent.tagged(PUBLICATION), 2, "{:?}", sent.by_tag());
    fixture.cleanup().await
}

#[tokio::test]
async fn bounded_permissions_compose_attribution_names_once() -> Result<()> {
    // Each name's registry transfer was observed on the resource the name was bound to then.
    // The name has since been bound to another resource, so the observation reaches that one.
    // The page holds both resources of every name.
    let fixture = Fixture::new("bounded_permissions_moved", 2).await?;
    let mut ids = Vec::new();
    for n in 1..=6_u128 {
        let name = format!("ens:0x{:064x}", 0xabc000 + n);
        let (first, second) = (Uuid::from_u128(0x7300 + n), Uuid::from_u128(0x7400 + n));
        let log = n as i64;
        fixture
            .binding(
                &Uuid::from_u128(0x7500 + n).to_string(),
                &name,
                &first.to_string(),
                "ens_v1",
                1,
                log,
                Some(2),
            )
            .await?;
        fixture
            .write(
                1,
                100 + log,
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                Some(&name),
                Some(&first.to_string()),
                json!({"source_event":"Transfer", "owner":OWNER, "owner_getter":OWNER,
                    "authority_kind":"registry_only"}),
                REGISTRY,
            )
            .await?;
        fixture
            .binding(
                &Uuid::from_u128(0x7600 + n).to_string(),
                &name,
                &second.to_string(),
                "ens_v1",
                2,
                log,
                None,
            )
            .await?;
        ids.extend([first, second]);
    }
    approval(&fixture, OWNER, 0).await?;
    rebuild(&fixture).await?;
    let moved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_registry_binding_observation
         WHERE attributed_via = 'name' AND target_resource_id <> resource_id",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        moved, 6,
        "every observation must reach its name's later resource"
    );

    for budget in [1, 5, 6, 7, 1001] {
        let rows = matching_answer(&fixture.pool, &ids, None, budget)
            .await?
            .expect("the chain is published");
        assert_eq!(rows.len(), 6.min(budget as usize + 1), "budget {budget}");
        // The operator's row is on the resource the name is bound to now.
        assert!(
            rows.iter()
                .all(|row| row.resource_id.as_u128() >> 8 == 0x74 && row.subject == SUBJECT),
            "{rows:?}"
        );
    }
    // The attribution reads each name's current resource and authority arm from the composed
    // row before its name is rendered and its topology attached. Both compositions agree.
    let names: Vec<String> = sqlx::query_scalar("SELECT logical_name_id FROM name_surfaces")
        .fetch_all(&fixture.pool)
        .await?;
    let [full, base] =
        bigname_storage::families::control::permissions::name_attributions_by_both_compositions(
            &fixture.pool,
            &names,
        )
        .await?;
    assert_eq!(full, base);
    assert_eq!(base.len(), 6, "{base:?}");
    assert!(
        base.values().all(|(resource, arm)| {
            resource.is_some_and(|id| id.as_u128() >> 8 == 0x74) && arm.as_deref() == Some("ens_v1")
        }),
        "{base:?}"
    );
    // One name or six: the names are composed in one read.
    let one = statements(&fixture.pool, &ids[..2], None, 1001).await?;
    let all = statements(&fixture.pool, &ids, None, 1001).await?;
    assert_eq!(
        all.tagged("families.name.surfaces"),
        1,
        "{:?}",
        all.by_tag()
    );
    assert_eq!(
        one.without_transaction_control(),
        all.without_transaction_control(),
        "{:?}\n{:?}",
        one.by_tag(),
        all.by_tag()
    );
    fixture.cleanup().await
}
