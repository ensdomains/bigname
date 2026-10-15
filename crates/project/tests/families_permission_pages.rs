//! Wrapper permission pages retain fuse, grace, cursor, namespace and overflow behavior
//! across actual family publications and rebuild windows.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
#[path = "families_read_support/mod.rs"]
mod read_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_storage::{
    EffectivePermissionScope, PermissionGrantRelation, load_serving_effective_permissions_page,
    load_serving_permission_summaries,
};
use read_support::{
    publish,
    wrapper::{
        CANNOT_UNWRAP, GRACE_PERIOD, HOLDER, HOLDER_POWERS, IS_DOT_ETH, OPERATOR,
        PARENT_CANNOT_CONTROL, node, timestamp, wrapped, wrapper_event,
    },
};
use serde_json::{Value, json};
use support::Fixture;
use uuid::Uuid;

/// The resource's page with the switch at `on`: `(subject, grant relation, powers)` per row
/// (the wrapper operator fan-out is a resource row of its own, with the operator relation), and
/// its summary's restriction block.
async fn read(
    fixture: &Fixture,
    resource: &str,
) -> Result<(
    Vec<(String, Option<PermissionGrantRelation>, Value)>,
    Option<Value>,
)> {
    let id: Uuid = resource.parse()?;
    let page =
        load_serving_effective_permissions_page(&fixture.pool, None, Some(id), None, None, 50)
            .await?;
    let summaries = load_serving_permission_summaries(&fixture.pool, &[id]).await?;
    // A per-name operator row exists through the holder's NameWrapper `ApprovalForAll`.
    for row in &page.rows {
        if row.grant_relation == Some(PermissionGrantRelation::Operator)
            && matches!(row.scope, EffectivePermissionScope::Direct(_))
        {
            assert_eq!(row.grant_source["relation_kind"], "operator", "{row:?}");
            assert_eq!(
                row.grant_source["source_event_kind"], "ApprovalForAll",
                "{row:?}"
            );
        }
    }
    Ok((
        page.rows
            .iter()
            .map(|row| {
                (
                    row.subject.clone(),
                    row.grant_relation,
                    row.effective_powers.clone(),
                )
            })
            .collect(),
        summaries
            .get(&id)
            .and_then(|summary| summary.resource_restrictions.clone()),
    ))
}

/// The bounded read an address page's `role_summary` uses must answer as the read it replaced,
/// which took one resource at a time: the same rows in the same order, or a refusal on the same
/// chain. Checked over every resource of the fixture, in each namespace and at budgets that cut
/// inside the rows.
async fn assert_bounded_reads_match_the_loop(fixture: &Fixture) -> Result<()> {
    use bigname_storage::families::{
        control::permissions::page::load_family_bounded_permissions_per_resource,
        name::FamilyPublicationUnavailable,
    };
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT resource_id FROM resources")
        .fetch_all(&fixture.pool)
        .await?;
    type Rows = Vec<bigname_storage::EffectivePermissionRow>;
    let answer = |read: Result<Rows>| -> Result<std::result::Result<Rows, String>> {
        match read {
            Ok(rows) => Ok(Ok(rows)),
            Err(error) => match error.downcast_ref::<FamilyPublicationUnavailable>() {
                Some(unavailable) => Ok(Err(unavailable.chain_id.clone())),
                None => Err(error),
            },
        }
    };
    for namespace in [None, Some("ens"), Some("basenames")] {
        for budget in [1, 2, 10, 1001] {
            let served = answer(
                bigname_storage::load_bounded_effective_permissions_by_resource_ids(
                    &fixture.pool,
                    &ids,
                    namespace,
                    budget,
                )
                .await,
            )?;
            let looped = answer(
                load_family_bounded_permissions_per_resource(
                    &fixture.pool,
                    &ids,
                    namespace,
                    budget,
                )
                .await,
            )?;
            assert_eq!(served, looped, "namespace {namespace:?}, budget {budget}");
        }
    }
    // The registry binding of a resource reads each observed name's current resource and
    // authority arm from the name's composed row before its name is rendered and its topology
    // attached. Both compositions must give the same two fields for every name.
    let names: Vec<String> = sqlx::query_scalar("SELECT logical_name_id FROM name_surfaces")
        .fetch_all(&fixture.pool)
        .await?;
    match bigname_storage::families::control::permissions::name_attributions_by_both_compositions(
        &fixture.pool,
        &names,
    )
    .await
    {
        Ok([full, base]) => assert_eq!(full, base),
        Err(error) => anyhow::ensure!(
            bigname_storage::families::name::is_publication_unavailable(&error),
            "{error:#}"
        ),
    }
    Ok(())
}

fn without(powers: &[&str], dropped: &[&str]) -> Value {
    json!(
        powers
            .iter()
            .filter(|power| !dropped.contains(power))
            .collect::<Vec<_>>()
    )
}

/// A fuse burn between two publications: the emancipated `.eth` name published at 12 serves
/// `unwrap` to its holder and the operator fan-out; at 13 the holder burns CANNOT_UNWRAP, and the
/// page published at 14 has neither `unwrap` nor `resource_control`, with the restriction block
/// locked.
#[tokio::test]
async fn a_fuse_burn_between_two_publications_moves_the_page() -> Result<()> {
    let fixture = Fixture::new("families_permission_pages_fuse_burn", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let expiry = timestamp(20) + 10 * GRACE_PERIOD;
    let resource = wrapped(&fixture, fuses, expiry).await?;
    publish(&fixture, 12).await?;
    let (rows, restrictions) = read(&fixture, &resource).await?;
    let emancipated = without(HOLDER_POWERS, &["extend_expiry"]);
    assert_eq!(
        rows,
        vec![
            (HOLDER.to_owned(), None, emancipated.clone()),
            (
                OPERATOR.to_owned(),
                Some(PermissionGrantRelation::Operator),
                emancipated.clone()
            ),
        ],
        "at 12"
    );
    assert_eq!(
        restrictions.as_ref().map(|block| &block["wrapper_state"]),
        Some(&json!("emancipated"))
    );
    let burnt = fuses | CANNOT_UNWRAP;
    wrapper_event(
        &fixture,
        13,
        0,
        "PermissionScopeChanged",
        &resource,
        json!({"fuses": fuses, "wrapper_state": "emancipated", "expiry": expiry}),
        json!({"source_event": "FusesSet", "node": node(1), "fuses": burnt,
               "wrapper_state": "locked", "expiry": expiry}),
    )
    .await?;
    publish(&fixture, 14).await?;
    let (rows, restrictions) = read(&fixture, &resource).await?;
    let locked = without(
        HOLDER_POWERS,
        &["extend_expiry", "unwrap", "resource_control"],
    );
    assert_eq!(
        rows,
        vec![
            (HOLDER.to_owned(), None, locked.clone()),
            (
                OPERATOR.to_owned(),
                Some(PermissionGrantRelation::Operator),
                locked
            ),
        ],
        "at 14"
    );
    assert_eq!(
        restrictions,
        Some(
            json!({"kind": "ens_v1_wrapper", "wrapper_state": "locked", "fuses": burnt,
                    "expiry_seconds": expiry})
        )
    );
    assert_bounded_reads_match_the_loop(&fixture).await?;
    fixture.cleanup().await
}

/// A role change that crosses the block clock with no event between two publications: the
/// wrapper expiry is one grace period after block 14's timestamp, so the page published at 14
/// serves every emancipated power, and the page published at 15, inside the grace window, only
/// approval; the families mask at each publication's own block time, as the served build does.
#[tokio::test]
async fn a_role_change_crossing_the_clock_moves_the_page() -> Result<()> {
    let fixture = Fixture::new("families_permission_pages_clock", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let resource = wrapped(&fixture, fuses, timestamp(14) + GRACE_PERIOD).await?;
    publish(&fixture, 14).await?;
    let emancipated = without(HOLDER_POWERS, &["extend_expiry"]);
    assert_eq!(
        read(&fixture, &resource).await?.0,
        vec![
            (HOLDER.to_owned(), None, emancipated.clone()),
            (
                OPERATOR.to_owned(),
                Some(PermissionGrantRelation::Operator),
                emancipated
            ),
        ],
        "at 14"
    );
    publish(&fixture, 15).await?;
    assert_eq!(
        read(&fixture, &resource).await?.0,
        vec![
            (HOLDER.to_owned(), None, json!(["approve"])),
            (
                OPERATOR.to_owned(),
                Some(PermissionGrantRelation::Operator),
                json!(["approve"])
            ),
        ],
        "at 15"
    );
    assert_bounded_reads_match_the_loop(&fixture).await?;
    fixture.cleanup().await
}

/// A permission read with the switch on while the families are not servable (a rebuild in
/// flight) refuses with the publication-unavailable error the API answers as a stale 409.
#[tokio::test]
async fn a_permission_read_refuses_while_the_families_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_permission_pages_rebuild", 20).await?;
    let resource = wrapped(&fixture, PARENT_CANNOT_CONTROL | IS_DOT_ETH, timestamp(40)).await?;
    publish(&fixture, 12).await?;
    sqlx::query("UPDATE project_family_marker SET state = 'bootstrap_pending'")
        .execute(&fixture.pool)
        .await?;
    let id: Uuid = resource.parse()?;
    let page =
        load_serving_effective_permissions_page(&fixture.pool, Some(HOLDER), None, None, None, 50)
            .await;
    let summaries = load_serving_permission_summaries(&fixture.pool, &[id]).await;
    let by_resource =
        load_serving_effective_permissions_page(&fixture.pool, None, Some(id), None, None, 50)
            .await;
    for (read, error) in [
        ("subject page", page.err()),
        ("summaries", summaries.err()),
        ("resource page", by_resource.err()),
    ] {
        assert!(
            error
                .as_ref()
                .is_some_and(bigname_storage::families::name::is_publication_unavailable),
            "{read}: {error:?}"
        );
    }
    assert_bounded_reads_match_the_loop(&fixture).await?;
    fixture.cleanup().await
}

// A real second-chain grant for the same account must not make an ENS-only page depend on
// that chain's publication. The unscoped request still refuses that rebuilding chain.
#[tokio::test]
async fn namespace_filters_permission_resources_before_their_publication() -> Result<()> {
    use support::{Event, uuid};
    let fixture = Fixture::new("permission_namespace_publication", 20).await?;
    wrapped(&fixture, PARENT_CANNOT_CONTROL, timestamp(40)).await?;
    let base = "base-mainnet";
    let base_resource = uuid(2);
    fixture.lineage(base, 20).await?;
    fixture.resource(&base_resource).await?;
    sqlx::query("UPDATE resources SET chain_id = $1 WHERE resource_id = $2::uuid")
        .bind(base)
        .bind(&base_resource)
        .execute(&fixture.pool)
        .await?;
    fixture
        .event(
            Event::new(
                "base-permission",
                10,
                0,
                "PermissionChanged",
                "basenames_registry_l2",
            )
            .on(base)
            .resource(&base_resource)
            .after(json!({"subject": HOLDER, "scope": {"kind": "resource"},
            "effective_powers": ["set_resolver"],
            "grant_source": {"authority_kind": "registry", "relation_kind": "holder"},
            "inheritance_path": [], "transfer_behavior": {}})),
        )
        .await?;
    sqlx::query("UPDATE normalized_events SET namespace = 'basenames' WHERE chain_id = $1")
        .bind(base)
        .execute(&fixture.pool)
        .await?;
    read_support::publish(&fixture, 12).await?;
    let outcome = fixture.apply_on(base, 12).await?;
    anyhow::ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(12),
        "{outcome:?}"
    );
    sqlx::query("UPDATE project_family_marker SET state = 'bootstrap_pending' WHERE chain_id = $1")
        .bind(base)
        .execute(&fixture.pool)
        .await?;
    let ens = load_serving_effective_permissions_page(
        &fixture.pool,
        Some(HOLDER),
        None,
        Some("ens"),
        None,
        50,
    )
    .await?;
    assert_eq!(ens.rows.len(), 1);
    let all =
        load_serving_effective_permissions_page(&fixture.pool, Some(HOLDER), None, None, None, 50)
            .await;
    assert!(
        all.as_ref()
            .err()
            .is_some_and(bigname_storage::families::name::is_publication_unavailable)
    );
    assert_bounded_reads_match_the_loop(&fixture).await?;
    fixture.cleanup().await
}

// A registry operator also holding direct grants exercises the candidate union, duplicates,
// and the continuation within a resource (account scope sorts before resource scope).
#[tokio::test]
async fn account_pages_walk_direct_and_registry_permissions_without_skips() -> Result<()> {
    use support::{CHAIN, Event, uuid};
    let fixture = Fixture::new("permission_candidate_cursor", 20).await?;
    let registry = "0x00000000000000000000000000000000000000e5";
    for n in 1..=12 {
        let resource = uuid(1000 + n);
        let name = read_support::wrapper::name(u64::from(n));
        fixture
            .binding(
                &uuid(2000 + n),
                &name,
                &resource,
                "ens_v1",
                9,
                i64::from(n),
                None,
            )
            .await?;
        fixture
            .write(
                9,
                i64::from(n),
                "SurfaceBound",
                "ens_v1_registrar_l1",
                Some(&name),
                Some(&resource),
                json!({"authority_kind": "registrar", "state_derived": false,
                "registry_contract": registry, "owner_getter": HOLDER}),
                registry,
            )
            .await?;
        fixture
            .write(
                10,
                i64::from(n),
                "PermissionChanged",
                "ens_v1_registrar_l1",
                Some(&name),
                Some(&resource),
                json!({"subject": OPERATOR, "scope": {"kind": "resource"},
                "effective_powers": ["resource_control"],
                "grant_source": {"authority_kind": "registrar", "relation_kind": "holder"},
                "inheritance_path": [], "transfer_behavior": {}}),
                registry,
            )
            .await?;
    }
    fixture
        .event(
            Event::new(
                "registry-approval",
                11,
                0,
                "AccountPermissionChanged",
                "ens_v1_registry_l1",
            )
            .after(
                json!({"subject": OPERATOR, "relation_kind": "operator", "approved": true,
            "scope": {"kind": "account", "chain_id": CHAIN, "authority_kind": "registry",
                "authority_contract": registry, "authority_contract_instance_id": "00000000-0000-0000-0000-0000000000e5", "owner": HOLDER},
            "effective_powers": ["registry_control"],
            "grant_source": {"kind": "raw_log", "source_event": "ApprovalForAll"},
            "revocation_source": null, "inheritance_path": [],
            "transfer_behavior": {"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"}}),
            )
            .raw(json!({"emitting_address": registry})),
        )
        .await?;
    read_support::publish(&fixture, 12).await?;
    let mut answers = Vec::new();
    {
        let mut cursor = None;
        let mut keys = Vec::new();
        loop {
            let page = load_serving_effective_permissions_page(
                &fixture.pool,
                Some(OPERATOR),
                None,
                Some("ens"),
                cursor.as_ref(),
                1,
            )
            .await?;
            keys.extend(
                page.rows
                    .iter()
                    .map(|row| (row.resource_id, row.scope.storage_key())),
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
            anyhow::ensure!(keys.len() <= 24, "cursor did not advance");
        }
        answers.push(keys);
    }
    assert_eq!(answers[0].len(), 24);
    let ids: Vec<Uuid> = (1..=12).map(|n| uuid(1000 + n).parse().unwrap()).collect();
    let mut inline_answers = Vec::new();
    {
        let overflow = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
            &fixture.pool,
            &ids,
            Some("ens"),
            3,
        )
        .await?;
        assert_eq!(
            overflow.len(),
            4,
            "the inline expansion keeps one overflow sentinel"
        );
        let complete = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
            &fixture.pool,
            &ids,
            Some("ens"),
            30,
        )
        .await?;
        let mut keys: Vec<_> = complete
            .iter()
            .map(|row| (row.resource_id, row.scope.storage_key()))
            .collect();
        keys.sort();
        inline_answers.push(keys);
    }
    assert_eq!(inline_answers[0].len(), 24);
    assert_bounded_reads_match_the_loop(&fixture).await?;
    fixture.cleanup().await
}

// A summary is independent of how many operators a wrapper holder approved. Removing the
// approval relation after a real publication proves the reader never expands that fan-out.
#[tokio::test]
async fn wrapper_summary_does_not_expand_operator_approvals() -> Result<()> {
    let fixture = Fixture::new("permission_summary_without_approval_scan", 20).await?;
    let resource = wrapped(&fixture, PARENT_CANNOT_CONTROL, timestamp(40)).await?;
    read_support::publish(&fixture, 12).await?;
    assert_bounded_reads_match_the_loop(&fixture).await?;
    let ids = [resource.parse()?];
    let before = load_serving_permission_summaries(&fixture.pool, &ids).await?;
    sqlx::query("ALTER TABLE project_account_approval RENAME TO approvals_not_read_by_summary")
        .execute(&fixture.pool)
        .await?;
    let after = load_serving_permission_summaries(&fixture.pool, &ids).await?;
    assert_eq!(
        before[&ids[0]].resource_restrictions,
        after[&ids[0]].resource_restrictions
    );
    assert!(after[&ids[0]].resource_restrictions.is_some());
    fixture.cleanup().await
}
