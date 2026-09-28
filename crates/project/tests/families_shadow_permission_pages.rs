//! The effective-permission page and resource summary `GET /v1/permissions` reads, served from
//! the owned key families under the publication switch (TYR-36 step 7b slice 4): each case
//! publishes twice, and at each publication the page read with the switch on equals the page
//! read with the switch off, and moves between the two publications as the served page does.
//! `publish_and_compare` also walks every subject and resource of the chain under both switch
//! states (apps/phase-runner/tests/project_end_to_end/permissions_shadow.rs).
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_storage::{
    load_serving_effective_permissions_page, load_serving_permission_summaries,
    publication_source::with_serve_from_families,
};
use serde_json::{Value, json};
use shadow_support::{
    publish_and_compare,
    wrapper::{
        CANNOT_UNWRAP, GRACE_PERIOD, HOLDER, HOLDER_POWERS, IS_DOT_ETH, OPERATOR,
        PARENT_CANNOT_CONTROL, node, timestamp, wrapped, wrapper_event,
    },
};
use support::Fixture;
use uuid::Uuid;

/// The resource's page with the switch at `on`: `(subject, registry-operator row, powers)` per
/// row (the wrapper operator fan-out is a resource row of its own), and
/// its summary's restriction block.
async fn read(
    fixture: &Fixture,
    resource: &str,
    on: bool,
) -> Result<(Vec<(String, bool, Value)>, Option<Value>)> {
    let id: Uuid = resource.parse()?;
    let page = with_serve_from_families(
        on,
        load_serving_effective_permissions_page(&fixture.pool, None, Some(id), None, None, 50),
    )
    .await?;
    let summaries =
        with_serve_from_families(on, load_serving_permission_summaries(&fixture.pool, &[id]))
            .await?;
    Ok((
        page.rows
            .iter()
            .map(|row| {
                (
                    row.subject.clone(),
                    row.grant_relation.is_some(),
                    row.effective_powers.clone(),
                )
            })
            .collect(),
        summaries
            .get(&id)
            .and_then(|summary| summary.resource_restrictions.clone()),
    ))
}

/// The page with the switch on, checked equal to the page with the switch off.
async fn page(
    fixture: &Fixture,
    resource: &str,
) -> Result<(Vec<(String, bool, Value)>, Option<Value>)> {
    let served = read(fixture, resource, false).await?;
    let families = read(fixture, resource, true).await?;
    assert_eq!(
        served, families,
        "the switch-off page (left) and the switch-on page (right)"
    );
    Ok(families)
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
/// locked, from the families as from the served tables.
#[tokio::test]
async fn a_fuse_burn_between_two_publications_moves_the_page_with_the_switch_on() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permission_pages_fuse_burn", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let expiry = timestamp(20) + 10 * GRACE_PERIOD;
    let resource = wrapped(&fixture, fuses, expiry).await?;
    let before = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(&before, &[], &[]);
    let (rows, restrictions) = page(&fixture, &resource).await?;
    let emancipated = without(HOLDER_POWERS, &["extend_expiry"]);
    assert_eq!(
        rows,
        vec![
            (HOLDER.to_owned(), false, emancipated.clone()),
            (OPERATOR.to_owned(), false, emancipated.clone()),
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
    let after = publish_and_compare(&fixture, 14).await?;
    shadow_support::assert_counts(&after, &[], &[]);
    let (rows, restrictions) = page(&fixture, &resource).await?;
    let locked = without(
        HOLDER_POWERS,
        &["extend_expiry", "unwrap", "resource_control"],
    );
    assert_eq!(
        rows,
        vec![
            (HOLDER.to_owned(), false, locked.clone()),
            (OPERATOR.to_owned(), false, locked),
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
    fixture.cleanup().await
}

/// A role change that crosses the block clock with no event between two publications: the
/// wrapper expiry is one grace period after block 14's timestamp, so the page published at 14
/// serves every emancipated power, and the page published at 15, inside the grace window, only
/// approval; the families mask at each publication's own block time, as the served build does.
#[tokio::test]
async fn a_role_change_crossing_the_clock_moves_the_page_with_the_switch_on() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permission_pages_clock", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
    let resource = wrapped(&fixture, fuses, timestamp(14) + GRACE_PERIOD).await?;
    let before = publish_and_compare(&fixture, 14).await?;
    shadow_support::assert_counts(&before, &[], &[]);
    let emancipated = without(HOLDER_POWERS, &["extend_expiry"]);
    assert_eq!(
        page(&fixture, &resource).await?.0,
        vec![
            (HOLDER.to_owned(), false, emancipated.clone()),
            (OPERATOR.to_owned(), false, emancipated),
        ],
        "at 14"
    );
    let after = publish_and_compare(&fixture, 15).await?;
    shadow_support::assert_counts(&after, &[], &[]);
    assert_eq!(
        page(&fixture, &resource).await?.0,
        vec![
            (HOLDER.to_owned(), false, json!(["approve"])),
            (OPERATOR.to_owned(), false, json!(["approve"])),
        ],
        "at 15"
    );
    fixture.cleanup().await
}

/// A permission read with the switch on while the families are not servable (a rebuild in
/// flight) refuses with the publication-unavailable error the API answers as a stale 409.
#[tokio::test]
async fn a_permission_read_refuses_while_the_families_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permission_pages_rebuild", 20).await?;
    let resource = wrapped(&fixture, PARENT_CANNOT_CONTROL | IS_DOT_ETH, timestamp(40)).await?;
    publish_and_compare(&fixture, 12).await?;
    sqlx::query("UPDATE project_family_marker SET state = 'bootstrap_pending'")
        .execute(&fixture.pool)
        .await?;
    let id: Uuid = resource.parse()?;
    let page = with_serve_from_families(
        true,
        load_serving_effective_permissions_page(&fixture.pool, Some(HOLDER), None, None, None, 50),
    )
    .await;
    let summaries = with_serve_from_families(
        true,
        load_serving_permission_summaries(&fixture.pool, &[id]),
    )
    .await;
    let by_resource = with_serve_from_families(
        true,
        load_serving_effective_permissions_page(&fixture.pool, None, Some(id), None, None, 50),
    )
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
    // The served tables still answer with the switch off.
    assert!(!read(&fixture, &resource, false).await?.0.is_empty());
    fixture.cleanup().await
}
