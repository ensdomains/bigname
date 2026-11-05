//! F18's rows (TYR-127): the stored address listing (`project_address_listing`) for a
//! surface-less registry child, a registration's aliases and their lead, and a stored name that
//! heals at the block that observes one of its labels. Every publication is also checked against
//! the address-names route's own rows (`families_support/address_listing.rs`).
mod families_support;

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode};
use families_support::{
    CHAIN, Event, Fixture, ens_v2_path, hash,
    listing_cases::{
        OWNER, REGISTRY, edge, ensure_equal_but_listing, journalled, label_state, name, node,
        preimage,
    },
    uuid,
};
use serde_json::json;

const V1_REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const FAMILY: &str = "project_address_listing";

/// The label hash of `label`.
fn labelhash(label: &str) -> String {
    format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))
}

/// `logical_name_id` bound at `block` to `resource` by binding `n`, closed at `closed_at`, and
/// owned in the ENSv1 registry by `OWNER`, as `ens_v2_path::subname` writes a subname.
async fn owned_at(
    fixture: &Fixture,
    logical_name_id: &str,
    n: u32,
    resource: &str,
    block: i64,
    closed_at: Option<i64>,
) -> Result<()> {
    fixture
        .binding(
            &uuid(n),
            logical_name_id,
            resource,
            "ens_v1",
            block,
            n.into(),
            closed_at,
        )
        .await?;
    fixture
        .event(
            Event::new(
                &format!("owned-{logical_name_id}-{block}"),
                block,
                n.into(),
                "AuthorityTransferred",
                "ens_v1_registry_l1",
            )
            .name(logical_name_id)
            .resource(resource)
            .after(
                json!({"authority_kind": "registry", "owner": OWNER, "registry_owner": OWNER,
                          "node": logical_name_id.trim_start_matches("ens:")}),
            )
            .raw(json!({"emitting_address": V1_REGISTRY})),
        )
        .await?;
    ens_v2_path::resolver(fixture, logical_name_id, block, 100 + i64::from(n)).await
}

/// `name`, spelled, owned from `block` on `resource` (`owned_at`).
async fn owned(
    fixture: &Fixture,
    name: &str,
    n: u32,
    resource: &str,
    block: i64,
    closed_at: Option<i64>,
) -> Result<String> {
    let logical_name_id = ens_v2_path::surface(fixture, name).await?;
    owned_at(fixture, &logical_name_id, n, resource, block, closed_at).await?;
    Ok(logical_name_id)
}

/// `name` with no raw bytes, owned from block 1 on its own resource (`owned_at`).
async fn unspelled(fixture: &Fixture, name: &str, n: u32) -> Result<String> {
    let logical_name_id = ens_v2_path::unknown_surface(fixture, name).await?;
    owned_at(fixture, &logical_name_id, n, &uuid(0x4100 + n), 1, None).await?;
    Ok(logical_name_id)
}

/// `OWNER`'s stored registration leads, as (name, resource).
async fn leads(fixture: &Fixture) -> Result<BTreeSet<(String, String)>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT logical_name_id, resource_id::text FROM project_address_listing
         WHERE chain_id = $1 AND address = $2 AND registration_lead",
    )
    .bind(CHAIN)
    .bind(OWNER)
    .fetch_all(&fixture.pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// `OWNER`'s stored spelling of `logical_name_id`: its name and the labels it leaves unhealed.
async fn spelled(fixture: &Fixture, logical_name_id: &str) -> Result<(String, Vec<String>)> {
    Ok(sqlx::query_as(
        "SELECT display_name, unhealed_labelhashes FROM project_address_listing
         WHERE chain_id = $1 AND address = $2 AND logical_name_id = $3",
    )
    .bind(CHAIN)
    .bind(OWNER)
    .bind(logical_name_id)
    .fetch_one(&fixture.pool)
    .await?)
}

/// A label-only preimage observation of `label` at `block`, as an ENSv2 registry's
/// `LabelRegistered` writes one.
async fn observe_label(fixture: &Fixture, block: i64, label: &str) -> Result<()> {
    fixture
        .write(
            block,
            0,
            "PreimageObserved",
            "ens_v2_registry_l1",
            None,
            None,
            label_state(label),
            REGISTRY,
        )
        .await?;
    Ok(())
}

/// The bracketed placeholder of a label hash.
fn placeholder(labelhash: &str) -> String {
    format!("[{}]", &labelhash[2..])
}

// Rows 6, 7 and 30: a surface-less ENSv1 registry child, which a new controller candidate alone
// derives (an F13 key with no before-image), lists its registry owner as owner and manager under
// its parent, with no timestamp key. When the child gains a surface at block 12 the registry child
// row goes, as the route then reads the child through its surface.
#[tokio::test]
async fn a_surface_less_registry_child_lists_its_owner_and_manager() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_registry_child", 20).await?;
    fixture.surface(&name(1), &node(1)).await?;
    edge(&fixture, (10, 0), 1, 11, &labelhash("kid"), OWNER).await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    let child: Vec<(String, bool, bool, bool, Option<String>, String, bool)> = sqlx::query_as(
        "SELECT address, token_holder, effective_controller, registry_child,
                registry_parent_logical_name_id, expires_key::text, listed
         FROM project_address_listing WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name(11))
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        child
            == [(
                OWNER.to_owned(),
                true,
                true,
                true,
                Some(name(1)),
                "-1".to_owned(),
                true
            )],
        "the registry child lists its registry owner as owner and manager: {child:?}"
    );
    ensure!(journalled(&fixture, 10, FAMILY).await? == (1, 0));

    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, 'ens', 'kid.parent.eth', $2, ARRAY[$3, $4, $5], $6, 'active', $7, $8, 12,
                 'canonical')",
    )
    .bind(name(11))
    .bind(node(11))
    .bind(labelhash("kid"))
    .bind(labelhash("parent"))
    .bind(labelhash("eth"))
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(12))
    .execute(&fixture.pool)
    .await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let children: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_address_listing
         WHERE chain_id = $1 AND logical_name_id = $2 AND registry_child",
    )
    .bind(CHAIN)
    .bind(name(11))
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        children == 0,
        "the child's surface ends its registry child row"
    );
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Rows 13, 23 and 24: two names on one resource held by one address lead by the smaller name. A
// third, smaller alias takes the lead at its block; when it moves to a resource of its own at
// block 5 it leads that one, and the lead of the first resource goes back to the smaller of the
// two left. The undo of block 5 restores the listing byte for byte, and a rebuild equals it.
#[tokio::test]
async fn the_registration_lead_moves_with_the_smallest_alias() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_lead", 8).await?;
    let (shared, own) = (uuid(0x4001), uuid(0x4002));
    let b = owned(&fixture, "b.box", 1, &shared, 1, None).await?;
    owned(&fixture, "c.box", 2, &shared, 1, None).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let first = leads(&fixture).await?;
    ensure!(
        first == BTreeSet::from([(b.clone(), shared.clone())]),
        "b.box leads its resource: {first:?}"
    );

    let a = owned(&fixture, "a.box", 3, &shared, 3, Some(5)).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    let taken = leads(&fixture).await?;
    ensure!(
        taken == BTreeSet::from([(a.clone(), shared.clone())]),
        "the smaller alias takes the lead: {taken:?}"
    );

    owned_at(&fixture, &a, 4, &own, 5, None).await?;
    fixture.assert_undo_restores(5).await?;
    let moved = leads(&fixture).await?;
    ensure!(
        moved == BTreeSet::from([(a, own), (b, shared)]),
        "the moved alias leads its own resource and b.box leads again: {moved:?}"
    );
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

// Row 25a: a stored name with no raw bytes spells each label as `[labelhash]`; the block that
// observes one of its labels spells that label, finding the row through its unhealed labels, and
// journals the one row.
#[tokio::test]
async fn an_observed_label_respells_its_holders() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_heal", 8).await?;
    let (x, tld) = (labelhash("x"), labelhash("box"));
    let held = unspelled(&fixture, "x.box", 1).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let before = spelled(&fixture, &held).await?;
    ensure!(
        before
            == (
                format!("{}.{}", placeholder(&x), placeholder(&tld)),
                vec![x.clone(), tld.clone()]
            ),
        "{before:?}"
    );
    observe_label(&fixture, 3, "x").await?;
    preimage(&fixture, &x, "x").await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    let after = spelled(&fixture, &held).await?;
    ensure!(
        after == (format!("x.{}", placeholder(&tld)), vec![tld.clone()]),
        "the observed label heals: {after:?}"
    );
    ensure!(journalled(&fixture, 3, FAMILY).await? == (1, 1));
    fixture.assert_rebuild_equal(3).await?;
    fixture.cleanup().await
}

// Row 25c: an imported preimage is not chain data: the stored name keeps the placeholder, no
// block journals a row for it, and the route serves the imported spelling at read (the oracle
// allows exactly that difference).
#[tokio::test]
async fn an_imported_preimage_changes_no_stored_spelling() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_import", 8).await?;
    let (y, tld) = (labelhash("y"), labelhash("box"));
    let held = unspelled(&fixture, "y.box", 1).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let before = spelled(&fixture, &held).await?;
    sqlx::query(
        "INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority)
         VALUES ($1, convert_to('y', 'UTF8'), 'y', $2, TRUE, 'ens_rainbow_import', 0)",
    )
    .bind(&y)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .execute(&fixture.pool)
    .await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    ensure!(spelled(&fixture, &held).await? == before);
    ensure!(before.1 == [y, tld]);
    ensure!(journalled(&fixture, 3, FAMILY).await? == (0, 0));
    ensure!(journalled(&fixture, 4, FAMILY).await? == (0, 0));
    fixture.cleanup().await
}

// Row 25d: the undo of the healing block restores every other family byte for byte, and derives
// the row again at the block before. `label_preimages` keeps the preimage, so the row stays
// healed, as a rebuild there spells it, and the block applied again changes no row.
#[tokio::test]
async fn an_undo_keeps_the_healed_spelling() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_heal_undo", 8).await?;
    let (z, tld) = (labelhash("z"), labelhash("box"));
    let held = unspelled(&fixture, "z.box", 1).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let kept = fixture.exact().await?;
    observe_label(&fixture, 3, "z").await?;
    preimage(&fixture, &z, "z").await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    let healed = spelled(&fixture, &held).await?;
    ensure!(healed == (format!("z.{}", placeholder(&tld)), vec![tld]));

    let undone = families::undo_to(&fixture.pool, CHAIN, 2).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    ensure_equal_but_listing(&kept, &fixture.exact().await?)?;
    ensure!(
        spelled(&fixture, &held).await? == healed,
        "the undo keeps the healed spelling"
    );
    fixture.assert_address_listing(CHAIN).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    ensure!(spelled(&fixture, &held).await? == healed);
    ensure!(
        journalled(&fixture, 3, FAMILY).await? == (0, 0),
        "the block applied again changes no row"
    );
    fixture.assert_rebuild_equal(3).await?;
    fixture.cleanup().await
}
