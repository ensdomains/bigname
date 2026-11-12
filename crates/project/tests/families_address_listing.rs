//! F18's rows (TYR-127): the stored address listing (`project_address_listing`) for a
//! surface-less registry child, a registration's aliases and their lead, and a stored name that
//! heals at the block that observes one of its labels. Every publication is also checked against
//! the address-names route's own rows (`families_support/address_listing.rs`).
mod families_support;

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, RebuildRanges};
use families_support::{
    CHAIN, CONTENT_HASH, Event, Fixture,
    address_listing::q_read,
    ens_v2_path, hash,
    listing_cases::{
        OWNER, REGISTRY, ensure_equal_but_listing, journalled, label_state, name, node, preimage,
    },
    uuid,
    workers::{held, hold, side, until},
};
use serde_json::json;

const V1_REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const BASE_REGISTRAR: &str = "0x00000000000000000000000000000000000000e6";
const FAMILY: &str = "project_address_listing";

/// A stored registry child row: (address, token holder, effective controller, registry child,
/// registry parent, expiry key, listed).
type ChildRow = (String, bool, bool, bool, Option<String>, String, bool);

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
    // setSubnodeOwner(parent 1, kid, OWNER) on the child's registry-only resource.
    fixture
        .write(
            10,
            0,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            Some(&uuid(0x4200)),
            json!({"source_event": "NewOwner", "node": node(1), "child_node": node(11),
                   "labelhash": labelhash("kid"), "owner": OWNER, "owner_getter": OWNER,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    let child: Vec<ChildRow> = sqlx::query_as(
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

    let spelled = bigname_domain::normalization::normalize_name("kid.parent.eth")?;
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', 'kid.parent.eth', $9, $10, $2, ARRAY[$3, $4, $5], $6, 'active', $7,
                 $8, 12, 'canonical')",
    )
    .bind(name(11))
    .bind(node(11))
    .bind(labelhash("kid"))
    .bind(labelhash("parent"))
    .bind(labelhash("eth"))
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(12))
    .bind(spelled.normalized_labels)
    .bind(spelled.dns_encoded_name)
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
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}

// Row "Basenames child" of TYR-127: the route reads a surface-less registry child only under a
// parent an ENSv1 registry NewOwner named for it (`registry_child_parents`). A Basenames registry
// NewOwner gives the children relation a child, and its registrar lease indexes the holder for
// the child's id, yet the address listing stores no row for it. The parent's surface appears at
// block 12, so that block derives the parent's registry children whole, the path no
// `registry_child_parents` read guards. The fixture writes every event in the `ens` namespace,
// and both reads key on the source family, not the namespace.
#[tokio::test]
async fn a_basenames_registry_child_lists_no_row() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_basenames_child", 20).await?;
    fixture
        .write(
            10,
            0,
            "SubregistryChanged",
            "basenames_base_registry",
            None,
            Some(&uuid(0x4200)),
            json!({"source_event": "NewOwner", "node": node(1), "child_node": node(11),
                   "labelhash": labelhash("kid"), "owner": OWNER, "owner_getter": OWNER,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "TokenControlTransferred",
            "basenames_base_registrar",
            None,
            Some(&uuid(0x4201)),
            json!({"namehash": node(11), "to": OWNER}),
            BASE_REGISTRAR,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    let indexed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_address_name_index
         WHERE chain_id = $1 AND address = $2 AND logical_name_id = $3
           AND relation = 'effective_controller'",
    )
    .bind(CHAIN)
    .bind(OWNER)
    .bind(name(11))
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        indexed == 1,
        "the lease indexes its holder for the child: {indexed} rows"
    );
    let parent = bigname_domain::normalization::normalize_name("parent.eth")?;
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', 'parent.eth', $8, $9, $2, ARRAY[$3, $4], $5, 'active', $6, $7, 12,
                 'canonical')",
    )
    .bind(name(1))
    .bind(node(1))
    .bind(labelhash("parent"))
    .bind(labelhash("eth"))
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(12))
    .bind(parent.normalized_labels)
    .bind(parent.dns_encoded_name)
    .execute(&fixture.pool)
    .await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let edges: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_child_edge_candidate
         WHERE chain_id = $1 AND child_node = lower($2) AND authority_arm = 'basenames'",
    )
    .bind(CHAIN)
    .bind(node(11))
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        edges == 1,
        "the children relation reads the Basenames child: {edges} edges"
    );
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_address_listing WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name(11))
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(rows == 0, "the Basenames child lists {rows} rows");
    fixture.assert_rebuild_equal(12).await?;
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}

// Row "100 boundary" of TYR-127: the q key is the first 100 characters of the normalized name. A
// name of exactly 100 characters and a longer one that starts with it share that key; a reader
// takes the q's first 100 characters as the index range and checks the whole q against the name,
// so each q finds exactly the names it starts.
#[tokio::test]
async fn a_q_finds_the_names_it_starts_across_the_100_character_key() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_q_cut", 8).await?;
    let label = "a".repeat(96);
    let (exact, longer) = (format!("{label}.box"), format!("{label}.boxy"));
    ensure!(exact.chars().count() == 100 && longer.chars().count() == 101);
    let exact_id = owned(&fixture, &exact, 1, &uuid(0x4301), 1, None).await?;
    let longer_id = owned(&fixture, &longer, 2, &uuid(0x4302), 1, None).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let both = BTreeSet::from([exact_id.clone(), longer_id.clone()]);
    for (q, expected) in [
        (exact[..99].to_owned(), both.clone()),
        (exact.clone(), both),
        (longer.clone(), BTreeSet::from([longer_id])),
        (format!("{label}.boxz"), BTreeSet::new()),
    ] {
        let found: BTreeSet<String> = fixture
            .listing_read(&q_read(CHAIN, OWNER, &q))
            .await?
            .into_iter()
            .collect();
        ensure!(found == expected, "q {q} finds {found:?}, not {expected:?}");
    }
    fixture
        .ensure_index_serves(
            &q_read(CHAIN, OWNER, &longer),
            "project_address_listing_q_idx",
            "normalized_name, 100)",
        )
        .await?;
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
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
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}

// M3.2 (stop C): one resource held by one address under two names, b.box and c.box, in a
// rebuild whose pass composes one name a chunk on two workers. The chunk of the name the work
// list claims first is held at its commit until the other's has committed, so neither chunk sees
// the other's row. The group has one lead, the smaller name, as block-by-block publication and
// one worker give it, and every family table equals block by block.
#[tokio::test]
async fn a_group_in_two_workers_chunks_has_one_lead() -> Result<()> {
    let fixture = Fixture::with_connections("families_address_listing_workers", 8, 4).await?;
    let shared = uuid(0x4001);
    let b = owned(&fixture, "b.box", 1, &shared, 1, None).await?;
    let c = owned(&fixture, "c.box", 2, &shared, 2, None).await?;
    let off = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Off);
    fixture.apply_with(8, FamilyMode::Rebuild, &off).await?;
    let expected = fixture.exact().await?;
    ensure!(
        leads(&fixture).await? == BTreeSet::from([(b.clone(), shared.clone())]),
        "block by block, b.box leads"
    );
    let ranged = |workers: u16| {
        FamilyOptions::new(CONTENT_HASH)
            .with_max_blocks_per_run(u64::MAX)
            .with_rebuild_ranges(RebuildRanges::Through(6))
            .with_compose_chunk_keys(1)
            .with_compose_workers(workers)
    };
    fixture
        .apply_with(8, FamilyMode::Rebuild, &ranged(1))
        .await?;
    ensure!(
        fixture.exact().await? == expected,
        "one worker equals block by block"
    );

    // The work list is in `COLLATE "C"` order, which is byte order.
    let (claimed_first, claimed_second) = if b < c { (&b, &c) } else { (&c, &b) };
    let side = side(&fixture).await?;
    let first = format!("NEW.logical_name_id = '{claimed_first}'");
    let held_chunk = hold(&fixture, &side, 91, &first, false).await?;
    let two = ranged(2);
    let pass = fixture.apply_with(8, FamilyMode::Rebuild, &two);
    let control = async {
        let observed = async {
            let formed = until(60, || held(&side)).await?;
            let overlapped = formed
                && until(60, || async {
                    Ok(sqlx::query_scalar(
                        "SELECT EXISTS (SELECT 1 FROM project_compose_name_work
                         WHERE logical_name_id = $1 AND composed)",
                    )
                    .bind(claimed_second)
                    .fetch_one(&side)
                    .await?)
                })
                .await?;
            anyhow::Ok(overlapped)
        }
        .await;
        held_chunk.release().await?;
        ensure!(
            observed?,
            "the second name's chunk committed while the first's was held"
        );
        anyhow::Ok(())
    };
    let (outcome, control) = tokio::join!(pass, control);
    control?;
    outcome?;
    let found = leads(&fixture).await?;
    ensure!(
        found == BTreeSet::from([(b, shared)]),
        "two workers: the group has one lead, b.box, not {found:?}"
    );
    let now = fixture.exact().await?;
    for ((table, was), (_, rebuilt)) in expected.iter().zip(&now) {
        ensure!(
            was == rebuilt,
            "two workers: {table} is {rebuilt}, block by block it is {was}"
        );
    }
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
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}

/// Interpret filling `name`'s raw bytes in place from an observation of the whole name at
/// `block`, as it does for a surface it already holds: the raw bytes name that observation as
/// their witness, so a publication below `block` still reads the surface without them.
async fn fill_raw_bytes_at(
    fixture: &Fixture,
    logical_name_id: &str,
    name: &str,
    block: i64,
) -> Result<()> {
    let witness = fixture
        .write(
            block,
            0,
            "PreimageObserved",
            "ens_v2_registry_l1",
            Some(logical_name_id),
            None,
            json!({"source_event": "NameRegistered", "raw_name": name}),
            REGISTRY,
        )
        .await?;
    let identity: String = sqlx::query_scalar(
        "SELECT event_identity FROM normalized_events WHERE normalized_event_id = $1",
    )
    .bind(witness)
    .fetch_one(&fixture.pool)
    .await?;
    let spelled = bigname_domain::normalization::normalize_name(name)?;
    sqlx::query(
        "UPDATE name_surfaces SET raw_name = $2, raw_labels = $3, dns_encoded_name = $4,
             preimage_event_identity = $5
         WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .bind(name)
    .bind(spelled.normalized_labels)
    .bind(spelled.dns_encoded_name)
    .bind(identity)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// Row 25e: a publication below the block whose observation filled a name's raw bytes composes the
// name without them, so the stored name spells each label its chain-observed preimage spells and
// leaves the others as `[labelhash]`, listing exactly those as unhealed, though the surface
// already holds the raw bytes. At the observation's block the name is stored whole.
#[tokio::test]
async fn a_name_whose_raw_bytes_arrive_later_spells_its_known_labels() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_late_raw", 8).await?;
    let tld = labelhash("box");
    let held = unspelled(&fixture, "w.box", 1).await?;
    observe_label(&fixture, 1, "w").await?;
    preimage(&fixture, &labelhash("w"), "w").await?;
    fill_raw_bytes_at(&fixture, &held, "w.box", 6).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    let early = spelled(&fixture, &held).await?;
    ensure!(
        early == (format!("w.{}", placeholder(&tld)), vec![tld.clone()]),
        "below the observation the stored name spells its known label: {early:?}"
    );
    let parent: Option<String> = sqlx::query_scalar(
        "SELECT parent_normalized_name FROM project_address_listing
         WHERE chain_id = $1 AND address = $2 AND logical_name_id = $3",
    )
    .bind(CHAIN)
    .bind(OWNER)
    .bind(&held)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        parent == Some(placeholder(&tld)),
        "the parent key: {parent:?}"
    );
    fixture.apply(6, FamilyMode::Normal).await?;
    let whole = spelled(&fixture, &held).await?;
    ensure!(
        whole == ("w.box".to_owned(), Vec::new()),
        "at the observation the stored name is whole: {whole:?}"
    );
    fixture.assert_rebuild_equal(6).await?;
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}

// Row 25f: an undo below the block whose observation filled a name's raw bytes restores the row
// the undone block healed to `[labelhash]`, and spells it again from the kept preimage, as a
// publication at the restored block reads the surface without the raw bytes.
#[tokio::test]
async fn an_undo_below_later_raw_bytes_heals_the_restored_name() -> Result<()> {
    let fixture = Fixture::new("families_address_listing_late_raw_undo", 8).await?;
    let (v, tld) = (labelhash("v"), labelhash("box"));
    let held = unspelled(&fixture, "v.box", 1).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    observe_label(&fixture, 3, "v").await?;
    preimage(&fixture, &v, "v").await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    let healed = spelled(&fixture, &held).await?;
    ensure!(healed == (format!("v.{}", placeholder(&tld)), vec![tld]));
    fill_raw_bytes_at(&fixture, &held, "v.box", 6).await?;
    let undone = families::undo_to(&fixture.pool, CHAIN, 2).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let restored = spelled(&fixture, &held).await?;
    ensure!(
        restored == healed,
        "the undo spells the restored name from the kept preimage: {restored:?}"
    );
    fixture.assert_address_listing(CHAIN).await?;
    fixture.assert_rebuild_equal(2).await?;
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
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
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
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
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}
