//! F17's heal rows (TYR-455): a stored name heals at the block that observes one of the labels it
//! still spells as `[labelhash]` (`unhealed_labelhashes`), each observation spelling what it can,
//! and the stored rows equal today's read-time answer before and after.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode};
use families_support::{
    CHAIN, Fixture, hash,
    listing_cases::{
        OWNER, REGISTRY, assert_served, edge, ensure_equal_but_listing, journalled, label_state,
        listed, name, node, preimage,
    },
};
use serde_json::json;

/// The label hash of `label`.
fn labelhash(label: &str) -> String {
    format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))
}

/// A surface of `id` with label hashes `labels` and no raw bytes.
async fn unspelled(fixture: &Fixture, id: u64, labels: &[&str]) -> Result<()> {
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, 'ens', $2, $3, $4, 'active', $5, $6, 0, 'canonical')",
    )
    .bind(name(id))
    .bind(node(id))
    .bind(labels)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// A preimage observation of `id` at `block`.
async fn observe(fixture: &Fixture, block: i64, id: u64) -> Result<()> {
    fixture
        .write(
            block,
            0,
            "PreimageObserved",
            "ens_v1_registry_l1",
            Some(&name(id)),
            None,
            json!({"node": node(id)}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

/// A label-only preimage observation of `label` at `block`, log `log`: no name, the label hash
/// in its after state, as an ENSv2 registry's `LabelRegistered` writes one.
async fn observe_label(fixture: &Fixture, block: i64, log: i64, label: &str) -> Result<()> {
    fixture
        .write(
            block,
            log,
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

/// The stored unhealed labels of `child`.
async fn unhealed(fixture: &Fixture, child: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT unhealed_labelhashes FROM project_child_listing
         WHERE chain_id = $1 AND child_logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(child)
    .fetch_one(&fixture.pool)
    .await?)
}

// Row 29, the observed-label residual: the label `anc` sits on a spelled ancestor's raw bytes
// (`anc.eth`) but has no preimage until the observation of `leaf.anc.eth` at block 12 writes one.
// An edge child carrying that label under another parent heals at that block, its stored name
// and key equal to today's read-time answer before and after. A work list that left out the
// labels a spelled ancestor carries would miss it.
#[tokio::test]
async fn a_label_on_a_spelled_ancestor_heals_its_children_when_first_observed() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_ancestor_label", 20).await?;
    fixture.surface(&name(1), &node(1)).await?;
    let (leaf, anc, eth) = (labelhash("leaf"), labelhash("anc"), labelhash("eth"));
    let spelled = bigname_domain::normalization::normalize_name("anc.eth")?;
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', $2, $3, $4, $5, ARRAY[$6, $7], $8, 'active', $9, $10, 0,
                 'canonical')",
    )
    .bind(name(5))
    .bind(&spelled.normalized_name)
    .bind(&spelled.normalized_labels)
    .bind(&spelled.dns_encoded_name)
    .bind(node(5))
    .bind(&anc)
    .bind(&eth)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, 'ens', $2, ARRAY[$3, $4, $5], $6, 'active', $7, $8, 0, 'canonical')",
    )
    .bind(name(6))
    .bind(node(6))
    .bind(&leaf)
    .bind(&anc)
    .bind(&eth)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    edge(&fixture, (10, 0), 1, 11, &anc, OWNER).await?;
    edge(&fixture, (10, 1), 1, 12, &node(0x112), OWNER).await?;
    fixture
        .write(
            12,
            0,
            "PreimageObserved",
            "ens_v1_registry_l1",
            Some(&name(6)),
            None,
            json!({"node": node(6)}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    assert_served(&fixture, &name(1)).await?;
    let before = listed(&fixture, &name(1)).await?;
    let placeholder = format!("[{}].", &anc[2..]);
    ensure!(
        before[0].1.starts_with(&placeholder),
        "the child carries the unknown label's placeholder: {before:?}"
    );

    // The observation writes the preimages of its labels with its block.
    preimage(&fixture, &leaf, "leaf").await?;
    preimage(&fixture, &anc, "anc").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let after = listed(&fixture, &name(1)).await?;
    ensure!(
        after[0].1.starts_with("anc.") && after[0].2 == after[0].1,
        "the child heals at the observation's block, its key with it: {after:?}"
    );
    ensure!(after[1..] == before[1..], "the other child keeps its row");
    ensure!(
        journalled(&fixture, 12, "project_child_listing").await? == (1, 1),
        "block 12 journals the healed row"
    );
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// The two-unknown-labels row: a child `c` of an unspelled parent `p.eth` spells both its own
// label and its parent's as `[labelhash]`. An observation of a name carrying `c` at block 12
// heals its own label, and the row keeps the parent's; one of a name carrying `p` at block 13
// spells it whole and leaves nothing unhealed. Each block journals the one row. The undo of
// block 13 restores every other family byte for byte and derives the row again at block 12.
// `label_preimages` keeps `p`, so the row stays whole, as a rebuild at block 12 spells it. The
// stored rows equal today's read-time answer at every step.
#[tokio::test]
async fn a_child_with_two_unknown_labels_heals_once_per_observation() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_two_labels", 20).await?;
    let (c, p, eth, o, q) = (
        labelhash("c"),
        labelhash("p"),
        labelhash("eth"),
        labelhash("o"),
        labelhash("q"),
    );
    preimage(&fixture, &eth, "eth").await?;
    unspelled(&fixture, 1, &[&p, &eth]).await?;
    unspelled(&fixture, 7, &[&c, &o]).await?;
    unspelled(&fixture, 8, &[&q, &p, &eth]).await?;
    edge(&fixture, (10, 0), 1, 11, &c, OWNER).await?;
    observe(&fixture, 12, 7).await?;
    observe(&fixture, 13, 8).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let child = name(11);
    ensure!(unhealed(&fixture, &child).await? == [c.clone(), p.clone()]);
    assert_served(&fixture, &name(1)).await?;

    preimage(&fixture, &c, "c").await?;
    preimage(&fixture, &o, "o").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let first = listed(&fixture, &name(1)).await?;
    ensure!(
        first[0].1 == format!("c.[{}].eth", &p[2..]) && first[0].2 == first[0].1,
        "block 12 spells the child's own label: {first:?}"
    );
    ensure!(unhealed(&fixture, &child).await? == std::slice::from_ref(&p));
    ensure!(journalled(&fixture, 12, "project_child_listing").await? == (1, 1));
    assert_served(&fixture, &name(1)).await?;
    let kept = fixture.exact().await?;

    preimage(&fixture, &p, "p").await?;
    preimage(&fixture, &q, "q").await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    let second = listed(&fixture, &name(1)).await?;
    ensure!(
        second[0].1 == "c.p.eth" && second[0].2 == "c.p.eth",
        "block 13 spells the child whole: {second:?}"
    );
    ensure!(unhealed(&fixture, &child).await?.is_empty());
    ensure!(journalled(&fixture, 13, "project_child_listing").await? == (1, 1));
    assert_served(&fixture, &name(1)).await?;

    let undone = families::undo_to(&fixture.pool, CHAIN, 12).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    ensure_equal_but_listing(&kept, &fixture.exact().await?)?;
    ensure!(
        listed(&fixture, &name(1)).await? == second,
        "the undo derives the row again at block 12, where both labels are known"
    );
    ensure!(unhealed(&fixture, &child).await?.is_empty());
    fixture.assert_child_listing(CHAIN).await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await? == second);
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

/// A child `c` of the spelled parent 1, published at block 11 with `c` unknown. Returns the label
/// hash of `c`.
async fn held_by_one_child(fixture: &Fixture) -> Result<String> {
    let c = labelhash("c");
    fixture.surface(&name(1), &node(1)).await?;
    edge(fixture, (10, 0), 1, 11, &c, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    ensure!(unhealed(fixture, &name(11)).await? == std::slice::from_ref(&c));
    assert_served(fixture, &name(1)).await?;
    Ok(c)
}

/// Block 12 heals the one child of `held_by_one_child` and journals its row alone.
async fn heals_the_one_child(fixture: &Fixture) -> Result<()> {
    let healed = listed(fixture, &name(1)).await?;
    ensure!(
        healed[0].1.starts_with("c.") && healed[0].2 == healed[0].1,
        "the child heals at the observation's block, its key with it: {healed:?}"
    );
    ensure!(unhealed(fixture, &name(11)).await?.is_empty());
    ensure!(
        journalled(fixture, 12, "project_child_listing").await? == (1, 1),
        "block 12 journals the healed row"
    );
    assert_served(fixture, &name(1)).await
}

// Label-only row 2: an observation of a label with no name heals the stored row whose own label
// it is at its block. The adapter writes one for a label an ENSv2 registry registers before the
// registry's parent name is known: the event has no name, and its after state carries the label
// hash. A work list that reached labels only through the surfaces of observed names would miss
// it, and the stored row would differ from a rebuild, which reads the stored preimage.
#[tokio::test]
async fn a_label_only_observation_heals_a_rows_own_label() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_label_only_own", 20).await?;
    let c = held_by_one_child(&fixture).await?;
    observe_label(&fixture, 12, 0, "c").await?;
    preimage(&fixture, &c, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    heals_the_one_child(&fixture).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Label-only row 3: a label-only observation of the parent's label heals the child row that
// spells it as `[labelhash]` through its parent's name.
#[tokio::test]
async fn a_label_only_observation_heals_the_parents_label() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_label_only_parent", 20).await?;
    let (c, p, eth) = (labelhash("c"), labelhash("p"), labelhash("eth"));
    preimage(&fixture, &eth, "eth").await?;
    preimage(&fixture, &c, "c").await?;
    unspelled(&fixture, 1, &[&p, &eth]).await?;
    edge(&fixture, (10, 0), 1, 11, &c, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    ensure!(unhealed(&fixture, &name(11)).await? == std::slice::from_ref(&p));
    assert_served(&fixture, &name(1)).await?;

    observe_label(&fixture, 12, 0, "p").await?;
    preimage(&fixture, &p, "p").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let healed = listed(&fixture, &name(1)).await?;
    ensure!(
        healed[0].1 == "c.p.eth" && healed[0].2 == "c.p.eth",
        "block 12 spells the parent's label: {healed:?}"
    );
    ensure!(unhealed(&fixture, &name(11)).await?.is_empty());
    ensure!(journalled(&fixture, 12, "project_child_listing").await? == (1, 1));
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Label-only row 4: one label-only observation heals every row that holds its label, under
// every parent: a child carrying it under each of two spelled parents, and a child whose
// unspelled parent carries it.
#[tokio::test]
async fn a_label_only_observation_heals_holders_under_every_parent() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_label_only_parents", 20).await?;
    let (c, k, eth) = (labelhash("c"), labelhash("k"), labelhash("eth"));
    preimage(&fixture, &eth, "eth").await?;
    preimage(&fixture, &k, "k").await?;
    fixture.surface(&name(1), &node(1)).await?;
    fixture.surface(&name(2), &node(2)).await?;
    unspelled(&fixture, 3, &[&c, &eth]).await?;
    edge(&fixture, (10, 0), 1, 11, &c, OWNER).await?;
    edge(&fixture, (10, 1), 2, 21, &c, OWNER).await?;
    edge(&fixture, (10, 2), 3, 31, &k, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    for child in [11, 21, 31] {
        ensure!(unhealed(&fixture, &name(child)).await? == std::slice::from_ref(&c));
    }

    observe_label(&fixture, 12, 0, "c").await?;
    preimage(&fixture, &c, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    for child in [11, 21, 31] {
        ensure!(
            unhealed(&fixture, &name(child)).await?.is_empty(),
            "block 12 heals child {child}"
        );
    }
    ensure!(listed(&fixture, &name(3)).await?[0].1 == "k.c.eth");
    ensure!(
        journalled(&fixture, 12, "project_child_listing").await? == (3, 3),
        "block 12 journals the three healed rows"
    );
    for parent in [1, 2, 3] {
        assert_served(&fixture, &name(parent)).await?;
    }
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Label-only row 5: a label-only observation of a label no stored row holds changes nothing:
// the row that holds another unknown label keeps it.
#[tokio::test]
async fn a_label_only_observation_no_row_holds_changes_nothing() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_label_only_none", 20).await?;
    let (k, u) = (labelhash("k"), labelhash("u"));
    preimage(&fixture, &k, "k").await?;
    fixture.surface(&name(1), &node(1)).await?;
    edge(&fixture, (10, 0), 1, 11, &k, OWNER).await?;
    edge(&fixture, (10, 1), 1, 12, &u, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    ensure!(unhealed(&fixture, &name(12)).await? == std::slice::from_ref(&u));
    let before = listed(&fixture, &name(1)).await?;

    observe_label(&fixture, 12, 0, "z").await?;
    preimage(&fixture, &labelhash("z"), "z").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await? == before);
    ensure!(unhealed(&fixture, &name(12)).await? == std::slice::from_ref(&u));
    ensure!(journalled(&fixture, 12, "project_child_listing").await? == (0, 0));
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Label-only row 6: one block observes the label `c` alone and through a name that carries it.
// The row that holds it heals once, and the block journals it once.
#[tokio::test]
async fn a_label_observed_alone_and_by_name_in_one_block_heals_once() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_label_only_both", 20).await?;
    let c = held_by_one_child(&fixture).await?;
    let o = labelhash("o");
    unspelled(&fixture, 7, &[&c, &o]).await?;
    observe(&fixture, 12, 7).await?;
    observe_label(&fixture, 12, 1, "c").await?;
    preimage(&fixture, &c, "c").await?;
    preimage(&fixture, &o, "o").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    heals_the_one_child(&fixture).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Label-only row 7: the block of a label-only observation is undone, as a reorg's undo does, and
// the block is applied again with the label observed again. The undo derives the row again at
// block 11, where the kept preimage spells it, so the row stays healed and the second apply
// changes nothing.
#[tokio::test]
async fn a_label_only_observation_replayed_after_an_undo_stays_healed() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_label_only_undo", 20).await?;
    let c = held_by_one_child(&fixture).await?;
    let kept = fixture.exact().await?;
    observe_label(&fixture, 12, 0, "c").await?;
    preimage(&fixture, &c, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    heals_the_one_child(&fixture).await?;

    undo_keeps_the_one_child_healed(&fixture, &kept).await?;
    replace_events(&fixture).await?;
    observe_label(&fixture, 12, 0, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    replayed_without_change(&fixture).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

/// Undo block 12 to block 11, which `kept` snapshots: every family but the listing is restored
/// byte for byte, and the one child of `held_by_one_child` stays healed, equal to the relation at
/// block 11.
async fn undo_keeps_the_one_child_healed(
    fixture: &Fixture,
    kept: &[(String, String)],
) -> Result<()> {
    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    ensure_equal_but_listing(kept, &fixture.exact().await?)?;
    let healed = listed(fixture, &name(1)).await?;
    ensure!(
        healed[0].1.starts_with("c.") && healed[0].2 == healed[0].1,
        "the undo keeps the child healed: {healed:?}"
    );
    ensure!(unhealed(fixture, &name(11)).await?.is_empty());
    fixture.assert_child_listing(CHAIN).await
}

/// Interpret's redo deletes the undone blocks' events, from block 12 up, before it writes the
/// replacement blocks'.
async fn replace_events(fixture: &Fixture) -> Result<()> {
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1 AND block_number >= 12")
        .bind(CHAIN)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

/// The replacement block 12 leaves the healed child as it stands and journals no listing row.
async fn replayed_without_change(fixture: &Fixture) -> Result<()> {
    ensure!(unhealed(fixture, &name(11)).await?.is_empty());
    ensure!(
        journalled(fixture, 12, "project_child_listing").await? == (0, 0),
        "the replacement block changes no listing row"
    );
    assert_served(fixture, &name(1)).await
}

// Undo row U1: the block that observed a name carrying `c` is undone, and its replacement does
// not observe it. `label_preimages` keeps `c`, so the undo derives the child again at block 11
// and it stays healed, as a rebuild spells it.
#[tokio::test]
async fn undoing_a_named_observation_keeps_its_holder_healed() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_undo_named", 20).await?;
    let (c, o) = (labelhash("c"), labelhash("o"));
    // The observed name's surface stands before block 11, as Interpret writes it with its block.
    unspelled(&fixture, 7, &[&c, &o]).await?;
    held_by_one_child(&fixture).await?;
    let kept = fixture.exact().await?;
    observe(&fixture, 12, 7).await?;
    preimage(&fixture, &c, "c").await?;
    preimage(&fixture, &o, "o").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    heals_the_one_child(&fixture).await?;
    undo_keeps_the_one_child_healed(&fixture, &kept).await?;
    replace_events(&fixture).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    replayed_without_change(&fixture).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Undo row U2: the same with a label-only observation.
#[tokio::test]
async fn undoing_a_label_only_observation_keeps_its_holder_healed() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_undo_label_only", 20).await?;
    let c = held_by_one_child(&fixture).await?;
    let kept = fixture.exact().await?;
    observe_label(&fixture, 12, 0, "c").await?;
    preimage(&fixture, &c, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    heals_the_one_child(&fixture).await?;
    undo_keeps_the_one_child_healed(&fixture, &kept).await?;
    replace_events(&fixture).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    replayed_without_change(&fixture).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Undo row U4: the replacement block observes another label, `d`. The undo keeps `c`'s holder
// healed, and the replacement heals `d`'s holder and journals only that row.
#[tokio::test]
async fn a_replacement_observing_another_label_heals_only_its_holder() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_undo_other_label", 20).await?;
    let (c, d) = (labelhash("c"), labelhash("d"));
    fixture.surface(&name(1), &node(1)).await?;
    edge(&fixture, (10, 0), 1, 11, &c, OWNER).await?;
    edge(&fixture, (10, 1), 1, 12, &d, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    ensure!(unhealed(&fixture, &name(11)).await? == std::slice::from_ref(&c));
    ensure!(unhealed(&fixture, &name(12)).await? == std::slice::from_ref(&d));
    let kept = fixture.exact().await?;
    observe_label(&fixture, 12, 0, "c").await?;
    preimage(&fixture, &c, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(unhealed(&fixture, &name(11)).await?.is_empty());

    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    ensure_equal_but_listing(&kept, &fixture.exact().await?)?;
    ensure!(unhealed(&fixture, &name(11)).await?.is_empty());
    ensure!(unhealed(&fixture, &name(12)).await? == std::slice::from_ref(&d));
    fixture.assert_child_listing(CHAIN).await?;
    replace_events(&fixture).await?;
    observe_label(&fixture, 12, 0, "d").await?;
    preimage(&fixture, &d, "d").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(unhealed(&fixture, &name(11)).await?.is_empty());
    ensure!(unhealed(&fixture, &name(12)).await?.is_empty());
    ensure!(
        journalled(&fixture, 12, "project_child_listing").await? == (1, 1),
        "the replacement block journals only the row `d` heals"
    );
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Undo row U5: an undo of two blocks. A child `c` of the unspelled parent `p.eth` heals its own
// label at block 12 and its parent's at block 13, so both undone blocks touch it. Each undo step
// derives it again at its own restored block, and the row stays whole. The replacement blocks
// observe nothing.
#[tokio::test]
async fn a_two_block_undo_keeps_a_holder_both_blocks_healed() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_undo_two_blocks", 20).await?;
    let (c, p, eth) = (labelhash("c"), labelhash("p"), labelhash("eth"));
    preimage(&fixture, &eth, "eth").await?;
    unspelled(&fixture, 1, &[&p, &eth]).await?;
    edge(&fixture, (10, 0), 1, 11, &c, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    ensure!(unhealed(&fixture, &name(11)).await? == [c.clone(), p.clone()]);
    let kept = fixture.exact().await?;
    observe_label(&fixture, 12, 0, "c").await?;
    observe_label(&fixture, 13, 0, "p").await?;
    preimage(&fixture, &c, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    preimage(&fixture, &p, "p").await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    for block in [12, 13] {
        ensure!(
            journalled(&fixture, block, "project_child_listing").await? == (1, 1),
            "block {block} journals the row"
        );
    }

    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    ensure!(undone == 2, "undid {undone} blocks");
    ensure_equal_but_listing(&kept, &fixture.exact().await?)?;
    let whole = listed(&fixture, &name(1)).await?;
    ensure!(
        whole[0].1 == "c.p.eth" && whole[0].2 == "c.p.eth",
        "the undo keeps the row whole: {whole:?}"
    );
    ensure!(unhealed(&fixture, &name(11)).await?.is_empty());
    fixture.assert_child_listing(CHAIN).await?;
    replace_events(&fixture).await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await? == whole);
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

// Undo row U6: the holder's edge is created in the undone block, which also observes its label.
// Its row has no before-image, and at block 11 the relation has no such child, so the undo
// removes the row and its count, and nothing is left to heal.
#[tokio::test]
async fn undoing_the_block_that_created_a_holder_removes_it() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_undo_created", 20).await?;
    let c = labelhash("c");
    fixture.surface(&name(1), &node(1)).await?;
    edge(&fixture, (10, 0), 1, 21, &labelhash("k"), OWNER).await?;
    preimage(&fixture, &labelhash("k"), "k").await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let kept = fixture.exact().await?;
    edge(&fixture, (12, 0), 1, 11, &c, OWNER).await?;
    observe_label(&fixture, 12, 1, "c").await?;
    preimage(&fixture, &c, "c").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await?.len() == 2);

    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let restored = fixture.exact().await?;
    for ((table, was), (_, now)) in kept.iter().zip(&restored) {
        ensure!(was == now, "the undo left {table} as {now}, not {was}");
    }
    fixture.assert_child_listing(CHAIN).await?;
    replace_events(&fixture).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await?.len() == 1);
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
