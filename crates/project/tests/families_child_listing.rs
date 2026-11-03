//! F17 (`project_child_listing`) and its counts through the family loop. Every publication of
//! every family test already checks the listing against the children relation composed at the
//! published block, and the counts against the listing (`families_support/child_listing.rs`);
//! these cases drive the inputs the work list keys on and check the rows, their journal, an
//! undo and a rebuild, and today's read-time answer (`load_children_shadow_page`).
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode};
use families_support::{
    CHAIN, Fixture, hash,
    listing_cases::{
        CHAIN_KIND, OWNER, REGISTRY, ZERO, assert_served, counted, edge, journalled, listed, name,
        node, preimage,
    },
};
use serde_json::json;

// Rows 1, 6 and 25: a parent with three served ENSv1 children and four zero-owner edges stores
// three rows and a count of three, as the served relation lists and totals them; a later
// zero-owner NewOwner removes one row and moves the count by the diff; the undo and both
// rebuilds restore and equal the rows and the counts.
#[tokio::test]
async fn a_parent_stores_one_row_per_served_child_and_an_exact_count() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_rows", 20).await?;
    fixture.surface(&name(1), &node(1)).await?;
    for (log, child) in (11..=13).enumerate() {
        edge(
            &fixture,
            (10, log as i64),
            1,
            child,
            &node(100 + child),
            OWNER,
        )
        .await?;
    }
    for (log, child) in (21..=24).enumerate() {
        edge(
            &fixture,
            (10, 10 + log as i64),
            1,
            child,
            &node(100 + child),
            ZERO,
        )
        .await?;
    }
    fixture.apply(10, FamilyMode::Normal).await?;
    let children: Vec<String> = listed(&fixture, &name(1))
        .await?
        .into_iter()
        .map(|(child, _, _)| child)
        .collect();
    ensure!(
        children == vec![name(11), name(12), name(13)],
        "the zero-owner edges store no row: {children:?}"
    );
    ensure!(counted(&fixture, &name(1)).await? == vec![(String::new(), 3)]);
    assert_served(&fixture, &name(1)).await?;

    edge(&fixture, (11, 0), 1, 12, &node(112), ZERO).await?;
    fixture.assert_undo_restores(11).await?;
    ensure!(listed(&fixture, &name(1)).await?.len() == 2);
    ensure!(counted(&fixture, &name(1)).await? == vec![(String::new(), 2)]);
    ensure!(
        journalled(&fixture, 11, "project_child_listing").await? == (1, 1),
        "block 11 journals the one row it removed"
    );
    ensure!(
        journalled(&fixture, 11, "project_child_listing_count").await? == (1, 1),
        "block 11 journals the one count it moved"
    );
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// Row 12 and the parent-wide arm: a parent with no raw bytes and 2,000 ENSv1 children spells
// every child's name from its own labels. An observation of the parent's first label at block
// 12 spells the parent, so the block derives the parent whole: all 2,000 rows are re-keyed in
// one statement, each journalled with its before-image, the count stays, and the undo restores
// every row.
#[tokio::test]
async fn a_parent_spelled_by_an_observation_rekeys_and_journals_all_its_children() -> Result<()> {
    const CHILDREN: i64 = 2_000;
    let fixture = Fixture::new("families_child_listing_parent_wide", 20).await?;
    let (first, eth) = (node(0xa1), node(0xe7));
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, 'ens', $2, ARRAY[$3, $4], $5, 'active', $6, $7, 0, 'canonical')",
    )
    .bind(name(1))
    .bind(node(1))
    .bind(&first)
    .bind(&eth)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    preimage(&fixture, &eth, "eth").await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, before_state,
             after_state, raw_fact_ref)
         SELECT 'edge:' || child, 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 1, $1, 10,
                $2, '0xtx10_0', 0, child, 'ens_v2_registry_resource_surface', 'canonical',
                '{}'::jsonb,
                jsonb_build_object('source_event', 'NewOwner', 'node', $3::text,
                                   'child_node', '0x' || lpad(to_hex(child), 64, '0'),
                                   'labelhash', '0x' || lpad(to_hex(child + 4096), 64, '0'),
                                   'owner', $4::text),
                jsonb_build_object('emitting_address', $5::text)
         FROM generate_series(1, $6::bigint) child",
    )
    .bind(CHAIN)
    .bind(hash(10))
    .bind(node(1))
    .bind(OWNER)
    .bind(REGISTRY)
    .bind(CHILDREN)
    .execute(&fixture.pool)
    .await?;
    fixture
        .write(
            12,
            0,
            "PreimageObserved",
            "ens_v1_registry_l1",
            Some(&name(1)),
            None,
            json!({"node": node(1)}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let unspelled = format!("[{}].eth", &first[2..]);
    let before = listed(&fixture, &name(1)).await?;
    ensure!(before.len() == CHILDREN as usize, "{} rows", before.len());
    ensure!(
        before
            .iter()
            .all(|(_, display, _)| display.ends_with(&format!("].{unspelled}"))),
        "the children end in the parent's placeholder spelling: {:?}",
        before.first()
    );
    let kept = fixture.exact().await?;

    // The observation writes its preimage with its block.
    preimage(&fixture, &first, "parent").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let after = listed(&fixture, &name(1)).await?;
    ensure!(after.len() == CHILDREN as usize);
    ensure!(
        after
            .iter()
            .all(|(_, display, key)| display.ends_with("].parent.eth") && key == display),
        "every child is re-keyed under the spelled parent: {:?}",
        after.first()
    );
    ensure!(
        journalled(&fixture, 12, "project_child_listing").await? == (CHILDREN, CHILDREN),
        "every re-keyed row is journalled with its before-image"
    );
    ensure!(
        journalled(&fixture, 12, "project_child_listing_count").await? == (0, 0),
        "a re-key moves no count"
    );
    ensure!(counted(&fixture, &name(1)).await? == vec![(String::new(), CHILDREN)]);

    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let restored = fixture.exact().await?;
    for ((table, was), (_, now)) in kept.iter().zip(&restored) {
        ensure!(was == now, "the undo left {table} as {now}, not {was}");
    }
    ensure!(listed(&fixture, &name(1)).await? == before);
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await? == after);
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Rows 14 and 28, and the long-name key: one label carried by edge children under two parents
// is observed at block 12 for a third name. Both children are re-keyed at that block under
// their own parents and the other children keep their rows; the stored rows equal today's
// read-time rows before and after, and the undo and rebuilds equal them. A child whose name is
// longer than a btree entry allows stores its full name and its first 500 characters as the key.
#[tokio::test]
async fn an_observed_label_rekeys_its_edge_children_under_every_parent() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_shared_label", 20).await?;
    fixture.surface(&name(1), &node(1)).await?;
    fixture.surface(&name(2), &node(2)).await?;
    fixture.surface(&name(3), &node(3)).await?;
    let (shared, label): (String, String) = sqlx::query_as(
        "SELECT labelhashes[1], raw_labels[1] FROM name_surfaces WHERE logical_name_id = $1",
    )
    .bind(name(3))
    .fetch_one(&fixture.pool)
    .await?;
    let long_label = format!("l{}", "o".repeat(2_100));
    let long = format!("{:#x}", alloy_primitives::keccak256(long_label.as_bytes()));
    preimage(&fixture, &long, &long_label).await?;
    edge(&fixture, (10, 0), 1, 11, &shared, OWNER).await?;
    edge(&fixture, (10, 1), 1, 12, &node(0x112), OWNER).await?;
    edge(&fixture, (10, 2), 1, 13, &long, OWNER).await?;
    edge(&fixture, (10, 3), 2, 21, &shared, OWNER).await?;
    fixture
        .write(
            12,
            0,
            "PreimageObserved",
            "ens_v1_registry_l1",
            Some(&name(3)),
            None,
            json!({"node": node(3)}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    for parent in [name(1), name(2)] {
        assert_served(&fixture, &parent).await?;
    }
    let placeholder = format!("[{}].", &shared[2..]);
    let first = listed(&fixture, &name(1)).await?;
    let second = listed(&fixture, &name(2)).await?;
    ensure!(first[0].1.starts_with(&placeholder) && second[0].1.starts_with(&placeholder));
    let (_, long_display, long_key) = &first[2];
    ensure!(
        long_display.starts_with(&long_label)
            && long_key.chars().count() == 500
            && long_display.starts_with(long_key.as_str()),
        "a long name keys by its first 500 characters: {} bytes, key {} characters",
        long_display.len(),
        long_key.chars().count()
    );
    let kept = fixture.exact().await?;

    preimage(&fixture, &shared, &label).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let spelled = format!("{label}.");
    let first_after = listed(&fixture, &name(1)).await?;
    let second_after = listed(&fixture, &name(2)).await?;
    ensure!(
        first_after[0].1.starts_with(&spelled) && second_after[0].1.starts_with(&spelled),
        "both children carry the observed label: {first_after:?} {second_after:?}"
    );
    ensure!(
        first_after[1..] == first[1..],
        "the other children keep their rows"
    );
    ensure!(
        journalled(&fixture, 12, "project_child_listing").await? == (2, 2),
        "block 12 journals the two re-keyed rows"
    );
    for parent in [name(1), name(2)] {
        assert_served(&fixture, &parent).await?;
    }

    families::undo_to(&fixture.pool, CHAIN, 11).await?;
    let restored = fixture.exact().await?;
    for ((table, was), (_, now)) in kept.iter().zip(&restored) {
        ensure!(was == now, "the undo left {table} as {now}, not {was}");
    }
    fixture.apply(12, FamilyMode::Normal).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Row 13: a child spelled from a preimage that fails normalization serves its escaped raw
// label; a surface with no raw bytes that appears for it at block 12 makes it a named child
// without a spelling, so the block's summary composes it and its row takes the placeholder.
#[tokio::test]
async fn a_child_surface_that_appears_rekeys_its_row() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_surface", 20).await?;
    fixture.surface(&name(1), &node(1)).await?;
    let label = node(0x111);
    sqlx::query(
        "INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, normalization_error, source_kind, source_priority)
         VALUES ($1, convert_to('Upper', 'UTF8'), NULL, $2, FALSE, 'disallowed', $3, 0)",
    )
    .bind(&label)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN_KIND)
    .execute(&fixture.pool)
    .await?;
    edge(&fixture, (10, 0), 1, 11, &label, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let raw = listed(&fixture, &name(1)).await?;
    ensure!(raw.len() == 1 && raw[0].1.starts_with("Upper."), "{raw:?}");
    assert_served(&fixture, &name(1)).await?;

    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, 'ens', $2, ARRAY[$3], $4, 'active', $5, $6, 12, 'canonical')",
    )
    .bind(name(11))
    .bind(node(11))
    .bind(&label)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(12))
    .execute(&fixture.pool)
    .await?;
    fixture
        .event(families_support::Event::new(
            "filler:12",
            12,
            90,
            "PreimageObserved",
            "ens_v1_registry_l1",
        ))
        .await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let named = listed(&fixture, &name(1)).await?;
    ensure!(
        named.len() == 1 && named[0].1.starts_with(&format!("[{}].", &label[2..])),
        "{named:?}"
    );
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
