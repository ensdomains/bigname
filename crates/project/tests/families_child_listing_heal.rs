//! F17's heal rows (TYR-455): a stored name heals at the block that observes one of the labels it
//! still spells as `[labelhash]` (`unhealed_labelhashes`), each observation spelling what it can,
//! and the stored rows equal today's read-time answer before and after.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode};
use families_support::{
    CHAIN, Fixture, hash,
    listing_cases::{OWNER, REGISTRY, assert_served, edge, journalled, listed, name, node, preimage},
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
// block 13 restores the row, its unhealed labels included, byte for byte, and the stored rows
// equal today's read-time answer at every step.
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
    ensure!(unhealed(&fixture, &child).await? == [p.clone()]);
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
    let restored = fixture.exact().await?;
    for ((table, was), (_, now)) in kept.iter().zip(&restored) {
        ensure!(was == now, "the undo left {table} as {now}, not {was}");
    }
    ensure!(unhealed(&fixture, &child).await? == [p.clone()]);
    fixture.apply(13, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await? == second);
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}
