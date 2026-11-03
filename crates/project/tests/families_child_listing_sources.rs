//! F17's heals from other chains (TYR-455): `label_preimages` is shared by every chain, so a
//! label first observed on another chain heals this chain's stored names at this chain's next
//! block. Each chain keeps, per other chain, the highest block at or below the other chain's
//! finality floor whose observations it has read (`project_child_preimage_watermark`),
//! journalled with its block.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, RebuildRanges};
use families_support::{
    CHAIN, CONTENT_HASH, Event, Fixture, hash,
    listing_cases::{OWNER, assert_served, edge, journalled, listed, name, node, preimage},
};
use sqlx::raw_sql;

/// The other chain the observations come from.
const SOURCE: &str = "base-sepolia";

const WATERMARK: &str = "project_child_preimage_watermark";

/// The label hash of `label`.
fn labelhash(label: &str) -> String {
    format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))
}

/// `chain`'s canonical blocks `0..=20`, with its latest head at block 20 and its safe and
/// finalized heads at `safe` and `finalized`.
async fn chain_with_heads(
    fixture: &Fixture,
    chain: &str,
    safe: Option<i64>,
    finalized: Option<i64>,
) -> Result<()> {
    fixture.lineage(chain, 20).await?;
    for (state, through) in [("safe", safe), ("finalized", finalized)] {
        let Some(through) = through else {
            continue;
        };
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = $2::canonicality_state
             WHERE chain_id = $1 AND block_number <= $3
               AND canonicality_state IN ('canonical', 'safe')
               AND canonicality_state::text <> $2",
        )
        .bind(chain)
        .bind(state)
        .bind(through)
        .execute(&fixture.pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number,
             safe_block_hash, safe_block_number, finalized_block_hash, finalized_block_number)
         VALUES ($1, $2, 20, $3, $4, $5, $6)",
    )
    .bind(chain)
    .bind(hash(20))
    .bind(safe.map(hash))
    .bind(safe)
    .bind(finalized.map(hash))
    .bind(finalized)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// A surface on `SOURCE` of `id` with label hashes `labels` and no raw bytes.
async fn source_surface(fixture: &Fixture, id: u64, labels: &[&str]) -> Result<()> {
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
    .bind(SOURCE)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// `SOURCE` with every head at block 20 and a surface of `id` with label hashes `labels`.
async fn source(fixture: &Fixture, id: u64, labels: &[&str]) -> Result<()> {
    chain_with_heads(fixture, SOURCE, Some(20), Some(20)).await?;
    source_surface(fixture, id, labels).await
}

/// `count` preimage observations of `id` on `SOURCE` at `block`, one log each.
async fn observe_many(fixture: &Fixture, block: i64, id: u64, count: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
             event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state)
         SELECT 'dense:' || $3::bigint || ':' || log, 'ens', $1, 'PreimageObserved',
                'ens_v1_registry_l1', 1, $2, $3::bigint, $4, '0xdense', 0, log,
                'ens_v2_registry_resource_surface', 'canonical'
         FROM generate_series(1::bigint, $5::bigint) log",
    )
    .bind(name(id))
    .bind(SOURCE)
    .bind(block)
    .bind(hash(block))
    .bind(count)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// Replace `SOURCE`'s events from block `from` up, as a reorg's Interpret redo does before it
/// interprets the replacement blocks.
async fn reorg(fixture: &Fixture, from: i64) -> Result<()> {
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1 AND block_number >= $2")
        .bind(SOURCE)
        .bind(from)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

/// A preimage observation of `id` on `SOURCE` at `block`.
async fn observe(fixture: &Fixture, block: i64, id: u64) -> Result<()> {
    let identity = format!("source:{block}:{id}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                0,
                "PreimageObserved",
                "ens_v1_registry_l1",
            )
            .on(SOURCE)
            .name(&name(id)),
        )
        .await?;
    Ok(())
}

/// `CHAIN`'s watermark for `SOURCE`, none without a row.
async fn watermark(fixture: &Fixture) -> Result<Option<i64>> {
    watermark_of(fixture, SOURCE).await
}

/// `CHAIN`'s watermark for `source`, none without a row.
async fn watermark_of(fixture: &Fixture, source: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT source_block_number FROM project_child_preimage_watermark
         WHERE chain_id = $1 AND source_chain_id = $2",
    )
    .bind(CHAIN)
    .bind(source)
    .fetch_optional(&fixture.pool)
    .await?)
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

/// A child `c` of the spelled parent 1 whose label `c` has no preimage, published at block 11;
/// then `SOURCE` observes name 7, which carries `c`, at its block 5, and the observation writes
/// the preimages of its labels. Returns the label hash of `c`.
async fn observed_elsewhere(fixture: &Fixture) -> Result<String> {
    let (c, o) = (labelhash("c"), labelhash("o"));
    fixture.surface(&name(1), &node(1)).await?;
    edge(fixture, (10, 0), 1, 11, &c, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    ensure!(unhealed(fixture, &name(11)).await? == std::slice::from_ref(&c));
    assert_served(fixture, &name(1)).await?;
    source(fixture, 7, &[&c, &o]).await?;
    observe(fixture, 5, 7).await?;
    preimage(fixture, &c, "c").await?;
    preimage(fixture, &o, "o").await?;
    Ok(c)
}

// Row 1: a label observed on another chain heals a holder here at this chain's next block, its
// stored name and key equal to today's read-time answer, and the block journals the healed row
// and the watermark's first read. A step that read only its own chain's observations would
// leave the row spelling `[labelhash]`.
#[tokio::test]
async fn a_label_observed_on_another_chain_heals_a_holder_at_the_next_block() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_heal", 20).await?;
    observed_elsewhere(&fixture).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let healed = listed(&fixture, &name(1)).await?;
    ensure!(
        healed[0].1.starts_with("c.") && healed[0].2 == healed[0].1,
        "the child heals at the next block, its key with it: {healed:?}"
    );
    ensure!(unhealed(&fixture, &name(11)).await?.is_empty());
    ensure!(
        journalled(&fixture, 12, "project_child_listing").await? == (1, 1),
        "block 12 journals the healed row"
    );
    ensure!(
        journalled(&fixture, 12, WATERMARK).await? == (1, 0),
        "block 12 journals the watermark's first read, with no row before it"
    );
    ensure!(watermark(&fixture).await? == Some(5));
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Row 2: reading an already-read block of the other chain again derives the rows it healed
// again and changes nothing: the listing journals nothing, and only the watermark moves back to
// where it was.
#[tokio::test]
async fn reading_an_already_read_source_block_again_changes_nothing() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_reread", 20).await?;
    observed_elsewhere(&fixture).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let healed = listed(&fixture, &name(1)).await?;
    sqlx::query(
        "UPDATE project_child_preimage_watermark SET source_block_number = 4
         WHERE chain_id = $1 AND source_chain_id = $2",
    )
    .bind(CHAIN)
    .bind(SOURCE)
    .execute(&fixture.pool)
    .await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await? == healed);
    ensure!(
        journalled(&fixture, 13, "project_child_listing").await? == (0, 0),
        "the second read derives no new row"
    );
    ensure!(journalled(&fixture, 13, WATERMARK).await? == (1, 1));
    ensure!(watermark(&fixture).await? == Some(5));
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

// Row 3: undoing this chain's block restores the watermark from the journal with the rows it
// healed, byte for byte, and running the block again heals them the same way.
#[tokio::test]
async fn undoing_the_block_restores_the_watermark_and_the_rerun_heals_again() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_undo", 20).await?;
    let c = observed_elsewhere(&fixture).await?;
    let kept = fixture.exact().await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let healed = listed(&fixture, &name(1)).await?;
    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let restored = fixture.exact().await?;
    for ((table, was), (_, now)) in kept.iter().zip(&restored) {
        ensure!(was == now, "the undo left {table} as {now}, not {was}");
    }
    ensure!(watermark(&fixture).await?.is_none());
    ensure!(unhealed(&fixture, &name(11)).await? == std::slice::from_ref(&c));
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(listed(&fixture, &name(1)).await? == healed);
    ensure!(watermark(&fixture).await? == Some(5));
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Row 4: the schema-migration that adds the watermarks starts this chain's at the other chain's
// highest observation or its finality floor, whichever is lower, and seeds none for a chain
// with neither finality marker. The first family step after it reads only what follows or
// what a reorg can still replace, and a rebuild, whose reset clears the watermarks, reads the
// other chain from block 0 again.
#[tokio::test]
async fn the_migration_starts_at_the_source_head_and_a_rebuild_from_zero() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_start", 20).await?;
    let kid = labelhash("kid");
    preimage(&fixture, &kid, "kid").await?;
    fixture.surface(&name(1), &node(1)).await?;
    edge(&fixture, (10, 0), 1, 11, &kid, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    // No stored name holds the observed labels, so reading them heals nothing.
    chain_with_heads(&fixture, SOURCE, Some(4), Some(4)).await?;
    source_surface(&fixture, 7, &[&labelhash("x"), &labelhash("y")]).await?;
    observe(&fixture, 3, 7).await?;
    observe(&fixture, 5, 7).await?;
    let unmarked = "ethereum-holesky";
    chain_with_heads(&fixture, unmarked, None, None).await?;
    ensure!(watermark(&fixture).await?.is_none());

    raw_sql(include_str!(
        "../../../migrations/20261103120000_project_child_listing.sql"
    ))
    .execute(&fixture.pool)
    .await?;
    ensure!(
        watermark(&fixture).await? == Some(4),
        "the migration starts the watermark at the source's floor, below its last observation"
    );
    ensure!(
        watermark_of(&fixture, unmarked).await?.is_none(),
        "a source with no finality marker is not seeded"
    );
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(
        journalled(&fixture, 12, WATERMARK).await? == (0, 0),
        "the first step after the migration moves no watermark"
    );
    ensure!(watermark(&fixture).await? == Some(4));
    ensure!(watermark_of(&fixture, unmarked).await?.is_none());

    // A value no read of the source's observations writes, which only a reset clears.
    sqlx::query(
        "UPDATE project_child_preimage_watermark SET source_block_number = 9
         WHERE chain_id = $1 AND source_chain_id = $2",
    )
    .bind(CHAIN)
    .bind(SOURCE)
    .execute(&fixture.pool)
    .await?;
    let options = FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Off);
    fixture
        .apply_with(12, FamilyMode::Rebuild, &options)
        .await?;
    ensure!(
        watermark(&fixture).await? == Some(4),
        "the rebuild reads the source from block 0"
    );
    assert_served(&fixture, &name(1)).await?;
    fixture.cleanup().await
}

/// A child `c` of the spelled parent 1 published at block 11, and `SOURCE` with its safe and
/// finalized heads at `safe` and `finalized`, which observes an unrelated name 9 at block 5.
/// Block 12 reads it. Returns the label hashes of `c` and `o`.
async fn read_before_reorg(
    fixture: &Fixture,
    safe: Option<i64>,
    finalized: Option<i64>,
) -> Result<(String, String)> {
    let (c, o) = (labelhash("c"), labelhash("o"));
    fixture.surface(&name(1), &node(1)).await?;
    edge(fixture, (10, 0), 1, 11, &c, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    chain_with_heads(fixture, SOURCE, safe, finalized).await?;
    source_surface(fixture, 7, &[&c, &o]).await?;
    source_surface(fixture, 9, &[&labelhash("x")]).await?;
    observe(fixture, 5, 9).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    Ok((c, o))
}

/// A reorg of `SOURCE` from block 3 whose replacement block 4 reveals `c`, below block 5 that
/// block 12 read; block 13 reads the replacement and heals the child.
async fn replacement_heals(fixture: &Fixture, (c, o): (String, String)) -> Result<()> {
    ensure!(unhealed(fixture, &name(11)).await? == std::slice::from_ref(&c));
    reorg(fixture, 3).await?;
    observe(fixture, 4, 7).await?;
    preimage(fixture, &c, "c").await?;
    preimage(fixture, &o, "o").await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    let healed = listed(fixture, &name(1)).await?;
    ensure!(
        healed[0].1.starts_with("c.") && healed[0].2 == healed[0].1,
        "the replacement block heals the child at the next block: {healed:?}"
    );
    ensure!(journalled(fixture, 13, "project_child_listing").await? == (1, 1));
    assert_served(fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(13).await
}

// Row 5: the watermark stops at the other chain's finalized block, below the block it read, so
// a label revealed only in a replacement block of a reorg below that read heals here too. A
// watermark at the highest block read would skip the replacement.
#[tokio::test]
async fn a_replacement_block_below_the_read_heals_with_finality_markers() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_reorg", 20).await?;
    let labels = read_before_reorg(&fixture, Some(2), Some(2)).await?;
    replacement_heals(&fixture, labels).await?;
    ensure!(
        watermark(&fixture).await? == Some(2),
        "blocks 12 and 13 read through blocks 5 and 4 and stop the watermark at the finalized block"
    );
    fixture.cleanup().await
}

// Row 6: the same with a safe head and no finalized one: the watermark stops at the safe block.
#[tokio::test]
async fn a_replacement_block_below_the_read_heals_with_only_a_safe_marker() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_reorg_safe", 20).await?;
    let labels = read_before_reorg(&fixture, Some(2), None).await?;
    replacement_heals(&fixture, labels).await?;
    ensure!(
        watermark(&fixture).await? == Some(2),
        "blocks 12 and 13 read through blocks 5 and 4 and stop the watermark at the safe block"
    );
    fixture.cleanup().await
}

// Row 8: at the finality floor, a step reads the whole tail above it, so a label revealed after
// more than a read's worth of observations in the unfinalized tail heals at the next block.
#[tokio::test]
async fn a_dense_unfinalized_tail_heals_its_last_row_in_one_step() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_dense_tail", 20).await?;
    let (c, o) = (labelhash("c"), labelhash("o"));
    fixture.surface(&name(1), &node(1)).await?;
    edge(&fixture, (10, 0), 1, 11, &c, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    chain_with_heads(&fixture, SOURCE, Some(0), Some(0)).await?;
    source_surface(&fixture, 7, &[&c, &o]).await?;
    source_surface(&fixture, 8, &[&labelhash("x")]).await?;
    source_surface(&fixture, 9, &[&labelhash("y")]).await?;
    observe(&fixture, 0, 9).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(
        watermark(&fixture).await? == Some(0),
        "the watermark reaches the floor"
    );

    observe_many(&fixture, 1, 8, 1001).await?;
    observe(&fixture, 15, 7).await?;
    preimage(&fixture, &c, "c").await?;
    preimage(&fixture, &o, "o").await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    let healed = listed(&fixture, &name(1)).await?;
    ensure!(
        healed[0].1.starts_with("c.") && healed[0].2 == healed[0].1,
        "the last observation of the tail heals the child at the next block: {healed:?}"
    );
    ensure!(watermark(&fixture).await? == Some(0));
    assert_served(&fixture, &name(1)).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

// Row 10: below the floor, a read stops only at a block boundary, so a block that alone holds
// more than a read's worth of observations is read whole, its last observation included, and
// the next step reads past it.
#[tokio::test]
async fn a_block_denser_than_a_read_is_read_whole_and_passed() -> Result<()> {
    let fixture = Fixture::new("families_child_listing_source_dense_block", 20).await?;
    let (c, o) = (labelhash("c"), labelhash("o"));
    fixture.surface(&name(1), &node(1)).await?;
    edge(&fixture, (10, 0), 1, 11, &c, OWNER).await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    chain_with_heads(&fixture, SOURCE, Some(20), Some(20)).await?;
    source_surface(&fixture, 7, &[&c, &o]).await?;
    source_surface(&fixture, 8, &[&labelhash("x")]).await?;
    source_surface(&fixture, 9, &[&labelhash("y")]).await?;
    observe_many(&fixture, 3, 8, 1001).await?;
    observe(&fixture, 3, 7).await?;
    observe(&fixture, 6, 9).await?;
    preimage(&fixture, &c, "c").await?;
    preimage(&fixture, &o, "o").await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let healed = listed(&fixture, &name(1)).await?;
    ensure!(
        healed[0].1.starts_with("c.") && healed[0].2 == healed[0].1,
        "the block's last observation heals the child: {healed:?}"
    );
    ensure!(
        watermark(&fixture).await? == Some(3),
        "the read stops at the dense block's end"
    );
    fixture.apply(13, FamilyMode::Normal).await?;
    ensure!(
        watermark(&fixture).await? == Some(6),
        "the next step reads past it"
    );
    // No rebuild comparison: a rebuild reads the source again from block 0 and catches up over
    // the blocks it composes, so its watermark can stand below this one until its next steps.
    assert_served(&fixture, &name(1)).await?;
    fixture.cleanup().await
}
