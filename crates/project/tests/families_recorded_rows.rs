//! Every projected row of one fixed history, recorded as a digest.
//!
//! The other rebuild tests compare two runs of the same build with each other. This one
//! compares a build with the rows recorded in `RECORDED`, so a change that is meant to leave
//! every projected row alone, such as a change to how a statement finds its rows, shows here
//! if it moves one. A change that means to project other rows records a new digest in the
//! same commit: run the test, read the digest the failure prints, and put it in `RECORDED`.
//!
//! The history is `rebuild_performance/seed.sql` for 60 names, a mix of ENSv1, NameWrapper and
//! ENSv2 names with records, reverse claims and role grants, with its clock moved to the
//! fixture's. On top is the ENSv2 path history of `families_support/ens_v2_path.rs`: an
//! admitted root, a mounted subregistry, an entry change that reaches a subname no event
//! names, resolver changes and a created registry.
//!
//! The digest covers every family table. The test requires rows in the tables the path history
//! is there to fill, and the lookup and summary rows of the subname. Eight tables are empty in
//! this history and the digest says nothing about them: the two hydration work tables, the two
//! address record indexes, the record values by record id, the resolver links, the account
//! approvals and the Universal Resolver proxies.
//!
//! Project clears obsolete hydration retry scheduling while it applies each Ethereum Mainnet
//! block, and only there (`docs/projections.md`, "Follow-only hydration"). So the same seed is
//! also recorded on Ethereum Mainnet, without the ENSv2 path history, in `RECORDED_MAINNET`.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, RebuildRanges};
use serde_json::Value;
use sqlx::raw_sql;
use support::{CHAIN, CONTENT_HASH, Fixture, ens_v2_path, marker};

const SEED: &str = include_str!("rebuild_performance/seed.sql");
const NAMES: &str = "60";
const TARGET: i64 = 300;
/// SHA-256 over every family table and the family marker after a rebuild to `TARGET`. The
/// family marker and the history catalogue marker are taken without their generation and
/// without the content hash of the build that wrote them, which differs between builds by
/// design.
const RECORDED: &str = "ce61ff3fb2433f8e01537e63561ad5ebd3b8b0b042f71c434a475fd6fcad0bd1";
const MAINNET: &str = "ethereum-mainnet";
/// The same digest for the seed alone on `MAINNET`.
const RECORDED_MAINNET: &str = "9344ccd591a578fc3d512489d2c0ebcecf30db75aaebc7303253a99275b7da0f";

/// The tables the ENSv2 path history fills. The digest holds a selection change to account
/// only where they have rows.
const PATH_TABLES: [&str; 4] = [
    "project_ens_v2_entry_owner",
    "project_ens_v2_registry_parent",
    "project_parent_subregistry",
    "project_child_edge_candidate",
];
/// The tables that hold the published rows of a name the selection reaches.
const PUBLISHED_TABLES: [&str; 2] = ["project_lookup_name", "project_name_summary"];
/// Two of the tables whose changes say which hydration retry scheduling an Ethereum Mainnet block
/// clears. The mainnet digest holds a change to that clearing to account only where they have
/// rows.
const MAINNET_TABLES: [&str; 2] = ["project_node_record_value", "project_reverse_tuple"];

/// Drop the content hash of the build that wrote a marker row.
fn without_content_hash(row: &mut Value) {
    row.as_object_mut()
        .expect("a marker row")
        .remove("input_content_hash");
}

/// The digest of the families and the marker of `chain` as they stand, and one digest per table
/// to locate a difference. Each of `filled` must have rows. `subname` is the name only the path
/// selection reaches, when the history has one.
async fn digest(
    fixture: &Fixture,
    chain: &str,
    filled: &[&str],
    subname: Option<&str>,
) -> Result<(String, Vec<(String, String)>)> {
    let sha256 = |text: String| async move {
        sqlx::query_scalar::<_, String>("SELECT encode(sha256(convert_to($1, 'UTF8')), 'hex')")
            .bind(text)
            .fetch_one(&fixture.pool)
            .await
    };
    let mut every = String::new();
    let mut tables = Vec::new();
    for (table, mut rows) in fixture.exact().await? {
        if table == "marker" {
            rows = sqlx::query_scalar(
                "SELECT (to_jsonb(m) - 'sequence')::text FROM project_family_marker m
                 WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(&fixture.pool)
            .await?;
            let mut marker: Value = serde_json::from_str(&rows)?;
            without_content_hash(&mut marker);
            rows = marker.to_string();
        }
        if table == "project_history_catalogue_marker" {
            let mut stamps: Vec<Value> = serde_json::from_str(&rows)?;
            stamps.iter_mut().for_each(without_content_hash);
            rows = Value::Array(stamps).to_string();
        }
        ensure!(
            !filled.contains(&table.as_str()) || rows != "[]",
            "{table} has no rows, so the digest does not cover it"
        );
        if let Some(subname) = subname {
            ensure!(
                !PUBLISHED_TABLES.contains(&table.as_str()) || rows.contains(subname),
                "{table} has no row of {subname}, so the digest does not cover it"
            );
        }
        every.push_str(&format!("{table}\n{rows}\n"));
        tables.push((table, sha256(rows).await?));
    }
    Ok((sha256(every).await?, tables))
}

/// Seed the history, rebuild it in ranges of at most `blocks` blocks and require the recorded
/// rows.
async fn rebuilt(blocks: u64) -> Result<()> {
    let fixture = Fixture::new(&format!("families_recorded_rows_{blocks}"), 0).await?;
    raw_sql(
        &SEED
            .replace("__NAMES__", NAMES)
            .replace("__CHAIN__", CHAIN)
            .replace("1700000000", "1800000000"),
    )
    .execute(&fixture.pool)
    .await?;
    let names = ens_v2_path::history(&fixture).await?;
    let options = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Through(TARGET))
        .with_range_caps(blocks, u64::MAX);
    let token = families::input_token(&fixture.pool, CHAIN).await?;
    let outcome = families::apply(
        &fixture.pool,
        CHAIN,
        &marker(TARGET),
        FamilyMode::Rebuild,
        &token,
        &options,
    )
    .await?;
    ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(TARGET),
        "the rebuild in ranges of {blocks} blocks stopped at {:?}",
        outcome.marker
    );
    let covered: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM project_address_history_anchor WHERE historical_mask <> 0)",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        covered,
        "the recorded digest must cover historical address membership"
    );
    // A rebuild in ranges composes the ids with no surface in its composition pass, from the
    // base rows and the interpreted events, so the digest must hold the empty rows block by block
    // leaves for them.
    let unsurfaced: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM project_lookup_name lookup WHERE lookup.core IS NULL
                 AND NOT EXISTS (SELECT 1 FROM name_surfaces surface
                                 WHERE surface.logical_name_id = lookup.logical_name_id)),
                (SELECT count(*) FROM project_history_source source
                 WHERE source.source_kind = 0 AND source.event_mask = 0
                   AND NOT EXISTS (SELECT 1 FROM name_surfaces surface
                                   WHERE surface.logical_name_id = source.source_key))",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        unsurfaced.0 > 0 && unsurfaced.1 > 0,
        "the recorded digest must cover the lookup rows and name sources of ids with no surface: \
         {unsurfaced:?}"
    );
    let (digest, tables) = digest(&fixture, CHAIN, &PATH_TABLES, Some(&names.sub)).await?;
    ensure!(
        digest == RECORDED,
        "a rebuild in ranges of {blocks} blocks projects rows with digest {digest}, not the \
         recorded {RECORDED}; by table: {tables:#?}"
    );
    fixture.cleanup().await
}

// Rebuilt block by block, in ranges of four blocks and in ranges of sixty-four, the history
// projects the recorded rows. The three rebuilds run side by side, each in its own database.
#[tokio::test]
async fn the_all_families_fixture_rebuilds_to_the_recorded_rows() -> Result<()> {
    tokio::try_join!(rebuilt(1), rebuilt(4), rebuilt(64))?;
    Ok(())
}

/// The seed alone on `MAINNET`, with its clock moved to the fixture's.
async fn mainnet_fixture(label: &str) -> Result<Fixture> {
    let fixture = Fixture::new(label, 0).await?;
    fixture.lineage(MAINNET, 0).await?;
    raw_sql(
        &SEED
            .replace("__NAMES__", NAMES)
            .replace("__CHAIN__", MAINNET)
            .replace("1700000000", "1800000000"),
    )
    .execute(&fixture.pool)
    .await?;
    Ok(fixture)
}

/// One family run of `MAINNET` to `TARGET`.
async fn mainnet_run(
    fixture: &Fixture,
    mode: FamilyMode,
    options: &FamilyOptions,
) -> Result<families::FamilyOutcome> {
    let token = families::input_token(&fixture.pool, MAINNET).await?;
    Ok(families::apply(
        &fixture.pool,
        MAINNET,
        &marker(TARGET),
        mode,
        &token,
        options,
    )
    .await?)
}

/// Require the recorded mainnet rows after a rebuild described by `label`.
async fn require_recorded_mainnet(fixture: &Fixture, label: &str) -> Result<()> {
    let (digest, tables) = digest(fixture, MAINNET, &MAINNET_TABLES, None).await?;
    ensure!(
        digest == RECORDED_MAINNET,
        "a mainnet rebuild {label} projects rows with digest {digest}, not the recorded \
         {RECORDED_MAINNET}; by table: {tables:#?}"
    );
    Ok(())
}

/// Seed the mainnet history, rebuild it in ranges of at most `blocks` blocks and require the
/// recorded rows.
async fn rebuilt_on_mainnet(blocks: u64) -> Result<()> {
    let fixture = mainnet_fixture(&format!("families_recorded_rows_mainnet_{blocks}")).await?;
    let options = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Through(TARGET))
        .with_range_caps(blocks, u64::MAX);
    let outcome = mainnet_run(&fixture, FamilyMode::Rebuild, &options).await?;
    ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(TARGET),
        "the mainnet rebuild in ranges of {blocks} blocks stopped at {:?}",
        outcome.marker
    );
    require_recorded_mainnet(&fixture, &format!("in ranges of {blocks} blocks")).await?;
    fixture.cleanup().await
}

// The same history on Ethereum Mainnet, rebuilt block by block, in ranges of four blocks and in
// ranges of sixty-four, projects its recorded rows.
#[tokio::test]
async fn the_mainnet_fixture_rebuilds_to_the_recorded_rows() -> Result<()> {
    tokio::try_join!(
        rebuilt_on_mainnet(1),
        rebuilt_on_mainnet(4),
        rebuilt_on_mainnet(64)
    )?;
    Ok(())
}

// A rebuild stopped by its budget resumes on the next run with a range of its whole budget, the
// largest range a run applies in one transaction. Rebuilt seven work blocks a run on Ethereum
// Mainnet, and then again block by block, the history projects its recorded rows both times.
#[tokio::test]
async fn a_mainnet_range_rebuild_resumed_after_budget_stops_equals_block_by_block() -> Result<()> {
    let fixture = mainnet_fixture("families_recorded_rows_mainnet_resumed").await?;
    let options = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(7)
        .with_rebuild_ranges(RebuildRanges::Through(TARGET));
    let mut mode = FamilyMode::Rebuild;
    let mut runs = 0;
    loop {
        let outcome = mainnet_run(&fixture, mode.clone(), &options).await?;
        ensure!(
            !outcome.reset || runs == 0,
            "run {runs} restarted the rebuild"
        );
        // A run spends its budget on ranges, on the composition pass or, once the pass has
        // ended, on the target block alone.
        ensure!(
            runs == 0 || outcome.ranges > 0 || outcome.compose_chunks > 0 || outcome.blocks > 0,
            "run {runs} applied no block and no chunk of the composition pass"
        );
        runs += 1;
        mode = FamilyMode::Normal;
        if outcome.marker.as_ref().map(|marker| marker.number) == Some(TARGET) {
            break;
        }
        ensure!(runs < 200, "the rebuild did not finish");
    }
    ensure!(
        runs > 2,
        "the rebuild finished in {runs} runs, so it never resumed"
    );
    require_recorded_mainnet(&fixture, "resumed seven work blocks a run").await?;
    let per_block = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Off);
    let outcome = mainnet_run(&fixture, FamilyMode::Rebuild, &per_block).await?;
    ensure!(
        (
            outcome.marker.as_ref().map(|marker| marker.number),
            outcome.ranges
        ) == (Some(TARGET), 0),
        "the block-by-block rebuild stopped at {:?} after {} ranges",
        outcome.marker,
        outcome.ranges
    );
    require_recorded_mainnet(&fixture, "block by block").await?;
    fixture.cleanup().await
}
