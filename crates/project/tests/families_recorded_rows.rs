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
const RECORDED: &str = "7a61dec99f6ea22c2a236cf6f33f9130b9422e2d4057c7222b0d42efb4c2da0a";

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

/// Drop the content hash of the build that wrote a marker row.
fn without_content_hash(row: &mut Value) {
    row.as_object_mut()
        .expect("a marker row")
        .remove("input_content_hash");
}

/// The digest of the families as they stand, and one digest per table to locate a difference.
/// `subname` is the name only the path selection reaches.
async fn digest(fixture: &Fixture, subname: &str) -> Result<(String, Vec<(String, String)>)> {
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
            !PATH_TABLES.contains(&table.as_str()) || rows != "[]",
            "{table} has no rows, so the digest does not cover it"
        );
        ensure!(
            !PUBLISHED_TABLES.contains(&table.as_str()) || rows.contains(subname),
            "{table} has no row of {subname}, so the digest does not cover it"
        );
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
    let (digest, tables) = digest(&fixture, &names.sub).await?;
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
