//! Row 27 of the TYR-455 child listing table. The migration gate keeps a locked wrapped child
//! under its migrated parent while the child's wrapper fuses read, and its wrapper expiry ends
//! that with no block of its own. The stored listing (`project_child_listing`) carries the
//! first second past the expiry as the row's `recompose_at`, keeps the row at a block on the
//! expiry second, and drops it at the first block past it, as the served subnames do.
use super::*;

/// The locked child's wrapper expiry: after the migration block's time, before the parent's
/// reservation expires.
const CHILD_EXPIRY: u64 = 1_800_000_000;

/// The parent's stored rows as (display name, recompose time), in name order.
async fn stored(database: &TestDatabase, parent: &str) -> Result<Vec<(String, Option<i64>)>> {
    Ok(sqlx::query_as(
        "SELECT display_name, recompose_at::bigint FROM project_child_listing
         WHERE parent_logical_name_id = $1 ORDER BY display_name",
    )
    .bind(parent)
    .fetch_all(&database.pool)
    .await?)
}

/// The served subnames of the migrated parent, in name order.
async fn served(database: &TestDatabase) -> Result<Vec<String>> {
    let page = path_get(
        database,
        &format!("/v1/names/{NAME}/subnames?include_expired=true&page_size=200"),
    )
    .await?;
    let mut names: Vec<String> = page["data"]
        .as_array()
        .context("a subnames page")?
        .iter()
        .filter_map(|row| row["name"].as_str().map(str::to_owned))
        .collect();
    names.sort();
    Ok(names)
}

/// The stored rows equal the served subnames, and the child is listed or not.
async fn check(database: &TestDatabase, parent: &str, listed: bool) -> Result<()> {
    let rows = stored(database, parent).await?;
    let names: Vec<String> = rows.iter().map(|(name, _)| name.clone()).collect();
    assert_eq!(names, served(database).await?, "stored {rows:?}");
    let child = rows.iter().find(|(name, _)| name == CHILD);
    if listed {
        assert_eq!(
            child.map(|(_, at)| *at),
            Some(Some(i64::try_from(CHILD_EXPIRY)? + 1)),
            "the kept child recomposes at the first second past its wrapper expiry: {rows:?}"
        );
    } else {
        assert!(child.is_none(), "the child left: {rows:?}");
    }
    Ok(())
}

#[tokio::test]
async fn a_kept_locked_child_leaves_the_listing_at_the_first_block_past_its_expiry() -> Result<()> {
    let (database, _) = wrapped_setup_until(CHILD_EXPIRY).await?;
    let parent = format!("ens:{}", bigname_lookup::ens_namehash_hex(NAME)?);
    check(&database, &parent, true).await?;
    let expiry = i64::try_from(CHILD_EXPIRY)?;
    // A block on the expiry second: the fuses still read, so the gate keeps the child.
    seed_and_run_with(&database, &[], 123, 123, &[], Some(expiry)).await?;
    check(&database, &parent, true).await?;
    // The next second the fuses read as zero and the gate lets the child go.
    seed_and_run_with(&database, &[], 124, 124, &[], Some(expiry + 1)).await?;
    check(&database, &parent, false).await?;
    database.cleanup().await
}
