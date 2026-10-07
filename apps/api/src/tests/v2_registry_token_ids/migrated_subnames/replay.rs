use super::*;

pub(super) async fn families(database: &TestDatabase) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for table in bigname_project::families::family_tables() {
        let row = if table == "project_history_catalogue_marker" {
            "to_jsonb(t)-'publication_sequence'"
        } else {
            "to_jsonb(t)"
        };
        let data: String = sqlx::query_scalar(&format!(
            "SELECT coalesce(jsonb_agg({row} ORDER BY ({row})::text),'[]')::text FROM {table} t"
        ))
        .fetch_one(&database.pool)
        .await?;
        out.push((table.into(), data));
    }
    Ok(out)
}

pub(super) async fn assert_rebuild(database: &TestDatabase, offset: i64) -> Result<()> {
    use bigname_project::families::{FamilyMode, FamilyOptions, RebuildRanges};
    let expected = families(database).await?;
    let target = BASE + offset;
    for ranges in [RebuildRanges::Through(target), RebuildRanges::Off] {
        let token = bigname_project::families::input_token(&database.pool, PATH_CHAIN).await?;
        let outcome = bigname_project::families::apply(
            &database.pool,
            PATH_CHAIN,
            &bigname_project::Marker {
                number: target,
                hash: format!("0xhistory{target}"),
            },
            FamilyMode::Rebuild,
            &token,
            &FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
                .with_rebuild_ranges(ranges),
        )
        .await?;
        assert_eq!(
            outcome.marker.as_ref().map(|marker| marker.number),
            Some(target),
            "{outcome:#?}"
        );
        for ((table, old), (_, new)) in expected.iter().zip(families(database).await?) {
            assert_eq!(&new, old, "{table}: rebuild {ranges:?}");
        }
    }
    Ok(())
}
