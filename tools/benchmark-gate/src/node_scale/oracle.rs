use std::{fs::File, path::Path};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sqlx::PgPool;

use super::manifests::CHAIN;

pub(super) struct Expected {
    names: i64,
    shadows: i64,
    structural: bool,
}

#[derive(Serialize)]
pub(super) struct IdentityCounts {
    pub(super) surfaces: i64,
    pub(super) active: i64,
    pub(super) shadow: i64,
    pub(super) without_raw_bytes: i64,
    pub(super) complete_paths: i64,
    pub(super) byte_shadow_paths: i64,
    pub(super) expected_surfaces: i64,
    pub(super) expected_shadow: i64,
}

pub(super) fn expected(directory: &Path, head: i64) -> Result<Expected> {
    let corpus: serde_json::Value =
        serde_json::from_reader(File::open(directory.join("corpus.json"))?)?;
    ensure!(
        corpus["source_head"].as_str() == Some(crate::git_head().as_str()),
        "oracle corpus has a different source"
    );
    ensure!(
        corpus["interpreter_content_hash"].as_str()
            == Some(bigname_content_hash::INTERPRETER_CONTENT_HASH),
        "oracle corpus has a different interpreter hash"
    );
    ensure!(
        ["structural_head", "changed_head", "bytes_head"]
            .iter()
            .any(|key| corpus[*key].as_i64() == Some(head)),
        "oracle requires a declared complete epoch"
    );
    let names = corpus["topology"]["names_below_eth"]
        .as_i64()
        .context("missing expected population")?;
    ensure!(
        matches!(names, 10_000 | 100_000 | 1_000_000),
        "invalid expected population"
    );
    let structural = corpus["structural_head"].as_i64() == Some(head);
    let has_bytes = corpus["bytes_head"].as_i64() == Some(head);
    Ok(Expected {
        names: names + 1,
        shadows: if has_bytes { names / 100 } else { 0 },
        structural,
    })
}

pub(super) async fn identities(pool: &PgPool, expected: &Expected) -> Result<IdentityCounts> {
    let (surfaces,active,shadow,without_raw_bytes,complete_paths,byte_shadow_paths): (i64,i64,i64,i64,i64,i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER(WHERE visibility_state='active'), count(*) FILTER(WHERE visibility_state='shadow'),
         count(*) FILTER(WHERE raw_name IS NULL), count(*) FILTER(WHERE cardinality(labelhashes)>0),
         count(*) FILTER(WHERE visibility_state='shadow' AND raw_name='' AND cardinality(raw_labels)=0 AND cardinality(labelhashes)>0 AND preimage_event_identity IS NOT NULL)
         FROM name_surfaces WHERE chain_id=$1")
        .bind(CHAIN).fetch_one(pool).await?;
    let result = IdentityCounts {
        surfaces,
        active,
        shadow,
        without_raw_bytes,
        complete_paths,
        byte_shadow_paths,
        expected_surfaces: expected.names,
        expected_shadow: expected.shadows,
    };
    ensure!(
        surfaces == expected.names
            && active == expected.names - expected.shadows
            && shadow == expected.shadows,
        "identity population mismatch: {}",
        serde_json::to_string(&result)?
    );
    ensure!(
        complete_paths == surfaces && byte_shadow_paths == shadow,
        "incomplete identity/shadow paths: {}",
        serde_json::to_string(&result)?
    );
    if expected.structural {
        ensure!(
            without_raw_bytes == surfaces,
            "structural epoch contains invented byte evidence"
        );
    }
    Ok(result)
}
