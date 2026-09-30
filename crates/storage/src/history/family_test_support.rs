//! Complete the historical-query fixtures with a real current family publication.
use anyhow::{Result, ensure};
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions, RebuildRanges},
};
use sqlx::PgPool;

pub(super) async fn publish(pool: &PgPool, chain: &str, head: i64) -> Result<()> {
    for baseline in [
        include_str!("../../schema/baseline/07_labels.sql"),
        include_str!("../../schema/baseline/08_heartbeats.sql"),
        include_str!("../../schema/baseline/09_divergence.sql"),
        include_str!("../../schema/baseline/10_phase_state.sql"),
    ] {
        sqlx::raw_sql(baseline).execute(pool).await?;
    }
    // The tests intentionally use synthetic identity hashes. Their visible name metadata still
    // follows the same normalization contract as Interpret's active surfaces.
    let surfaces: Vec<(String, String)> =
        sqlx::query_as("SELECT logical_name_id,raw_name FROM name_surfaces WHERE chain_id=$1")
            .bind(chain)
            .fetch_all(pool)
            .await?;
    for (id, raw) in surfaces {
        let normalized = bigname_domain::normalization::normalize_name(&raw)?;
        let labels: Vec<String> = normalized
            .normalized_labels
            .iter()
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
            .collect();
        sqlx::query("UPDATE name_surfaces SET raw_name=$2,raw_labels=$3,dns_encoded_name=$4,labelhashes=$5,normalizer_version=$6 WHERE logical_name_id=$1")
            .bind(id).bind(normalized.normalized_name).bind(normalized.normalized_labels)
            .bind(normalized.dns_encoded_name).bind(labels)
            .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION).execute(pool).await?;
    }
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state)
        VALUES ($1,'history-genesis',0,to_timestamp(0),'canonical') ON CONFLICT DO NOTHING")
        .bind(chain).execute(pool).await?;
    let hash: String = sqlx::query_scalar("SELECT block_hash FROM chain_lineage WHERE chain_id=$1 AND block_number=$2 AND canonicality_state='canonical'")
        .bind(chain).bind(head).fetch_one(pool).await?;
    let target = Marker { number: head, hash };
    let options = FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .with_rebuild_ranges(RebuildRanges::Through(head))
        .with_max_blocks_per_run(u64::try_from(head + 1)?);
    let token = families::input_token(pool, chain).await?;
    let outcome =
        families::apply(pool, chain, &target, FamilyMode::Rebuild, &token, &options).await?;
    ensure!(
        outcome.marker.as_ref() == Some(&target),
        "fixture publication must reach its head: {outcome:?}"
    );
    Ok(())
}
