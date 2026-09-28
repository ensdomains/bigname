use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

pub(crate) async fn require_published_head(
    pool: &PgPool,
    chain_id: &str,
    head_block: i64,
) -> Result<()> {
    let head: Option<(i64, String)> = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM bigname_phase.chain_heads WHERE chain_id=$1")
        .bind(chain_id).fetch_optional(pool).await?;
    let selected_head_is_published = match head {
        Some((number, hash)) if number == head_block => {
            bigname_storage::load_served_project_generation(
                pool, chain_id, number, &hash, true, true,
            )
            .await?
            .is_some()
        }
        _ => false,
    };
    ensure!(
        selected_head_is_published,
        "selected head {head_block} must already be a completed Project publication at chain_heads.latest_block_number under the current interpreter content hash before the published-head re-apply"
    );
    Ok(())
}

pub(super) fn require_minimum_walk_blocks(walk_blocks: i64, minimum: u64) -> Result<()> {
    ensure!(
        u64::try_from(walk_blocks).unwrap_or_default() >= minimum,
        "Interpret walk contains {walk_blocks} blocks; release minimum is {minimum}"
    );
    Ok(())
}

pub(crate) async fn projection_name_count(pool: &PgPool, chain_id: &str) -> Result<u64> {
    let mut count = 0;
    let mut after = String::new();
    loop {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE chain_id=$1 AND logical_name_id>$2 ORDER BY logical_name_id LIMIT 256")
            .bind(chain_id).bind(&after).fetch_all(pool).await
            .context("failed to read selected-chain name candidates")?;
        let Some(last) = ids.last() else {
            break;
        };
        after.clone_from(last);
        let rows =
            bigname_storage::families::name::load_family_names_by_logical_name_ids(pool, &ids)
                .await?;
        count += rows
            .values()
            .filter(|row| {
                row.coverage["status"] != "unsupported" && row.provenance["chain_id"] == chain_id
            })
            .count() as u64;
    }
    Ok(count)
}
