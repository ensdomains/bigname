use anyhow::{Context, Result, bail};
use sqlx::PgPool;

pub(crate) async fn ensure_api_storage_compatible(pool: &PgPool) -> Result<()> {
    let missing_ddl = bigname_storage::load_missing_api_lookup_ddl(pool)
        .await
        .context("API storage compatibility preflight could not inspect required lookup DDL")?;
    if !missing_ddl.is_empty() {
        let diagnostics = missing_ddl
            .iter()
            .map(|object| format!("{}: {}", object.kind.as_str(), object.identity))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "API storage compatibility preflight failed: required lookup DDL is missing\n{diagnostics}"
        );
    }

    let expected_hash = bigname_content_hash::INTERPRETER_CONTENT_HASH;
    let mismatches =
        bigname_storage::load_incompatible_published_project_generations(pool, expected_hash)
            .await
            .context("API storage compatibility preflight could not inspect Project generations")?;
    if !mismatches.is_empty() {
        let diagnostics = mismatches
            .iter()
            .map(|mismatch| {
                format!(
                    "chain_id={} phase_status={} current_block_number={} \
                     stored input_content_hash={} expected input_content_hash={}",
                    mismatch.chain_id,
                    mismatch.phase_status,
                    mismatch.current_block_number,
                    mismatch.input_content_hash.as_deref().unwrap_or("<null>"),
                    expected_hash,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "API storage compatibility preflight failed: published Project generations are incompatible\n{diagnostics}"
        );
    }

    Ok(())
}
