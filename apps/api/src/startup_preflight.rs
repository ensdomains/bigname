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

    Ok(())
}
