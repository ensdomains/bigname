use anyhow::{Context, Result, bail};
use sqlx::PgPool;
use tracing::warn;

pub(crate) async fn ensure_verified_lookup_ddl_available(pool: &PgPool) -> Result<()> {
    let phase_schema_exists = bigname_storage::phase_schema_exists(pool)
        .await
        .context("API verified-lookup DDL preflight could not inspect the phase schema")?;
    if !phase_schema_exists {
        warn!(
            service = "api",
            "phase schema is not available at API startup; verified-lookup DDL preflight is skipped"
        );
        return Ok(());
    }

    let missing_ddl = bigname_storage::load_missing_api_lookup_ddl(pool)
        .await
        .context("API verified-lookup DDL preflight could not inspect required lookup DDL")?;
    if !missing_ddl.is_empty() {
        let diagnostics = missing_ddl
            .iter()
            .map(|object| format!("{}: {}", object.kind.as_str(), object.identity))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "API verified-lookup DDL preflight failed: required lookup DDL is missing\n{diagnostics}"
        );
    }

    Ok(())
}
