use anyhow::{Context, Result, bail};
use sqlx::PgPool;

pub(crate) async fn ensure_verified_lookup_ddl_available(pool: &PgPool) -> Result<()> {
    let phase_schema_exists = bigname_storage::phase_schema_exists(pool)
        .await
        .context("API verified-lookup DDL preflight could not inspect the phase schema")?;
    if !phase_schema_exists {
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
            "API verified-lookup DDL preflight failed: required lookup objects are missing or serving relations are unreadable\n{diagnostics}"
        );
    }

    // With the switch off the API serves the served tables at the Project row. A chain the
    // switch ran Project on has them stopped short of it until a Project redo replays the gap.
    if !bigname_storage::publication_source::serve_from_families() {
        let behind = bigname_storage::load_served_tables_behind(pool)
            .await
            .context("API startup could not inspect where the served tables stopped")?;
        if !behind.is_empty() {
            let diagnostics = behind
                .iter()
                .map(|chain| {
                    let stopped = chain.stopped_at.map_or_else(
                        || "before any block".to_owned(),
                        |block| format!("at block {block}"),
                    );
                    format!(
                        "chain {}: served tables stopped {stopped}, Project is at block {}",
                        chain.chain_id, chain.project_block
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            bail!(
                "API startup refused: the publication switch ran Project past the served tables; \
                 with the switch off, replay them with a Project redo over the gap first \
                 (docs/deployment.md, Publication switch), or turn the switch back on\n{diagnostics}"
            );
        }
    }

    Ok(())
}
