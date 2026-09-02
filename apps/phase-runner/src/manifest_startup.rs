use anyhow::Result;

pub async fn sync_loaded_manifests(
    pool: &sqlx::PgPool,
    root: &std::path::Path,
    repository: &bigname_manifests::ManifestRepository,
    profile: &str,
) -> Result<()> {
    let summary = bigname_manifests::sync_schema_v2_repository(pool, repository).await?;
    for notice in &summary.notices {
        tracing::warn!(notice = %notice, "schema-v2 manifest synchronization notice");
    }
    tracing::info!(
        manifests_root = %root.display(),
        manifest_profile = profile,
        manifest_count = summary.manifest_count,
        declaration_count = summary.declaration_count,
        discovery_rule_count = summary.discovery_rule_count,
        proxy_edge_count = summary.proxy_edge_count,
        "schema-v2 manifests synchronized"
    );
    Ok(())
}
