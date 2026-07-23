use super::*;

pub(super) async fn apply_registry_raw_logs(
    pool: &PgPool,
    raw_logs: &[loader::RegistryRawLogRow],
    chain: &str,
    current_registry: Option<&loader::ActiveEmitter>,
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<usize> {
    let mut matched_log_count = 0;
    for (index, raw_log) in raw_logs.iter().enumerate() {
        if apply_registry_raw_log(
            raw_log,
            chain,
            current_registry,
            latest_assignments,
            migrated_registry_nodes,
        )?
        .matched
        {
            matched_log_count += 1;
        }
        crate::startup_progress::record_processed_row_progress(
            pool,
            startup_progress,
            index + 1,
            raw_logs.len(),
        )
        .await?;
    }
    Ok(matched_log_count)
}
