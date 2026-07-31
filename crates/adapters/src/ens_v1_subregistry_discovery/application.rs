use super::*;

pub(super) fn apply_registry_raw_logs(
    raw_logs: &[loader::RegistryRawLogRow],
    chain: &str,
    current_registry: Option<&loader::ActiveEmitter>,
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<usize> {
    let mut matched_log_count = 0;
    for raw_log in raw_logs {
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
    }
    Ok(matched_log_count)
}
