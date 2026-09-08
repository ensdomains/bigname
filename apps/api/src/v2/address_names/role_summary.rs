use super::*;

const MAX_INLINE_GRANT_ROWS: u64 = 1_000;

pub(super) async fn load_rows(
    pool: &sqlx::PgPool,
    ids: &[sqlx::types::Uuid],
    namespace: Option<&str>,
    entries: &[AddressNameCurrentEntry],
) -> V2Result<Vec<EffectivePermissionRow>> {
    let rows = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
        pool,
        ids,
        namespace,
        MAX_INLINE_GRANT_ROWS,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load address-name role summaries"))?;
    let mut multiplicity = BTreeMap::<_, usize>::new();
    for entry in entries {
        *multiplicity.entry(entry.resource_id).or_default() += 1;
    }
    // Surface dedupe can repeat one resource's grants on several returned names. Count the
    // serialized expansion, not only distinct storage rows or permission subjects.
    let expanded_rows: usize = rows.iter().map(|row| multiplicity[&row.resource_id]).sum();
    if expanded_rows > MAX_INLINE_GRANT_ROWS as usize {
        return Err(V2Error::unsupported(
            "inline role_summary exceeds 1000 total grant rows; omit include and paginate /v2/permissions using the returned permission handle as registration_id; preserve only an explicitly requested namespace and do not add name or address filters",
        ));
    }
    Ok(rows)
}
