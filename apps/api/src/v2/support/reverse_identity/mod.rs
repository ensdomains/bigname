use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::{
    IdentityPrimaryNameSnapshot, ReverseIdentityGroup, ReverseIdentityStorageInput,
};
use sqlx::PgPool;

#[cfg(test)]
mod hooks;
#[cfg(test)]
pub(crate) use hooks::{relation_page_test_hooks, test_hooks};

pub(crate) async fn load_reverse_identity_records_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
    selected: Option<&bigname_storage::SelectedSnapshot>,
    include_inventory: bool,
) -> Result<Vec<ReverseIdentityGroup>> {
    load_reverse_identity_records_live_with_count_mode(
        pool,
        inputs,
        public_namespaces,
        selected,
        ReverseCountMode::Include,
        include_inventory,
    )
    .await
}

pub(crate) async fn load_reverse_identity_records_page_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
    selected: Option<&bigname_storage::SelectedSnapshot>,
    include_inventory: bool,
) -> Result<Vec<ReverseIdentityGroup>> {
    #[cfg(test)]
    relation_page_test_hooks::record_page_load(pool).await?;
    load_reverse_identity_records_live_with_count_mode(
        pool,
        inputs,
        public_namespaces,
        selected,
        ReverseCountMode::Omit,
        include_inventory,
    )
    .await
}

/// The readable primary-name claims for one address and coin type, keyed by public namespace.
/// Shares the single primary-name read of the reverse page loader so `relation=resolves_to`
/// computes `is_primary` from the same claim and snapshot rules as the authority relations.
pub(crate) async fn load_reverse_identity_primary_snapshots(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    public_namespaces: &[String],
    selected: Option<&bigname_storage::SelectedSnapshot>,
) -> Result<BTreeMap<String, IdentityPrimaryNameSnapshot>> {
    let chains = selected.map(|selected| {
        selected
            .chain_positions
            .as_map()
            .values()
            .map(|position| position.chain_id.clone())
            .collect::<Vec<_>>()
    });
    return bigname_storage::families::records::load_family_reverse_primary_snapshots(
        pool,
        address,
        coin_type,
        public_namespaces,
        chains.as_deref(),
    )
    .await;
}

pub(crate) async fn prepare_reverse_identity_additional_scan(
    pool: &PgPool,
    served_head: Option<&crate::v2::lookup::head::ServedHead>,
) -> crate::v2::V2Result<()> {
    #[cfg(test)]
    relation_page_test_hooks::pause_before_additional_scan(pool)
        .await
        .map_err(|_| crate::v2::V2Error::internal_error("failed to run reverse-page test hook"))?;
    if let Some(served_head) = served_head {
        crate::v2::lookup::head::revalidate_served_head(pool, served_head).await?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ReverseCountMode {
    Include,
    Omit,
}

async fn load_reverse_identity_records_live_with_count_mode(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
    selected: Option<&bigname_storage::SelectedSnapshot>,
    count_mode: ReverseCountMode,
    include_inventory: bool,
) -> Result<Vec<ReverseIdentityGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    let chains = selected.map(|selected| {
        selected
            .chain_positions
            .as_map()
            .values()
            .map(|position| position.chain_id.clone())
            .collect::<Vec<_>>()
    });
    #[cfg(test)]
    if matches!(count_mode, ReverseCountMode::Include) {
        test_hooks::record(pool).await?;
    }
    return bigname_storage::families::records::load_family_reverse_identity_groups(
        pool,
        inputs,
        public_namespaces,
        chains.as_deref(),
        matches!(count_mode, ReverseCountMode::Include),
        include_inventory,
    )
    .await;
}
