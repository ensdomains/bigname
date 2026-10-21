use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::{
    IdentityPrimaryNameSnapshot, ReadDb, ReverseIdentityGroup, ReverseIdentityStorageInput,
};

#[cfg(test)]
mod hooks;
#[cfg(test)]
pub(crate) use hooks::test_hooks;

pub(crate) async fn load_reverse_identity_records_live(
    db: impl Into<ReadDb<'_>>,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
    selected: Option<&bigname_storage::SelectedSnapshot>,
    include_inventory: bool,
) -> Result<Vec<ReverseIdentityGroup>> {
    load_reverse_identity_records_live_with_count_mode(
        db.into(),
        inputs,
        public_namespaces,
        selected,
        ReverseCountMode::Include,
        include_inventory,
    )
    .await
}

pub(crate) async fn load_reverse_identity_records_page_live(
    db: impl Into<ReadDb<'_>>,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
    selected: Option<&bigname_storage::SelectedSnapshot>,
    include_inventory: bool,
) -> Result<Vec<ReverseIdentityGroup>> {
    load_reverse_identity_records_live_with_count_mode(
        db.into(),
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
    db: impl Into<ReadDb<'_>>,
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
        db,
        address,
        coin_type,
        public_namespaces,
        chains.as_deref(),
    )
    .await;
}

#[derive(Clone, Copy)]
enum ReverseCountMode {
    Include,
    Omit,
}

async fn load_reverse_identity_records_live_with_count_mode(
    #[allow(unused_mut)] mut db: ReadDb<'_>,
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
        test_hooks::record(&mut db).await?;
    }
    return bigname_storage::families::records::load_family_reverse_identity_groups(
        db,
        inputs,
        public_namespaces,
        chains.as_deref(),
        matches!(count_mode, ReverseCountMode::Include),
        include_inventory,
    )
    .await;
}
