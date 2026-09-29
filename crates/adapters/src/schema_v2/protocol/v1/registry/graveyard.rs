//! The admitted Graveyard as a current ENSv1 registry owner.
//!
//! The Graveyard claims an expired `.eth` name and clears a subname by making itself the node's
//! registry owner, so a record it holds is burned: the served reads give such a node no owner.
//! The owner word, the getter and the retained registry state stay as the chain wrote them, which
//! migration correlation reads.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20260916@366de741)

use alloy_primitives::Address;
use anyhow::Context;

use crate::schema_v2::{catalog::Catalog, catalog::Selected, model::RawLogInput};

const MIGRATION_FAMILY: &str = "ens_v2_migration_l1";

/// The `owner_getter_reason` of a registry owner write naming the admitted Graveyard.
const OWNER_REASON: &str = "graveyard";

/// The admitted Graveyard a current ENSv1 registry log can name as a node's owner: the
/// migration manifest's `graveyard` role on the same chain and namespace, from its declared start
/// block. Any other log, and a chain or namespace without a migration manifest, has none.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L20-L25 @ ens_v2_sepolia_20260916@366de741)
pub(in crate::schema_v2) fn admitted(
    catalog: &Catalog,
    selected: &Selected,
    raw: &RawLogInput,
) -> anyhow::Result<Option<Address>> {
    if selected.source.source_family != "ens_v1_registry_l1"
        || selected.emitter_role.as_deref() != Some("registry")
    {
        return Ok(None);
    }
    let Some(migration_source) = catalog.source_for_family(MIGRATION_FAMILY) else {
        return Ok(None);
    };
    if migration_source.namespace != selected.source.namespace
        || migration_source.chain_id != selected.source.chain_id
        || catalog
            .declared_start_block_for_role(MIGRATION_FAMILY, "graveyard")
            .is_none_or(|start| raw.block_number < start)
    {
        return Ok(None);
    }
    let address = catalog
        .declared_address_for_role(MIGRATION_FAMILY, "graveyard")
        .context("migration manifest has no graveyard declaration")?;
    Ok(Some(
        address
            .parse()
            .context("the declared Graveyard address is malformed")?,
    ))
}

/// The getter reason of an authentic registry owner: `graveyard` when it is the admitted
/// Graveyard.
pub(super) fn owner_reason(graveyard: Option<Address>, owner: &str) -> Option<String> {
    graveyard
        .is_some_and(|graveyard| owner.parse::<Address>().ok() == Some(graveyard))
        .then(|| OWNER_REASON.to_owned())
}
