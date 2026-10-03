//! When a current ENSv1 registry owner write ends the NameWrapper's authority over a node.
//!
//! A name is wrapped only while the NameWrapper is its registry owner, so a write naming anyone
//! else ends the wrapper authority: a parent's `setSubnodeOwner` needs no NameWrapper consent and
//! leaves the token unburned.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1079 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
//! Two writes keep it. The NameWrapper's own unwrap writes the new controller before the
//! `NameUnwrapped` that releases the authority, and `setRecord` and the wrapped branch of
//! `setSubnodeRecord` rewrite the record to the NameWrapper and then move the token.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L612-L629 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L637-L660 @ ens_v1@91c966f)

use alloy_primitives::Address;
use anyhow::Context;

use crate::schema_v2::{
    catalog::{Catalog, Selected},
    migration::RegistrarContext,
    state::V1NameState,
};

const WRAPPER_FAMILY: &str = "ens_v1_wrapper_l1";

/// What a registry owner log needs to decide whether it keeps the NameWrapper authority.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::schema_v2) struct WrapperCustody {
    /// The admitted NameWrapper, from `admitted`.
    pub(in crate::schema_v2) name_wrapper: Option<Address>,
    /// A `NameUnwrapped` for the log's node follows it in its transaction.
    pub(in crate::schema_v2) unwrap_follows: bool,
}

/// The admitted NameWrapper a current ENSv1 registry log can name as a node's owner: the
/// wrapper manifest's `name_wrapper` role on the same chain and namespace.
pub(in crate::schema_v2) fn admitted(
    catalog: &Catalog,
    selected: &Selected,
) -> anyhow::Result<Option<Address>> {
    if selected.source.source_family != "ens_v1_registry_l1"
        || selected.emitter_role.as_deref() != Some("registry")
    {
        return Ok(None);
    }
    let Some(wrapper_source) = catalog.source_for_family(WRAPPER_FAMILY) else {
        return Ok(None);
    };
    if wrapper_source.namespace != selected.source.namespace
        || wrapper_source.chain_id != selected.source.chain_id
    {
        return Ok(None);
    }
    catalog
        .declared_address_for_role(WRAPPER_FAMILY, "name_wrapper")
        .map(|address| {
            address
                .parse()
                .context("the declared NameWrapper address is malformed")
        })
        .transpose()
}

/// Whether an authentic registry owner write leaves the active NameWrapper authority in place.
pub(super) fn keeps_authority(
    previous: Option<&V1NameState>,
    owner: &str,
    context: RegistrarContext,
) -> bool {
    let custody = context.wrapper_custody;
    previous.is_some_and(|authority| authority.authority_source_family == WRAPPER_FAMILY)
        && (custody.unwrap_follows
            || custody
                .name_wrapper
                .is_some_and(|wrapper| owner.parse::<Address>().ok() == Some(wrapper)))
}

/// Whether a zero-equivalent registry owner write closes the active authority: a registry-only
/// one always, a NameWrapper one unless the NameWrapper's own `_unwrap(node, 0)` wrote it.
pub(super) fn zero_write_closes(previous: Option<&V1NameState>, context: RegistrarContext) -> bool {
    previous.is_some_and(|authority| {
        authority.token_lineage_id.is_none()
            || (authority.authority_source_family == WRAPPER_FAMILY
                && !context.wrapper_custody.unwrap_follows)
    })
}
