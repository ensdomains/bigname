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
    common::hash_hex,
    migration::RegistrarContext,
    model::RawLogInput,
    protocol::{
        Interpreted,
        v1::{
            WrapperPermissionContext, append_delegate_permission,
            authority_transition::authority_kind,
        },
    },
    state::{State, V1NameState, v1_registration_is_live},
};

pub(super) const WRAPPER_FAMILY: &str = "ens_v1_wrapper_l1";

/// What a registry owner log needs to decide whether it keeps the NameWrapper authority.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::schema_v2) struct WrapperCustody {
    /// The admitted NameWrapper, from `admitted`.
    pub(in crate::schema_v2) name_wrapper: Option<Address>,
    /// A `NameUnwrapped` for the log's node follows it in its transaction.
    pub(in crate::schema_v2) unwrap_follows: bool,
    /// The log is a BaseRegistrar transfer to the admitted NameWrapper, and the next registry
    /// owner write for its node in the transaction is a `NewOwner` naming the NameWrapper.
    pub(in crate::schema_v2) reclaim_follows: bool,
}

/// The admitted NameWrapper on the same chain and namespace as a current ENSv1 registry log,
/// which can name it as a node's owner, or as a BaseRegistrar log, whose token it can receive:
/// the wrapper manifest's `name_wrapper` role.
pub(in crate::schema_v2) fn admitted(
    catalog: &Catalog,
    selected: &Selected,
) -> anyhow::Result<Option<Address>> {
    if (selected.source.source_family != "ens_v1_registry_l1"
        || selected.emitter_role.as_deref() != Some("registry"))
        && (selected.source.source_family != "ens_v1_registrar_l1"
            || selected.emitter_role.as_deref() != Some("registrar"))
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
    context: RegistrarContext<'_>,
) -> bool {
    let custody = context.wrapper_custody;
    previous.is_some_and(|authority| authority.authority_source_family == WRAPPER_FAMILY)
        && (custody.unwrap_follows
            || custody
                .name_wrapper
                .is_some_and(|wrapper| owner.parse::<Address>().ok() == Some(wrapper)))
}

/// A registry write that ends the NameWrapper authority `previous`, because the authority it
/// links is none or on another resource, also ends the per-token approved address's NameWrapper
/// permission, at the same log as the holder's. On chain the approval lasts until the old token
/// moves or burns, but those logs no longer reach this authority. The revoke names the admitted
/// NameWrapper, so it carries the grant's state scope. Restore keeps it beside the grant under its
/// own family's key and applies the two in chain order.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L228-L238 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L275 @ ens_v1@91c966f)
pub(super) fn revoke_delegate(
    output: &mut Interpreted,
    state: &mut State,
    (namespace, node): (&str, &str),
    previous: Option<&V1NameState>,
    linked: Option<&V1NameState>,
    context: RegistrarContext<'_>,
    raw: &RawLogInput,
) {
    let Some(ended) = previous.filter(|authority| {
        authority.authority_source_family == WRAPPER_FAMILY
            && linked.map(|current| current.resource_id) != Some(authority.resource_id)
    }) else {
        return;
    };
    let Some(name_wrapper) = context.wrapper_custody.name_wrapper else {
        return;
    };
    let Some(delegate) = state.set_v1_wrapper_delegate(namespace, node, None) else {
        return;
    };
    let permission = WrapperPermissionContext {
        name: ended,
        resolver: None,
        chain_id: &raw.chain_id,
        wrapper: format!("{name_wrapper:#x}"),
        source_event_kind: "AuthorityTransferred",
        identity_suffix: "AuthorityTransferred",
    };
    append_delegate_permission(output, &permission, &delegate, false);
}

/// Whether a zero-equivalent registry owner write closes the active authority: a registry-only
/// one always, a NameWrapper one unless the NameWrapper's own `_unwrap(node, 0)` wrote it.
pub(super) fn zero_write_closes(
    previous: Option<&V1NameState>,
    context: RegistrarContext<'_>,
) -> bool {
    previous.is_some_and(|authority| {
        authority.token_lineage_id.is_none()
            || (authority.authority_source_family == WRAPPER_FAMILY
                && !context.wrapper_custody.unwrap_follows)
    })
}

/// Whether a live registrar lease takes a registry owner write over, so the lease holds the
/// owner's grants: the write selected the lease, or it is the NameWrapper's own unwrap of a name
/// that a lease succeeds.
pub(super) fn lease_takes_write(
    state: &State,
    (namespace, node): (&str, &str),
    linked: Option<&V1NameState>,
    context: RegistrarContext<'_>,
    raw: &RawLogInput,
) -> bool {
    linked.is_some_and(|authority| authority_kind(authority) == "registrar")
        || (context.wrapper_custody.unwrap_follows
            && lease_succeeds_unwrap(state, namespace, node, raw.block_timestamp.unix_timestamp()))
}

/// Whether a registrar lease succeeds an unwrap of `node` at `at`: `NameUnwrapped` reactivates a
/// live lease, and the registrar transfer that follows `unwrapETH2LD` takes the name over.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390-L395 @ ens_v1@91c966f)
pub(in crate::schema_v2::protocol::v1) fn lease_succeeds_unwrap(
    state: &State,
    namespace: &str,
    node: &str,
    at: i64,
) -> bool {
    state
        .v1_registrar(namespace, node)
        .is_some_and(|lease| v1_registration_is_live(lease.expiry, at))
        || registrar_transfer_may_follow(state, namespace, node)
}

/// Whether the BaseRegistrar transfer that follows an unwrap of `node` in its transaction takes
/// the name over: a wrapped `.eth` second-level name with a wrapper-derived registrar expiry
/// and no materialized registrar identity yet. `unwrap` refuses a `.eth` second-level name, and
/// `unwrapETH2LD` runs `_unwrap`, which writes the registry and emits `NameUnwrapped`, and only
/// then transfers the registrar token to the registrant. That transfer's fallback registrar
/// identity needs only the derived expiry, so this check needs no more.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L381-L395 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
pub(in crate::schema_v2::protocol::v1) fn registrar_transfer_may_follow(
    state: &State,
    namespace: &str,
    node: &str,
) -> bool {
    state.v1_registrar(namespace, node).is_none()
        && state
            .v1_node_path(namespace, node)
            .is_some_and(|path| path.len() == 2 && path[1] == hash_hex(b"eth"))
        && state
            .v1_registrar_expiry_from_wrapper(namespace, node)
            .is_some()
}
