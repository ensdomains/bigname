//! The grants on a node's registry-only resource follow its ENSv1 registry owner.
//!
//! The registry lets a node's owner, or an operator it approved, write the node's record,
//! whichever authority is selected for the name.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
//! So the resource holds `resource_control`, and `resolver_control` at the node's resolver, for
//! one account: the owner the newest registry owner write named, its holder. Every permission row
//! on the resource goes through this module, which keeps the holder in interpreter state and
//! revokes it before it grants the next one. The holder is no one after a zero owner, after a
//! write a live registrar lease takes over, where the lease holds the owner's grants, and after a
//! write naming the admitted Graveyard, a burned record. A write that a registration later in
//! its transaction takes over keeps the holder when it names the current holder, because it makes
//! no row for the lease to take. So a registrant that registers its lapsed name again directly
//! keeps the resource beside its lease. A write a live lease takes over always ends the holder.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L20-L23 @ ens_v2_sepolia_20261001@07e55a05)
//! Each row's retained state is scoped by its subject, scope kind and resolver, so a restore
//! keeps the newest row of each and rebuilds the same holder.

use serde_json::json;
use uuid::Uuid;

use super::super::authority_transition::RegistryOwnerView;
use super::{append_authority_permissions, graveyard, wrapper_custody};
use crate::schema_v2::{
    common::stable_uuid,
    migration::RegistrarContext,
    model::RawLogInput,
    protocol::{
        EventDraft, Interpreted,
        permissions::{v1_grant_states, v1_revoke_states},
    },
    state::{State, V1NameState},
};

mod handoff;
pub(in crate::schema_v2::protocol::v1) use handoff::{end_resolver, hand_off, move_resolver};

/// A node's registry-only resource.
pub(in crate::schema_v2::protocol::v1) fn resource_id(chain_id: &str, node: &str) -> Uuid {
    stable_uuid(&format!("resource:registry-only:{chain_id}:{node}"))
}

/// The node's registry-only authority to anchor its rows on: the remembered registry authority,
/// else one built from the registry read anchor.
pub(in crate::schema_v2::protocol::v1) fn authority(
    state: &State,
    namespace: &str,
    node: &str,
    chain_id: &str,
) -> Option<V1NameState> {
    state
        .v1_current_registry_authority(namespace, node)
        .or_else(|| {
            let anchor = state.v1_registry_read_anchor(namespace, node)?;
            Some(V1NameState {
                logical_name_id: anchor.logical_name_id,
                surface_known: anchor.surface_known,
                resource_id: anchor.resource_id,
                token_lineage_id: None,
                authority_source_family: anchor.source_family,
                source_manifest_id: anchor.source_manifest_id,
                labelhash: None,
                expiry: None,
                owner: None,
                registry_contract: anchor.registry_contract,
                authority_key: Some(format!("registry-only:{chain_id}:{node}")),
                wrapper_fallback: false,
            })
        })
}

/// The holder a registry-only authority names when it becomes the selected authority: its
/// owner, unless that is the admitted Graveyard.
pub(in crate::schema_v2::protocol::v1) fn selected_holder<'a>(
    state: &State,
    (namespace, node): (&str, &str),
    authority: &'a V1NameState,
) -> Option<&'a str> {
    authority.owner.as_deref().filter(|_| {
        state.v1_registry_owner_reason(namespace, node).as_deref() != Some(graveyard::OWNER_REASON)
    })
}

/// A current or old ENSv1 registry owner write, as `apply_owner_write` reads it.
pub(in crate::schema_v2::protocol::v1) struct OwnerWrite<'a> {
    /// The namespace and node written.
    pub node: (&'a str, &'a str),
    pub view: &'a RegistryOwnerView,
    pub previous: Option<&'a V1NameState>,
    pub linked: Option<&'a V1NameState>,
    /// The registry-only authority of the owner this write names, when it names one.
    pub written: Option<&'a V1NameState>,
    /// The registry-only authority as it was before this write.
    pub before: Option<&'a V1NameState>,
}

/// The one place a registry owner write moves permissions: the old authority's grants end and
/// the new one's start where the write changes the selected authority, and the registry-only
/// resource's holder becomes the owner the write names, whichever authority is selected. A
/// registrar lease that takes the write over holds the owner's grants, and the admitted Graveyard
/// is the ENSv1→ENSv2 migration's burn address for ENSv1 names, so neither leaves a holder.
/// Each subject's revokes come before the grants.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L20-L23 @ ens_v2_sepolia_20261001@07e55a05)
pub(in crate::schema_v2::protocol::v1) fn apply_owner_write(
    output: &mut Interpreted,
    state: &mut State,
    write: OwnerWrite<'_>,
    context: RegistrarContext<'_>,
    raw: &RawLogInput,
) {
    let (namespace, node) = write.node;
    let registry_only = resource_id(&raw.chain_id, node);
    let holder = match write.view {
        RegistryOwnerView::Authentic { owner } => Some(owner.as_str()),
        RegistryOwnerView::ZeroEquivalent { .. } | RegistryOwnerView::UnavailableUnmasked => None,
    }
    .filter(|owner| graveyard::owner_reason(context.registry_graveyard, owner).is_none())
    .filter(|_| !wrapper_custody::lease_takes_write(state, write.node, write.linked, context, raw));
    let resolver = state.v1_resolver(namespace, node);
    let handoff = hand_off(
        state,
        registry_only,
        holder,
        resolver.as_deref(),
        context
            .registration_follows
            .then_some(raw.transaction_hash.as_str()),
    );
    let changed = write
        .previous
        .map(|authority| (&authority.owner, authority.resource_id))
        != write
            .linked
            .map(|authority| (&authority.owner, authority.resource_id));
    let elsewhere = |authority: &&V1NameState| authority.resource_id != registry_only;
    let rows = registry_rows(raw, "AuthorityTransferred");
    if changed {
        wrapper_custody::revoke_delegate(
            output,
            state,
            write.node,
            write.previous,
            write.linked,
            context,
            raw,
        );
        append_authority_permissions(
            output,
            write.previous.filter(elsewhere),
            None,
            resolver.clone(),
            raw,
        );
    }
    if let Some(authority) = write
        .previous
        .filter(|authority| authority.resource_id == registry_only)
        .or(write.before)
    {
        handoff.push_revokes(&mut output.events, authority, rows);
    }
    if changed {
        append_authority_permissions(output, None, write.linked.filter(elsewhere), resolver, raw);
    }
    if let Some(authority) = write.written {
        handoff.push_grants(&mut output.events, authority, rows);
    }
}

/// The rows of a registry log: the registry adapter's own identity suffixes.
pub(in crate::schema_v2::protocol::v1) fn registry_rows<'a>(
    raw: &'a RawLogInput,
    source_event_kind: &'a str,
) -> Rows<'a> {
    Rows {
        emitter: &raw.emitting_address,
        chain_id: &raw.chain_id,
        source_event_kind,
        resource: ["resource-revoke", "resource-grant"],
        resolver: ["resolver-revoke", "resolver-grant"],
        boundary: false,
    }
}

/// How one log names its permission rows on the registry-only resource.
#[derive(Clone, Copy)]
pub(in crate::schema_v2::protocol::v1) struct Rows<'a> {
    /// The emitting contract, or `boundary` for a block-boundary row.
    pub emitter: &'a str,
    pub chain_id: &'a str,
    pub source_event_kind: &'a str,
    /// The revoke and grant identity suffixes of `resource_control`.
    pub resource: [&'a str; 2],
    /// The revoke and grant identity suffixes of `resolver_control`.
    pub resolver: [&'a str; 2],
    /// Block-boundary rows also name the authority key in their identity.
    pub boundary: bool,
}

/// One registry-only permission row, with a retained state scope of its own subject, scope kind
/// and resolver.
#[allow(clippy::too_many_arguments)]
pub(in crate::schema_v2::protocol::v1) fn row(
    authority: &V1NameState,
    subject: &str,
    resolver: Option<&str>,
    grant: bool,
    emitter: &str,
    chain_id: &str,
    source_event_kind: &str,
    identity_suffix: String,
) -> Option<EventDraft> {
    let authority_key = authority.authority_key.as_deref()?;
    let (scope, power, scope_key) = match resolver {
        Some(resolver) => (
            json!({"kind":"resolver","chain_id":chain_id,"resolver_address":resolver}),
            "resolver_control",
            format!("resolver:{}", resolver.to_ascii_lowercase()),
        ),
        None => (
            json!({"kind":"resource"}),
            "resource_control",
            "resource".to_owned(),
        ),
    };
    let states = if grant {
        v1_grant_states
    } else {
        v1_revoke_states
    };
    let (before, after) = states(
        subject,
        scope,
        power,
        "registry_only",
        authority_key,
        source_event_kind,
    );
    let node = authority
        .logical_name_id
        .rsplit(':')
        .next()
        .unwrap_or_default();
    Some(EventDraft {
        event_kind: "PermissionChanged".to_owned(),
        logical_name_id: authority
            .surface_known
            .then(|| authority.logical_name_id.clone()),
        resource_id: Some(authority.resource_id),
        identity_suffix,
        explicit_before: Some(before),
        after_state: after,
        state_scope: format!(
            "{}:{node}:-:{}:permission:{scope_key}",
            emitter.to_ascii_lowercase(),
            subject.to_ascii_lowercase()
        ),
    })
}

/// A lapsed lease hands the node's registry-only resource back to its registry owner at the
/// block boundary, which makes that owner the holder unless it already is.
pub(in crate::schema_v2) fn hand_back(
    events: &mut Vec<EventDraft>,
    state: &mut State,
    node: (&str, &str),
    authority: &V1NameState,
    resolver: Option<&str>,
    chain_id: &str,
) {
    let target = selected_holder(state, node, authority).map(str::to_owned);
    let handoff = hand_off(
        state,
        authority.resource_id,
        target.as_deref(),
        resolver,
        None,
    );
    let rows = Rows {
        emitter: "boundary",
        chain_id,
        source_event_kind: "RegistrationReleased",
        resource: ["registry-revoke", "registry-grant"],
        resolver: ["registry-resolver-revoke", "registry-resolver-grant"],
        boundary: true,
    };
    handoff.push_revokes(events, authority, rows);
    handoff.push_grants(events, authority, rows);
}
