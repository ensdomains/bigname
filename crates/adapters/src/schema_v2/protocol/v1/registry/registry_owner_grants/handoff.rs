//! Moving a registry-only resource's holder in interpreter state, with the rows that record it.

use uuid::Uuid;

use super::{Rows, row};
use crate::schema_v2::{
    protocol::EventDraft,
    state::{RegistryOnlyHolder, State, V1NameState},
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// A change of holder, already made in state, whose rows the caller places.
pub(in crate::schema_v2::protocol::v1) struct Handoff {
    revoked: Option<RegistryOnlyHolder>,
    granted: Option<RegistryOnlyHolder>,
}

/// Makes `target` the holder of `resource_id`, with `resolver_control` at `resolver`. The old
/// holder is revoked unless it is `target`, and `target` is granted unless it already holds the
/// resource. `registration_transaction` marks a holder that a registration later in that
/// transaction takes over.
pub(in crate::schema_v2::protocol::v1) fn hand_off(
    state: &mut State,
    resource_id: Uuid,
    target: Option<&str>,
    resolver: Option<&str>,
    registration_transaction: Option<&str>,
) -> Handoff {
    let holder = state.v1_registry_only_holder(resource_id);
    if let (Some(holder), Some(target)) = (holder.as_ref(), target)
        && holder.subject.eq_ignore_ascii_case(target)
    {
        return Handoff {
            revoked: None,
            granted: None,
        };
    }
    let granted = target.map(|target| RegistryOnlyHolder {
        subject: target.to_owned(),
        resolver: resolver
            .filter(|resolver| !resolver.eq_ignore_ascii_case(ZERO_ADDRESS))
            .map(str::to_owned),
        registration_transaction: registration_transaction.map(str::to_owned),
    });
    state.set_v1_registry_only_holder(resource_id, granted.clone());
    Handoff {
        revoked: holder,
        granted,
    }
}

impl Handoff {
    pub(in crate::schema_v2::protocol::v1) fn push_revoke_resource(
        &self,
        events: &mut Vec<EventDraft>,
        authority: &V1NameState,
        rows: Rows<'_>,
    ) {
        if let Some(holder) = self.revoked.as_ref() {
            push(events, authority, &holder.subject, None, false, rows);
        }
    }

    pub(in crate::schema_v2::protocol::v1) fn push_revoke_resolver(
        &self,
        events: &mut Vec<EventDraft>,
        authority: &V1NameState,
        rows: Rows<'_>,
    ) {
        if let Some(holder) = self.revoked.as_ref()
            && let Some(resolver) = holder.resolver.as_deref()
        {
            push(
                events,
                authority,
                &holder.subject,
                Some(resolver),
                false,
                rows,
            );
        }
    }

    pub(in crate::schema_v2::protocol::v1) fn push_grant_resource(
        &self,
        events: &mut Vec<EventDraft>,
        authority: &V1NameState,
        rows: Rows<'_>,
    ) {
        if let Some(holder) = self.granted.as_ref() {
            push(events, authority, &holder.subject, None, true, rows);
        }
    }

    pub(in crate::schema_v2::protocol::v1) fn push_grant_resolver(
        &self,
        events: &mut Vec<EventDraft>,
        authority: &V1NameState,
        rows: Rows<'_>,
    ) {
        if let Some(holder) = self.granted.as_ref()
            && let Some(resolver) = holder.resolver.as_deref()
        {
            push(
                events,
                authority,
                &holder.subject,
                Some(resolver),
                true,
                rows,
            );
        }
    }

    pub(in crate::schema_v2::protocol::v1) fn push_revokes(
        &self,
        events: &mut Vec<EventDraft>,
        authority: &V1NameState,
        rows: Rows<'_>,
    ) {
        self.push_revoke_resource(events, authority, rows);
        self.push_revoke_resolver(events, authority, rows);
    }

    pub(in crate::schema_v2::protocol::v1) fn push_grants(
        &self,
        events: &mut Vec<EventDraft>,
        authority: &V1NameState,
        rows: Rows<'_>,
    ) {
        self.push_grant_resource(events, authority, rows);
        self.push_grant_resolver(events, authority, rows);
    }
}

/// Moves the holder's `resolver_control` to `resolver`, the node's new nonzero resolver or none.
/// Nothing moves when the resource has no holder.
pub(in crate::schema_v2::protocol::v1) fn move_resolver(
    events: &mut Vec<EventDraft>,
    state: &mut State,
    authority: &V1NameState,
    resolver: Option<&str>,
    rows: Rows<'_>,
) {
    let Some(mut holder) = state.v1_registry_only_holder(authority.resource_id) else {
        return;
    };
    let resolver = resolver.filter(|resolver| !resolver.eq_ignore_ascii_case(ZERO_ADDRESS));
    if holder.resolver.as_deref().map(str::to_ascii_lowercase)
        == resolver.map(str::to_ascii_lowercase)
    {
        return;
    }
    if let Some(previous) = holder.resolver.as_deref() {
        push(
            events,
            authority,
            &holder.subject,
            Some(previous),
            false,
            rows,
        );
    }
    if let Some(resolver) = resolver {
        push(
            events,
            authority,
            &holder.subject,
            Some(resolver),
            true,
            rows,
        );
    }
    holder.resolver = resolver.map(str::to_owned);
    state.set_v1_registry_only_holder(authority.resource_id, Some(holder));
}

/// Ends the holder's `resolver_control` at `resolver` when `subject` holds it there, and says
/// whether it did.
pub(in crate::schema_v2::protocol::v1) fn end_resolver(
    state: &mut State,
    resource_id: Uuid,
    subject: &str,
    resolver: &str,
) -> bool {
    let Some(mut holder) = state
        .v1_registry_only_holder(resource_id)
        .filter(|holder| holder.subject.eq_ignore_ascii_case(subject))
        .filter(|holder| {
            holder
                .resolver
                .as_deref()
                .is_some_and(|current| current.eq_ignore_ascii_case(resolver))
        })
    else {
        return false;
    };
    holder.resolver = None;
    state.set_v1_registry_only_holder(resource_id, Some(holder));
    true
}

fn push(
    events: &mut Vec<EventDraft>,
    authority: &V1NameState,
    subject: &str,
    resolver: Option<&str>,
    grant: bool,
    rows: Rows<'_>,
) {
    let suffixes = if resolver.is_some() {
        rows.resolver
    } else {
        rows.resource
    };
    let suffix = suffixes[usize::from(grant)];
    let identity_suffix = if rows.boundary {
        format!(
            "PermissionChanged:{suffix}:{subject}:{}",
            authority.authority_key.as_deref().unwrap_or_default()
        )
    } else {
        format!("PermissionChanged:{suffix}:{subject}")
    };
    events.extend(row(
        authority,
        subject,
        resolver,
        grant,
        rows.emitter,
        rows.chain_id,
        rows.source_event_kind,
        identity_suffix,
    ));
}
