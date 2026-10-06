//! Account-wide events have direct participants, never implied name ownership.

use sqlx::{Postgres, QueryBuilder};

use super::HistoryScope;
use crate::AddressNameRelation;

pub(super) const ANCHORED_EVENT: &str = "ne.event_kind NOT IN ('RootPermissionChanged', 'AccountPermissionChanged', 'ReverseChanged', 'RegistryCreated', 'ParentChanged', 'Upgraded')";

#[derive(Clone, Copy)]
pub(super) enum DirectAccount {
    Owner,
    Operator,
    Reverse,
}

pub(super) fn participants(
    scope: HistoryScope,
    relations: Option<&[AddressNameRelation]>,
) -> Vec<DirectAccount> {
    if scope != HistoryScope::Both {
        return Vec::new();
    }
    let relations = relations.filter(|values| !values.is_empty());
    let mut kinds = Vec::new();
    if relations.is_none_or(|values| values.contains(&AddressNameRelation::TokenHolder)) {
        kinds.push(DirectAccount::Owner);
    }
    if relations.is_none_or(|values| values.contains(&AddressNameRelation::RoleHolder)) {
        kinds.push(DirectAccount::Operator);
    }
    if relations.is_none_or(|values| {
        [
            AddressNameRelation::TokenHolder,
            AddressNameRelation::EffectiveController,
            AddressNameRelation::RoleHolder,
        ]
        .iter()
        .all(|relation| values.contains(relation))
    }) {
        kinds.push(DirectAccount::Reverse);
    }
    kinds
}

impl DirectAccount {
    pub(super) fn push_predicate<'a>(
        self,
        query: &mut QueryBuilder<'a, Postgres>,
        address: &'a str,
        namespace: Option<&'a str>,
    ) {
        query.push(match self {
            Self::Owner => "ne.event_kind = 'AccountPermissionChanged' AND ne.after_state #>> '{scope,kind}' = 'account' AND ne.after_state ->> 'relation_kind' = 'operator' AND lower(ne.after_state #>> '{scope,owner}') = ",
            Self::Operator => "ne.event_kind = 'AccountPermissionChanged' AND ne.after_state #>> '{scope,kind}' = 'account' AND ne.after_state ->> 'relation_kind' = 'operator' AND lower(ne.after_state ->> 'subject') = ",
            Self::Reverse => "ne.event_kind = 'ReverseChanged' AND lower(ne.after_state ->> 'address') = ",
        });
        query.push_bind(address);
        if let Some(namespace) = namespace {
            query.push(" AND ne.namespace = ").push_bind(namespace);
        }
    }
}
