//! The account that holds a node's registry-only resource: the one subject with a live
//! `resource_control` grant on it, and the resolver of its live `resolver_control` grant. Every
//! permission row on that resource moves this holder, so live interpretation and every restore
//! read the same account.

use imbl::OrdMap;
use uuid::Uuid;

use super::State;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) struct RegistryOnlyHolder {
    pub subject: String,
    pub resolver: Option<String>,
    /// The transaction of the registry write that made this subject the holder, when a
    /// registration later in that transaction takes the write over. The registration's lease
    /// holds those grants from then on, which block reconciliation records by moving the rows.
    pub registration_transaction: Option<String>,
}

/// One restored permission row's place: its subject and, for `resolver_control`, its resolver,
/// both lowercase.
type RowScope = (String, Option<String>);

/// A restored row's subject and resolver as written, and whether it grants.
type RestoredRow = (String, Option<String>, bool);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct RegistryOnlyHolders {
    holders: OrdMap<Uuid, RegistryOnlyHolder>,
    /// While a restore runs, the newest row of each subject and scope on each resource it read,
    /// as (subject, resolver, granted). The rows of one subject and scope can sit under several
    /// retained keys, one per emitter, and folding keeps the newest row of each key, so only the
    /// newest row of each subject and scope says whether its grant is live.
    restoring: OrdMap<Uuid, OrdMap<RowScope, RestoredRow>>,
}

impl State {
    pub(in crate::schema_v2) fn v1_registry_only_holder(
        &self,
        resource_id: Uuid,
    ) -> Option<RegistryOnlyHolder> {
        self.v1_registry_only_holders
            .holders
            .get(&resource_id)
            .cloned()
    }

    pub(in crate::schema_v2) fn set_v1_registry_only_holder(
        &mut self,
        resource_id: Uuid,
        holder: Option<RegistryOnlyHolder>,
    ) {
        match holder {
            Some(holder) => {
                self.v1_registry_only_holders
                    .holders
                    .insert(resource_id, holder);
            }
            None => {
                self.v1_registry_only_holders.holders.remove(&resource_id);
            }
        }
    }

    /// Ends the holder a registry write of `transaction` set for the registration that takes
    /// that write over.
    pub(in crate::schema_v2) fn end_v1_registry_only_holder_for_registration(
        &mut self,
        resource_id: Uuid,
        transaction: &str,
    ) {
        if self
            .v1_registry_only_holder(resource_id)
            .is_some_and(|holder| holder.registration_transaction.as_deref() == Some(transaction))
        {
            self.set_v1_registry_only_holder(resource_id, None);
        }
    }

    /// Records a restored permission row on `resource_id`. The last row recorded for a subject
    /// and scope is taken as its newest, so this relies on rows arriving in chain order. Every
    /// seam delivers them that way: the fold keeps the rows of a batch in the order the batch
    /// produced them, which is chain order, and the loads order them by block and event. A cold
    /// restore relies on the same ordering.
    pub(in crate::schema_v2) fn restore_v1_registry_only_row(
        &mut self,
        resource_id: Uuid,
        subject: &str,
        resolver: Option<&str>,
        granted: bool,
    ) {
        let holder = self.v1_registry_only_holder(resource_id);
        let rows = self
            .v1_registry_only_holders
            .restoring
            .entry(resource_id)
            .or_insert_with(|| {
                // A delta restore starts from the holder the committed state already has.
                let mut rows = OrdMap::new();
                if let Some(holder) = holder {
                    rows.insert(
                        (holder.subject.to_ascii_lowercase(), None),
                        (holder.subject.clone(), None, true),
                    );
                    if let Some(resolver) = holder.resolver.as_deref() {
                        rows.insert(
                            (
                                holder.subject.to_ascii_lowercase(),
                                Some(resolver.to_ascii_lowercase()),
                            ),
                            (holder.subject.clone(), Some(resolver.to_owned()), true),
                        );
                    }
                }
                rows
            });
        rows.insert(
            (
                subject.to_ascii_lowercase(),
                resolver.map(str::to_ascii_lowercase),
            ),
            (subject.to_owned(), resolver.map(str::to_owned), granted),
        );
    }

    /// Sets the holder of every resource the restore read from the newest row of each subject
    /// and scope. Rows that leave two subjects with `resource_control`, a holder with two
    /// resolver scopes, or `resolver_control` with a subject that is not the holder break the
    /// one-holder rule, so they fail the restore rather than pick one.
    pub(in crate::schema_v2) fn finish_v1_registry_only_restore(&mut self) {
        let restoring = std::mem::take(&mut self.v1_registry_only_holders.restoring);
        for (resource_id, rows) in restoring {
            let live = rows
                .values()
                .filter(|(_, _, granted)| *granted)
                .collect::<Vec<_>>();
            let holders = live
                .iter()
                .filter(|(_, resolver, _)| resolver.is_none())
                .map(|(subject, _, _)| subject.as_str())
                .collect::<Vec<_>>();
            let resolvers = live
                .iter()
                .filter_map(|(subject, resolver, _)| {
                    resolver
                        .as_deref()
                        .map(|resolver| (subject.as_str(), resolver))
                })
                .collect::<Vec<_>>();
            let holder = match holders.as_slice() {
                [] => None,
                [holder] => Some(*holder),
                _ => {
                    self.record_restore_error(anyhow::anyhow!(
                        "registry-only resource {resource_id} has more than one live holder: {holders:?}"
                    ));
                    continue;
                }
            };
            let stray = resolvers.iter().any(|(subject, _)| {
                holder.is_none_or(|holder| !holder.eq_ignore_ascii_case(subject))
            });
            if stray || resolvers.len() > 1 {
                self.record_restore_error(anyhow::anyhow!(
                    "registry-only resource {resource_id} has resolver_control outside one holder scope: {resolvers:?}, holder {holder:?}"
                ));
                continue;
            }
            let holder = holder.map(|subject| RegistryOnlyHolder {
                subject: subject.to_owned(),
                resolver: resolvers
                    .first()
                    .map(|(_, resolver)| (*resolver).to_owned()),
                registration_transaction: None,
            });
            self.set_v1_registry_only_holder(resource_id, holder);
        }
    }
}
