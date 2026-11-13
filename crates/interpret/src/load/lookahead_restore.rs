//! The restore of a lookahead batch: one query over the closure's final set, applied in
//! restore order a chunk at a time.
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::ControlFlow,
};

use bigname_adapters::schema_v2::{
    AdapterSession, AdapterSessionRestore, LookaheadRestore, PriorEventInput, V1BatchDependencies,
    v2_key,
};

use super::{lookahead_query, name_key};
use crate::Result;

/// What one attempt's restore read: the session, or the adapter error that ended the attempt,
/// with the number of events read and the whole ENSv2 registries among them.
pub(super) struct Restored {
    pub(super) session: anyhow::Result<AdapterSession>,
    pub(super) events: usize,
    pub(super) whole_registries: Vec<(String, usize, usize, usize)>,
    #[cfg(test)]
    pub(super) kinds: Vec<String>,
}

/// Read the events of the closure's final set in one query and apply them to `restore` in
/// restore order, a chunk at a time, under the loaded-keys check. A chunk is dropped once it
/// is applied. The restored state is not: it holds every loaded name until the batch ends.
pub(super) async fn restore_closure(
    connection: &mut sqlx::PgConnection,
    chain_id: &str,
    from_block: i64,
    dependencies: &V1BatchDependencies,
    restore: AdapterSessionRestore,
    predecessor: Option<time::OffsetDateTime>,
    latest_v2_topology: Option<time::OffsetDateTime>,
) -> Result<Restored> {
    let names: Vec<_> = dependencies.nodes.iter().map(name_key).collect();
    let resources: Vec<_> = dependencies.resource_ids.iter().copied().collect();
    let v2_keys: Vec<_> = dependencies.v2_keys.iter().cloned().collect();
    #[cfg(test)]
    let mut recorded = super::CLOSURES.with_borrow_mut(|closure| {
        closure.as_mut().map(|closure| {
            closure
                .final_passes
                .push((names.clone(), resources.clone(), v2_keys.clone()));
            Vec::new()
        })
    });
    #[cfg(test)]
    let mut kinds = Vec::new();
    let mut restore = LookaheadRestore::new(restore);
    let mut whole_registries = WholeRegistryLoads::new(dependencies);
    let mut failed = None;
    let apply = |chunk: Vec<PriorEventInput>| {
        whole_registries.observe(&chunk);
        #[cfg(test)]
        {
            super::note_resident(chunk.len());
            kinds.extend(chunk.iter().map(|event| event.event_kind.clone()));
            if let Some(recorded) = recorded.as_mut() {
                recorded.extend(chunk.iter().cloned());
            }
        }
        match restore.apply(chunk, dependencies) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => {
                failed = Some(error);
                ControlFlow::Break(())
            }
        }
    };
    let events = lookahead_query::stream_events(
        connection, chain_id, from_block, &names, &resources, &v2_keys, apply,
    )
    .await?;
    #[cfg(test)]
    super::CLOSURES.with_borrow_mut(|closure| {
        if let (Some(closure), Some(recorded)) = (closure, recorded) {
            closure.attempts.push((recorded, dependencies.clone()));
        }
    });
    let session = match failed {
        Some(error) => Err(error),
        None => restore.finish(predecessor, latest_v2_topology, dependencies),
    };
    Ok(Restored {
        session,
        events,
        whole_registries: whole_registries.finish(),
        #[cfg(test)]
        kinds,
    })
}

/// For each whole registry the batch read: its tokens, its restored events and the bytes of
/// those events' state, counted as the restore reads the events.
pub(super) struct WholeRegistryLoads(BTreeMap<String, (BTreeSet<String>, usize, usize)>);

impl WholeRegistryLoads {
    pub(super) fn new(dependencies: &V1BatchDependencies) -> Self {
        Self(
            dependencies
                .v2_keys
                .iter()
                .filter_map(|key| key.strip_suffix(":*"))
                .map(|registry| (registry.to_ascii_lowercase(), (BTreeSet::new(), 0, 0)))
                .collect(),
        )
    }

    pub(super) fn observe(&mut self, prior: &[PriorEventInput]) {
        if self.0.is_empty() || prior.len() < 2 {
            return;
        }
        for event in prior {
            // `<registry>:-:<token or resource id>:-:<source event>`, as `lookahead/v2_keys.sql`
            // reads it. A token's ids share one key, so the key counts the token.
            let mut scope = event.state_scope.as_deref().unwrap_or_default().split(':');
            let Some((emitter, (tokens, events, bytes))) = scope.next().and_then(|emitter| {
                self.0
                    .get_mut(&emitter.to_ascii_lowercase())
                    .map(|loads| (emitter, loads))
            }) else {
                continue;
            };
            *events += 1;
            *bytes += event.after_state.to_string().len();
            tokens.extend(
                scope
                    .nth(1)
                    .filter(|token| !matches!(*token, "" | "-"))
                    .map(|token| v2_key(emitter, token)),
            );
        }
    }

    pub(super) fn finish(self) -> Vec<(String, usize, usize, usize)> {
        self.0
            .into_iter()
            .map(|(registry, (tokens, events, bytes))| (registry, tokens.len(), events, bytes))
            .collect()
    }
}
