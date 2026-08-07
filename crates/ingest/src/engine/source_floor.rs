use crate::{
    Result,
    engine::{BatchRequest, Engine, Marker, SourceDescriptor},
    plan::enforce_source_floor,
    provider::{ProviderKind, normalized_kind, provider_error},
};

impl Engine {
    /// Refuses, before any ingest work, a plan whose range starts below what a source holds.
    ///
    /// Only a source reading a node's database directly reports a floor at all.
    pub(super) async fn enforce_source_floors(&self, request: &BatchRequest) -> Result<()> {
        for source in &request.sources {
            if normalized_kind(&source.kind) == ProviderKind::Coinbase {
                continue;
            }
            let Some((from, to)) =
                planned_range(source, request.redo_range, request.resume_current.as_ref())
            else {
                continue;
            };
            let Some(floor) = self.source_floor(&request.chain_id, source).await? else {
                continue;
            };
            enforce_source_floor(&source.key, from, to, floor)?;
        }
        Ok(())
    }

    /// Checks one concrete window against the floor the source reports now.
    ///
    /// Used where a range is known but not yet recorded: after a batch fetches a window and
    /// before it stores it, and for the live suffix once its common ancestor is known. Both
    /// read the floor through the call that makes a read-only provider catch up with the node.
    pub(super) async fn enforce_window_floor(
        &self,
        chain_id: &str,
        source: &SourceDescriptor,
        from: i64,
        to: i64,
    ) -> Result<()> {
        if normalized_kind(&source.kind) == ProviderKind::Coinbase {
            return Ok(());
        }
        let Some(floor) = self.source_floor(chain_id, source).await? else {
            return Ok(());
        };
        enforce_source_floor(&source.key, from, Some(to), floor)
    }

    async fn source_floor(&self, chain_id: &str, source: &SourceDescriptor) -> Result<Option<i64>> {
        if let Some(floor) = injected_floor(&source.endpoint) {
            return Ok(Some(floor));
        }
        let provider = self.provider(chain_id, source).await?;
        provider.earliest_available_block().await.map_err(|error| {
            provider_error(
                &format!(
                    "failed to read the earliest available block for source {}",
                    source.key
                ),
                error,
            )
        })
    }
}

/// The block range a batch plans for one source, or `None` when it plans nothing.
///
/// A normal batch is judged against the source's whole declared window rather than the
/// one window it fetches: planning cannot tell coverage recorded before the node pruned
/// from coverage recorded through a pruned window, so it refuses both until the node holds
/// the declared range again or the declared start block moves. A redo carries its own
/// range and its own durable progress, so it is judged on what it has left to read — a
/// redo whose remaining suffix sits above the floor keeps running on a pruned node.
fn planned_range(
    source: &SourceDescriptor,
    redo_range: Option<(i64, i64)>,
    resume_current: Option<&Marker>,
) -> Option<(i64, Option<i64>)> {
    let Some((from, to)) = redo_range else {
        return Some((source.start_block, None));
    };
    let resumed = resume_current.map_or(from, |marker| marker.number.saturating_add(1));
    let from = from.max(resumed).max(source.start_block);
    (from <= to).then_some((from, Some(to)))
}

#[cfg(test)]
fn injected_floor(endpoint: &str) -> Option<i64> {
    test_floors::floor(endpoint)
}

#[cfg(not(test))]
const fn injected_floor(_endpoint: &str) -> Option<i64> {
    None
}

/// Stands in for a pruned datadir, which no unit test can build.
#[cfg(test)]
mod test_floors {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry};

    /// A node that prunes at a chosen moment: the floor rises only once the source has
    /// served the window, so a read taken before the fetch cannot observe the new floor.
    pub(super) struct PruningNode {
        before_fetch: i64,
        after_fetch: i64,
        fetched: AtomicBool,
    }

    impl PruningNode {
        pub(super) fn observe_fetch(&self) {
            self.fetched.store(true, Ordering::SeqCst);
        }

        fn floor(&self) -> i64 {
            if self.fetched.load(Ordering::SeqCst) {
                self.after_fetch
            } else {
                self.before_fetch
            }
        }
    }

    static FLOORS: ScopedTestHookRegistry<String, Arc<PruningNode>> = ScopedTestHookRegistry::new();

    pub(super) fn install(
        endpoint: &str,
        floor: i64,
    ) -> ScopedTestHookGuard<String, Arc<PruningNode>> {
        install_node(endpoint, pruning_node(floor, floor))
    }

    pub(super) fn pruning_node(before_fetch: i64, after_fetch: i64) -> Arc<PruningNode> {
        Arc::new(PruningNode {
            before_fetch,
            after_fetch,
            fetched: AtomicBool::new(false),
        })
    }

    /// Installed after the source is listening, so the node and its endpoint agree.
    pub(super) fn install_node(
        endpoint: &str,
        node: Arc<PruningNode>,
    ) -> ScopedTestHookGuard<String, Arc<PruningNode>> {
        FLOORS.install(endpoint.to_owned(), node)
    }

    pub(super) fn floor(endpoint: &str) -> Option<i64> {
        FLOORS
            .get_cloned(&endpoint.to_owned())
            .map(|node| node.floor())
    }
}

#[cfg(test)]
#[path = "source_floor/tests.rs"]
mod tests;
