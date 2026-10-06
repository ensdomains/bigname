use crate::AppState;
use sqlx::{PgConnection, PgPool, Postgres, Transaction, types::time::OffsetDateTime};

use super::support::{
    PublicNamespaceSet, derive_public_namespace_set, ensure_public_namespace, request_scope_meta,
    revalidate_collection_manifests,
};
use super::{CursorPayload, Meta, V2Error, V2Result, api_error_to_v2};

/// The publication captured for one request. A current-state page reads it on one read-only
/// REPEATABLE READ snapshot ([`Self::conn`]), so its reads cannot mix publications; its
/// continuation positions survive later publications. Address history also admits its publication
/// on that snapshot; other history pages retain their documented bounded-walk behavior.
pub(crate) struct CollectionSnapshot {
    namespaces: PublicNamespaceSet,
    evaluated_at: OffsetDateTime,
    namespace: Option<String>,
    pool: PgPool,
    reads: Option<Transaction<'static, Postgres>>,
    served: bool,
    captured_at: std::time::Instant,
}

impl CollectionSnapshot {
    #[cfg(test)]
    pub(crate) async fn capture(state: &AppState, cursor: Option<&str>) -> V2Result<Self> {
        Self::capture_for_namespace(state, cursor, None).await
    }

    pub(crate) async fn capture_for_namespace(
        state: &AppState,
        cursor: Option<&str>,
        namespace: Option<&str>,
    ) -> V2Result<Self> {
        let (snapshot, _) = Self::capture_scope(state, cursor, namespace).await?;
        Ok(snapshot)
    }

    /// Admission for the history collections, whose cursors are keyset positions that no
    /// publication binds (`history_keyset`). An unserved namespace is refused first (404), then
    /// `decode_cursor` decodes and binds the request cursor (400), and only then must every scope
    /// have a publication (409). A cursor's publication token, if any, is not checked.
    pub(crate) async fn capture_history<C>(
        state: &AppState,
        namespace: Option<&str>,
        decode_cursor: impl FnOnce() -> V2Result<C>,
    ) -> V2Result<(Self, C)> {
        if let Some(namespace) = namespace {
            ensure_public_namespace(namespace).map_err(api_error_to_v2)?;
        }
        let cursor = decode_cursor()?;
        let (snapshot, _) = Self::capture_scope(state, None, namespace).await?;
        Ok((snapshot, cursor))
    }

    /// Address history admits its publication on the same snapshot used by the bounded page.
    /// Manifest loading precedes acquiring the transaction, including for one-connection pools.
    pub(crate) async fn capture_address_history<C>(
        state: &AppState,
        namespace: Option<&str>,
        decode_cursor: impl FnOnce() -> V2Result<C>,
    ) -> V2Result<(Self, C)> {
        if let Some(namespace) = namespace {
            ensure_public_namespace(namespace).map_err(api_error_to_v2)?;
        }
        let cursor = decode_cursor()?;
        let prepared = super::support::prepare_public_namespace_admission(state)
            .await
            .map_err(api_error_to_v2)?;
        #[cfg(test)]
        finish_test_hooks::run_at(&state.pool, finish_test_hooks::Stage::BeforeRead).await?;
        let captured_at = std::time::Instant::now();
        let mut reads = bigname_storage::begin_read_snapshot(&state.pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to begin the address-history read"))?;
        let namespaces = prepared
            .select_on(&mut reads)
            .await
            .map_err(api_error_to_v2)?
            .for_namespace(namespace);
        let snapshot =
            Self::from_namespaces(state, namespace, namespaces, captured_at, Some(reads))?;
        Ok((snapshot, cursor))
    }

    async fn capture_scope(
        state: &AppState,
        cursor: Option<&str>,
        namespace: Option<&str>,
    ) -> V2Result<(Self, Option<CursorPayload>)> {
        let captured_at = std::time::Instant::now();
        if let Some(namespace) = namespace {
            ensure_public_namespace(namespace).map_err(api_error_to_v2)?;
        }
        let cursor = cursor.map(super::cursor::decode_collection).transpose()?;
        let namespaces = derive_public_namespace_set(state)
            .await
            .map_err(api_error_to_v2)?
            .for_namespace(namespace);
        let snapshot = Self::from_namespaces(state, namespace, namespaces, captured_at, None)?;
        Ok((snapshot, cursor))
    }

    fn from_namespaces(
        state: &AppState,
        namespace: Option<&str>,
        namespaces: PublicNamespaceSet,
        captured_at: std::time::Instant,
        reads: Option<Transaction<'static, Postgres>>,
    ) -> V2Result<Self> {
        if namespaces.is_empty()
            || namespaces
                .request_scope()
                .iter()
                .any(|scope| scope.selected().is_none())
        {
            return Err(V2Error::stale(
                "collection publication is not available; retry after indexing is ready",
            ));
        }

        let evaluated_at = { publication_clock(&namespaces)? };
        Ok(Self {
            namespaces,
            evaluated_at,
            namespace: namespace.map(str::to_owned),
            pool: state.pool.clone(),
            reads,
            served: false,
            captured_at,
        })
    }

    pub(crate) fn evaluated_at(&self) -> OffsetDateTime {
        self.evaluated_at
    }

    pub(crate) fn history_catalogue_publication(
        &self,
    ) -> bigname_storage::HistoryCataloguePublicationFence {
        self.namespaces
            .history_catalogue_publication(self.captured_at)
    }

    pub(crate) fn block_bounds(&self) -> std::collections::BTreeMap<String, i64> {
        let mut bounds = std::collections::BTreeMap::<String, i64>::new();
        for position in self
            .namespaces
            .request_scope()
            .iter()
            .filter_map(|scope| scope.selected())
            .flat_map(|selected| selected.chain_positions.as_map().values())
        {
            bounds
                .entry(position.chain_id.clone())
                .and_modify(|bound| *bound = (*bound).min(position.block_number))
                .or_insert(position.block_number);
        }
        bounds
    }

    /// The connection every current-state read of this request runs on: one read-only
    /// REPEATABLE READ snapshot, begun on first use. Before the first read on it, the snapshot must
    /// still serve each captured publication; a publication since admission is the retry 409.
    /// Marker sequences only grow, so that check also covers the published rows the request read
    /// on the pool between admission and the snapshot. After it, every read sees the captured publication
    /// whatever is published meanwhile; nothing may read the pool until [`Self::finish`].
    pub(crate) async fn conn(&mut self) -> V2Result<&mut PgConnection> {
        self.begin().await?;
        let reads = self
            .reads
            .as_deref_mut()
            .expect("the collection read snapshot was just begun");
        if !self.served {
            if !self
                .namespaces
                .served_on(reads)
                .await
                .map_err(api_error_to_v2)?
            {
                return Err(changed_during_read());
            }
            self.served = true;
            #[cfg(test)]
            finish_test_hooks::run_on(reads, finish_test_hooks::Stage::Pinned).await?;
        }
        Ok(self
            .reads
            .as_deref_mut()
            .expect("the collection read snapshot was just begun"))
    }

    /// The snapshot before [`Self::conn`]'s publication check, for a route whose own pinned
    /// selection must be checked on it first.
    pub(crate) async fn begin(&mut self) -> V2Result<&mut PgConnection> {
        if self.reads.is_none() {
            #[cfg(test)]
            finish_test_hooks::run_at(&self.pool, finish_test_hooks::Stage::BeforeRead).await?;
            let reads = bigname_storage::begin_read_snapshot(&self.pool)
                .await
                .map_err(|_| V2Error::internal_error("failed to begin the collection read"))?;
            self.reads = Some(reads);
        }
        Ok(self
            .reads
            .as_deref_mut()
            .expect("the collection read snapshot was just begun"))
    }

    /// A name with no
    /// composed row may be one a family rebuild has yet to reach: before a route answers it not
    /// found, the family markers of this snapshot's chains must be servable, otherwise it is the
    /// stale 409 for `resource`. Read on the request's snapshot when it has begun one.
    pub(crate) async fn ensure_families_published(
        &mut self,
        state: &AppState,
        resource: super::SnapshotReadResource,
    ) -> V2Result<()> {
        let chains: Vec<String> = self.block_bounds().into_keys().collect();
        let db = match self.reads.as_deref_mut() {
            Some(conn) => bigname_storage::ReadDb::from(conn),
            None => bigname_storage::ReadDb::from(&state.pool),
        };
        bigname_storage::families::name::ensure_family_publications(db, &chains)
            .await
            .map(|_| ())
            .map_err(super::name_rows_error(resource, |_| {
                super::V2Error::internal_error("failed to read the family publication")
            }))
    }

    /// The `meta` of the captured publication, which every read on [`Self::conn`] sees.
    pub(crate) fn meta(&self) -> V2Result<Meta> {
        request_scope_meta(self.namespaces.request_scope())
    }

    /// Ends the snapshot and reports the captured publication. A page that never read on the
    /// snapshot still has its publication checked on one. After that only the namespace authority
    /// is rechecked: a publication after the snapshot began is not part of this page.
    pub(crate) async fn finish(&mut self, state: &AppState) -> V2Result<Meta> {
        if !self.served {
            self.conn().await?;
        }
        #[cfg(test)]
        finish_test_hooks::run_on(
            self.reads.as_deref_mut().expect("served snapshot"),
            finish_test_hooks::Stage::Finish,
        )
        .await?;
        if let Some(reads) = self.reads.take() {
            reads
                .commit()
                .await
                .map_err(|_| V2Error::internal_error("failed to end the collection read"))?;
        }
        revalidate_collection_manifests(state, &self.namespaces, self.namespace.as_deref())
            .await
            .map_err(|error| {
                if error.status != axum::http::StatusCode::CONFLICT {
                    api_error_to_v2(error)
                } else {
                    changed_during_read()
                }
            })?;
        self.meta()
    }

    /// `error` for a page refused after its reads, once [`Self::finish`] succeeds; otherwise
    /// finish's own error, such as the retry 409 for a namespace manifest change.
    pub(crate) async fn refuse(&mut self, state: &AppState, error: V2Error) -> V2Error {
        match self.finish(state).await {
            Ok(_) => error,
            Err(finish_error) => finish_error,
        }
    }

    /// The `meta` of a history page: the publication captured when the request was admitted. A
    /// history page is not a snapshot, so a publication during the read does not refuse it.
    pub(crate) async fn finish_history(&self, state: &AppState) -> V2Result<Meta> {
        #[cfg(test)]
        finish_test_hooks::run(&state.pool).await?;
        #[cfg(not(test))]
        let _ = state;
        request_scope_meta(self.namespaces.request_scope())
    }
}

/// The expiry clock: the published block's time, on a first
/// page and every continuation alike. Every selected position is the family marker's block,
/// since the fence admits a scope
/// only when the marker sits exactly there, so its lineage timestamp is the marker's
/// `block_timestamp`. A scope spanning chains takes the earliest, as `block_bounds` takes the
/// lowest block.
fn publication_clock(namespaces: &PublicNamespaceSet) -> V2Result<OffsetDateTime> {
    namespaces
        .request_scope()
        .iter()
        .filter_map(|scope| scope.selected())
        .flat_map(|selected| selected.chain_positions.as_map().values())
        .map(|position| position.timestamp)
        .min()
        .ok_or_else(|| {
            V2Error::stale("collection publication is not available; retry after indexing is ready")
        })
}

pub(super) fn restart_required() -> V2Error {
    V2Error::stale(
        "collection publication is no longer available; restart pagination without a cursor",
    )
}

/// The publication or namespace authority moved before the read could pin it; the same request
/// and cursor can be retried against the new publication.
pub(super) fn changed_during_read() -> V2Error {
    V2Error::stale("collection publication changed during the read; retry the request")
}

/// Pauses one test database's next request at a [`Stage`], so a test can publish a block there.
/// Finding the hook takes a second pool connection while the read snapshot may hold one.
#[cfg(test)]
pub(crate) mod finish_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use super::{V2Error, V2Result};

    #[derive(Clone)]
    pub(crate) struct FinishHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct FinishControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl FinishControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub(crate) enum Stage {
        /// After admission, before the read snapshot begins.
        BeforeRead,
        /// Once the snapshot has passed its publication check, before the page reads on it.
        Pinned,
        /// After the page was read, while its snapshot is still open.
        Finish,
    }

    type Key = (String, Stage);

    static HOOKS: ScopedTestHookRegistry<Key, FinishHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(ScopedTestHookGuard<Key, FinishHook>, FinishControl)> {
        install_at(pool, Stage::Finish).await
    }

    pub(crate) async fn install_at(
        pool: &PgPool,
        stage: Stage,
    ) -> Result<(ScopedTestHookGuard<Key, FinishHook>, FinishControl)> {
        let database = (current_test_database(pool).await?, stage);
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            FinishHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, FinishControl { reached, resume }))
    }

    pub(crate) async fn run(pool: &PgPool) -> V2Result<()> {
        run_at(pool, Stage::Finish).await
    }

    pub(crate) async fn run_at(pool: &PgPool, stage: Stage) -> V2Result<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to run collection finish test hook"))?;
        run_for_database(database, stage).await
    }

    pub(crate) async fn run_on(connection: &mut sqlx::PgConnection, stage: Stage) -> V2Result<()> {
        let database = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(connection)
            .await
            .map_err(|_| V2Error::internal_error("failed to run collection finish test hook"))?;
        run_for_database(database, stage).await
    }

    async fn run_for_database(database: String, stage: Stage) -> V2Result<()> {
        let database = (database, stage);
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}
