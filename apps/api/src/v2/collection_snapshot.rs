use crate::AppState;
use sqlx::types::time::OffsetDateTime;

use super::support::{
    PublicNamespaceSet, derive_public_namespace_set, ensure_public_namespace, request_scope_meta,
    revalidate_collection_namespace_set,
};
use super::{CursorPayload, Meta, V2Error, V2Result, api_error_to_v2};

/// Current projections are not retained after publication. Continuations must restart then.
pub(crate) struct CollectionSnapshot {
    namespaces: PublicNamespaceSet,
    token: String,
    evaluated_at: OffsetDateTime,
    namespace: Option<String>,
    /// Whether the request continued an earlier page with a cursor.
    continues_cursor: bool,
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
        if let Some(namespace) = namespace {
            ensure_public_namespace(namespace).map_err(api_error_to_v2)?;
        }
        let cursor = cursor.map(super::decode).transpose()?;
        let namespaces = derive_public_namespace_set(state)
            .await
            .map_err(api_error_to_v2)?
            .for_namespace(namespace);
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
        let token = namespaces.collection_fingerprint();

        let evaluated_at = match cursor.as_ref() {
            Some(cursor) => bigname_storage::parse_rfc3339_utc_timestamp(
                cursor
                    .evaluated_at
                    .as_deref()
                    .ok_or_else(restart_required)?,
            )
            .map_err(|_| super::cursor::invalid_cursor_error())?,
            None => OffsetDateTime::now_utc()
                .replace_nanosecond(0)
                .expect("zero nanoseconds are valid"),
        };
        let snapshot = Self {
            namespaces,
            token,
            evaluated_at,
            namespace: namespace.map(str::to_owned),
            continues_cursor: cursor.is_some(),
        };
        if let Some(cursor) = cursor.as_ref() {
            snapshot.validate_cursor(cursor)?;
        }
        Ok(snapshot)
    }

    /// Records that the request carried a cursor that this snapshot did not decode, for
    /// routes whose cursors have their own layout and validate the publication token
    /// themselves. A continuation must be told to restart, not to retry.
    pub(crate) fn continuing_from_request_cursor(mut self, present: bool) -> Self {
        self.continues_cursor |= present;
        self
    }

    pub(crate) fn evaluated_at(&self) -> OffsetDateTime {
        self.evaluated_at
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

    pub(crate) fn validate_cursor(&self, cursor: &CursorPayload) -> V2Result<()> {
        self.validate_token(cursor.snapshot.as_deref())
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    pub(crate) fn validate_token(&self, token: Option<&str>) -> V2Result<()> {
        if token != Some(self.token()) {
            return Err(restart_required());
        }
        Ok(())
    }

    pub(crate) fn bind_cursor(&self, mut cursor: CursorPayload) -> CursorPayload {
        cursor.snapshot = Some(self.token.clone());
        cursor.evaluated_at = Some(super::format_timestamp(self.evaluated_at));
        cursor
    }

    pub(crate) async fn finish(&self, state: &AppState) -> V2Result<Meta> {
        #[cfg(test)]
        finish_test_hooks::run(&state.pool).await?;
        revalidate_collection_namespace_set(state, &self.namespaces, self.namespace.as_deref())
            .await
            .map_err(|error| {
                if error.status != axum::http::StatusCode::CONFLICT {
                    api_error_to_v2(error)
                } else if self.continues_cursor {
                    restart_required()
                } else {
                    changed_during_read()
                }
            })?;
        request_scope_meta(self.namespaces.request_scope())
    }
}

fn restart_required() -> V2Error {
    V2Error::stale(
        "collection publication is no longer available; restart pagination without a cursor",
    )
}

/// A request without a cursor has nothing to restart: the next attempt reads the new
/// publication.
fn changed_during_read() -> V2Error {
    V2Error::stale("collection publication changed during the read; retry the request")
}

/// Pauses the next `finish()` for one test database so a test can republish between a
/// handler's last generation check and the publication revalidation.
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

    static HOOKS: ScopedTestHookRegistry<String, FinishHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(ScopedTestHookGuard<String, FinishHook>, FinishControl)> {
        let database = current_test_database(pool).await?;
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

    pub(super) async fn run(pool: &PgPool) -> V2Result<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to run collection finish test hook"))?;
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}
