use crate::AppState;
use sqlx::types::time::OffsetDateTime;

use super::support::{
    PublicNamespaceSet, derive_public_namespace_set, request_scope_meta,
    revalidate_collection_namespace_set,
};
use super::{CursorPayload, Meta, V2Error, V2Result, api_error_to_v2};

/// Current projections are not retained after publication. Continuations must restart then.
pub(crate) struct CollectionSnapshot {
    namespaces: PublicNamespaceSet,
    token: String,
    evaluated_at: OffsetDateTime,
    namespace: Option<String>,
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
        };
        if let Some(cursor) = cursor.as_ref() {
            snapshot.validate_cursor(cursor)?;
        }
        Ok(snapshot)
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
        if cursor.snapshot.as_deref() != Some(self.token.as_str()) {
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
        revalidate_collection_namespace_set(state, &self.namespaces, self.namespace.as_deref())
            .await
            .map_err(|error| {
                if error.status == axum::http::StatusCode::CONFLICT {
                    restart_required()
                } else {
                    api_error_to_v2(error)
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
