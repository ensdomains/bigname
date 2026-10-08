use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{FromRequestParts, State},
    http::request::Parts,
};
use bigname_storage::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListFilter, NameCurrentListRow,
};
use serde::Deserialize;
use tracing::error;

use crate::{AppState, state::is_recognized_public_namespace};

use super::list_cursor::{ListCursor, ListPosition};
use super::{
    AtSelector, Envelope, Finality, Page, QueryParams, RawQueryParams, RegistrationStatus, V2Error,
    V2Result, api_error_to_v2,
    name_record::{ens_v1_of_row, name_registration_fields},
    support::{derive_public_namespace_set, revalidate_public_namespace_set},
    support::{
        explicit_namespace_request_scope, request_scope_meta,
        revalidate_explicit_namespace_request_scope,
    },
    validate_latest_collection_selectors,
};

const SEARCH_SORT: &str = "name_asc";
const Q_FILTER_KEY: &str = "q";
const MATCH_FILTER_KEY: &str = "match";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const NONE_FILTER_VALUE: &str = "";
const DISPLAY_NAME_CURSOR_KEY: &str = "display_name";
const NORMALIZED_NAME_CURSOR_KEY: &str = "normalized_name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";
const POSITION_KEYS: [&str; 4] = [
    DISPLAY_NAME_CURSOR_KEY,
    NAMESPACE_FILTER_KEY,
    NORMALIZED_NAME_CURSOR_KEY,
    NAMEHASH_CURSOR_KEY,
];
const SEARCH_QUERY_PARAMS: &[&str] = &[
    "q",
    "match",
    "namespace",
    "at",
    "finality",
    "cursor",
    "page_size",
];

mod types;
pub(crate) use types::SearchName;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SearchQueryParams {
    pub(crate) at: Option<AtSelector>,
    pub(crate) finality: Finality,
    pub(crate) q: String,
    pub(crate) match_mode: SearchMatch,
    pub(crate) namespace: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: u64,
}

/// Search's `match`, the vocabulary the name-list `q` filters share.
pub(crate) use super::name_filter::NameMatch as SearchMatch;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RawSearchQueryParams {
    q: Option<String>,
    #[serde(rename = "match")]
    match_mode: Option<String>,
    namespace: Option<String>,
    at: Option<String>,
    finality: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

impl<S> FromRequestParts<S> for SearchQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let raw = super::parse_raw_query_params_with_allowlist::<RawSearchQueryParams, S>(
            parts,
            state,
            SEARCH_QUERY_PARAMS,
        )
        .await?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawSearchQueryParams> for SearchQueryParams {
    type Error = V2Error;

    fn try_from(raw: RawSearchQueryParams) -> Result<Self, Self::Error> {
        let shared = QueryParams::try_from(RawQueryParams {
            at: raw.at,
            finality: raw.finality,
            namespace: raw.namespace,
            cursor: raw.cursor,
            page_size: raw.page_size,
            ..RawQueryParams::default()
        })?;

        if let Some(namespace) = shared.namespace.as_deref() {
            validate_namespace(namespace)?;
        }

        let match_mode = SearchMatch::parse(raw.match_mode.as_deref())?;
        Ok(Self {
            at: shared.at,
            finality: shared.finality,
            q: parse_q(raw.q, match_mode)?,
            match_mode,
            namespace: shared.namespace,
            cursor: shared.cursor,
            page_size: shared.page_size,
        })
    }
}

pub(crate) async fn get_search(
    params: SearchQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<SearchName>>>> {
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    // A cursor this list could not have written is refused before any read; a bare search's
    // namespace anchor needs the namespace set, so only that value waits for the full read.
    let preflight = search_list_cursor(&SearchCursorBinding {
        q: &params.q,
        match_mode: params.match_mode,
        namespace: params.namespace.as_deref(),
        public_namespaces: &[],
    });
    let preflight = if params.namespace.is_none() {
        preflight.deferring(NAMESPACE_FILTER_KEY)
    } else {
        preflight
    };
    preflight.check_shape(params.cursor.as_deref(), &POSITION_KEYS, false)?;
    let public_namespace_set = if params.namespace.is_none() {
        let namespaces = derive_public_namespace_set(&state)
            .await
            .map_err(|load_error| {
                error!(
                    service = "api",
                    status = %load_error.status,
                    code = load_error.code,
                    message = %load_error.message,
                    "failed to derive the public namespace set"
                );
                V2Error::internal_error("failed to select search namespaces")
            })?;
        if namespaces.is_empty() {
            return Err(V2Error::conflict(
                "the deployment does not serve a public namespace",
            ));
        }
        Some(namespaces)
    } else {
        None
    };
    let public_namespaces = public_namespace_set
        .as_ref()
        .map(|namespaces| namespaces.names())
        .unwrap_or_default();
    let explicit_namespace_meta = match params.namespace.as_deref() {
        Some(namespace) => Some(explicit_namespace_request_scope(&state, namespace).await?),
        None => None,
    };
    let cursor_binding = SearchCursorBinding {
        q: &params.q,
        match_mode: params.match_mode,
        namespace: params.namespace.as_deref(),
        public_namespaces,
    };
    let list = search_list_cursor(&cursor_binding);
    let storage_cursor = list
        .read(params.cursor.as_deref(), &POSITION_KEYS)?
        .map(|position| search_storage_cursor(&position))
        .transpose()?;

    let filter = search_filter(&params, public_namespaces);
    #[cfg(test)]
    if public_namespace_set.is_some() {
        public_namespace_read_test_hooks::run(&state.pool).await?;
    }
    // Identity text, finished public fields and live creation context share one snapshot.
    let mut reads = bigname_storage::begin_read_snapshot(&state.pool)
        .await
        .map_err(|_| V2Error::internal_error("failed to load search results"))?;
    let scope_changed = if let Some(namespaces) = public_namespace_set.as_ref() {
        !namespaces
            .served_on(&mut reads)
            .await
            .map_err(api_error_to_v2)?
    } else if let Some(scope) = explicit_namespace_meta.as_ref() {
        !scope.served_on(&mut reads).await?
    } else {
        false
    };
    // Matching rows must report an unavailable publication as stale before a scope conflict.
    let storage_page = load_search_storage_page(
        &mut reads,
        &filter,
        storage_cursor.as_ref(),
        params.page_size,
        !scope_changed
            && explicit_namespace_meta
                .as_ref()
                .is_none_or(|scope| scope.has_publication()),
    )
    .await?;
    // Closed before the revalidation below, which reads through the pool.
    reads
        .commit()
        .await
        .map_err(|_| V2Error::internal_error("failed to load search results"))?;
    #[cfg(test)]
    if public_namespace_set.is_none() {
        public_namespace_read_test_hooks::run(&state.pool).await?;
    }
    if let Some(public_namespace_set) = public_namespace_set.as_ref() {
        revalidate_public_namespace_set(&state, public_namespace_set)
            .await
            .map_err(api_error_to_v2)?;
    }
    let meta = match public_namespace_set.as_ref() {
        Some(public_namespace_set) => request_scope_meta(public_namespace_set.request_scope())?,
        None => {
            revalidate_explicit_namespace_request_scope(
                &state,
                params
                    .namespace
                    .as_deref()
                    .expect("explicit search scope must include a namespace"),
                explicit_namespace_meta
                    .expect("explicit search scope metadata must be captured before the page read"),
            )
            .await?
        }
    };
    if scope_changed {
        return Err(V2Error::conflict(
            "search namespace position changed while the request was being read",
        ));
    }

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| search_position(cursor).map(|position| list.next(position)))
        .transpose()?;
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .map(build_compact_search_name)
        .collect::<V2Result<_>>()?;
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta,
    }))
}

pub(crate) fn build_compact_search_name(
    row: &bigname_storage::families::search_dictionary::SearchRow,
) -> V2Result<SearchName> {
    let registration = &row.fields.registration;
    let authority = row
        .authority
        .as_deref()
        .map(|value| {
            crate::v2::vocab::Authority::from_wire(value)
                .ok_or_else(|| V2Error::internal_error("stored search authority is invalid"))
        })
        .transpose()?;
    Ok(SearchName {
        name: row.name.clone(),
        display_name: row.display_name.clone(),
        namespace: row.namespace.clone(),
        namehash: row.namehash.clone(),
        owner: row.owner.clone(),
        manager: registration.manager.clone(),
        status: registration.status,
        registered_at: registration.registered_at.clone(),
        created_at: row.created_at.clone(),
        expires_at: registration.expires_at.clone(),
        expires_at_reason: registration.expires_at_reason.clone(),
        grace_ends_at: registration.grace_ends_at.clone(),
        authority,
        ens_v1: row.fields.ens_v1.clone(),
        lapsed_registration: None,
        expires_window_index: None,
        grace_ends_window_index: None,
    })
}

pub(crate) fn build_search_name(row: &NameCurrentListRow) -> V2Result<SearchName> {
    let registration = name_registration_fields(Some(&row.row), &row.row.namespace);
    let ens_v1 = ens_v1_of_row(Some(&row.row))?;

    Ok(SearchName {
        name: row.row.normalized_name.clone(),
        display_name: row.row.canonical_display_name.clone(),
        namespace: row.row.namespace.clone(),
        namehash: row.row.namehash.clone(),
        owner: registration.owner,
        manager: registration.manager,
        status: registration.status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        expires_at_reason: registration.expires_at_reason,
        grace_ends_at: registration.grace_ends_at,
        authority: crate::v2::vocab::Authority::from_provenance(&row.row.provenance),
        ens_v1,
        lapsed_registration: None,
        expires_window_index: None,
        grace_ends_window_index: None,
    })
}

/// The search cursor binds `q`, `match` and the namespace anchor; it holds the last row's
/// position and no publication (`list_cursor`).
fn search_list_cursor(binding: &SearchCursorBinding<'_>) -> ListCursor {
    ListCursor::new(SEARCH_SORT, cursor_filters(binding))
}

fn search_position(cursor: &NameCurrentListCursor) -> V2Result<ListPosition> {
    let NameCurrentListCursorValue::Name(display_name) = &cursor.sort_value else {
        return Err(V2Error::internal_error(
            "search pagination cursor must use name sort",
        ));
    };

    Ok(ListPosition::new([
        (DISPLAY_NAME_CURSOR_KEY, display_name.clone()),
        (NAMESPACE_FILTER_KEY, cursor.namespace.clone()),
        (NORMALIZED_NAME_CURSOR_KEY, cursor.normalized_name.clone()),
        (NAMEHASH_CURSOR_KEY, cursor.namehash.clone()),
    ]))
}

fn search_storage_cursor(position: &ListPosition) -> V2Result<NameCurrentListCursor> {
    Ok(NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(
            position.get(DISPLAY_NAME_CURSOR_KEY)?.to_owned(),
        ),
        namespace: position.get(NAMESPACE_FILTER_KEY)?.to_owned(),
        normalized_name: position.get(NORMALIZED_NAME_CURSOR_KEY)?.to_owned(),
        namehash: position.get(NAMEHASH_CURSOR_KEY)?.to_owned(),
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SearchCursorBinding<'a> {
    pub(crate) q: &'a str,
    pub(crate) match_mode: SearchMatch,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) public_namespaces: &'a [String],
}

fn search_filter(
    params: &SearchQueryParams,
    public_namespaces: &[String],
) -> NameCurrentListFilter {
    // A search result carries no status or unsupported_reason field, so a name whose exact-name
    // authority is unsupported is omitted here rather than served from an unselected registration;
    // name detail and batch lookup carry the reason.
    let mut filter = NameCurrentListFilter {
        namespace: params.namespace.clone(),
        namespaces: params
            .namespace
            .is_none()
            .then(|| public_namespaces.to_vec()),
        supported_only: true,
        ..NameCurrentListFilter::default()
    };

    match params.match_mode {
        SearchMatch::Prefix => filter.prefix = Some(params.q.clone()),
        SearchMatch::Contains => filter.contains = Some(params.q.clone()),
    }

    filter
}

async fn load_search_storage_page(
    reads: &mut sqlx::PgConnection,
    filter: &NameCurrentListFilter,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    scope_ready: bool,
) -> V2Result<bigname_storage::families::search_dictionary::SearchPage> {
    bigname_storage::families::search_dictionary::load_page_for_scope(
        reads,
        filter,
        cursor,
        page_size,
        scope_ready,
    )
    .await
    .map_err(crate::v2::name_rows_error(
        crate::v2::SnapshotReadResource::Name,
        |_| V2Error::internal_error("failed to load search results"),
    ))
}

fn cursor_filters(binding: &SearchCursorBinding<'_>) -> BTreeMap<String, String> {
    BTreeMap::from([
        (Q_FILTER_KEY.to_owned(), binding.q.to_owned()),
        (
            MATCH_FILTER_KEY.to_owned(),
            binding.match_mode.as_str().to_owned(),
        ),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            namespace_cursor_anchor(binding),
        ),
    ])
}

fn namespace_cursor_anchor(binding: &SearchCursorBinding<'_>) -> String {
    if let Some(namespace) = binding.namespace {
        return namespace.to_owned();
    }

    if binding.public_namespaces.len() == 2
        && binding
            .public_namespaces
            .iter()
            .any(|namespace| namespace == "ens")
        && binding
            .public_namespaces
            .iter()
            .any(|namespace| namespace == "basenames")
    {
        return NONE_FILTER_VALUE.to_owned();
    }

    format!("public:{}", binding.public_namespaces.join(","))
}

fn parse_q(value: Option<String>, match_mode: SearchMatch) -> V2Result<String> {
    let value = value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| V2Error::invalid_input("q is required and must be non-empty"))?;
    match_mode.normalize(&value)
}

fn validate_namespace(namespace: &str) -> V2Result<()> {
    if is_recognized_public_namespace(namespace) {
        Ok(())
    } else {
        Err(V2Error::invalid_input("namespace is invalid"))
    }
}

#[cfg(test)]
pub(crate) mod public_namespace_read_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use crate::v2::{V2Error, V2Result};

    #[derive(Clone)]
    pub(crate) struct ReadHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub(crate) enum ReadHookPoint {
        AfterPage,
        AfterExplicitSelection,
        BeforeUnfencedGenerations,
    }

    pub(crate) struct ReadControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl ReadControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<(String, ReadHookPoint), ReadHook> =
        ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<(String, ReadHookPoint), ReadHook>,
        ReadControl,
    )> {
        install_at(pool, ReadHookPoint::AfterPage).await
    }

    pub(crate) async fn install_at(
        pool: &PgPool,
        point: ReadHookPoint,
    ) -> Result<(
        ScopedTestHookGuard<(String, ReadHookPoint), ReadHook>,
        ReadControl,
    )> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            (database, point),
            ReadHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, ReadControl { reached, resume }))
    }

    pub(crate) async fn run(pool: &PgPool) -> V2Result<()> {
        run_at(pool, ReadHookPoint::AfterPage).await
    }

    pub(crate) async fn run_at(pool: &PgPool, point: ReadHookPoint) -> V2Result<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to run search read test hook"))?;
        if let Some(hook) = HOOKS.take(&(database, point)) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod publication_fields_tests;
