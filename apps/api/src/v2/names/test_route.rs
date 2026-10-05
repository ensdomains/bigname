//! Test-only admission of the prepared contract through the same handler and snapshot path.
//! Production remains gated until union correctness and bounded-work validation is complete.
use axum::{Json, Router, extract::State, routing::get};

use super::{QueryParamAllowlist, SearchName, get_names_page, query};
use crate::{
    AppState,
    v2::{Envelope, V2Result},
};

pub(super) struct WindowQueryParams;
impl QueryParamAllowlist for WindowQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "expires_after",
        "expires_before",
        "expires_window",
        "authority",
        "parent",
        "sort",
        "order",
        "at",
        "finality",
        "cursor",
        "page_size",
    ];
}

async fn get_names_with_windows(
    params: query::NamesQuery<WindowQueryParams>,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<SearchName>>>> {
    get_names_page(params.params, params.windows, state).await
}

pub(crate) fn names_windows_test_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/names", get(get_names_with_windows))
        .with_state(state)
}
