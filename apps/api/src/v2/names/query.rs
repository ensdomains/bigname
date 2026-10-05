use std::collections::BTreeSet;

use axum::{
    extract::{FromRequestParts, Query},
    http::{Uri, request::Parts},
};

use crate::v2::{QueryParamAllowlist, QueryParams, RawQueryParams, V2Error};

use super::{
    NamesQueryParams,
    windows::{ExpiryWindows, WINDOW_KEY},
};

/// A route-local extractor: only expiry windows may repeat, in their original input order.
pub(crate) struct NamesQuery {
    pub(super) params: QueryParams,
    pub(super) windows: Option<ExpiryWindows>,
}

impl<S> FromRequestParts<S> for NamesQuery
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(pairs) = Query::<Vec<(String, String)>>::from_request_parts(parts, state)
            .await
            .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        let mut singleton_keys = BTreeSet::new();
        let mut windows = Vec::new();
        let mut scalar = form_urlencoded::Serializer::new(String::new());
        for (key, value) in &pairs {
            if !NamesQueryParams::ALLOWED.contains(&key.as_str()) {
                return Err(V2Error::invalid_input(format!(
                    "unknown query parameter: {key}"
                )));
            }
            if key == WINDOW_KEY {
                windows.push(value.as_str());
            } else {
                if !singleton_keys.insert(key.as_str()) {
                    return Err(V2Error::invalid_input(format!(
                        "query parameter must not repeat: {key}"
                    )));
                }
                scalar.append_pair(key, value);
            }
        }
        if !windows.is_empty()
            && (singleton_keys.contains("expires_after")
                || singleton_keys.contains("expires_before"))
        {
            return Err(V2Error::invalid_input(
                "expires_window cannot be combined with expires_after or expires_before",
            ));
        }
        let windows = ExpiryWindows::parse(&windows)?;
        // Reuse the shared scalar parser without changing the request URI or other routes.
        let uri: Uri = format!("/?{}", scalar.finish())
            .parse()
            .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        let Query(raw) = Query::<RawQueryParams>::try_from_uri(&uri)
            .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        Ok(Self {
            params: QueryParams::try_from(raw)?,
            windows,
        })
    }
}
