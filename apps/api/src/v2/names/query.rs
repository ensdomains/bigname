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

/// A route-local extractor: only date windows may repeat, in their original input order.
pub(crate) struct NamesQuery {
    pub(super) params: QueryParams,
    pub(super) windows: Option<ExpiryWindows>,
    pub(super) deadline: bigname_storage::NameCurrentDeadline,
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
        let expiry = pairs.iter().any(|(key, _)| {
            matches!(
                key.as_str(),
                "expires_after" | "expires_before" | "expires_window"
            )
        });
        let grace = pairs.iter().any(|(key, _)| {
            matches!(
                key.as_str(),
                "grace_ends_after" | "grace_ends_before" | "grace_ends_window"
            )
        });
        if expiry && grace {
            return Err(V2Error::invalid_input(
                "exactly one date family is allowed: expires or grace_ends",
            ));
        }
        let deadline = if grace {
            bigname_storage::NameCurrentDeadline::GraceEnds
        } else {
            bigname_storage::NameCurrentDeadline::Expiry
        };
        let mut windows = Vec::new();
        let mut scalar = form_urlencoded::Serializer::new(String::new());
        for (key, value) in &pairs {
            if !NamesQueryParams::ALLOWED.contains(&key.as_str()) {
                return Err(V2Error::invalid_input(format!(
                    "unknown query parameter: {key}"
                )));
            }
            if key == WINDOW_KEY || key == "grace_ends_window" {
                windows.push(value.as_str());
            } else {
                if !singleton_keys.insert(key.as_str()) {
                    return Err(V2Error::invalid_input(format!(
                        "query parameter must not repeat: {key}"
                    )));
                }
                // Reuse exact scalar timestamp parsing without adding grace filters to other routes.
                let scalar_key = match key.as_str() {
                    "grace_ends_after" => "expires_after",
                    "grace_ends_before" => "expires_before",
                    other => other,
                };
                scalar.append_pair(scalar_key, value);
            }
        }
        if !windows.is_empty()
            && (singleton_keys.contains("expires_after")
                || singleton_keys.contains("expires_before")
                || singleton_keys.contains("grace_ends_after")
                || singleton_keys.contains("grace_ends_before"))
        {
            return Err(V2Error::invalid_input(
                "date windows cannot be combined with scalar date bounds",
            ));
        }
        let windows = ExpiryWindows::parse(&windows)?;
        // Reuse the shared scalar parser without changing the request URI or other routes.
        let uri: Uri = format!("/?{}", scalar.finish())
            .parse()
            .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        let Query(mut raw) = Query::<RawQueryParams>::try_from_uri(&uri)
            .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        let sort_wire = raw
            .sort
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if sort_wire.as_deref() == Some("grace_ends_at") {
            raw.sort = Some("expires_at".to_owned());
        }
        let mut params = QueryParams::try_from(raw)?;
        params.sort_wire = sort_wire;
        if let Some(sort) = params.sort_wire.as_deref()
            && sort != deadline.column()
        {
            return Err(V2Error::invalid_input(
                "sort must match the selected date family",
            ));
        }
        Ok(Self {
            params,
            windows,
            deadline,
        })
    }
}
