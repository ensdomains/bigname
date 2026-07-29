use std::fmt;

use reqwest::StatusCode;
use serde_json::Value;

#[derive(Debug)]
pub(super) struct CoinbaseSqlHttpError {
    pub(super) status: StatusCode,
    pub(super) body: String,
    pub(super) attempt_count: usize,
}

impl CoinbaseSqlHttpError {
    fn query_resource_limit(&self) -> Option<CoinbaseSqlQueryResourceLimit> {
        if self.status != StatusCode::BAD_REQUEST {
            return None;
        }
        let structured_message = serde_json::from_str::<Value>(&self.body)
            .ok()
            .and_then(|body| {
                body.get("errorMessage")
                    .and_then(Value::as_str)
                    .map(str::to_ascii_lowercase)
            });
        let rendered_body = self.body.to_ascii_lowercase();
        let contains = |needle: &str| {
            structured_message
                .as_deref()
                .is_some_and(|message| message.contains(needle))
                || rendered_body.contains(needle)
        };
        if contains("query memory limit exceeded") {
            Some(CoinbaseSqlQueryResourceLimit::Memory)
        } else if contains("limit for rows or bytes to read on leaf node exceeded")
            || contains("maximum bytes to read")
            || contains("too many bytes read")
        {
            Some(CoinbaseSqlQueryResourceLimit::BytesRead)
        } else {
            None
        }
    }
}

impl fmt::Display for CoinbaseSqlHttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Coinbase SQL request failed with status {}: {}",
            self.status,
            truncate_error_body(&self.body)
        )
    }
}

impl std::error::Error for CoinbaseSqlHttpError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CoinbaseSqlQueryResourceLimit {
    Memory,
    BytesRead,
}

impl CoinbaseSqlQueryResourceLimit {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "query_memory_limit",
            Self::BytesRead => "query_bytes_read_limit",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlQueryResourceLimitError {
    pub(super) error_class: CoinbaseSqlQueryResourceLimit,
    pub(super) attempt_count: usize,
}

pub(super) fn query_resource_limit_error(
    error: &anyhow::Error,
) -> Option<CoinbaseSqlQueryResourceLimitError> {
    let error = error.downcast_ref::<CoinbaseSqlHttpError>()?;
    Some(CoinbaseSqlQueryResourceLimitError {
        error_class: error.query_resource_limit()?,
        attempt_count: error.attempt_count,
    })
}

pub(super) fn truncate_error_body(body: &str) -> String {
    const MAX_ERROR_BODY_CHARS: usize = 2_000;
    let mut truncated = body.chars().take(MAX_ERROR_BODY_CHARS).collect::<String>();
    if body.chars().count() > MAX_ERROR_BODY_CHARS {
        truncated.push_str("...");
    }
    truncated
}

#[cfg(test)]
pub(crate) fn test_bad_request_error(body: &str) -> anyhow::Error {
    CoinbaseSqlHttpError {
        status: StatusCode::BAD_REQUEST,
        body: body.to_owned(),
        attempt_count: 1,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_structured_memory_limit_errors() {
        let error = test_bad_request_error(
            r#"{"errorMessage":"Query memory limit exceeded","errorType":"invalid_request"}"#,
        );

        assert_eq!(
            query_resource_limit_error(&error),
            Some(CoinbaseSqlQueryResourceLimitError {
                error_class: CoinbaseSqlQueryResourceLimit::Memory,
                attempt_count: 1,
            })
        );
    }

    #[test]
    fn classifies_leaf_bytes_read_limit_errors() {
        let error = test_bad_request_error(
            "Limit for rows or bytes to read on leaf node exceeded, max bytes: 93.13 GiB",
        );

        assert_eq!(
            query_resource_limit_error(&error),
            Some(CoinbaseSqlQueryResourceLimitError {
                error_class: CoinbaseSqlQueryResourceLimit::BytesRead,
                attempt_count: 1,
            })
        );
    }

    #[test]
    fn leaves_unrelated_bad_requests_non_adaptive() {
        let error = test_bad_request_error("syntax error");
        assert_eq!(query_resource_limit_error(&error), None);
    }
}
