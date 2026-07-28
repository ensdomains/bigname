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
    fn is_query_memory_limit(&self) -> bool {
        if self.status != StatusCode::BAD_REQUEST {
            return false;
        }
        let structured_message_matches = serde_json::from_str::<Value>(&self.body)
            .ok()
            .and_then(|body| {
                body.get("errorMessage")
                    .and_then(Value::as_str)
                    .map(str::to_ascii_lowercase)
            })
            .is_some_and(|message| message.contains("query memory limit exceeded"));
        structured_message_matches
            || self
                .body
                .to_ascii_lowercase()
                .contains("query memory limit exceeded")
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

pub(super) fn query_memory_limit_attempt_count(error: &anyhow::Error) -> Option<usize> {
    error
        .downcast_ref::<CoinbaseSqlHttpError>()
        .filter(|error| error.is_query_memory_limit())
        .map(|error| error.attempt_count)
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
