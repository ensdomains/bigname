use std::fmt;

use reqwest::StatusCode;

#[derive(Debug)]
pub(super) struct CoinbaseSqlHttpError {
    pub status: StatusCode,
    pub body: String,
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

pub(super) fn truncate_error_body(body: &str) -> String {
    const MAX_ERROR_BODY_CHARS: usize = 2_000;
    let mut truncated = body.chars().take(MAX_ERROR_BODY_CHARS).collect::<String>();
    if body.chars().count() > MAX_ERROR_BODY_CHARS {
        truncated.push_str("...");
    }
    truncated
}
