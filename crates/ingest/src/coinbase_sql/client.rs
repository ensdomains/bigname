use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;

use super::{
    auth::CoinbaseSqlAuth, error::CoinbaseSqlHttpError, rate_limit::CoinbaseSqlRateLimiter,
    rows::CoinbaseLogRow, transport,
};

const MAX_ATTEMPTS: usize = 5;

#[derive(Clone)]
pub(super) struct CoinbaseSqlClient {
    url: String,
    auth: CoinbaseSqlAuth,
    rate_limiter: Arc<CoinbaseSqlRateLimiter>,
    timeout_secs: u64,
}

impl CoinbaseSqlClient {
    pub fn new(
        url: &str,
        key_id_env: &str,
        key_secret_env: &str,
        timeout_secs: u64,
        qps: u32,
    ) -> Result<Self> {
        let url = reqwest::Url::parse(url)
            .with_context(|| format!("failed to parse Coinbase SQL URL {url}"))?;
        if url.scheme() != "https" {
            bail!("Coinbase SQL URL must use https://");
        }
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().context("URL has no host")?),
            None => url.host_str().context("URL has no host")?.to_owned(),
        };
        let mut path = url.path().to_owned();
        if let Some(query) = url.query() {
            path.push('?');
            path.push_str(query);
        }
        Ok(Self {
            url: url.to_string(),
            auth: CoinbaseSqlAuth::from_env(key_id_env, key_secret_env, host, path)?,
            rate_limiter: Arc::new(CoinbaseSqlRateLimiter::new(qps)),
            timeout_secs,
        })
    }

    pub async fn run_query(&self, sql: &str) -> Result<Vec<CoinbaseLogRow>> {
        let rows = self.run_raw_query(sql).await?;
        rows.into_iter().map(CoinbaseLogRow::from_value).collect()
    }

    pub(super) async fn run_raw_query(&self, sql: &str) -> Result<Vec<Value>> {
        for attempt in 0..MAX_ATTEMPTS {
            self.rate_limiter.wait().await;
            let response = transport::run(
                self.url.clone(),
                self.auth.bearer_token()?,
                sql.to_owned(),
                self.timeout_secs,
            )
            .await;
            match response {
                Ok(response) if response.status.is_success() => {
                    return Ok(serde_json::from_str::<RunResponse>(&response.body)
                        .context("failed to decode Coinbase SQL response")?
                        .result
                        .unwrap_or_default());
                }
                Ok(response) if retryable_status(response.status) && attempt + 1 < MAX_ATTEMPTS => {
                    backoff(attempt, false).await;
                }
                Ok(response)
                    if response.status == StatusCode::FORBIDDEN
                        && serde_json::from_str::<Value>(&response.body).is_err()
                        && attempt + 1 < MAX_ATTEMPTS =>
                {
                    backoff(attempt, true).await;
                }
                Ok(response) => {
                    return Err(CoinbaseSqlHttpError {
                        status: response.status,
                        body: response.body,
                    }
                    .into());
                }
                Err(error) if attempt + 1 < MAX_ATTEMPTS => backoff(attempt, false).await,
                Err(error) => return Err(error).context("Coinbase SQL request failed"),
            }
        }
        bail!("Coinbase SQL request exhausted retries")
    }
}

#[derive(Deserialize)]
struct RunResponse {
    result: Option<Vec<Value>>,
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    ) || matches!(status.as_u16(), 520 | 521 | 522 | 524)
}

async fn backoff(attempt: usize, waf: bool) {
    let base = if waf { 2_000 } else { 250 };
    tokio::time::sleep(Duration::from_millis(base * (1_u64 << attempt.min(4)))).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinbase_edge_gateway_status_is_retried_in_client() {
        let status = StatusCode::from_u16(520).expect("520 is a valid HTTP status");

        assert!(retryable_status(status));
    }
}
