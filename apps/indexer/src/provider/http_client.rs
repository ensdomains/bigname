use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result};

/// Keep at most four idle connections per provider host and discard them after
/// 15 seconds. Issue #148 exposed a drpc load balancer half-closing pooled
/// keep-alive connections while reqwest still considered them reusable; these
/// tighter bounds limit both the number and lifetime of those stale sockets.
const JSON_RPC_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const JSON_RPC_POOL_MAX_IDLE_PER_HOST: usize = 4;

/// Rebuild the complete reqwest client after the first transport timeout from
/// one client generation. A production timeout can already consume 45 seconds,
/// while replacing this shared handle is cheap and does not cancel requests
/// that already hold a snapshot of the old client. The request layer retries
/// at most five times with 250/500/1000/2000 ms delays, so the second attempt
/// uses a fresh connection pool.
pub(super) const JSON_RPC_POOL_RESET_TIMEOUT_THRESHOLD: usize = 1;

#[derive(Clone)]
pub(super) struct RecoveringHttpClient {
    inner: Arc<RecoveringHttpClientInner>,
}

struct RecoveringHttpClientInner {
    state: Mutex<RecoveringHttpClientState>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

struct RecoveringHttpClientState {
    client: reqwest::Client,
    generation: u64,
    consecutive_timeouts: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct HttpClientPoolReset {
    pub(super) previous_generation: u64,
    pub(super) new_generation: u64,
}

impl RecoveringHttpClient {
    pub(super) fn new(connect_timeout: Duration, request_timeout: Duration) -> Result<Self> {
        let client = build_client(connect_timeout, request_timeout)?;
        Ok(Self {
            inner: Arc::new(RecoveringHttpClientInner {
                state: Mutex::new(RecoveringHttpClientState {
                    client,
                    generation: 0,
                    consecutive_timeouts: 0,
                }),
                connect_timeout,
                request_timeout,
            }),
        })
    }

    pub(super) fn snapshot(&self) -> (reqwest::Client, u64) {
        let state = self.lock_state();
        (state.client.clone(), state.generation)
    }

    pub(super) fn record_transport_success(&self, generation: u64) {
        let mut state = self.lock_state();
        if state.generation == generation {
            state.consecutive_timeouts = 0;
        }
    }

    pub(super) fn record_transport_error(
        &self,
        generation: u64,
        error: &reqwest::Error,
    ) -> Result<Option<HttpClientPoolReset>> {
        if error.is_timeout() && !error.is_connect() {
            return self.record_transport_timeout(generation);
        }

        let mut state = self.lock_state();
        if state.generation != generation {
            return Ok(None);
        }
        state.consecutive_timeouts = 0;
        Ok(None)
    }

    fn record_transport_timeout(&self, generation: u64) -> Result<Option<HttpClientPoolReset>> {
        let replacement = build_client(self.inner.connect_timeout, self.inner.request_timeout)
            .context("failed to rebuild JSON-RPC HTTP client after transport timeouts")?;
        let mut state = self.lock_state();
        if state.generation != generation {
            return Ok(None);
        }
        state.consecutive_timeouts = state.consecutive_timeouts.saturating_add(1);
        if state.consecutive_timeouts < JSON_RPC_POOL_RESET_TIMEOUT_THRESHOLD {
            return Ok(None);
        }

        let previous_generation = state.generation;
        state.client = replacement;
        state.generation = state.generation.saturating_add(1);
        state.consecutive_timeouts = 0;
        Ok(Some(HttpClientPoolReset {
            previous_generation,
            new_generation: state.generation,
        }))
    }

    fn lock_state(&self) -> MutexGuard<'_, RecoveringHttpClientState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

fn build_client(connect_timeout: Duration, request_timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .pool_idle_timeout(JSON_RPC_POOL_IDLE_TIMEOUT)
        .pool_max_idle_per_host(JSON_RPC_POOL_MAX_IDLE_PER_HOST)
        .build()
        .context("failed to build JSON-RPC HTTP client")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_transport_timeout_rebuilds_the_client_generation() -> Result<()> {
        let client =
            RecoveringHttpClient::new(Duration::from_millis(10), Duration::from_millis(20))?;
        let (_, initial_generation) = client.snapshot();

        let reset = client
            .record_transport_timeout(initial_generation)?
            .expect("the first timeout must rebuild the HTTP client");

        assert_eq!(reset.previous_generation, initial_generation);
        assert_eq!(reset.new_generation, initial_generation + 1);
        assert_eq!(client.snapshot().1, reset.new_generation);
        assert_eq!(
            client.record_transport_timeout(initial_generation)?,
            None,
            "late outcomes from the replaced generation must be ignored"
        );
        Ok(())
    }
}
