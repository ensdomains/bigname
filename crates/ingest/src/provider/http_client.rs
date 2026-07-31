use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result};

const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const POOL_MAX_IDLE_PER_HOST: usize = 4;

#[derive(Clone)]
pub(super) struct RecoveringHttpClient {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

struct State {
    client: reqwest::Client,
    client_id: u64,
}

impl RecoveringHttpClient {
    pub(super) fn new(connect_timeout: Duration, request_timeout: Duration) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    client: build_client(connect_timeout, request_timeout)?,
                    client_id: 0,
                }),
                connect_timeout,
                request_timeout,
            }),
        })
    }

    pub(super) fn snapshot(&self) -> (reqwest::Client, u64) {
        let state = self.lock();
        (state.client.clone(), state.client_id)
    }

    pub(super) fn record_error(&self, client_id: u64, error: &reqwest::Error) -> Result<()> {
        if !error.is_timeout() {
            return Ok(());
        }
        let replacement = build_client(self.inner.connect_timeout, self.inner.request_timeout)
            .context("failed to rebuild JSON-RPC HTTP client")?;
        let mut state = self.lock();
        if state.client_id == client_id {
            state.client = replacement;
            state.client_id = state.client_id.saturating_add(1);
        }
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
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
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .build()
        .context("failed to build JSON-RPC HTTP client")
}
