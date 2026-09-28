//! The comparison side of the harness under the publication switch (TYR-36 step 7b). With the
//! switch on the runner's Project batch is the owned key family loop and no longer publishes the
//! served tables, but the shadow comparisons still compare the family readers with the served
//! tables. So the harness publishes the served tables itself, outside the served clock, by
//! driving the served engine and hydrator directly (`bigname_project::Engine::run_batch` and
//! `Hydrator`), exactly as the switch-off batch does. With the switch off the runner's batch
//! publishes them and nothing here runs.
use std::{io::Read, io::Write, net::SocketAddr};

use anyhow::{Context, Result, anyhow, ensure};
use bigname_lookup::ChainRpcUrls;
use bigname_project::{BatchRequest, Engine, Hydrator, Marker, RunMode as ProjectRunMode};
use phase_runner::heads::BlockMarker;
use sqlx::PgPool;

pub struct ServedBatch {
    engine: Engine,
    hydrator: Hydrator,
}

impl ServedBatch {
    pub fn new(pool: &PgPool) -> Self {
        Self {
            engine: Engine::new(pool.clone()),
            hydrator: Hydrator::new(pool.clone(), ChainRpcUrls::default()),
        }
    }

    /// Publishes the served tables at `target` from `resume` (a full rebuild without one), the
    /// switch-off batch's engine call and hydration.
    pub async fn publish(
        &self,
        chain_id: &str,
        target: &BlockMarker,
        resume: Option<&BlockMarker>,
    ) -> Result<()> {
        self.hydrator
            .require_rpc_configuration(chain_id)
            .map_err(|error| anyhow!("{error}"))?;
        let outcome = self
            .engine
            .run_batch(BatchRequest {
                chain_id: chain_id.to_owned(),
                target_block: target.number,
                affected_from_block: resume
                    .map_or(0, |resume| (resume.number + 1).min(target.number)),
                affected_to_block: target.number,
                resume_current: resume.map(|resume| Marker {
                    number: resume.number,
                    hash: resume.hash.clone(),
                }),
                mode: ProjectRunMode::Normal,
            })
            .await
            .map_err(|error| anyhow!("the served comparison batch failed: {error}"))?;
        self.hydrator
            .hydrate_if_canonical_head(chain_id, &outcome.current)
            .await
            .map_err(|error| anyhow!("the served comparison hydration failed: {error}"))?;
        Ok(())
    }
}

/// The D3 measure (TYR-36 step 7a-1): `phase_runner_project_family_block_seconds`, the wall time
/// of each family block the batch applied in a transaction of its own, read from the runner's own
/// metrics endpoint. Cumulative; [`D3::line`] prints what one target added.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct D3 {
    pub count: f64,
    pub sum: f64,
}

impl D3 {
    /// Scrapes until the histogram has more than `after.count` samples, or three seconds pass.
    pub async fn scrape(address: SocketAddr, chain: &str, after: D3) -> Result<D3> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let body = tokio::task::spawn_blocking(move || get(address))
                .await
                .context("the metrics scrape panicked")??;
            let label = format!("chain=\"{chain}\"");
            let sample = |name: &str| -> Option<f64> {
                body.lines()
                    .find(|line| line.starts_with(name) && line.contains(&label))
                    .and_then(|line| line.rsplit_once(' '))
                    .and_then(|(_, value)| value.parse().ok())
            };
            let read = D3 {
                count: sample("phase_runner_project_family_block_seconds_count").unwrap_or(0.0),
                sum: sample("phase_runner_project_family_block_seconds_sum").unwrap_or(0.0),
            };
            if read.count > after.count {
                return Ok(read);
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "phase_runner_project_family_block_seconds observed no family block for {chain}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    pub fn line(&self, before: D3, target: i64) -> String {
        let blocks = self.count - before.count;
        let seconds = self.sum - before.sum;
        format!(
            "SEPOLIA_END_TO_END_D3 target={target} metric=phase_runner_project_family_block_seconds \
             blocks={blocks} sum_ms={:.1} mean_ms={:.1}",
            seconds * 1_000.0,
            if blocks > 0.0 {
                seconds * 1_000.0 / blocks
            } else {
                0.0
            }
        )
    }
}

fn get(address: SocketAddr) -> Result<String> {
    let mut stream = std::net::TcpStream::connect(address)?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .context("the metrics response has no header boundary")?;
    ensure!(head.starts_with("HTTP/1.1 200"), "{head}");
    Ok(body.to_owned())
}
