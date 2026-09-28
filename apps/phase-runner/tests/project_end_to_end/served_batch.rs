//! Read family-block timings from the real runner metrics endpoint.
use anyhow::{Context, Result, ensure};
use std::{io::Read, io::Write, net::SocketAddr};
/// `phase_runner_project_family_block_seconds`, the wall time of each family block the batch
/// applied in a transaction of its own, read from the runner's own metrics endpoint. Cumulative;
/// [`FamilyBlockSeconds::line`] prints what one target added.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FamilyBlockSeconds {
    pub count: f64,
    pub sum: f64,
}

impl FamilyBlockSeconds {
    /// Scrapes until the histogram has more than `after.count` samples, or three seconds pass.
    pub async fn scrape(
        address: SocketAddr,
        chain: &str,
        after: FamilyBlockSeconds,
    ) -> Result<FamilyBlockSeconds> {
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
            let read = FamilyBlockSeconds {
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

    pub fn line(&self, before: FamilyBlockSeconds, target: i64) -> String {
        let blocks = self.count - before.count;
        let seconds = self.sum - before.sum;
        format!(
            "SEPOLIA_END_TO_END_FAMILY_BLOCKS target={target} metric=phase_runner_project_family_block_seconds \
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
