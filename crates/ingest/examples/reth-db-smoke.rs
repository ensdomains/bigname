use anyhow::{Context, Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !(3..=4).contains(&args.len()) {
        bail!(
            "usage: reth-db-smoke <ethereum-mainnet|ethereum-sepolia> <datadir> <block,block,...> [topic0,topic0,...]"
        );
    }
    let numbers = args[2]
        .split(',')
        .map(|number| number.parse().context("invalid block number"))
        .collect::<Result<Vec<i64>>>()?;
    let topics = args.get(3).map_or_else(Vec::new, |topics| {
        topics.split(',').map(str::to_owned).collect()
    });
    let sample = bigname_ingest::read_reth_sample(&args[0], &args[1], &numbers, &topics).await?;
    println!("{}", serde_json::to_string(&sample)?);
    Ok(())
}
