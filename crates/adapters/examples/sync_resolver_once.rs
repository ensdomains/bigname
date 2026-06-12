//! One-off ops helper: re-run the ENSv2 resolver adapter over the full raw-log corpus for a
//! chain. Normalized-event upserts are identity-keyed and idempotent, so existing events are
//! untouched and only previously-skipped logs produce inserts. Used to re-derive record events
//! after the emitter-interval merge fix (the full `replay normalized-events` path is blocked by a
//! pre-existing registry replay nondeterminism on `SubregistryChanged.to_contract_instance_id`).
//!
//! Usage: BIGNAME_DATABASE_URL=… cargo run -p bigname-adapters --example sync_resolver_once -- ethereum-sepolia

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let chain = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ethereum-sepolia".to_owned());
    let database_url = std::env::var("BIGNAME_DATABASE_URL")?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    let summary = bigname_adapters::sync_ens_v2_resolver(&pool, &chain).await?;
    println!(
        "scanned={} matched={} synced={} inserted={}",
        summary.scanned_log_count,
        summary.matched_log_count,
        summary.total_synced_count,
        summary.total_inserted_count
    );
    for (kind, counts) in &summary.by_kind {
        println!(
            "  {kind}: synced={} inserted={}",
            counts.synced_count, counts.inserted_count
        );
    }
    Ok(())
}
