#[allow(dead_code)]
mod support;
use anyhow::Result;
use phase_runner::{
    config::{SeedBasis, SourceConfig, SourceRole},
    phase::PhaseName,
    phase_lock::PhaseLock,
    source_transport::transition,
};
use support::ScratchDatabase;

#[tokio::test]
async fn active_writer_refuses_transport_change_before_provider_access() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_lock").await?;
    let old = SourceConfig::new_with_role(
        "ethereum-sepolia",
        "node",
        "drpc",
        SeedBasis::EthereumHead,
        0,
        SourceRole::Intake,
        "http://127.0.0.1:1",
    )?;
    let new = SourceConfig::new_with_role(
        "ethereum-sepolia",
        "node",
        "reth_db",
        SeedBasis::EthereumHead,
        0,
        SourceRole::Intake,
        "/missing/reth",
    )?;
    for phase in PhaseName::ALL {
        let lock =
            PhaseLock::acquire(db.writer_connect_options(), "ethereum-sepolia", phase).await?;
        let error = transition(&db.runner(), &old, &new).await.unwrap_err();
        assert!(
            error.to_string().contains("stop all phase writers"),
            "{error:#}"
        );
        lock.release().await?;
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ingest_cursors")
        .fetch_one(db.pool())
        .await?;
    assert_eq!(count, 0);
    db.cleanup().await
}
