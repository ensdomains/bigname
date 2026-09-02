#[allow(dead_code)]
mod support;

use anyhow::Result;
use phase_runner::{
    heads::BlockMarker,
    phase::{PhaseName, PhaseProgress, RunMode},
    state::PhaseStore,
};

use support::ScratchDatabase;

type StoredProgress = (Option<i64>, Option<String>, Option<i64>, Option<String>);

#[tokio::test]
async fn normal_progress_without_current_preserves_durable_cursor() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_normal_cursor_preservation").await?;
    let chain_id = "normal-cursor-preservation-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;

    record_interpret_progress(&store, chain_id, Some(marker(10)?), marker(20)?).await?;
    record_interpret_progress(&store, chain_id, None, marker(30)?).await?;

    assert_eq!(
        stored_progress(&scratch, chain_id).await?,
        (
            Some(10),
            Some("normal-cursor-block-10".to_owned()),
            Some(30),
            Some("normal-cursor-block-30".to_owned()),
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn normal_progress_with_current_replaces_durable_cursor_as_a_pair() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_normal_cursor_replacement").await?;
    let chain_id = "normal-cursor-replacement-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;

    record_interpret_progress(&store, chain_id, Some(marker(10)?), marker(20)?).await?;
    record_interpret_progress(&store, chain_id, Some(marker(15)?), marker(30)?).await?;

    assert_eq!(
        stored_progress(&scratch, chain_id).await?,
        (
            Some(15),
            Some("normal-cursor-block-15".to_owned()),
            Some(30),
            Some("normal-cursor-block-30".to_owned()),
        )
    );
    scratch.cleanup().await
}

async fn record_interpret_progress(
    store: &PhaseStore,
    chain_id: &str,
    current: Option<BlockMarker>,
    target: BlockMarker,
) -> Result<()> {
    store
        .record_progress(
            chain_id,
            PhaseName::Interpret,
            &RunMode::Normal,
            None,
            &PhaseProgress {
                current,
                target: Some(target),
                ..PhaseProgress::default()
            },
        )
        .await?;
    Ok(())
}

async fn stored_progress(scratch: &ScratchDatabase, chain_id: &str) -> Result<StoredProgress> {
    Ok(sqlx::query_as(
        "
        SELECT current_block_number,
               current_block_hash,
               target_block_number,
               target_block_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?)
}

fn marker(number: i64) -> Result<BlockMarker> {
    Ok(BlockMarker::new(
        number,
        format!("normal-cursor-block-{number}"),
    )?)
}
