#[allow(dead_code)]
mod support;

use anyhow::Result;
use phase_runner::{
    error::ErrorKind,
    phase::{PhaseName, RunMode},
    state::PhaseStore,
};
use support::{ScratchDatabase, assert_connection_hash_stamp};

#[tokio::test]
async fn retained_blue_brain_checkpoint_resumes_only_with_the_reviewed_build() -> Result<()> {
    let scratch = ScratchDatabase::create("blue_brain_compatibility").await?;
    let store = PhaseStore::new(scratch.pool().clone());
    let chain = "ethereum-mainnet";
    let retained = "keccak256:e292847c25244de4a800c580f291fca2d2259b9ac6915fb828a390032c6a7f43";
    store.initialize_chain(chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET phase_status='completed', started_at=now(),
         finished_at=now(), current_block_number=25967798, current_block_hash='target',
         target_block_number=25967798, target_block_hash='target',
         live_handoff_block_number=25967798, live_handoff_block_hash='target'
         WHERE chain_id=$1 AND phase_name='ingest'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET phase_status='running', started_at=now(),
         current_block_number=14684499, current_block_hash='checkpoint',
         target_block_number=25967798, target_block_hash='target', input_content_hash=$2
         WHERE chain_id=$1 AND phase_name='interpret'",
    )
    .bind(chain)
    .bind(retained)
    .execute(scratch.pool())
    .await?;
    let result = store
        .start_phase(chain, PhaseName::Interpret, &RunMode::Normal)
        .await;
    if bigname_content_hash::INTERPRETER_COMPATIBILITY_EXCEPTION.is_some() {
        result?;
        assert_eq!(bigname_content_hash::INTERPRETER_CONTENT_HASH, retained);
        assert_connection_hash_stamp(&scratch.runner()).await?;
    } else {
        assert_eq!(
            result
                .expect_err("ordinary build must require replay")
                .kind(),
            ErrorKind::ContentHashMismatch
        );
    }
    let state: (i64, String, i64, String, String, bool, Option<i64>) = sqlx::query_as(
        "SELECT current_block_number,current_block_hash,target_block_number,target_block_hash,
         input_content_hash,redo_in_progress,redo_current_block_number
         FROM chain_phase_state WHERE chain_id=$1 AND phase_name='interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            14684499,
            "checkpoint".into(),
            25967798,
            "target".into(),
            retained.into(),
            false,
            None
        )
    );
    scratch.cleanup().await
}
