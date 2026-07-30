use anyhow::{Context, Result};
use sqlx::{Postgres, QueryBuilder};

use super::OrphanedLineageConflict;
use crate::lineage::{CanonicalityState, ChainLineageBlock};

pub(super) async fn advance_existing_lineage_chunk_through_adjacent_states(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chunk: &[ChainLineageBlock],
    orphaned_conflict: OrphanedLineageConflict,
) -> Result<()> {
    let readable_targets = [
        CanonicalityState::Observed,
        CanonicalityState::Canonical,
        CanonicalityState::Safe,
        CanonicalityState::Finalized,
    ];
    if matches!(orphaned_conflict, OrphanedLineageConflict::Recanonicalize) {
        transition_existing_lineage_chunk(
            transaction,
            chunk,
            CanonicalityState::Orphaned,
            CanonicalityState::Canonical,
            &readable_targets,
        )
        .await?;
    }

    transition_existing_lineage_chunk(
        transaction,
        chunk,
        CanonicalityState::Observed,
        CanonicalityState::Canonical,
        &[
            CanonicalityState::Canonical,
            CanonicalityState::Safe,
            CanonicalityState::Finalized,
        ],
    )
    .await?;
    transition_existing_lineage_chunk(
        transaction,
        chunk,
        CanonicalityState::Canonical,
        CanonicalityState::Safe,
        &[CanonicalityState::Safe, CanonicalityState::Finalized],
    )
    .await?;
    transition_existing_lineage_chunk(
        transaction,
        chunk,
        CanonicalityState::Safe,
        CanonicalityState::Finalized,
        &[CanonicalityState::Finalized],
    )
    .await
}

async fn transition_existing_lineage_chunk(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chunk: &[ChainLineageBlock],
    from_state: CanonicalityState,
    to_state: CanonicalityState,
    eligible_targets: &[CanonicalityState],
) -> Result<()> {
    let target_states = eligible_targets
        .iter()
        .map(|state| state.as_str().to_owned())
        .collect::<Vec<_>>();
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        WITH input (
            chain_id,
            block_hash,
            target_state
        ) AS (
        "#,
    );
    builder.push_values(chunk, |mut row, block| {
        row.push_bind(&block.chain_id)
            .push_bind(&block.block_hash)
            .push_bind(block.canonicality_state.as_str());
    });
    builder
        .push(
            r#"
        )
        UPDATE chain_lineage AS lineage
        SET canonicality_state =
        "#,
        )
        .push_bind(to_state.as_str())
        .push(
            r#"::canonicality_state,
            observed_at = now()
        FROM input
        WHERE lineage.chain_id = input.chain_id
          AND lineage.block_hash = input.block_hash
          AND lineage.canonicality_state =
        "#,
        )
        .push_bind(from_state.as_str())
        .push(
            r#"::canonicality_state
          AND input.target_state = ANY(
        "#,
        )
        .push_bind(target_states)
        .push("::TEXT[])");

    builder
        .build()
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to advance chain lineage from {} to {}",
                from_state.as_str(),
                to_state.as_str()
            )
        })?;
    Ok(())
}
