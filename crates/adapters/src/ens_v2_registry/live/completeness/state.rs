use anyhow::{Context, Result, ensure};
use bigname_domain::block_interval::{InclusiveBlockInterval, coalesce_inclusive_block_intervals};
use sqlx::PgPool;

#[derive(Clone, Copy)]
pub(super) struct RetainedHistoryState {
    pub(super) retention_generation: i64,
    pub(super) retained_history_complete: bool,
    pub(super) proven_retention_generation: Option<i64>,
    pub(super) proven_discovery_admission_epoch: Option<i64>,
    pub(super) proven_through_block: Option<i64>,
}

pub(super) async fn load_locked_retained_history_state(
    connection: &mut sqlx::PgConnection,
    chain: &str,
) -> Result<RetainedHistoryState> {
    let state = sqlx::query_as::<_, (i64, bool, Option<i64>, Option<i64>, Option<i64>)>(
        r#"
        SELECT
            retention_generation,
            retained_history_complete,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        FOR UPDATE
        "#,
    )
    .bind(chain)
    .fetch_optional(connection)
    .await
    .with_context(|| format!("failed to lock raw-log retained-history state for {chain}"))?
    .with_context(|| {
        format!(
            "raw-log retained-history state is absent for {chain}; run generation-bound bootstrap coverage before full-source reconciliation"
        )
    })?;
    Ok(RetainedHistoryState {
        retention_generation: state.0,
        retained_history_complete: state.1,
        proven_retention_generation: state.2,
        proven_discovery_admission_epoch: state.3,
        proven_through_block: state.4,
    })
}

pub(super) async fn ensure_discovery_epoch_row(pool: &PgPool, chain: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ($1, 0)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await
    .with_context(|| format!("failed to establish discovery admission epoch fence for {chain}"))?;
    Ok(())
}

pub(super) async fn ensure_retained_history_state_row(pool: &PgPool, chain: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since
        )
        VALUES ($1, 0, 0, false, clock_timestamp())
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await
    .with_context(|| format!("failed to establish raw-log retained-history state for {chain}"))?;
    Ok(())
}

pub(super) async fn load_selected_live_block_intervals(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    selected_block_hashes: &[String],
) -> Result<Vec<(i64, i64)>> {
    ensure!(
        !selected_block_hashes.is_empty(),
        "ENSv2 live retained-history proof requires at least one selected block hash"
    );
    let mut unique_hashes = selected_block_hashes.to_vec();
    unique_hashes.sort();
    unique_hashes.dedup();
    let rows = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain)
    .bind(&unique_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load exact live coverage blocks for {chain}"))?;
    ensure!(
        rows.len() == unique_hashes.len(),
        "ENSv2 live retained-history proof selected {} block hashes on {chain}, but only {} are retained as non-orphaned lineage",
        unique_hashes.len(),
        rows.len()
    );
    ensure!(
        rows.last()
            .is_some_and(|(block_number, _)| *block_number == through_block),
        "ENSv2 live retained-history proof selection on {chain} does not include its target block {through_block}"
    );

    let mut block_numbers = Vec::with_capacity(rows.len());
    for (block_number, _) in rows {
        ensure!(
            block_numbers.last().copied() != Some(block_number),
            "ENSv2 live retained-history proof selected multiple non-orphaned hashes at block {block_number} on {chain}"
        );
        block_numbers.push(block_number);
    }
    Ok(coalesced_block_number_intervals(block_numbers))
}

fn coalesced_block_number_intervals(
    block_numbers: impl IntoIterator<Item = i64>,
) -> Vec<(i64, i64)> {
    coalesce_inclusive_block_intervals(block_numbers.into_iter().map(|block_number| {
        InclusiveBlockInterval::new(block_number, block_number)
            .expect("single-block interval must not be inverted")
    }))
    .into_iter()
    .map(|interval| (interval.from_block(), interval.through_block()))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_block_numbers_coalesce_in_order_without_crossing_gaps() {
        assert_eq!(
            coalesced_block_number_intervals([2, 3, 4, 8, 9, 10]),
            vec![(2, 4), (8, 10)]
        );
    }

    #[test]
    fn selected_terminal_block_numbers_coalesce_without_overflow() {
        assert_eq!(
            coalesced_block_number_intervals([i64::MAX - 2, i64::MAX - 1, i64::MAX]),
            vec![(i64::MAX - 2, i64::MAX)]
        );
    }
}
