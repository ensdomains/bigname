use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{PgConnection, PgPool, Row};
use tracing::warn;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StartupCanonicalLineageHead {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupAdapterLineageState {
    pub mutation_revision: i64,
    pub canonical_lineage_head: Option<StartupCanonicalLineageHead>,
}

pub async fn load_startup_adapter_lineage_state(
    pool: &PgPool,
    chain: &str,
) -> Result<Option<StartupAdapterLineageState>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start startup lineage-state transaction")?;
    lock_canonical_lineage(transaction.as_mut(), chain).await?;
    let state =
        load_startup_adapter_lineage_state_from_connection(transaction.as_mut(), chain).await?;
    transaction
        .commit()
        .await
        .context("failed to finish startup lineage-state transaction")?;
    Ok(state)
}

pub(super) async fn lock_canonical_lineage(
    connection: &mut PgConnection,
    chain: &str,
) -> Result<()> {
    // This short SHARE lock blocks lineage INSERT/UPDATE/DELETE while both the
    // mutation revision and head identity are read. Prepare and completion
    // take the same lock, so movement during a full scan becomes an
    // optimistic-key mismatch.
    sqlx::query("LOCK TABLE chain_lineage IN SHARE MODE")
        .execute(connection)
        .await
        .with_context(|| format!("failed to fence canonical lineage for {chain}"))?;
    Ok(())
}

pub(super) async fn load_startup_adapter_lineage_state_from_connection(
    connection: &mut PgConnection,
    chain: &str,
) -> Result<Option<StartupAdapterLineageState>> {
    let mutation_revision = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT revision
        FROM chain_lineage_mutation_revisions
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(&mut *connection)
    .await
    .with_context(|| format!("failed to load startup lineage mutation revision for {chain}"))?;
    let mutation_revision = match mutation_revision {
        Some(revision) => revision,
        None => {
            let lineage_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM chain_lineage WHERE chain_id = $1)",
            )
            .bind(chain)
            .fetch_one(&mut *connection)
            .await
            .with_context(|| format!("failed to inspect retained lineage for {chain}"))?;
            if lineage_exists {
                warn!(
                    service = "storage",
                    operation = "startup-adapter-sync",
                    chain,
                    "retained lineage has no mutation revision; the startup checkpoint key is \
                     unknown and the full adapter sync will run"
                );
                return Ok(None);
            }
            0
        }
    };

    let canonical_head = load_canonical_lineage_head(connection, chain).await?;
    let canonical_lineage_head = match canonical_head {
        None => None,
        Some((head, 1)) => Some(head),
        Some((_, same_height_count)) => {
            warn!(
                service = "storage",
                operation = "startup-adapter-sync",
                chain,
                same_height_count,
                "canonical lineage has multiple rows at its highest block; the startup \
                 checkpoint key is unknown and the full adapter sync will run"
            );
            return Ok(None);
        }
    };

    Ok(Some(StartupAdapterLineageState {
        mutation_revision,
        canonical_lineage_head,
    }))
}

async fn load_canonical_lineage_head(
    connection: &mut PgConnection,
    chain: &str,
) -> Result<Option<(StartupCanonicalLineageHead, i64)>> {
    let row = sqlx::query(
        r#"
        WITH canonical_state_heads AS MATERIALIZED (
            (
                SELECT block_number
                FROM chain_lineage
                WHERE chain_id = $1
                  AND canonicality_state = 'canonical'::canonicality_state
                ORDER BY block_number DESC
                LIMIT 1
            )
            UNION ALL
            (
                SELECT block_number
                FROM chain_lineage
                WHERE chain_id = $1
                  AND canonicality_state = 'safe'::canonicality_state
                ORDER BY block_number DESC
                LIMIT 1
            )
            UNION ALL
            (
                SELECT block_number
                FROM chain_lineage
                WHERE chain_id = $1
                  AND canonicality_state = 'finalized'::canonicality_state
                ORDER BY block_number DESC
                LIMIT 1
            )
        )
        SELECT block_number, block_hash, same_height_count
        FROM (
            SELECT
                block_number,
                block_hash,
                COUNT(*) OVER () AS same_height_count
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_number = (
                  SELECT MAX(block_number)
                  FROM canonical_state_heads
              )
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ) AS canonical_lineage
        ORDER BY block_hash
        LIMIT 1
        "#,
    )
    .bind(chain)
    .fetch_optional(connection)
    .await
    .with_context(|| format!("failed to load canonical lineage head for {chain}"))?;
    row.map(|row| {
        Ok((
            StartupCanonicalLineageHead {
                block_number: row.try_get("block_number")?,
                block_hash: row.try_get("block_hash")?,
            },
            row.try_get("same_height_count")?,
        ))
    })
    .transpose()
}
