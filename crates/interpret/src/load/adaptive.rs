use sqlx::PgPool;

use crate::{InterpretError, Result, engine::adaptive::BlockWork};

/// Load bounded metadata for the same canonical raw facts as `load_raw_logs`.
///
/// Materializing the limited lineage prefix keeps each lateral raw-log aggregate
/// constrained to one selected block, using raw_logs' chain/block indexes. Receipts
/// are not joined: the production input loader reads raw logs directly as well.
/// The byte estimate allows fixed structure/allocator overhead and twice the actual
/// string/buffer payload. It estimates pending loaded input, not total process RSS.
pub(crate) async fn block_work(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    max_blocks: i64,
) -> Result<Vec<BlockWork>> {
    if max_blocks <= 0 || from_block > to_block {
        return Ok(Vec::new());
    }
    let to_block = to_block.min(from_block.saturating_add(max_blocks - 1));
    let rows: Vec<(i64, String, i64, i64)> = sqlx::query_as(
        "
        WITH bounded_lineage AS MATERIALIZED (
            SELECT chain_id, block_number, block_hash
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_number BETWEEN $2 AND $3
              AND canonicality_state IN ('canonical', 'safe', 'finalized')
            ORDER BY block_number, block_hash
            LIMIT $4
        )
        SELECT lineage.block_number,
               lineage.block_hash,
               work.raw_logs,
               (256::numeric
                + 2 * (octet_length(lineage.chain_id)::numeric
                       + octet_length(lineage.block_hash)::numeric)
                + work.input_bytes)::bigint AS estimated_input_bytes
        FROM bounded_lineage lineage
        CROSS JOIN LATERAL (
            SELECT count(*)::bigint AS raw_logs,
                   COALESCE(sum(
                       512::numeric
                       + 2 * (octet_length(raw.chain_id)::numeric
                              + octet_length(raw.block_hash)::numeric
                              + octet_length(raw.transaction_hash)::numeric
                              + octet_length(raw.emitting_address)::numeric
                              + octet_length(raw.data)::numeric)
                       + COALESCE((
                           SELECT sum(32::numeric + 2 * octet_length(topic)::numeric)
                           FROM unnest(raw.topics) topic
                       ), 0)
                   ), 0) AS input_bytes
            FROM raw_logs raw
            WHERE raw.chain_id = lineage.chain_id
              AND raw.block_hash = lineage.block_hash
              AND raw.block_number = lineage.block_number
        ) work
        ORDER BY lineage.block_number, lineage.block_hash
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .bind(max_blocks)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load adaptive block work", error))?;
    super::require_unique_live_heights(
        chain_id,
        rows.iter()
            .map(|(number, hash, _, _)| (*number, hash.as_str())),
    )?;
    rows.into_iter()
        .map(|(number, hash, raw_logs, estimated_input_bytes)| {
            Ok(BlockWork {
                number,
                hash,
                raw_logs: nonnegative(raw_logs)?,
                estimated_input_bytes: nonnegative(estimated_input_bytes)?,
            })
        })
        .collect()
}

fn nonnegative(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        InterpretError::data_integrity("adaptive block work cannot have a negative estimate")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    #[test]
    fn estimate_conversion_rejects_negative_values() {
        assert_eq!(nonnegative(0).unwrap(), 0);
        assert_eq!(nonnegative(i64::MAX).unwrap(), i64::MAX as u64);
        assert!(nonnegative(-1).is_err());
    }

    #[tokio::test]
    async fn bounded_work_matches_canonical_raw_input_including_empty_blocks() -> anyhow::Result<()>
    {
        let db = TestDatabase::create(TestDatabaseConfig::new("adaptive_block_work")).await?;
        for schema in [
            include_str!("../../../storage/schema/baseline/01_chain.sql"),
            include_str!("../../../storage/schema/baseline/02_raw_facts.sql"),
        ] {
            sqlx::raw_sql(schema).execute(db.pool()).await?;
        }
        sqlx::raw_sql(
            "
            INSERT INTO chain_lineage
                (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
            VALUES
                ('work-test', 'one', 1, to_timestamp(1), 'canonical'),
                ('work-test', 'two', 2, to_timestamp(2), 'safe'),
                ('work-test', 'three', 3, to_timestamp(3), 'finalized'),
                ('work-test', 'hidden', 4, to_timestamp(4), 'observed'),
                ('work-test', 'fork', 5, to_timestamp(5), 'orphaned'),
                ('work-test', 'five', 5, to_timestamp(5), 'canonical'),
                ('other-test', 'other', 1, to_timestamp(1), 'canonical');
            INSERT INTO raw_transactions
                (chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, from_address)
            SELECT chain_id, block_hash, block_number, 'tx-' || block_hash, 0, 'sender'
            FROM chain_lineage;
            INSERT INTO raw_logs
                (chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, log_index, emitting_address, topics, data)
            SELECT chain_id, block_hash, block_number, transaction_hash,
                   0, n, 'emitter', ARRAY['topic-one', 'topic-two'],
                   decode(repeat('01', CASE WHEN block_hash = 'fork' THEN 10000 ELSE 16 END), 'hex')
            FROM raw_transactions
            CROSS JOIN LATERAL generate_series(1, CASE
                WHEN block_hash = 'three' THEN 0
                WHEN block_hash = 'two' THEN 2
                ELSE 1
            END) n;
            ",
        )
        .execute(db.pool())
        .await?;
        let limited = block_work(db.pool(), "work-test", 1, 5, 3).await?;
        assert_eq!(
            limited.iter().map(|block| block.number).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            limited
                .iter()
                .map(|block| block.raw_logs)
                .collect::<Vec<_>>(),
            vec![1, 2, 0]
        );
        assert!(limited[1].estimated_input_bytes > limited[0].estimated_input_bytes);
        assert!(limited[2].estimated_input_bytes > 0);

        let all = block_work(db.pool(), "work-test", 2, 5, 10).await?;
        assert_eq!(
            all.iter()
                .map(|block| (block.number, block.hash.as_str()))
                .collect::<Vec<_>>(),
            vec![(2, "two"), (3, "three"), (5, "five")]
        );
        let mut connection = db.pool().acquire().await?;
        let raw = super::super::load_raw_logs(&mut connection, "work-test", 2, 5).await?;
        drop(connection);
        for block in all {
            assert_eq!(
                block.raw_logs,
                raw.iter()
                    .filter(|log| log.block_hash == block.hash)
                    .count() as u64
            );
        }
        assert!(
            block_work(db.pool(), "work-test", 6, 5, 10)
                .await?
                .is_empty()
        );
        assert!(
            block_work(db.pool(), "work-test", 1, 5, 0)
                .await?
                .is_empty()
        );
        db.cleanup().await?;
        Ok(())
    }
}
