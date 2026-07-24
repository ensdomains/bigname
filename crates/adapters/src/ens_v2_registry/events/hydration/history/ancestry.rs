use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, QueryBuilder};

#[derive(Clone, Copy)]
pub(super) struct SelectedPathAnchor<'a> {
    pub(super) request_index: i64,
    pub(super) chain_id: &'a str,
    pub(super) block_number: i64,
    pub(super) block_hash: &'a str,
}

pub(super) async fn ensure_selected_paths_reach_stable_boundaries(
    pool: &PgPool,
    hydration_kind: &str,
    anchors: &[SelectedPathAnchor<'_>],
) -> Result<()> {
    if anchors.is_empty() {
        return Ok(());
    }

    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        WITH RECURSIVE requested (
            request_index,
            chain_id,
            block_number,
            block_hash
        ) AS (
        "#,
    );
    query.push_values(anchors, |mut row, anchor| {
        row.push_bind(anchor.request_index)
            .push_bind(anchor.chain_id)
            .push_bind(anchor.block_number)
            .push_bind(anchor.block_hash);
    });
    query.push(
        r#"
        ), selected_tail AS (
            SELECT
                requested.request_index,
                lineage.chain_id,
                lineage.block_number,
                lineage.block_hash,
                lineage.parent_hash,
                lineage.canonicality_state
            FROM requested
            JOIN chain_lineage lineage
              ON lineage.chain_id = requested.chain_id
             AND lineage.block_number = requested.block_number
             AND lineage.block_hash = requested.block_hash
             AND lineage.canonicality_state <> 'orphaned'::canonicality_state

            UNION ALL

            SELECT
                child.request_index,
                parent.chain_id,
                parent.block_number,
                parent.block_hash,
                parent.parent_hash,
                parent.canonicality_state
            FROM selected_tail child
            JOIN chain_lineage parent
              ON parent.chain_id = child.chain_id
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
             AND parent.canonicality_state <> 'orphaned'::canonicality_state
            WHERE child.canonicality_state NOT IN (
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
        )
        SELECT
            requested.request_index,
            requested.chain_id,
            requested.block_number,
            requested.block_hash
        FROM requested
        WHERE NOT EXISTS (
            SELECT 1
            FROM selected_tail
            WHERE selected_tail.request_index = requested.request_index
              AND selected_tail.canonicality_state IN (
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        ORDER BY requested.request_index
        LIMIT 1
        "#,
    );

    let missing = query
        .build_query_as::<(i64, String, i64, String)>()
        .fetch_optional(pool)
        .await
        .with_context(|| format!("failed to validate selected ENSv2 {hydration_kind} ancestry"))?;
    if let Some((request_index, chain_id, block_number, block_hash)) = missing {
        bail!(
            "ENSv2 {hydration_kind} cannot prove selected-path ancestry for request \
             {request_index} from block {block_number} ({block_hash}) to a safe or finalized \
             boundary on {chain_id}"
        );
    }
    Ok(())
}
