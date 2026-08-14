use bigname_adapters::schema_v2::seam::REDO_RESOLVER_EVIDENCE_SELECT_SQL;
use sqlx::{Postgres, Transaction};

use crate::Result;

pub(super) async fn capture_resolver_evidence(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let statement = format!(
        r#"INSERT INTO project_redo_resolver_evidence (
            chain_id, event_identity, block_number, event_kind,
            source_family, resource_id,
            before_resolver_address, after_resolver_address
        )
        {REDO_RESOLVER_EVIDENCE_SELECT_SQL}
        ON CONFLICT (chain_id, event_identity) DO NOTHING"#,
    );
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            crate::InterpretError::database(
                "failed to stage resolver evidence for Project redo",
                error,
            )
        })?;
    Ok(())
}
