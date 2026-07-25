use anyhow::{Context, Result};
use sqlx::PgConnection;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RawInputFence {
    Current,
    Drifted,
}

pub(super) async fn fence_raw_input(
    connection: &mut PgConnection,
    chain: &str,
    expected_revision: i64,
    expected_generation: i64,
) -> Result<RawInputFence> {
    let observed = match sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT revision, retention_generation
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        FOR SHARE
        "#,
    )
    .bind(chain)
    .fetch_optional(connection)
    .await
    {
        Ok(observed) => observed.unwrap_or_default(),
        Err(error) if super::is_serialization_failure(&error) => {
            return Ok(RawInputFence::Drifted);
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to fence raw-log input version for stored-lineage coverage publication on {chain}"
                )
            });
        }
    };
    Ok(if observed == (expected_revision, expected_generation) {
        RawInputFence::Current
    } else {
        RawInputFence::Drifted
    })
}
