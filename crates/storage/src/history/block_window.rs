use anyhow::{Context, Result};
use sqlx::{PgConnection, types::time::OffsetDateTime};

use super::ChainBlockRange;

/// Resolve an inclusive timestamp window to per-chain inclusive block ranges
/// using readable `chain_lineage` rows only; no RPC is involved.
///
/// `from` maps to the first readable block whose timestamp is at or after it and
/// `to` maps to the last readable block whose timestamp is at or before it. A
/// chain with no block satisfying a supplied bound is omitted, so its rows are
/// excluded by the resulting window. Block timestamps are monotonic per chain,
/// so each bound is one ordered index probe.
pub async fn resolve_chain_block_ranges(
    db: impl Into<crate::ReadDb<'_>>,
    chain_ids: &[&str],
    from: Option<OffsetDateTime>,
    to: Option<OffsetDateTime>,
) -> Result<Vec<ChainBlockRange>> {
    let mut connection = db.into().acquire().await?;
    let mut ranges = Vec::with_capacity(chain_ids.len());
    for chain_id in chain_ids {
        let from_block = match from {
            Some(from) => {
                match first_readable_block_at_or_after(&mut connection, chain_id, from).await? {
                    Some(block_number) => Some(block_number),
                    None => continue,
                }
            }
            None => None,
        };
        let to_block = match to {
            Some(to) => {
                match last_readable_block_at_or_before(&mut connection, chain_id, to).await? {
                    Some(block_number) => Some(block_number),
                    None => continue,
                }
            }
            None => None,
        };
        if matches!((from_block, to_block), (Some(from), Some(to)) if from > to) {
            continue;
        }
        ranges.push(ChainBlockRange {
            chain_id: (*chain_id).to_owned(),
            from_block,
            to_block,
        });
    }
    Ok(ranges)
}

async fn first_readable_block_at_or_after(
    connection: &mut PgConnection,
    chain_id: &str,
    at: OffsetDateTime,
) -> Result<Option<i64>> {
    let Some(parameter) = microsecond_bound(at, true) else {
        return Ok(None);
    };
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT block_number
        FROM bigname_phase.chain_lineage
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND block_timestamp >= $2
        ORDER BY block_timestamp ASC, block_number ASC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(parameter)
    .fetch_optional(connection)
    .await
    .with_context(|| format!("failed to resolve the first block at or after {at} on {chain_id}"))
}

async fn last_readable_block_at_or_before(
    connection: &mut PgConnection,
    chain_id: &str,
    at: OffsetDateTime,
) -> Result<Option<i64>> {
    let Some(parameter) = microsecond_bound(at, false) else {
        return Ok(None);
    };
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT block_number
        FROM bigname_phase.chain_lineage
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND block_timestamp <= $2
        ORDER BY block_timestamp DESC, block_number DESC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(parameter)
    .fetch_optional(connection)
    .await
    .with_context(|| format!("failed to resolve the last block at or before {at} on {chain_id}"))
}

// PostgreSQL stores microseconds, while accepted clock bounds retain nanoseconds. SQLx's
// implicit truncation would admit an earlier block at a precise inclusive lower bound.
// Round only the SQL parameter inward; request and cursor identities keep the original instant.
fn microsecond_bound(at: OffsetDateTime, round_up: bool) -> Option<OffsetDateTime> {
    // PostgreSQL's finite minimum is later than the time crate's minimum. An upper
    // bound before it is empty; a lower bound must still admit every finite SQL instant.
    const POSTGRES_MIN_NANOS: i128 = -210_866_803_200_000_000_000;
    let nanos = at.unix_timestamp_nanos();
    if nanos < POSTGRES_MIN_NANOS && !round_up {
        return None;
    }
    let nanos = nanos.max(POSTGRES_MIN_NANOS);
    let floor = nanos.checked_sub(nanos.rem_euclid(1_000))?;
    let rounded = if round_up && floor != nanos {
        floor.checked_add(1_000)?
    } else {
        floor
    };
    OffsetDateTime::from_unix_timestamp_nanos(rounded).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::time::PrimitiveDateTime;

    #[test]
    fn precise_bounds_round_inward_on_both_sides_of_the_database_epoch() {
        for nanos in [1_700_000_201_000_000_001, 946_684_799_999_999_999, -1] {
            let at = OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap();
            let lower = microsecond_bound(at, true).unwrap().unix_timestamp_nanos();
            let upper = microsecond_bound(at, false).unwrap().unix_timestamp_nanos();
            assert!(lower >= nanos && lower - nanos < 1_000);
            assert!(upper <= nanos && nanos - upper < 1_000);
            assert_eq!((lower.rem_euclid(1_000), upper.rem_euclid(1_000)), (0, 0));
        }
    }

    #[test]
    fn microsecond_bounds_handle_supported_clock_endpoints_without_overflow() {
        let first = PrimitiveDateTime::MIN.assume_utc();
        let database_first = OffsetDateTime::from_unix_timestamp(-210_866_803_200).unwrap();
        assert_eq!(microsecond_bound(first, true), Some(database_first));
        assert_eq!(microsecond_bound(first, false), None);
        let minimum = database_first.unix_timestamp_nanos();
        for delta in [-1, 0, 1] {
            let at = OffsetDateTime::from_unix_timestamp_nanos(minimum + delta).unwrap();
            assert_eq!(
                microsecond_bound(at, true).map(|value| value.unix_timestamp_nanos()),
                Some(minimum + if delta > 0 { 1_000 } else { 0 }),
            );
            assert_eq!(
                microsecond_bound(at, false).map(|value| value.unix_timestamp_nanos()),
                (delta >= 0).then_some(minimum),
            );
        }
        let last = PrimitiveDateTime::MAX.assume_utc();
        assert_eq!(microsecond_bound(last, true), None);
        let last_microsecond = microsecond_bound(last, false).unwrap();
        assert_eq!(
            last.unix_timestamp_nanos() - last_microsecond.unix_timestamp_nanos(),
            999
        );
        assert_eq!(
            microsecond_bound(last_microsecond, true),
            Some(last_microsecond)
        );
    }
}
