//! F2b, the NameWrapper masks at the publication block's clock (TYR-36 D6): the served state and
//! fuses past the wrapper expiry, the owner lapse of an emancipated or locked name, and the
//! `.eth` grace period. The clock is the publication block's timestamp, passed in, never the
//! request time.
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;

use super::rows::WrapperRow;

/// The fuse the NameWrapper burns on a `.eth` second-level name (permissions.rs:25).
/// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L19 @ ens_v1@91c966f)
pub const IS_DOT_ETH: i64 = 131072;
/// The `.eth` registrar grace period in seconds (permissions.rs:25).
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f)
pub const GRACE_PERIOD_SECONDS: i128 = 7_776_000;

/// What the wrapper serves at one clock.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EffectiveWrapper {
    /// Null when the state, fuses or expiry is unknown, and for an emancipated or locked name
    /// past its wrapper expiry (build.sql:600-608, permissions.rs:319-327).
    pub wrapper_state: Option<String>,
    /// Zero past the wrapper expiry; null when the state, fuses or expiry is unknown
    /// (permissions.rs:311-318, resource_summary.rs:400-407).
    pub fuses: Option<i64>,
    /// Past its own expiry the NameWrapper reports no owner for an emancipated or locked name,
    /// and zero fuses for any name (build.sql:616-623).
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
    pub owner_lapsed: bool,
    /// A `.eth` name inside the grace period after its wrapper expiry (permissions.rs:329-335).
    pub in_grace: bool,
}

fn expiry(row: &WrapperRow) -> Option<i128> {
    row.expiry_seconds.as_deref()?.parse().ok()
}

/// The wrapper masks of one F2b row at `clock_seconds`.
pub fn effective_wrapper(row: &WrapperRow, clock_seconds: i64) -> EffectiveWrapper {
    let clock = i128::from(clock_seconds);
    let expiry = expiry(row);
    let expired = expiry.map(|expiry| expiry < clock);
    let known = row.wrapper_state.is_some() && row.fuses.is_some() && expiry.is_some();
    let sticky = matches!(row.wrapper_state.as_deref(), Some("emancipated" | "locked"));
    let wrapper_state = if !known || (expired == Some(true) && sticky) {
        None
    } else {
        row.wrapper_state.clone()
    };
    let fuses = if !known {
        None
    } else if expired == Some(true) {
        Some(0)
    } else {
        row.fuses
    };
    let owner_lapsed = sticky && row.fuses.is_some() && expired == Some(true);
    let in_grace = match (fuses, expiry) {
        (Some(fuses), Some(expiry)) => {
            fuses & IS_DOT_ETH != 0 && expiry - GRACE_PERIOD_SECONDS < clock
        }
        _ => false,
    };
    EffectiveWrapper {
        wrapper_state,
        fuses,
        owner_lapsed,
        in_grace,
    }
}

/// The wrapper expiry a wrapped name with no registrar lease serves: an integral word between
/// 1 and 253402300799 (build.sql:568-574).
pub fn servable_expiry(row: &WrapperRow) -> Option<i64> {
    expiry(row)
        .filter(|expiry| (1..=253_402_300_799).contains(expiry))
        .map(|expiry| expiry as i64)
}

/// The wrapper restriction block `permissions_current_resource_summary` serves for a wrapper
/// resource that is still wrapped (resource_summary.rs:306-315); `None` when the effective
/// state is null.
pub fn restrictions(row: &WrapperRow, clock_seconds: i64) -> Option<Value> {
    let effective = effective_wrapper(row, clock_seconds);
    let state = effective.wrapper_state?;
    let expiry = row
        .expiry_seconds
        .as_deref()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .unwrap_or(Value::Null);
    Some(serde_json::json!({
        "kind": "ens_v1_wrapper",
        "wrapper_state": state,
        "fuses": effective.fuses,
        "expiry_seconds": expiry,
    }))
}

/// The F2b rows of `resource_ids`.
pub async fn load_wrapper_rows(
    pool: &PgPool,
    chain_id: &str,
    resource_ids: &[String],
) -> Result<Vec<WrapperRow>> {
    if resource_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.wrapper_rows */ SELECT to_jsonb(wrapper)
         FROM bigname_phase.project_wrapper_state wrapper
         WHERE wrapper.chain_id = $1 AND wrapper.resource_id::text = ANY($2)",
    )
    .bind(chain_id)
    .bind(resource_ids)
    .fetch_all(pool)
    .await
    .context("failed to load the wrapper family rows")?;
    Ok(rows.iter().filter_map(WrapperRow::from_row).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(state: &str, fuses: i64, expiry: i64) -> WrapperRow {
        WrapperRow {
            resource_id: "w".into(),
            wrapper_state: Some(state.into()),
            fuses: Some(fuses),
            has_modifier: true,
            expiry_seconds: Some(expiry.to_string()),
            has_expiry: true,
            ..WrapperRow::default()
        }
    }

    #[test]
    fn the_expiry_boundary_is_strict() {
        // Expiry equal to the clock is not past it; one second earlier is.
        let at = effective_wrapper(&row("locked", 5, 1_000), 1_000);
        assert_eq!(
            (at.wrapper_state.as_deref(), at.fuses, at.owner_lapsed),
            (Some("locked"), Some(5), false)
        );
        let after = effective_wrapper(&row("locked", 5, 999), 1_000);
        assert_eq!(
            (after.wrapper_state, after.fuses, after.owner_lapsed),
            (None, Some(0), true)
        );
        let before = effective_wrapper(&row("locked", 5, 1_001), 1_000);
        assert_eq!(before.fuses, Some(5));
    }

    #[test]
    fn a_wrapped_name_keeps_its_state_past_expiry_with_fuses_reset() {
        let past = effective_wrapper(&row("wrapped", 64, 10), 1_000);
        assert_eq!(
            (past.wrapper_state.as_deref(), past.fuses, past.owner_lapsed),
            (Some("wrapped"), Some(0), false)
        );
    }

    #[test]
    fn grace_needs_the_dot_eth_fuse_and_the_expiry_within_the_period() {
        let clock = 10_000_000;
        let inside = effective_wrapper(&row("wrapped", IS_DOT_ETH, clock + 7_776_000 - 1), clock);
        assert!(inside.in_grace);
        let edge = effective_wrapper(&row("wrapped", IS_DOT_ETH, clock + 7_776_000), clock);
        assert!(!edge.in_grace);
        let no_fuse = effective_wrapper(&row("wrapped", 0, clock + 1), clock);
        assert!(!no_fuse.in_grace);
        // Past the expiry the fuses are zero, so the grace bit is gone too.
        let expired = effective_wrapper(&row("wrapped", IS_DOT_ETH, clock - 1), clock);
        assert!(!expired.in_grace);
    }

    #[test]
    fn an_unknown_part_nulls_the_masks() {
        let mut unknown = row("wrapped", 1, 10);
        unknown.expiry_seconds = None;
        assert_eq!(effective_wrapper(&unknown, 0), EffectiveWrapper::default());
        assert_eq!(servable_expiry(&row("wrapped", 0, 0)), None);
        assert_eq!(servable_expiry(&row("wrapped", 0, 5)), Some(5));
    }
}
