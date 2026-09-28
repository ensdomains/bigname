//! SQL fragments the topology readers share: the canonical event order over a family row's
//! position columns, as a row value or read from a JSON position, and a child's effective
//! NameWrapper fuses at the publication clock.
//!
//! The children surface filter (`CHILD_SURFACE_FILTER`, `children.rs`) drops every child whose
//! surface is unreadable; no writer records a label-preimage exception for an unreadable
//! surface.

/// The canonical event order over a family row's own position columns
/// (docs/glossary.md#canonical-event-order), as a row value that
/// compares later-is-greater: block number, transaction index, log index, the emission ordinal
/// (docs/glossary.md#emission-ordinal), then the event identity as bytes. A synthesised event
/// (no transaction or log index) sorts before every transaction of its block and has no ordinal,
/// so it keeps the identity byte order. Every component is non-null, so a greater-than over two
/// positions is always decisive and a descending order puts an event without an ordinal after
/// one with an ordinal.
pub(super) fn row_position(alias: &str) -> String {
    let ordinal = emission_ordinal(
        &format!("{alias}.event_identity"),
        &format!("{alias}.transaction_index"),
        &format!("{alias}.log_index"),
    );
    format!(
        "({alias}.block_number, COALESCE({alias}.transaction_index, -1), \
         COALESCE({alias}.log_index, -1), {ordinal}, {alias}.event_identity COLLATE \"C\")"
    )
}

/// The emission ordinal (docs/glossary.md#emission-ordinal) of an event as a bigint, -1 when it
/// has none: the identity's final `:`-separated segment when the event has both a transaction and
/// a log index and that segment is a nonempty run of ASCII digits no greater than 4294967295,
/// leading zeros allowed. This is the glossary's checked SQL form, which
/// crates/project/tests/families_ordinal_sql.rs checks against the Rust parse in
/// crates/project/src/families/position.rs: strip leading zeros, check the significant length
/// against the ten-digit bound, and only then cast, so no suffix errors where the Rust parse
/// yields none. Absent is -1 rather than null so the row value
/// stays decisive; every valid ordinal is at least 0, so -1 sorts first as `NULLS FIRST` does.
fn emission_ordinal(identity: &str, transaction: &str, log: &str) -> String {
    format!(
        "COALESCE(CASE WHEN {transaction} IS NOT NULL AND {log} IS NOT NULL THEN (
            SELECT CASE WHEN digits.d = '' THEN 0::bigint
                        WHEN length(digits.d) < 10
                          OR (length(digits.d) = 10
                              AND digits.d COLLATE \"C\" <= '4294967295' COLLATE \"C\")
                            THEN digits.d::bigint END
            FROM (SELECT ltrim(m[1], '0') AS d
                  FROM regexp_match(({identity}) COLLATE \"C\", ':([0-9]+)$') m) digits
        ) END, -1::bigint)"
    )
}

/// The same order, emission ordinal included, over a secondary position stored as a JSON
/// object.
pub(super) fn json_position(expression: &str) -> String {
    let transaction = format!("({expression} ->> 'transaction_index')::bigint");
    let log = format!("({expression} ->> 'log_index')::bigint");
    let identity = format!("({expression} ->> 'event_identity')");
    let ordinal = emission_ordinal(&identity, &transaction, &log);
    format!(
        "(({expression} ->> 'block_number')::bigint, COALESCE({transaction}, -1), \
         COALESCE({log}, -1), {ordinal}, {identity} COLLATE \"C\")"
    )
}

/// The child's effective NameWrapper fuses at the block clock `epoch` (seconds): the wrapper
/// row whose latest PermissionScopeChanged is the name's latest, its fuses when the wrapper state
/// is known and its expiry is not behind the clock, else 0; null when the name has no wrapper
/// row. Fuses outside the 32-bit range read as 0.
pub(super) fn effective_child_fuses(chain: &str, child: &str, epoch: &str) -> String {
    let position = json_position("wrapper.wrapper_state_position");
    format!(
        "(SELECT CASE WHEN wrapper.wrapper_state IS NULL
                        OR wrapper.fuses IS NULL
                        OR wrapper.fuses NOT BETWEEN 0 AND 4294967295
                        OR wrapper.expiry_seconds IS NULL OR {epoch} IS NULL
                        OR wrapper.expiry_seconds < {epoch} THEN 0
                   ELSE wrapper.fuses END
          FROM bigname_phase.project_wrapper_state wrapper
          WHERE wrapper.chain_id = {chain} AND wrapper.logical_name_id = {child}
            AND wrapper.wrapper_state_position IS NOT NULL
          ORDER BY {position} DESC
          LIMIT 1)"
    )
}

#[cfg(test)]
#[path = "shims_tests.rs"]
mod tests;
