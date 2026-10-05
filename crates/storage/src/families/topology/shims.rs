//! SQL fragments the topology readers share: the canonical event order over a family row's
//! position columns, as a row value or read from a JSON position, and a child's effective
//! NameWrapper fuses at the publication clock. Also the fragments the name-ordered candidate
//! walks (search, a resolver's bound names, an address's reverse page) share for the surfaces
//! that store no raw bytes.
//!
//! The children surface filter (`CHILD_SURFACE_FILTER`, `children.rs`) drops every child whose
//! surface is unreadable; no writer records a label-preimage exception for an unreadable
//! surface.

use super::super::{name::rendered::rendered_name_sql, position::emission_ordinal_sql};

/// The canonical event order over a family row's own position columns
/// (docs/glossary.md#canonical-event-order), as a row value that
/// compares later-is-greater: block number, transaction index, log index, the emission ordinal
/// (docs/glossary.md#emission-ordinal), then the event identity as bytes. A synthesised event
/// (no transaction or log index) sorts before every transaction of its block and has no ordinal,
/// so it keeps the identity byte order. Every component is non-null, so a greater-than over two
/// positions is always decisive and a descending order puts an event without an ordinal after
/// one with an ordinal.
pub(super) fn row_position(alias: &str) -> String {
    let ordinal = emission_ordinal_sql(
        &format!("{alias}.event_identity"),
        &format!("{alias}.transaction_index"),
        &format!("{alias}.log_index"),
    );
    format!(
        "({alias}.block_number, COALESCE({alias}.transaction_index, -1), \
         COALESCE({alias}.log_index, -1), {ordinal}, {alias}.event_identity COLLATE \"C\")"
    )
}

/// The same order, emission ordinal included, over a secondary position stored as a JSON
/// object.
pub(super) fn json_position(expression: &str) -> String {
    let transaction = format!("({expression} ->> 'transaction_index')::bigint");
    let log = format!("({expression} ->> 'log_index')::bigint");
    let identity = format!("({expression} ->> 'event_identity')");
    let ordinal = emission_ordinal_sql(&identity, &transaction, &log);
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

/// SQL for "the `name_surfaces` row aliased `surface` stores no raw bytes". The raw columns are
/// null together (`name_surfaces_raw_evidence_check`) and `hash_array_extended` is strict, so the
/// second test repeats the first in the form `name_surfaces_project_suffix_hash_idx` indexes,
/// which lets a statement that names the namespace reach these rows without reading the
/// surfaces that have their bytes.
pub(crate) fn textless_surface_sql(surface: &str) -> String {
    format!("{surface}.raw_name IS NULL AND hash_array_extended({surface}.raw_labels, 0) IS NULL")
}

/// SQL that is true when any surface stores no raw bytes. It reads no column of the statement
/// around it, so the planner evaluates it once, before the arm it guards, and does not run the
/// arm when it is false: a database whose surfaces all have their bytes pays a few index probes
/// for the arm. The namespaces are stepped through one at a time (each `LIMIT 1` reads one entry
/// of an index led by `namespace`) so that every probe for a surface without bytes names the
/// leading column of `name_surfaces_project_suffix_hash_idx`.
pub(crate) fn textless_surfaces_exist_sql() -> String {
    format!(
        "(SELECT EXISTS (
              SELECT 1 FROM bigname_phase.name_surfaces probe
              WHERE probe.namespace = ANY (ARRAY(
                        WITH RECURSIVE step(namespace) AS (
                            (SELECT head.namespace FROM bigname_phase.name_surfaces head
                             ORDER BY head.namespace LIMIT 1)
                            UNION ALL
                            SELECT (SELECT following.namespace
                                    FROM bigname_phase.name_surfaces following
                                    WHERE following.namespace > step.namespace
                                    ORDER BY following.namespace LIMIT 1)
                            FROM step WHERE step.namespace IS NOT NULL)
                        SELECT step.namespace FROM step WHERE step.namespace IS NOT NULL))
                AND {textless}))",
        textless = textless_surface_sql("probe")
    )
}

/// A walk's surfaces without raw bytes, as `rendered.name` beside `surface`: the served name,
/// computed once per row however often the arm reads it.
pub(crate) fn rendered_lateral_sql() -> String {
    format!(
        "CROSS JOIN LATERAL (SELECT {} AS name OFFSET 0) rendered",
        rendered_name_sql("surface")
    )
}

#[cfg(test)]
#[path = "shims_tests.rs"]
mod tests;
