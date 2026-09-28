//! Reads these readers need that are not yet computed from the family tables. The ones in this
//! file are one small function each, so the family read that replaces one replaces exactly one
//! function; every function names the interim source it reads today. Several other interim reads
//! stay inline in the reader queries and are listed after them, so not every interim dependency
//! is one replaceable function.
//!
//! - [`effective_child_fuses`]: the child's wrapper fuses masked at the family marker's block
//!   time over `project_wrapper_state`, the mask `effective_wrapper_state` applies in
//!   crates/project/src/builders/children.rs, until a shared wrapper mask read exists.
//!
//! The child's selected arm, serving resource and zero-owner transfer, and the registration
//! status and times the subnames page sorts and fences by, are no longer interim: they come from
//! the name summary family (`name_summary.rs`, TYR-36 step 7b slice 2b).
//!
//! Inline reads of served projection tables. Each must be replaced by a family read before step 7
//! serves the reader that holds it:
//!
//! - `load_bound_names_shadow` (`resolver.rs`): only the resolver match comes from
//!   `project_resource_pointer`. The name's eligibility, the registration, release and control
//!   checks, the namespace filter and the page order are `name_current` columns, the same
//!   predicate block as `load_phase_resolver_bound_name_rows`, and the returned rows are the
//!   served rows, hydrated through `load_phase_name_current_rows_by_ids`. A serving-only name is
//!   admitted through `resolver_current.declared_summary.bindings.status`, which the F3
//!   classification row does not replace.
//! - `load_bound_names_shadow` takes a name's selected resource from `name_current.resource_id`,
//!   else its serving resource, to pick the one pointer that counts. Today's name row instead
//!   takes the latest of the selected authority's pointer and the serving pointer, by block,
//!   transaction index and log index, then normalized event id, descending with nulls last (the
//!   `resolver` lateral of name_current/build.sql). So a name with a non-null selected resource A
//!   and a distinct serving resource B differs when B's pointer wins that order and names another
//!   resolver: the served row follows B, the shadow follows A. It also differs when A has no
//!   pointer and B has an eligible serving pointer, since the non-null A blocks the fallback to
//!   B. No fixture covers either case.
//!
//! Inline reads of `normalized_events`:
//!
//! - `/links` (`collections.rs`) looks up each link's event by identity for its block hash and
//!   transaction hash, and the wildcard arm of `load_name_topology_shadow` (`name_topology.rs`)
//!   looks up the boundary event for its id and block hash. These are metadata lookups by key
//!   and are intended to stay as inputs.
//!
//! The identity and lineage tables the readers join (`name_surfaces`, `resources`,
//! `surface_bindings`, `token_lineages`, `chain_lineage`) and the label and discovery tables are
//! inputs, not served projections, and are intended to stay.
//!
//! Known gaps outside this file:
//!
//! - The children surface filter (`CHILD_SURFACE_FILTER`, `children.rs`) drops every child whose
//!   surface is unreadable. Today's `DEFAULT_CHILDREN_CURRENT_READ_FILTER` also keeps such a child
//!   when `provenance.label.source = 'label_preimage'`. No current writer sets that key: the only
//!   `children_current` writer (crates/project/src/builders/children.rs) builds its provenance
//!   without a `label` object, so the branch is unreachable today and the family rows carry no
//!   label source to mirror it with. If a writer starts setting it, the shadow must learn it.

/// The canonical event order over a family row's own position columns
/// (docs/glossary.md#canonical-event-order; D12 as amended on 2026-09-26), as a row value that
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
/// row. The 32-bit fuse bound and the expiry mask are those of
/// crates/project/src/builders/children.rs.
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
