//! Reads these readers need that are not yet computed from the family tables. The ones in this
//! file are one small function each, so the family read that replaces one replaces exactly one
//! function; every function names the interim source it reads today. Several other interim reads
//! stay inline in the reader queries and are listed after them, so not every interim dependency
//! is one replaceable function.
//!
//! - [`selected_binding`]: the name's selected binding. Interim: `name_current.surface_binding_id`
//!   joined to its `project_binding_candidate` row, until the selection among a name's binding
//!   candidates is computed at read.
//! - [`selected_authority_arm`]: the child's selected authority arm. Interim:
//!   `name_current.provenance.authority_selection.authority_arm`, until it is computed from the
//!   binding candidates.
//! - [`serving_row_exists`]: whether the child has a serving row. Interim:
//!   `name_current.provenance.read_reachability.serving_resource_id`, until the serving
//!   selection is computed from the resource pointers, registrations, registry owners and
//!   binding candidates.
//! - [`effective_child_fuses`]: the child's wrapper fuses masked at the family marker's block
//!   time over `project_wrapper_state`, the mask `effective_wrapper_state` applies in
//!   crates/project/src/builders/children.rs, until a shared wrapper mask read exists.
//! - [`attributed_zero_owner`]: whether the latest registry Transfer attributed to the child
//!   reports a zero owner. Interim: the attribution of `project_latest_registry_owner` over
//!   `normalized_events`, because `project_registry_node_state` keys a Transfer by the node it
//!   carries and cannot attribute it through its name or resource.
//!
//! Inline reads of served projection tables. Each must be replaced by a family read before step 7
//! serves the reader that holds it:
//!
//! - `/aliases` (`collections.rs`), binding arm: reads `name_current` directly for the selected
//!   binding, resource and token lineage ids, the raw name and namehash it returns, and the
//!   shared current-name readability predicates (`DEFAULT_NAME_CURRENT_READ_FILTER`). That is
//!   both its eligibility and its payload, and more than [`selected_binding`].
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
//! - The subnames page (`children_page.rs`) takes a child's registration and expiry times, and
//!   the released status its expiry fence checks, from the served `name_current.declared_summary`
//!   through `push_registered_at_timestamp_expr` and `push_expires_at_timestamp_expr`, the same
//!   expressions today's page uses. The timestamp sorts and the fence therefore compare one
//!   served column on both sides.
//!
//! Inline reads of `normalized_events`:
//!
//! - The declaration fallback in `load_resolver_shadow` (`resolver.rs`) reads the latest
//!   `SourceManifestUpdated` of each manifest. Interim: it covers a resolver with no
//!   classification row and has no place once the table is complete, so it goes before step 7.
//! - [`attributed_zero_owner`], above. Interim, to be replaced before step 7.
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
//! - `/roles` (`collections.rs`) mirrors only the resource predicate of the served permissions
//!   read filter, so for corresponding grant facts the served results are contained in the
//!   shadow's. The filter's other two predicates have no counterpart, because `project_grant`
//!   carries no block hash or row state: a readable resource can sit beside a grant row whose own
//!   canonicality state is unreadable, and the publication block can be orphaned. Orphaning the
//!   publication need not orphan the granting event, so no family undo may ever remove the grant.
//!   In both cases today's reader drops a grant the shadow keeps. Row-state and publication-lineage
//!   parity are prerequisites for serving `/roles` from the families, with the masks and the
//!   `grant_event` provenance.
//! - The children surface filter (`CHILD_SURFACE_FILTER`, `children.rs`) drops every child whose
//!   surface is unreadable. Today's `DEFAULT_CHILDREN_CURRENT_READ_FILTER` also keeps such a child
//!   when `provenance.label.source = 'label_preimage'`. No current writer sets that key: the only
//!   `children_current` writer (crates/project/src/builders/children.rs) builds its provenance
//!   without a `label` object, so the branch is unreachable today and the family rows carry no
//!   label source to mirror it with. If a writer starts setting it, the shadow must learn it.
use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

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

/// An event today's Project stages: activated, canonical, and on the readable lineage at or
/// below the block `block` unless it has no block (crates/project/src/stage/events.rs).
fn staged_event(alias: &str, block: &str) -> String {
    format!(
        "{alias}.consumer_visibility = 'activated'
         AND {alias}.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND (({alias}.block_number IS NULL AND {alias}.block_hash IS NULL)
              OR ({alias}.block_number <= {block}
                  AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage {alias}_lineage
                              WHERE {alias}_lineage.chain_id = {alias}.chain_id
                                AND {alias}_lineage.block_hash = {alias}.block_hash
                                AND {alias}_lineage.block_number = {alias}.block_number
                                AND {alias}_lineage.canonicality_state
                                    IN ('canonical', 'safe', 'finalized'))))"
    )
}

/// Interim: whether the latest ENSv1 or Basenames registry AuthorityTransferred attributed to the
/// child `child` (a name id expression, at node `node` in chain `chain`) reports a zero owner
/// getter, at the block `block`. The attribution and order are those of
/// `project_latest_registry_owner` (crates/project/src/builders/name_authority/stage.rs): the
/// name the event carries, else the latest named event of the same resource and source family,
/// else an active, readable surface at the event's `child_node` or `node`; latest by block,
/// transaction index and log index with nulls lowest, then event identity. The candidates are
/// the Transfers that can attribute to the child: those naming it, those of a resource one of
/// its named registry events carries, and unnamed ones at its node.
pub(super) fn attributed_zero_owner(chain: &str, child: &str, node: &str, block: &str) -> String {
    let registries = "('ens_v1_registry_l1', 'basenames_base_registry')";
    let transfer_readable = staged_event("transfer", block);
    let candidate_readable = staged_event("candidate", block);
    let named_readable = staged_event("named", block);
    let transfer_node = "lower(COALESCE(NULLIF(transfer.after_state ->> 'child_node', ''),
                                        NULLIF(transfer.after_state ->> 'node', '')))";
    format!(
        "COALESCE((
            SELECT attributed.owner_getter = '0x0000000000000000000000000000000000000000'
            FROM (
                SELECT transfer.after_state ->> 'owner_getter' AS owner_getter,
                       COALESCE(transfer.logical_name_id, linked.logical_name_id,
                                attributed_surface.logical_name_id) AS logical_name_id,
                       transfer.block_number, transfer.transaction_index, transfer.log_index,
                       transfer.event_identity
                FROM bigname_phase.normalized_events transfer
                LEFT JOIN LATERAL (
                    SELECT candidate.logical_name_id
                    FROM bigname_phase.normalized_events candidate
                    WHERE transfer.logical_name_id IS NULL
                      AND candidate.logical_name_id IS NOT NULL
                      AND candidate.chain_id = transfer.chain_id
                      AND candidate.resource_id = transfer.resource_id
                      AND candidate.source_family = transfer.source_family
                      AND {candidate_readable}
                    ORDER BY candidate.block_number DESC NULLS LAST,
                             candidate.transaction_index DESC NULLS LAST,
                             candidate.log_index DESC NULLS LAST,
                             candidate.event_identity DESC
                    LIMIT 1
                ) linked ON TRUE
                LEFT JOIN bigname_phase.name_surfaces attributed_surface
                  ON transfer.logical_name_id IS NULL
                 AND attributed_surface.chain_id = transfer.chain_id
                 AND attributed_surface.namespace = transfer.namespace
                 AND attributed_surface.visibility_state = 'active'
                 AND attributed_surface.block_number <= {block}
                 AND attributed_surface.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage surface_lineage
                             WHERE surface_lineage.chain_id = attributed_surface.chain_id
                               AND surface_lineage.block_hash = attributed_surface.block_hash
                               AND surface_lineage.block_number = attributed_surface.block_number
                               AND surface_lineage.canonicality_state
                                   IN ('canonical', 'safe', 'finalized'))
                 AND lower(attributed_surface.namehash) = {transfer_node}
                WHERE transfer.chain_id = {chain}
                  AND transfer.event_kind = 'AuthorityTransferred'
                  AND transfer.source_family IN {registries}
                  AND {transfer_readable}
                  AND (transfer.logical_name_id = {child}
                       OR (transfer.logical_name_id IS NULL
                           AND ({transfer_node} = {node}
                                OR transfer.resource_id IN (
                                    SELECT named.resource_id
                                    FROM bigname_phase.normalized_events named
                                    WHERE named.logical_name_id = {child}
                                      AND named.chain_id = {chain}
                                      AND named.source_family IN {registries}
                                      AND named.resource_id IS NOT NULL
                                      AND {named_readable}))))
            ) attributed
            WHERE attributed.logical_name_id = {child}
            ORDER BY attributed.block_number DESC NULLS LAST,
                     attributed.transaction_index DESC NULLS LAST,
                     attributed.log_index DESC NULLS LAST,
                     attributed.event_identity DESC
            LIMIT 1
        ), FALSE)"
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

/// A name's selected binding: its `project_binding_candidate` row.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SelectedBinding {
    pub(super) chain_id: String,
    pub(super) resource_id: Uuid,
    pub(super) binding_kind: String,
    pub(super) block_number: i64,
}

/// Interim: the binding `name_current` selected, read from its `project_binding_candidate` row.
pub(super) async fn selected_binding(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<SelectedBinding>> {
    let row: Option<(String, Uuid, String, i64)> = sqlx::query_as(
        "SELECT candidate.chain_id, candidate.resource_id, candidate.binding_kind,
                candidate.block_number
         FROM bigname_phase.name_current nc
         JOIN bigname_phase.project_binding_candidate candidate
           ON candidate.surface_binding_id = nc.surface_binding_id
         WHERE nc.logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the selected binding of {logical_name_id}"))?;
    Ok(row.map(
        |(chain_id, resource_id, binding_kind, block_number)| SelectedBinding {
            chain_id,
            resource_id,
            binding_kind,
            block_number,
        },
    ))
}

/// Interim: the child's selected authority arm (`ens_v1`, `basenames` or `ens_v2`), null when
/// undetermined, as a scalar subquery over the name id expression `child`.
pub(super) fn selected_authority_arm(child: &str) -> String {
    format!(
        "(SELECT arm_nc.provenance #>> '{{authority_selection,authority_arm}}'
          FROM bigname_phase.name_current arm_nc WHERE arm_nc.logical_name_id = {child})"
    )
}

/// Interim: whether the child named by `child` has a serving row
/// (crates/project/src/builders/children.rs, the `project_name_serving` eligibility of an
/// ownerless child).
pub(super) fn serving_row_exists(child: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM bigname_phase.name_current serving_nc
                 WHERE serving_nc.logical_name_id = {child}
                   AND serving_nc.provenance #>> '{{read_reachability,serving_resource_id}}'
                       IS NOT NULL)"
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
