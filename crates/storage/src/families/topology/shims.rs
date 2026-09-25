//! The reads step 5 borrows from steps 3 and 4 before they land, one small function each, so the
//! reader that replaces a shim replaces exactly one function. Every shim names its interim source.
//!
//! - [`selected_binding`]: the name's selected binding. Interim: `name_current.surface_binding_id`
//!   joined to its F1 candidate row; step 3 computes the F1 selection at read.
//! - [`selected_authority_arm`]: the child's selected authority arm. Interim:
//!   `name_current.provenance.authority_selection.authority_arm`; step 3 computes it from F1.
//! - [`serving_row_exists`]: whether the child has a serving row. Interim:
//!   `name_current.provenance.read_reachability.serving_resource_id`; steps 3 and 4 compute the
//!   serving selection over F5, F2a, F2c and F1.
//! - [`effective_child_fuses`]: the child's wrapper fuses masked at the block clock, the mask of
//!   children.rs `effective_wrapper_state` over F2b; step 3 owns the wrapper masks.
use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

/// The canonical event order over a family row's own position columns, as a row value that
/// compares later-is-greater: a synthesised event (no transaction or log index) sorts before
/// every transaction of its block, and the event identity breaks an exact-position tie as bytes.
pub(super) fn row_position(alias: &str) -> String {
    format!(
        "({alias}.block_number, COALESCE({alias}.transaction_index, -1), \
         COALESCE({alias}.log_index, -1), {alias}.event_identity COLLATE \"C\")"
    )
}

/// The same order over a secondary position stored as a JSON object.
pub(super) fn json_position(expression: &str) -> String {
    format!(
        "(({expression} ->> 'block_number')::bigint, \
         COALESCE(({expression} ->> 'transaction_index')::bigint, -1), \
         COALESCE(({expression} ->> 'log_index')::bigint, -1), \
         ({expression} ->> 'event_identity') COLLATE \"C\")"
    )
}

/// A name's selected binding: its F1 candidate row.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SelectedBinding {
    pub(super) chain_id: String,
    pub(super) resource_id: Uuid,
    pub(super) binding_kind: String,
    pub(super) block_number: i64,
}

/// Interim: the binding `name_current` selected, read from its F1 candidate row.
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

/// Interim: whether the child named by `child` has a serving row (children.rs, the
/// `project_name_serving` eligibility of an ownerless child).
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
/// row. The 32-bit fuse bound and the expiry mask are children.rs's.
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
