//! The child lists' per-child reads of the name summary family (`project_name_summary`): the
//! fields the family step stores for every name a block touches, from the
//! composed name reader's own selection (`families::name::compose_name_summaries`). A child with
//! no summary row is one the composed reader serves no name row for, which the lists treat as
//! having no name row: no arm, no serving resource, no zero-owner transfer, and no
//! registration status or timestamps.

/// The child's selected authority arm (`ens_v1`, `basenames` or `ens_v2`), null when none is
/// selected or the child has no summary, as a scalar subquery over the chain expression `chain`
/// and the name id expression `child`.
pub(super) fn selected_authority_arm(chain: &str, child: &str) -> String {
    format!(
        "(SELECT arm_summary.authority_arm FROM bigname_phase.project_name_summary arm_summary
          WHERE arm_summary.chain_id = {chain} AND arm_summary.logical_name_id = {child})"
    )
}

/// Whether the child has a serving resource, which admits an ownerless child
/// (crates/project/src/builders/children.rs, the `project_name_serving` eligibility).
pub(super) fn serving(chain: &str, child: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM bigname_phase.project_name_summary serving_summary
                 WHERE serving_summary.chain_id = {chain}
                   AND serving_summary.logical_name_id = {child}
                   AND serving_summary.serving)"
    )
}

/// Whether the latest registry transfer of the child's node names the zero owner, which zeroes
/// the child's registry owner (`project_latest_registry_owner` keeps zero owners only).
pub(super) fn zero_owner(chain: &str, child: &str) -> String {
    format!(
        "COALESCE((SELECT owner_summary.zero_owner
                   FROM bigname_phase.project_name_summary owner_summary
                   WHERE owner_summary.chain_id = {chain}
                     AND owner_summary.logical_name_id = {child}), FALSE)"
    )
}

/// The page's join of the child's summary for its timestamp sorts and expiry fence, as `summary`.
pub(super) const CHILD_SUMMARY_JOIN: &str = "
    LEFT JOIN bigname_phase.project_name_summary summary
      ON summary.chain_id = clock.chain_id
     AND summary.logical_name_id = selected.child_logical_name_id";
