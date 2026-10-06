//! Seek permission keys before composing grants, wrapper operators or registry bindings.
//! Each source returns only a bounded batch. Registry observations over-select; composition
//! decides whether the approval still applies to the selected resource.
use anyhow::Result;
use serde::Serialize;
use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::PermissionsCurrentAccountResourceCursor;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, FromRow)]
pub(super) struct Key {
    pub subject: String,
    pub resource_id: Uuid,
    pub scope: String,
}

impl From<&Key> for PermissionsCurrentAccountResourceCursor {
    fn from(key: &Key) -> Self {
        Self {
            subject: key.subject.clone(),
            resource_id: key.resource_id,
            scope: key.scope.clone(),
        }
    }
}

/// What a read bound to one resource resolved before seeking keys.
#[derive(Clone, Debug)]
pub(super) struct TokenRead {
    /// The resource's chain, which lets the ENSv2 entry lookup use its `(chain_id,
    /// resource_id)` index.
    pub chain_id: String,
    /// The registry root whose holders the read lists with an ENSv2 registry token resource.
    pub root: Option<Uuid>,
}

const DIRECT: &str = "SELECT grant_row.subject, grant_row.resource_id, grant_row.scope
    FROM bigname_phase.project_grant grant_row";
const WRAPPER: &str = "SELECT approval.subject, grant_row.resource_id, grant_row.scope
    FROM bigname_phase.project_account_approval approval
    JOIN bigname_phase.project_grant grant_row
      ON grant_row.chain_id = approval.chain_id AND grant_row.subject = approval.owner
    WHERE approval.authority_kind = 'wrapper' AND approval.approved
      AND grant_row.grant_source ->> 'authority_kind' = 'wrapper'
      AND grant_row.grant_source ->> 'relation_kind' = 'holder'
      AND lower(grant_row.grant_source ->> 'authority_contract') = approval.authority_contract";
const REGISTRY: &str = "FROM bigname_phase.project_account_approval approval
    JOIN bigname_phase.project_registry_binding_observation observation
      ON observation.chain_id = approval.chain_id
     AND observation.registry_contract = approval.authority_contract
     AND observation.registry_owner = approval.owner AND observation.applicable";
const APPROVED: &str = "approval.authority_kind = 'registry'
    AND approval.relation_kind = 'operator' AND approval.approved";
const ACCOUNT_SCOPE: &str = "concat('account:', approval.chain_id, ':registry:',
    approval.authority_contract, ':', approval.owner) AS scope";

/// The next candidate keys in the public bytewise tuple order. A cursor can continue within
/// one resource or one subject without loading the keys that precede it. Namespace membership
/// precedes publication checks, so an unrelated namespace's rebuilding chain is not read.
/// `token` is what a read bound to one resource knows about it.
pub(super) async fn page(
    conn: &mut PgConnection,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    after: Option<&PermissionsCurrentAccountResourceCursor>,
    limit: i64,
    token: Option<&TokenRead>,
) -> Result<Vec<Key>> {
    let registry_target = format!(
        "SELECT approval.subject, observation.target_resource_id AS resource_id,
         {ACCOUNT_SCOPE} {REGISTRY} WHERE {APPROVED}"
    );
    let registry_resource = format!(
        "SELECT approval.subject, observation.resource_id,
         {ACCOUNT_SCOPE} {REGISTRY} WHERE {APPROVED}"
    );
    let registry_name = format!(
        "SELECT approval.subject, candidate.resource_id, {ACCOUNT_SCOPE} {REGISTRY}
         JOIN bigname_phase.project_binding_candidate candidate
           ON candidate.logical_name_id = observation.logical_name_id
         WHERE {APPROVED} AND observation.attributed_via = 'name'"
    );
    // Expose the leading seek key to the source indexes, even when the tuple contains a
    // computed account scope. The full tuple comparison below retains within-resource paging.
    let seek = if subject
        .zip(after)
        .is_some_and(|(subject, after)| subject == after.subject)
    {
        "AND candidate.resource_id >= $5::uuid"
    } else if resource_id.is_some() && after.is_some() {
        "AND candidate.subject COLLATE \"C\" >= $4 COLLATE \"C\""
    } else {
        ""
    };
    let wrapper_owners = super::wrapper_registry::owner_candidates();
    let wrapper_operators = super::wrapper_registry::operator_candidates();
    let arms = [
        DIRECT, WRAPPER, &registry_target, &registry_resource, &registry_name,
        super::ens_v2::OPERATOR_CANDIDATES, super::ens_v2::ROOT_HOLDER_CANDIDATES,
        &wrapper_owners, &wrapper_operators,
    ]
        .into_iter().map(|source| format!(
            "(SELECT DISTINCT candidate.subject COLLATE \"C\" AS subject,
                    candidate.resource_id, candidate.scope COLLATE \"C\" AS scope
              FROM ({source}) candidate
              WHERE ($1::text IS NULL OR candidate.subject COLLATE \"C\" = $1 COLLATE \"C\")
                AND ($2::uuid IS NULL OR candidate.resource_id = $2) {seek}
                AND ($4::text IS NULL OR
                    (candidate.subject COLLATE \"C\", candidate.resource_id, candidate.scope COLLATE \"C\")
                        > ($4 COLLATE \"C\", $5::uuid, $6 COLLATE \"C\"))
                AND ($3::text IS NULL OR EXISTS (
                    SELECT 1 FROM bigname_phase.normalized_events event
                    JOIN bigname_phase.chain_lineage lineage
                      ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
                    WHERE event.resource_id = candidate.resource_id AND event.namespace = $3
                      AND event.consumer_visibility = 'activated'
                      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
              ORDER BY subject, resource_id, scope LIMIT $7)"
        )).collect::<Vec<_>>().join(" UNION ");
    sqlx::query_as(&format!(
        "/* storage:families.control.permissions.candidate_page */
         SELECT subject, resource_id, scope FROM ({arms}) candidates
         ORDER BY subject COLLATE \"C\", resource_id, scope COLLATE \"C\" LIMIT $7"
    ))
    .persistent(false)
    .bind(subject)
    .bind(resource_id)
    .bind(namespace)
    .bind(after.map(|key| key.subject.as_str()))
    .bind(after.map(|key| key.resource_id))
    .bind(after.map(|key| key.scope.as_str()))
    .bind(limit)
    .bind(token.and_then(|token| token.root))
    .bind(token.map(|token| token.chain_id.as_str()))
    .fetch_all(conn)
    .await
    .map_err(Into::into)
}
