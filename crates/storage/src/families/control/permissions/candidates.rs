//! Seek permission keys before composing grants, wrapper operators or registry bindings.
//! Each source returns only a bounded batch. Registry observations over-select; composition
//! decides whether the approval still applies to the selected resource.
use std::collections::BTreeMap;

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
/// The approvals of an observation's registry owner, for the registry sources of a read bound
/// to one resource on chain `$9`. Those start from the resource instead of from every approval.
///
/// Keeping to the resource's chain drops no served key. A resource is on one chain (the
/// resource id is the primary key of `resources`), Project writes observations and binding
/// candidates under the chain it is projecting (`crates/project/src/families/registry.rs`,
/// `identity.rs`), and composition already drops a key of another chain: it reads a key's
/// binding and approvals on the resource's chain and builds the scope from that chain.
const OBSERVED: &str = "JOIN bigname_phase.project_account_approval approval
      ON approval.chain_id = observation.chain_id
     AND approval.authority_contract = observation.registry_contract
     AND approval.owner = observation.registry_owner
    WHERE observation.chain_id = $9 AND observation.applicable";

/// The registry sources of a read bound to one resource on chain `$9`: the observations that
/// reach the resource, the resource's own, and those of the names bound to it.
fn resource_registry_sources() -> [String; 3] {
    [
        format!(
            "SELECT approval.subject, observation.target_resource_id AS resource_id,
             {ACCOUNT_SCOPE}
             FROM bigname_phase.project_registry_binding_observation observation
             {OBSERVED} AND {APPROVED} AND observation.target_resource_id = $2"
        ),
        format!(
            "SELECT approval.subject, observation.resource_id, {ACCOUNT_SCOPE}
             FROM bigname_phase.project_registry_binding_observation observation
             {OBSERVED} AND {APPROVED} AND observation.resource_id = $2"
        ),
        // A named observation's identity is its name, so the primary key finds it:
        // `observation_identity` is `COALESCE(logical_name_id, resource_id)`, as its column
        // comment in the baseline schema says and as its one writer sets it (`observation` in
        // `crates/project/src/families/registry.rs`). The name comparison stays, so a row
        // that broke the rule would be dropped, never wrongly added.
        format!(
            "SELECT approval.subject, candidate.resource_id, {ACCOUNT_SCOPE}
             FROM bigname_phase.project_binding_candidate candidate
             JOIN bigname_phase.project_registry_binding_observation observation
               ON observation.chain_id = candidate.chain_id
              AND observation.observation_identity = candidate.logical_name_id
              AND observation.logical_name_id = candidate.logical_name_id
             {OBSERVED} AND {APPROVED} AND observation.attributed_via = 'name'
               AND candidate.chain_id = $9 AND candidate.resource_id = $2"
        ),
    ]
}

/// [`resource_registry_sources`] for the resources `$2` of a page on chain `$9`, with the same
/// conditions. Each source reads the page's observations in a subquery of its own and joins the
/// approvals to them. `OFFSET 0` keeps the planner from merging the subquery into the join. Left
/// to merge it, the planner starts from every approved operator of the chain and walks each
/// owner's observations: it expects a few observations per owner, and the owners who approve
/// operators are the ones with many.
fn page_registry_sources() -> [String; 3] {
    let observed = |resource: &str, from: &str| {
        format!(
            "SELECT approval.subject, observation.resource_id, {ACCOUNT_SCOPE}
             FROM (SELECT {resource} AS resource_id, observation.chain_id,
                          observation.registry_contract, observation.registry_owner
                   {from} AND observation.chain_id = $9 AND observation.applicable
                   OFFSET 0) observation
             JOIN bigname_phase.project_account_approval approval
               ON approval.chain_id = observation.chain_id
              AND approval.authority_contract = observation.registry_contract
              AND approval.owner = observation.registry_owner
             WHERE {APPROVED}"
        )
    };
    [
        observed(
            "observation.target_resource_id",
            "FROM bigname_phase.project_registry_binding_observation observation
             WHERE observation.target_resource_id = ANY($2::uuid[])",
        ),
        observed(
            "observation.resource_id",
            "FROM bigname_phase.project_registry_binding_observation observation
             WHERE observation.resource_id = ANY($2::uuid[])",
        ),
        observed(
            "candidate.resource_id",
            "FROM bigname_phase.project_binding_candidate candidate
             JOIN bigname_phase.project_registry_binding_observation observation
               ON observation.chain_id = candidate.chain_id
              AND observation.observation_identity = candidate.logical_name_id
              AND observation.logical_name_id = candidate.logical_name_id
             WHERE candidate.chain_id = $9 AND candidate.resource_id = ANY($2::uuid[])
               AND observation.attributed_via = 'name'",
        ),
    ]
}

/// The candidate-key statement of [`page`]: nine sources, each cut to the page before the
/// union. `token` is what a read bound to one resource knows about it. With it, the three
/// registry sources look the resource up on its chain, and without it they are unchanged.
pub(super) fn statement(
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    after: Option<&PermissionsCurrentAccountResourceCursor>,
    token: Option<&TokenRead>,
) -> String {
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
    let [registry_target, registry_resource, registry_name] =
        if resource_id.is_some() && token.is_some() {
            resource_registry_sources()
        } else {
            [registry_target, registry_resource, registry_name]
        };
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
    format!(
        "/* storage:families.control.permissions.candidate_page */
         SELECT subject, resource_id, scope FROM ({arms}) candidates
         ORDER BY subject COLLATE \"C\", resource_id, scope COLLATE \"C\" LIMIT $7"
    )
}

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
    sqlx::query_as(&statement(subject, resource_id, after, token))
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

/// The candidate-key statement of [`resource_page`]: the sources of [`statement`] for the
/// resources `$2` of chain `$9`, each cut to the limit `$7` before the union, in resource,
/// subject and scope order after the key `($5, $4, $6)`.
///
/// The parameter numbers are those of [`statement`], whose direct, wrapper and ENSv2 operator
/// sources it shares, so `$1`, `$3` and `$8` are bound and not read. It has no root-holder source: the read it serves lists no
/// root holders. It has no namespace source either, because its caller keeps only the
/// resources of the namespace before it seeks.
pub(super) fn resource_page_statement() -> String {
    let [registry_target, registry_resource, registry_name] = page_registry_sources();
    let wrapper_owners = super::wrapper_registry::page_owner_candidates();
    let wrapper_operators = super::wrapper_registry::page_operator_candidates();
    let arms = [
        DIRECT,
        WRAPPER,
        &registry_target,
        &registry_resource,
        &registry_name,
        super::ens_v2::OPERATOR_CANDIDATES,
        &wrapper_owners,
        &wrapper_operators,
    ]
    .into_iter()
    .map(|source| {
        format!(
            "(SELECT DISTINCT candidate.resource_id, candidate.subject COLLATE \"C\" AS subject,
                    candidate.scope COLLATE \"C\" AS scope
              FROM ({source}) candidate
              WHERE candidate.resource_id = ANY($2::uuid[])
                AND ($4::text IS NULL OR
                    (candidate.resource_id, candidate.subject COLLATE \"C\", candidate.scope COLLATE \"C\")
                        > ($5::uuid, $4 COLLATE \"C\", $6 COLLATE \"C\"))
              ORDER BY resource_id, subject, scope LIMIT $7)"
        )
    })
    .collect::<Vec<_>>()
    .join(" UNION ");
    format!(
        "/* storage:families.control.permissions.resource_candidate_page */
         SELECT subject, resource_id, scope FROM ({arms}) candidates
         ORDER BY resource_id, subject COLLATE \"C\", scope COLLATE \"C\" LIMIT $7"
    )
}

/// The next `limit` candidate keys of the resources of every chain of `chains`, in resource,
/// subject and scope order: one statement per chain, each asked for `limit` keys, merged. Fewer
/// than `limit` keys means no key is left.
pub(super) async fn resource_pages(
    conn: &mut PgConnection,
    chains: &BTreeMap<&str, Vec<Uuid>>,
    after: Option<&Key>,
    limit: usize,
) -> Result<Vec<Key>> {
    let mut keys = Vec::new();
    for (chain_id, resources) in chains {
        keys.extend(resource_page(conn, chain_id, resources, after, limit as i64).await?);
    }
    keys.sort_by(|left, right| {
        (left.resource_id, &left.subject, &left.scope).cmp(&(
            right.resource_id,
            &right.subject,
            &right.scope,
        ))
    });
    // The merge can hold keys past the cut. The next read finds them again after it.
    keys.truncate(limit);
    Ok(keys)
}

/// The next candidate keys of `resources`, all on `chain_id`, ordered by resource, then subject
/// and scope as bytes. One statement reads the keys of every resource, where [`page`] reads
/// those of one. `after` continues past a key: no resource before its own is read again.
async fn resource_page(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[Uuid],
    after: Option<&Key>,
    limit: i64,
) -> Result<Vec<Key>> {
    let resources: Vec<Uuid> = resources
        .iter()
        .copied()
        .filter(|resource| after.is_none_or(|key| *resource >= key.resource_id))
        .collect();
    if resources.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as(&resource_page_statement())
        .persistent(false)
        .bind(None::<&str>)
        .bind(&resources)
        .bind(None::<&str>)
        .bind(after.map(|key| key.subject.as_str()))
        .bind(after.map(|key| key.resource_id))
        .bind(after.map(|key| key.scope.as_str()))
        .bind(limit)
        .bind(None::<Uuid>)
        .bind(chain_id)
        .fetch_all(conn)
        .await
        .map_err(Into::into)
}

#[cfg(test)]
#[path = "candidates_tests.rs"]
mod tests;
