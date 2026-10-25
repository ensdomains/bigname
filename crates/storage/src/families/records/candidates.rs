//! The resources whose records may resolve to an address, for the inverse address read.
//!
//! The derived inverse address index (F14) is a superset by design: it keeps every successful
//! non-zero EVM-shaped address value, whatever its partition's version, under the logical name it
//! was written under, and leaves the version and link boundary to the reader (derived.rs,
//! `NODE_INSERT`; docs/projections.md). It reads the served payload, the `AddressChanged` half of
//! a coin-60 pair, from `value` or raw address bytes. The forward inventory assembly then narrows
//! the candidates to what is served.
//!
//! An index row reaches the resources whose pointer can admit it: a node-keyed row the pointers
//! at its node and resolver (or at a mirror resolver for that node), a row written under a name
//! also the pointers of that name's namehash at its resolver (the named arm admits by logical
//! name with no node test), and a record-id row every pointer at its resolver. Those are the
//! conditions under which the forward read's arms and link selection (`rows.rs`, `links.rs`) can
//! load the value, so every resource that serves the address is a candidate.
//!
//! A node-keyed row also reaches the ENSv1 pointers at its node when the node's ENSv1 registry
//! pointer (`project_registry_pointer`) names the row's resolver, because the forward read serves
//! an ENSv1 pointer through that registry pointer when the two disagree (`serving.rs`). A
//! record-id row likewise reaches the ENSv1 pointers at every node whose registry pointer names
//! its resolver.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

/// A resource whose records may answer for the address.
#[derive(Clone, Debug, Default)]
pub(crate) struct Candidate {
    /// The coin types the derived index found the resource through.
    pub(crate) indexed_coin_types: BTreeSet<String>,
}

/// The candidate statement: one piece per way a pointer can admit an index row, each joined on
/// equalities, so no pointer is tested against a key it shares only a chain with. A node-keyed
/// row reaches the pointers at its node, and a row written under a name those at that name's
/// namehash, in either case at the row's resolver or at a mirror resolver of its chain. The
/// `admitted` set pairs each row with those resolvers once. A record-id row reaches every
/// pointer at its resolver. A pointer that more than one piece or row reaches is one candidate
/// per coin type. The earlier statement also matched a pointer's resource against a named
/// resource that neither key arm ever set, so that unreachable arm is dropped.
///
/// The registry pieces join the node's registry pointer on its primary key, in the namespace the
/// row's logical name carries, or, for a row written under no name such as a record written
/// before Interpret surfaced the name, in each namespace the request spans (`$3`). The namespace
/// comes from the row or from the bound array, never from an array per row, which the planner
/// left out of the index condition and so walked the chain's registry pointers. Each piece reads
/// the inverse index itself, so the planner sizes it from the table's statistics: a filter on the
/// shared `node_keys` set is a guess, which picked a probe per row for an address whose rows are
/// all unnamed. They then reach the node's ENSv1 resource pointers on
/// `(chain_id, namespace, namehash)`. A pointer at the registry pointer's own resolver is left to
/// the node piece, which already reaches it.
///
/// A record-id row, a linked record such as a PermissionedResolver holds, carries no node, so
/// its registry piece reads the registry pointers at the row's resolver through
/// `project_registry_pointer_resolver_idx`, in each namespace the request spans, then the ENSv1
/// resource pointers at each pointer's node. That walks the registry pointers at the resolver,
/// the same class of walk the record-id piece below makes over the resource pointers at it. A
/// pointer at the registry pointer's own resolver is left to that piece.
pub(super) const CANDIDATES: &str = "WITH node_keys AS (
         SELECT chain_id, resolver_address, node,
                CASE WHEN logical_name_id <> ''
                     THEN lower(split_part(logical_name_id, ':', 2)) END AS named_namehash,
                coin_type
         FROM bigname_phase.project_address_record_node_index
         WHERE address = $1 AND coin_type = ANY($2::text[])
     ),
     mirrors AS (
         SELECT chain_id, resolver_address
         FROM bigname_phase.project_resolver_classification
         WHERE classification ->> 'role' = 'ensv1_mirror_resolver'
     ),
     admitted AS (
         SELECT chain_id, resolver_address, node, named_namehash, coin_type FROM node_keys
         UNION ALL
         SELECT key.chain_id, mirror.resolver_address, key.node, key.named_namehash,
                key.coin_type
         FROM node_keys key
         JOIN mirrors mirror ON mirror.chain_id = key.chain_id
     ),
     matched AS (
         SELECT pointer.chain_id, pointer.resource_id, admitted.coin_type
         FROM admitted
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = admitted.chain_id
          AND pointer.namehash = admitted.node
          AND pointer.resolver_address = admitted.resolver_address
         UNION ALL
         SELECT pointer.chain_id, pointer.resource_id, admitted.coin_type
         FROM admitted
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = admitted.chain_id
          AND pointer.namehash = admitted.named_namehash
          AND pointer.resolver_address = admitted.resolver_address
         UNION ALL
         SELECT pointer.chain_id, pointer.resource_id, key.coin_type
         FROM bigname_phase.project_address_record_node_index key
         JOIN bigname_phase.project_registry_pointer registry
           ON registry.chain_id = key.chain_id
          AND registry.namespace = split_part(key.logical_name_id, ':', 1)
          AND registry.node = key.node
          AND registry.resolver_address = key.resolver_address
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = registry.chain_id
          AND pointer.namespace = registry.namespace
          AND pointer.namehash = registry.node
          AND pointer.source_family IN
              ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
          AND pointer.resolver_address IS DISTINCT FROM registry.resolver_address
         WHERE key.address = $1 AND key.coin_type = ANY($2::text[]) AND key.logical_name_id <> ''
         UNION ALL
         SELECT pointer.chain_id, pointer.resource_id, key.coin_type
         FROM bigname_phase.project_address_record_node_index key
         JOIN bigname_phase.project_registry_pointer registry
           ON registry.chain_id = key.chain_id
          AND registry.namespace = ANY($3::text[])
          AND registry.node = key.node
          AND registry.resolver_address = key.resolver_address
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = registry.chain_id
          AND pointer.namespace = registry.namespace
          AND pointer.namehash = registry.node
          AND pointer.source_family IN
              ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
          AND pointer.resolver_address IS DISTINCT FROM registry.resolver_address
         WHERE key.address = $1 AND key.coin_type = ANY($2::text[]) AND key.logical_name_id = ''
         UNION ALL
         SELECT pointer.chain_id, pointer.resource_id, record.coin_type
         FROM bigname_phase.project_address_record_id_index record
         JOIN bigname_phase.project_registry_pointer registry
           ON registry.chain_id = record.chain_id
          AND registry.resolver_address = record.resolver_address
          AND registry.namespace = ANY($3::text[])
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = registry.chain_id
          AND pointer.namespace = registry.namespace
          AND pointer.namehash = registry.node
          AND pointer.source_family IN
              ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')
          AND pointer.resolver_address IS DISTINCT FROM registry.resolver_address
         WHERE record.address = $1 AND record.coin_type = ANY($2::text[])
         UNION ALL
         SELECT pointer.chain_id, pointer.resource_id, record.coin_type
         FROM bigname_phase.project_address_record_id_index record
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = record.chain_id
          AND pointer.resolver_address = record.resolver_address
         WHERE record.address = $1 AND record.coin_type = ANY($2::text[])
     )
     SELECT chain_id, resource_id, coin_type, true AS indexed
     FROM matched
     GROUP BY chain_id, resource_id, coin_type";

/// The candidate resources of `address` for `coin_types` from `source`, a row written under no
/// name reaching the registry pointers of each of `namespaces`, the namespaces the request spans.
pub(crate) async fn candidate_resources_from(
    conn: &mut PgConnection,
    address: &str,
    coin_types: &[String],
    namespaces: &[String],
) -> Result<BTreeMap<(String, Uuid), Candidate>> {
    let rows = sqlx::query(CANDIDATES)
        .bind(address)
        .bind(coin_types)
        .bind(namespaces)
        .fetch_all(&mut *conn)
        .await
        .with_context(|| format!("failed to find the family candidates of {address}"))?;
    let mut candidates: BTreeMap<(String, Uuid), Candidate> = BTreeMap::new();
    for row in rows {
        let candidate = candidates
            .entry((row.try_get("chain_id")?, row.try_get("resource_id")?))
            .or_default();
        if row.try_get::<bool, _>("indexed")? {
            candidate
                .indexed_coin_types
                .insert(row.try_get("coin_type")?);
        }
    }
    Ok(candidates)
}
