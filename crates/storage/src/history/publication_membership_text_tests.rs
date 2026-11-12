//! The live publication matcher's statement, frozen as it was before the matcher's arms became
//! optional: the production writer must still emit it byte for byte.
use sqlx::{Postgres, QueryBuilder};

use super::{
    Address, AddressNameRelation, publication_memberships, push_address_match_filter,
    push_evidence_eligibility,
};

fn frozen(chain_id: &str, block_number: i64, names: &[String], resources: &[uuid::Uuid]) -> String {
    let mut query = QueryBuilder::<Postgres>::new(
        "/* storage:history.publication_memberships */ WITH evidence AS (
        SELECT ne.namespace, ne.logical_name_id, ne.resource_id, ne.event_kind,
            ne.source_family, ne.chain_id, ne.block_number, ne.block_hash,
            ne.transaction_index, ne.log_index, ne.derivation_kind,
            ne.canonicality_state, ne.consumer_visibility, ne.after_state FROM unnest(",
    );
    query.push_bind(names).push(
        "::text[]) wanted(name)
        CROSS JOIN LATERAL (SELECT ne.namespace, ne.logical_name_id, ne.resource_id, ne.event_kind,
            ne.source_family, ne.chain_id, ne.block_number, ne.block_hash,
            ne.transaction_index, ne.log_index, ne.derivation_kind,
            ne.canonicality_state, ne.consumer_visibility, ne.after_state FROM normalized_events ne
            WHERE ne.logical_name_id = wanted.name AND ne.chain_id = ",
    );
    query
        .push_bind(chain_id)
        .push(" AND ne.block_number <= ")
        .push_bind(block_number);
    query
        .push(
            " AND ne.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
              ) OFFSET 0) ne UNION ALL
        SELECT ne.namespace, ne.logical_name_id, ne.resource_id, ne.event_kind,
            ne.source_family, ne.chain_id, ne.block_number, ne.block_hash,
            ne.transaction_index, ne.log_index, ne.derivation_kind,
            ne.canonicality_state, ne.consumer_visibility, ne.after_state FROM unnest(",
        )
        .push_bind(resources)
        .push(
            "::uuid[]) wanted(resource)
        CROSS JOIN LATERAL (SELECT ne.namespace, ne.logical_name_id, ne.resource_id, ne.event_kind,
            ne.source_family, ne.chain_id, ne.block_number, ne.block_hash,
            ne.transaction_index, ne.log_index, ne.derivation_kind,
            ne.canonicality_state, ne.consumer_visibility, ne.after_state FROM normalized_events ne
            WHERE ne.resource_id = wanted.resource AND ne.chain_id = ",
        );
    query
        .push_bind(chain_id)
        .push(" AND ne.block_number <= ")
        .push_bind(block_number);
    query.push(
        " AND ne.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
          ) OFFSET 0) ne
        WHERE (ne.logical_name_id IS NULL OR ne.logical_name_id <> ALL(",
    );
    query.push_bind(names);
    query.push(
        "::text[])))
        SELECT DISTINCT matched_address.address, ne.namespace, ne.logical_name_id,
               ne.resource_id, relation.mask AS relation_mask
        FROM evidence ne
        CROSS JOIN LATERAL (
            SELECT DISTINCT lower(address) AS address FROM (VALUES
                (ne.after_state ->> 'registrant'), (ne.after_state ->> 'to'),
                (ne.after_state ->> 'owner')) candidate(address)
            WHERE address IS NOT NULL
        ) matched_address
        LEFT JOIN resources r ON r.resource_id = ne.resource_id
        LEFT JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = r.chain_id
         AND resource_lineage.block_hash = r.block_hash ",
    );
    query.push(
        " CROSS JOIN LATERAL (
        SELECT relation.mask FROM (VALUES (1::smallint), (2::smallint)) relation(mask)
        WHERE (relation.mask = 1 AND ",
    );
    push_address_match_filter(
        &mut query,
        Address::Candidate,
        Some(&[AddressNameRelation::TokenHolder]),
    );
    query.push(") OR (relation.mask = 2 AND ");
    push_address_match_filter(
        &mut query,
        Address::Candidate,
        Some(&[AddressNameRelation::EffectiveController]),
    );
    query.push(") OFFSET 0) relation ");
    push_evidence_eligibility(&mut query, true, false);
    query.into_sql()
}

#[test]
fn the_live_matcher_statement_is_unchanged() {
    let names = vec!["ens:a".to_owned(), "ens:b".to_owned()];
    let resources = vec![uuid::Uuid::nil()];
    let live = publication_memberships("ethereum-sepolia", 7, &names, &resources);
    assert_eq!(
        live.sql(),
        frozen("ethereum-sepolia", 7, &names, &resources)
    );
}
