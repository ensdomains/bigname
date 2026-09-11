//! Relation-set predicates for reverse lookup inputs: which inputs the storage role groups
//! answer directly, which need the post-filter scan, and how a facet maps to a requested
//! relation. `resolves_to` belongs to neither path; `resolves_to.rs` serves it.

use crate::v2::{Relation, RelationSet};

pub(super) fn requires_relation_post_filter(relation: Option<&RelationSet>) -> bool {
    relation.is_some_and(|relation| {
        !relation.is_all()
            && !relation.is_exact_manager()
            && !relation.is_exact_owner_and_registrant()
            && !relation.is_resolves_to()
    })
}

pub(super) fn reverse_record_matches_relation(
    record: &bigname_storage::ReverseIdentityRecordRow,
    relation: Option<&RelationSet>,
) -> bool {
    relation.is_none_or(|relation| {
        record.relation_facets.iter().any(|facet| {
            relation
                .as_slice()
                .iter()
                .any(|relation| relation_to_storage(*relation) == Some(*facet))
        })
    })
}

pub(super) fn trim_reverse_record_relations(
    mut record: bigname_storage::ReverseIdentityRecordRow,
    relation: Option<&RelationSet>,
) -> bigname_storage::ReverseIdentityRecordRow {
    if let Some(relation) = relation {
        record.relation_facets.retain(|facet| {
            relation
                .as_slice()
                .iter()
                .any(|relation| relation_to_storage(*relation) == Some(*facet))
        });
    }
    record
}

pub(super) fn relation_to_storage(
    relation: Relation,
) -> Option<bigname_storage::AddressNameRelation> {
    match relation {
        Relation::Owner => Some(bigname_storage::AddressNameRelation::TokenHolder),
        Relation::Manager => Some(bigname_storage::AddressNameRelation::EffectiveController),
        Relation::Registrant => Some(bigname_storage::AddressNameRelation::Registrant),
        // Served from `address_records_current` by `resolves_to.rs`; never an authority facet.
        Relation::ResolvesTo => None,
    }
}
