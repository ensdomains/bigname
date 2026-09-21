use bigname_storage::{
    AddressNameRelation, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort,
};

use crate::v2::{AddressNamesDedupe, AddressNamesSort, Relation, RelationSet, SortOrder};

/// The `address_names_current` relation an authority relation reads. `resolves_to` reads
/// `address_records_current` instead and has no storage relation here.
pub(crate) fn relation_to_storage(relation: Relation) -> Option<AddressNameRelation> {
    match relation {
        Relation::Owner => Some(AddressNameRelation::TokenHolder),
        Relation::Manager => Some(AddressNameRelation::EffectiveController),
        Relation::Registrant => Some(AddressNameRelation::Registrant),
        Relation::ResolvesTo => None,
    }
}

pub(crate) fn relation_set_to_storage(relation_set: &RelationSet) -> Vec<AddressNameRelation> {
    relation_set
        .as_slice()
        .iter()
        .copied()
        .filter_map(relation_to_storage)
        .collect()
}

pub(crate) fn relation_from_storage(relation: AddressNameRelation) -> Relation {
    match relation {
        AddressNameRelation::TokenHolder => Relation::Owner,
        AddressNameRelation::EffectiveController => Relation::Manager,
        AddressNameRelation::Registrant => Relation::Registrant,
    }
}

pub(crate) fn dedupe_to_storage(dedupe: AddressNamesDedupe) -> AddressNamesCurrentDedupe {
    match dedupe {
        AddressNamesDedupe::Name => AddressNamesCurrentDedupe::Surface,
        AddressNamesDedupe::Registration => AddressNamesCurrentDedupe::Resource,
    }
}

pub(crate) fn sort_to_storage(sort: AddressNamesSort) -> AddressNamesCurrentSort {
    match sort {
        AddressNamesSort::Name => AddressNamesCurrentSort::Name,
        AddressNamesSort::ExpiresAt => AddressNamesCurrentSort::ExpiresAt,
        AddressNamesSort::RegisteredAt => AddressNamesCurrentSort::RegisteredAt,
    }
}

pub(crate) fn order_to_storage(order: SortOrder) -> AddressNamesCurrentOrder {
    match order {
        SortOrder::Asc => AddressNamesCurrentOrder::Asc,
        SortOrder::Desc => AddressNamesCurrentOrder::Desc,
    }
}
