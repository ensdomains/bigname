//! `resolves_to` relation-set canonicalization.

use super::*;

#[test]
fn resolves_to_relation_set_stands_alone() {
    let set = RelationSet::from_relations([Relation::ResolvesTo, Relation::ResolvesTo])
        .expect("resolves_to set must canonicalize");
    assert!(set.is_resolves_to());
    assert!(!set.is_all());
    assert_eq!(set.canonical_value(), "resolves_to");
    assert!(RelationSet::from_relations([Relation::ResolvesTo, Relation::Owner]).is_none());
    assert!(!RelationSet::all().is_resolves_to());
    assert!(
        !RelationSet::all()
            .as_slice()
            .contains(&Relation::ResolvesTo)
    );
}
