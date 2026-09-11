//! `relation=resolves_to` parsing rules for the shared relation-set parameter.

use super::*;

#[test]
fn resolves_to_relation_parses_alone_and_rejects_mixing() {
    let params = parse(RawQueryParams {
        relation: Some("resolves_to".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("resolves_to must parse");
    assert!(
        params
            .relation
            .as_ref()
            .is_some_and(RelationSet::is_resolves_to)
    );

    for mixed in [
        "resolves_to,owner",
        "any,resolves_to",
        "registrant,resolves_to",
    ] {
        let error = parse(RawQueryParams {
            relation: Some(mixed.to_owned()),
            ..RawQueryParams::default()
        })
        .expect_err("mixed resolves_to set must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
        let message = error.envelope().error.message;
        assert!(message.contains("resolves_to"), "{mixed}: {message}");
    }
    // `any` stays the three authority relations.
    let params = parse(RawQueryParams {
        relation: Some("any".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("any must parse");
    assert!(
        !params
            .relation
            .as_ref()
            .is_some_and(RelationSet::is_resolves_to)
    );
}
