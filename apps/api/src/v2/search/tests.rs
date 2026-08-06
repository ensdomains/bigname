use bigname_storage::{NameCurrentListCursor, NameCurrentListCursorValue};

use crate::v2::ErrorCode;

use super::*;

fn cursor_binding<'a>(
    q: &'a str,
    match_mode: SearchMatch,
    namespace: Option<&'a str>,
) -> SearchCursorBinding<'a> {
    SearchCursorBinding {
        q,
        match_mode,
        namespace,
    }
}

fn name_cursor() -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name("alpha.eth".to_owned()),
        namespace: "ens".to_owned(),
        normalized_name: "alpha.eth".to_owned(),
        namehash: "node:alpha.eth".to_owned(),
    }
}

#[test]
fn search_cursor_payload_round_trips_name_cursor() {
    let cursor = name_cursor();
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"));
    let payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");

    assert_eq!(
        search_storage_cursor(&payload, &binding).expect("cursor must decode"),
        cursor
    );
    assert_eq!(payload.sort, SEARCH_SORT);
    assert_eq!(payload.filters[Q_FILTER_KEY], "al");
    assert_eq!(payload.filters[MATCH_FILTER_KEY], "prefix");
    assert_eq!(payload.filters[NAMESPACE_FILTER_KEY], "ens");
    assert!(payload.snapshot.is_none());
}

#[test]
fn search_cursor_rejects_cross_filter_match_namespace_or_sort() {
    let cursor = name_cursor();
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"));

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload
        .filters
        .insert(Q_FILTER_KEY.to_owned(), "be".to_owned());
    assert!(search_storage_cursor(&payload, &binding).is_err());

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload
        .filters
        .insert(MATCH_FILTER_KEY.to_owned(), "contains".to_owned());
    assert!(search_storage_cursor(&payload, &binding).is_err());

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload
        .filters
        .insert(NAMESPACE_FILTER_KEY.to_owned(), "basenames".to_owned());
    assert!(search_storage_cursor(&payload, &binding).is_err());

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload.sort = "name_desc".to_owned();
    assert!(search_storage_cursor(&payload, &binding).is_err());
}

#[test]
fn search_cursor_ignores_legacy_snapshot_component() {
    let cursor = name_cursor();
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"));
    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload.snapshot = Some("legacy-snapshot".to_owned());

    assert_eq!(
        search_storage_cursor(&payload, &binding)
            .expect("legacy snapshot component must not bind a latest-state cursor"),
        cursor
    );
}

#[test]
fn search_cursor_payload_rejects_non_name_storage_cursor() {
    let cursor = NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Timestamp(None),
        ..name_cursor()
    };
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"));

    let error =
        search_cursor_payload(&cursor, &binding).expect_err("non-name cursor must not encode");

    assert_eq!(error.code(), ErrorCode::InternalError);
}

#[test]
fn search_query_requires_q_and_parses_match_controls() {
    let parsed = SearchQueryParams::try_from(RawSearchQueryParams {
        q: Some(" AL ".to_owned()),
        ..RawSearchQueryParams::default()
    })
    .expect("default search params must parse");
    assert_eq!(parsed.q, "al");
    assert_eq!(parsed.match_mode, SearchMatch::Prefix);

    let contains = SearchQueryParams::try_from(RawSearchQueryParams {
        q: Some("ha".to_owned()),
        match_mode: Some("contains".to_owned()),
        ..RawSearchQueryParams::default()
    })
    .expect("contains match must parse");
    assert_eq!(contains.match_mode, SearchMatch::Contains);

    for raw in [
        RawSearchQueryParams::default(),
        RawSearchQueryParams {
            q: Some(" ".to_owned()),
            ..RawSearchQueryParams::default()
        },
        RawSearchQueryParams {
            q: Some("al".to_owned()),
            match_mode: Some("suffix".to_owned()),
            ..RawSearchQueryParams::default()
        },
        RawSearchQueryParams {
            q: Some("al".to_owned()),
            namespace: Some("internal".to_owned()),
            ..RawSearchQueryParams::default()
        },
    ] {
        assert!(SearchQueryParams::try_from(raw).is_err());
    }
}
