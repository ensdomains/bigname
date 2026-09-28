use bigname_storage::{NameCurrentListCursor, NameCurrentListCursorValue};

use crate::v2::ErrorCode;

use super::*;

fn cursor_binding<'a>(
    q: &'a str,
    match_mode: SearchMatch,
    namespace: Option<&'a str>,
    public_namespaces: &'a [String],
) -> SearchCursorBinding<'a> {
    SearchCursorBinding {
        q,
        match_mode,
        namespace,
        public_namespaces,
    }
}

fn codeployed_public_namespaces() -> Vec<String> {
    vec!["basenames".to_owned(), "ens".to_owned()]
}

fn name_cursor() -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name("alpha.eth".to_owned()),
        namespace: "ens".to_owned(),
        normalized_name: "alpha.eth".to_owned(),
        namehash: "node:alpha.eth".to_owned(),
    }
}

fn read(binding: &SearchCursorBinding<'_>, cursor: &str) -> V2Result<NameCurrentListCursor> {
    let position = search_list_cursor(binding)
        .read(Some(cursor), &POSITION_KEYS)?
        .expect("a cursor was sent");
    search_storage_cursor(&position)
}

fn issued(binding: &SearchCursorBinding<'_>) -> crate::v2::CursorPayload {
    let cursor = search_list_cursor(binding)
        .next(search_position(&name_cursor()).expect("name cursor must encode"));
    crate::v2::decode(&cursor).expect("issued cursor decodes")
}

#[test]
fn search_cursor_payload_round_trips_name_cursor() {
    let public_namespaces = codeployed_public_namespaces();
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"), &public_namespaces);
    let payload = issued(&binding);

    assert_eq!(
        read(&binding, &crate::v2::encode(&payload)).expect("cursor must decode"),
        name_cursor()
    );
    assert_eq!(payload.sort, SEARCH_SORT);
    assert_eq!(payload.filters[Q_FILTER_KEY], "al");
    assert_eq!(payload.filters[MATCH_FILTER_KEY], "prefix");
    assert_eq!(payload.filters[NAMESPACE_FILTER_KEY], "ens");
    assert!(payload.snapshot.is_none());
}

#[test]
fn search_cursor_rejects_cross_filter_match_namespace_or_sort() {
    let public_namespaces = codeployed_public_namespaces();
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"), &public_namespaces);

    let mut payload = issued(&binding);
    payload
        .filters
        .insert(Q_FILTER_KEY.to_owned(), "be".to_owned());
    assert!(read(&binding, &crate::v2::encode(&payload)).is_err());

    let mut payload = issued(&binding);
    payload
        .filters
        .insert(MATCH_FILTER_KEY.to_owned(), "contains".to_owned());
    assert!(read(&binding, &crate::v2::encode(&payload)).is_err());

    let mut payload = issued(&binding);
    payload
        .filters
        .insert(NAMESPACE_FILTER_KEY.to_owned(), "basenames".to_owned());
    assert!(read(&binding, &crate::v2::encode(&payload)).is_err());

    let mut payload = issued(&binding);
    payload.sort = "name_desc".to_owned();
    assert!(read(&binding, &crate::v2::encode(&payload)).is_err());
}

// A current-state list cursor holds no snapshot (`list_cursor`): one carrying the snapshot
// component search cursors held before July 2026 is refused, and the client restarts.
#[test]
fn search_cursor_refuses_legacy_snapshot_component() {
    let public_namespaces = codeployed_public_namespaces();
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"), &public_namespaces);
    let mut payload = issued(&binding);
    payload.snapshot = Some("legacy-snapshot".to_owned());

    let error = read(&binding, &crate::v2::encode(&payload))
        .expect_err("a snapshot component is not part of the cursor");
    assert_eq!(error.code(), ErrorCode::InvalidInput);
}

#[test]
fn search_cursor_payload_rejects_non_name_storage_cursor() {
    let cursor = NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Timestamp(None),
        ..name_cursor()
    };
    let error = search_position(&cursor).expect_err("non-name cursor must not encode");

    assert_eq!(error.code(), ErrorCode::InternalError);
}

#[test]
fn bare_search_cursor_preserves_codeployed_encoding_and_binds_the_namespace_set() {
    let codeployed = codeployed_public_namespaces();
    let codeployed_binding = cursor_binding("al", SearchMatch::Prefix, None, &codeployed);
    let payload = issued(&codeployed_binding);

    assert_eq!(payload.filters.len(), 3);
    assert_eq!(payload.filters[NAMESPACE_FILTER_KEY], NONE_FILTER_VALUE);

    let ens_only = vec!["ens".to_owned()];
    let ens_only_binding = cursor_binding("al", SearchMatch::Prefix, None, &ens_only);
    assert!(read(&ens_only_binding, &crate::v2::encode(&payload)).is_err());
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
