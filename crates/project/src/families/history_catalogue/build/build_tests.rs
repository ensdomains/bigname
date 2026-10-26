//! The catalogue build's cursor, partition order and persisted relations, without a database.
use uuid::Uuid;

use super::{Cursor, Layout, PersistedRelation, Step, analyzed, is_done, next, spans};

const LAYOUT: Layout = Layout {
    spans: 3,
    partitions: 2,
};

/// Every partition of a build of `layout`, in order, from no cursor to the terminal one.
fn walk(layout: Layout) -> Vec<Cursor> {
    let mut cursors = Vec::new();
    let mut text: Option<String> = None;
    while !is_done(text.as_deref()).expect("a cursor the build wrote parses") {
        let cursor = next(text.as_deref(), layout, 170_004).expect("the next partition");
        text = Some(cursor.text());
        cursors.push(cursor);
    }
    cursors
}

#[test]
fn the_build_runs_every_partition_of_every_step_once_in_order() {
    let cursors = walk(LAYOUT);
    let seen: Vec<(&str, u16)> = cursors
        .iter()
        .map(|cursor| (cursor.step.name(), cursor.part))
        .collect();
    assert_eq!(
        seen,
        [
            ("keys", 0),
            ("spans", 0),
            ("spans", 1),
            ("spans", 2),
            ("edges", 0),
            ("edges", 1),
            ("sources", 0),
            ("sources", 1),
            ("edge_bounds", 0),
            ("edge_bounds", 1),
            ("name_anchors", 0),
            ("name_anchors", 1),
            ("resource_anchors", 0),
            ("resource_anchors", 1),
            ("done", 0),
        ]
    );
    // 1 + S + 5K + 1 transactions.
    assert_eq!(
        walk(Layout {
            spans: 64,
            partitions: 16
        })
        .len(),
        1 + 64 + 5 * 16 + 1
    );
    let analyzing: Vec<(&str, &[&str])> = cursors
        .iter()
        .filter(|cursor| !analyzed(cursor).is_empty())
        .map(|cursor| (cursor.step.name(), analyzed(cursor)))
        .collect();
    assert_eq!(
        analyzing,
        [
            ("spans", &["project_compose_history_work"][..]),
            ("edges", &["project_history_source_edge"][..]),
            ("sources", &["project_history_source"][..]),
            ("edge_bounds", &["project_history_source_edge"][..]),
            ("resource_anchors", &["project_address_history_anchor"][..]),
        ]
    );
}

#[test]
fn a_build_resumed_with_other_partitions_or_on_another_server_is_refused() {
    let cursor = Cursor {
        step: Step::Sources,
        part: 1,
        layout: LAYOUT,
        server_version_num: 170_004,
    };
    let text = cursor.text();
    let fields: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(
        fields,
        serde_json::json!({"step": "sources", "part": 1, "spans": 3, "partitions": 2,
                           "server_version_num": 170_004})
    );
    assert_eq!(Cursor::parse(&text).ok(), Some(cursor));
    assert_eq!(
        next(Some(&text), LAYOUT, 170_004).map(|c| c.step).ok(),
        Some(Step::EdgeBounds)
    );
    for (layout, version) in [
        (
            Layout {
                spans: 4,
                partitions: 2,
            },
            170_004,
        ),
        (
            Layout {
                spans: 3,
                partitions: 4,
            },
            170_004,
        ),
        (LAYOUT, 170_005),
    ] {
        let error = next(Some(&text), layout, version)
            .expect_err("refused")
            .to_string();
        assert!(
            error.contains("(3, 2) on server version 170004")
                && error.contains(&format!(
                    "({}, {}) on server version {version}",
                    layout.spans, layout.partitions
                )),
            "{error}"
        );
    }
    assert!(next(Some("7"), LAYOUT, 170_004).is_err());
    assert!(is_done(Some("not json")).is_err());
}

#[test]
fn the_block_spans_cover_every_block_to_the_pass_once() {
    for pass in [0, 1, 5, 299, 300, 24_000_000] {
        for spans_count in [1, 2, 3, 64, 1000] {
            let mut expected = 0;
            for span in 0..spans_count {
                let (from, to) = spans::bounds(pass, span, spans_count);
                assert_eq!(
                    from, expected,
                    "pass {pass}, {spans_count} spans, span {span}"
                );
                assert!(to >= from);
                expected = to;
            }
            assert_eq!(expected, pass + 1, "pass {pass}, {spans_count} spans");
        }
    }
}

#[test]
fn a_persisted_relation_reads_back_as_written() {
    let relation = PersistedRelation {
        address: "0x00000000000000000000000000000000000000aA".to_owned(),
        namespace: "ens".to_owned(),
        resource_id: Uuid::from_u128(0x387),
        relation: "token_holder".to_owned(),
    };
    let json = relation.to_json();
    assert_eq!(
        json,
        serde_json::json!({"address": "0x00000000000000000000000000000000000000aA",
                           "namespace": "ens", "relation": "token_holder",
                           "resource_id": "00000000-0000-0000-0000-000000000387"})
    );
    assert_eq!(PersistedRelation::from_json(&json), Some(relation));
}
