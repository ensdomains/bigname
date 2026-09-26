//! Same-log ties in the record inventory's boundary and winner selection. Synthetic: no adapter
//! emits two record facts for one record key at one log (the coin-60 `AddrChanged` sibling is one
//! log later, crates/project/src/families/records.rs:270-277), and no fixture carries a version
//! change and a write at one log. The emission ordinal (docs/glossary.md#emission-ordinal) decides
//! each tie here, with identity bytes ordering the other way, so a comparator that skips the
//! ordinal fails every case.
use serde_json::Value;

use super::{Boundary, FamilyPosition, RecordCandidate, combined_boundary, latest_eligible};

const KEY: &str = "text:url";

/// A position at block 20, transaction 0, log 3.
fn at_log(identity: &str) -> FamilyPosition {
    FamilyPosition {
        block_number: 20,
        transaction_index: Some(0),
        log_index: Some(3),
        event_identity: identity.to_owned(),
    }
}

fn write(identity: &str) -> RecordCandidate {
    RecordCandidate {
        record_key: KEY.to_owned(),
        position: at_log(identity),
        normalized_event_id: None,
        source_family: "ens_v1_resolver_l1".to_owned(),
        status: "success".to_owned(),
        payload: Value::Null,
        sibling_position: None,
    }
}

fn version(identity: &str) -> Boundary {
    (at_log(identity), "RecordVersionChanged", None)
}

fn winner(candidates: Vec<RecordCandidate>, cutoff: Option<&FamilyPosition>) -> Option<String> {
    latest_eligible(candidates, cutoff)
        .remove(KEY)
        .map(|winner| winner.position.event_identity)
}

#[test]
fn the_higher_ordinal_write_of_one_key_at_one_log_wins() {
    // "a:1" sorts before "b:0" as bytes; its ordinal 1 makes it the later write.
    for candidates in [
        vec![write("b:0"), write("a:1")],
        vec![write("a:1"), write("b:0")],
    ] {
        assert_eq!(winner(candidates, None).as_deref(), Some("a:1"));
    }
}

#[test]
fn a_version_change_at_the_same_log_cuts_off_by_ordinal() {
    // The version change has ordinal 0 and the write ordinal 1: the write is after the cutoff.
    let (_, cutoff) = combined_boundary(vec![version("b:0")]);
    assert_eq!(
        winner(vec![write("a:1")], cutoff.as_ref()).as_deref(),
        Some("a:1")
    );
    // The version change has ordinal 1 and the write ordinal 0: the write is cut off.
    let (_, cutoff) = combined_boundary(vec![version("a:1")]);
    assert_eq!(winner(vec![write("b:0")], cutoff.as_ref()), None);
}

#[test]
fn the_combined_boundary_is_the_higher_ordinal() {
    // A link with ordinal 0 and a version change with ordinal 1 at one log: the version change is
    // the boundary and cuts off.
    let (boundary, cutoff) = combined_boundary(vec![
        (at_log("b:0"), "ResolverRecordLinked", Some(7)),
        version("a:1"),
    ]);
    assert_eq!(
        boundary.map(|(position, kind, _)| (position.event_identity, kind)),
        Some(("a:1".to_owned(), "RecordVersionChanged"))
    );
    assert_eq!(cutoff, Some(at_log("a:1")));
    // The reverse: the link has ordinal 1, so it is the boundary and nothing is cut off.
    let (boundary, cutoff) = combined_boundary(vec![
        version("b:0"),
        (at_log("a:1"), "ResolverRecordLinked", Some(7)),
    ]);
    assert_eq!(
        boundary.map(|(_, kind, _)| kind),
        Some("ResolverRecordLinked")
    );
    assert_eq!(cutoff, None);
}
