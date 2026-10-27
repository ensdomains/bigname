use super::{BlockMarker, HeadMarkers, retain_higher_finality};

fn marker(number: i64) -> BlockMarker {
    BlockMarker {
        number,
        hash: format!("0x{number:x}"),
    }
}

#[test]
fn lower_provider_markers_keep_the_stored_ones() {
    let proposed = HeadMarkers {
        latest: marker(100),
        safe: Some(marker(60)),
        finalized: Some(marker(30)),
    };
    let retained = retain_higher_finality("chain", &proposed, Some(&marker(90)), Some(&marker(40)));
    assert_eq!(retained.latest, marker(100));
    assert_eq!(retained.safe, Some(marker(90)));
    assert_eq!(retained.finalized, Some(marker(40)));
}

#[test]
fn equal_or_higher_provider_markers_are_published_as_reported() {
    let proposed = HeadMarkers {
        latest: marker(100),
        safe: Some(marker(95)),
        finalized: Some(marker(40)),
    };
    let retained = retain_higher_finality("chain", &proposed, Some(&marker(90)), Some(&marker(40)));
    assert_eq!(retained, proposed);
}

#[test]
fn missing_markers_are_left_for_the_publication_check() {
    let proposed = HeadMarkers {
        latest: marker(100),
        safe: None,
        finalized: None,
    };
    let retained = retain_higher_finality("chain", &proposed, Some(&marker(90)), None);
    assert_eq!(retained, proposed);
    let retained = retain_higher_finality("chain", &proposed, None, None);
    assert_eq!(retained, proposed);
}
