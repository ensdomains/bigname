//! Pins of interpreter output that must stay as it is. A pin keeps one line per output row in a
//! fixture file: the collection, a readable key for a normalized event, and a digest of the
//! row's `Debug`. A failing pin prints the rows that went away and the rows that came, with the
//! new rows in full, so a later change shows exactly what moved before anyone re-takes a pin.
use std::fmt::Debug;

use super::*;

/// A pinned row: its fixture line and its full `Debug`.
pub(super) struct PinRow {
    line: String,
    debug: String,
}

fn row(collection: &str, key: String, value: &impl Debug) -> PinRow {
    let debug = format!("{value:?}");
    let digest = keccak256(debug.as_bytes());
    PinRow {
        line: format!("{collection} {key} {}", &hex::encode(digest)[..16]),
        debug,
    }
}

/// The pinned rows of normalized events.
pub(super) fn event_rows<'a>(events: impl IntoIterator<Item = &'a NormalizedEvent>) -> Vec<PinRow> {
    events
        .into_iter()
        .map(|event| {
            let key = format!(
                "{}:{} {} {} {}",
                event.block_number.unwrap_or(-1),
                event.log_index.unwrap_or(-1),
                event.event_kind,
                event.source_family,
                event
                    .resource_id
                    .map_or_else(|| "-".to_owned(), |id| id.to_string())
            );
            row("normalized_events", key, event)
        })
        .collect()
}

/// The pinned rows of any other collection, keyed by position.
pub(super) fn rows<'a, T: Debug + 'a>(
    collection: &str,
    values: impl IntoIterator<Item = &'a T>,
) -> Vec<PinRow> {
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| row(collection, format!("#{index}"), value))
        .collect()
}

/// Every row of a batch output. The destructuring names every collection, so a new one cannot
/// be left out of a pin.
pub(super) fn output_rows(output: &BatchOutput) -> Vec<PinRow> {
    let BatchOutput {
        decode_skips,
        normalized_events,
        label_preimages,
        name_surfaces,
        token_lineages,
        resources,
        surface_bindings,
        binding_closures,
        contract_instances,
        contract_addresses,
        discovery_edges,
        discovery_edge_closures,
        migration_event_associations,
        migration_discovery_associations,
        migration_candidate_identity_effects,
        migration_candidate_discovery_effects,
        migration_authority_transitions,
    } = output;
    let mut all = rows("decode_skips", decode_skips);
    all.extend(event_rows(normalized_events));
    all.extend(rows("label_preimages", label_preimages));
    all.extend(rows("name_surfaces", name_surfaces));
    all.extend(rows("token_lineages", token_lineages));
    all.extend(rows("resources", resources));
    all.extend(rows("surface_bindings", surface_bindings));
    all.extend(rows("binding_closures", binding_closures));
    all.extend(rows("contract_instances", contract_instances));
    all.extend(rows("contract_addresses", contract_addresses));
    all.extend(rows("discovery_edges", discovery_edges));
    all.extend(rows("discovery_edge_closures", discovery_edge_closures));
    all.extend(rows(
        "migration_event_associations",
        migration_event_associations,
    ));
    all.extend(rows(
        "migration_discovery_associations",
        migration_discovery_associations,
    ));
    all.extend(rows(
        "migration_candidate_identity_effects",
        migration_candidate_identity_effects,
    ));
    all.extend(rows(
        "migration_candidate_discovery_effects",
        migration_candidate_discovery_effects,
    ));
    all.extend(rows(
        "migration_authority_transitions",
        migration_authority_transitions,
    ));
    all
}

/// Fails unless `actual` has exactly the lines of the `expected` fixture, in order.
pub(super) fn assert_pinned(name: &str, actual: &[PinRow], expected: &str) {
    assert!(
        pin_matches(name, actual, expected),
        "pinned output {name} moved: see the PIN lines above"
    );
}

/// Whether `actual` has exactly the lines of the `expected` fixture, in order. A mismatch
/// prints each line that went away, each line that came with its row in full, and the whole
/// new line list for the fixture.
pub(super) fn pin_matches(name: &str, actual: &[PinRow], expected: &str) -> bool {
    let expected = expected.lines().collect::<Vec<_>>();
    let lines = actual
        .iter()
        .map(|row| row.line.as_str())
        .collect::<Vec<_>>();
    if lines == expected {
        return true;
    }
    for line in expected.iter().filter(|line| !lines.contains(line)) {
        println!("PIN {name} - {line}");
    }
    for row in actual
        .iter()
        .filter(|row| !expected.contains(&row.line.as_str()))
    {
        println!("PIN {name} + {}", row.line);
        println!("PIN {name} + {}", row.debug);
    }
    for line in &lines {
        println!("PIN {name} = {line}");
    }
    false
}
