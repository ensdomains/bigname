//! Class D of the registry-only holder table: an original child of the C-01 parent unwrapped
//! after the parent's locked ENSv1→ENSv2 migration, which opens no successor, so the child's
//! registry owner holds its registry-only resource with nothing selected. Each later write moves
//! that holder as any registry owner write does: to the owner it names, to the NameWrapper on a
//! re-wrap, to no one on a zero write, and to the new resolver on a resolver change. The rows
//! run with the NameWrapper selected before the unwrap ("orig", emancipated) and with the
//! registry-only authority selected before it ("lockaway"). Every row takes the locked re-run in
//! one batch, and runs in every layout of `every_layout`. The table's other classes are in
//! `unwrapped_child_under_wrapped_parent::registry_only_holder`.

use super::*;
use crate::schema_v2::tests::unwrapped_child_under_wrapped_parent::{
    LiveGrants,
    registry_only_holder::{
        HolderRow, assert_no_failures, held_by, holder_row_failures, registry_only_of,
    },
};

const GATE_NEXT_RESOLVER: &str = "0x00000000000000000000000000000000000000d3";

/// The child `label`'s history up to the parent's locked ENSv1→ENSv2 migration, and its unwrap
/// by GATE_OWNER to itself in the block after. "orig" is created wrapped with
/// PARENT_CANNOT_CONTROL and a resolver. "lockaway" is created wrapped with fuses 0, and its
/// record leaves the NameWrapper and comes back while the parent is unwrapped, so the
/// registry-only authority is selected at its unwrap.
fn locked_unwrap(
    fixture: &Value,
    label: &str,
) -> anyhow::Result<(Vec<RawLogInput>, Vec<RawLogInput>, String)> {
    let scenario = &fixture["scenarios"]["C-01"];
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let parent_node = scenario["parent"]["namehash"].as_str().unwrap();
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let node = namehash_under(&scenario["parent"]["namehash"], label)?;
    let mut history = if label == "orig" {
        let mut logs = gate_create_wrapped(230, 0, label, 65_536, fixture)?;
        logs.push(gate_new_resolver(230, 3, &node));
        logs
    } else {
        let mut logs = gate_create_wrapped(210, 0, label, 0, fixture)?;
        logs.push(gate_new_owner(211, 0, parent_node, label, GATE_AWAY_OWNER));
        logs.push(gate_new_owner(212, 0, parent_node, label, wrapper));
        logs
    };
    let level = level_logs(&scenario["parent"], &fixture["addresses"])?;
    assert!(
        level.iter().all(|raw| raw.block_number <= migration_block),
        "the parent's ENSv1→ENSv2 migration ends by its migration block"
    );
    history.extend(level);
    let unwrap = gate_unwrap_tail(migration_block + 1, &node, GATE_OWNER, wrapper);
    Ok((history, unwrap, node))
}

/// The writes after the locked unwrap, each in a block of its own, with the grants the child's
/// registry-only resource holds after them.
#[test]
fn a_write_after_a_locked_unwrap_moves_the_registry_only_holder() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let parent_label = scenario["parent"]["label"].as_str().unwrap();
    let after = scenario["parent"]["migration_block"].as_i64().unwrap() + 2;
    let mut rows = Vec::new();
    for (label, resolver) in [("orig", Some(GATE_RESOLVER)), ("lockaway", None)] {
        let (history, unwrap, node) = locked_unwrap(&fixture, label)?;
        let resource = registry_only_of(&node);
        let rewrap = gate_wrap_existing(after, 0, &node, &[label, parent_label, "eth"], wrapper);
        let row = |row, mut steps: Vec<Vec<RawLogInput>>, expected| {
            steps.insert(0, unwrap.clone());
            HolderRow::new(row, history.clone(), steps, resource, expected).rerun()
        };
        let orig = label == "orig";
        rows.push(row(
            if orig {
                "B10 the unwrap opens no successor, orig"
            } else {
                "B10 the unwrap opens no successor, lockaway"
            },
            Vec::new(),
            held_by(GATE_OWNER, resolver),
        ));
        let next_owner = if orig {
            GATE_AWAY_OWNER
        } else {
            GATE_PROBE_OWNER
        };
        rows.push(
            row(
                if orig {
                    "D1 setOwner, orig"
                } else {
                    "D6 setOwner, lockaway"
                },
                vec![vec![gate_registry_transfer(after, 0, &node, next_owner)]],
                held_by(next_owner, resolver),
            )
            .lookahead(),
        );
        rows.push(row(
            if orig {
                "D2 re-wrap, orig"
            } else {
                "D7 re-wrap, lockaway"
            },
            vec![rewrap.clone()],
            held_by(wrapper, resolver),
        ));
        // `wrap` set no fuses, so the re-wrapped child unwraps again.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
        rows.push(row(
            if orig {
                "D2 re-wrap, then an unwrap to another account, orig"
            } else {
                "D7 re-wrap, then an unwrap to another account, lockaway"
            },
            vec![
                rewrap,
                gate_unwrap_into(
                    (after + 1, 0),
                    &node,
                    (GATE_OWNER, GATE_TRANSFEREE),
                    wrapper,
                ),
            ],
            held_by(GATE_TRANSFEREE, resolver),
        ));
        rows.push(row(
            if orig {
                "D4 zero write, orig"
            } else {
                "D7 zero write, lockaway"
            },
            vec![vec![gate_registry_transfer(after, 0, &node, ZERO_ADDRESS)]],
            LiveGrants::new(),
        ));
        rows.push(row(
            if orig {
                "D5 setResolver, orig"
            } else {
                "D7 setResolver, lockaway"
            },
            vec![vec![gate_resolver_at(
                (after, 0, 0),
                &node,
                GATE_NEXT_RESOLVER.parse()?,
            )]],
            held_by(GATE_OWNER, Some(GATE_NEXT_RESOLVER)),
        ));
    }
    let build = |logs: Vec<RawLogInput>| gate_batch(logs, &fixture);
    assert_no_failures(holder_row_failures(&build, rows)?)
}
