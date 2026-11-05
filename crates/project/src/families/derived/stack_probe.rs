//! Probe only, never merged: the sizes of the family step's futures.
#![allow(unreachable_code, unused_variables, clippy::diverging_sub_expression)]

fn output_size<R>(_: &impl FnOnce() -> R) -> usize {
    std::mem::size_of::<R>()
}

macro_rules! size {
    ($out:ident, $name:literal, $call:expr) => {{
        let make = || $call;
        $out.push(format!("FUTSIZE {:>8} {}", output_size(&make), $name));
    }};
}

#[test]
fn stack_probe_future_sizes() {
    let mut out: Vec<String> = Vec::new();
    size!(out, "families::apply", crate::families::apply(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "families::undo_to", crate::families::undo_to(unreachable!(), unreachable!(), unreachable!()));
    size!(out, "block::publish", super::super::block::publish(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "derived::refresh_block", super::refresh_block(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "derived::refresh", super::refresh(unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "summary::refresh", super::summary::refresh(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "summary::compose", super::summary::compose(unreachable!(), unreachable!(), unreachable!()));
    size!(out, "storage::compose_name_summary_publication", bigname_storage::families::name::compose_name_summary_publication(unreachable!(), unreachable!(), unreachable!()));
    size!(out, "lookup::refresh", super::super::lookup::refresh(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "resolution_paths::refresh", super::super::resolution_paths::refresh(unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "history_catalogue::refresh", super::super::history_catalogue::refresh(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "children::refresh", super::children::refresh(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    size!(out, "compose::names_chunk", super::super::compose::names_chunk(unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!(), unreachable!()));
    out.push(format!("TYPESIZE {:>8} NameSummaryPublication", std::mem::size_of::<bigname_storage::families::name::NameSummaryPublication>()));
    out.push(format!("TYPESIZE {:>8} FamilyPublication", std::mem::size_of::<bigname_storage::families::name::FamilyPublication>()));
    panic!("STACK PROBE\n{}", out.join("\n"));
}
