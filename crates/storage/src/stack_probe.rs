//! Probe only, never merged: how deep the stack is, relative to `start`, at each `mark`.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

thread_local! {
    static BASE: Cell<usize> = const { Cell::new(0) };
    static DEPTHS: RefCell<BTreeMap<&'static str, usize>> = const { RefCell::new(BTreeMap::new()) };
}

#[inline(never)]
fn here() -> usize {
    let probe = 0u8;
    std::hint::black_box(&probe) as *const u8 as usize
}

#[inline(never)]
pub fn start() {
    BASE.with(|base| base.set(here()));
    DEPTHS.with(|depths| depths.borrow_mut().clear());
}

#[inline(never)]
pub fn mark(name: &'static str) {
    let base = BASE.with(Cell::get);
    if base == 0 {
        return;
    }
    let depth = base.saturating_sub(here());
    DEPTHS.with(|depths| {
        let mut depths = depths.borrow_mut();
        let entry = depths.entry(name).or_insert(0);
        if depth > *entry {
            *entry = depth;
        }
    });
}

pub fn report() -> String {
    DEPTHS.with(|depths| {
        let mut rows: Vec<_> = depths.borrow().iter().map(|(n, d)| (*d, *n)).collect();
        rows.sort();
        rows.iter()
            .map(|(d, n)| format!("DEPTH {:>9} {:>7.1} KiB {}", d, *d as f64 / 1024.0, n))
            .collect::<Vec<_>>()
            .join("\n")
    })
}
