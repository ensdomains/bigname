//! A test's record of the jsonb arrays the family statements bind on its thread, and the bind
//! budget it sets there.
use std::cell::{Cell, RefCell};

use serde_json::Value;

/// One bound array: the statement's tag, its JSON text in bytes and its element count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Bind {
    pub(super) tag: &'static str,
    pub(super) bytes: usize,
    pub(super) elements: usize,
}

thread_local! {
    static BUDGET: Cell<Option<usize>> = const { Cell::new(None) };
    /// `None` unless a test records, so no other test pays for serializing its binds.
    static BINDS: RefCell<Option<Vec<Bind>>> = const { RefCell::new(None) };
}

pub(super) fn record(tag: &'static str, values: &[Value]) {
    BINDS.with_borrow_mut(|binds| {
        if let Some(binds) = binds {
            binds.push(Bind {
                tag,
                bytes: serde_json::to_vec(values).map_or(usize::MAX, |text| text.len()),
                elements: values.len(),
            });
        }
    });
}

/// Record every bind on this thread from now until `take`.
pub(super) fn start() {
    BINDS.set(Some(Vec::new()));
}

/// The binds recorded since `start`, which stops recording.
pub(super) fn take() -> Vec<Bind> {
    BINDS.take().unwrap_or_default()
}

/// The bind budget on this thread: `None` keeps the one in code.
pub(super) fn set_budget(bytes: Option<usize>) {
    BUDGET.set(bytes);
}

pub(super) fn budget() -> Option<usize> {
    BUDGET.get()
}
