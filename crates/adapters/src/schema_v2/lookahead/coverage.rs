use super::V1NodeRequest;
use std::{cell::RefCell, collections::BTreeSet};

// The adapter prepare call is synchronous. This scope is only a rejection backstop for the
// explicit collector: it never discovers a successful scope by repeatedly interpreting.
thread_local! {
    static COVERAGE: RefCell<Option<Coverage>> = const { RefCell::new(None) };
}
struct Coverage {
    loaded: BTreeSet<String>,
    missing: BTreeSet<String>,
}
struct Clear;
impl Drop for Clear {
    fn drop(&mut self) {
        COVERAGE.with_borrow_mut(|scope| *scope = None);
    }
}

pub(super) fn checked<T>(
    nodes: &BTreeSet<V1NodeRequest>,
    operation: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    COVERAGE.with_borrow_mut(|scope| {
        anyhow::ensure!(scope.is_none(), "nested V1 lookahead preparation");
        *scope = Some(Coverage {
            loaded: nodes
                .iter()
                .map(|n| format!("{}:{}", n.namespace, n.node))
                .collect(),
            missing: BTreeSet::new(),
        });
        Ok::<_, anyhow::Error>(())
    })?;
    let _clear = Clear;
    let result = operation();
    let missing =
        COVERAGE.with_borrow_mut(|scope| std::mem::take(&mut scope.as_mut().unwrap().missing));
    anyhow::ensure!(
        missing.is_empty(),
        "V1 lookahead accessed unloaded nodes: {missing:?}"
    );
    result
}

pub(in crate::schema_v2) fn observe_node(key: &str) {
    COVERAGE.with_borrow_mut(|scope| {
        if let Some(scope) = scope
            && !scope.loaded.contains(key)
        {
            scope.missing.insert(key.to_owned());
        }
    });
}
