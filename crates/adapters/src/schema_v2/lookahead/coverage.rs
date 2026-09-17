use super::V1NodeRequest;
use std::{cell::RefCell, collections::BTreeSet};

// The adapter restore and prepare calls are synchronous. This scope is only a rejection
// backstop for the explicit collector: it never discovers a successful scope by repeatedly
// interpreting.
//
// A read of per-name ENSv1 state is reported here when its key is built by `v1_key` or
// `v1_surface_key` (state_registrar.rs). Reads that build or receive their key another way,
// and why each cannot reach a name that was not loaded:
//
// - `settle_v1_releases` (state_expiry.rs) takes keys from the expiry index. It reports
//   every due key itself before releasing it.
// - `known_surfaces` and `active_resources` reads and writes keyed by a stored
//   `logical_name_id` (`promote_known_v1_authority`, `observe_v1_active_surface`,
//   `materialize_v1_active_surface`, `bind_v1_active_surface` in state_surfaces.rs;
//   `observe_v1_name`, `observe_v1_registrar`, `activate_v1_authority`,
//   `reactivate_v1_registrar*` in state.rs; `release_v1_name` in state_expiry.rs): each
//   sits in a function that calls `v1_key` for the same name first, or is handed the key
//   its caller built with `v1_key`, so the name is already reported.
// - `v1_registry_authority_if_authentic` (state.rs) and the binding helpers in
//   state_surfaces.rs take a prebuilt key: callers pass a `v1_key` result, except
//   `settle_v1_releases`, covered above.
// - `v1_registrar_controllers`, `v1_pending_wrapper_sync_expiries` and the renewal
//   expiries of `v1_registrar_transaction` (state_wrapper.rs) are iterated and cleared
//   whole, but they are emptied at the start of every transaction, so they never hold
//   state from before the batch.
// - `surface_removal_candidates` (state_incremental.rs) is iterated whole; it holds only
//   names the current batch or restore touched through the functions above. The wholesale
//   map replacement in the same file moves a session's state, it reads no name.
// - `name_link_by_namehash` (state_v2.rs) and the other ENSv2 writers of the shared maps
//   run only for ENSv2 source families, for which lookahead is never chosen.
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
