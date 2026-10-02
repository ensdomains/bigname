use super::{V1BatchDependencies, V1NodeRequest};
use std::{cell::RefCell, collections::BTreeSet};

// The adapter restore and prepare calls are synchronous. A restore or batch that reads a key
// outside the loaded set fails with `UnloadedKeys`, and its output is discarded. The loader
// may load those keys as well and try again: the collector cannot know every key in advance,
// because ENSv2 derives a token's name from registry state during the batch. Only an attempt
// that read no unloaded key is used, so the certificate below is the same however many
// attempts it took.
//
// Two kinds of key are checked:
//
// - A name (`namespace:namehash`). Loading it loads every retained event filed under that
//   name, in every covered family.
// - An [ENSv2 state key](../../../../../docs/glossary.md#ensv2-state-key) (`address:id`,
//   see `v2_key`), or a whole registry (`address:*`). Loading it loads every retained ENSv2
//   event that `v2_event_keys` files under it.
//
// A read of per-name ENSv1-model state (the ENSv1 families and the Basenames Base families,
// which share that state) is reported here when its key is built by `v1_key` or
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
//
// ENSv2 state lives in the maps of state_v2_maps.rs, whose keyed reads and writes report
// their key. The exceptions:
//
// - The expiry index is range-scanned for the tokens whose expiry a refresh crosses. A batch
//   refresh reports its window, which must lie inside the window whose due tokens the loader
//   loaded (`v2_due_window`). A refresh that ends at or before the window's start is the
//   restore's own catch-up to the batch's predecessor: it changes only loaded tokens, and
//   each of those reports its key again when the batch reads it.
// - A restore re-derives the names of the loaded tokens of a registry it marks dirty, such as
//   one whose restored `ParentChanged` or crossed expiry renames its tokens, without loading
//   the whole registry: a token it did not load is not read by the batch, and if the batch
//   does read it the retry loads it and restores again. A batch that marks a registry dirty
//   reads every token in it, because each of them emits its new name.
// - Restore finish and `replace_v2_suffix_anchors` re-derive the name of every loaded token.
//   Each derivation reads the token's ancestors through the reporting maps, and a token that
//   is not loaded is not read by the batch.
// - `latest_v2_timestamp` is the latest restored ENSv2 topology timestamp; the loader reads
//   it for the whole chain and supplies it to the restore.
// - The dirty sets and terminal-closure hits are emptied by every refresh, so they hold only
//   keys the restore or batch touched through the maps.
//
// Where ENSv2 code reads the name-keyed state it shares with ENSv1-model names
// (`known_surfaces`, `active_resources` and the surface counts), it reports the name with
// `observe_name`. Its writes there insert or overwrite a value regardless of the prior one.
thread_local! {
    static COVERAGE: RefCell<Option<Coverage>> = const { RefCell::new(None) };
}
struct Coverage {
    names: BTreeSet<String>,
    v2_keys: BTreeSet<String>,
    v2_due_window: Option<(i64, i64)>,
    restoring: bool,
    missing_names: BTreeSet<String>,
    missing_v2_keys: BTreeSet<String>,
    uncovered_window: Option<(i64, i64)>,
}
struct Clear;
impl Drop for Clear {
    fn drop(&mut self) {
        COVERAGE.with_borrow_mut(|scope| *scope = None);
    }
}

pub(super) fn checked<T>(
    loaded: &V1BatchDependencies,
    restoring: bool,
    operation: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    COVERAGE.with_borrow_mut(|scope| {
        anyhow::ensure!(scope.is_none(), "nested lookahead preparation");
        *scope = Some(Coverage {
            names: loaded
                .nodes
                .iter()
                .map(|n| format!("{}:{}", n.namespace, n.node))
                .collect(),
            v2_keys: loaded.v2_keys.clone(),
            v2_due_window: loaded.v2_due_window,
            restoring,
            missing_names: BTreeSet::new(),
            missing_v2_keys: BTreeSet::new(),
            uncovered_window: None,
        });
        Ok::<_, anyhow::Error>(())
    })?;
    let _clear = Clear;
    let result = operation();
    let (missing_names, v2_keys, uncovered_window) = COVERAGE.with_borrow_mut(|scope| {
        let scope = scope.as_mut().unwrap();
        (
            std::mem::take(&mut scope.missing_names),
            std::mem::take(&mut scope.missing_v2_keys),
            scope.uncovered_window,
        )
    });
    if let Some((previous, current)) = uncovered_window {
        anyhow::bail!(
            "lookahead refreshed ENSv2 expiries in ({previous}, {current}] outside the loaded due window {:?}",
            loaded.v2_due_window
        );
    }
    if missing_names.is_empty() && v2_keys.is_empty() {
        return result;
    }
    let names = missing_names
        .into_iter()
        .map(|key| {
            let (namespace, node) = key
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("lookahead name key {key} has no namespace"))?;
            // The loader requests names in this spelling, so a key in any other could never
            // load and would pass the check on the next attempt.
            let node: alloy_primitives::B256 = node
                .parse()
                .map_err(|_| anyhow::anyhow!("lookahead name key {key} is not a namehash"))?;
            Ok(V1NodeRequest {
                namespace: namespace.to_owned(),
                node: format!("{node:#x}"),
            })
        })
        .collect::<anyhow::Result<_>>()?;
    Err(UnloadedKeys { names, v2_keys }.into())
}

/// The names and ENSv2 state keys a lookahead restore or batch read without their history
/// being loaded. The attempt's output is discarded; the caller may load these too and try
/// again.
#[derive(Debug)]
pub struct UnloadedKeys {
    pub names: BTreeSet<V1NodeRequest>,
    pub v2_keys: BTreeSet<String>,
}

impl std::fmt::Display for UnloadedKeys {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "lookahead accessed unloaded names {:?} and ENSv2 keys {:?}",
            self.names, self.v2_keys
        )
    }
}

impl std::error::Error for UnloadedKeys {}

fn active() -> bool {
    COVERAGE.with_borrow(Option::is_some)
}

/// Whether a lookahead restore, rather than a batch, is running.
pub(in crate::schema_v2) fn restoring() -> bool {
    COVERAGE.with_borrow(|scope| scope.as_ref().is_some_and(|scope| scope.restoring))
}

/// Report a read of name-keyed state shared between the ENSv1-model and ENSv2 code
/// (`known_surfaces`, `active_resources` and the restored surface counts) by its logical name.
pub(in crate::schema_v2) fn observe_name(logical_name_id: &str) {
    if active() {
        observe_node(&logical_name_id.to_ascii_lowercase());
    }
}

pub(in crate::schema_v2) fn observe_node(key: &str) {
    COVERAGE.with_borrow_mut(|scope| {
        if let Some(scope) = scope
            && !scope.names.contains(key)
        {
            scope.missing_names.insert(key.to_owned());
        }
    });
}

/// Report a read of the ENSv2 state an address keeps under `id`: a token, label, resource or
/// observation id, or `-` for state that names none, such as a registry's parent claim.
pub(in crate::schema_v2) fn observe_v2(address: &str, id: &str) {
    if active() {
        observe_v2_key(v2_key(address, id), address);
    }
}

/// Report a read of every token a registry holds.
pub(in crate::schema_v2) fn observe_v2_registry(address: &str) {
    if active() {
        observe_v2_key(v2_registry_key(address), address);
    }
}

fn observe_v2_key(key: String, address: &str) {
    COVERAGE.with_borrow_mut(|scope| {
        if let Some(scope) = scope
            && !scope.v2_keys.contains(&key)
            && !scope.v2_keys.contains(&v2_registry_key(address))
        {
            scope.missing_v2_keys.insert(key);
        }
    });
}

/// Report a refresh that releases the tokens whose expiry lies in `(previous, current]`.
pub(in crate::schema_v2) fn observe_v2_expiry_window(previous: i64, current: i64) {
    COVERAGE.with_borrow_mut(|scope| {
        let Some(scope) = scope else { return };
        let covered = scope
            .v2_due_window
            .is_some_and(|(start, end)| current <= start || (previous >= start && current <= end));
        if !covered {
            scope.uncovered_window.get_or_insert((previous, current));
        }
    });
}

/// The ENSv2 state key for `id` at `address`. Token, resource and label ids share one key
/// whatever their version: an ENSv2 registry derives every version of a label's token id and
/// resource id from its labelhash by replacing only the low 32 bits
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/utils/LibLabel.sol:L15-16 @ ens_v2_sepolia_20260916@366de741).
pub fn v2_key(address: &str, id: &str) -> String {
    format!("{}:{}", address.to_ascii_lowercase(), v2_observation_id(id))
}

/// The key that loads every ENSv2 event of a registry.
pub fn v2_registry_key(address: &str) -> String {
    format!("{}:*", address.to_ascii_lowercase())
}

/// `id` with its low 32 bits (its last eight hex digits) zeroed.
pub(in crate::schema_v2) fn v2_observation_id(id: &str) -> String {
    id.get(..id.len().saturating_sub(8))
        .map(|prefix| format!("{prefix}00000000"))
        .unwrap_or_else(|| id.to_owned())
        .to_ascii_lowercase()
}
