//! The NameWrapper entry's stored expiry that a composed name serves beside its wrapper fields
//! (`ens_v1.wrapper_expires_at`). A composed read attaches it itself, or, inside
//! [`defer_wrapper_expiries`], leaves a pending marker so the response reads every wrapper it
//! serves at once: one primary-key read per chain ([`load_wrapper_expiries`]).
use std::{collections::BTreeMap, future::Future};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    ReadDb,
    families::control::{rows::WrapperRow, wrapper::load_wrapper_rows},
};

/// The declared-summary key holding the stored expiry word a composed read attached.
pub const WRAPPER_EXPIRY_KEY: &str = "wrapper_expiry_seconds";
/// The declared-summary key a deferred composed read writes instead:
/// `{chain_id, resource_id, backed}` of the wrapper whose expiry the response still has to read.
pub const WRAPPER_EXPIRY_PENDING_KEY: &str = "wrapper_expiry_pending";

tokio::task_local! {
    static DEFERRED: ();
}

/// Runs `future` with every composed read in it leaving the wrapper expiry pending, for a
/// response that reads the expiries of the rows it serves once, after its page is known.
pub async fn defer_wrapper_expiries<F: Future>(future: F) -> F::Output {
    DEFERRED.scope((), future).await
}

pub(crate) fn deferred() -> bool {
    DEFERRED.try_with(|_| ()).is_ok()
}

/// One NameWrapper entry a response serves the expiry of.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WrapperExpiryKey {
    pub chain_id: String,
    pub resource_id: Uuid,
}

/// The pending marker of a wrapper whose composed row serves a state (`backed`) or masks a lapsed
/// or unknown one.
pub(crate) fn pending_marker(key: &WrapperExpiryKey, backed: bool) -> Value {
    json!({"chain_id": key.chain_id, "resource_id": key.resource_id, "backed": backed})
}

/// The wrapper and whether it is backed, from a pending marker.
pub fn parse_pending_marker(marker: &Value) -> Option<(WrapperExpiryKey, bool)> {
    Some((
        WrapperExpiryKey {
            chain_id: marker.get("chain_id")?.as_str()?.to_owned(),
            resource_id: marker.get("resource_id")?.as_str()?.parse().ok()?,
        },
        marker.get("backed")?.as_bool()?,
    ))
}

/// The stored expiry word served for each wanted wrapper, keyed as asked; `true` marks a backed
/// wrapper. A backed wrapper serves its expiry. A masked one serves it only when it lapsed:
/// composition masks a wrapper whose stored state, fuses and expiry are all known only because
/// an emancipated or locked wrapper is past its expiry (`effective_wrapper`). A wrapper whose
/// latest lifecycle event is an unwrap serves JSON null, backed or not: the stored row keeps the
/// state and expiry of the entry that was unwrapped, composition does not read the lifecycle, and
/// that entry is no longer the name's current one. A wanted wrapper with no stored row, or a
/// backed one with no expiry, is an error. One read per chain.
pub async fn load_wrapper_expiries(
    db: impl Into<ReadDb<'_>>,
    wanted: &BTreeMap<WrapperExpiryKey, bool>,
) -> Result<BTreeMap<WrapperExpiryKey, Value>> {
    let mut db = db.into();
    let mut by_chain: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for key in wanted.keys() {
        by_chain
            .entry(&key.chain_id)
            .or_default()
            .push(key.resource_id.to_string());
    }
    let mut stored: BTreeMap<WrapperExpiryKey, WrapperRow> = BTreeMap::new();
    for (chain_id, resources) in by_chain {
        let mut conn = db.reborrow().acquire().await?;
        seams::note_read();
        for row in load_wrapper_rows(&mut *conn, chain_id, &resources).await? {
            let resource_id = row
                .resource_id
                .parse()
                .context("stored wrapper row has an unreadable resource")?;
            stored.insert(
                WrapperExpiryKey {
                    chain_id: chain_id.to_owned(),
                    resource_id,
                },
                row,
            );
        }
    }
    let mut served = BTreeMap::new();
    for (key, backed) in wanted {
        let row = stored
            .get(key)
            .with_context(|| format!("wrapper {} has no stored row", key.resource_id))?;
        if row.lifecycle_unwrapped == Some(true) {
            served.insert(key.clone(), Value::Null);
            continue;
        }
        if !backed && !lapsed(row) {
            continue;
        }
        let expiry: Value = row
            .expiry_seconds
            .as_deref()
            .and_then(|text| serde_json::from_str(text).ok())
            .filter(|word: &Value| !word.is_null())
            .with_context(|| format!("wrapper {} has no stored expiry", key.resource_id))?;
        served.insert(key.clone(), expiry);
    }
    Ok(served)
}

fn lapsed(wrapper: &WrapperRow) -> bool {
    matches!(
        wrapper.wrapper_state.as_deref(),
        Some("emancipated" | "locked")
    ) && wrapper.fuses.is_some()
        && wrapper.expiry_seconds.is_some()
}

/// Test seam counting [`load_wrapper_expiries`] reads, one per chain read.
pub mod seams {
    #[cfg(any(test, feature = "test-support"))]
    mod scoped {
        use std::{
            future::Future,
            sync::{
                Arc,
                atomic::{AtomicU64, Ordering},
            },
        };

        tokio::task_local! {
            static READS: Arc<AtomicU64>;
        }

        /// Runs `future` adding every wrapper expiry read in it to `counter`.
        pub async fn with_wrapper_expiry_read_counter<F: Future>(
            counter: Arc<AtomicU64>,
            future: F,
        ) -> F::Output {
            READS.scope(counter, future).await
        }

        pub(in crate::name_current::wrapper_expiry) fn note_read() {
            let _ = READS.try_with(|counter| counter.fetch_add(1, Ordering::Relaxed));
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(super) use scoped::note_read;
    #[cfg(any(test, feature = "test-support"))]
    pub use scoped::with_wrapper_expiry_read_counter;

    #[cfg(not(any(test, feature = "test-support")))]
    pub(super) fn note_read() {}
}
