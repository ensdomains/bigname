//! Allowances: the logs a test drops on purpose, each with its reason.

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use alloy_primitives::keccak256;
use sqlx::PgPool;

use super::{Unmatched, report};
use crate::{ScopedTestHookGuard, ScopedTestHookRegistry};

/// The allowances of one scope. Each entry matches by topic0 or by emitter.
pub(super) struct Allowances {
    entries: Vec<Entry>,
    used: Mutex<Vec<bool>>,
}

struct Entry {
    key: String,
    matcher: Matcher,
    reason: String,
}

enum Matcher {
    Topic0(String),
    Emitter(String),
}

impl Allowances {
    fn new(entries: &[(&str, &str)]) -> Self {
        assert!(
            !entries.is_empty(),
            "an allowance scope needs at least one entry"
        );
        let entries = entries
            .iter()
            .map(|(key, reason)| {
                assert!(
                    !reason.trim().is_empty(),
                    "fixture-log allowance {key} needs a reason"
                );
                Entry {
                    key: (*key).to_owned(),
                    matcher: Matcher::parse(key),
                    reason: (*reason).to_owned(),
                }
            })
            .collect::<Vec<_>>();
        Self {
            used: Mutex::new(vec![false; entries.len()]),
            entries,
        }
    }

    /// Whether any entry accepts the log. Every matching entry is marked used.
    fn accepts(&self, unmatched: &Unmatched) -> bool {
        let topic0 = unmatched
            .log
            .topics
            .first()
            .map(|topic| topic.to_ascii_lowercase());
        let emitter = unmatched.log.emitting_address.to_ascii_lowercase();
        let mut used = self.used.lock().expect("allowance usage mutex poisoned");
        let mut accepted = false;
        for (index, entry) in self.entries.iter().enumerate() {
            let matches = match &entry.matcher {
                Matcher::Topic0(want) => topic0.as_deref() == Some(want.as_str()),
                Matcher::Emitter(want) => emitter == *want,
            };
            if matches {
                used[index] = true;
                accepted = true;
            }
        }
        accepted
    }

    fn fail_if_stale(&self) {
        if std::thread::panicking() {
            return;
        }
        let used = self.used.lock().expect("allowance usage mutex poisoned");
        let stale = self
            .entries
            .iter()
            .zip(used.iter())
            .filter(|(_, used)| !**used)
            .map(|(entry, _)| format!("  {} ({})", entry.key, entry.reason))
            .collect::<Vec<_>>();
        assert!(
            stale.is_empty(),
            "fixture-log allowance matched no unmatched log before its scope ended; remove it, \
             or fix the test so the log it names is the one it drops:\n  test: {}\n{}",
            report::test_name(),
            stale.join("\n")
        );
    }
}

impl Matcher {
    fn parse(key: &str) -> Self {
        let lower = key.to_ascii_lowercase();
        match (lower.strip_prefix("0x"), key.contains('(')) {
            (Some(hex), false) if hex.len() == 64 => Self::Topic0(lower),
            (Some(hex), false) if hex.len() == 40 => Self::Emitter(lower),
            (None, true) => Self::Topic0(format!("{:#x}", keccak256(key.as_bytes()))),
            _ => panic!(
                "fixture-log allowance key {key} is neither a topic0, an emitter address, nor an \
                 event signature such as Transfer(address,address,uint256)"
            ),
        }
    }
}

std::thread_local! {
    static THREAD_SCOPES: RefCell<Vec<Arc<Allowances>>> = const { RefCell::new(Vec::new()) };
}

static DATABASE_SCOPES: ScopedTestHookRegistry<String, Arc<Allowances>> =
    ScopedTestHookRegistry::new();

/// An allowance scope for in-process batches on this thread. Entries are `(key, reason)`. A key
/// is an event signature (`"URIUpdated(string,address,address)"`), a topic0 (`"0x…"`, 32 bytes) or
/// an emitter address (`"0x…"`, 20 bytes). When the guard drops, every entry must have accepted
/// at least one unmatched log, or the test fails. A worker thread the test spawns does not see
/// the scope.
pub fn allow_unmatched(entries: &[(&str, &str)]) -> AllowGuard {
    let allowances = Arc::new(Allowances::new(entries));
    THREAD_SCOPES.with_borrow_mut(|scopes| scopes.push(Arc::clone(&allowances)));
    AllowGuard { allowances }
}

/// An allowance scope for batches the Interpret engine runs against `pool`'s test database, on
/// any thread. One scope per test database at a time. Keys and the unused-entry rule are those of
/// [`allow_unmatched`].
pub async fn allow_unmatched_in(pool: &PgPool, entries: &[(&str, &str)]) -> DatabaseAllowGuard {
    let allowances = Arc::new(Allowances::new(entries));
    let registration = DATABASE_SCOPES.install(database_key(pool).await, Arc::clone(&allowances));
    DatabaseAllowGuard {
        allowances,
        _registration: registration,
    }
}

#[must_use = "the allowance lasts until the guard drops"]
pub struct AllowGuard {
    allowances: Arc<Allowances>,
}

impl Drop for AllowGuard {
    fn drop(&mut self) {
        THREAD_SCOPES.with_borrow_mut(|scopes| {
            scopes.retain(|scope| !Arc::ptr_eq(scope, &self.allowances));
        });
        self.allowances.fail_if_stale();
    }
}

#[must_use = "the allowance lasts until the guard drops"]
pub struct DatabaseAllowGuard {
    allowances: Arc<Allowances>,
    _registration: ScopedTestHookGuard<String, Arc<Allowances>>,
}

impl Drop for DatabaseAllowGuard {
    fn drop(&mut self) {
        self.allowances.fail_if_stale();
    }
}

pub(super) fn thread_scopes() -> Vec<Arc<Allowances>> {
    THREAD_SCOPES.with_borrow(Clone::clone)
}

pub(super) fn database_scope(database: &str) -> Option<Arc<Allowances>> {
    DATABASE_SCOPES.get_cloned(&database.to_owned())
}

pub(super) async fn database_key(pool: &PgPool) -> String {
    let options = pool.connect_options();
    if let Some(database) = options.get_database() {
        return database.to_owned();
    }
    crate::current_test_database(pool)
        .await
        .expect("the fixture-log check needs the test database name")
}

/// Panics with a report of every unmatched log no allowance in `scopes` accepts.
pub(super) fn fail_unless_allowed(
    unmatched: &[Unmatched],
    scopes: &[Arc<Allowances>],
    database: Option<&str>,
) {
    let rejected = unmatched
        .iter()
        .filter(|log| {
            // Every scope sees the log, so each matching entry is marked used.
            let mut accepted = false;
            for scope in scopes {
                accepted |= scope.accepts(log);
            }
            !accepted
        })
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        panic!("{}", report::render(&rejected, database));
    }
}
