//! The API's one database write: labels a request proves, staged as rainbow candidates.
//!
//! A request that names a node by its plain labels proves each label's text, since the node it
//! reaches is the namehash of those labels (docs/api-v1.md, "Name inputs"). When the response
//! still serves one of those labels as its labelhash placeholder, the label is inserted into
//! `bigname_phase.ens_names`, the unverified table the operator's preimage import proof-checks
//! (docs/storage.md, "Rainbow-table preimage import"). No serving path reads that table, so a
//! write changes no answer until the import runs.
//!
//! The write is best effort and never delays or fails the response. It runs on its own pool
//! connection after the route's reads, conflict-ignored, behind a token bucket and a bounded
//! in-process set of labelhashes already staged. When the database refuses it, as a hot standby
//! or a role without `INSERT` does, the API serves as before. The API owns this statement. The
//! table and the import that reads it are storage's (docs/internal/workstreams.md).
use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, ensure};
use clap::Args;
use sqlx::PgPool;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const DEFAULT_PER_SECOND: u32 = 10;
const DEFAULT_BURST: u32 = 20;
const DEFAULT_MAX_LABELS: usize = 65_536;
const DEFAULT_MAX_IN_FLIGHT: usize = 2;

const WRITE_STATEMENT_TIMEOUT: &str = "SET LOCAL statement_timeout = '1s'";
// Rows go in labelhash order, so two writes that share labels wait on each other instead of
// deadlocking. The primary key is the table's only unique constraint, so the bare `ON CONFLICT`
// arbitrates on it. Naming it as a conflict target would need `SELECT` on the column too.
// A unique index added to ens_names later would silently widen DO NOTHING to its conflicts.
const INSERT: &str = "/* api:label_staging.insert */
    INSERT INTO bigname_phase.ens_names (hash, name)
    SELECT hash, name FROM unnest($1::text[], $2::text[]) AS input(hash, name)
    ORDER BY hash
    ON CONFLICT DO NOTHING";

/// SQLSTATE of a write in a read-only transaction, as on a hot standby.
const READ_ONLY_SQL_TRANSACTION: &str = "25006";
/// SQLSTATE of a write the role holds no privilege for.
const INSUFFICIENT_PRIVILEGE: &str = "42501";

const OUTCOMES: usize = 7;

// Each id is spelled out, since the flattened bounds already use `max_in_flight`.
#[derive(Args, Clone, Debug)]
pub(crate) struct LabelStagingConfig {
    /// Candidate writes a second. Zero stages nothing, and responses still show proven labels.
    #[arg(
        id = "label_staging_per_second",
        long = "label-staging-per-second",
        env = "BIGNAME_API_LABEL_STAGING_PER_SECOND",
        default_value_t = DEFAULT_PER_SECOND
    )]
    pub(crate) per_second: u32,
    #[arg(
        id = "label_staging_burst",
        long = "label-staging-burst",
        env = "BIGNAME_API_LABEL_STAGING_BURST",
        default_value_t = DEFAULT_BURST
    )]
    pub(crate) burst: u32,
    /// Labelhashes remembered as staged before the set starts again.
    #[arg(
        id = "label_staging_max_labels",
        long = "label-staging-max-labels",
        env = "BIGNAME_API_LABEL_STAGING_MAX_LABELS",
        default_value_t = DEFAULT_MAX_LABELS
    )]
    pub(crate) max_labels: usize,
    #[arg(
        id = "label_staging_max_in_flight",
        long = "label-staging-max-in-flight",
        env = "BIGNAME_API_LABEL_STAGING_MAX_IN_FLIGHT",
        default_value_t = DEFAULT_MAX_IN_FLIGHT
    )]
    pub(crate) max_in_flight: usize,
}

impl Default for LabelStagingConfig {
    fn default() -> Self {
        Self {
            per_second: DEFAULT_PER_SECOND,
            burst: DEFAULT_BURST,
            max_labels: DEFAULT_MAX_LABELS,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
        }
    }
}

impl LabelStagingConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.per_second > 0 {
            ensure!(
                self.burst > 0,
                "BIGNAME_API_LABEL_STAGING_BURST must be greater than zero when staging is enabled"
            );
            ensure!(
                self.max_labels > 0,
                "BIGNAME_API_LABEL_STAGING_MAX_LABELS must be greater than zero when staging is \
                 enabled"
            );
            ensure!(
                self.max_in_flight > 0,
                "BIGNAME_API_LABEL_STAGING_MAX_IN_FLIGHT must be greater than zero when staging \
                 is enabled"
            );
        }
        Ok(())
    }
}

/// What became of one response's candidates, the `outcome` label of
/// `label_candidate_writes_total`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    /// The insert committed at least one new row.
    Staged,
    /// Every label was already staged, by this process (no insert ran) or by another.
    Duplicate,
    /// The write budget or the in-flight limit was spent, so nothing was written.
    Throttled,
    /// The role holds no `INSERT` on `ens_names`.
    Denied,
    /// The database refused the write as read-only, as a hot standby does.
    ReadOnly,
    /// Staging is configured off.
    Disabled,
    /// Any other error, a timeout included.
    Failed,
}

impl Outcome {
    pub(crate) const ALL: [Self; OUTCOMES] = [
        Self::Staged,
        Self::Duplicate,
        Self::Throttled,
        Self::Denied,
        Self::ReadOnly,
        Self::Disabled,
        Self::Failed,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Staged => "staged",
            Self::Duplicate => "duplicate",
            Self::Throttled => "throttled",
            Self::Denied => "denied",
            Self::ReadOnly => "read_only",
            Self::Disabled => "disabled",
            Self::Failed => "failed",
        }
    }

    fn of(result: &Result<u64, sqlx::Error>) -> Self {
        match result {
            Ok(0) => Self::Duplicate,
            Ok(_) => Self::Staged,
            Err(error) => match error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref()
            {
                Some(READ_ONLY_SQL_TRANSACTION) => Self::ReadOnly,
                Some(INSUFFICIENT_PRIVILEGE) => Self::Denied,
                _ => Self::Failed,
            },
        }
    }
}

/// The process's staging writer. Clones share one budget, one set and one in-flight limit.
#[derive(Clone)]
pub(crate) struct LabelStaging {
    inner: Arc<Inner>,
}

struct Inner {
    config: LabelStagingConfig,
    bucket: Mutex<Bucket>,
    staged: Mutex<HashSet<[u8; 32]>>,
    in_flight: Arc<Semaphore>,
    warned_denied: AtomicBool,
    warned_read_only: AtomicBool,
    // RED-PROOF MUTATION: a refused or failed write turns the heal off.
    refused: AtomicBool,
    counts: [AtomicU64; OUTCOMES],
}

/// A write the budget admitted: the labels, now reserved in the set, and the in-flight permit.
struct Admitted {
    labels: BTreeMap<[u8; 32], String>,
    permit: OwnedSemaphorePermit,
}

enum Decision {
    Write(Admitted),
    Skip(Outcome),
}

impl LabelStaging {
    pub(crate) fn new(config: LabelStagingConfig) -> Self {
        crate::metrics::register_label_candidate_outcomes(Outcome::ALL.map(Outcome::as_str));
        let now = Instant::now();
        Self {
            inner: Arc::new(Inner {
                bucket: Mutex::new(Bucket::full(now, config.burst)),
                staged: Mutex::new(HashSet::new()),
                in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
                warned_denied: AtomicBool::new(false),
                warned_read_only: AtomicBool::new(false),
                refused: AtomicBool::new(false),
                counts: Default::default(),
                config,
            }),
        }
    }

    /// Stage `labels`, each labelhash with its normalized text, every one proven by the request
    /// and served as its placeholder. Returns at once, and the write runs on its own task.
    pub(crate) fn submit(&self, pool: &PgPool, labels: BTreeMap<[u8; 32], String>) {
        if labels.is_empty() {
            return;
        }
        let admitted = match self.decide(labels, Instant::now()) {
            Decision::Write(admitted) => admitted,
            Decision::Skip(outcome) => {
                self.count(outcome);
                return;
            }
        };
        let staging = self.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let Admitted { labels, permit } = admitted;
            let (hashes, names): (Vec<String>, Vec<String>) = labels
                .iter()
                .map(|(labelhash, name)| (format!("0x{}", hex::encode(labelhash)), name.clone()))
                .unzip();
            let result = write(&pool, &hashes, &names).await;
            let outcome = Outcome::of(&result);
            if !matches!(outcome, Outcome::Staged | Outcome::Duplicate) {
                // Another request may try these labels again, as after the grant a refusal
                // names. The bucket still bounds how often.
                let mut staged = staging.inner.lock_staged();
                for labelhash in labels.keys() {
                    staged.remove(labelhash);
                }
            }
            staging.report(outcome, result.err());
            drop(permit);
        });
    }

    /// The labels not yet staged, reserved and admitted when the budget and the in-flight limit
    /// allow. Reserving under the set's lock makes concurrent requests for one label write once.
    fn decide(&self, labels: BTreeMap<[u8; 32], String>, now: Instant) -> Decision {
        let inner = &self.inner;
        if inner.config.per_second == 0 {
            return Decision::Skip(Outcome::Disabled);
        }
        let mut staged = inner.lock_staged();
        let labels: BTreeMap<[u8; 32], String> = labels
            .into_iter()
            .filter(|(labelhash, _)| !staged.contains(labelhash))
            .collect();
        if labels.is_empty() {
            return Decision::Skip(Outcome::Duplicate);
        }
        let Ok(permit) = inner.in_flight.clone().try_acquire_owned() else {
            return Decision::Skip(Outcome::Throttled);
        };
        let admitted = inner
            .bucket
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take(now, inner.config.per_second, inner.config.burst);
        if !admitted {
            return Decision::Skip(Outcome::Throttled);
        }
        if staged.len() + labels.len() > inner.config.max_labels {
            staged.clear();
        }
        staged.extend(labels.keys().copied());
        Decision::Write(Admitted { labels, permit })
    }

    fn report(&self, outcome: Outcome, error: Option<sqlx::Error>) {
        if matches!(
            outcome,
            Outcome::Denied | Outcome::ReadOnly | Outcome::Failed
        ) {
            self.inner.refused.store(true, Ordering::Relaxed);
        }
        self.count(outcome);
        let warned = match outcome {
            Outcome::Denied => &self.inner.warned_denied,
            Outcome::ReadOnly => &self.inner.warned_read_only,
            Outcome::Failed => {
                let failures = self.count_of(Outcome::Failed);
                if failures.is_power_of_two() {
                    tracing::warn!(
                        service = "api",
                        failures,
                        error = ?error,
                        "failed to stage proven labels into bigname_phase.ens_names"
                    );
                }
                return;
            }
            _ => return,
        };
        if !warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                service = "api",
                outcome = outcome.as_str(),
                error = ?error,
                "the database refuses label candidate writes; responses still show proven labels, \
                 and later refusals are counted in label_candidate_writes_total only"
            );
        }
    }

    fn count(&self, outcome: Outcome) {
        self.inner.counts[outcome as usize].fetch_add(1, Ordering::Relaxed);
        crate::metrics::observe_label_candidate_write(outcome.as_str());
    }

    pub(crate) fn refused(&self) -> bool {
        self.inner.refused.load(Ordering::Relaxed)
    }

    pub(crate) fn count_of(&self, outcome: Outcome) -> u64 {
        self.inner.counts[outcome as usize].load(Ordering::Relaxed)
    }

    /// Waits until no write is in flight.
    #[cfg(test)]
    pub(crate) async fn settled(&self) {
        let permits = u32::try_from(self.inner.config.max_in_flight).expect("a small limit");
        if permits > 0 {
            let _all = self
                .inner
                .in_flight
                .acquire_many(permits)
                .await
                .expect("the in-flight semaphore is never closed");
        }
    }
}

impl Inner {
    fn lock_staged(&self) -> std::sync::MutexGuard<'_, HashSet<[u8; 32]>> {
        self.staged.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

async fn write(pool: &PgPool, hashes: &[String], names: &[String]) -> Result<u64, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query(WRITE_STATEMENT_TIMEOUT)
        .execute(&mut *transaction)
        .await?;
    let inserted = sqlx::query(INSERT)
        .bind(hashes)
        .bind(names)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
    transaction.commit().await?;
    Ok(inserted)
}

struct Bucket {
    tokens: f64,
    updated_at: Instant,
}

impl Bucket {
    fn full(now: Instant, burst: u32) -> Self {
        Self {
            tokens: f64::from(burst),
            updated_at: now,
        }
    }

    fn take(&mut self, now: Instant, per_second: u32, burst: u32) -> bool {
        let elapsed = now.saturating_duration_since(self.updated_at).as_secs_f64();
        self.tokens = (self.tokens + elapsed * f64::from(per_second)).min(f64::from(burst));
        self.updated_at = self.updated_at.max(now);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

#[cfg(test)]
#[path = "label_staging_tests.rs"]
mod tests;
