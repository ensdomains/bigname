//! Opt-in diagnostics. Content sizes are not allocator or RSS measurements.
use crate::{
    VerificationLog,
    provider::{Block, BlockBundle, Log, Receipt, ResolvedBlock, Transaction},
};
use std::{
    cell::RefCell,
    future::Future,
    mem::size_of,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Default)]
struct Run {
    invalid: AtomicBool,
    emission: Mutex<()>,
    sequence: AtomicU64,
    attempts: AtomicU64,
    stored_rows: AtomicU64,
    stored_bytes: AtomicU64,
    provider_rows: AtomicU64,
    provider_bytes: AtomicU64,
    transport_bytes: AtomicU64,
    transport_attempts: AtomicU64,
    failed_body_bytes: AtomicU64,
    successful_body_bytes: AtomicU64,
    scratch_bytes: AtomicU64,
}
/// A bounded diagnostic session; supplying one explicitly also enables isolated tests.
#[derive(Clone)]
pub struct Session {
    id: Arc<str>,
    run: Arc<Run>,
    started: Instant,
}
#[derive(Clone)]
pub struct Context {
    session: Session,
    chain: Arc<str>,
    from: i64,
    to: i64,
    attempt: u64,
    kind: &'static str,
    ordinal: usize,
}
tokio::task_local! { static TASK: Context; }
thread_local! { static BLOCKING: RefCell<Option<Context>> = const { RefCell::new(None) }; }
static ENVIRONMENT: OnceLock<Option<Session>> = OnceLock::new();
impl Session {
    pub fn new(id: &str) -> Option<Self> {
        (id.len() <= 64
            && !id.is_empty()
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
        .then(|| Self {
            id: Arc::from(id),
            run: Arc::default(),
            started: Instant::now(),
        })
    }
    fn root(&self) -> Context {
        Context {
            session: self.clone(),
            chain: Arc::from(""),
            from: 0,
            to: 0,
            attempt: 0,
            kind: "none",
            ordinal: 0,
        }
    }
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        TASK.scope(self.root(), future).await
    }
    #[cfg(test)]
    pub(crate) fn observation_count(&self) -> u64 {
        self.run.sequence.load(Ordering::Relaxed)
    }
    pub fn valid(&self) -> bool {
        !self.run.invalid.load(Ordering::Relaxed)
    }
}
pub fn capture() -> Option<Context> {
    TASK.try_with(Clone::clone)
        .ok()
        .or_else(|| BLOCKING.with(|slot| slot.borrow().clone()))
        .or_else(|| {
            ENVIRONMENT
                .get_or_init(|| {
                    std::env::var("BIGNAME_VERIFY_MEMORY_RUN_ID")
                        .ok()
                        .and_then(|id| Session::new(&id))
                })
                .as_ref()
                .map(Session::root)
        })
}
/// Explicitly propagates diagnostics into reth's blocking worker, restoring nesting on unwind.
pub fn blocking<T>(context: Option<Context>, operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<Context>);
    impl Drop for Restore {
        fn drop(&mut self) {
            BLOCKING.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(BLOCKING.with(|slot| slot.replace(context)));
    operation()
}
pub async fn attempt<F: Future>(chain: &str, from: i64, to: i64, future: F) -> F::Output {
    match capture() {
        Some(mut context) => {
            context.chain = Arc::from(chain);
            context.from = from;
            context.to = to;
            context.attempt = increment(&context.session.run.attempts, 1, &context.session.run);
            TASK.scope(context, future).await
        }
        None => future.await,
    }
}
pub async fn query<F: Future>(
    kind: &'static str,
    ordinal: usize,
    from: i64,
    to: i64,
    future: F,
) -> F::Output {
    match capture() {
        Some(mut context) => {
            context.kind = kind;
            context.ordinal = ordinal;
            context.from = from;
            context.to = to;
            TASK.scope(context, future).await
        }
        None => future.await,
    }
}
fn invalid() {
    if let Some(c) = capture() {
        c.session.run.invalid.store(true, Ordering::Relaxed);
    }
}
fn increment(value: &AtomicU64, by: u64, run: &Run) -> u64 {
    match value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(by)) {
        Ok(previous) => previous + by,
        Err(_) => {
            run.invalid.store(true, Ordering::Relaxed);
            u64::MAX
        }
    }
}
pub fn sum(a: u64, b: u64) -> u64 {
    add(a, b)
}
fn add(a: u64, b: u64) -> u64 {
    a.checked_add(b).unwrap_or_else(|| {
        invalid();
        u64::MAX
    })
}
fn mul(a: u64, b: u64) -> u64 {
    a.checked_mul(b).unwrap_or_else(|| {
        invalid();
        u64::MAX
    })
}
/// `owned` counts exposed buffer capacities, excluding allocator metadata and B-tree nodes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Footprint {
    pub rows: u64,
    pub bytes: u64,
    pub owned: u64,
    pub max_item: u64,
}
impl Footprint {
    pub fn combine(self, rhs: Self) -> Self {
        Self {
            rows: add(self.rows, rhs.rows),
            bytes: add(self.bytes, rhs.bytes),
            owned: add(self.owned, rhs.owned),
            max_item: self.max_item.max(rhs.max_item),
        }
    }
    pub fn entries<T>(count: usize) -> Self {
        Self {
            rows: count as u64,
            bytes: mul(count as u64, size_of::<T>() as u64),
            ..Self::default()
        }
    }
}
pub fn inline<T>(values: &Vec<T>) -> Footprint {
    Footprint {
        rows: values.len() as u64,
        owned: mul(values.capacity() as u64, size_of::<T>() as u64),
        ..Footprint::default()
    }
}
pub trait Measured {
    fn footprint(&self) -> Footprint;
}
impl<A: Measured, B: Measured> Measured for (A, B) {
    fn footprint(&self) -> Footprint {
        self.0.footprint().combine(self.1.footprint())
    }
}
impl Measured for i64 {
    fn footprint(&self) -> Footprint {
        Footprint::entries::<i64>(1)
    }
}
impl Measured for String {
    fn footprint(&self) -> Footprint {
        Footprint {
            bytes: self.len() as u64,
            owned: self.capacity() as u64,
            ..Footprint::default()
        }
    }
}
impl<T: Measured> Measured for Option<T> {
    fn footprint(&self) -> Footprint {
        self.as_ref()
            .map_or(Footprint::default(), Measured::footprint)
    }
}
impl Measured for u8 {
    fn footprint(&self) -> Footprint {
        Footprint {
            bytes: 1,
            ..Footprint::default()
        }
    }
}
impl<T: Measured> Measured for Vec<T> {
    fn footprint(&self) -> Footprint {
        let mut f = Footprint {
            rows: self.len() as u64,
            owned: mul(self.capacity() as u64, size_of::<T>() as u64),
            ..Footprint::default()
        };
        for value in self {
            let v = value.footprint();
            f.bytes = add(f.bytes, v.bytes);
            f.owned = add(f.owned, v.owned);
            f.max_item = f.max_item.max(v.max_item);
        }
        f
    }
}
pub fn log(
    block: &String,
    transaction: &String,
    address: &String,
    topics: &Vec<String>,
    data: &Vec<u8>,
) -> Footprint {
    let mut f = block
        .footprint()
        .combine(transaction.footprint())
        .combine(address.footprint())
        .combine(topics.footprint())
        .combine(data.footprint());
    f.rows = 1;
    f.bytes = add(f.bytes, 24);
    f.max_item = f.bytes;
    f
}
macro_rules! measured_log {
    ($t:ty) => {
        impl Measured for $t {
            fn footprint(&self) -> Footprint {
                log(
                    &self.block_hash,
                    &self.transaction_hash,
                    &self.address,
                    &self.topics,
                    &self.data,
                )
            }
        }
    };
}
measured_log!(Log);
measured_log!(VerificationLog);
impl Measured for ResolvedBlock {
    fn footprint(&self) -> Footprint {
        self.hash.footprint().combine(Footprint {
            bytes: 8,
            ..Footprint::default()
        })
    }
}
macro_rules! fields { ($t:ty, $($field:ident),+) => { impl Measured for $t { fn footprint(&self) -> Footprint { let mut f = Footprint::default(); $(f = f.combine(self.$field.footprint());)+ f.rows = 1; f } } }; }
fields!(Transaction, hash, block_hash, from, to, input, value);
fields!(
    Receipt,
    transaction_hash,
    block_hash,
    contract_address,
    cumulative_gas_used,
    gas_used,
    logs_bloom
);
fields!(
    Block,
    hash,
    parent_hash,
    logs_bloom,
    transactions_root,
    receipts_root,
    state_root
);
fields!(BlockBundle, block, transactions, receipts, logs);
impl Measured for serde_json::Value {
    fn footprint(&self) -> Footprint {
        use serde_json::Value;
        match self {
            Value::String(s) => s.footprint(),
            Value::Array(v) => v.footprint(),
            Value::Object(v) => v.iter().fold(
                Footprint::entries::<(String, Value)>(v.len()),
                |f, (k, v)| f.combine(k.footprint()).combine(v.footprint()),
            ),
            _ => Footprint {
                bytes: size_of::<Self>() as u64,
                ..Footprint::default()
            },
        }
    }
}
/// Incremental exposed buffers; B-tree nodes and allocator metadata remain unknown.
#[derive(Default)]
pub struct Identity {
    pub retained: Footprint,
    pub peak_bytes: u64,
    pub duplicates: u64,
    pub clone_overlap: u64,
    remaining: Footprint,
    backing: u64,
    current: Footprint,
    peak_owned: u64,
}
pub fn subtract(a: u64, b: u64) -> u64 {
    a.checked_sub(b).unwrap_or_else(|| {
        invalid();
        0
    })
}
impl Identity {
    pub fn query(&mut self, f: Footprint, backing: u64) {
        self.remaining = f;
        self.remaining.owned = subtract(f.owned, backing);
        self.backing = backing;
    }
    pub fn consume<T: Measured>(&mut self, value: &T) {
        if capture().is_none() {
            return;
        }
        self.current = value.footprint();
        self.remaining.bytes = subtract(self.remaining.bytes, self.current.bytes);
        self.remaining.owned = subtract(self.remaining.owned, self.current.owned);
    }
    pub fn inserted<T: Measured>(
        &mut self,
        old: Option<T>,
        map: &std::collections::BTreeMap<(String, i64), T>,
        key: &(String, i64),
    ) -> Option<T> {
        if capture().is_none() {
            return old;
        }
        let (retained_key, value) = map.get_key_value(key).expect("just inserted identity");
        let next = value.footprint();
        let previous = old.as_ref().map(Measured::footprint).unwrap_or_default();
        if old.is_some() {
            self.duplicates = add(self.duplicates, 1);
            self.retained.bytes = subtract(self.retained.bytes, previous.bytes);
            self.retained.owned = subtract(self.retained.owned, previous.owned);
        } else {
            self.retained.rows = add(self.retained.rows, 1);
            self.retained.bytes = add(self.retained.bytes, add(retained_key.0.len() as u64, 8));
            self.retained.owned = add(self.retained.owned, retained_key.0.capacity() as u64);
        }
        self.retained.bytes = add(self.retained.bytes, next.bytes);
        self.retained.owned = add(self.retained.owned, next.owned);
        self.retained.max_item = self.retained.max_item.max(next.max_item);
        self.peak_bytes = self.peak_bytes.max(self.retained.bytes);
        let overlap = self
            .retained
            .combine(self.remaining)
            .combine(self.current)
            .combine(previous)
            .combine(key.0.footprint());
        self.clone_overlap = self.clone_overlap.max(overlap.bytes);
        self.peak_owned = self.peak_owned.max(add(overlap.owned, self.backing));
        old
    }
    pub fn emit(&self, stage: &'static str) {
        observe(stage, || self.retained);
        observe("identity_high_water", || Footprint {
            rows: self.duplicates,
            bytes: self.peak_bytes,
            owned: self.peak_owned,
            max_item: self.clone_overlap,
        });
    }
}
/// The closure is not evaluated when diagnostics are disabled.
pub fn observe(stage: &'static str, measure: impl FnOnce() -> Footprint) {
    observe_context(capture(), stage, measure);
}
fn observe_context(
    context: Option<Context>,
    stage: &'static str,
    measure: impl FnOnce() -> Footprint,
) {
    if let Some(context) = context {
        emit(&context, stage, measure());
    }
}
pub fn returned(
    side: &'static str,
    kind: &'static str,
    ordinal: usize,
    from: i64,
    to: i64,
    measure: impl FnOnce() -> Footprint,
) -> Footprint {
    let Some(mut context) = capture() else {
        return Footprint::default();
    };
    context.kind = kind;
    context.ordinal = ordinal;
    context.from = from;
    context.to = to;
    let footprint = measure();
    let run = &context.session.run;
    let (rows, bytes) = if side == "stored" {
        (&run.stored_rows, &run.stored_bytes)
    } else {
        (&run.provider_rows, &run.provider_bytes)
    };
    increment(rows, footprint.rows, run);
    increment(bytes, footprint.bytes, run);
    emit(
        &context,
        if side == "stored" {
            "stored_query_return"
        } else {
            "provider_query_return"
        },
        footprint,
    );
    footprint
}
pub fn scratch(_stage: &'static str, measure: impl FnOnce() -> Footprint) {
    if let Some(c) = capture() {
        c.session
            .run
            .scratch_bytes
            .fetch_max(measure().owned, Ordering::Relaxed);
    }
}
pub fn scratch_summary() {
    if let Some(c) = capture() {
        emit(
            &c,
            "scratch_run_high_water",
            Footprint {
                owned: c.session.run.scratch_bytes.load(Ordering::Relaxed),
                ..Footprint::default()
            },
        );
    }
}
pub fn transport(bytes: usize) {
    if let Some(c) = capture() {
        increment(&c.session.run.transport_bytes, bytes as u64, &c.session.run);
    }
}
pub fn transport_start() -> Option<(Context, u64)> {
    capture().map(|c| {
        let ordinal = increment(&c.session.run.transport_attempts, 1, &c.session.run);
        (c, ordinal)
    })
}
pub fn transport_outcome(
    context: &Context,
    ordinal: u64,
    method: &str,
    range: (Option<u64>, Option<u64>),
    outcome: &str,
    bytes: Option<u64>,
) {
    let run = &context.session.run;
    if let Some(bytes) = bytes {
        increment(
            if outcome == "success" {
                &run.successful_body_bytes
            } else {
                &run.failed_body_bytes
            },
            bytes,
            run,
        );
    }
    emit_detail(
        context,
        "rpc_transport",
        Footprint::default(),
        None,
        None,
        Some((ordinal, method, range.0, range.1, outcome, bytes)),
    );
}
pub fn marker(number: i64, hash: &str, level: &str) {
    if let Some(c) = capture() {
        if hash.len() > 128 {
            invalid();
            return;
        }
        emit_detail(
            &c,
            "compared_end",
            Footprint::default(),
            Some((number, hash, level)),
            None,
            None,
        );
    }
}
fn emit(c: &Context, stage: &'static str, f: Footprint) {
    emit_detail(c, stage, f, None, None, None);
}
pub fn progress_due(index: usize) -> bool {
    index.is_multiple_of(256)
}
pub fn native(
    stage: &'static str,
    block: i64,
    receipts: Footprint,
    hashes: Footprint,
    output: Footprint,
) {
    if let Some(c) = capture() {
        emit_detail(
            &c,
            stage,
            receipts.combine(hashes).combine(output),
            None,
            Some((block, receipts, hashes, output)),
            None,
        );
    }
}
type RpcDetail<'a> = (u64, &'a str, Option<u64>, Option<u64>, &'a str, Option<u64>);

fn emit_detail(
    c: &Context,
    stage: &'static str,
    f: Footprint,
    marker: Option<(i64, &str, &str)>,
    native: Option<(i64, Footprint, Footprint, Footprint)>,
    rpc: Option<RpcDetail<'_>>,
) {
    let run = &c.session.run;
    let Ok(_guard) = run.emission.lock() else {
        run.invalid.store(true, Ordering::Relaxed);
        return;
    };
    let sequence = increment(&run.sequence, 1, run);
    tracing::info!(target: "bigname_memory", schema = 1, run = %c.session.id, sequence,
        elapsed_us = c.session.started.elapsed().as_micros() as u64, chain = %c.chain,
        from = c.from, to = c.to, attempt = c.attempt, query_kind = c.kind, query_ordinal = c.ordinal,
        stage, transport_ordinal = rpc.map(|r| r.0), rpc_method = rpc.map(|r| r.1), split_from = rpc.and_then(|r| r.2), split_to = rpc.and_then(|r| r.3), transport_outcome = rpc.map(|r| r.4), known_body_bytes = rpc.and_then(|r| r.5), failed_body_bytes = run.failed_body_bytes.load(Ordering::Relaxed), successful_body_bytes = run.successful_body_bytes.load(Ordering::Relaxed), native_block = native.map(|n| n.0), receipt_rows = native.map(|n| n.1.rows), receipt_bytes = native.map(|n| n.1.bytes), receipt_capacity = native.map(|n| n.1.owned), hash_bytes = native.map(|n| n.2.bytes), hash_capacity = native.map(|n| n.2.owned), output_rows = native.map(|n| n.3.rows), output_bytes = native.map(|n| n.3.bytes), output_capacity = native.map(|n| n.3.owned), marker_number = marker.map(|m| m.0), marker_hash = marker.map(|m| m.1), source_level = marker.map(|m| m.2), rows = f.rows, logical_bytes = f.bytes, exposed_capacity_bytes = f.owned, max_item_bytes = f.max_item,
        stored_rows = run.stored_rows.load(Ordering::Relaxed), stored_bytes = run.stored_bytes.load(Ordering::Relaxed),
        provider_rows = run.provider_rows.load(Ordering::Relaxed), provider_bytes = run.provider_bytes.load(Ordering::Relaxed),
        transport_attempts = run.transport_attempts.load(Ordering::Relaxed), transport_bytes = run.transport_bytes.load(Ordering::Relaxed), valid = c.session.valid(),
        "verify memory observation");
}
#[cfg(test)]
#[path = "measurement_tests.rs"]
mod tests;
