//! Test seam for the gateway destination filter. The gateway client is a process static, so the
//! seam is process-wide too. Fake gateways admit their exact socket address, which keeps the
//! production class table unchanged in test builds. Hostnames resolve only through scripted
//! answers, and a fixed operator list stands in for `BIGNAME_API_CCIP_GATEWAY_DENY_CIDRS`.

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    io,
    net::SocketAddr,
    sync::{LazyLock, Mutex, MutexGuard, PoisonError},
};

use tokio::task::LocalKey;

use super::GatewayRefusalClass;

/// The operator list for every test in the process. Tests that need a public but unroutable
/// stand-in use 192.0.2.0/24, 203.0.113.0/24 or 2001:db8:2::/48, never these.
pub(crate) const TEST_DENY_LIST: &str = "198.51.100.0/24, 2001:db8:1::/48";

#[derive(Default)]
struct Seam {
    admitted: HashSet<SocketAddr>,
    hosts: HashMap<String, VecDeque<Vec<SocketAddr>>>,
}

static SEAM: LazyLock<Mutex<Seam>> = LazyLock::new(Mutex::default);

fn state() -> MutexGuard<'static, Seam> {
    SEAM.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn admit(peer: SocketAddr) {
    state().admitted.insert(peer);
}

/// A listener that now owns a port an earlier, closed fake admitted must not inherit it.
pub(crate) fn withdraw(peer: SocketAddr) {
    state().admitted.remove(&peer);
}

pub(crate) fn is_admitted(peer: SocketAddr) -> bool {
    state().admitted.contains(&peer)
}

/// Each resolution of `host` takes the next answer, and the last answer repeats. An answer's
/// nonzero port is dialled when the URL names no port.
pub(crate) fn script_host(host: &str, answers: Vec<Vec<SocketAddr>>) {
    state().hosts.insert(host.to_owned(), answers.into());
}

pub(super) fn scripted_answer(host: &str) -> io::Result<Vec<SocketAddr>> {
    let mut seam = state();
    let answers = seam.hosts.get_mut(host).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("test host {host} has no scripted answer"),
        )
    })?;
    let answer = if answers.len() > 1 {
        answers.pop_front()
    } else {
        answers.front().cloned()
    };
    answer.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("test host {host} has an empty script"),
        )
    })
}

tokio::task_local! {
    static REFUSALS: RefCell<Vec<GatewayRefusalClass>>;
    static JUDGED_ADMITTED: RefCell<Vec<SocketAddr>>;
}

async fn capture<T, F>(sink: &'static LocalKey<RefCell<Vec<T>>>, future: F) -> (F::Output, Vec<T>)
where
    T: 'static,
    F: Future,
{
    sink.scope(RefCell::new(Vec::new()), async move {
        let output = future.await;
        let captured = sink.with(|captured| std::mem::take(&mut *captured.borrow_mut()));
        (output, captured)
    })
    .await
}

/// Runs `future` and returns the refusal classes the observer hook reported while it ran. The
/// hook fires on the request task, so a task-local sink sees every refusal of that lookup.
pub(crate) async fn capture_refusals<F>(future: F) -> (F::Output, Vec<GatewayRefusalClass>)
where
    F: Future,
{
    capture(&REFUSALS, future).await
}

pub(super) fn record_refusal(class: GatewayRefusalClass) {
    let _ = REFUSALS.try_with(|refusals| refusals.borrow_mut().push(class));
}

/// Runs `future` and returns each address the class table admitted while it ran, one entry per
/// judgement. An IP-literal host is judged in `fetch_one` on the request task, so every literal
/// judgement of the lookup is seen. A hostname is judged in the connector's resolver, which may
/// run on another task, so its judgements may be missed. No test relies on them.
pub(crate) async fn capture_judged_admitted<F>(future: F) -> (F::Output, Vec<SocketAddr>)
where
    F: Future,
{
    capture(&JUDGED_ADMITTED, future).await
}

/// Records one address the class table admitted. It says nothing about the resolution it came
/// from, which a refused sibling address still refuses whole. Addresses a fake admitted skip the
/// table and are not recorded.
pub(super) fn record_judged_admitted(peer: SocketAddr) {
    let _ = JUDGED_ADMITTED.try_with(|judged| judged.borrow_mut().push(peer));
}
