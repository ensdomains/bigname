//! Its own test binary on purpose: tokio installs each signal handler
//! process-wide on the first `signal()` call, so a sibling test that registered
//! first would hide the window this asserts.

#[cfg(unix)]
#[test]
fn cancel_on_signal_registers_sigint_before_it_returns() {
    use std::{process::Command, time::Duration};

    use tokio_util::sync::CancellationToken;

    // One worker thread, so the waiter `cancel_on_signal` spawns cannot be polled
    // while this test does synchronous work -- the same shape as `main` hashing the
    // manifest repository right after the call. Registering inside that task would
    // leave SIGINT at its default disposition here and kill the process instead of
    // cancelling the token, so there is deliberately no sleep before signalling.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build a current-thread runtime");
    runtime.block_on(async {
        let cancellation = CancellationToken::new();
        phase_runner::shutdown::cancel_on_signal(&cancellation);

        let killed = Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .expect("raise SIGINT");
        assert!(killed.success(), "kill -INT failed: {killed}");

        tokio::time::timeout(Duration::from_secs(5), cancellation.cancelled())
            .await
            .expect("SIGINT was not registered before cancel_on_signal returned");
    });
}
