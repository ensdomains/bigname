//! Task-local pause immediately before the token evidence read, after name selection.
use std::{future::Future, sync::Arc};
use tokio::sync::Notify;

tokio::task_local! {
    static PAUSE: (Arc<Notify>, Arc<Notify>);
}

pub(crate) async fn with_pause<F: Future>(
    reached: Arc<Notify>,
    resume: Arc<Notify>,
    future: F,
) -> F::Output {
    PAUSE.scope((reached, resume), future).await
}

pub(super) async fn before_read() {
    if let Ok((reached, resume)) = PAUSE.try_with(Clone::clone) {
        reached.notify_one();
        resume.notified().await;
    }
}
