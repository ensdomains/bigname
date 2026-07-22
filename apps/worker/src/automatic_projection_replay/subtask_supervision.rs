use std::{future::Future, pin::Pin};

use anyhow::{Result, anyhow};
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::mpsc,
    task::{AbortHandle, JoinError},
};

type SubtaskFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
type JoinedSubtask = Pin<Box<dyn Future<Output = SubtaskExit> + Send + 'static>>;

struct SubtaskRegistration {
    name: &'static str,
    future: SubtaskFuture,
}

struct SubtaskExit {
    name: &'static str,
    result: std::result::Result<Result<()>, JoinError>,
}

impl SubtaskExit {
    fn into_error(self) -> anyhow::Error {
        match self.result {
            Ok(Ok(())) => anyhow!("required spawned subtask {} exited unexpectedly", self.name),
            Ok(Err(error)) => {
                error.context(format!("required spawned subtask {} failed", self.name))
            }
            Err(error) if error.is_panic() => {
                anyhow!("required spawned subtask {} panicked: {error}", self.name)
            }
            Err(error) => anyhow!(
                "required spawned subtask {} was cancelled unexpectedly: {error}",
                self.name
            ),
        }
    }
}

#[derive(Clone)]
pub(super) struct SubtaskSpawner {
    registrations: mpsc::UnboundedSender<SubtaskRegistration>,
}

impl SubtaskSpawner {
    pub(super) fn spawn<Subtask>(&self, name: &'static str, subtask: Subtask) -> Result<()>
    where
        Subtask: Future<Output = Result<()>> + Send + 'static,
    {
        self.registrations
            .send(SubtaskRegistration {
                name,
                future: Box::pin(subtask),
            })
            .map_err(|_| anyhow!("subtask supervisor stopped before {name} could start"))
    }
}

pub(super) struct SubtaskMonitor {
    service: &'static str,
    registrations: mpsc::UnboundedReceiver<SubtaskRegistration>,
}

pub(super) fn channel(service: &'static str) -> (SubtaskSpawner, SubtaskMonitor) {
    let (registrations, registration_rx) = mpsc::unbounded_channel();
    (
        SubtaskSpawner { registrations },
        SubtaskMonitor {
            service,
            registrations: registration_rx,
        },
    )
}

impl SubtaskMonitor {
    pub(super) async fn run<Work>(mut self, work: Work) -> Result<()>
    where
        Work: Future<Output = Result<()>>,
    {
        let mut running = FuturesUnordered::<JoinedSubtask>::new();
        let mut abort_on_drop = AbortOnDrop::default();
        let mut registrations_open = true;
        tokio::pin!(work);

        loop {
            tokio::select! {
                result = &mut work => return result,
                registration = self.registrations.recv(), if registrations_open => {
                    let Some(registration) = registration else {
                        registrations_open = false;
                        continue;
                    };
                    let name = registration.name;
                    let handle = tokio::spawn(registration.future);
                    abort_on_drop.handles.push(handle.abort_handle());
                    running.push(Box::pin(async move {
                        SubtaskExit {
                            name,
                            result: handle.await,
                        }
                    }));
                }
                exit = running.next(), if !running.is_empty() => {
                    let exit = exit.expect("a non-empty subtask set must yield an exit");
                    let name = exit.name;
                    let error = exit.into_error();
                    tracing::error!(
                        service = self.service,
                        subtask = name,
                        error = %format!("{error:#}"),
                        "required spawned subtask stopped; terminating parent loop"
                    );
                    return Err(error);
                }
            }
        }
    }
}

#[derive(Default)]
struct AbortOnDrop {
    handles: Vec<AbortHandle>,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
