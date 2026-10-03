use std::{collections::HashMap, sync::Mutex};

use crate::{InterpretError, Result, RunMode, load::RetainedPrior};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct SessionKey {
    pub(super) chain_id: String,
    pub(super) from_block: i64,
    pub(super) mode: RunMode,
}

/// What a loader carries from one batch to the next, at most one per chain: the full-state
/// loader's interpreter session or the lookahead loader's whole ENSv2 registries.
#[derive(Default)]
pub(super) struct PriorSessions(Mutex<HashMap<String, Retained<RetainedPrior>>>);

struct Retained<T> {
    resumes: Resumes,
    next_block: i64,
    prior: T,
}

#[derive(Debug, Eq, PartialEq)]
enum Resumes {
    Request {
        from_block: i64,
        mode: RunMode,
    },
    /// A completed redo ends at the block Normal resumes from, so it hands its session to
    /// Normal's next batch, whatever block Normal's own request starts at.
    Normal,
}

impl PriorSessions {
    pub(super) fn take(
        &self,
        key: &SessionKey,
        next_block: i64,
        allow_resume: bool,
    ) -> Result<Option<RetainedPrior>> {
        Ok(take_resumable(
            &mut *self.lock()?,
            key,
            next_block,
            allow_resume,
        ))
    }

    pub(super) fn store(
        &self,
        key: SessionKey,
        next_block: i64,
        prior: RetainedPrior,
        complete: bool,
    ) -> Result<()> {
        retain(&mut *self.lock()?, key, next_block, prior, complete);
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<String, Retained<RetainedPrior>>>> {
        self.0.lock().map_err(|_| {
            InterpretError::transient("interpret prior-state session lock was poisoned")
        })
    }
}

fn take_resumable<T>(
    sessions: &mut HashMap<String, Retained<T>>,
    key: &SessionKey,
    next_block: i64,
    allow_resume: bool,
) -> Option<T> {
    // Moving the value out prevents a retained copy from overlapping the active batch and
    // makes the chain ID itself the one-session ownership boundary.
    let retained = sessions.remove(&key.chain_id)?;
    let resumes = match retained.resumes {
        Resumes::Request { from_block, mode } => from_block == key.from_block && mode == key.mode,
        Resumes::Normal => key.mode == RunMode::Normal,
    };
    (allow_resume && resumes && retained.next_block == next_block).then_some(retained.prior)
}

fn retain<T>(
    sessions: &mut HashMap<String, Retained<T>>,
    key: SessionKey,
    next_block: i64,
    prior: T,
    complete: bool,
) {
    let resumes = if key.mode == RunMode::Redo && complete {
        Resumes::Normal
    } else {
        Resumes::Request {
            from_block: key.from_block,
            mode: key.mode,
        }
    };
    sessions.insert(
        key.chain_id,
        Retained {
            resumes,
            next_block,
            prior,
        },
    );
}

#[cfg(test)]
#[path = "prior_sessions_tests.rs"]
mod tests;
