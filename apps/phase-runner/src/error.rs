use std::{error::Error, fmt};

pub(crate) const VERIFICATION_MISMATCH_PREFIX: &str = "verification mismatch: ";
pub(crate) const COMPLETED_VALIDATION_FAILURE_PREFIX: &str = "completed phase validation failed: ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Transient,
    DataIntegrity,
    VerificationMismatch,
    LockHeld,
    ContentHashMismatch,
    InvalidTransition,
    Configuration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerError {
    kind: ErrorKind,
    message: String,
    lock_connection_lost: bool,
    redo_attempt_superseded: bool,
    stop_bound_expired: bool,
}

impl RunnerError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            lock_connection_lost: false,
            redo_attempt_superseded: false,
            stop_bound_expired: false,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, message)
    }

    /// Required work outran the deadline an accepted stop put on it. Transient in
    /// nature -- the next start repeats the work -- but not retried by this run,
    /// which is stopping; it surfaces so the stop exits nonzero.
    pub(crate) fn stop_bound_expired(message: impl Into<String>) -> Self {
        Self {
            stop_bound_expired: true,
            ..Self::transient(message)
        }
    }

    pub fn data_integrity(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::DataIntegrity, message)
    }

    pub fn verification_mismatch(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::VerificationMismatch, message)
    }

    pub(crate) fn database(message: impl Into<String>, error: sqlx::Error) -> Self {
        let message = format!("{}: {error}", message.into());
        Self::new(database_error_kind(&error), message)
    }

    pub(crate) fn database_anyhow(message: impl Into<String>, error: anyhow::Error) -> Self {
        let kind = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
            .map_or(ErrorKind::DataIntegrity, database_error_kind);
        Self::new(kind, format!("{}: {error:#}", message.into()))
    }

    pub(crate) fn with_secondary(self, action: &str, secondary: Self) -> Self {
        // A retryable primary defers to the secondary's kind; a stop-bound
        // expiry on either side keeps the combined error from being retried.
        let kind = if self.is_retryable() {
            secondary.kind
        } else {
            self.kind
        };
        Self {
            stop_bound_expired: self.stop_bound_expired || secondary.stop_bound_expired,
            ..Self::new(
                kind,
                format!("{self}; additionally failed to {action}: {secondary}"),
            )
        }
    }

    pub(crate) fn lock_connection_lost(message: impl Into<String>) -> Self {
        Self {
            lock_connection_lost: true,
            ..Self::transient(message)
        }
    }

    pub(crate) fn redo_attempt_superseded(message: impl Into<String>) -> Self {
        Self {
            redo_attempt_superseded: true,
            ..Self::data_integrity(message)
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn is_retryable(&self) -> bool {
        self.kind == ErrorKind::Transient && !self.stop_bound_expired
    }

    pub(crate) fn permits_pool_writes_after_error(&self) -> bool {
        !self.lock_connection_lost && !self.redo_attempt_superseded
    }
}

fn database_error_kind(error: &sqlx::Error) -> ErrorKind {
    if matches!(
        error,
        sqlx::Error::Database(database)
            if database.code().is_some_and(|code| code.starts_with("23"))
    ) {
        ErrorKind::DataIntegrity
    } else {
        ErrorKind::Transient
    }
}

impl fmt::Display for RunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RunnerError {}

pub type RunnerResult<T> = Result<T, RunnerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anyhow_wrapped_transient_database_error_stays_retryable() {
        let error = anyhow::Error::new(sqlx::Error::PoolTimedOut)
            .context("injected discovery repair database timeout");
        let classified = RunnerError::database_anyhow(
            "failed to classify discovery-owned required Ingest work",
            error,
        );

        assert_eq!(classified.kind(), ErrorKind::Transient);
        assert!(classified.is_retryable());
    }
}
