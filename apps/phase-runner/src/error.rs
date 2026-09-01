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
}

impl RunnerError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            lock_connection_lost: false,
            redo_attempt_superseded: false,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, message)
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
        let kind = if self.is_retryable() {
            secondary.kind
        } else {
            self.kind
        };
        Self::new(
            kind,
            format!("{self}; additionally failed to {action}: {secondary}"),
        )
    }

    pub(crate) fn lock_connection_lost(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Transient,
            message: message.into(),
            lock_connection_lost: true,
            redo_attempt_superseded: false,
        }
    }

    pub(crate) fn redo_attempt_superseded(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::DataIntegrity,
            message: message.into(),
            lock_connection_lost: false,
            redo_attempt_superseded: true,
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn is_retryable(&self) -> bool {
        self.kind == ErrorKind::Transient
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
