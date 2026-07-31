use std::{error::Error, fmt};

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
}

impl RunnerError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            lock_connection_lost: false,
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

    pub(crate) fn lock_connection_lost(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Transient,
            message: message.into(),
            lock_connection_lost: true,
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn is_retryable(&self) -> bool {
        self.kind == ErrorKind::Transient
    }

    pub(crate) fn permits_pool_writes_after_error(&self) -> bool {
        !self.lock_connection_lost
    }
}

impl fmt::Display for RunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RunnerError {}

pub type RunnerResult<T> = Result<T, RunnerError>;
