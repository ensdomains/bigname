use std::{error::Error, fmt};

use crate::integrity::GenerationFailureEvidence;

pub type Result<T> = std::result::Result<T, ProjectError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Transient,
    DataIntegrity,
    Configuration,
}

#[derive(Debug)]
pub struct ProjectError {
    kind: ErrorKind,
    message: String,
    evidence: Option<Box<GenerationFailureEvidence>>,
}

impl ProjectError {
    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, message)
    }

    pub fn data_integrity(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::DataIntegrity, message)
    }

    pub fn configuration(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Configuration, message)
    }

    pub fn database(context: impl AsRef<str>, error: sqlx::Error) -> Self {
        let message = format!("{}: {error}", context.as_ref());
        if matches!(
            &error,
            sqlx::Error::Database(database)
                if database.code().is_some_and(|code| code.starts_with("23"))
        ) {
            Self::data_integrity(message)
        } else {
            Self::transient(message)
        }
    }

    /// A projection-blocking invariant failure, carrying the evidence the phase
    /// runner appends after this transaction rolls back.
    pub fn generation_failure(
        message: impl Into<String>,
        evidence: GenerationFailureEvidence,
    ) -> Self {
        Self {
            kind: ErrorKind::DataIntegrity,
            message: message.into(),
            evidence: Some(Box::new(evidence)),
        }
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn generation_failure_evidence(&self) -> Option<&GenerationFailureEvidence> {
        self.evidence.as_deref()
    }

    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            evidence: None,
        }
    }
}

impl fmt::Display for ProjectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProjectError {}
