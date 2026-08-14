use std::{error::Error, fmt};

pub const REDO_BOUNDARY_DIVERGENCE_PREFIX: &str = "ingest redo boundary changed during resume";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Transient,
    DataIntegrity,
    Configuration,
}

#[derive(Debug)]
pub struct IngestError {
    kind: ErrorKind,
    message: String,
    source: Option<anyhow::Error>,
}

impl IngestError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, message)
    }

    pub fn data_integrity(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::DataIntegrity, message)
    }

    pub fn configuration(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Configuration, message)
    }

    pub fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl Into<anyhow::Error>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(source.into()),
        }
    }

    pub fn database(message: impl Into<String>, error: sqlx::Error) -> Self {
        let kind = match &error {
            sqlx::Error::Database(database)
                if database.code().is_some_and(|code| code.starts_with("23")) =>
            {
                ErrorKind::DataIntegrity
            }
            _ => ErrorKind::Transient,
        };
        Self::with_source(kind, message, error)
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl fmt::Display for IngestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)?;
        if let Some(source) = &self.source {
            write!(formatter, ": {source:#}")?;
        }
        Ok(())
    }
}

impl Error for IngestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn Error + 'static))
    }
}

pub type Result<T> = std::result::Result<T, IngestError>;
