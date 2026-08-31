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
        let kind = database_error_kind(&error);
        Self::with_source(kind, message, error)
    }

    pub fn database_anyhow(message: impl Into<String>, error: anyhow::Error) -> Self {
        let kind = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
            .map_or(ErrorKind::DataIntegrity, database_error_kind);
        Self::with_source(kind, message, error)
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anyhow_wrapped_transient_database_error_stays_retryable() {
        let error = anyhow::Error::new(sqlx::Error::PoolTimedOut)
            .context("injected discovery watch database timeout");
        let classified = IngestError::database_anyhow(
            "failed to load discovery-derived ingest intervals",
            error,
        );

        assert_eq!(classified.kind(), ErrorKind::Transient);
    }

    #[test]
    fn anyhow_without_a_database_error_stays_data_integrity() {
        let classified = IngestError::database_anyhow(
            "failed to load discovery-derived ingest intervals",
            anyhow::anyhow!("invalid discovery watch payload"),
        );

        assert_eq!(classified.kind(), ErrorKind::DataIntegrity);
    }
}
