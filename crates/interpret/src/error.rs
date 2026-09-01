use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Transient,
    DataIntegrity,
    Configuration,
}

#[derive(Debug)]
pub struct InterpretError {
    kind: ErrorKind,
    message: String,
}

impl InterpretError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
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

    pub fn database(context: impl Into<String>, error: sqlx::Error) -> Self {
        Self::new(
            database_error_kind(&error),
            format!("{}: {error}", context.into()),
        )
    }

    pub fn database_anyhow(context: impl Into<String>, error: anyhow::Error) -> Self {
        let kind = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
            .map_or(ErrorKind::DataIntegrity, database_error_kind);
        Self::new(kind, format!("{}: {error:#}", context.into()))
    }

    pub fn kind(&self) -> ErrorKind {
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

impl fmt::Display for InterpretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for InterpretError {}

pub type Result<T> = std::result::Result<T, InterpretError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anyhow_wrapped_transient_database_error_stays_retryable() {
        let error = anyhow::Error::new(sqlx::Error::PoolTimedOut)
            .context("injected discovery admission database timeout");
        let classified = InterpretError::database_anyhow(
            "failed to derive final discovery watch admissions",
            error,
        );

        assert_eq!(classified.kind(), ErrorKind::Transient);
    }
}
