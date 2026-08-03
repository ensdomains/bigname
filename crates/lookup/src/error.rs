#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Configuration,
    Unsupported,
    Stale,
    Execution,
    Database,
    ConcurrentState,
}

#[derive(Debug)]
pub struct LookupError {
    kind: ErrorKind,
    message: String,
}

impl LookupError {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn configuration(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Configuration, message)
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }

    pub(crate) fn stale(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Stale, message)
    }

    pub(crate) fn execution(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Execution, message)
    }

    pub(crate) fn database(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Database, message)
    }

    pub(crate) fn concurrent_state(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ConcurrentState, message)
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LookupError {}

pub type Result<T> = std::result::Result<T, LookupError>;

pub(crate) fn database(context: &'static str) -> impl FnOnce(sqlx::Error) -> LookupError {
    move |error| LookupError::database(format!("{context}: {error}"))
}
