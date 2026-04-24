use std::fmt;

/// API-facing failure class for exact-name snapshot selection and projection eligibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotSelectionErrorKind {
    InvalidInput,
    Conflict,
    Stale,
    InternalError,
}

impl SnapshotSelectionErrorKind {
    pub const fn api_error_code(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::Conflict => "conflict",
            Self::Stale => "stale",
            Self::InternalError => "internal_error",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSelectionError {
    kind: SnapshotSelectionErrorKind,
    message: String,
}

impl SnapshotSelectionError {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::InvalidInput,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::Conflict,
            message: message.into(),
        }
    }

    pub fn stale(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::Stale,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::InternalError,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> SnapshotSelectionErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn api_error_code(&self) -> &'static str {
        self.kind.api_error_code()
    }
}

impl fmt::Display for SnapshotSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.kind.api_error_code(),
            self.message
        )
    }
}

impl std::error::Error for SnapshotSelectionError {}

pub type SnapshotSelectionResult<T> = std::result::Result<T, SnapshotSelectionError>;
