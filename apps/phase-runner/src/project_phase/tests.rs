//! How a family failure reaches the runner: its kind, and whether the supervised runner retries
//! it. Database failures are built by `ProjectError::database`, as every family write builds them.
use std::{borrow::Cow, error::Error, fmt};

use bigname_project::{Marker, ProjectError, families::FamilyOutcome};
use sqlx::{error::DatabaseError, postgres::PgPoolOptions};

use super::{FamilySettings, ProjectPhase};
use crate::error::{ErrorKind, RunnerError};

// PostgreSQL's primary messages, as an English server words them. sqlx displays only this
// message, not its detail or hint.
const NUL_IN_JSONB: &str = "unsupported Unicode escape sequence";
const NUL_IN_TEXT: &str = "invalid byte sequence for encoding \"UTF8\": 0x00";
const KEY_OVER_BTREE_ROW: &str = "index row size 4016 exceeds btree version 4 maximum 2704 for \
                                  index \"project_node_record_value_pkey\"";
const KEY_OVER_INDEX_ROW: &str = "index row requires 10016 bytes, maximum size is 8191";

/// A PostgreSQL error as sqlx reports one.
#[derive(Debug)]
struct ServerError {
    code: &'static str,
    message: &'static str,
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for ServerError {}

impl DatabaseError for ServerError {
    fn message(&self) -> &str {
        self.message
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.code))
    }

    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> sqlx::error::ErrorKind {
        sqlx::error::ErrorKind::Other
    }
}

fn write_failure(code: &'static str, message: &'static str) -> ProjectError {
    ProjectError::database(
        "failed to write project_node_record_value rows",
        sqlx::Error::Database(Box::new(ServerError { code, message })),
    )
}

fn runner_error(retry_family_failures: bool, error: &ProjectError) -> RunnerError {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://bigname@127.0.0.1:1/bigname")
        .expect("a lazy pool never connects");
    ProjectPhase::new(pool)
        .with_family_settings(FamilySettings {
            retry_family_failures,
            ..FamilySettings::default()
        })
        .family_error(
            "ethereum-mainnet",
            &Marker {
                number: 2,
                hash: "0x02".to_owned(),
            },
            &FamilyOutcome::default(),
            error,
        )
}

/// A value PostgreSQL refuses to store stops the chain once, as a data-integrity error, whether
/// or not family failures are retried.
fn assert_not_retried(code: &'static str, message: &'static str) {
    let error = write_failure(code, message);
    for retry_family_failures in [true, false] {
        let runner = runner_error(retry_family_failures, &error);
        assert_eq!(
            (runner.kind(), runner.is_retryable()),
            (ErrorKind::DataIntegrity, false),
            "{code} under retry {retry_family_failures}: {runner}"
        );
        assert!(runner.to_string().ends_with(message), "{runner}");
    }
}

#[tokio::test]
async fn a_nul_in_a_jsonb_value_is_not_retried() {
    assert_not_retried("22P05", NUL_IN_JSONB);
}

#[tokio::test]
async fn a_nul_in_a_text_value_is_not_retried() {
    assert_not_retried("22021", NUL_IN_TEXT);
}

#[tokio::test]
async fn a_key_over_the_btree_row_limit_is_not_retried() {
    assert_not_retried("54000", KEY_OVER_BTREE_ROW);
}

#[tokio::test]
async fn a_key_over_the_index_row_limit_is_not_retried() {
    assert_not_retried("54000", KEY_OVER_INDEX_ROW);
}

// Wording the match does not know keeps today's classification: retried under supervision, and
// returned as a transient error by the one-shot redo. That covers a server whose `lc_messages`
// is not English, and the refusal's words appearing anywhere but in a database error.
#[tokio::test]
async fn an_unknown_wording_keeps_its_classification() {
    for error in [
        write_failure("22021", "ungültige Byte-Sequenz für Kodierung »UTF8«: 0x00"),
        write_failure("22001", "value too long for type character varying(64)"),
        ProjectError::transient("unsupported Unicode escape sequence in a fixture"),
    ] {
        let supervised = runner_error(true, &error);
        assert_eq!(
            (supervised.kind(), supervised.is_retryable()),
            (ErrorKind::Transient, true),
            "{supervised}"
        );
        let redo = runner_error(false, &error);
        assert_eq!(
            (redo.kind(), redo.is_retryable()),
            (ErrorKind::Transient, false),
            "{redo}"
        );
    }
}

// A class-23 integrity violation is unchanged: folded into a retried transient error under
// supervision, and a data-integrity error that is not retried by the one-shot redo.
#[tokio::test]
async fn a_class_23_violation_keeps_its_classification() {
    let error = write_failure(
        "23505",
        "duplicate key value violates unique constraint \"project_node_record_value_pkey\"",
    );
    assert_eq!(error.kind(), bigname_project::ErrorKind::DataIntegrity);
    let supervised = runner_error(true, &error);
    assert_eq!(
        (supervised.kind(), supervised.is_retryable()),
        (ErrorKind::Transient, true),
        "{supervised}"
    );
    let redo = runner_error(false, &error);
    assert_eq!(
        (redo.kind(), redo.is_retryable()),
        (ErrorKind::DataIntegrity, false),
        "{redo}"
    );
}

// A lost or exhausted connection is unchanged: retried under supervision.
#[tokio::test]
async fn a_connection_failure_keeps_its_classification() {
    let error = ProjectError::database(
        "failed to write project_node_record_value rows",
        sqlx::Error::PoolTimedOut,
    );
    let supervised = runner_error(true, &error);
    assert_eq!(
        (supervised.kind(), supervised.is_retryable()),
        (ErrorKind::Transient, true),
        "{supervised}"
    );
    let redo = runner_error(false, &error);
    assert_eq!(
        (redo.kind(), redo.is_retryable()),
        (ErrorKind::Transient, false),
        "{redo}"
    );
}
