use anyhow::{Context, Result};
use bigname_storage::{
    BackfillJob, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange, CanonicalityInspection,
    CanonicalityInspectionStatus, CanonicalityState, DatabaseConfig, RawFactAuditCounts,
};
use clap::{Args, Subcommand};
use serde_json::{Value, json};
use sqlx::types::time::{OffsetDateTime, UtcOffset};

#[derive(Args, Debug)]
pub(crate) struct InspectArgs {
    #[command(subcommand)]
    pub(crate) command: InspectCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum InspectCommand {
    #[command(about = "Inspect one persisted backfill job and its child ranges")]
    BackfillJob(InspectBackfillJobArgs),
    #[command(about = "Inspect canonicality and block-scoped audit counts for one block hash")]
    Canonicality(InspectCanonicalityArgs),
}

#[derive(Args, Debug)]
pub(crate) struct InspectBackfillJobArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) backfill_job_id: i64,
}

#[derive(Args, Debug)]
pub(crate) struct InspectCanonicalityArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) block_hash: String,
}

pub(crate) async fn inspect_command(args: InspectArgs) -> Result<()> {
    match args.command {
        InspectCommand::BackfillJob(args) => inspect_backfill_job(args).await,
        InspectCommand::Canonicality(args) => inspect_canonicality(args).await,
    }
}

async fn inspect_backfill_job(args: InspectBackfillJobArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection = load_backfill_job_inspection(&pool, args.backfill_job_id).await?;

    println!("{}", render_backfill_job_inspection(&inspection));
    Ok(())
}

async fn inspect_canonicality(args: InspectCanonicalityArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection =
        bigname_storage::inspect_block_canonicality(&pool, &args.chain_id, &args.block_hash)
            .await?;

    println!("{}", render_canonicality_inspection(&inspection));
    Ok(())
}

async fn load_backfill_job_inspection(
    pool: &sqlx::PgPool,
    backfill_job_id: i64,
) -> Result<BackfillJobRecord> {
    let job = bigname_storage::load_backfill_job(pool, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    let mut ranges = bigname_storage::load_backfill_ranges(pool, backfill_job_id).await?;
    ranges.sort_by_key(|range| {
        (
            range.range_start_block_number,
            range.range_end_block_number,
            range.backfill_range_id,
        )
    });

    Ok(BackfillJobRecord { job, ranges })
}

fn render_backfill_job_inspection(inspection: &BackfillJobRecord) -> Value {
    json!({
        "job": render_backfill_job(&inspection.job),
        "ranges": inspection
            .ranges
            .iter()
            .map(render_backfill_range)
            .collect::<Vec<_>>(),
    })
}

fn render_backfill_job(job: &BackfillJob) -> Value {
    json!({
        "backfill_job_id": job.backfill_job_id,
        "deployment_profile": job.deployment_profile.as_str(),
        "chain_id": job.chain_id.as_str(),
        "source_identity": job.source_identity.clone(),
        "scan_mode": job.scan_mode.as_str(),
        "status": job.status.as_str(),
        "lifecycle": render_lifecycle_state(job.status),
        "declared_range": render_declared_range(
            job.range_start_block_number,
            job.range_end_block_number,
        ),
        "idempotency_key": job.idempotency_key.as_str(),
        "timestamps": render_timestamps(job.created_at, job.updated_at, job.completed_at),
        "failure": render_failure(job.failure_reason.as_deref(), &job.failure_metadata),
    })
}

fn render_backfill_range(range: &BackfillRange) -> Value {
    json!({
        "backfill_range_id": range.backfill_range_id,
        "backfill_job_id": range.backfill_job_id,
        "status": range.status.as_str(),
        "lifecycle": render_lifecycle_state(range.status),
        "declared_range": render_declared_range(
            range.range_start_block_number,
            range.range_end_block_number,
        ),
        "checkpoint": {
            "block_number": range.checkpoint_block_number,
        },
        "lease": {
            "owner": range.lease_owner.as_deref(),
            "token": range.lease_token.as_deref(),
            "expires_at": range.lease_expires_at.map(format_timestamp),
        },
        "attempt_count": range.attempt_count,
        "timestamps": render_timestamps(range.created_at, range.updated_at, range.completed_at),
        "failure": render_failure(range.failure_reason.as_deref(), &range.failure_metadata),
    })
}

fn render_lifecycle_state(status: BackfillLifecycleStatus) -> Value {
    json!({
        "status": status.as_str(),
        "pending": status == BackfillLifecycleStatus::Pending,
        "reserved": status == BackfillLifecycleStatus::Reserved,
        "running": status == BackfillLifecycleStatus::Running,
        "completed": status == BackfillLifecycleStatus::Completed,
        "failed": status == BackfillLifecycleStatus::Failed,
    })
}

fn render_declared_range(start_block_number: i64, end_block_number: i64) -> Value {
    json!({
        "start_block_number": start_block_number,
        "end_block_number": end_block_number,
    })
}

fn render_timestamps(
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
) -> Value {
    json!({
        "created_at": format_timestamp(created_at),
        "updated_at": format_timestamp(updated_at),
        "completed_at": completed_at.map(format_timestamp),
    })
}

fn render_failure(reason: Option<&str>, metadata: &Value) -> Value {
    json!({
        "reason": reason,
        "metadata": metadata.clone(),
    })
}

fn render_canonicality_inspection(inspection: &CanonicalityInspection) -> Value {
    json!({
        "chain_id": inspection.chain_id.as_str(),
        "block_hash": inspection.block_hash.as_str(),
        "status": canonicality_inspection_status_label(inspection.status),
        "lineage_canonicality": inspection.lineage_state.map(canonicality_state_label),
        "parent_hash": inspection.parent_hash.as_deref(),
        "block_number": inspection.block_number,
        "raw_fact_counts": render_raw_fact_counts(&inspection.raw_fact_counts),
        "normalized_event_count": inspection.normalized_event_count,
        "states": {
            "observed": inspection.status == CanonicalityInspectionStatus::Observed,
            "canonical": inspection.status == CanonicalityInspectionStatus::Canonical,
            "safe": inspection.status == CanonicalityInspectionStatus::Safe,
            "finalized": inspection.status == CanonicalityInspectionStatus::Finalized,
            "missing": inspection.status == CanonicalityInspectionStatus::Missing,
            "orphaned": inspection.status == CanonicalityInspectionStatus::Orphaned,
        }
    })
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn render_raw_fact_counts(counts: &RawFactAuditCounts) -> Value {
    json!({
        "raw_blocks": counts.raw_block_count,
        "raw_code_hashes": counts.raw_code_hash_count,
        "raw_transactions": counts.raw_transaction_count,
        "raw_receipts": counts.raw_receipt_count,
        "raw_logs": counts.raw_log_count,
        "raw_call_snapshots": counts.raw_call_snapshot_count,
        "total": counts.total(),
    })
}

const fn canonicality_inspection_status_label(
    status: CanonicalityInspectionStatus,
) -> &'static str {
    match status {
        CanonicalityInspectionStatus::Missing => "missing",
        CanonicalityInspectionStatus::Observed => "observed",
        CanonicalityInspectionStatus::Canonical => "canonical",
        CanonicalityInspectionStatus::Safe => "safe",
        CanonicalityInspectionStatus::Finalized => "finalized",
        CanonicalityInspectionStatus::Orphaned => "orphaned",
    }
}

const fn canonicality_state_label(state: CanonicalityState) -> &'static str {
    match state {
        CanonicalityState::Observed => "observed",
        CanonicalityState::Canonical => "canonical",
        CanonicalityState::Safe => "safe",
        CanonicalityState::Finalized => "finalized",
        CanonicalityState::Orphaned => "orphaned",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_storage::{BackfillJobCreate, BackfillRangeSpec};
    use serde_json::json;
    use sqlx::{
        ConnectOptions,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: sqlx::PgPool,
        pool: sqlx::PgPool,
        database_name: String,
        database_url: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| bigname_storage::default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for worker inspect tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_worker_inspect_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker inspect tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool_options = base_options.database(&database_name);
            let database_url = pool_options.to_url_lossy().to_string();
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(pool_options)
                .await
                .context("failed to connect worker inspect test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker inspect tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
                database_url,
            })
        }

        fn pool(&self) -> &sqlx::PgPool {
            &self.pool
        }

        fn database_config(&self) -> DatabaseConfig {
            DatabaseConfig {
                database_url: Some(self.database_url.clone()),
                max_connections: 2,
            }
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn backfill_job_create(idempotency_key: &str) -> BackfillJobCreate {
        BackfillJobCreate {
            deployment_profile: "mainnet".to_owned(),
            chain_id: "eth-mainnet".to_owned(),
            source_identity: json!({
                "source_family": "ens_v1_registry_l1",
                "watch_targets": ["0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e"]
            }),
            scan_mode: "logs".to_owned(),
            range_start_block_number: 100,
            range_end_block_number: 120,
            idempotency_key: idempotency_key.to_owned(),
            ranges: vec![
                BackfillRangeSpec {
                    range_start_block_number: 100,
                    range_end_block_number: 109,
                },
                BackfillRangeSpec {
                    range_start_block_number: 110,
                    range_end_block_number: 120,
                },
            ],
        }
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn lease_deadline() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
            .expect("lease deadline must be valid")
    }

    #[test]
    fn renders_backfill_job_inspection_json() {
        let inspection = BackfillJobRecord {
            job: BackfillJob {
                backfill_job_id: 42,
                deployment_profile: "mainnet".to_owned(),
                chain_id: "eth-mainnet".to_owned(),
                source_identity: json!({
                    "source_family": "ens_v1_registry_l1",
                    "watch_targets": ["0xregistry"]
                }),
                scan_mode: "logs".to_owned(),
                range_start_block_number: 100,
                range_end_block_number: 120,
                idempotency_key: "job-json-shape".to_owned(),
                status: BackfillLifecycleStatus::Running,
                failure_reason: None,
                failure_metadata: json!({}),
                created_at: timestamp(1_700_000_000),
                updated_at: timestamp(1_700_000_030),
                completed_at: None,
            },
            ranges: vec![
                BackfillRange {
                    backfill_range_id: 7,
                    backfill_job_id: 42,
                    range_start_block_number: 100,
                    range_end_block_number: 109,
                    checkpoint_block_number: 105,
                    status: BackfillLifecycleStatus::Running,
                    lease_token: Some("lease-a".to_owned()),
                    lease_owner: Some("worker-a".to_owned()),
                    lease_expires_at: Some(timestamp(1_700_000_300)),
                    attempt_count: 2,
                    failure_reason: None,
                    failure_metadata: json!({}),
                    created_at: timestamp(1_700_000_000),
                    updated_at: timestamp(1_700_000_040),
                    completed_at: None,
                },
                BackfillRange {
                    backfill_range_id: 8,
                    backfill_job_id: 42,
                    range_start_block_number: 110,
                    range_end_block_number: 120,
                    checkpoint_block_number: 110,
                    status: BackfillLifecycleStatus::Failed,
                    lease_token: None,
                    lease_owner: None,
                    lease_expires_at: None,
                    attempt_count: 1,
                    failure_reason: Some("rpc timeout".to_owned()),
                    failure_metadata: json!({ "block": 111 }),
                    created_at: timestamp(1_700_000_000),
                    updated_at: timestamp(1_700_000_050),
                    completed_at: None,
                },
            ],
        };

        let rendered = render_backfill_job_inspection(&inspection);

        assert_eq!(rendered["job"]["backfill_job_id"], 42);
        assert_eq!(rendered["job"]["deployment_profile"], "mainnet");
        assert_eq!(rendered["job"]["chain_id"], "eth-mainnet");
        assert_eq!(
            rendered["job"]["source_identity"]["source_family"],
            "ens_v1_registry_l1"
        );
        assert_eq!(rendered["job"]["scan_mode"], "logs");
        assert_eq!(rendered["job"]["status"], "running");
        assert_eq!(rendered["job"]["lifecycle"]["running"], true);
        assert_eq!(rendered["job"]["lifecycle"]["completed"], false);
        assert_eq!(rendered["job"]["declared_range"]["start_block_number"], 100);
        assert_eq!(rendered["job"]["declared_range"]["end_block_number"], 120);
        assert_eq!(rendered["job"]["idempotency_key"], "job-json-shape");
        assert_eq!(
            rendered["job"]["timestamps"]["created_at"],
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            rendered["job"]["timestamps"]["updated_at"],
            "2023-11-14T22:13:50Z"
        );
        assert!(rendered["job"]["timestamps"]["completed_at"].is_null());
        assert!(rendered["job"]["failure"]["reason"].is_null());
        assert_eq!(rendered["job"]["failure"]["metadata"], json!({}));

        assert_eq!(
            rendered["ranges"]
                .as_array()
                .expect("ranges must be an array")
                .len(),
            2
        );
        assert_eq!(rendered["ranges"][0]["backfill_range_id"], 7);
        assert_eq!(rendered["ranges"][0]["backfill_job_id"], 42);
        assert_eq!(rendered["ranges"][0]["status"], "running");
        assert_eq!(
            rendered["ranges"][0]["declared_range"]["start_block_number"],
            100
        );
        assert_eq!(
            rendered["ranges"][0]["declared_range"]["end_block_number"],
            109
        );
        assert_eq!(rendered["ranges"][0]["checkpoint"]["block_number"], 105);
        assert_eq!(rendered["ranges"][0]["lease"]["owner"], "worker-a");
        assert_eq!(rendered["ranges"][0]["lease"]["token"], "lease-a");
        assert_eq!(
            rendered["ranges"][0]["lease"]["expires_at"],
            "2023-11-14T22:18:20Z"
        );
        assert_eq!(rendered["ranges"][0]["attempt_count"], 2);
        assert_eq!(rendered["ranges"][1]["status"], "failed");
        assert_eq!(rendered["ranges"][1]["lifecycle"]["failed"], true);
        assert!(rendered["ranges"][1]["lease"]["owner"].is_null());
        assert!(rendered["ranges"][1]["lease"]["token"].is_null());
        assert!(rendered["ranges"][1]["lease"]["expires_at"].is_null());
        assert_eq!(rendered["ranges"][1]["failure"]["reason"], "rpc timeout");
        assert_eq!(
            rendered["ranges"][1]["failure"]["metadata"],
            json!({ "block": 111 })
        );
    }

    #[test]
    fn renders_canonicality_inspection_json() {
        let rendered = render_canonicality_inspection(&CanonicalityInspection {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xabc".to_owned(),
            status: CanonicalityInspectionStatus::Safe,
            lineage_state: Some(CanonicalityState::Safe),
            parent_hash: Some("0xparent".to_owned()),
            block_number: Some(123),
            raw_fact_counts: RawFactAuditCounts {
                raw_block_count: 1,
                raw_code_hash_count: 2,
                raw_transaction_count: 3,
                raw_receipt_count: 4,
                raw_log_count: 5,
                raw_call_snapshot_count: 6,
            },
            normalized_event_count: 7,
        });

        assert_eq!(rendered["chain_id"], "eth-mainnet");
        assert_eq!(rendered["block_hash"], "0xabc");
        assert_eq!(rendered["status"], "safe");
        assert_eq!(rendered["lineage_canonicality"], "safe");
        assert_eq!(rendered["parent_hash"], "0xparent");
        assert_eq!(rendered["block_number"], 123);
        assert_eq!(rendered["raw_fact_counts"]["raw_blocks"], 1);
        assert_eq!(rendered["raw_fact_counts"]["raw_code_hashes"], 2);
        assert_eq!(rendered["raw_fact_counts"]["raw_transactions"], 3);
        assert_eq!(rendered["raw_fact_counts"]["raw_receipts"], 4);
        assert_eq!(rendered["raw_fact_counts"]["raw_logs"], 5);
        assert_eq!(rendered["raw_fact_counts"]["raw_call_snapshots"], 6);
        assert_eq!(rendered["raw_fact_counts"]["total"], 21);
        assert_eq!(rendered["normalized_event_count"], 7);
        assert_eq!(rendered["states"]["safe"], true);
        assert_eq!(rendered["states"]["canonical"], false);
    }

    #[test]
    fn renders_missing_lineage_as_nulls() {
        let rendered = render_canonicality_inspection(&CanonicalityInspection {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xmissing".to_owned(),
            status: CanonicalityInspectionStatus::Missing,
            lineage_state: None,
            parent_hash: None,
            block_number: None,
            raw_fact_counts: RawFactAuditCounts::default(),
            normalized_event_count: 0,
        });

        assert_eq!(rendered["status"], "missing");
        assert!(rendered["lineage_canonicality"].is_null());
        assert!(rendered["parent_hash"].is_null());
        assert!(rendered["block_number"].is_null());
        assert_eq!(rendered["raw_fact_counts"]["total"], 0);
        assert_eq!(rendered["states"]["missing"], true);
        assert_eq!(rendered["states"]["orphaned"], false);
    }

    #[tokio::test]
    async fn inspect_backfill_job_missing_job_returns_error() -> Result<()> {
        let database = TestDatabase::new().await?;

        let error = inspect_backfill_job(InspectBackfillJobArgs {
            database: database.database_config(),
            backfill_job_id: 9_999_999,
        })
        .await
        .expect_err("missing backfill job inspection must fail");
        assert!(
            error.to_string().contains("missing backfill job 9999999"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_backfill_job_does_not_mutate_backfill_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let created = bigname_storage::create_backfill_job(
            database.pool(),
            &backfill_job_create("worker-inspect-readonly"),
        )
        .await?;
        let reserved = bigname_storage::reserve_backfill_range(
            database.pool(),
            created.job.backfill_job_id,
            "worker-a",
            "lease-a",
            lease_deadline(),
        )
        .await?
        .expect("range must be reservable");
        bigname_storage::advance_backfill_range(
            database.pool(),
            reserved.backfill_range_id,
            "lease-a",
            105,
        )
        .await?;

        let before =
            load_backfill_job_inspection(database.pool(), created.job.backfill_job_id).await?;

        inspect_backfill_job(InspectBackfillJobArgs {
            database: database.database_config(),
            backfill_job_id: created.job.backfill_job_id,
        })
        .await?;

        let after =
            load_backfill_job_inspection(database.pool(), created.job.backfill_job_id).await?;
        assert_eq!(after, before);

        database.cleanup().await
    }
}
