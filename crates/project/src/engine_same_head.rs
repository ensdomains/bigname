//! PROFILED DIAGNOSTIC ONLY. Current published pointers/inventory differ from a pre-window
//! baseline, so neither this scope nor elapsed time proves authentic catch-up performance.
use super::super::{BatchRequest, Marker, RunMode};
use anyhow::{Context, Result, ensure};
use sqlx::{
    Postgres, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{path::Path, str::FromStr, time::Instant};

const CHAIN: &str = "ethereum-sepolia";
const HEAD: i64 = 11_756_310;
const FROM: i64 = 11_756_251;
const BURST_FROM: i64 = 11_756_236;

#[tokio::test]
#[ignore = "explicit opt-in paused same-head Sepolia profiling diagnostic only"]
async fn retained_sepolia_same_head_profile_rollback() -> Result<()> {
    ensure!(
        std::env::var("BIGNAME_SAME_HEAD_PROFILE").as_deref() == Ok("1"),
        "same-head diagnostic requires explicit opt-in"
    );
    let _ = tracing_subscriber::fmt()
        .with_env_filter("bigname_project=debug")
        .with_target(false)
        .try_init();
    let options = PgConnectOptions::from_str(&std::env::var("BIGNAME_BENCHMARK_DATABASE_URL")?)?
        .application_name("bigname-project-same-head-profile")
        .options([("search_path", "bigname_phase,public")]);
    run(
        options,
        Path::new(&std::env::var("BIGNAME_BENCHMARK_EVIDENCE_DIR")?),
        std::env::var("BIGNAME_SAME_HEAD_TARGET")?.parse()?,
        std::env::var("BIGNAME_SAME_HEAD_FROM")?.parse()?,
        std::env::var("BIGNAME_SAME_HEAD_TO")?.parse()?,
        linked_index_opt_in(
            std::env::var("BIGNAME_SAME_HEAD_LINKED_INDEX")
                .ok()
                .as_deref(),
        )?,
        disable_jit_opt_in(
            std::env::var("BIGNAME_SAME_HEAD_DISABLE_JIT")
                .ok()
                .as_deref(),
        )?,
    )
    .await?;
    Ok(())
}

fn linked_index_opt_in(value: Option<&str>) -> Result<bool> {
    match value {
        None => Ok(false),
        Some("1") => Ok(true),
        _ => anyhow::bail!("same-head linked index requires explicit value 1 or omission"),
    }
}

fn disable_jit_opt_in(value: Option<&str>) -> Result<bool> {
    match value {
        None => Ok(false),
        Some("1") => Ok(true),
        _ => anyhow::bail!("same-head JIT diagnostic requires explicit value 1 or omission"),
    }
}

async fn disable_jit(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("SET LOCAL jit=off").execute(&mut **tx).await?;
    eprintln!("SEPOLIA_SAME_HEAD_JIT disabled=true transaction_local=true acceptance=forbidden");
    Ok(())
}

// Diagnostic-only: the exact additive index is created after the existing guards,
// belongs to the diagnostic transaction, and disappears with its outer rollback.
async fn create_linked_index(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let old_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&mut **tx)
        .await?;
    sqlx::query("SET LOCAL lock_timeout='5s'")
        .execute(&mut **tx)
        .await?;
    let statement = include_str!("../testdata/sql/stage/linked_records_index_candidate.sql")
        .replace("ON normalized_events", "ON bigname_phase.normalized_events");
    sqlx::query(&statement).execute(&mut **tx).await?;
    let (valid, needs_new_snapshot): (bool, bool) = sqlx::query_as(
        "SELECT indisvalid AND indisready AND indrelid='bigname_phase.normalized_events'::regclass, indcheckxmin
         FROM pg_index WHERE indexrelid='bigname_phase.normalized_events_linked_resolver_history_idx'::regclass",
    ).fetch_one(&mut **tx).await?;
    ensure!(
        valid,
        "candidate linked resolver index is not valid and ready"
    );
    ensure!(
        !needs_new_snapshot,
        "candidate linked resolver index cannot be measured in its creating snapshot"
    );
    sqlx::query("SELECT set_config('lock_timeout',$1,true)")
        .bind(old_timeout)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn bounds(target: i64, from: i64, to: i64) -> Result<()> {
    ensure!(
        target == HEAD && to == HEAD && matches!(from, FROM | BURST_FROM),
        "same-head diagnostic is pinned to Sepolia11756310 and either11756251..11756310 or11756236..11756310"
    );
    Ok(())
}

/// One `chain_phase_state` row as the guard reads it: phase name, status, current block number
/// and hash, redo flag, last error, and whether the run finished after it started.
type PhaseState = (
    String,
    String,
    Option<i64>,
    Option<String>,
    bool,
    Option<String>,
    bool,
);

async fn guard(
    tx: &mut Transaction<'_, Postgres>,
    target: &Marker,
    from: i64,
    to: i64,
) -> Result<()> {
    bounds(target.number, from, to)?;
    let phases: Vec<PhaseState> = sqlx::query_as(
        "SELECT phase_name,phase_status,current_block_number,current_block_hash,redo_in_progress,last_error,
                started_at IS NOT NULL AND finished_at IS NOT NULL AND finished_at>=started_at
         FROM chain_phase_state WHERE chain_id=$1 AND phase_name IN ('project','interpret') FOR SHARE")
        .bind(CHAIN).fetch_all(&mut **tx).await?;
    ensure!(
        phases.len() == 2,
        "same-head diagnostic requires both Project and Interpret state"
    );
    for (phase, status, number, hash, redo, error, finished) in phases {
        ensure!(
            status == "completed"
                && number == Some(target.number)
                && hash.as_deref() == Some(&target.hash)
                && !redo
                && error.is_none()
                && finished,
            "same-head diagnostic phase guard rejected {phase}"
        );
    }
    let readable: (i64, i64) = sqlx::query_as(
        "SELECT count(*),count(DISTINCT block_number) FROM chain_lineage
         WHERE chain_id=$1 AND block_number BETWEEN $2 AND $3
           AND canonicality_state IN ('canonical','safe','finalized')",
    )
    .bind(CHAIN)
    .bind(from)
    .bind(to)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        readable == (to - from + 1, to - from + 1),
        "same-head diagnostic needs one readable hash at every window height"
    );
    super::super::revalidate_target(tx, CHAIN, target).await?;
    super::safety::projection_schema(tx).await?;
    Ok(())
}

async fn run(
    options: PgConnectOptions,
    evidence: &Path,
    number: i64,
    from: i64,
    to: i64,
    linked_index: bool,
    jit_off: bool,
) -> Result<(u64, i64)> {
    bounds(number, from, to)?;
    let blocks = to - from + 1;
    eprintln!(
        "SEPOLIA_SAME_HEAD_DIAGNOSTIC target={number} from={from} to={to} blocks={blocks} mode=PROFILED acceptance=forbidden authentic_catchup=false current_pointer_baseline=true comparison=not_run"
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    let target = super::super::load_marker(&pool, CHAIN, number).await?;
    let request = BatchRequest {
        chain_id: CHAIN.into(),
        target_block: number,
        affected_from_block: from,
        affected_to_block: to,
        resume_current: Some(target.clone()),
        mode: RunMode::Normal,
    };
    super::super::validate_request(&request)?;
    super::super::validate_resume(&pool, &request, &target).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL bigname.benchmark_reference='off'")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL statement_timeout='20min'")
        .execute(&mut *tx)
        .await?;
    guard(&mut tx, &target, from, to).await?;
    if jit_off {
        disable_jit(&mut tx).await?;
    }
    if linked_index {
        let setup = Instant::now();
        create_linked_index(&mut tx).await?;
        eprintln!(
            "SEPOLIA_SAME_HEAD_LINKED_INDEX setup_ms={} transactional=true rollback=required excluded_from_diagnostic_elapsed=true acceptance=forbidden",
            setup.elapsed().as_millis()
        );
    }
    let session = crate::profile::Session::create(evidence)?;
    let started = Instant::now();
    let rows = session
        .scope(super::super::derive(&mut tx, &request, &target))
        .await?;
    let elapsed = started.elapsed().as_millis();
    let counts:(i64,i64,i64)=sqlx::query_as(
        "SELECT (SELECT count(*) FROM project_scope_names),(SELECT count(*) FROM project_scope_resources),(SELECT count(*) FROM project_events)")
        .fetch_one(&mut *tx).await?;
    eprintln!(
        "SEPOLIA_SAME_HEAD_DIAGNOSTIC target={number} from={from} to={to} blocks={blocks} rows={rows} elapsed_ms={elapsed} names={} resources={} events={} measurement=PROFILED acceptance=forbidden",
        counts.0, counts.1, counts.2
    );
    tx.rollback()
        .await
        .context("same-head diagnostic explicit rollback failed")?;
    pool.close().await;
    eprintln!(
        "SEPOLIA_SAME_HEAD_DIAGNOSTIC_ROLLBACK complete=true phase_metadata=not_invoked hydration=not_invoked"
    );
    Ok((rows, counts.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn fixture() -> Result<bigname_test_support::TestDatabase> {
        let database = super::super::lifecycle_database().await?;
        sqlx::raw_sql(&format!("SET search_path TO bigname_phase,public;
            INSERT INTO chain_lineage(chain_id,block_number,block_hash,block_timestamp,canonicality_state)
            SELECT '{CHAIN}',i,'block-'||i,to_timestamp(1800000000+i),'canonical' FROM generate_series({BURST_FROM},{HEAD})i;
            DELETE FROM chain_phase_state;
            INSERT INTO chain_phase_state(chain_id,phase_name,phase_status,current_block_number,current_block_hash,started_at,finished_at)
            SELECT '{CHAIN}',phase,'completed',{HEAD},'block-{HEAD}',now(),now() FROM unnest(ARRAY['project','interpret'])phase;
            INSERT INTO name_surfaces(logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
            VALUES('ens:profile-name','ens','profile-name',ARRAY['profile-name'],'\\x00','profile-name',ARRAY['label'],'test','active','{CHAIN}','block-{HEAD}',{HEAD},'canonical');
            INSERT INTO normalized_events(event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state)
            VALUES('same-head-fixture','ens','ens:profile-name','ResolverChanged','ens_v1_registry_l1',1,'{CHAIN}',{HEAD},'block-{HEAD}','ens_v1_unwrapped_authority','canonical');
            INSERT INTO resources(resource_id,chain_id,block_hash,block_number,provenance,canonicality_state)
            VALUES('69100000-0000-0000-0000-000000000001','{CHAIN}','block-{HEAD}',{HEAD},'{{\"source_family\":\"ens_v1_registry_l1\",\"authority_kind\":\"registry\"}}','canonical');
            UPDATE normalized_events SET resource_id='69100000-0000-0000-0000-000000000001',after_state='{{\"resolver\":\"0x1111111111111111111111111111111111111111\",\"authority_kind\":\"registry\"}}' WHERE event_identity='same-head-fixture';
            INSERT INTO name_current(logical_name_id,namespace,raw_name,namehash,support_status,unsupported_reason,manifest_version,provenance)
            VALUES('ens:profile-name','ens','sentinel','profile-name','unsupported','fixture',1,'{{\"sentinel\":true}}');"))
            .execute(database.pool()).await?;
        let payload = serde_json::json!({"contracts": [{
            "address": "0x1111111111111111111111111111111111111111",
            "role": "public_resolver", "start_block": FROM
        }]});
        let manifest: i64 = sqlx::query_scalar(
            "INSERT INTO bigname_phase.manifest_versions(manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload)
             VALUES(1,'ens','ens_v1_resolver_l1',$1,'test','active','test','test/profile.toml',$2) RETURNING manifest_id")
            .bind(CHAIN).bind(&payload).fetch_one(database.pool()).await?;
        sqlx::query("INSERT INTO bigname_phase.normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state)
                     VALUES('profile-manifest','ens','SourceManifestUpdated','ens_v1_resolver_l1',1,$1,$2,'manifest_sync','canonical',$3)")
            .bind(manifest).bind(CHAIN).bind(serde_json::json!({"rollout_status":"active","normalizer_version":"test","manifest_payload":payload}))
            .execute(database.pool()).await?;
        Ok(database)
    }

    #[tokio::test]
    async fn same_head_guards_reject_wrong_phase_markers_window_and_schema() -> Result<()> {
        assert!(!disable_jit_opt_in(None)?);
        assert!(disable_jit_opt_in(Some("1"))?);
        assert!(!linked_index_opt_in(None)?);
        assert!(linked_index_opt_in(Some("1"))?);
        for invalid in ["", "0", "true", "2"] {
            assert!(linked_index_opt_in(Some(invalid)).is_err());
            assert!(disable_jit_opt_in(Some(invalid)).is_err());
        }
        let database = fixture().await?;
        let mut tx = database.pool().begin().await?;
        sqlx::query("SET LOCAL search_path TO bigname_phase,public")
            .execute(&mut *tx)
            .await?;
        let target = Marker {
            number: HEAD,
            hash: format!("block-{HEAD}"),
        };
        guard(&mut tx, &target, FROM, HEAD).await?;
        guard(&mut tx, &target, BURST_FROM, HEAD).await?;
        for (number, from, to) in [
            (HEAD, FROM + 1, HEAD),
            (HEAD, BURST_FROM - 1, HEAD),
            (HEAD, BURST_FROM + 1, HEAD),
            (HEAD + 1, FROM, HEAD),
            (HEAD, FROM, HEAD - 1),
            (HEAD, HEAD, FROM),
            (HEAD, i64::MIN, i64::MAX),
        ] {
            assert!(bounds(number, from, to).is_err());
        }
        // Missing a height unique to the 75-block window must reject only that window.
        sqlx::query("SAVEPOINT burst_lineage")
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE block_number=$1")
            .bind(BURST_FROM)
            .execute(&mut *tx)
            .await?;
        guard(&mut tx, &target, FROM, HEAD).await?;
        assert!(guard(&mut tx, &target, BURST_FROM, HEAD).await.is_err());
        sqlx::query("ROLLBACK TO SAVEPOINT burst_lineage")
            .execute(&mut *tx)
            .await?;
        sqlx::query("RELEASE SAVEPOINT burst_lineage")
            .execute(&mut *tx)
            .await?;
        // Shadow phase rows only in this test transaction to exercise every rejection
        // independently of schema CHECK constraints that also prevent invalid states.
        sqlx::query(
            "CREATE TEMP TABLE chain_phase_state AS SELECT * FROM bigname_phase.chain_phase_state",
        )
        .execute(&mut *tx)
        .await?;
        for mutation in [
            "UPDATE chain_phase_state SET current_block_number=current_block_number-1 WHERE phase_name='project'",
            "UPDATE chain_phase_state SET current_block_hash='different' WHERE phase_name='interpret'",
            "UPDATE chain_phase_state SET phase_status='running' WHERE phase_name='project'",
            "UPDATE chain_phase_state SET redo_in_progress=true WHERE phase_name='interpret'",
            "UPDATE chain_phase_state SET last_error='fixture error' WHERE phase_name='project'",
            "UPDATE chain_phase_state SET finished_at=NULL WHERE phase_name='interpret'",
            "DELETE FROM chain_phase_state WHERE phase_name='project'",
            "UPDATE chain_lineage SET canonicality_state='orphaned' WHERE block_number=11756260",
            "ALTER TABLE name_current ADD COLUMN dangerous_identity bigint GENERATED ALWAYS AS IDENTITY",
            "CREATE FUNCTION bigname_phase.unreviewed_profile_trigger() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'; CREATE TRIGGER unreviewed_profile AFTER INSERT ON name_current FOR EACH ROW EXECUTE FUNCTION bigname_phase.unreviewed_profile_trigger()",
            "ALTER TABLE name_current ALTER COLUMN manifest_version SET DEFAULT nextval('normalized_events_normalized_event_id_seq')",
        ] {
            sqlx::query("SAVEPOINT rejected").execute(&mut *tx).await?;
            sqlx::raw_sql(mutation).execute(&mut *tx).await?;
            assert!(
                guard(&mut tx, &target, FROM, HEAD).await.is_err(),
                "accepted {mutation}"
            );
            sqlx::query("ROLLBACK TO SAVEPOINT rejected")
                .execute(&mut *tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT rejected")
                .execute(&mut *tx)
                .await?;
        }
        tx.rollback().await?;
        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn same_head_profile_writes_are_rolled_back_and_metadata_unchanged() -> Result<()> {
        let database = fixture().await?;
        let options = database
            .pool()
            .connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]);
        let before:(serde_json::Value,serde_json::Value)=sqlx::query_as(
            "SELECT (SELECT jsonb_agg(to_jsonb(n)) FROM bigname_phase.name_current n),
                    (SELECT jsonb_agg(to_jsonb(p) ORDER BY phase_name) FROM bigname_phase.chain_phase_state p)")
            .fetch_one(database.pool()).await?;
        let evidence =
            std::env::temp_dir().join(format!("same-head-profile-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&evidence)?;
        for from in [FROM, BURST_FROM] {
            let (rows, names) = run(
                options.clone(),
                &evidence,
                HEAD,
                from,
                HEAD,
                from == BURST_FROM,
                false,
            )
            .await?;
            ensure!(
                rows > 0 && names == 1,
                "fixture did not exercise nonempty publication"
            );
        }
        let candidate_index: bool = sqlx::query_scalar(
            "SELECT to_regclass('bigname_phase.normalized_events_linked_resolver_history_idx') IS NOT NULL",
        ).fetch_one(database.pool()).await?;
        ensure!(
            !candidate_index,
            "candidate diagnostic index survived rollback"
        );
        let after:(serde_json::Value,serde_json::Value)=sqlx::query_as(
            "SELECT (SELECT jsonb_agg(to_jsonb(n)) FROM bigname_phase.name_current n),
                    (SELECT jsonb_agg(to_jsonb(p) ORDER BY phase_name) FROM bigname_phase.chain_phase_state p)")
            .fetch_one(database.pool()).await?;
        ensure!(
            after == before,
            "same-head diagnostic persisted projection or metadata changes"
        );
        ensure!(
            std::fs::read_dir(&evidence)?.count() == 2,
            "profile evidence missing"
        );
        std::fs::remove_dir_all(evidence)?;
        database.cleanup().await?;
        Ok(())
    }
    #[tokio::test]
    async fn same_head_jit_setting_is_transaction_local() -> Result<()> {
        use sqlx::Acquire;
        let database = fixture().await?;
        let mut connection = database.pool().acquire().await?;
        let before: String = sqlx::query_scalar("SHOW jit")
            .fetch_one(&mut *connection)
            .await?;
        let mut tx = connection.begin().await?;
        sqlx::query("SET LOCAL jit=on").execute(&mut *tx).await?;
        disable_jit(&mut tx).await?;
        let during: String = sqlx::query_scalar("SHOW jit").fetch_one(&mut *tx).await?;
        assert_eq!(during, "off");
        tx.rollback().await?;
        let after: String = sqlx::query_scalar("SHOW jit")
            .fetch_one(&mut *connection)
            .await?;
        assert_eq!(after, before);
        drop(connection);
        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn same_head_profile_and_unprofiled_match_all_ten_nonempty_outputs() -> Result<()> {
        let database = fixture().await?;
        let mut tx = database.pool().begin().await?;
        sqlx::query("SET LOCAL search_path TO bigname_phase,public")
            .execute(&mut *tx)
            .await?;
        let target = Marker {
            number: HEAD,
            hash: format!("block-{HEAD}"),
        };
        guard(&mut tx, &target, FROM, HEAD).await?;
        let request = BatchRequest {
            chain_id: CHAIN.into(),
            target_block: HEAD,
            affected_from_block: FROM,
            affected_to_block: HEAD,
            resume_current: Some(target.clone()),
            mode: RunMode::Normal,
        };
        let evidence =
            std::env::temp_dir().join(format!("same-head-equality-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&evidence)?;
        sqlx::query("SAVEPOINT baseline").execute(&mut *tx).await?;
        super::super::super::derive(&mut tx, &request, &target).await?;
        ensure!(
            sqlx::query_scalar::<_, String>(
                "SELECT raw_name FROM name_current WHERE logical_name_id='ens:profile-name'"
            )
            .fetch_one(&mut *tx)
            .await?
                != "sentinel",
            "fixture projection did not change"
        );
        for table in ["permissions_current_resource_summary", "resolver_current"] {
            ensure!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&mut *tx)
                    .await?
                    > 0,
                "empty downstream fixture: {table}"
            );
        }
        let expected = super::super::reference_output::Snapshot::capture(
            &mut tx,
            super::super::TABLES,
            &evidence,
        )
        .await?;
        sqlx::query("ROLLBACK TO SAVEPOINT baseline")
            .execute(&mut *tx)
            .await?;
        disable_jit(&mut tx).await?;
        let session = crate::profile::Session::create(&evidence)?;
        session
            .scope(super::super::super::derive(&mut tx, &request, &target))
            .await?;
        expected.assert_equal(&mut tx, super::super::TABLES).await?;
        expected.cleanup()?;
        let names: Vec<String> = std::fs::read_dir(session.directory())?
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        for stage in [
            "project_events",
            "linked_records",
            "resource_permission_summary",
            "name_current",
            "resolver_current",
        ] {
            ensure!(
                names
                    .iter()
                    .filter(|name| name.contains(&format!("-{stage}-")))
                    .count()
                    == 1,
                "downstream stage was not profiled exactly once: {stage}"
            );
        }
        tx.rollback().await?;
        std::fs::remove_dir_all(evidence)?;
        database.cleanup().await?;
        Ok(())
    }
}
