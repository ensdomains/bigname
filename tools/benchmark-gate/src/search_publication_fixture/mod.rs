//! Disposable functional fixture. Inputs follow apps/api/src/tests/support.rs and
//! v2_name_record.rs; Project creates every serving row. This is not a raw Interpret fixture.
mod inputs;
mod pointer_matrix;
mod seed;

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{path::PathBuf, str::FromStr};

const OWNER: &str = "tyr228-gate1-functional-20261005-r1";
pub const CLOCK: i64 = 1_700_000_100;
pub const HEAD: i64 = 100;
pub const ETH: &str = "ethereum-mainnet";
pub const BASE: &str = "base-mainnet";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Step {
    Initialize,
    L1Context,
    L1Advance,
    BaseAdvance,
    PointerMatrix,
}
#[derive(Parser)]
struct Args {
    #[arg(long)]
    expected_database: String,
    #[arg(long)]
    report: PathBuf,
    #[arg(long, value_enum)]
    step: Step,
    #[arg(long, required = true)]
    allow_owned_functional_fixture: bool,
}

pub async fn run() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.allow_owned_functional_fixture,
        "owned fixture acknowledgement missing"
    );
    ensure!(
        [
            "bigname_api_test_tyr228_gate1_",
            "bigname_api_test_tyr228_gate2_"
        ]
        .iter()
        .any(|prefix| args.expected_database.starts_with(prefix)),
        "fixture database prefix required"
    );
    let url = std::env::var("BIGNAME_DATABASE_URL").context("database URL required")?;
    let options = PgConnectOptions::from_str(&url)?.options([
        ("search_path", "bigname_phase,public"),
        ("statement_timeout", "25000"),
    ]);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    ensure!(
        database == args.expected_database,
        "unexpected fixture database"
    );
    let started = std::time::Instant::now();
    let mut pointer_matrix = serde_json::Value::Null;
    match args.step {
        Step::Initialize => {
            let exists: bool =
                sqlx::query_scalar("SELECT to_regnamespace('bigname_phase') IS NOT NULL")
                    .fetch_one(&pool)
                    .await?;
            ensure!(!exists, "fixture database is not empty");
            initialize(&pool).await?;
            seed::initial(&pool).await?;
            publish(&pool, ETH, 90, CLOCK - 20).await?;
            publish(&pool, BASE, HEAD, CLOCK).await?;
        }
        step => {
            let owner: String = sqlx::query_scalar("SELECT owner FROM bigname_gate1_fixture.owner")
                .fetch_one(&pool)
                .await?;
            ensure!(owner == OWNER, "owned fixture marker mismatch");
            match step {
                Step::L1Context => seed::execution_context(&pool).await?,
                Step::L1Advance => inputs::lineage(&pool, ETH, 91, CLOCK - 5).await?,
                Step::BaseAdvance => publish(&pool, BASE, HEAD + 1, CLOCK + 20).await?,
                Step::PointerMatrix => {
                    pointer_matrix = pointer_matrix::run(&pool).await?;
                }
                Step::Initialize => unreachable!(),
            }
        }
    }
    let publications: serde_json::Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'chain_id',chain_id,'head',current_block_number,'hash',current_block_hash,
        'input_hash',input_content_hash,'generation',sequence) ORDER BY chain_id),'[]'::jsonb)
        FROM project_family_marker WHERE state='live'",
    )
    .fetch_one(&pool)
    .await?;
    let identities: Vec<(String, bool)> = sqlx::query_as(
        "SELECT logical_name_id,raw_name IS NOT NULL FROM name_surfaces ORDER BY logical_name_id",
    )
    .fetch_all(&pool)
    .await?;
    let ids: Vec<String> = identities.iter().map(|(id, _)| id.clone()).collect();
    let rows =
        bigname_storage::families::name::load_family_names_by_logical_name_ids(&pool, &ids).await?;
    let observations: Vec<_> = identities.iter().map(|(id,raw_backed)| {
        let row=rows.get(id);
        Ok(json!({"id":id,"row_present":row.is_some(),"row":row.map(|r|json!({"name":r.normalized_name,"display_name":r.canonical_display_name,"summary":r.declared_summary,"provenance":r.provenance,"coverage":r.coverage,"chain_positions":r.chain_positions})),
            "static_fields":bigname_storage::families::search_dictionary::shape::from_composed(row, *raw_backed)?}))
    }).collect::<Result<_>>()?;
    std::fs::write(
        &args.report,
        serde_json::to_vec_pretty(&json!({"database":database,
        "step":format!("{:?}",args.step),"seconds":started.elapsed().as_secs_f64(),
        "provenance":"normal identity and normalized-event fixture inputs followed by real Project; not raw Interpret",
        "publications":publications,"observations":observations,"pointer_matrix":pointer_matrix}))?,
    )?;
    pool.close().await;
    Ok(())
}

async fn initialize(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase,public;")
        .execute(&mut *tx)
        .await?;
    for sql in [
        include_str!("../../../../crates/storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../crates/storage/schema/baseline/02_raw_facts.sql"),
        include_str!("../../../../crates/storage/schema/baseline/03_identity.sql"),
        include_str!("../../../../crates/storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../../crates/storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../../../crates/storage/schema/baseline/06_projections.sql"),
        include_str!("../../../../crates/storage/schema/baseline/07_labels.sql"),
        include_str!("../../../../crates/storage/schema/baseline/08_heartbeats.sql"),
        include_str!("../../../../crates/storage/schema/baseline/09_divergence.sql"),
        include_str!("../../../../crates/storage/schema/baseline/10_phase_state.sql"),
        include_str!(
            "../../../../crates/storage/schema/baseline/11_manifest_authority_attestations.sql"
        ),
        include_str!(
            "../../../../crates/storage/schema/baseline/12_project_generation_failures.sql"
        ),
        include_str!("../../../../crates/storage/schema/baseline/13_interpret_decode_skips.sql"),
        include_str!(
            "../../../../crates/storage/schema/baseline/14_discovery_watch_admissions.sql"
        ),
    ] {
        sqlx::raw_sql(sql).execute(&mut *tx).await?;
    }
    sqlx::raw_sql("CREATE SCHEMA bigname_gate1_fixture; CREATE TABLE bigname_gate1_fixture.owner(owner text PRIMARY KEY)").execute(&mut *tx).await?;
    sqlx::query("INSERT INTO bigname_gate1_fixture.owner VALUES($1)")
        .bind(OWNER)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn publish(pool: &PgPool, chain: &str, block: i64, clock: i64) -> Result<()> {
    inputs::lineage(pool, chain, block, clock).await?;
    let hash = inputs::hash(chain, block);
    sqlx::query("INSERT INTO chain_heads(chain_id,latest_block_hash,latest_block_number,safe_block_hash,safe_block_number,finalized_block_hash,finalized_block_number)
        VALUES($1,$2,$3,$2,$3,$2,$3) ON CONFLICT(chain_id) DO UPDATE SET latest_block_hash=$2,latest_block_number=$3,safe_block_hash=$2,safe_block_number=$3,finalized_block_hash=$2,finalized_block_number=$3,updated_at=now()")
        .bind(chain).bind(&hash).bind(block).execute(pool).await?;
    // The existing normal-input fixture admission state; the family marker below is written only by Project.
    sqlx::query("INSERT INTO chain_phase_state(chain_id,phase_name,phase_status,current_block_number,current_block_hash,target_block_number,target_block_hash,input_content_hash,started_at,finished_at)
        SELECT $1,phase,'completed',$2,$3,$2,$3,$4,now(),now() FROM unnest(ARRAY['interpret','project']) phase
        ON CONFLICT(chain_id,phase_name) DO UPDATE SET phase_status='completed',current_block_number=$2,current_block_hash=$3,target_block_number=$2,target_block_hash=$3,input_content_hash=$4,finished_at=now(),updated_at=now()")
        .bind(chain).bind(block).bind(&hash).bind(bigname_content_hash::INTERPRETER_CONTENT_HASH).execute(pool).await?;
    let token = bigname_project::families::input_token(pool, chain).await?;
    let outcome = bigname_project::families::apply(
        pool,
        chain,
        &bigname_project::Marker {
            number: block,
            hash,
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    ensure!(
        outcome.marker.as_ref().map(|m| m.number) == Some(block),
        "fixture Project failed to publish: {outcome:?}"
    );
    Ok(())
}
