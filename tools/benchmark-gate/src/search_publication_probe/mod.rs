//! Populate only the owned static scratch schema from one authoritative RR generation.
use anyhow::{Context, Result, ensure};
use bigname_storage::families::search_dictionary::shape::from_composed;
use clap::Parser;
use serde_json::{Value, json};
use sqlx::{
    Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{path::PathBuf, str::FromStr, time::Instant};

const SCRATCH_SCHEMA: &str = "tyr228_publication_http_20261005_r1";
const OWNER: &str = "tyr228-gate1-publication-20261005-r1";

#[derive(Parser)]
struct Args {
    #[arg(long)]
    expected_database: String,
    #[arg(long)]
    expected_publications: PathBuf,
    #[arg(long)]
    report: PathBuf,
    #[arg(long, required = true)]
    allow_owned_static_scratch: bool,
}

const PUBLICATIONS: &str = "SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'chain_id',chain_id,'head',current_block_number,'hash',current_block_hash,
    'input_hash',input_content_hash,'generation',sequence) ORDER BY chain_id),'[]'::jsonb)
    FROM bigname_phase.project_family_marker WHERE state='live'";

pub async fn run() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.allow_owned_static_scratch,
        "owned scratch acknowledgement missing"
    );
    let url = std::env::var("BIGNAME_DATABASE_URL").context("database URL required")?;
    let options = PgConnectOptions::from_str(&url)?.options([
        ("search_path", "bigname_phase"),
        ("statement_timeout", "25000"),
    ]);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    ensure!(database == args.expected_database, "unexpected database");
    let expected: Value = serde_json::from_slice(&std::fs::read(&args.expected_publications)?)?;
    let started = Instant::now();
    let mut stages = Vec::new();
    let mut transaction = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await?;
    let before: Value = sqlx::query_scalar(PUBLICATIONS)
        .fetch_one(&mut *transaction)
        .await?;
    ensure!(
        before == expected,
        "source publication changed before static generation"
    );
    ensure!(
        before.as_array().is_some_and(|rows| !rows.is_empty()),
        "no source publication"
    );
    for row in before.as_array().expect("validated array") {
        ensure!(
            row["input_hash"].as_str() == Some(bigname_content_hash::INTERPRETER_CONTENT_HASH),
            "source publication has a different compiled interpreter hash"
        );
    }
    let exists: bool = sqlx::query_scalar("SELECT to_regnamespace($1) IS NOT NULL")
        .bind(SCRATCH_SCHEMA)
        .fetch_one(&mut *transaction)
        .await?;
    ensure!(!exists, "owned static scratch schema already exists");
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {SCRATCH_SCHEMA}; COMMENT ON SCHEMA {SCRATCH_SCHEMA} IS '{OWNER}';"
    ))
    .execute(&mut *transaction)
    .await?;
    sqlx::raw_sql(include_str!("fields-schema.sql"))
        .execute(&mut *transaction)
        .await?;
    let identities=sqlx::query("SELECT surface.logical_name_id,surface.chain_id,surface.raw_name,
        summary.owner,summary.public_authority,summary.logical_name_id AS summary_name,
        (surface.block_number<=marker.current_block_number
            AND surface.visibility_state='active'
            AND surface.canonicality_state IN ('canonical','safe','finalized')
            AND EXISTS(SELECT 1 FROM bigname_phase.chain_lineage lineage
                WHERE lineage.chain_id=surface.chain_id AND lineage.block_hash=surface.block_hash
                AND lineage.block_number=surface.block_number
                AND lineage.canonicality_state IN ('canonical','safe','finalized'))) AS admitted
        FROM bigname_phase.name_surfaces surface
        LEFT JOIN bigname_phase.project_family_marker marker ON marker.chain_id=surface.chain_id AND marker.state='live'
        LEFT JOIN bigname_phase.project_name_summary summary ON summary.chain_id=surface.chain_id AND summary.logical_name_id=surface.logical_name_id
        ORDER BY surface.logical_name_id").fetch_all(&mut *transaction).await?;
    let mut supported = 0usize;
    let mut absent = 0usize;
    let mut unsupported = 0usize;
    let mut payload_bytes = Vec::new();
    let shape_start = Instant::now();
    for chunk in identities.chunks(200) {
        let ids: Vec<String> = chunk
            .iter()
            .filter(|row| row.try_get::<Option<bool>, _>("admitted").ok().flatten() == Some(true))
            .map(|row| row.try_get("logical_name_id"))
            .collect::<Result<_, _>>()?;
        let composed = bigname_storage::families::name::load_family_names_by_logical_name_ids(
            &mut *transaction,
            &ids,
        )
        .await?;
        let mut values = Vec::with_capacity(chunk.len());
        for identity in chunk {
            let id: String = identity.try_get("logical_name_id")?;
            let row = composed.get(&id);
            let raw_backed = identity.try_get::<Option<String>, _>("raw_name")?.is_some();
            let fields = from_composed(row, raw_backed)?;
            if identity.try_get::<Option<bool>, _>("admitted")? == Some(true) {
                ensure!(
                    identity
                        .try_get::<Option<String>, _>("summary_name")?
                        .is_some(),
                    "admitted identity is missing its Project summary: {id}"
                );
            }
            if fields.search_supported {
                ensure!(
                    fields.owner == identity.try_get::<Option<String>, _>("owner")?,
                    "canonical owner differs from Project summary: {id}"
                );
                ensure!(
                    fields.public_authority
                        == identity.try_get::<Option<String>, _>("public_authority")?,
                    "canonical authority differs from Project summary: {id}"
                );
                supported += 1;
                payload_bytes.push(
                    serde_json::to_vec(fields.search_fields.as_ref().expect("supported payload"))?
                        .len(),
                );
            } else if row.is_none() {
                absent += 1;
            } else {
                unsupported += 1;
            }
            let mut value = serde_json::to_value(fields)?;
            value["logical_name_id"] = json!(id);
            value["chain_id"] = json!(identity.try_get::<String, _>("chain_id")?);
            values.push(value);
        }
        sqlx::query(
            "INSERT INTO tyr228_publication_http_20261005_r1.search_fields
            SELECT * FROM jsonb_to_recordset($1) AS fields(logical_name_id text,chain_id text,
                search_supported boolean,owner text,public_authority text,search_fields jsonb,
                search_creation_transport_resource_id uuid,display_name_override text)",
        )
        .bind(json!(values))
        .execute(&mut *transaction)
        .await?;
    }
    stages.push(json!({"stage":"authoritative_full_composition_and_shared_shaping","seconds":shape_start.elapsed().as_secs_f64(),"identities":identities.len(),"supported":supported,"absent":absent,"unsupported":unsupported}));
    for (label, sql) in [
        ("create-spellings", include_str!("create-table.sql")),
        ("populate-spellings", include_str!("populate.sql")),
        ("index-spellings", include_str!("create-indexes.sql")),
        ("create-documents", include_str!("create-documents.sql")),
        ("populate-documents", include_str!("populate-documents.sql")),
        ("populate-postings", include_str!("populate-postings.sql")),
        ("index-postings", include_str!("index-postings.sql")),
    ] {
        let at = Instant::now();
        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
        stages.push(json!({"stage":label,"seconds":at.elapsed().as_secs_f64()}));
    }
    sqlx::raw_sql("ALTER TABLE tyr228_publication_http_20261005_r1.tyr228_documents ADD FOREIGN KEY(logical_name_id) REFERENCES tyr228_publication_http_20261005_r1.search_fields(logical_name_id);
        ALTER TABLE tyr228_publication_http_20261005_r1.tyr228_spelling_probe ADD FOREIGN KEY(logical_name_id) REFERENCES tyr228_publication_http_20261005_r1.search_fields(logical_name_id);
        INSERT INTO tyr228_publication_http_20261005_r1.generation
        SELECT DISTINCT marker.chain_id,surface.namespace,marker.current_block_number,marker.current_block_hash,marker.input_content_hash
        FROM bigname_phase.project_family_marker marker JOIN bigname_phase.name_surfaces surface USING(chain_id)
        WHERE marker.state='live';
        ANALYZE tyr228_publication_http_20261005_r1.search_fields;
        ANALYZE tyr228_publication_http_20261005_r1.generation;")
        .execute(&mut *transaction).await?;
    let mut checks = Vec::new();
    for (label, sql) in [
        ("spellings", include_str!("verify-spellings.sql")),
        ("documents", include_str!("verify-documents.sql")),
        ("postings", include_str!("verify-postings.sql")),
    ] {
        let value: Value = sqlx::query_scalar(sql).fetch_one(&mut *transaction).await?;
        ensure!(
            value["missing_or_extra"].as_i64() == Some(0),
            "static lexical parity failed: {label}"
        );
        checks.push(json!({"check":label,"value":value}));
    }
    // Round-trip the actual stored JSONB, including present null expiry/grace and ENSv1 expiry.
    let stored_payloads: Vec<Value> = sqlx::query_scalar("SELECT search_fields FROM tyr228_publication_http_20261005_r1.search_fields WHERE search_supported").fetch_all(&mut *transaction).await?;
    ensure!(
        stored_payloads.len() == supported,
        "stored payload count differs from supported count"
    );
    for payload in &stored_payloads {
        let typed: bigname_storage::public_name_fields::SearchFields =
            serde_json::from_value(payload.clone())?;
        ensure!(
            serde_json::to_value(typed)? == *payload,
            "stored search fields changed JSON presence or exact values"
        );
    }
    let after: Value = sqlx::query_scalar(PUBLICATIONS)
        .fetch_one(&mut *transaction)
        .await?;
    ensure!(
        before == after,
        "source publication changed during static generation"
    );
    let sizes: Value = sqlx::query_scalar(
        "SELECT jsonb_object_agg(relname,pg_total_relation_size(c.oid))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='tyr228_publication_http_20261005_r1' AND c.relkind='r'",
    )
    .fetch_one(&mut *transaction)
    .await?;
    let total: i64 = sizes
        .as_object()
        .context("table size object")?
        .values()
        .map(|value| value.as_i64().unwrap_or(i64::MAX))
        .sum();
    ensure!(
        total <= 512 * 1024 * 1024,
        "combined static scratch exceeds 512 MiB"
    );
    transaction.commit().await?;
    let report = json!({"completed":true,"database":database,"owner":OWNER,"schema":SCRATCH_SCHEMA,
        "interpreter_content_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH,"source_publications":before,
        "stages":stages,"checks":checks,"table_sizes":sizes,"combined_relation_bytes":total,
        "stored_payload_roundtrips":stored_payloads.len(),"payload_bytes":payload_bytes,"seconds":started.elapsed().as_secs_f64(),
        "authority_composition":"load_family_names_by_logical_name_ids / full shared composer","source_rows_unchanged":true});
    std::fs::write(&args.report, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "{}",
        json!({"completed":true,"identities":identities.len(),"supported":supported,"seconds":started.elapsed().as_secs_f64(),"combined_relation_bytes":total})
    );
    pool.close().await;
    Ok(())
}
