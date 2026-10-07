//! Redo keeps lexical work at its mutation boundaries, while ordinary writes remain atomic.
use super::*;
use std::{
    collections::BTreeSet,
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Default)]
struct SqlLog(Arc<Mutex<Vec<u8>>>);
impl Write for SqlLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl SqlLog {
    fn take(&self) -> String {
        String::from_utf8(std::mem::take(&mut *self.0.lock().unwrap())).unwrap()
    }
}

async fn fixture(prefix: &str, blocks: i64) -> TestResult<TestDatabase> {
    let db = database(prefix).await?;
    sync_schema_v2_repository(
        db.pool(),
        &load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
        )?,
    )
    .await?;
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
        SELECT $1,'0x'||lpad(to_hex(n+1),64,'0'),n,to_timestamp(n),'canonical'
        FROM generate_series($2::bigint,$3::bigint) n")
        .bind(CHAIN).bind(SETUP_BLOCK).bind(SETUP_BLOCK+blocks-1).execute(db.pool()).await?;
    Ok(db)
}

async fn wrapped(pool: &PgPool, block: i64, index: i64, labels: &[&str]) -> TestResult<String> {
    let mut node = B256::ZERO;
    for label in labels.iter().rev() {
        node = keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat());
    }
    let mut dns = Vec::new();
    for label in labels {
        dns.push(label.len().try_into()?);
        dns.extend_from_slice(label.as_bytes());
    }
    dns.push(0);
    insert_log(
        pool,
        block,
        index,
        NAME_WRAPPER,
        NameWrapped {
            node,
            name: dns.into(),
            owner: OWNER.parse()?,
            fuses: 0,
            expiry: 2_000_000_000,
        }
        .encode_log_data(),
    )
    .await?;
    Ok(format!("ens:{node:#x}"))
}

fn request(from: i64, to: i64, resume: Option<Marker>, mode: RunMode) -> BatchRequest {
    BatchRequest {
        chain_id: CHAIN.into(),
        from_block: from,
        to_block: to,
        resume_current: resume,
        mode,
    }
}

async fn corpus(count: usize) -> TestResult<(TestDatabase, String)> {
    let db = fixture("search_redo_corpus", 2500).await?;
    insert_transaction(db.pool(), SETUP_BLOCK, NAME_WRAPPER).await?;
    let mut first = String::new();
    for n in 0..count {
        let id = wrapped(
            db.pool(),
            SETUP_BLOCK,
            n as i64,
            &[&format!("redo{n:05}"), "eth"],
        )
        .await?;
        if n == 0 {
            first = id;
        }
    }
    Engine::new(db.pool().clone())
        .run_batch(request(
            SETUP_BLOCK,
            SETUP_BLOCK + 499,
            None,
            RunMode::Normal,
        ))
        .await?;
    let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM name_search_documents")
        .fetch_one(db.pool())
        .await?;
    assert_eq!(actual, count as i64);
    Ok((db, first))
}

/// Observe the actual waiter, then release it before failing, so baseline failure is an
/// intended broad-search lock assertion and never a leaked transaction or timeout artifact.
#[tokio::test]
async fn empty_middle_does_not_lock_retained_search_sources() -> TestResult {
    let (db, id) = corpus(128).await?;
    let engine = Engine::new(db.pool().clone());
    let first = engine
        .run_batch(request(
            SETUP_BLOCK,
            SETUP_BLOCK + 2499,
            None,
            RunMode::Redo,
        ))
        .await?;
    let mut lock = db.pool().begin().await?;
    let holder: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await?;
    sqlx::query("SELECT logical_name_id FROM name_surfaces WHERE logical_name_id=$1 FOR UPDATE")
        .bind(id)
        .execute(&mut *lock)
        .await?;
    let resumed = Engine::new(db.pool().clone());
    let job = tokio::spawn(async move {
        resumed
            .run_batch(request(
                SETUP_BLOCK,
                SETUP_BLOCK + 2499,
                Some(first.current),
                RunMode::Redo,
            ))
            .await
    });
    let until = Instant::now() + Duration::from_secs(15);
    let mut blocked = false;
    while !job.is_finished() && Instant::now() < until {
        blocked=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%storage:identity_search.name_locks%')")
            .bind(holder).fetch_one(db.pool()).await?;
        if blocked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let completed_while_locked = job.is_finished();
    lock.rollback().await?;
    let outcome = tokio::time::timeout(Duration::from_secs(15), job)
        .await???
        .current;
    assert_eq!(outcome.number, SETUP_BLOCK + 999);
    db.cleanup().await?;
    assert!(
        blocked || completed_while_locked,
        "batch reached neither completion nor the observed lock within the bounded observation window"
    );
    assert!(
        !blocked,
        "empty middle batch entered broad retained-name search row locks"
    );
    Ok(())
}

/// A finite operator measurement; always creates its own database and uses the same 500
/// canonical blocks per batch as the runner. SQLx records the actual query/returned-row work.
#[tokio::test]
#[ignore = "matched local work/timing experiment; SEARCH_REDO_COUNT chooses inventory"]
async fn matched_search_redo_work() -> TestResult {
    let count = std::env::var("SEARCH_REDO_COUNT")
        .unwrap_or_else(|_| "1000".into())
        .parse()?;
    let (db, _) = corpus(count).await?;
    // Freeze a representative planner-statistics state on this owned fixture only.
    sqlx::query("ANALYZE").execute(db.pool()).await?;
    let log = SqlLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer({
            let log = log.clone();
            move || log.clone()
        })
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let settings:Vec<(String,String)>=sqlx::query_as("SELECT name,setting FROM pg_settings WHERE name IN ('shared_buffers','work_mem','effective_cache_size','effective_io_concurrency','max_parallel_workers_per_gather','jit','synchronous_commit') ORDER BY name").fetch_all(db.pool()).await?;
    println!("SETTINGS {}", serde_json::to_string(&settings)?);
    println!(
        "FIXTURE {}",
        serde_json::json!({"analyzed_before_redo":true,"batch_size":500,"range":[SETUP_BLOCK, SETUP_BLOCK+2499],"raw_logs":count,"surfaces":count,"compiled_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH})
    );
    let engine = Engine::new(db.pool().clone());
    let mut marker = None;
    for batch in 0..5 {
        log.take();
        let start = Instant::now();
        let out = engine
            .run_batch(request(
                SETUP_BLOCK,
                SETUP_BLOCK + 2499,
                marker,
                RunMode::Redo,
            ))
            .await?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let queries = log.take();
        let work: Vec<_> = queries
            .lines()
            .filter(|line| {
                line.contains("identity_search.redo_names")
                    || line.contains("identity_search.spellings")
            })
            .collect();
        println!(
            "SAMPLE {}",
            serde_json::json!({"count":count,"batch":batch,"milliseconds":elapsed,"complete":out.complete,"work":work})
        );
        marker = Some(out.current);
    }
    db.cleanup().await?;
    Ok(())
}

async fn lexical(pool: &PgPool, id: &str) -> TestResult<(String, i16, BTreeSet<(i16, String)>)> {
    let (name, class): (String, i16) = sqlx::query_as(
        "SELECT name,spelling_class FROM name_search_documents WHERE logical_name_id=$1",
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    let tokens:Vec<(i16,String)>=sqlx::query_as("SELECT p.token_kind,convert_from(p.token_bytes,'UTF8') FROM name_search_postings p JOIN name_search_documents d USING(search_id) WHERE d.logical_name_id=$1").bind(id).fetch_all(pool).await?;
    Ok((name, class, tokens.into_iter().collect()))
}

async fn new_owner(
    pool: &PgPool,
    block: i64,
    index: i64,
    parent: B256,
    label: &str,
) -> TestResult<B256> {
    let hash = keccak256(label.as_bytes());
    insert_log(
        pool,
        block,
        index,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: parent,
            label: hash,
            owner: OWNER.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    Ok(keccak256([parent.as_slice(), hash.as_slice()].concat()))
}

struct Transition {
    db: TestDatabase,
    lost: String,
    keeper: String,
    unreadable: String,
    fanout: String,
}
async fn transition() -> TestResult<Transition> {
    let db = fixture("search_redo_transition", 8).await?;
    let pool = db.pool();
    for offset in [0, 1, 2, 4, 7] {
        insert_transaction(pool, SETUP_BLOCK + offset, NAME_WRAPPER).await?;
    }
    let eth = new_owner(pool, SETUP_BLOCK, 0, B256::ZERO, "eth").await?;
    let beta = new_owner(pool, SETUP_BLOCK, 1, eth, "beta").await?;
    let lost = new_owner(pool, SETUP_BLOCK, 2, eth, "lost").await?;
    let fanout = new_owner(pool, SETUP_BLOCK, 3, beta, "fresh").await?;
    let unreadable = new_owner(pool, SETUP_BLOCK, 4, eth, "unreadable").await?;
    wrapped(pool, SETUP_BLOCK + 1, 0, &["unreadable", "eth"]).await?;
    wrapped(pool, SETUP_BLOCK + 2, 0, &["lost", "eth"]).await?;
    let keeper = wrapped(pool, SETUP_BLOCK + 2, 1, &["keeper", "eth"]).await?;
    wrapped(pool, SETUP_BLOCK + 7, 0, &["keeper", "eth"]).await?;
    Engine::new(pool.clone())
        .run_batch(request(SETUP_BLOCK, SETUP_BLOCK + 7, None, RunMode::Normal))
        .await?;
    // Retain raw facts, orphan their old block and admit an empty replacement block.
    for offset in [1, 2] {
        sqlx::query("UPDATE chain_lineage SET canonicality_state='orphaned' WHERE block_number=$1")
            .bind(SETUP_BLOCK + offset)
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,'replacement-' || $2, $2,to_timestamp($2),'canonical')")
        .bind(CHAIN).bind(SETUP_BLOCK+offset).execute(pool).await?;
    }
    // This preimage did not exist in the original pass. The actual adapter must refresh
    // fresh.beta.eth through label fanout although the event emits only fresh.eth.
    wrapped(pool, SETUP_BLOCK + 4, 0, &["fresh", "eth"]).await?;
    Ok(Transition {
        db,
        lost: format!("ens:{lost:#x}"),
        keeper,
        unreadable: format!("ens:{unreadable:#x}"),
        fanout: format!("ens:{fanout:#x}"),
    })
}

async fn snapshot(pool: &PgPool) -> TestResult<serde_json::Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object(
        'surfaces',(SELECT jsonb_agg(jsonb_build_array(logical_name_id,raw_name,raw_labels,preimage_event_identity,block_hash,block_number,canonicality_state,visibility_state) ORDER BY logical_name_id) FROM name_surfaces),
        'documents',(SELECT jsonb_agg(jsonb_build_array(logical_name_id,name,spelling_class,display_name_override) ORDER BY logical_name_id) FROM name_search_documents),
        'postings',(SELECT jsonb_agg(jsonb_build_array(d.logical_name_id,p.spelling_class,p.token_kind,p.token_length,encode(p.token_bytes,'hex')) ORDER BY d.logical_name_id,p.spelling_class,p.token_kind,p.token_length,p.token_bytes) FROM name_search_postings p JOIN name_search_documents d USING(search_id)))")
        .fetch_one(pool).await?)
}

async fn assert_spelling(pool: &PgPool, id: &str, name: &str, class: i16) -> TestResult {
    let (actual, actual_class, tokens) = lexical(pool, id).await?;
    assert_eq!((actual.as_str(), actual_class), (name, class));
    // Independent ASCII contract oracle: enumerate all substrings and select lengths 1..3,
    // then add the three anchored prefixes. Do not call the production token generator.
    assert!(name.is_ascii());
    let mut expected = BTreeSet::new();
    for start in 0..name.len() {
        for end in start + 1..=name.len() {
            if end - start <= 3 {
                expected.insert((1, name[start..end].to_owned()));
            }
        }
    }
    for end in 1..=3.min(name.len()) {
        expected.insert((2, name[..end].to_owned()));
    }
    assert_eq!(tokens, expected);
    let wrong_class:i64=sqlx::query_scalar("SELECT count(*) FROM name_search_postings p JOIN name_search_documents d USING(search_id) WHERE d.logical_name_id=$1 AND p.spelling_class<>d.spelling_class")
        .bind(id).fetch_one(pool).await?;
    assert_eq!(wrong_class, 0);
    Ok(())
}

#[tokio::test]
async fn resumed_middle_refreshes_label_fanout_and_empty_final_repairs_witnesses() -> TestResult {
    let fixture = transition().await?;
    let pool = fixture.db.pool();
    let from = SETUP_BLOCK + 2;
    let to = SETUP_BLOCK + 6;
    let before = lexical(pool, &fixture.fanout).await?.0;
    assert!(
        before.contains('['),
        "fresh label must initially be unknown"
    );
    let engine = || Engine::new(pool.clone()).with_blocks_per_batch(NonZeroU32::new(1).unwrap());
    let first = engine()
        .run_batch(request(from, to, None, RunMode::Redo))
        .await?;
    // Repeat a committed first boundary with its pre-commit checkpoint.
    engine()
        .run_batch(request(from, to, None, RunMode::Redo))
        .await?;
    let mut marker = first.current;
    while marker.number < to - 1 {
        let out = engine()
            .run_batch(request(from, to, Some(marker), RunMode::Redo))
            .await?;
        marker = out.current;
        if marker.number == SETUP_BLOCK + 4 {
            assert!(!out.complete);
            let (name, class, _) = lexical(pool, &fixture.fanout).await?;
            assert!(
                name.starts_with("fresh."),
                "changed-label fanout is visible before completion: {name}"
            );
            assert_eq!(class, 1);
            assert_spelling(
                pool,
                &fixture.fanout,
                &format!("fresh.[{:x}].eth", keccak256(b"beta")),
                1,
            )
            .await?;
            // No raw observation for this name was produced by the middle event.
            let raw: Option<String> =
                sqlx::query_scalar("SELECT raw_name FROM name_surfaces WHERE logical_name_id=$1")
                    .bind(&fixture.fanout)
                    .fetch_one(pool)
                    .await?;
            assert_eq!(raw, None);
        }
    }
    for _ in 0..2 {
        let out = engine()
            .run_batch(request(from, to, Some(marker.clone()), RunMode::Redo))
            .await?;
        assert!(out.complete);
    }
    assert_spelling(pool, &fixture.lost, "lost.eth", 1).await?;
    assert_spelling(pool, &fixture.unreadable, "unreadable.eth", 1).await?;
    assert_spelling(pool, &fixture.keeper, "keeper.eth", 0).await?;
    let source: (Option<String>, i64) =
        sqlx::query_as("SELECT raw_name,block_number FROM name_surfaces WHERE logical_name_id=$1")
            .bind(&fixture.lost)
            .fetch_one(pool)
            .await?;
    assert_eq!(source, (None, SETUP_BLOCK));
    let anchor: i64 =
        sqlx::query_scalar("SELECT block_number FROM name_surfaces WHERE logical_name_id=$1")
            .bind(&fixture.keeper)
            .fetch_one(pool)
            .await?;
    assert_eq!(anchor, SETUP_BLOCK + 7);
    let expected = transition().await?;
    Engine::new(expected.db.pool().clone())
        .run_batch(request(from, to, None, RunMode::Redo))
        .await?;
    assert_eq!(snapshot(pool).await?, snapshot(expected.db.pool()).await?);
    expected.db.cleanup().await?;
    fixture.db.cleanup().await?;
    Ok(())
}
