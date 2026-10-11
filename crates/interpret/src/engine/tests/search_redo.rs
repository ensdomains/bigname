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

/// Runs `job` while `lock` holds rows and reports whether the job waited on `lock` at the
/// statement tagged `tag`. The lock is released before the job is joined, so a failed
/// assertion is never a leaked transaction or a timeout.
async fn waited_on<T: Send + 'static>(
    pool: &PgPool,
    mut lock: sqlx::Transaction<'static, sqlx::Postgres>,
    tag: &str,
    job: tokio::task::JoinHandle<T>,
) -> TestResult<(bool, T)> {
    let holder: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await?;
    let until = Instant::now() + Duration::from_secs(15);
    let mut blocked = false;
    while !job.is_finished() && !blocked && Instant::now() < until {
        blocked = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE $2)")
            .bind(holder).bind(format!("%{tag}%")).fetch_one(pool).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let finished = job.is_finished();
    lock.rollback().await?;
    let output = tokio::time::timeout(Duration::from_secs(30), job).await??;
    assert!(
        blocked || finished,
        "job reached neither completion nor the observed lock within the observation window"
    );
    Ok((blocked, output))
}

async fn lock_rows(
    pool: &PgPool,
    statement: &str,
    key: &str,
) -> TestResult<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut lock = pool.begin().await?;
    sqlx::query(statement).bind(key).execute(&mut *lock).await?;
    Ok(lock)
}

const LOCK_SURFACE: &str = "SELECT 1 FROM name_surfaces WHERE logical_name_id=$1 FOR NO KEY UPDATE";

struct Labels {
    db: TestDatabase,
    marker: Marker,
    /// `[x].eth`: unnamed, and carries only the stored label `eth` besides its own.
    x: String,
    /// `[fresh].[beta].eth`: unnamed, and carries the absent label `fresh`.
    fanout: String,
}

/// One Normal batch stores `eth` through `seed.eth` and leaves `x`, `beta` and `fresh` absent.
async fn labels(prefix: &str) -> TestResult<Labels> {
    let db = fixture(prefix, 8).await?;
    let pool = db.pool();
    for offset in 0..8 {
        insert_transaction(pool, SETUP_BLOCK + offset, NAME_WRAPPER).await?;
    }
    let eth = new_owner(pool, SETUP_BLOCK, 0, B256::ZERO, "eth").await?;
    let x = new_owner(pool, SETUP_BLOCK, 1, eth, "x").await?;
    let beta = new_owner(pool, SETUP_BLOCK, 2, eth, "beta").await?;
    let fanout = new_owner(pool, SETUP_BLOCK, 3, beta, "fresh").await?;
    wrapped(pool, SETUP_BLOCK + 1, 0, &["seed", "eth"]).await?;
    let marker = Engine::new(pool.clone())
        .run_batch(request(SETUP_BLOCK, SETUP_BLOCK + 1, None, RunMode::Normal))
        .await?
        .current;
    Ok(Labels {
        db,
        marker,
        x: format!("ens:{x:#x}"),
        fanout: format!("ens:{fanout:#x}"),
    })
}

fn next_batch(
    pool: &PgPool,
    block: i64,
    marker: Marker,
) -> tokio::task::JoinHandle<crate::Result<BatchOutcome>> {
    let engine = Engine::new(pool.clone());
    tokio::spawn(async move {
        engine
            .run_batch(request(block, block, Some(marker), RunMode::Normal))
            .await
    })
}

#[tokio::test]
async fn known_label_batch_does_not_lock_unnamed_surfaces() -> TestResult {
    let fixture = labels("search_known_label_locks").await?;
    let pool = fixture.db.pool();
    let block = SETUP_BLOCK + 2;
    let foo = wrapped(pool, block, 0, &["foo", "eth"]).await?;
    let lock = lock_rows(pool, LOCK_SURFACE, &fixture.x).await?;
    let job = next_batch(pool, block, fixture.marker.clone());
    let (blocked, outcome) =
        waited_on(pool, lock, "storage:identity_search.name_locks", job).await?;
    assert_eq!(outcome?.current.number, block);
    assert_spelling(pool, &foo, "foo.eth", 0).await?;
    assert_spelling(pool, &fixture.x, &format!("[{:x}].eth", keccak256(b"x")), 1).await?;
    fixture.db.cleanup().await?;
    assert!(
        !blocked,
        "a batch whose only stored label is eth locked an unnamed eth surface"
    );
    Ok(())
}

#[tokio::test]
async fn absent_label_batch_locks_and_respells_fanout() -> TestResult {
    let fixture = labels("search_absent_label_locks").await?;
    let pool = fixture.db.pool();
    let block = SETUP_BLOCK + 2;
    wrapped(pool, block, 0, &["fresh", "eth"]).await?;
    let lock = lock_rows(pool, LOCK_SURFACE, &fixture.fanout).await?;
    let job = next_batch(pool, block, fixture.marker.clone());
    let (blocked, outcome) =
        waited_on(pool, lock, "storage:identity_search.name_locks", job).await?;
    assert_eq!(outcome?.current.number, block);
    assert!(
        blocked,
        "a batch that stores a new label must lock the unnamed surfaces carrying it"
    );
    assert_spelling(
        pool,
        &fixture.fanout,
        &format!("fresh.[{:x}].eth", keccak256(b"beta")),
        1,
    )
    .await?;
    fixture.db.cleanup().await?;
    Ok(())
}

/// A label whose lock bucket differs from every given label's bucket.
async fn label_outside_buckets(pool: &PgPool, labels: &[&str]) -> TestResult<String> {
    let hash = |label: &str| format!("{:#x}", keccak256(label.as_bytes()));
    let taken: Vec<String> = labels.iter().map(|label| hash(label)).collect();
    for n in 0..300 {
        let label = format!("y{n}");
        let separate: bool = sqlx::query_scalar(
            "SELECT NOT EXISTS (SELECT 1 FROM unnest($2::text[]) AS taken(label)
             WHERE hashtext(taken.label) & 255 = hashtext($1) & 255)",
        )
        .bind(hash(&label))
        .bind(&taken)
        .fetch_one(pool)
        .await?;
        if separate {
            return Ok(label);
        }
    }
    Err("no label outside the given buckets".into())
}

/// The batch holds `eth` (stored) and `foo` (absent) while an import of an absent `y` respells
/// `[y].eth`. Neither waits for the other, because the batch cannot respell `[y].eth`.
#[tokio::test]
async fn known_label_batch_does_not_block_absent_label_import() -> TestResult {
    let fixture = labels("search_known_label_import").await?;
    let pool = fixture.db.pool();
    let y = label_outside_buckets(pool, &["eth", "foo"]).await?;
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    let y_eth = new_owner(pool, SETUP_BLOCK + 2, 0, eth, &y).await?;
    let marker = Engine::new(pool.clone())
        .run_batch(request(
            SETUP_BLOCK + 2,
            SETUP_BLOCK + 2,
            Some(fixture.marker.clone()),
            RunMode::Normal,
        ))
        .await?
        .current;
    let y_eth = format!("ens:{y_eth:#x}");
    sqlx::query("INSERT INTO ens_names VALUES ($1,$2)")
        .bind(format!("{:#x}", keccak256(y.as_bytes())))
        .bind(&y)
        .execute(pool)
        .await?;
    // Hold the batch after its search locks, at its upsert of the stored eth preimage.
    let lock = lock_rows(
        pool,
        "SELECT 1 FROM label_preimages WHERE labelhash=$1 FOR UPDATE",
        &format!("{:#x}", keccak256(b"eth")),
    )
    .await?;
    let wrapped_foo = wrapped(pool, SETUP_BLOCK + 3, 0, &["foo", "eth"]).await?;
    let batch = next_batch(pool, SETUP_BLOCK + 3, marker);
    let until = Instant::now() + Duration::from_secs(15);
    let mut held = false;
    while !batch.is_finished() && !held && Instant::now() < until {
        held = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%INSERT INTO label_preimages%')")
            .fetch_one(pool).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut importer = {
        let pool = pool.clone();
        tokio::spawn(async move {
            bigname_storage::import_label_preimages_from_ens_names_table(&pool, None, None).await
        })
    };
    let early = tokio::time::timeout(Duration::from_secs(10), &mut importer).await;
    lock.rollback().await?;
    let outcome = tokio::time::timeout(Duration::from_secs(30), batch).await??;
    let waited = early.is_err();
    let imported = match early {
        Ok(joined) => joined??,
        Err(_) => tokio::time::timeout(Duration::from_secs(30), importer).await???,
    };
    assert!(held, "the batch never reached the held preimage upsert");
    assert_eq!(outcome?.current.number, SETUP_BLOCK + 3);
    assert_eq!(imported.retained_row_count, 1);
    assert_spelling(pool, &wrapped_foo, "foo.eth", 0).await?;
    assert_spelling(pool, &y_eth, &format!("{y}.eth"), 1).await?;
    fixture.db.cleanup().await?;
    assert!(
        !waited,
        "an absent-label import waited for a batch that holds only the stored label eth"
    );
    Ok(())
}

async fn stored_with_versions(pool: &PgPool) -> TestResult<serde_json::Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object(
        'documents',(SELECT jsonb_agg(jsonb_build_array(logical_name_id,name,spelling_class,display_name_override,xmin::text) ORDER BY logical_name_id) FROM name_search_documents),
        'postings',(SELECT jsonb_agg(jsonb_build_array(search_id,spelling_class,token_kind,token_length,encode(token_bytes,'hex'),xmin::text) ORDER BY search_id,spelling_class,token_kind,token_length,token_bytes) FROM name_search_postings))")
        .fetch_one(pool).await?)
}

async fn refresh_every_surface(pool: &PgPool) -> TestResult {
    let names: Vec<String> = sqlx::query_scalar("SELECT logical_name_id FROM name_surfaces")
        .fetch_all(pool)
        .await?;
    let mut tx = pool.begin().await?;
    bigname_storage::identity_search::refresh(&mut tx, &names, &[]).await?;
    tx.commit().await?;
    Ok(())
}

/// Batches with stored and new labels leave exactly what a refresh of every surface derives:
/// that refresh changes no document or posting and rewrites no tuple.
#[tokio::test]
async fn label_writes_match_full_refresh() -> TestResult {
    let fixture = labels("search_label_full_refresh").await?;
    let pool = fixture.db.pool();
    let block = SETUP_BLOCK + 2;
    wrapped(pool, block, 0, &["foo", "eth"]).await?;
    wrapped(pool, block, 1, &["fresh", "eth"]).await?;
    next_batch(pool, block, fixture.marker.clone()).await??;
    assert_spelling(pool, &fixture.x, &format!("[{:x}].eth", keccak256(b"x")), 1).await?;
    assert_spelling(
        pool,
        &fixture.fanout,
        &format!("fresh.[{:x}].eth", keccak256(b"beta")),
        1,
    )
    .await?;
    let before = stored_with_versions(pool).await?;
    refresh_every_surface(pool).await?;
    assert_eq!(stored_with_versions(pool).await?, before);
    fixture.db.cleanup().await?;
    Ok(())
}

fn statement_cost(queries: &str, tag: &str) -> serde_json::Value {
    let field = |line: &str, key: &str| {
        line.split_whitespace()
            .find_map(|part| part.strip_prefix(key))
            .and_then(|value| value.trim_matches('"').parse::<f64>().ok())
    };
    let lines: Vec<&str> = queries.lines().filter(|line| line.contains(tag)).collect();
    serde_json::json!({
        "statements": lines.len(),
        "milliseconds": lines.iter().filter_map(|line| field(line, "elapsed_secs=")).sum::<f64>() * 1000.0,
        "rows_returned": lines.iter().filter_map(|line| field(line, "rows_returned=")).sum::<f64>(),
    })
}

/// The cost of a batch that submits the stored label `eth` while SEARCH_LABEL_FANOUT unnamed
/// `[h].eth` surfaces exist. Run the same file at two commits on one host and compare.
#[tokio::test]
#[ignore = "operator measurement; SEARCH_LABEL_FANOUT sets the unnamed eth surfaces"]
async fn known_label_lock_cost() -> TestResult {
    let fanout: i64 = std::env::var("SEARCH_LABEL_FANOUT")
        .unwrap_or_else(|_| "210000".into())
        .parse()?;
    let db = fixture("search_label_lock_cost", 8).await?;
    let pool = db.pool();
    for offset in 0..=5 {
        insert_transaction(pool, SETUP_BLOCK + offset, NAME_WRAPPER).await?;
    }
    let seed = wrapped(pool, SETUP_BLOCK, 0, &["seed", "eth"]).await?;
    for k in 1..=5 {
        wrapped(pool, SETUP_BLOCK + k, 0, &[&format!("foo{k}"), "eth"]).await?;
    }
    let engine = Engine::new(pool.clone());
    let mut marker = engine
        .run_batch(request(SETUP_BLOCK, SETUP_BLOCK, None, RunMode::Normal))
        .await?
        .current;
    let setup = Instant::now();
    sqlx::query("INSERT INTO name_surfaces(logical_name_id,namespace,namehash,labelhashes,
            normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
        SELECT 'ens:'||node.hash,'ens',node.hash,
               ARRAY['0x'||md5('label-'||n)||md5('hash-'||n),$2],
               seed.normalizer_version,'active',seed.chain_id,seed.block_hash,seed.block_number,'canonical'
        FROM generate_series(1,$3::bigint) n
        CROSS JOIN (SELECT * FROM name_surfaces WHERE logical_name_id=$1) seed
        CROSS JOIN LATERAL (SELECT '0x'||md5('node-'||n)||md5('name-'||n) AS hash) node")
        .bind(&seed)
        .bind(format!("{:#x}", keccak256(b"eth")))
        .bind(fanout)
        .execute(pool)
        .await?;
    refresh_every_surface(pool).await?;
    sqlx::query("ANALYZE").execute(pool).await?;
    let settings:Vec<(String,String)>=sqlx::query_as("SELECT name,setting FROM pg_settings WHERE name IN ('shared_buffers','work_mem','effective_cache_size','max_parallel_workers_per_gather','jit','synchronous_commit','fsync') ORDER BY name").fetch_all(pool).await?;
    println!("SETTINGS {}", serde_json::to_string(&settings)?);
    println!(
        "FIXTURE {}",
        serde_json::json!({"fanout":fanout,"setup_seconds":setup.elapsed().as_secs_f64(),"compiled_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH})
    );
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
    for k in 1..=5 {
        log.take();
        let start = Instant::now();
        marker = engine
            .run_batch(request(
                SETUP_BLOCK + k,
                SETUP_BLOCK + k,
                Some(marker),
                RunMode::Normal,
            ))
            .await?
            .current;
        let milliseconds = start.elapsed().as_secs_f64() * 1000.0;
        let queries = log.take();
        println!(
            "SAMPLE {}",
            serde_json::json!({"batch":k,"milliseconds":milliseconds,
                "name_locks":statement_cost(&queries,"identity_search.name_locks"),
                "label_locks":statement_cost(&queries,"identity_search.label_locks"),
                "new_labels":statement_cost(&queries,"identity_search.new_labels")})
        );
    }
    let digest: (i64, i64, String) = sqlx::query_as(
        "WITH tokens AS (
             SELECT search_id, count(*) AS n, string_agg(concat_ws(',',spelling_class,token_kind,
                    token_length,encode(token_bytes,'hex')),';'
                    ORDER BY spelling_class,token_kind,token_length,token_bytes) AS tokens
             FROM name_search_postings GROUP BY search_id)
         SELECT count(*), COALESCE(sum(tokens.n),0)::bigint,
                md5(string_agg(md5(concat_ws('|',d.logical_name_id,d.chain_id,d.namespace,d.namehash,
                    d.name,d.display_name_override,d.spelling_class,tokens.tokens)),'' ORDER BY d.logical_name_id))
         FROM name_search_documents d LEFT JOIN tokens USING (search_id)",
    )
    .fetch_one(pool)
    .await?;
    println!(
        "SNAPSHOT {}",
        serde_json::json!({"documents":digest.0,"postings":digest.1,"md5":digest.2})
    );
    db.cleanup().await?;
    Ok(())
}
