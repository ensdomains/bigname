//! The real verified-import transaction racing the same lexical helper Interpret calls.
use super::*;
use crate::identity_search;
use anyhow::ensure;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgConnection, Postgres, Transaction};
use std::time::{Duration, Instant};

async fn database(name: &str) -> Result<TestDatabase> {
    let db = TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(6)).await?;
    db.pool().set_connect_options(
        db.pool()
            .connect_options()
            .as_ref()
            .clone()
            .options([("statement_timeout", "10000"), ("lock_timeout", "8000")]),
    );
    for source in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/07_labels.sql"),
    ] {
        sqlx::raw_sql(source).execute(db.pool()).await?;
    }
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
        VALUES ('search-test','block-1',1,to_timestamp(1),'canonical')").execute(db.pool()).await?;
    Ok(db)
}

fn preimage(label: &str) -> RainbowPreimage {
    proven_rainbow_preimage(&format!("{:#x}", keccak256(label.as_bytes())), label).unwrap()
}

fn name_id(number: u64) -> String {
    format!("ens:0x{number:064x}")
}

async fn insert_surface(conn: &mut PgConnection, id: &str, path: &[String]) -> Result<()> {
    sqlx::query("INSERT INTO name_surfaces(logical_name_id,namespace,namehash,labelhashes,
        normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
        VALUES ($1,'ens',substring($1 FROM 5),$2,$3,'active','search-test','block-1',1,'canonical')")
        .bind(id).bind(path).bind(ENS_NORMALIZER_VERSION).execute(conn).await?;
    Ok(())
}

async fn surface(db: &TestDatabase, number: u64, path: &[String]) -> Result<String> {
    let id = name_id(number);
    let mut tx = db.pool().begin().await?;
    identity_search::prepare(&mut tx, &[], &[path.to_vec()], std::slice::from_ref(&id)).await?;
    insert_surface(&mut tx, &id, path).await?;
    identity_search::refresh(&mut tx, std::slice::from_ref(&id), &[]).await?;
    tx.commit().await?;
    Ok(id)
}

async fn spelling(conn: &mut PgConnection, id: &str) -> Result<(String, i16)> {
    Ok(sqlx::query_as(
        "SELECT name,spelling_class FROM name_search_documents WHERE logical_name_id=$1",
    )
    .bind(id)
    .fetch_one(conn)
    .await?)
}

async fn stored(pool: &PgPool) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object(
        'documents',(SELECT jsonb_agg(to_jsonb(d)||jsonb_build_object('xmin',d.xmin::text) ORDER BY logical_name_id) FROM name_search_documents d),
        'postings',(SELECT jsonb_agg(to_jsonb(p)||jsonb_build_object('xmin',p.xmin::text) ORDER BY namespace,spelling_class,token_kind,token_length,token_bytes,search_id) FROM name_search_postings p))")
        .fetch_one(pool).await?)
}

async fn import(pool: &PgPool, labels: &[&str]) -> Result<u64> {
    insert_label_preimages(
        pool,
        &labels
            .iter()
            .map(|label| preimage(label))
            .collect::<Vec<_>>(),
    )
    .await
}

async fn wait_for_lock(pool: &PgPool, query: &str, count: i64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
            WHERE datname=current_database() AND pid<>pg_backend_pid()
              AND wait_event_type='Lock' AND query LIKE $1",
        )
        .bind(format!("%{query}%"))
        .fetch_one(pool)
        .await?;
        if waiting >= count {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "expected {count} writers waiting at {query}; saw {waiting}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn import_publishes_all_text_and_postings_on_one_commit_and_rerun_does_no_work() -> Result<()>
{
    let db = database("search_import_atomic").await?;
    let label = preimage("rainbow");
    let id = surface(&db, 1, std::slice::from_ref(&label.labelhash)).await?;
    let mut held = db.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *held)
        .await?;
    let before = spelling(&mut held, &id).await?;
    ensure!(before.0.starts_with('[') && before.1 == 1);
    ensure!(import(db.pool(), &["rainbow"]).await? == 1);
    ensure!(spelling(&mut held, &id).await? == before);
    let old_hits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_search_postings WHERE token_kind=2 AND token_bytes=$1",
    )
    .bind(b"rai".as_slice())
    .fetch_one(&mut *held)
    .await?;
    ensure!(old_hits == 0);
    held.commit().await?;
    let mut current = db.pool().acquire().await?;
    ensure!(spelling(&mut current, &id).await? == ("rainbow".to_owned(), 1));
    let new_hits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_search_postings WHERE token_kind=2 AND token_bytes=$1",
    )
    .bind(b"rai".as_slice())
    .fetch_one(&mut *current)
    .await?;
    ensure!(new_hits == 1);
    drop(current);
    let after = stored(db.pool()).await?;
    ensure!(import(db.pool(), &["rainbow"]).await? == 0);
    ensure!(
        stored(db.pool()).await? == after,
        "idempotence preserves rows and tuple versions"
    );
    db.cleanup().await
}

#[tokio::test]
async fn absent_label_import_waits_for_new_structural_surface_then_refreshes_it() -> Result<()> {
    let db = database("search_new_surface_race").await?;
    let label = preimage("newborn");
    let id = name_id(2);
    let path = vec![label.labelhash];
    let mut creator = db.pool().begin().await?;
    identity_search::prepare(
        &mut creator,
        &[],
        &[path.clone()],
        std::slice::from_ref(&id),
    )
    .await?;
    insert_surface(&mut creator, &id, &path).await?;
    identity_search::refresh(&mut creator, std::slice::from_ref(&id), &[]).await?;
    let pool = db.pool().clone();
    let importer = tokio::spawn(async move { import(&pool, &["newborn"]).await });
    wait_for_lock(db.pool(), "identity_search.label_locks", 1).await?;
    creator.commit().await?;
    ensure!(importer.await?? == 1);
    ensure!(spelling(&mut db.pool().acquire().await?, &id).await?.0 == "newborn");
    db.cleanup().await
}

#[tokio::test]
async fn different_label_imports_wait_for_shared_names_and_use_fresh_post_wait_reads() -> Result<()>
{
    let db = database("search_overlap_imports").await?;
    let words: Vec<_> = (0..300).map(|n| format!("overlap-{n}")).collect();
    let hashes: Vec<_> = words.iter().map(|word| preimage(word).labelhash).collect();
    let buckets: Vec<i32> =
        sqlx::query_scalar("SELECT hashtext(label) & 255 FROM unnest($1::text[]) AS input(label)")
            .bind(&hashes)
            .fetch_all(db.pool())
            .await?;
    let second = buckets
        .iter()
        .position(|bucket| *bucket != buckets[0])
        .context("test inputs must include distinct lock buckets")?;
    let left = words[0].clone();
    let right = words[second].clone();
    let a = preimage(&left);
    let b = preimage(&right);
    let ids = vec![
        surface(&db, 3, &[a.labelhash.clone(), b.labelhash.clone()]).await?,
        surface(&db, 4, &[b.labelhash.clone(), a.labelhash.clone()]).await?,
    ];
    let mut blocker = db.pool().begin().await?;
    sqlx::query(
        "SELECT logical_name_id FROM name_surfaces ORDER BY logical_name_id FOR NO KEY UPDATE",
    )
    .execute(&mut *blocker)
    .await?;
    let pool_a = db.pool().clone();
    let left_task = left.clone();
    let task_a = tokio::spawn(async move { import(&pool_a, &[&left_task]).await });
    let pool_b = db.pool().clone();
    let right_task = right.clone();
    let task_b = tokio::spawn(async move { import(&pool_b, &[&right_task]).await });
    wait_for_lock(db.pool(), "identity_search.name_locks", 2).await?;
    blocker.commit().await?;
    ensure!(task_a.await?? == 1 && task_b.await?? == 1);
    let mut conn = db.pool().acquire().await?;
    ensure!(spelling(&mut conn, &ids[0]).await?.0 == format!("{left}.{right}"));
    ensure!(spelling(&mut conn, &ids[1]).await?.0 == format!("{right}.{left}"));
    drop(conn);
    db.cleanup().await
}

async fn raw_evidence(tx: &mut Transaction<'_, Postgres>, id: &str, name: &str) -> Result<()> {
    identity_search::prepare(tx, &[], &[], &[id.to_owned()]).await?;
    sqlx::query("UPDATE name_surfaces SET raw_name=$2, raw_labels=string_to_array($2,'.'),
        dns_encoded_name=convert_to($2,'UTF8'), preimage_event_identity='revealed' WHERE logical_name_id=$1")
        .bind(id).bind(name).execute(&mut **tx).await?;
    identity_search::refresh(tx, &[id.to_owned()], &[]).await
}

#[tokio::test]
async fn long_structural_import_and_raw_revelation_move_exact_membership_and_roll_back()
-> Result<()> {
    let db = database("search_class_and_rollback").await?;
    let label = preimage("a");
    let id = surface(&db, 5, &vec![label.labelhash; 32]).await?;
    let mut conn = db.pool().acquire().await?;
    let initial = spelling(&mut conn, &id).await?;
    ensure!(initial.0.len() > 2000 && initial.1 == 2);
    drop(conn);
    import(db.pool(), &["a"]).await?;
    let name = vec!["a"; 32].join(".");
    let mut conn = db.pool().acquire().await?;
    ensure!(spelling(&mut conn, &id).await? == (name.clone(), 1));
    let obsolete: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_search_postings WHERE spelling_class=2")
            .fetch_one(&mut *conn)
            .await?;
    ensure!(obsolete == 0);
    drop(conn);
    let before = stored(db.pool()).await?;
    let mut tx = db.pool().begin().await?;
    raw_evidence(&mut tx, &id, &name).await?;
    ensure!(spelling(&mut tx, &id).await?.1 == 0);
    tx.rollback().await?;
    ensure!(stored(db.pool()).await? == before);
    let mut tx = db.pool().begin().await?;
    raw_evidence(&mut tx, &id, &name).await?;
    tx.commit().await?;
    let remaining: Vec<i16> =
        sqlx::query_scalar("SELECT DISTINCT spelling_class FROM name_search_postings")
            .fetch_all(db.pool())
            .await?;
    ensure!(remaining == vec![0]);
    db.cleanup().await
}

#[tokio::test]
async fn colliding_buckets_serialize_complete_multi_name_work_without_lock_upgrade() -> Result<()> {
    let db = database("search_bucket_collision").await?;
    let labels: Vec<_> = (0..300).map(|n| format!("collision-{n}")).collect();
    let hashes: Vec<_> = labels
        .iter()
        .map(|label| preimage(label).labelhash)
        .collect();
    let buckets: Vec<i32> =
        sqlx::query_scalar("SELECT hashtext(label) & 255 FROM unnest($1::text[]) AS input(label)")
            .bind(&hashes)
            .fetch_all(db.pool())
            .await?;
    let (a, b) = (0..buckets.len())
        .find_map(|a| {
            ((a + 1)..buckets.len())
                .find(|&b| buckets[a] == buckets[b])
                .map(|b| (a, b))
        })
        .unwrap();
    let ids = [
        surface(&db, 6, &[hashes[a].clone(), hashes[b].clone()]).await?,
        surface(&db, 7, &[hashes[b].clone(), hashes[a].clone()]).await?,
    ];
    let mut first = db.pool().begin().await?;
    identity_search::prepare(
        &mut first,
        &[hashes[a].clone()],
        &[vec![hashes[b].clone()]],
        &ids,
    )
    .await?;
    let advisory_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks
        WHERE pid=pg_backend_pid() AND locktype='advisory' AND classid=228001",
    )
    .fetch_one(&mut *first)
    .await?;
    ensure!(
        advisory_count == 1,
        "exclusive wins within the complete colliding bucket set"
    );
    let pool = db.pool().clone();
    let wanted = labels[b].clone();
    let second = tokio::spawn(async move { import(&pool, &[&wanted]).await });
    wait_for_lock(db.pool(), "identity_search.label_locks", 1).await?;
    first.commit().await?;
    ensure!(second.await?? == 1);
    import(db.pool(), &[&labels[a]]).await?;
    let mut conn = db.pool().acquire().await?;
    ensure!(spelling(&mut conn, &ids[0]).await?.0 == format!("{}.{}", labels[a], labels[b]));
    ensure!(spelling(&mut conn, &ids[1]).await?.0 == format!("{}.{}", labels[b], labels[a]));
    drop(conn);
    db.cleanup().await
}

#[tokio::test]
async fn lexical_writes_refuse_repeatable_read_before_mutating_anything() -> Result<()> {
    let db = database("search_writer_isolation").await?;
    let mut tx = db.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let error = identity_search::prepare(&mut tx, &[], &[], &[])
        .await
        .unwrap_err();
    ensure!(error.to_string().contains("READ COMMITTED"));
    tx.rollback().await?;
    db.cleanup().await
}

#[tokio::test]
async fn new_surface_waiting_on_import_reads_the_committed_preimage() -> Result<()> {
    let db = database("search_import_before_surface").await?;
    let label = preimage("arrived");
    let mut importing = db.pool().begin().await?;
    identity_search::prepare(
        &mut importing,
        std::slice::from_ref(&label.labelhash),
        &[],
        &[],
    )
    .await?;
    // Hold the same source transaction shape as the importer, with the verified import row.
    sqlx::query(
        "INSERT INTO label_preimages(labelhash,raw_label,decoded_label,normalizer_version,
        normalized_under_version,source_kind,source_priority,provenance)
        VALUES ($1,$2,$3,$4,true,$5,5,'{}')",
    )
    .bind(&label.labelhash)
    .bind(&label.raw_label)
    .bind(&label.decoded_label)
    .bind(ENS_NORMALIZER_VERSION)
    .bind(ENS_RAINBOW_SOURCE_KIND)
    .execute(&mut *importing)
    .await?;
    identity_search::refresh(&mut importing, &[], std::slice::from_ref(&label.labelhash)).await?;
    let pool = db.pool().clone();
    let labelhash = label.labelhash.clone();
    let creator = tokio::spawn(async move {
        let id = name_id(8);
        let mut tx = pool.begin().await?;
        identity_search::prepare(
            &mut tx,
            &[],
            &[vec![labelhash.clone()]],
            std::slice::from_ref(&id),
        )
        .await?;
        insert_surface(&mut tx, &id, &[labelhash]).await?;
        identity_search::refresh(&mut tx, std::slice::from_ref(&id), &[]).await?;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(id)
    });
    wait_for_lock(db.pool(), "identity_search.label_locks", 1).await?;
    importing.commit().await?;
    let id = creator.await??;
    ensure!(spelling(&mut db.pool().acquire().await?, &id).await?.0 == "arrived");
    db.cleanup().await
}
