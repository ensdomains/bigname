//! What the composition pass worker tests (families_compose_workers.rs) watch a pass with: a
//! connection of the test's own, a hold that stops one names chunk at its commit until the test
//! lets it go, a log of every work list row a committed chunk marked composed, and a sampler of
//! the lock waits of the database's backends.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use sqlx::{Connection, PgConnection, PgPool, postgres::PgPoolOptions, raw_sql};
use tokio::task::JoinHandle;

use super::{CHAIN, Fixture};

/// A pool of the test's own, beside the fixture's pool the pass uses: one connection for a
/// hold and two for the test's reads.
pub async fn side(fixture: &Fixture) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(3)
        .connect_with(fixture.pool.connect_options().as_ref().clone())
        .await?)
}

/// The lowest work list serial, of the ids a lookup name row is composed for, as a SQL
/// predicate on the lookup row `NEW`.
pub const FIRST_LOOKED_UP: &str = "NEW.logical_name_id = (SELECT logical_name_id
    FROM project_compose_name_work WHERE chain_id = NEW.chain_id AND reach >= 1
    ORDER BY serial LIMIT 1)";
/// The highest such serial.
pub const LAST_LOOKED_UP: &str = "NEW.logical_name_id = (SELECT logical_name_id
    FROM project_compose_name_work WHERE chain_id = NEW.chain_id AND reach >= 1
    ORDER BY serial DESC LIMIT 1)";

/// A names chunk stopped at its commit: the chunk has written every row and committed none, and
/// holds its claim. It waits on the advisory lock `key`, which a connection of the test's own
/// holds. The connection is detached from its pool, so dropping the hold on any path, an early
/// return or a panic included, closes the session and frees the lock, and no hold outlives its
/// test.
pub struct Hold {
    connection: PgConnection,
    key: i64,
}

/// Stop the names chunk that composes the lookup name `NEW` matching `predicate` at its commit,
/// once. With `fail`, the chunk raises "injected held chunk failure" when let go, once. The hook
/// is a deferred constraint trigger on `project_lookup_name` inserts, so it runs after the
/// chunk's last statement, inside its transaction, whatever the chunk wrote before.
pub async fn hold(
    fixture: &Fixture,
    side: &PgPool,
    key: i64,
    predicate: &str,
    fail: bool,
) -> Result<Hold> {
    let raise = if fail {
        format!(
            "IF nextval('compose_hold_fail_{key}') = 1 THEN
                 RAISE EXCEPTION 'injected held chunk failure';
             END IF;"
        )
    } else {
        String::new()
    };
    raw_sql(&format!(
        "CREATE SEQUENCE compose_hold_once_{key};
         CREATE SEQUENCE compose_hold_fail_{key};
         CREATE FUNCTION compose_hold_{key}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF ({predicate}) AND nextval('compose_hold_once_{key}') = 1 THEN
                 PERFORM pg_advisory_xact_lock({key});
                 {raise}
             END IF;
             RETURN NULL;
         END $$;
         CREATE CONSTRAINT TRIGGER compose_hold_{key} AFTER INSERT ON project_lookup_name
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION compose_hold_{key}();"
    ))
    .execute(&fixture.pool)
    .await?;
    let mut connection = side.acquire().await?.detach();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(key)
        .execute(&mut connection)
        .await?;
    Ok(Hold { connection, key })
}

impl Hold {
    /// Let the held chunk go on to its commit, and close the hold's session.
    pub async fn release(mut self) -> Result<()> {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(self.key)
            .execute(&mut self.connection)
            .await?;
        self.connection.close().await?;
        Ok(())
    }
}

/// Wait, at most `seconds`, until `probe` holds, checking every 50 ms.
pub async fn until<F, Fut>(seconds: u64, mut probe: F) -> Result<bool>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        if probe().await? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Whether a backend of the database waits on an advisory lock: a held chunk has reached its
/// hold.
pub async fn held(side: &PgPool) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database()
             AND wait_event_type = 'Lock' AND wait_event = 'advisory')",
    )
    .fetch_one(side)
    .await?)
}

/// The work list rows marked composed, as the test's connection sees them: committed chunks only.
pub async fn composed(side: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM project_compose_name_work WHERE chain_id = $1 AND composed",
    )
    .bind(CHAIN)
    .fetch_one(side)
    .await?)
}

/// The work list rows not marked composed, as the test's connection sees them.
pub async fn uncomposed(side: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM project_compose_name_work WHERE chain_id = $1 AND NOT composed",
    )
    .bind(CHAIN)
    .fetch_one(side)
    .await?)
}

/// The repair record's stream and committed chunk count, as the test's connection sees them.
pub async fn stream(side: &PgPool) -> Result<(Option<i16>, i64)> {
    Ok(sqlx::query_as(
        "SELECT compose_stream, compose_chunks FROM project_repair_record WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(side)
    .await?)
}

/// Log every work list row a committed statement moved to composed (`compose_transitions`),
/// by its id.
pub async fn log_transitions(fixture: &Fixture) -> Result<()> {
    raw_sql(
        "CREATE TABLE compose_transitions (seen bigserial PRIMARY KEY, serial bigint NOT NULL,
             logical_name_id text NOT NULL);
         CREATE FUNCTION compose_transitions() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO compose_transitions (serial, logical_name_id)
             SELECT moved.serial, moved.logical_name_id FROM moved
             JOIN was USING (chain_id, serial)
             WHERE moved.composed AND NOT was.composed
             ORDER BY moved.serial;
             RETURN NULL;
         END $$;
         CREATE TRIGGER compose_transitions AFTER UPDATE ON project_compose_name_work
         REFERENCING OLD TABLE AS was NEW TABLE AS moved
         FOR EACH STATEMENT EXECUTE FUNCTION compose_transitions();",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// The composed transitions logged so far, in commit order, by id.
pub async fn transitions(fixture: &Fixture) -> Result<Vec<(i64, String)>> {
    Ok(
        sqlx::query_as("SELECT serial, logical_name_id FROM compose_transitions ORDER BY seen")
            .fetch_all(&fixture.pool)
            .await?,
    )
}

/// Log the lookup inventory rows standing each time the repair record leaves the names stream
/// (`names_end_inventories`). No names chunk writes one, so each count is zero.
pub async fn log_names_end_inventories(fixture: &Fixture) -> Result<()> {
    raw_sql(
        "CREATE TABLE names_end_inventories (seen bigserial PRIMARY KEY, rows bigint NOT NULL);
         CREATE FUNCTION names_end_inventories() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO names_end_inventories (rows)
             SELECT count(*) FROM project_lookup_inventory;
             RETURN NULL;
         END $$;
         CREATE TRIGGER names_end_inventories AFTER UPDATE ON project_repair_record
         FOR EACH ROW WHEN (OLD.compose_stream = 0 AND NEW.compose_stream = 1)
         EXECUTE FUNCTION names_end_inventories();",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// The lookup inventory rows logged at each end of the names stream, in order.
pub async fn names_end_inventories(fixture: &Fixture) -> Result<Vec<i64>> {
    Ok(
        sqlx::query_scalar("SELECT rows FROM names_end_inventories ORDER BY seen")
            .fetch_all(&fixture.pool)
            .await?,
    )
}

/// Require every id the latest pass listed to have moved to composed exactly once.
pub async fn require_composed_once(fixture: &Fixture, listed: i64) -> Result<()> {
    let seen = transitions(fixture).await?;
    let mut ids: Vec<&str> = seen.iter().map(|(_, id)| id.as_str()).collect();
    let all = ids.len();
    ids.sort_unstable();
    ids.dedup();
    ensure!(
        all == ids.len() && i64::try_from(all)? == listed,
        "{all} transitions to composed of {} ids, {listed} listed",
        ids.len()
    );
    Ok(())
}

/// A sampler of the database's client backends waiting on a heavyweight lock another client
/// backend holds, every 50 ms until stopped. A hold's advisory wait is the test's own, and a
/// relation extension wait is two inserts into one table growing it, not a key two chunks
/// share, so neither is kept. Neither is a wait on autovacuum, which can hold a table's share
/// update exclusive lock against the coordinator's `ANALYZE`.
pub struct LockWaits {
    stop: Arc<AtomicBool>,
    task: JoinHandle<Result<Vec<String>>>,
}

pub fn lock_waits(side: PgPool) -> LockWaits {
    let stop = Arc::new(AtomicBool::new(false));
    let running = Arc::clone(&stop);
    let task = tokio::spawn(async move {
        let mut seen = Vec::new();
        while !running.load(Ordering::SeqCst) {
            let waits: Vec<String> = sqlx::query_scalar(
                "SELECT waiter.wait_event || ': ' || left(waiter.query, 120)
                 FROM pg_stat_activity waiter
                 WHERE waiter.datname = current_database() AND waiter.pid <> pg_backend_pid()
                   AND waiter.backend_type = 'client backend'
                   AND waiter.wait_event_type = 'Lock'
                   AND waiter.wait_event NOT IN ('advisory', 'extend')
                   AND EXISTS (SELECT 1 FROM pg_stat_activity blocker
                               WHERE blocker.pid = ANY (pg_blocking_pids(waiter.pid))
                                 AND blocker.backend_type = 'client backend')",
            )
            .fetch_all(&side)
            .await?;
            seen.extend(waits);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(seen)
    });
    LockWaits { stop, task }
}

impl LockWaits {
    /// Stop sampling and return every wait seen.
    pub async fn stop(self) -> Result<Vec<String>> {
        self.stop.store(true, Ordering::SeqCst);
        self.task.await?
    }
}
