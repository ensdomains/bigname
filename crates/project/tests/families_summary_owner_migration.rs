//! The schema-migration that adds `project_name_summary.owner`
//! (migrations/20260929170000_project_name_summary_owner.sql). On a database without the column it
//! resets every owned key family, so the next family run rebuilds them and writes every owner.
//! It takes the marker table before touching anything and holds it to commit. So a family run
//! already holding a marker finishes first, and a run that starts meanwhile, including one that
//! would create the marker of a chain that has none, waits for the column instead of publishing
//! summaries that lost their owner.
#[path = "families_support/mod.rs"]
mod support;

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use bigname_storage::families::name::load_family_name;
use serde_json::{Value, json};
use sqlx::{
    Connection, PgConnection, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    raw_sql,
};
use support::{CHAIN, CONTENT_HASH, Event, Fixture, hash, marker, uuid};
use tokio::sync::oneshot;

const MIGRATION: &str =
    include_str!("../../../migrations/20260929170000_project_name_summary_owner.sql");
const V2_REGISTRY: &str = "ens_v2_registry_l1";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const OTHER_OWNER: &str = "0x00000000000000000000000000000000000000bb";
/// A chain with lineage and owner-bearing events but no family marker.
const NEW_CHAIN: &str = "ethereum-holesky";
const NAME: &str = "ens:0x0000000000000000000000000000000000000000000000000000000000000001";
const NEW_NAME: &str = "ens:0x0000000000000000000000000000000000000000000000000000000000000002";
/// The tables the migration resets besides the family inventory.
const CONTROL_TABLES: [&str; 3] = [
    "project_family_marker",
    "project_family_undo",
    "project_repair_record",
];

/// The quoted names of the migration's `ARRAY[...]` reset list, read as text: it does not parse
/// SQL, so a name inside a comment would still count.
fn reset_literal() -> BTreeSet<String> {
    let start = MIGRATION.find("ARRAY ARRAY[").expect("reset list") + "ARRAY ARRAY[".len();
    let end = start + MIGRATION[start..].find(']').expect("end of reset list");
    MIGRATION[start..end]
        .split('\'')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

/// The family inventory when this historical migration was introduced. The Universal Resolver
/// table was added later by 20260929200000_project_universal_resolver_proxy.sql and the ENSv2
/// registry entry tables by 20261005140000_project_ens_v2_registry_entries.sql, so the owner
/// migration cannot reset them. Keep it installed for the current family runner used below, but
/// do not seed it as predecessor state or include it in the historical reset assertion.
fn reset_tables() -> Vec<String> {
    families::family_tables()
        .filter(|table| {
            !matches!(
                *table,
                "project_universal_resolver_proxy"
                    | "project_ens_v2_entry_owner"
                    | "project_ens_v2_registry_parent"
                    | "project_text_hydration_work"
                    | "project_reverse_hydration_work"
            )
        })
        .map(str::to_owned)
        .chain(CONTROL_TABLES.map(str::to_owned))
        .collect()
}

/// A drift check on the migration's literal reset list: it names exactly the family inventory
/// (crates/project/src/families/tables.rs) and the control tables. What the migration actually
/// deletes is checked at run time by the concurrency test below.
#[test]
fn the_reset_literal_names_every_family_table() {
    assert_eq!(
        reset_literal(),
        reset_tables()
            .into_iter()
            .chain(support::RETIRED_FAMILY_TABLES.map(str::to_owned))
            .collect::<BTreeSet<_>>()
    );
}

/// An ENSv2 registration of `name` on `chain` at `block`, as the adapter writes it: the pending
/// LabelRegistered grant, then the TokenResource log's SurfaceBound, grant, AuthorityTransferred
/// naming `owner` and ExpiryChanged, and the name's open ENSv2 binding to `resource`.
#[allow(clippy::too_many_arguments)]
async fn register_on(
    fixture: &Fixture,
    chain: &str,
    name: &str,
    block: i64,
    token: &str,
    resource: &str,
    binding: &str,
    owner: &str,
) -> Result<()> {
    let instance = uuid(0x901);
    let namehash = name.split_once(':').map_or(name, |(_, hash)| hash);
    fixture.surface_on(chain, name, namehash).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 0, 'canonical')",
    )
    .bind(resource)
    .bind(chain)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
             binding_kind, authority_arm, active_from, chain_id, block_hash, block_number,
             provenance, canonicality_state)
         VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2',
                 to_timestamp(1800000000 + $5 * 12), $4, $6, $5,
                 jsonb_build_object('transaction_index', 0, 'log_index', 2), 'canonical')",
    )
    .bind(binding)
    .bind(name)
    .bind(resource)
    .bind(chain)
    .bind(block)
    .bind(hash(block))
    .execute(&fixture.pool)
    .await?;
    let linked = json!({"token_id": token, "current_token_id": token, "upstream_resource": token});
    let mut bound = linked.clone();
    bound["source_event"] = json!("TokenResource");
    bound["binding_kind"] = json!("declared_registry_path");
    bound["surface_binding_id"] = json!(binding);
    let mut granted = linked.clone();
    for (key, value) in [
        ("source_event", json!("LabelRegistered")),
        ("registrant", json!(owner)),
        ("expiry", json!(2_000_000_000u64)),
        ("status", json!("registered")),
        ("authority_kind", json!("ens_v2_registry")),
        (
            "authority_key",
            json!(format!("ens-v2-registry:test:{instance}:{token}")),
        ),
        ("resource_pending", json!(false)),
        ("registry_contract_instance_id", json!(instance)),
    ] {
        granted[key] = value;
    }
    let mut transferred = linked.clone();
    transferred["source_event"] = json!("LabelRegistered");
    transferred["owner"] = json!(owner);
    let mut expiry = linked.clone();
    expiry["source_event"] = json!("LabelRegistered");
    expiry["expiry"] = json!(2_000_000_000u64);
    let pending = json!({"source_event": "LabelRegistered", "registrant": owner,
                         "expiry": 2_000_000_000u64, "token_id": token, "resource_pending": true,
                         "status": "registered", "registry_contract_instance_id": instance});
    for (log, kind, with_resource, after) in [
        (0, "RegistrationGranted", false, pending),
        (2, "SurfaceBound", true, bound),
        (2, "RegistrationGranted", true, granted),
        (2, "AuthorityTransferred", true, transferred),
        (2, "ExpiryChanged", true, expiry),
    ] {
        let identity = format!("{chain}:{kind}:{block}:{log}");
        let mut event = Event::new(&identity, block, log, kind, V2_REGISTRY)
            .on(chain)
            .name(name)
            .after(after)
            .raw(json!({"emitting_address": REGISTRY}));
        if with_resource {
            event = event.resource(resource);
        }
        fixture.event(event).await?;
    }
    Ok(())
}

async fn row_count(connection: &mut PgConnection, table: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(connection)
        .await?)
}

/// One row in `table` when it has none, so the reset has something to remove from every
/// table: each NOT NULL column without a default gets a placeholder of its type.
async fn populate(connection: &mut PgConnection, table: &str) -> Result<()> {
    if row_count(connection, table).await? > 0 {
        return Ok(());
    }
    let columns = sqlx::query(
        "SELECT attribute.attname::text AS name,
                format_type(attribute.atttypid, attribute.atttypmod) AS type,
                kind.typtype::text AS kind, attribute.atttypid::bigint AS oid,
                kind.typcategory::text AS category
         FROM pg_attribute attribute JOIN pg_type kind ON kind.oid = attribute.atttypid
         WHERE attribute.attrelid = $1::regclass AND attribute.attnum > 0
           AND NOT attribute.attisdropped AND attribute.attnotnull AND NOT attribute.atthasdef
         ORDER BY attribute.attnum",
    )
    .bind(table)
    .fetch_all(&mut *connection)
    .await?;
    let checks: Vec<String> = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = $1::regclass AND contype = 'c'",
    )
    .bind(table)
    .fetch_all(&mut *connection)
    .await?;
    let mut names = Vec::new();
    let mut values = Vec::new();
    for column in &columns {
        let name: String = column.try_get("name")?;
        let kind: String = column.try_get("type")?;
        let typtype: String = column.try_get("kind")?;
        let category: String = column.try_get("category")?;
        let oid: i64 = column.try_get("oid")?;
        let value = match (typtype.as_str(), category.as_str(), kind.as_str()) {
            ("e", _, _) => format!(
                "(SELECT enumlabel FROM pg_enum WHERE enumtypid = {oid} \
                 ORDER BY enumsortorder LIMIT 1)::{kind}"
            ),
            (_, "A", _) => format!("'{{}}'::{kind}"),
            (_, _, "uuid") => "gen_random_uuid()".to_owned(),
            (_, _, "jsonb" | "json") => format!("'{{}}'::{kind}"),
            (_, _, "boolean") => "false".to_owned(),
            (_, _, "bytea") => "'\\x'::bytea".to_owned(),
            (_, "N", _) => format!("0::{kind}"),
            (_, "D", _) => format!("now()::{kind}"),
            // A text column a check limits to literals takes the first literal after its name.
            _ => checks
                .iter()
                .find_map(|check| {
                    let after = &check[check.find(name.as_str())? + name.len()..];
                    let literal = after.split('\'').nth(1)?;
                    Some(format!("'{literal}'::{kind}"))
                })
                .unwrap_or_else(|| format!("'placeholder'::{kind}")),
        };
        names.push(format!("\"{name}\""));
        values.push(value);
    }
    let statement = if names.is_empty() {
        format!("INSERT INTO {table} DEFAULT VALUES")
    } else {
        format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            names.join(", "),
            values.join(", ")
        )
    };
    sqlx::query(&statement)
        .execute(&mut *connection)
        .await
        .with_context(|| format!("placeholder row for {table}: {statement}"))?;
    Ok(())
}

async fn session(options: &PgConnectOptions, name: &str) -> Result<PgConnection> {
    Ok(PgConnection::connect_with(&options.clone().application_name(name)).await?)
}

/// The lock `session` waits for: its kind, its relation and the sessions blocking it, by
/// application name; `None` while it waits for nothing.
async fn waiting(
    observer: &mut PgConnection,
    session: &str,
) -> Result<Option<(String, Option<String>, Vec<String>)>> {
    let row = sqlx::query(
        "SELECT lock.locktype::text AS kind, lock.relation::regclass::text AS relation,
                ARRAY(SELECT blocker.application_name::text FROM pg_stat_activity blocker
                      WHERE blocker.pid = ANY(pg_blocking_pids(activity.pid))
                      ORDER BY 1) AS blockers
         FROM pg_stat_activity activity
         JOIN pg_locks lock ON lock.pid = activity.pid AND NOT lock.granted
         WHERE activity.application_name = $1 AND activity.datname = current_database()",
    )
    .bind(session)
    .fetch_optional(&mut *observer)
    .await?;
    row.map(|row| -> Result<_> {
        Ok((
            row.try_get("kind")?,
            row.try_get("relation")?,
            row.try_get("blockers")?,
        ))
    })
    .transpose()
}

/// The lock `session` waits for, once it waits.
async fn await_wait(
    observer: &mut PgConnection,
    session: &str,
) -> Result<(String, Option<String>, Vec<String>)> {
    for _ in 0..400 {
        if let Some(wait) = waiting(observer, session).await? {
            return Ok(wait);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("{session} never waited on a lock")
}

fn marker_wait(blocker: &str) -> (String, Option<String>, Vec<String>) {
    (
        "relation".to_owned(),
        Some("project_family_marker".to_owned()),
        vec![blocker.to_owned()],
    )
}

/// The stored owner of `name` on `chain` must be the owner its composed row serves, `expected`.
async fn assert_summary_owner(
    pool: &PgPool,
    chain: &str,
    name: &str,
    expected: &str,
) -> Result<()> {
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT owner FROM project_name_summary WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(chain)
    .bind(name)
    .fetch_one(pool)
    .await?;
    let served = load_family_name(pool, name).await?.expect("composed name");
    let control = &served.declared_summary["control"];
    let declared = [&control["owner"], &control["registry_owner"]]
        .into_iter()
        .find_map(Value::as_str)
        .map(str::to_ascii_lowercase);
    ensure!(
        declared.as_deref() == Some(expected) && stored == declared,
        "{name}: stored {stored:?}, served {declared:?}, expected {expected}"
    );
    Ok(())
}

/// The interleaving the lock exists for, made deterministic with lock waits:
/// 1. a family run already holds its chain's marker (the statements `marker::lock` runs);
/// 2. the migration starts and must wait for that run on the marker table itself, before it
///    deletes a row or adds the column, which rejects a lock taken any later;
/// 3. a real family run starts for a chain with no marker and must queue behind the migration on
///    the marker table;
/// 4. the in-flight run ends; the migration resets every family table and adds the column, still
///    uncommitted, and the new run is still waiting behind it;
/// 5. the migration commits; the new run creates its marker and rebuilds its chain with the
///    column, storing its name's owner.
#[tokio::test]
async fn the_migration_takes_the_marker_table_before_any_reset() -> Result<()> {
    let fixture = Fixture::new("summary_owner_migration", 12).await?;
    fixture.lineage(NEW_CHAIN, 12).await?;
    register_on(
        &fixture,
        CHAIN,
        NAME,
        2,
        "0x1",
        &uuid(0xa01),
        &uuid(0xb01),
        OWNER,
    )
    .await?;
    register_on(
        &fixture,
        NEW_CHAIN,
        NEW_NAME,
        3,
        "0x2",
        &uuid(0xa02),
        &uuid(0xb02),
        OTHER_OWNER,
    )
    .await?;
    fixture.apply(7, FamilyMode::Normal).await?;
    // A database from before the column, with a row in every table the reset must empty.
    sqlx::query("ALTER TABLE project_name_summary DROP COLUMN owner")
        .execute(&fixture.pool)
        .await?;
    let options = fixture.pool.connect_options().as_ref().clone();
    let mut observer = session(&options, "observer").await?;
    let tables = reset_tables();
    for table in &tables {
        populate(&mut observer, table).await?;
        ensure!(
            row_count(&mut observer, table).await? > 0,
            "{table} is empty"
        );
    }
    let marker_rows = row_count(&mut observer, "project_family_marker").await?;

    // 1. A family run of CHAIN holds its marker.
    let mut in_flight = session(&options, "in_flight").await?;
    let mut in_flight_tx = in_flight.begin().await?;
    sqlx::query(
        "INSERT INTO project_family_marker (chain_id, state) VALUES ($1, 'live')
         ON CONFLICT (chain_id) DO NOTHING",
    )
    .bind(CHAIN)
    .execute(&mut *in_flight_tx)
    .await?;
    sqlx::query("SELECT chain_id FROM project_family_marker WHERE chain_id = $1 FOR UPDATE")
        .bind(CHAIN)
        .fetch_one(&mut *in_flight_tx)
        .await?;

    // 2. The migration waits for it on the marker table, before its first delete. It runs on
    // its own connection, in a plain BEGIN/COMMIT as sqlx runs a migration, concurrently with the
    // steps below.
    let mut migration = session(&options, "migration").await?;
    let (migrated_tx, migrated) = oneshot::channel::<Vec<(String, i64)>>();
    let (commit_tx, commit) = oneshot::channel::<()>();
    let migrate = async {
        raw_sql("BEGIN").execute(&mut migration).await?;
        raw_sql(MIGRATION).execute(&mut migration).await?;
        // What the migration left, seen from its own uncommitted transaction.
        let mut left = Vec::new();
        for table in &tables {
            left.push((table.clone(), row_count(&mut migration, table).await?));
        }
        let _ = migrated_tx.send(left);
        commit.await?;
        raw_sql("COMMIT").execute(&mut migration).await?;
        anyhow::Ok(())
    };
    let interleave = async {
        assert_eq!(
            await_wait(&mut observer, "migration").await?,
            marker_wait("in_flight")
        );

        // 3. A family run for NEW_CHAIN, which has no marker, queues behind the migration.
        let writer_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone().application_name("new_marker"))
            .await?;
        let writer = tokio::spawn(async move {
            let token = families::input_token(&writer_pool, NEW_CHAIN).await?;
            families::apply(
                &writer_pool,
                NEW_CHAIN,
                &marker(7),
                FamilyMode::Normal,
                &token,
                &FamilyOptions::new(CONTENT_HASH),
            )
            .await
        });
        assert_eq!(
            await_wait(&mut observer, "new_marker").await?,
            marker_wait("migration")
        );
        ensure!(
            row_count(&mut observer, "project_family_marker").await? == marker_rows,
            "a marker changed while the migration waited"
        );

        // 4. The in-flight run ends; the migration resets every table and adds the column,
        // still uncommitted, and the new run still waits behind it.
        in_flight_tx.commit().await?;
        let left = migrated.await?;
        let survivors: Vec<_> = left.iter().filter(|(_, rows)| *rows > 0).collect();
        ensure!(
            survivors.is_empty(),
            "rows survived the reset: {survivors:?}"
        );
        ensure!(
            left.len() == tables.len(),
            "the reset check covered {} tables",
            left.len()
        );
        assert_eq!(
            await_wait(&mut observer, "new_marker").await?,
            marker_wait("migration")
        );
        ensure!(!writer.is_finished(), "the new run went past the migration");

        // 5. After the commit the new run stores its name's owner.
        commit_tx
            .send(())
            .map_err(|()| anyhow::anyhow!("the migration stopped before its commit"))?;
        anyhow::Ok(writer)
    };
    let (migrated, writer) = tokio::join!(migrate, interleave);
    // The interleaving's own failure first: the migration then only reports its dropped commit.
    let writer = writer?;
    migrated?;
    writer.await??;
    assert_summary_owner(&fixture.pool, NEW_CHAIN, NEW_NAME, OTHER_OWNER).await?;
    drop(observer);

    // The reset chain rebuilds on its next run and stores its owner too.
    fixture.apply(7, FamilyMode::Normal).await?;
    assert_summary_owner(&fixture.pool, CHAIN, NAME, OWNER).await?;
    fixture.cleanup().await
}
