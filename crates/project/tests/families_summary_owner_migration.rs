//! The schema-migration that adds `project_name_summary.owner`
//! (migrations/20260929170000_project_name_summary_owner.sql). On a database without the column it
//! resets every owned key family, so the next family run rebuilds them and writes every owner. It
//! holds the marker table from the start, so a family run that starts meanwhile, including one
//! that would create the marker of a chain that has none, waits for the column instead of
//! publishing summaries that lost their owner.
#[path = "families_support/mod.rs"]
mod support;

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use bigname_storage::families::name::load_family_name;
use serde_json::{Value, json};
use sqlx::{postgres::PgPoolOptions, raw_sql};
use support::{CHAIN, CONTENT_HASH, Fixture, marker, uuid};

const MIGRATION: &str =
    include_str!("../../../migrations/20260929170000_project_name_summary_owner.sql");
const V2_REGISTRY: &str = "ens_v2_registry_l1";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
/// A chain with lineage and no family marker yet.
const NEW_CHAIN: &str = "ethereum-holesky";
const NAME: &str = "ens:0x0000000000000000000000000000000000000000000000000000000000000001";

/// The tables the migration's reset deletes: the quoted names of its `ARRAY[...]` list.
fn reset_list() -> BTreeSet<String> {
    let start = MIGRATION.find("ARRAY ARRAY[").expect("reset list") + "ARRAY ARRAY[".len();
    let end = start + MIGRATION[start..].find(']').expect("end of reset list");
    MIGRATION[start..end]
        .split('\'')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

#[test]
fn the_reset_covers_every_family_table() {
    let mut expected: BTreeSet<String> = families::family_tables().map(str::to_owned).collect();
    expected.extend(
        [
            "project_family_marker",
            "project_family_undo",
            "project_repair_record",
        ]
        .map(str::to_owned),
    );
    assert_eq!(reset_list(), expected);
}

fn instance() -> String {
    uuid(0x901)
}

/// One registration as the adapter writes it at `block`: LabelRegistered's grant at log 0 (its
/// resource still pending, so it is kept under the registry and token), then the TokenResource
/// log's SurfaceBound, grant, AuthorityTransferred and ExpiryChanged at log 2, and the name's
/// ENSv2 binding to `resource`, closed at `closed_at`.
async fn register(
    fixture: &Fixture,
    block: i64,
    token: &str,
    resource: &str,
    binding: &str,
    owner: &str,
    closed_at: Option<i64>,
) -> Result<()> {
    let instance = instance();
    fixture
        .binding(binding, NAME, resource, "ens_v2", block, 2, closed_at)
        .await?;
    fixture
        .write(
            block,
            0,
            "RegistrationGranted",
            V2_REGISTRY,
            Some(NAME),
            None,
            json!({"source_event": "LabelRegistered", "registrant": owner,
                   "expiry": 2_000_000_000u64, "token_id": token, "resource_pending": true,
                   "status": "registered", "registry_contract_instance_id": instance}),
            REGISTRY,
        )
        .await?;
    let linked = json!({"source_event": "TokenResource", "token_id": token,
                        "current_token_id": token, "upstream_resource": token});
    let mut bound = linked.clone();
    bound["binding_kind"] = json!("declared_registry_path");
    bound["surface_binding_id"] = json!(binding);
    fixture
        .write(
            block,
            2,
            "SurfaceBound",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            bound,
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            2,
            "RegistrationGranted",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            json!({"source_event": "LabelRegistered", "registrant": owner,
                   "expiry": 2_000_000_000u64, "token_id": token, "current_token_id": token,
                   "upstream_resource": token, "status": "registered",
                   "authority_kind": "ens_v2_registry",
                   "authority_key": format!("ens-v2-registry:test:{instance}:{token}"),
                   "resource_pending": false, "registry_contract_instance_id": instance}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            2,
            "AuthorityTransferred",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            json!({"source_event": "LabelRegistered", "token_id": token,
                   "current_token_id": token, "upstream_resource": token, "owner": owner}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            2,
            "ExpiryChanged",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            json!({"source_event": "LabelRegistered", "token_id": token,
                   "current_token_id": token, "upstream_resource": token,
                   "expiry": 2_000_000_000u64}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

async fn reset_rows(connection: &mut sqlx::PgConnection) -> Result<i64> {
    let mut rows = 0;
    for table in reset_list() {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&mut *connection)
            .await?;
        rows += count;
    }
    Ok(rows)
}

#[tokio::test]
async fn the_migration_resets_the_families_and_holds_off_a_new_marker() -> Result<()> {
    let fixture = Fixture::new("summary_owner_migration", 12).await?;
    fixture.lineage(NEW_CHAIN, 12).await?;
    register(&fixture, 2, "0x1", &uuid(0xa01), &uuid(0xb01), OWNER, None).await?;
    fixture.apply(7, FamilyMode::Normal).await?;
    // A database from before the column: the families are populated and the summary has no owner.
    sqlx::query("ALTER TABLE project_name_summary DROP COLUMN owner")
        .execute(&fixture.pool)
        .await?;
    let sessions = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(fixture.pool.connect_options().as_ref().clone())
        .await?;
    let mut observer = sessions.acquire().await?;
    ensure!(
        reset_rows(&mut observer).await? > 0,
        "the fixture populated no family"
    );

    let mut migration = sessions.begin().await?;
    raw_sql(MIGRATION).execute(&mut *migration).await?;
    ensure!(
        reset_rows(&mut migration).await? == 0,
        "a family row survived the reset"
    );

    // A family run for a chain with no marker starts while the migration is uncommitted. It must
    // wait on the marker table, not create its marker and rebuild without the column.
    let writer = {
        let pool = sessions.clone();
        tokio::spawn(async move {
            let token = families::input_token(&pool, NEW_CHAIN).await?;
            families::apply(
                &pool,
                NEW_CHAIN,
                &marker(7),
                FamilyMode::Normal,
                &token,
                &FamilyOptions::new(CONTENT_HASH),
            )
            .await
        })
    };
    let mut waiting = false;
    for _ in 0..200 {
        waiting = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_locks
                 WHERE NOT granted AND locktype = 'relation'
                   AND relation = 'project_family_marker'::regclass)",
        )
        .fetch_one(&mut *observer)
        .await?;
        if waiting || writer.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(
        waiting && !writer.is_finished(),
        "the family run did not wait on the marker table"
    );
    let markers: i64 = sqlx::query_scalar("SELECT count(*) FROM project_family_marker")
        .fetch_one(&mut *observer)
        .await?;
    ensure!(
        markers == 1,
        "the marker table changed under the migration: {markers} rows"
    );

    migration.commit().await?;
    writer.await??;
    drop(observer);
    sessions.close().await;

    // The next run of the reset chain rebuilds it and writes the owner the name row serves.
    fixture.apply(7, FamilyMode::Normal).await?;
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner FROM project_name_summary WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(NAME)
    .fetch_one(&fixture.pool)
    .await?;
    let served = load_family_name(&fixture.pool, NAME)
        .await?
        .expect("composed name");
    let control = &served.declared_summary["control"];
    let declared = [&control["owner"], &control["registry_owner"]]
        .into_iter()
        .find_map(Value::as_str)
        .map(str::to_ascii_lowercase);
    ensure!(
        declared.is_some(),
        "the fixture name serves no owner: {control}"
    );
    ensure!(
        owner == declared,
        "summary owner {owner:?}, served {declared:?}"
    );
    fixture.cleanup().await
}
