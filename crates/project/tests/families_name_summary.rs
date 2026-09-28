//! The name summary family (TYR-36 step 7b slice 2b, `project_name_summary`): the per-name fields
//! the child and label lists read inside one statement, written by the family step for the names
//! a block touches and journalled like every other family. A block that touches one name rewrites
//! that name's row and no other, undo puts the previous row back, and a rebuild writes the same
//! rows as the incremental follow. Every row carries the fields of the name's served row, and a
//! name whose composition the clock changes is composed again at the first block past it.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families;
use serde_json::{Value, json};
use shadow_support::{publish, served};
use support::{CHAIN, Event, Fixture, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_REGISTRY: &str = "ens_v1_registry_l1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

fn name(n: u64) -> String {
    format!("ens:0x{n:064x}")
}

/// Name `n` bound at `block` to its own lease under arm ens_v1 and granted one block later
/// until `expiry`.
async fn registered(fixture: &Fixture, n: u64, block: i64, expiry: u64) -> Result<()> {
    let lease = uuid(0x1000 + u32::try_from(n)?);
    fixture
        .binding(
            &uuid(100 + u32::try_from(n)?),
            &name(n),
            &lease,
            "ens_v1",
            block,
            0,
            None,
        )
        .await?;
    fixture
        .write(
            block,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(n)),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            block + 1,
            0,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(n)),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": expiry}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// The summary row of `logical_name_id` without its chain, and the row version it was written in.
async fn summary(fixture: &Fixture, logical_name_id: &str) -> Result<Option<(Value, String)>> {
    Ok(sqlx::query_as(
        "SELECT to_jsonb(summary) - 'chain_id', summary.xmin::text
         FROM project_name_summary summary
         WHERE summary.chain_id = $1 AND summary.logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_optional(&fixture.pool)
    .await?)
}

/// The summary row of `logical_name_id` must carry its served row's selected arm, serving flag,
/// registration status and expiry.
async fn assert_matches_served(fixture: &Fixture, logical_name_id: &str) -> Result<Value> {
    let (row, _) = summary(fixture, logical_name_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no summary row"))?;
    let served = served(fixture, logical_name_id).await?;
    let expiry: Option<i64> = sqlx::query_scalar(
        "SELECT extract(epoch FROM expires_at)::bigint FROM project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        row["authority_arm"] == served.provenance["authority_selection"]["authority_arm"],
        "{logical_name_id}: arm {row} against {}",
        served.provenance
    );
    ensure!(
        row["serving"]
            == json!(
                !served.provenance["read_reachability"]["serving_resource_id"].is_null()
                    && served.provenance["read_reachability"]["serving_resource_id"] != json!(null)
            ),
        "{logical_name_id}: serving {row} against {}",
        served.provenance
    );
    ensure!(
        row["registration_status"] == served.registration("status"),
        "{logical_name_id}: status {row} against {}",
        served.summary
    );
    ensure!(
        expiry.map(Value::from).unwrap_or(Value::Null) == served.registration("expiry"),
        "{logical_name_id}: expiry {expiry:?} against {}",
        served.summary
    );
    Ok(row)
}

#[tokio::test]
async fn a_block_rewrites_the_summary_of_the_name_it_touches_and_undo_restores_it() -> Result<()> {
    let fixture = Fixture::new("families_name_summary", 12).await?;
    registered(&fixture, 1, 2, 2_000_000_000).await?;
    registered(&fixture, 2, 4, 2_100_000_000).await?;
    publish(&fixture, 7).await?;
    let first = assert_matches_served(&fixture, &name(1)).await?;
    let second = assert_matches_served(&fixture, &name(2)).await?;
    assert_eq!(first["authority_arm"], json!("ens_v1"));
    assert_eq!(first["registration_status"], json!("active"));
    assert_eq!(second["registration_status"], json!("active"));
    let (_, first_version) = summary(&fixture, &name(1)).await?.expect("first row");
    let (_, second_version) = summary(&fixture, &name(2)).await?.expect("second row");

    // Block 8 renews the first name only.
    fixture
        .write(
            8,
            0,
            "RegistrationRenewed",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&uuid(0x1001)),
            json!({"expiry": 2_050_000_000u64}),
            REGISTRAR,
        )
        .await?;
    publish(&fixture, 8).await?;
    let renewed = assert_matches_served(&fixture, &name(1)).await?;
    assert_matches_served(&fixture, &name(2)).await?;
    ensure!(renewed != first, "the renewal left {renewed}");
    let (_, renewed_version) = summary(&fixture, &name(1)).await?.expect("first row");
    let (untouched, untouched_version) = summary(&fixture, &name(2)).await?.expect("second row");
    ensure!(
        renewed_version != first_version,
        "block 8 did not rewrite the renewed name's summary"
    );
    ensure!(
        untouched_version == second_version && untouched == second,
        "block 8 rewrote the summary of a name it does not touch"
    );
    let journalled: Vec<String> = sqlx::query_scalar(
        "SELECT key FROM project_family_undo
         WHERE chain_id = $1 AND block_number = 8 AND family = 'project_name_summary'",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        journalled == vec![json!([CHAIN, name(1)]).to_string()],
        "block 8 journalled the summaries {journalled:?}"
    );

    // Undo block 8: the renewed name's summary is the block-7 row again.
    let undone = families::undo_to(&fixture.pool, CHAIN, 7).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let (restored, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(restored == first, "undo left {restored}, not {first}");
    let (kept, _) = summary(&fixture, &name(2)).await?.expect("second row");
    ensure!(kept == second, "undo changed {kept}");

    // Replay, then the block-by-block and ranged rebuilds write the same rows.
    fixture
        .apply(8, bigname_project::families::FamilyMode::Normal)
        .await;
    let (replayed, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(
        replayed == renewed,
        "the replay wrote {replayed}, not {renewed}"
    );
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn the_family_undo_and_rebuild_keep_every_summary_row() -> Result<()> {
    let fixture = Fixture::new("families_name_summary_undo", 12).await?;
    registered(&fixture, 1, 2, 2_000_000_000).await?;
    registered(&fixture, 2, 6, 2_100_000_000).await?;
    shadow_support::publish_served(&fixture, 9).await?;
    // Block 7 grants the second name: undoing it restores its summary row as block 6 left it.
    fixture.assert_undo_restores(7).await?;
    fixture
        .apply(9, bigname_project::families::FamilyMode::Normal)
        .await;
    fixture.assert_rebuild_equal(9).await?;
    let rows = fixture.rows("project_name_summary").await?;
    ensure!(rows.len() == 2, "{rows:#?}");
    fixture.cleanup().await
}

/// Block times in the fixture: `1800000000 + 12 * block` seconds.
fn block_time(block: i64) -> i64 {
    1_800_000_000 + 12 * block
}

#[tokio::test]
async fn a_binding_that_closes_by_the_clock_is_composed_again_at_the_first_block_past_it()
-> Result<()> {
    let fixture = Fixture::new("families_name_summary_clock", 12).await?;
    // Name 1's binding closes at block 8's time, which no event of name 1 marks.
    let lease = uuid(0x1001);
    fixture
        .binding(&uuid(101), &name(1), &lease, "ens_v1", 2, 0, Some(8))
        .await?;
    fixture
        .write(
            2,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            3,
            0,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    registered(&fixture, 2, 4, 2_100_000_000).await?;
    // Block 8 carries family work that touches neither name.
    fixture
        .event(Event::new(
            "filler:8",
            8,
            90,
            "PreimageObserved",
            "ens_v1_registry_l1",
        ))
        .await?;
    publish(&fixture, 7).await?;
    let (open, _) = summary(&fixture, &name(1)).await?.expect("first row");
    let recompose_at: Option<i64> = sqlx::query_scalar(
        "SELECT recompose_at FROM project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name(1))
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        recompose_at == Some(block_time(8)),
        "the open binding's row recomposes at {recompose_at:?}, not block 8's time"
    );
    let (second, second_version) = summary(&fixture, &name(2)).await?.expect("second row");

    fixture
        .apply(8, bigname_project::families::FamilyMode::Normal)
        .await;
    let (closed, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(
        closed != open,
        "block 8 left the closed binding's row {closed}"
    );
    let (kept, kept_version) = summary(&fixture, &name(2)).await?.expect("second row");
    ensure!(
        kept == second && kept_version == second_version,
        "block 8 rewrote the summary of a name the clock does not change"
    );
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

/// Tuples the current transaction has read from `table` and its indexes.
async fn tuples_read(connection: &mut sqlx::PgConnection, table: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(sum(pg_stat_get_xact_tuples_returned(relation)
                             + pg_stat_get_xact_tuples_fetched(relation)), 0)::bigint
         FROM (SELECT $1::regclass::oid AS relation
               UNION ALL
               SELECT indexrelid FROM pg_index WHERE indrelid = $1::regclass) relations",
    )
    .bind(table)
    .fetch_one(connection)
    .await?)
}

// A name's zero-owner candidates are its own registry Transfers, the unnamed ones at its node and
// the unnamed ones of a resource its named registry events carry. Other names' resources are not
// candidates, so composing one name's summary reads a bounded number of registry owner events and
// registry events however many other names' resources carry unnamed Transfers. Counted in tuples
// read, with sequential scans off so the count follows the plan's selectivity, not the table size.
#[tokio::test]
async fn composing_one_summary_reads_only_that_names_zero_owner_candidates() -> Result<()> {
    const OTHERS: u64 = 60;
    let fixture = Fixture::new("families_name_summary_cost", 12).await?;
    registered(&fixture, 1, 2, 2_000_000_000).await?;
    for n in 0..OTHERS {
        let other = name(200 + n);
        let resource = uuid(0x5000 + u32::try_from(n)?);
        fixture
            .surface(&other, &format!("0x{:064x}", 200 + n))
            .await?;
        fixture.resource(&resource).await?;
        let named = format!("named-transfer:{n}");
        fixture
            .event(
                Event::new(
                    &named,
                    3,
                    i64::try_from(n)?,
                    "AuthorityTransferred",
                    V1_REGISTRY,
                )
                .name(&other)
                .resource(&resource)
                .after(
                    json!({"source_event": "Transfer", "node": format!("0x{:064x}", 200 + n),
                                  "owner": OWNER, "owner_getter": OWNER}),
                ),
            )
            .await?;
        let unnamed = format!("unnamed-transfer:{n}");
        fixture
            .event(
                Event::new(
                    &unnamed,
                    4,
                    i64::try_from(n)?,
                    "AuthorityTransferred",
                    V1_REGISTRY,
                )
                .resource(&resource)
                .after(json!({"source_event": "Transfer",
                                  "node": format!("0x{:064x}", 0x9000 + n),
                                  "owner": ZERO, "owner_getter": ZERO})),
            )
            .await?;
    }
    publish(&fixture, 5).await?;
    let publication =
        bigname_storage::families::name::load_family_publication(&fixture.pool, CHAIN)
            .await?
            .ok_or_else(|| anyhow::anyhow!("nothing is published"))?;
    let mut transaction = fixture.pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;
    let owner_events_before = tuples_read(&mut transaction, "project_registry_owner_event").await?;
    let events_before = tuples_read(&mut transaction, "normalized_events").await?;
    let composed = bigname_storage::families::name::compose_name_summaries(
        &mut transaction,
        &publication,
        &[name(1)],
    )
    .await?;
    ensure!(composed.contains_key(&name(1)), "{composed:?}");
    let owner_events =
        tuples_read(&mut transaction, "project_registry_owner_event").await? - owner_events_before;
    let events = tuples_read(&mut transaction, "normalized_events").await? - events_before;
    transaction.rollback().await?;
    let bound = i64::try_from(OTHERS / 4)?;
    ensure!(
        owner_events <= bound && events <= bound,
        "composing one name read {owner_events} registry owner event and {events} event tuples \
         with {OTHERS} other names' unnamed Transfers on the chain"
    );
    fixture.cleanup().await
}

// A NameWrapper expiry keeps the whole unsigned word, so a wrapped name's next clock boundary can
// be any second a 64-bit integer holds, far past the last instant a timestamp holds. The block
// that writes such a wrapper publishes, and its summary keeps the boundary in seconds (or none
// when the boundary does not fit a 64-bit integer).
#[tokio::test]
async fn a_wrapper_expiry_past_the_timestamp_range_keeps_its_boundary_in_seconds() -> Result<()> {
    let fixture = Fixture::new("families_name_summary_far_expiry", 12).await?;
    for (n, expiry) in [(1, json!(1_000_000_000_000_000u64)), (2, json!(u64::MAX))] {
        let wrapper = uuid(0x2000 + u32::try_from(n)?);
        fixture
            .binding(
                &uuid(200 + u32::try_from(n)?),
                &name(n),
                &wrapper,
                "ens_v1",
                2,
                0,
                None,
            )
            .await?;
        for (log, kind, after) in [
            (
                1,
                "PermissionScopeChanged",
                json!({"fuses": 65_536 | 1 << 17, "wrapper_state": "emancipated"}),
            ),
            (2, "ExpiryChanged", json!({"expiry": expiry})),
        ] {
            fixture
                .write(
                    3,
                    10 * i64::try_from(n)? + log,
                    kind,
                    "ens_v1_wrapper_l1",
                    Some(&name(n)),
                    Some(&wrapper),
                    after,
                    REGISTRAR,
                )
                .await?;
        }
    }
    publish(&fixture, 4).await?;
    for (n, boundary) in [
        (1, Some(1_000_000_000_000_000i64 - 7_776_000 + 1)),
        (2, None),
    ] {
        let stored: Option<i64> = sqlx::query_scalar(
            "SELECT recompose_at FROM project_name_summary
             WHERE chain_id = $1 AND logical_name_id = $2",
        )
        .bind(CHAIN)
        .bind(name(n))
        .fetch_one(&fixture.pool)
        .await?;
        ensure!(
            stored == boundary,
            "name {n} recomposes at {stored:?}, not {boundary:?}"
        );
    }
    fixture.assert_rebuild_equal(4).await?;
    fixture.cleanup().await
}

// A name whose selected binding's token lineage is not readable composes no row, but its
// composition still changes at a clock boundary: here its binding closes at block 8's time and a
// readable one, recorded at block 3, opens then. The summary keeps that boundary, and block 8,
// which touches no fact of the name, composes it again.
#[tokio::test]
async fn a_name_with_no_composed_row_keeps_its_clock_boundary() -> Result<()> {
    let fixture = Fixture::new("families_name_summary_unreadable", 12).await?;
    let unreadable = uuid(0x1001);
    let token = uuid(0x3001);
    sqlx::query(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 1, 'observed')",
    )
    .bind(&token)
    .bind(CHAIN)
    .bind(support::hash(1))
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2::uuid, $3, $4, 0, 'canonical')",
    )
    .bind(&unreadable)
    .bind(&token)
    .bind(CHAIN)
    .bind(support::hash(0))
    .execute(&fixture.pool)
    .await?;
    fixture
        .binding(&uuid(101), &name(1), &unreadable, "ens_v1", 2, 0, Some(8))
        .await?;
    let readable = uuid(0x1101);
    fixture.resource(&readable).await?;
    // The readable binding is recorded at block 3 and opens at block 8's time.
    sqlx::query(
        "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
             binding_kind, authority_arm, active_from, active_to, chain_id, block_hash,
             block_number, provenance, canonicality_state)
         VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                 to_timestamp($4), NULL, $5, $6, 3,
                 jsonb_build_object('transaction_index', 0, 'log_index', 1), 'canonical')",
    )
    .bind(uuid(102))
    .bind(name(1))
    .bind(&readable)
    .bind(block_time(8) as f64)
    .bind(CHAIN)
    .bind(support::hash(3))
    .execute(&fixture.pool)
    .await?;
    for (block, lease) in [(2, &unreadable), (3, &readable)] {
        fixture
            .write(
                block,
                2,
                "SurfaceBound",
                V1_REGISTRAR,
                Some(&name(1)),
                Some(lease),
                json!({"authority_kind": "registrar", "state_derived": false,
                       "registry_contract": REGISTRY, "owner_getter": OWNER}),
                REGISTRAR,
            )
            .await?;
        fixture
            .write(
                block,
                3,
                "RegistrationGranted",
                V1_REGISTRAR,
                Some(&name(1)),
                Some(lease),
                json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                       "expiry": 2_000_000_000u64}),
                REGISTRAR,
            )
            .await?;
    }
    fixture
        .event(Event::new(
            "filler:8",
            8,
            90,
            "PreimageObserved",
            "ens_v1_registry_l1",
        ))
        .await?;
    publish(&fixture, 7).await?;
    let (open, _) = summary(&fixture, &name(1)).await?.expect("a summary row");
    ensure!(
        open["registration_status"].is_null(),
        "the unreadable binding composes no row: {open}"
    );
    ensure!(
        open["recompose_at"] == json!(block_time(8)),
        "the summary with no composed row recomposes at {}, not block 8's time",
        open["recompose_at"]
    );
    fixture
        .apply(8, bigname_project::families::FamilyMode::Normal)
        .await;
    let (opened, _) = summary(&fixture, &name(1)).await?.expect("a summary row");
    ensure!(
        !opened["registration_status"].is_null(),
        "block 8 left the summary {opened}"
    );
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

// Undo of a block whose only summary change is a clock boundary (no other family writes)
// restores the summary exactly, and a replay writes the rebuild's rows. Undo of a block that
// changes nothing at all rewrites no summary.
#[tokio::test]
async fn undo_restores_a_clock_only_summary_and_rewrites_nothing_for_an_empty_block() -> Result<()>
{
    let fixture = Fixture::new("families_name_summary_clock_undo", 12).await?;
    fixture
        .binding(&uuid(101), &name(1), &uuid(0x1001), "ens_v1", 2, 0, Some(8))
        .await?;
    registered(&fixture, 2, 4, 2_100_000_000).await?;
    publish(&fixture, 7).await?;
    let (open, _) = summary(&fixture, &name(1)).await?.expect("first row");

    // Block 8 has no events: only the clock closes name 1's binding.
    fixture
        .apply(8, bigname_project::families::FamilyMode::Normal)
        .await;
    let (closed, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(closed != open, "block 8 left {closed}");
    let journalled: Vec<String> = sqlx::query_scalar(
        "SELECT family FROM project_family_undo WHERE chain_id = $1 AND block_number = 8
         ORDER BY family",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        journalled == ["marker", "project_name_summary"],
        "block 8 journalled {journalled:?}"
    );
    let undone = families::undo_to(&fixture.pool, CHAIN, 7).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let (restored, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(restored == open, "undo left {restored}, not {open}");
    fixture
        .apply(8, bigname_project::families::FamilyMode::Normal)
        .await;
    let (replayed, _) = summary(&fixture, &name(1)).await?.expect("first row");
    ensure!(
        replayed == closed,
        "the replay wrote {replayed}, not {closed}"
    );
    fixture.assert_rebuild_equal(8).await?;

    // Block 9 has no events and reaches no boundary: it and its undo rewrite no summary.
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT logical_name_id, xmin::text FROM project_name_summary ORDER BY 1")
            .fetch_all(&fixture.pool)
            .await?;
    fixture
        .apply(9, bigname_project::families::FamilyMode::Normal)
        .await;
    let summaries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_family_undo
         WHERE chain_id = $1 AND block_number = 9 AND family = 'project_name_summary'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(summaries == 0, "block 9 journalled {summaries} summaries");
    let undone = families::undo_to(&fixture.pool, CHAIN, 8).await?;
    ensure!(undone == 1, "undid {undone} blocks");
    let after: Vec<(String, String)> =
        sqlx::query_as("SELECT logical_name_id, xmin::text FROM project_name_summary ORDER BY 1")
            .fetch_all(&fixture.pool)
            .await?;
    ensure!(
        after == before,
        "block 9 or its undo rewrote summaries: {before:?} then {after:?}"
    );
    fixture.cleanup().await
}
