//! Which family publication a composed name read answers from (TYR-36 step 7b). A composed read
//! serves only from a servable marker: `live`, written by this interpreter build and on the
//! readable lineage, the publication fence's rule (snapshot_selection/project.rs). A read checks
//! the markers of the chains it was asked about even when it finds no name to compose, so a
//! rebuild answers stale rather than an empty or missing result, and it never composes names of
//! another namespace, whose marker it was not asked about.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use bigname_storage::{
    ChainPositions, NameCurrentExpiringFilter, NameCurrentListOrder, SnapshotProjectionRead,
    SnapshotSelectionErrorKind,
    families::name::{
        is_publication_unavailable, load_family_bound_names, load_family_expiring_page,
        load_family_name,
    },
    load_name_current_for_snapshot,
};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use support::{CHAIN, Event, Fixture, hash, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const BASE: &str = "base-sepolia";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// Name 1 bound and granted until 2_000_000_000, published at 12.
async fn registered(fixture: &Fixture) -> Result<()> {
    let lease = uuid(1);
    fixture
        .binding(&uuid(100), &name(1), &lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    ensure!(load_family_name(&fixture.pool, &name(1)).await?.is_some());
    Ok(())
}

async fn set_marker_state(fixture: &Fixture, chain: &str, state: &str) -> Result<()> {
    sqlx::query("UPDATE project_family_marker SET state = $2 WHERE chain_id = $1")
        .bind(chain)
        .bind(state)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

fn expiring_from(seconds: i64) -> Result<NameCurrentExpiringFilter> {
    Ok(NameCurrentExpiringFilter {
        namespace: "ens".to_owned(),
        expires_after: Some(OffsetDateTime::from_unix_timestamp(seconds)?),
        expires_before: None,
    })
}

/// A marker whose block a reorg orphaned is not servable, as the fence reads it.
#[tokio::test]
async fn a_marker_on_an_orphaned_block_serves_no_composed_row() -> Result<()> {
    let fixture = Fixture::new("families_name_orphaned_marker", 20).await?;
    registered(&fixture).await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 12",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    let read = load_family_name(&fixture.pool, &name(1)).await;
    assert!(
        read.as_ref().is_err_and(is_publication_unavailable),
        "a marker on an orphaned block must not serve: {read:?}"
    );
    fixture.cleanup().await
}

/// An exact-name read of a name with no surface during a rebuild is stale, not not-found: the
/// name may be one the rebuild has yet to reach.
#[tokio::test]
async fn an_unknown_name_during_a_rebuild_is_stale() -> Result<()> {
    let fixture = Fixture::new("families_name_unknown_rebuild", 20).await?;
    registered(&fixture).await?;
    let positions = ChainPositions::from_value(&json!({
        "ethereum": {"chain_id": CHAIN, "block_number": 12, "block_hash": hash(12),
                     "timestamp": "2027-01-15T08:02:24Z"}
    }))
    .map_err(|error| anyhow::anyhow!(error.message().to_owned()))?;
    let unknown = name(99);
    let read = || load_name_current_for_snapshot(&fixture.pool, &unknown, &positions);
    assert!(
        matches!(read().await, Ok(SnapshotProjectionRead::NotFound)),
        "a live marker answers not found"
    );
    set_marker_state(&fixture, CHAIN, "bootstrap_pending").await?;
    let during = read().await;
    assert!(
        during
            .as_ref()
            .is_err_and(|error| error.kind() == SnapshotSelectionErrorKind::Stale),
        "an unknown name during a rebuild must be stale: {during:?}"
    );
    fixture.cleanup().await
}

/// A bound-name page of a resolver with no names still reads its chain's marker, so a rebuild
/// answers stale rather than a resolver with no names.
#[tokio::test]
async fn an_empty_bound_name_walk_during_a_rebuild_is_unavailable() -> Result<()> {
    let fixture = Fixture::new("families_name_bound_rebuild", 20).await?;
    registered(&fixture).await?;
    let resolver = "0x00000000000000000000000000000000000000d9";
    let live = load_family_bound_names(&fixture.pool, CHAIN, resolver, None, None, 10).await?;
    ensure!(live.is_empty(), "the resolver has no names: {live:?}");
    set_marker_state(&fixture, CHAIN, "bootstrap_pending").await?;
    let during = load_family_bound_names(&fixture.pool, CHAIN, resolver, None, None, 10).await;
    assert!(
        during.as_ref().is_err_and(is_publication_unavailable),
        "an empty walk during a rebuild must be unavailable: {during:?}"
    );
    fixture.cleanup().await
}

/// The ENS expiring page (its rows and continuation).
async fn ens_expiring(fixture: &Fixture) -> Result<(Vec<String>, String)> {
    let page = load_family_expiring_page(
        &fixture.pool,
        &expiring_from(1_900_000_000)?,
        NameCurrentListOrder::Asc,
        None,
        10,
        &[CHAIN.to_owned()],
    )
    .await?;
    Ok((
        page.rows
            .iter()
            .map(|row| row.row.logical_name_id.clone())
            .collect(),
        format!("{:?}", page.next_cursor),
    ))
}

/// A Basenames name on `BASE`, granted with `expiry` (a JSON number) on its own lease.
async fn basenames_grant(fixture: &Fixture, n: u64, expiry: serde_json::Value) -> Result<()> {
    let other = format!("basenames:{}", node(n));
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'basenames', $1, ARRAY[$1], '\\x00', $2, ARRAY[$2], 'ensip15', 'active',
                 $3, $4, 0, 'canonical')",
    )
    .bind(&other)
    .bind(node(n))
    .bind(BASE)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    let lease = uuid(u32::try_from(n)?);
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 0, 'canonical')",
    )
    .bind(&lease)
    .bind(BASE)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    let identity = format!("base-grant-{n}");
    fixture
        .event(
            Event::new(
                &identity,
                10,
                i64::try_from(n)?,
                "RegistrationGranted",
                V1_REGISTRAR,
            )
            .on(BASE)
            .name(&other)
            .resource(&lease)
            .after(
                json!({"authority_kind": "registrar", "status": "registered",
                              "registrant": OWNER, "expiry": expiry}),
            ),
        )
        .await?;
    Ok(())
}

/// An ENS expiring page composes only ENS names: Basenames names on another chain whose families
/// are rebuilding, one with an integral expiry the walk places and one with an inexact expiry
/// every page considers, are neither composed nor able to refuse the ENS page, which is the same
/// with or without them.
#[tokio::test]
async fn an_expiring_page_ignores_another_namespaces_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_name_expiring_namespace", 20).await?;
    registered(&fixture).await?;
    let alone = ens_expiring(&fixture).await?;
    assert_eq!(alone.0, [name(1)]);
    fixture.lineage(BASE, 20).await?;
    basenames_grant(&fixture, 2, json!(2_000_000_100u64)).await?;
    basenames_grant(&fixture, 3, json!(2_000_000_200.5)).await?;
    fixture.apply_on(BASE, 12).await?;
    let (integral, inexact): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE expiry_seconds IS NOT NULL),
                count(*) FILTER (WHERE expiry_seconds IS NULL
                                   AND jsonb_typeof(expiry) = 'number')
         FROM project_lifecycle_event WHERE chain_id = $1",
    )
    .bind(BASE)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        integral > 0 && inexact > 0,
        "the Basenames names reach both walks: integral {integral}, inexact {inexact}"
    );
    set_marker_state(&fixture, BASE, "bootstrap_pending").await?;
    assert_eq!(ens_expiring(&fixture).await?, alone);
    fixture.cleanup().await
}

/// An exact-name read of a name with no composed row at a position below the publication is
/// stale, as a composed row is: the name may have existed there, and no older position is kept to
/// say it did not.
#[tokio::test]
async fn an_unknown_name_below_the_publication_is_stale() -> Result<()> {
    let fixture = Fixture::new("families_name_unknown_below", 20).await?;
    registered(&fixture).await?;
    let position = |block: i64, timestamp: &str| {
        ChainPositions::from_value(&json!({
            "ethereum": {"chain_id": CHAIN, "block_number": block, "block_hash": hash(block),
                         "timestamp": timestamp}
        }))
        .map_err(|error| anyhow::anyhow!(error.message().to_owned()))
    };
    let unknown = name(99);
    let mut answers = Vec::new();
    for positions in [
        position(12, "2027-01-15T08:02:24Z")?,
        position(11, "2027-01-15T08:02:12Z")?,
    ] {
        let read = load_name_current_for_snapshot(&fixture.pool, &unknown, &positions).await;
        answers.push(match read {
            Ok(SnapshotProjectionRead::NotFound) => "not found".to_owned(),
            Ok(_) => "found".to_owned(),
            Err(error) => format!("{:?}", error.kind()),
        });
    }
    assert_eq!(answers, ["not found", "Stale"]);
    fixture.cleanup().await
}
