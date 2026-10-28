//! The registration of an ENSv1 `.eth` second-level name whose selected authority records no
//! lease (TYR-405). The January 2020 registrar migration moved such names onto the
//! BaseRegistrar with no controller `NameRegistered`, so no grant opens their lease. Their first
//! controller renewal names it, with a grant the adapter derives there. The name is selected
//! through a registry-only binding that stands for no lease, or through no binding when its
//! registry owner is the zero address. Each test is one input class of the implied lease
//! (`docs/projections.md`, registry-only bindings), plus the cut-over registrar lease that an
//! earlier, replaced reservation left undated.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use bigname_storage::families::name::load_family_name;
use serde_json::{Value, json};
use support::{
    CHAIN, Event, Fixture,
    ens_v2_path::{declared_path, surface},
    uuid,
};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const V2_REGISTRY: &str = "0x00000000000000000000000000000000000000e6";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const HOLDER: &str = "0x00000000000000000000000000000000000000bb";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const DAY: i64 = 86_400;
const LEASE_EXPIRY: i64 = 2_000_000_000;
const RESERVED_EXPIRY: i64 = LEASE_EXPIRY + 62 * DAY;

/// The block time `Fixture::lineage` gives `block`, in Unix seconds.
fn block_time(block: i64) -> i64 {
    1_800_000_000 + block * 12
}

fn node(logical_name_id: &str) -> &str {
    logical_name_id.trim_start_matches("ens:")
}

/// The 2020 handoff of `name`: its first current-registry record at `block`, which opens a
/// registry-only binding `n` on the registry resource with no lease recorded. With `old`, the
/// record is the 2017 registry's instead and the node has no current-registry record.
async fn handoff(fixture: &Fixture, name: &str, n: u32, block: i64, old: bool) -> Result<String> {
    let resource = uuid(0x3000 + n);
    let log = i64::from(n) * 10;
    fixture
        .binding(&uuid(100 + n), name, &resource, "ens_v1", block, log, None)
        .await?;
    fixture
        .write(
            block,
            log,
            "SurfaceBound",
            "ens_v1_registry_l1",
            Some(name),
            Some(&resource),
            json!({"authority_kind": "registry_only", "state_derived": true, "owner": OWNER}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            log + 1,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(name),
            Some(&resource),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    owner_write(fixture, name, &resource, block, log + 2, OWNER, old).await?;
    Ok(resource)
}

async fn owner_write(
    fixture: &Fixture,
    name: &str,
    resource: &str,
    block: i64,
    log: i64,
    owner: &str,
    old: bool,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            Some(name),
            Some(resource),
            json!({"node": node(name), "owner": owner, "owner_getter": owner,
                   "emitter_role": if old { "registry_old" } else { "registry" }}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

/// A controller renewal of `name`'s lease `lease` at `block`, as the adapter writes it: the
/// first renewal of a lease no grant opened also carries the grant derived beside it, whose
/// registrant is the zero address.
async fn renewal(
    fixture: &Fixture,
    name: &str,
    lease: &str,
    block: i64,
    log: i64,
    expiry: Value,
    first: bool,
) -> Result<()> {
    let after = json!({"authority_kind": "registrar", "source_event": "NameRenewed",
                       "registrant": ZERO, "expiry": expiry, "namehash": node(name)});
    let kinds: &[&str] = if first {
        &["RegistrationGranted", "RegistrationRenewed", "ExpiryChanged"]
    } else {
        &["RegistrationRenewed", "ExpiryChanged"]
    };
    for (offset, kind) in kinds.iter().enumerate() {
        fixture
            .write(
                block,
                log + offset as i64,
                kind,
                "ens_v1_registrar_l1",
                Some(name),
                Some(lease),
                after.clone(),
                REGISTRAR,
            )
            .await?;
    }
    Ok(())
}

/// The lease's token moved to `to` without `reclaim`.
async fn transfer(
    fixture: &Fixture,
    name: &str,
    lease: &str,
    block: i64,
    log: i64,
    to: &str,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            Some(name),
            Some(lease),
            json!({"authority_kind": "registrar", "source_event": "Transfer", "to": to,
                   "namehash": node(name)}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// The release the interpreter settles from the block past the lease's grace.
async fn release(
    fixture: &Fixture,
    name: &str,
    lease: &str,
    block: i64,
    expiry: Value,
    holder: &str,
) -> Result<()> {
    let identity = format!("release-{lease}-{block}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                0,
                "RegistrationReleased",
                "ens_v1_registrar_l1",
            )
            .name(name)
            .resource(lease)
            .synthesised()
            .before(json!({"registrant": holder}))
            .after(json!({"expiry": expiry, "released_at": block_time(block),
                          "source_event": "RegistrationReleased"}))
            .raw(json!({"kind": "raw_block", "emitting_address": REGISTRAR})),
        )
        .await?;
    Ok(())
}

/// A premigration-shaped reservation of `name` on ENSv2 entry `entry`.
async fn reserved(
    fixture: &Fixture,
    name: &str,
    entry: &str,
    block: i64,
    log: i64,
    expiry: i64,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "RegistrationReserved",
            "ens_v2_registry_l1",
            Some(name),
            Some(entry),
            json!({"registry_contract_instance_id": "eth", "token_id": "1",
                   "source_event": "LabelReserved", "status": "reserved", "expiry": expiry}),
            V2_REGISTRY,
        )
        .await?;
    Ok(())
}

/// The registration block, the control owner and the summary row of a composed name.
async fn served(fixture: &Fixture, name: &str) -> Result<Value> {
    let row = load_family_name(&fixture.pool, name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{name} has no composed row"))?;
    let registration = &row.declared_summary["registration"];
    let field = |key: &str| registration.get(key).cloned().unwrap_or(Value::Null);
    let summary: (Option<String>, Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT registration_status, expires_at::bigint, grace_ends_at::bigint,
                extract(epoch FROM registered_at)::bigint
         FROM project_name_summary WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name)
    .fetch_one(&fixture.pool)
    .await?;
    Ok(json!({
        "status": summary.0,
        "expires_at": summary.1,
        "grace_ends_at": summary.2,
        "registered_at": summary.3,
        "resource_id": field("resource_id"),
        "identity_resource_id": field("identity_resource_id"),
        "ens_v1_expiry": field("ens_v1_expiry"),
        "authority_kind": field("authority_kind"),
        "lifecycle_status": field("lifecycle_status"),
        "lapsed_owner": registration.pointer("/lapsed_registration/owner").cloned().unwrap_or(Value::Null),
        "owner": row.declared_summary.pointer("/control/owner").cloned().unwrap_or(Value::Null),
    }))
}

fn check(row: &Value, expected: Value) -> Result<()> {
    for (key, value) in expected.as_object().expect("expected is an object") {
        ensure!(&row[key] == value, "{key}: {} != {value} in {row}", row[key]);
    }
    Ok(())
}

/// Shapes A on a chain with no ENSv2 root, and a lease with no handoff block: renewals after
/// the handoff serve their lease, its expiry and the handoff time; with no current-registry
/// record the lease's first row dates it. A reorg across the first renewal restores the
/// unleased row, and the schedule turns expired at the expiry.
#[tokio::test]
async fn a_renewed_migrated_name_serves_its_implied_lease() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_renewed", 10).await?;
    let coathup = surface(&fixture, "coathup.eth").await?;
    let lease = uuid(0x1001);
    handoff(&fixture, &coathup, 1, 1, false).await?;
    renewal(&fixture, &coathup, &lease, 3, 1, json!(LEASE_EXPIRY), true).await?;
    // The second renewal expires exactly at block 8's time.
    let expiry = block_time(8);
    renewal(&fixture, &coathup, &lease, 5, 1, json!(expiry), false).await?;

    let undated = surface(&fixture, "undated.eth").await?;
    let undated_lease = uuid(0x1002);
    handoff(&fixture, &undated, 2, 1, true).await?;
    renewal(
        &fixture,
        &undated,
        &undated_lease,
        3,
        11,
        json!(LEASE_EXPIRY),
        true,
    )
    .await?;

    // Undoing block 3 takes the lease away again.
    fixture.assert_undo_restores(3).await?;
    fixture.apply(7, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &coathup).await?,
        json!({"status": "active", "registered_at": block_time(1), "resource_id": lease,
               "ens_v1_expiry": expiry.to_string(), "expires_at": expiry,
               "grace_ends_at": expiry + 90 * DAY, "authority_kind": "registry_only",
               "owner": OWNER}),
    )?;
    check(
        &served(&fixture, &undated).await?,
        json!({"status": "active", "registered_at": block_time(3),
               "resource_id": undated_lease, "ens_v1_expiry": LEASE_EXPIRY.to_string()}),
    )?;
    fixture.apply(8, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &coathup).await?,
        json!({"status": "expired", "registered_at": block_time(1)}),
    )?;
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

/// Shape A on a cut-over chain: the reservation supplies the dates, and the implied lease is
/// the identity the reservation continues.
#[tokio::test]
async fn a_renewed_migrated_name_with_a_reservation_keeps_its_lease_identity() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_reserved", 8).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let name = surface(&fixture, "coathup.eth").await?;
    let lease = uuid(0x1001);
    handoff(&fixture, &name, 1, 1, false).await?;
    renewal(&fixture, &name, &lease, 3, 1, json!(LEASE_EXPIRY), true).await?;
    reserved(&fixture, &name, &uuid(0x2001), 5, 1, RESERVED_EXPIRY).await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "active", "registered_at": block_time(1), "resource_id": lease,
               "identity_resource_id": lease, "ens_v1_expiry": LEASE_EXPIRY.to_string(),
               "expires_at": RESERVED_EXPIRY, "owner": OWNER}),
    )?;
    fixture.assert_rebuild_equal(6).await?;
    fixture.cleanup().await
}

/// Shape B: a renewed lease whose token moved and that then lapsed past grace is released with
/// its holder; a release with no named renewal before it implies its lease the same way.
#[tokio::test]
async fn a_lapsed_implied_lease_is_released_with_its_holder() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_released", 8).await?;
    let lapsed = block_time(4) - 90 * DAY - 1;
    let mijin = surface(&fixture, "mijin.eth").await?;
    let lease = uuid(0x1001);
    handoff(&fixture, &mijin, 1, 1, false).await?;
    renewal(&fixture, &mijin, &lease, 2, 1, json!(lapsed), true).await?;
    transfer(&fixture, &mijin, &lease, 3, 1, HOLDER).await?;
    release(&fixture, &mijin, &lease, 4, json!(lapsed), HOLDER).await?;

    let quiet = surface(&fixture, "quiet.eth").await?;
    let quiet_lease = uuid(0x1002);
    handoff(&fixture, &quiet, 2, 1, false).await?;
    release(&fixture, &quiet, &quiet_lease, 4, json!(lapsed), ZERO).await?;

    fixture.apply(5, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &mijin).await?,
        json!({"status": "released", "registered_at": block_time(1), "resource_id": lease,
               "expires_at": lapsed, "ens_v1_expiry": lapsed.to_string(),
               "lapsed_owner": HOLDER, "owner": Value::Null}),
    )?;
    check(
        &served(&fixture, &quiet).await?,
        json!({"status": "released", "registered_at": block_time(1),
               "resource_id": quiet_lease, "expires_at": lapsed, "lapsed_owner": Value::Null}),
    )?;
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

/// A release of an implied lease that carries no expiry is still terminal.
#[tokio::test]
async fn a_release_without_an_expiry_releases_the_implied_lease() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_no_expiry", 6).await?;
    let name = surface(&fixture, "noexpiry.eth").await?;
    let lease = uuid(0x1001);
    handoff(&fixture, &name, 1, 1, false).await?;
    renewal(&fixture, &name, &lease, 2, 1, Value::Null, true).await?;
    release(&fixture, &name, &lease, 3, Value::Null, ZERO).await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "released", "lifecycle_status": "released",
               "registered_at": block_time(1), "resource_id": lease,
               "expires_at": Value::Null, "grace_ends_at": Value::Null}),
    )?;
    fixture.assert_rebuild_equal(4).await?;
    fixture.cleanup().await
}

/// Shape B with a reservation: the lease lapsed after premigration reserved it. The continued
/// lease keeps its release, holder and handoff time.
#[tokio::test]
async fn a_reserved_implied_lease_that_lapsed_keeps_its_release() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_reserved_released", 7).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let lapsed = block_time(4) - 90 * DAY - 1;
    let name = surface(&fixture, "lapsed.eth").await?;
    let lease = uuid(0x1001);
    handoff(&fixture, &name, 1, 1, false).await?;
    renewal(&fixture, &name, &lease, 2, 1, json!(lapsed), true).await?;
    transfer(&fixture, &name, &lease, 2, 5, HOLDER).await?;
    reserved(&fixture, &name, &uuid(0x2001), 3, 1, lapsed + 62 * DAY).await?;
    release(&fixture, &name, &lease, 4, json!(lapsed), HOLDER).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "released", "registered_at": block_time(1),
               "identity_resource_id": lease, "lapsed_owner": HOLDER,
               "expires_at": lapsed + 62 * DAY}),
    )?;
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

/// Shape D: the registry owner was set to the zero address after the handoff, so no binding is
/// open, while the renewed lease is alive. The name serves that lease, with no authority kind.
#[tokio::test]
async fn an_ownerless_registry_record_serves_its_live_implied_lease() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_ownerless", 7).await?;
    let name = surface(&fixture, "pitchbookdata.eth").await?;
    let lease = uuid(0x1001);
    let registry = uuid(0x3001);
    fixture
        .binding(&uuid(101), &name, &registry, "ens_v1", 1, 10, Some(4))
        .await?;
    fixture
        .write(
            1,
            10,
            "SurfaceBound",
            "ens_v1_registry_l1",
            Some(&name),
            Some(&registry),
            json!({"authority_kind": "registry_only", "state_derived": true, "owner": OWNER}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            1,
            11,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name),
            Some(&registry),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    owner_write(&fixture, &name, &registry, 1, 12, OWNER, false).await?;
    renewal(&fixture, &name, &lease, 2, 1, json!(LEASE_EXPIRY), true).await?;
    transfer(&fixture, &name, &lease, 3, 1, HOLDER).await?;
    owner_write(&fixture, &name, &registry, 4, 1, ZERO, false).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "active", "registered_at": block_time(1), "resource_id": lease,
               "ens_v1_expiry": LEASE_EXPIRY.to_string(), "expires_at": LEASE_EXPIRY,
               "authority_kind": Value::Null, "owner": HOLDER}),
    )?;
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

/// A registrar binding of `name` on `lease` opened at `block`, granted by a controller at
/// `block + 1`.
async fn registered(fixture: &Fixture, name: &str, n: u32, lease: &str, block: i64) -> Result<()> {
    let log = i64::from(n) * 10;
    fixture
        .binding(&uuid(100 + n), name, lease, "ens_v1", block, log, None)
        .await?;
    fixture
        .write(
            block,
            log,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(name),
            Some(lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            block + 1,
            log,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(name),
            Some(lease),
            json!({"authority_kind": "registrar", "source_event": "NameRegistered",
                   "status": "registered", "registrant": OWNER, "expiry": LEASE_EXPIRY}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// Shape C: a cut-over registrar lease whose premigration reservation was replaced by a later
/// one keeps its grant's time and identity. An ENSv2 registration between the lease and the
/// current entry still ends the lease's registration.
#[tokio::test]
async fn a_replaced_reservation_does_not_undate_the_lease_it_stood_for() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_rereserved", 8).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let kaaboosd = surface(&fixture, "kaaboosd.eth").await?;
    let lease = uuid(0x1001);
    registered(&fixture, &kaaboosd, 1, &lease, 1).await?;
    reserved(&fixture, &kaaboosd, &uuid(0x2001), 3, 10, RESERVED_EXPIRY - DAY).await?;
    reserved(&fixture, &kaaboosd, &uuid(0x2001), 5, 10, RESERVED_EXPIRY).await?;

    let taken = surface(&fixture, "taken.eth").await?;
    let taken_lease = uuid(0x1002);
    registered(&fixture, &taken, 2, &taken_lease, 1).await?;
    fixture
        .write(
            3,
            20,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&taken),
            Some(&uuid(0x2102)),
            json!({"registry_contract_instance_id": "eth", "token_id": "2",
                   "source_event": "LabelRegistered", "status": "registered",
                   "registrant": HOLDER, "expiry": RESERVED_EXPIRY}),
            V2_REGISTRY,
        )
        .await?;
    reserved(&fixture, &taken, &uuid(0x2002), 5, 20, RESERVED_EXPIRY).await?;

    fixture.apply(6, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &kaaboosd).await?,
        json!({"status": "active", "registered_at": block_time(2), "resource_id": lease,
               "identity_resource_id": lease, "ens_v1_expiry": LEASE_EXPIRY.to_string(),
               "expires_at": RESERVED_EXPIRY}),
    )?;
    check(
        &served(&fixture, &taken).await?,
        json!({"registered_at": Value::Null, "identity_resource_id": Value::Null}),
    )?;
    fixture.assert_rebuild_equal(6).await?;
    fixture.cleanup().await
}
