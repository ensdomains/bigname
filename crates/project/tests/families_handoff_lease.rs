//! The registration of an ENSv1 `.eth` second-level name whose selected authority records no
//! lease (TYR-405). The January 2020 registrar migration moved such names onto the
//! BaseRegistrar with no controller `NameRegistered`, so no grant opens their lease. Their first
//! controller renewal names it, with a grant the adapter derives there. The name is selected
//! through a registry-only binding that stands for no lease. Each test is one input class of
//! the implied lease
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
        &[
            "RegistrationGranted",
            "RegistrationRenewed",
            "ExpiryChanged",
        ]
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
    fixture.resource(lease).await?;
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

/// The ENSv2 registry a reservation is written on: the contract instance its events name and the
/// address that emits them.
#[derive(Clone, Copy)]
struct Registry<'a> {
    instance: &'a str,
    address: &'a str,
}

/// The generic test registry. The registrar grace policy (`lifecycle/policy.rs`, `load`) keeps
/// only registry identifiers that parse as a Uuid. `eth` does not, so `registries` never matches
/// it and it gets no grace.
const GENERIC: Registry<'static> = Registry {
    instance: "eth",
    address: V2_REGISTRY,
};

/// A premigration-shaped reservation of `name` on ENSv2 entry `entry`.
async fn reserved(
    fixture: &Fixture,
    name: &str,
    entry: &str,
    block: i64,
    log: i64,
    expiry: i64,
) -> Result<()> {
    reserved_on(fixture, GENERIC, name, entry, block, log, expiry).await
}

/// `reserved` on `registry`.
async fn reserved_on(
    fixture: &Fixture,
    registry: Registry<'_>,
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
            json!({"registry_contract_instance_id": registry.instance, "token_id": "1",
                   "source_event": "LabelReserved", "status": "reserved", "expiry": expiry}),
            registry.address,
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
        "registrant": field("registrant"),
        "owner": row.declared_summary.pointer("/control/owner").cloned().unwrap_or(Value::Null),
    }))
}

fn check(row: &Value, expected: Value) -> Result<()> {
    for (key, value) in expected.as_object().expect("expected is an object") {
        ensure!(
            &row[key] == value,
            "{key}: {} != {value} in {row}",
            row[key]
        );
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

/// Shape B with a reservation: the lease was in its grace period when premigration reserved it,
/// so the reservation's expiry had already passed when it was written, and the lease then lapsed.
/// A reservation already expired when written is never live, so the lease keeps its own expiry,
/// release, holder and handoff time.
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
               "expires_at": lapsed, "grace_ends_at": lapsed + 90 * DAY}),
    )?;
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

/// A registrar binding of `name` on `lease` opened at `block`, granted by a controller at
/// `block + 1` until `LEASE_EXPIRY`.
async fn registered(fixture: &Fixture, name: &str, n: u32, lease: &str, block: i64) -> Result<()> {
    registered_until(fixture, name, n, lease, block, LEASE_EXPIRY).await
}

/// `registered`, with the grant's expiry.
async fn registered_until(
    fixture: &Fixture,
    name: &str,
    n: u32,
    lease: &str,
    block: i64,
    expiry: i64,
) -> Result<()> {
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
                   "status": "registered", "registrant": OWNER, "expiry": expiry}),
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
    reserved(
        &fixture,
        &kaaboosd,
        &uuid(0x2001),
        3,
        10,
        RESERVED_EXPIRY - DAY,
    )
    .await?;
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

/// Shape C as kaaboosd.eth's own events order it on Ethereum Mainnet. Premigration reserved the
/// name while its lease was in grace, so that reservation was already expired when written, and
/// the interpreter derived its release in the same block. The lease was then renewed through the
/// ENSv2 renewal bridge, and premigration reserved the name again. A reservation already expired
/// when written is never live (`docs/architecture.md`, authority selection), so the ENSv1 lease
/// stays the registration throughout: before the second reservation with its own renewed expiry,
/// and after it with its grant's time and identity.
#[tokio::test]
async fn a_reservation_replaced_after_lapsing_leaves_the_renewed_lease_registered() -> Result<()> {
    const GRANTED_UNTIL: i64 = 1_785_656_670;
    const FIRST_RESERVED_UNTIL: i64 = 1_791_013_470;
    const RENEWED_UNTIL: i64 = 1_817_192_670;
    const SECOND_RESERVED_UNTIL: i64 = 1_822_549_470;
    let fixture = Fixture::new("families_handoff_lease_lapsed_reservation", 8).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let kaaboosd = surface(&fixture, "kaaboosd.eth").await?;
    let lease = uuid(0x1001);
    let entry = uuid(0x2001);
    registered_until(&fixture, &kaaboosd, 1, &lease, 1, GRANTED_UNTIL).await?;
    reserved(&fixture, &kaaboosd, &entry, 3, 152, FIRST_RESERVED_UNTIL).await?;
    fixture
        .event(
            Event::new(
                "kaaboosd-first-reservation-expired",
                3,
                0,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .name(&kaaboosd)
            .resource(&entry)
            .synthesised()
            .before(json!({"status": "reserved", "expiry": FIRST_RESERVED_UNTIL,
                           "registrant": Value::Null}))
            .after(json!({"source_event": "RegistryPathExpired",
                          "derived_from": "interpreter_state",
                          "terminal_reason": "registry_name_binding_expired",
                          "registry": V2_REGISTRY, "token_id": "1",
                          "registry_contract_instance_id": "eth",
                          "expiry": FIRST_RESERVED_UNTIL, "status": "released",
                          "released_at": block_time(3)}))
            .raw(json!({"kind": "raw_block", "emitting_address": V2_REGISTRY})),
        )
        .await?;
    fixture
        .write(
            5,
            694,
            "RegistrationRenewed",
            "ens_v2_migration_l1",
            Some(&kaaboosd),
            None,
            json!({"source_event": "NameRenewed", "token_id": "1",
                   "namehash": node(&kaaboosd), "expiry": RENEWED_UNTIL}),
            V2_REGISTRY,
        )
        .await?;
    fixture
        .write(
            5,
            695,
            "RegistrationRenewed",
            "ens_v1_registrar_l1",
            Some(&kaaboosd),
            Some(&lease),
            json!({"authority_kind": "registrar", "source_event": "NameRenewed",
                   "registrant": OWNER, "expiry": RENEWED_UNTIL,
                   "namehash": node(&kaaboosd)}),
            REGISTRAR,
        )
        .await?;
    reserved(&fixture, &kaaboosd, &entry, 7, 110, SECOND_RESERVED_UNTIL).await?;

    // Between the reservations the derived release ends nothing: the renewed lease serves its own
    // expiry and the ENSv1 grace period.
    fixture.apply(6, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &kaaboosd).await?,
        json!({"status": "active", "lifecycle_status": "active",
               "registered_at": block_time(2), "resource_id": lease,
               "ens_v1_expiry": RENEWED_UNTIL.to_string(), "expires_at": RENEWED_UNTIL,
               "grace_ends_at": RENEWED_UNTIL + 90 * DAY, "owner": OWNER}),
    )?;
    // The second reservation dates the lease it stands for.
    fixture.apply(8, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &kaaboosd).await?,
        json!({"status": "active", "lifecycle_status": "active",
               "registered_at": block_time(2), "resource_id": lease,
               "identity_resource_id": lease, "ens_v1_expiry": RENEWED_UNTIL.to_string(),
               "expires_at": SECOND_RESERVED_UNTIL, "owner": OWNER}),
    )?;
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

/// What `lapsing_reservation` wrote: the name, its lease and the two expiries.
struct Lapsing {
    name: String,
    lease: String,
    reserved_until: i64,
    lease_expiry: i64,
}

/// Row 21's fixture: a migrated name's implied lease, renewed at block 3 to 62 days before block
/// 7's time, and a premigration reservation on `registry` written live at block 5 until block 7's
/// time, with the path-expiry release Interpret writes at block 7.
async fn lapsing_reservation(fixture: &Fixture, registry: Registry<'_>) -> Result<Lapsing> {
    let name = surface(fixture, "graced.eth").await?;
    let lease = uuid(0x1001);
    let entry = uuid(0x2001);
    let reserved_until = block_time(7);
    let lease_expiry = reserved_until - 62 * DAY;
    handoff(fixture, &name, 1, 1, false).await?;
    renewal(fixture, &name, &lease, 3, 1, json!(lease_expiry), true).await?;
    reserved_on(fixture, registry, &name, &entry, 5, 1, reserved_until).await?;
    fixture
        .event(
            Event::new(
                "graced-reservation-expired",
                7,
                0,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .name(&name)
            .resource(&entry)
            .synthesised()
            .before(json!({"status": "reserved", "expiry": reserved_until,
                           "registrant": Value::Null}))
            .after(json!({"source_event": "RegistryPathExpired",
                          "derived_from": "interpreter_state",
                          "terminal_reason": "registry_name_binding_expired",
                          "registry": registry.address, "token_id": "1",
                          "registry_contract_instance_id": registry.instance,
                          "expiry": reserved_until, "status": "released",
                          "released_at": block_time(7)}))
            .raw(json!({"kind": "raw_block", "emitting_address": registry.address})),
        )
        .await?;
    Ok(Lapsing {
        name,
        lease,
        reserved_until,
        lease_expiry,
    })
}

/// Row 21 on the generic test registry. A reservation live when written that lapses later, on a
/// migrated name's implied lease. Only a reservation already expired when written is set aside,
/// so this one is the canonical allocation: while live and after it lapses the name serves its
/// expiry, not the lease's, and the path-expiry release changes nothing. The ENSv1 registry-only
/// binding stays selected and the lease keeps dating it. Grace belongs to a reviewed ETHRegistry
/// deployment (`lifecycle/policy.rs`), and this registry is not one, so the grace ends at the
/// expiry, as `families_expiry_grace.rs` pins for the same registry. The release at the expiry
/// follows from the exclusive boundary the `schedule()` unit test in `canonical.rs` pins.
#[tokio::test]
async fn a_live_reservation_that_lapses_later_keeps_its_own_schedule() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_live_reservation", 8).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let Lapsing {
        name,
        lease,
        reserved_until,
        lease_expiry,
    } = lapsing_reservation(&fixture, GENERIC).await?;

    fixture.apply(6, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "active", "lifecycle_status": "active",
               "registered_at": block_time(1), "identity_resource_id": lease,
               "authority_kind": "registry_only",
               "ens_v1_expiry": lease_expiry.to_string(), "expires_at": reserved_until,
               "grace_ends_at": reserved_until}),
    )?;
    fixture.apply(8, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "released", "lifecycle_status": "released",
               "registered_at": block_time(1), "identity_resource_id": lease,
               "authority_kind": "registry_only",
               "ens_v1_expiry": lease_expiry.to_string(), "expires_at": reserved_until,
               "grace_ends_at": reserved_until}),
    )?;
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

/// Row 21 on the reviewed Sepolia ETHRegistry, a deployment `lifecycle/policy.rs` lists, where
/// premigration writes its reservations. The policy gives it the reviewed registrar's 28-day
/// grace, so after the lapse at E the name is `expired` until E + 28 days and `released` from
/// then, the exclusive boundary. Premigration reserves a name until its lease's expiry plus 62
/// days, so that grace ends with the lease's 90-day ENSv1 grace. Selection, identity and expiry
/// are the generic registry's.
#[tokio::test]
async fn a_live_reservation_on_the_reviewed_registry_lapses_into_its_grace() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_reviewed_reservation", 8).await?;
    let instance = declared_path::reviewed_root_eth_entry(&fixture).await?;
    let registry = Registry {
        instance: &instance,
        address: declared_path::ETH,
    };
    let Lapsing {
        name,
        lease,
        reserved_until,
        lease_expiry,
    } = lapsing_reservation(&fixture, registry).await?;
    let grace_ends = reserved_until + 28 * DAY;

    fixture.apply(6, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "active", "lifecycle_status": "active",
               "registered_at": block_time(1), "identity_resource_id": lease,
               "authority_kind": "registry_only",
               "ens_v1_expiry": lease_expiry.to_string(), "expires_at": reserved_until,
               "grace_ends_at": grace_ends}),
    )?;
    fixture.apply(8, FamilyMode::Normal).await?;
    let lapsed = served(&fixture, &name).await?;
    check(
        &lapsed,
        json!({"status": "expired", "lifecycle_status": "expired",
               "registered_at": block_time(1), "identity_resource_id": lease,
               "authority_kind": "registry_only",
               "ens_v1_expiry": lease_expiry.to_string(), "expires_at": reserved_until,
               "grace_ends_at": grace_ends}),
    )?;
    ensure!(
        lapsed["grace_ends_at"] == json!(lease_expiry + 90 * DAY),
        "the reservation's grace ends with the lease's ENSv1 grace: {lapsed}"
    );
    // A block exactly at the grace end: the exclusive boundary releases the name there.
    fixture.block_at(9, grace_ends).await?;
    fixture.apply(9, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "released", "lifecycle_status": "released",
               "registered_at": block_time(1), "identity_resource_id": lease,
               "authority_kind": "registry_only",
               "ens_v1_expiry": lease_expiry.to_string(), "expires_at": reserved_until,
               "grace_ends_at": grace_ends}),
    )?;
    fixture.assert_rebuild_equal(9).await?;
    fixture.cleanup().await
}

/// A controller registration of lease `0x1000 + n` (`registered_until`, granted at block 2 until
/// 62 days before block 7's time), and a premigration reservation of it on `registry`, entry
/// `0x2000 + n` and token `n`, written live at block 5 until block 7's time. A BaseRegistrar
/// renewal at block 6 extends the lease a year, past the reservation, and no ENSv2
/// `ExpiryUpdated` follows before the path-expiry release Interpret writes at block 7. Returns
/// the name and its lease.
async fn renewed_past_reservation(
    fixture: &Fixture,
    registry: Registry<'_>,
    label: &str,
    n: u32,
) -> Result<(String, String)> {
    let name = surface(fixture, label).await?;
    let lease = uuid(0x1000 + n);
    let entry = uuid(0x2000 + n);
    let token = n.to_string();
    let log = i64::from(n) * 10;
    let reserved_until = block_time(7);
    let lease_expiry = reserved_until - 62 * DAY;
    registered_until(fixture, &name, n, &lease, 1, lease_expiry).await?;
    fixture
        .write(
            5,
            log,
            "RegistrationReserved",
            "ens_v2_registry_l1",
            Some(&name),
            Some(&entry),
            json!({"registry_contract_instance_id": registry.instance, "token_id": token,
                   "source_event": "LabelReserved", "status": "reserved",
                   "expiry": reserved_until}),
            registry.address,
        )
        .await?;
    renewal(
        fixture,
        &name,
        &lease,
        6,
        log,
        json!(lease_expiry + 365 * DAY),
        false,
    )
    .await?;
    fixture
        .event(
            Event::new(
                &format!("{label}-reservation-expired"),
                7,
                0,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .name(&name)
            .resource(&entry)
            .synthesised()
            .before(json!({"status": "reserved", "expiry": reserved_until,
                           "registrant": Value::Null}))
            .after(json!({"source_event": "RegistryPathExpired",
                          "derived_from": "interpreter_state",
                          "terminal_reason": "registry_name_binding_expired",
                          "registry": registry.address, "token_id": token,
                          "registry_contract_instance_id": registry.instance,
                          "expiry": reserved_until, "status": "released",
                          "released_at": block_time(7)}))
            .raw(json!({"kind": "raw_block", "emitting_address": registry.address})),
        )
        .await?;
    Ok((name, lease))
}

/// The late ENSv2 `ExpiryUpdated` of `renewed_past_reservation`'s reservation `n` at `block`, as
/// the adapter writes it for a reservation whose expiry had passed: the `ExpiryChanged` that
/// revives it, and the `RegistrationReserved` snapshot (`derived_from` `registry_state`) of the
/// name it is current for again (`v2_registry.rs` and `v2_registry/topology.rs` in the
/// adapters).
async fn expiry_updated(
    fixture: &Fixture,
    registry: Registry<'_>,
    name: &str,
    n: u32,
    block: i64,
    expiry: i64,
) -> Result<()> {
    let entry = uuid(0x2000 + n);
    let token = n.to_string();
    fixture
        .write(
            block,
            0,
            "ExpiryChanged",
            "ens_v2_registry_l1",
            Some(name),
            Some(&entry),
            json!({"source_event": "ExpiryUpdated", "token_id": token, "expiry": expiry,
                   "sender": OWNER, "revived_from_expiry": true,
                   "registry_contract_instance_id": registry.instance}),
            registry.address,
        )
        .await?;
    fixture
        .write(
            block,
            1,
            "RegistrationReserved",
            "ens_v2_registry_l1",
            Some(name),
            Some(&entry),
            json!({"source_event": "ExpiryUpdated", "status": "reserved",
                   "derived_from": "registry_state", "expiry": expiry, "token_id": token,
                   "current_token_id": token, "reservation_resource": true,
                   "registry_contract_instance_id": registry.instance}),
            registry.address,
        )
        .await?;
    Ok(())
}

/// The fields of a name ENSv1 selects through `renewed_past_reservation`'s lease `lease`,
/// registered at block 2.
fn on_lease(
    status: &str,
    authority_kind: &str,
    lease: &str,
    ens_v1_expiry: i64,
    expires_at: i64,
    grace_ends_at: i64,
) -> Value {
    json!({"status": status, "lifecycle_status": status, "registered_at": block_time(2),
           "identity_resource_id": lease, "authority_kind": authority_kind,
           "ens_v1_expiry": ens_v1_expiry.to_string(), "expires_at": expires_at,
           "grace_ends_at": grace_ends_at})
}

/// A controller-registered lease renewed past its reservation: a BaseRegistrar renewal extends
/// the lease after premigration reserved the name, and the reservation lapses before the matching
/// ENSv2 `ExpiryUpdated` arrives. The row pins the documented contract and passes on the base
/// branch.
/// Crossing an expiry "never switches a name on a cut-over chain back to a different
/// registration's dates" (`canonical.rs:1-3`). Past the cutover `expires_at` is the retained
/// canonical ENSv2 entry's expiry "whichever arm holds authority" (`docs/api-v1.md:56`), and the
/// reservation "supplies E/G through expiry and release", with "Passage of time never falls back
/// to the older lease" (`docs/api-v1.md:2166-2169`). So ENSv1 stays selected with the renewed
/// lease as `ens_v1_expiry`, and the reservation's expiry and grace set the status. On the
/// reviewed registry it is `expired` through the registrar's 28 days, then `released`. On the
/// generic registry it is `released` at the expiry. An `ExpiryUpdated` inside the grace renews
/// the reservation, and the name is `active` on the new expiry.
#[tokio::test]
async fn a_reservation_that_lapses_after_its_lease_renewed_keeps_its_own_schedule() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_renewed_past_reservation", 8).await?;
    let instance = declared_path::reviewed_root_eth_entry(&fixture).await?;
    let reviewed = Registry {
        instance: &instance,
        address: declared_path::ETH,
    };
    let (generic, generic_lease) =
        renewed_past_reservation(&fixture, GENERIC, "generic.eth", 1).await?;
    let (lapsed, lapsed_lease) =
        renewed_past_reservation(&fixture, reviewed, "lapsed.eth", 2).await?;
    let (updated, updated_lease) =
        renewed_past_reservation(&fixture, reviewed, "updated.eth", 3).await?;
    let reserved_until = block_time(7);
    let renewed_until = reserved_until - 62 * DAY + 365 * DAY;
    let grace_ends = reserved_until + 28 * DAY;
    let updated_until = renewed_until + 62 * DAY;

    // Before the reservation's expiry every name is active on it, after the lease's renewal.
    fixture.apply(6, FamilyMode::Normal).await?;
    for (name, lease, grace) in [
        (&generic, &generic_lease, reserved_until),
        (&lapsed, &lapsed_lease, grace_ends),
        (&updated, &updated_lease, grace_ends),
    ] {
        check(
            &served(&fixture, name).await?,
            on_lease(
                "active",
                "registrar",
                lease,
                renewed_until,
                reserved_until,
                grace,
            ),
        )?;
    }
    // Past it the renewed lease does not take the dates back.
    fixture.apply(8, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &generic).await?,
        on_lease(
            "released",
            "registrar",
            &generic_lease,
            renewed_until,
            reserved_until,
            reserved_until,
        ),
    )?;
    for (name, lease) in [(&lapsed, &lapsed_lease), (&updated, &updated_lease)] {
        check(
            &served(&fixture, name).await?,
            on_lease(
                "expired",
                "registrar",
                lease,
                renewed_until,
                reserved_until,
                grace_ends,
            ),
        )?;
    }
    // The ExpiryUpdated lands inside the grace.
    fixture.block_at(9, reserved_until + 14 * DAY).await?;
    expiry_updated(&fixture, reviewed, &updated, 3, 9, updated_until).await?;
    fixture.apply(9, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &updated).await?,
        on_lease(
            "active",
            "registrar",
            &updated_lease,
            renewed_until,
            updated_until,
            updated_until + 28 * DAY,
        ),
    )?;
    check(
        &served(&fixture, &lapsed).await?,
        on_lease(
            "expired",
            "registrar",
            &lapsed_lease,
            renewed_until,
            reserved_until,
            grace_ends,
        ),
    )?;
    // At the grace end the name with no ExpiryUpdated is released.
    fixture.block_at(10, grace_ends).await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &lapsed).await?,
        on_lease(
            "released",
            "registrar",
            &lapsed_lease,
            renewed_until,
            reserved_until,
            grace_ends,
        ),
    )?;
    check(
        &served(&fixture, &updated).await?,
        on_lease(
            "active",
            "registrar",
            &updated_lease,
            renewed_until,
            updated_until,
            updated_until + 28 * DAY,
        ),
    )?;
    fixture.assert_rebuild_equal(10).await?;
    fixture.cleanup().await
}

/// A `LabelReserved` of `name` on `registry`, entry `0x2000 + n` and token `n`, at `block`.
async fn label_reserved(
    fixture: &Fixture,
    registry: Registry<'_>,
    name: &str,
    n: u32,
    block: i64,
    expiry: i64,
) -> Result<()> {
    fixture
        .write(
            block,
            i64::from(n) * 10,
            "RegistrationReserved",
            "ens_v2_registry_l1",
            Some(name),
            Some(&uuid(0x2000 + n)),
            json!({"registry_contract_instance_id": registry.instance, "token_id": n.to_string(),
                   "source_event": "LabelReserved", "status": "reserved", "expiry": expiry}),
            registry.address,
        )
        .await?;
    Ok(())
}

/// The path-expiry release Interpret writes for `name`'s entry `0x2000 + n` at `block`, whose
/// time `released_at` has reached `expiry`.
async fn path_expired(
    fixture: &Fixture,
    registry: Registry<'_>,
    name: &str,
    n: u32,
    block: i64,
    expiry: i64,
    released_at: i64,
) -> Result<()> {
    fixture
        .event(
            Event::new(
                &format!("{name}-path-expired-{block}"),
                block,
                0,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .name(name)
            .resource(&uuid(0x2000 + n))
            .synthesised()
            .before(json!({"status": "reserved", "expiry": expiry, "registrant": Value::Null}))
            .after(json!({"source_event": "RegistryPathExpired",
                          "derived_from": "interpreter_state",
                          "terminal_reason": "registry_name_binding_expired",
                          "registry": registry.address, "token_id": n.to_string(),
                          "registry_contract_instance_id": registry.instance,
                          "expiry": expiry, "status": "released", "released_at": released_at}))
            .raw(json!({"kind": "raw_block", "emitting_address": registry.address})),
        )
        .await?;
    Ok(())
}

/// A controller registration of lease `0x1000 + n` (`registered_until`, granted at block 2 until
/// `expiry`), a premigration reservation on `registry` written live at block 3 until `expiry`
/// plus 62 days, which is block 4's time, and its path-expiry release at block 4. At block 5,
/// 70 days past `expiry`, a one-day BaseRegistrar renewal moves the lease to `expiry` plus a day.
/// Returns the name and its lease.
async fn lapsed_then_renewed(
    fixture: &Fixture,
    registry: Registry<'_>,
    label: &str,
    n: u32,
    expiry: i64,
) -> Result<(String, String)> {
    let name = surface(fixture, label).await?;
    let lease = uuid(0x1000 + n);
    registered_until(fixture, &name, n, &lease, 1, expiry).await?;
    label_reserved(fixture, registry, &name, n, 3, expiry + 62 * DAY).await?;
    path_expired(
        fixture,
        registry,
        &name,
        n,
        4,
        expiry + 62 * DAY,
        block_time(4),
    )
    .await?;
    renewal(
        fixture,
        &name,
        &lease,
        5,
        i64::from(n) * 10,
        json!(expiry + DAY),
        false,
    )
    .await?;
    Ok((name, lease))
}

/// The fields of a name ENSv1 selects through `lapsed_then_renewed`'s lease `lease`, registered
/// at block 2.
fn lease_row(
    status: &str,
    lease: &str,
    ens_v1_expiry: i64,
    expires_at: i64,
    grace_ends_at: i64,
) -> Value {
    json!({"status": status, "lifecycle_status": status, "registered_at": block_time(2),
           "resource_id": lease, "authority_kind": "registrar",
           "ens_v1_expiry": ens_v1_expiry.to_string(), "expires_at": expires_at,
           "grace_ends_at": grace_ends_at})
}

/// A live reservation that lapsed, replaced by one already expired when written. The lease E
/// lapses, premigration reserves the name live until E + 62 days, and that reservation lapses.
/// A one-day BaseRegistrar renewal at E + 70 days moves the lease to E + 1 day, and premigration
/// then reserves the name again at the renewed lease plus 62 days, E + 63 days, which has
/// already passed. A re-reservation keeps the entry's token and resource, so the replacement
/// lands on the first reservation's key. It is never live and supplies no dates, and it ends the
/// first reservation, so the lease supplies the dates and the status: `expired` through its
/// 90-day grace, E + 91 days, and `released` after it, on the reviewed registry as on the generic
/// one. A live re-reservation after the replacement opens a new instance and dates the name
/// again, and so does a renewal that revives the replacement: ETHRenewerV1 renews the lease and
/// the entry, and the adapter writes the revival as an `ExpiryChanged` and a `registry_state`
/// `RegistrationReserved` snapshot of the name the entry is current for again. An ENSv2
/// `ExpiryUpdated` is not a reservation and the rule does not reach it: a one-day renewal of the
/// lapsed reservation in its grace, to E + 63 days, renews the first reservation, which is
/// `expired` until its 28-day grace ends at E + 91 days and `released` from then.
#[tokio::test]
async fn a_never_live_replacement_ends_the_reservation_it_replaces() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_never_live_replacement", 4).await?;
    let instance = declared_path::reviewed_root_eth_entry(&fixture).await?;
    let reviewed = Registry {
        instance: &instance,
        address: declared_path::ETH,
    };
    let e = block_time(4) - 62 * DAY;
    let renewed = e + DAY;
    let lease_grace = renewed + 90 * DAY;
    for (block, time) in [
        (5, e + 70 * DAY),
        (6, e + 70 * DAY + 12),
        (7, e + 90 * DAY),
        (8, lease_grace),
        (9, lease_grace + 1),
    ] {
        fixture.block_at(block, time).await?;
    }
    let (replaced, replaced_lease) =
        lapsed_then_renewed(&fixture, reviewed, "replaced.eth", 1, e).await?;
    let (generic, generic_lease) =
        lapsed_then_renewed(&fixture, GENERIC, "generic.eth", 2, e).await?;
    let (rereserved, rereserved_lease) =
        lapsed_then_renewed(&fixture, reviewed, "rereserved.eth", 3, e).await?;
    let (extended, extended_lease) =
        lapsed_then_renewed(&fixture, reviewed, "extended.eth", 4, e).await?;
    let (revived, revived_lease) =
        lapsed_then_renewed(&fixture, reviewed, "revived.eth", 5, e).await?;
    // The replacement, already expired when written, and the release Interpret derives for it in
    // the same block.
    for (registry, name, n) in [
        (reviewed, &replaced, 1),
        (GENERIC, &generic, 2),
        (reviewed, &rereserved, 3),
        (reviewed, &revived, 5),
    ] {
        label_reserved(&fixture, registry, name, n, 6, renewed + 62 * DAY).await?;
        path_expired(
            &fixture,
            registry,
            name,
            n,
            6,
            renewed + 62 * DAY,
            e + 70 * DAY + 12,
        )
        .await?;
    }
    let reserved_again = renewed + 365 * DAY + 62 * DAY;
    label_reserved(&fixture, reviewed, &rereserved, 3, 7, reserved_again).await?;
    let revived_lease_expiry = renewed + 365 * DAY;
    renewal(
        &fixture,
        &revived,
        &revived_lease,
        7,
        50,
        json!(revived_lease_expiry),
        false,
    )
    .await?;
    expiry_updated(&fixture, reviewed, &revived, 5, 7, reserved_again).await?;
    // The ETHRenewerV1 renewal writes the BaseRegistrar renewal and the ENSv2 `ExpiryUpdated` in
    // one transaction. The new expiry has passed too, so it revives nothing.
    fixture
        .write(
            5,
            45,
            "ExpiryChanged",
            "ens_v2_registry_l1",
            Some(&extended),
            Some(&uuid(0x2004)),
            json!({"source_event": "ExpiryUpdated", "token_id": "4", "expiry": renewed + 62 * DAY,
                   "sender": OWNER, "revived_from_expiry": false,
                   "registry_contract_instance_id": reviewed.instance}),
            reviewed.address,
        )
        .await?;

    // E + 90 days: in the lease's grace.
    fixture.apply(7, FamilyMode::Normal).await?;
    for (name, lease) in [(&replaced, &replaced_lease), (&generic, &generic_lease)] {
        check(
            &served(&fixture, name).await?,
            lease_row("expired", lease, renewed, renewed, lease_grace),
        )?;
    }
    check(
        &served(&fixture, &rereserved).await?,
        lease_row(
            "active",
            &rereserved_lease,
            renewed,
            reserved_again,
            reserved_again + 28 * DAY,
        ),
    )?;
    check(
        &served(&fixture, &revived).await?,
        lease_row(
            "active",
            &revived_lease,
            revived_lease_expiry,
            reserved_again,
            reserved_again + 28 * DAY,
        ),
    )?;
    check(
        &served(&fixture, &extended).await?,
        lease_row(
            "expired",
            &extended_lease,
            renewed,
            renewed + 62 * DAY,
            lease_grace,
        ),
    )?;
    // E + 91 days: the lease's grace ends inclusive, the reservation's exclusive.
    fixture.apply(8, FamilyMode::Normal).await?;
    for (name, lease) in [(&replaced, &replaced_lease), (&generic, &generic_lease)] {
        check(
            &served(&fixture, name).await?,
            lease_row("expired", lease, renewed, renewed, lease_grace),
        )?;
    }
    check(
        &served(&fixture, &extended).await?,
        lease_row(
            "released",
            &extended_lease,
            renewed,
            renewed + 62 * DAY,
            lease_grace,
        ),
    )?;
    // One second later the lease is released.
    fixture.apply(9, FamilyMode::Normal).await?;
    for (name, lease) in [(&replaced, &replaced_lease), (&generic, &generic_lease)] {
        check(
            &served(&fixture, name).await?,
            lease_row("released", lease, renewed, renewed, lease_grace),
        )?;
    }
    check(
        &served(&fixture, &rereserved).await?,
        lease_row(
            "active",
            &rereserved_lease,
            renewed,
            reserved_again,
            reserved_again + 28 * DAY,
        ),
    )?;
    check(
        &served(&fixture, &revived).await?,
        lease_row(
            "active",
            &revived_lease,
            revived_lease_expiry,
            reserved_again,
            reserved_again + 28 * DAY,
        ),
    )?;
    fixture.assert_rebuild_equal(9).await?;
    fixture.cleanup().await
}

/// A controller registration of lease `0x1000 + n` (`registered_until`, granted at block 2 until
/// `expiry`), and a premigration reservation of it on registry `a`, entry `0x2000 + n`, written
/// live at block 3 until `expiry` plus 62 days, which is block 4's time. In the same block the
/// root moves the `.eth` subregistry from `a` to `b`. The adapter writes the named release of the
/// `a` entry that lost its path, with `source_event` `SubregistryUpdated`
/// (`v2_registry/topology.rs` in the adapters), and premigration reserves the name on `b`, entry
/// `0x2000 + n + 10`, until the same time. The `b` reservation lapses at block 4 with its
/// path-expiry release. At block 5, 70 days past `expiry`, a one-day BaseRegistrar renewal moves
/// the lease to `expiry` plus a day, and at block 6 premigration reserves the name on `b` again at
/// the renewed lease plus 62 days, already passed, with the path-expiry release Interpret derives
/// for it. Returns the name and its lease.
async fn moved_then_replaced(
    fixture: &Fixture,
    a: Registry<'_>,
    b: Registry<'_>,
    label: &str,
    n: u32,
    expiry: i64,
) -> Result<(String, String)> {
    let name = surface(fixture, label).await?;
    let lease = uuid(0x1000 + n);
    let on_b = n + 10;
    registered_until(fixture, &name, n, &lease, 1, expiry).await?;
    label_reserved(fixture, a, &name, n, 3, expiry + 62 * DAY).await?;
    fixture
        .event(
            Event::new(
                &format!("{label}-moved"),
                3,
                i64::from(n) * 10 + 1,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .name(&name)
            .resource(&uuid(0x2000 + n))
            .before(json!({"status": "reserved"}))
            .after(json!({"source_event": "SubregistryUpdated",
                          "terminal_reason": "registry_name_binding_changed",
                          "status": "released", "token_id": n.to_string(),
                          "registry_contract_instance_id": a.instance}))
            .raw(json!({"emitting_address": a.address})),
        )
        .await?;
    label_reserved(fixture, b, &name, on_b, 3, expiry + 62 * DAY).await?;
    path_expired(fixture, b, &name, on_b, 4, expiry + 62 * DAY, block_time(4)).await?;
    renewal(
        fixture,
        &name,
        &lease,
        5,
        i64::from(n) * 10,
        json!(expiry + DAY),
        false,
    )
    .await?;
    let replaced_until = expiry + DAY + 62 * DAY;
    label_reserved(fixture, b, &name, on_b, 6, replaced_until).await?;
    path_expired(
        fixture,
        b,
        &name,
        on_b,
        6,
        replaced_until,
        expiry + 70 * DAY + 12,
    )
    .await?;
    Ok((name, lease))
}

/// Rows 7, 7b and 7c: a never-live replacement on a newer registry keeps an older registry's
/// release from dating the name. `moved_then_replaced` writes the shape on the generic registry
/// `a` and the reviewed registry `b`. The replacement on `b` keeps `b`'s place as the name's
/// latest ENSv2 entry but supplies no dates, so `a`'s earlier release is not selected and the
/// renewed ENSv1 lease is the authority and the dates source: `expired` at E + 80 days and through
/// its inclusive 90-day grace, E + 91 days, and `released` one second later. A live
/// re-reservation on `b` dates the name again, and so does an ETHRenewerV1 revival of the
/// replacement on `b`, written as an `ExpiryChanged` and a `registry_state` snapshot.
#[tokio::test]
async fn a_never_live_replacement_keeps_an_older_registrys_release_from_dating_the_name()
-> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_moved_replacement", 4).await?;
    let instance = declared_path::reviewed_root_eth_entry(&fixture).await?;
    let reviewed = Registry {
        instance: &instance,
        address: declared_path::ETH,
    };
    let e = block_time(4) - 62 * DAY;
    let renewed = e + DAY;
    let lease_grace = renewed + 90 * DAY;
    for (block, time) in [
        (5, e + 70 * DAY),
        (6, e + 70 * DAY + 12),
        (7, e + 80 * DAY),
        (8, lease_grace),
        (9, lease_grace + 1),
    ] {
        fixture.block_at(block, time).await?;
    }
    let (moved, moved_lease) =
        moved_then_replaced(&fixture, GENERIC, reviewed, "moved.eth", 1, e).await?;
    let (rereserved, rereserved_lease) =
        moved_then_replaced(&fixture, GENERIC, reviewed, "moved-rereserved.eth", 2, e).await?;
    let (revived, revived_lease) =
        moved_then_replaced(&fixture, GENERIC, reviewed, "moved-revived.eth", 3, e).await?;
    let reserved_again = renewed + 365 * DAY + 62 * DAY;
    label_reserved(&fixture, reviewed, &rereserved, 12, 7, reserved_again).await?;
    let revived_lease_expiry = renewed + 365 * DAY;
    renewal(
        &fixture,
        &revived,
        &revived_lease,
        7,
        35,
        json!(revived_lease_expiry),
        false,
    )
    .await?;
    expiry_updated(&fixture, reviewed, &revived, 13, 7, reserved_again).await?;

    // E + 80 days: the renewed lease is in its grace.
    fixture.apply(7, FamilyMode::Normal).await?;
    let in_grace = served(&fixture, &moved).await?;
    check(
        &in_grace,
        lease_row("expired", &moved_lease, renewed, renewed, lease_grace),
    )?;
    ensure!(
        in_grace["authority_kind"] == json!("registrar")
            && in_grace["expires_at"] == json!(renewed)
            && in_grace["grace_ends_at"] == json!(renewed + 90 * DAY),
        "the renewed lease is the authority and dates the name: {in_grace}"
    );
    for (name, lease, lease_expiry) in [
        (&rereserved, &rereserved_lease, renewed),
        (&revived, &revived_lease, revived_lease_expiry),
    ] {
        check(
            &served(&fixture, name).await?,
            lease_row(
                "active",
                lease,
                lease_expiry,
                reserved_again,
                reserved_again + 28 * DAY,
            ),
        )?;
    }
    // E + 91 days: the lease's grace ends inclusive.
    fixture.apply(8, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &moved).await?,
        lease_row("expired", &moved_lease, renewed, renewed, lease_grace),
    )?;
    // One second later the lease is released.
    fixture.apply(9, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &moved).await?,
        lease_row("released", &moved_lease, renewed, renewed, lease_grace),
    )?;
    for (name, lease, lease_expiry) in [
        (&rereserved, &rereserved_lease, renewed),
        (&revived, &revived_lease, revived_lease_expiry),
    ] {
        check(
            &served(&fixture, name).await?,
            lease_row(
                "active",
                lease,
                lease_expiry,
                reserved_again,
                reserved_again + 28 * DAY,
            ),
        )?;
    }
    fixture.assert_rebuild_equal(9).await?;
    fixture.cleanup().await
}

/// The ownerless-registry profile with a live lease: a controller registration whose holder later
/// moved the token and then cleared the registry record. The cleared record does not end the
/// lease, so the name serves it, with the token holder as registrant and no authority, owner or
/// registry custody.
#[tokio::test]
async fn an_ownerless_name_serves_its_lease_and_holder() -> Result<()> {
    let fixture = Fixture::new("families_handoff_lease_ownerless", 6).await?;
    let name = surface(&fixture, "cleared.eth").await?;
    let lease = uuid(0x1001);
    owner_write(&fixture, &name, &lease, 1, 0, OWNER, false).await?;
    fixture
        .binding(&uuid(101), &name, &lease, "ens_v1", 1, 10, Some(4))
        .await?;
    fixture
        .write(
            1,
            10,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(&name),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            2,
            10,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name),
            Some(&lease),
            json!({"authority_kind": "registrar", "source_event": "NameRegistered",
                   "status": "registered", "registrant": OWNER, "expiry": LEASE_EXPIRY}),
            REGISTRAR,
        )
        .await?;
    transfer(&fixture, &name, &lease, 3, 1, HOLDER).await?;
    owner_write(&fixture, &name, &lease, 4, 0, ZERO, false).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    check(
        &served(&fixture, &name).await?,
        json!({"status": "active", "lifecycle_status": "active",
               "registered_at": block_time(2), "resource_id": lease,
               "identity_resource_id": lease, "ens_v1_expiry": LEASE_EXPIRY.to_string(),
               "expires_at": LEASE_EXPIRY, "registrant": HOLDER,
               "authority_kind": Value::Null, "owner": Value::Null}),
    )?;
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}
