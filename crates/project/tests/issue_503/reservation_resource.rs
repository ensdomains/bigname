use super::*;

// The end of a reservation that carries its own resource (TYR-36 step 6, review thread on
// build.sql, "Handle releases of resource-backed reservations"). Interpret gives a reservation its
// own resource when its token id is version zero (crates/adapters/src/schema_v2/protocol/
// v2_registry.rs, `reservation_resource`), for example a label reserved in a replacement registry
// after the name's registration in the old one was released. That reservation hands the released
// name to its live ENSv1 lease like any other, and when it is unregistered or lapses its release
// carries the same resource. The release must end the hand-back so the released ENSv2 tombstone
// returns.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L207 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L649-L651 @ ens_v2@a971bd64)

const BLOCK_11_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000611";

/// A live ENSv1 lease; an ENSv2 registration granted and unregistered in block 9; a
/// version-zero reservation of the name with its own resource at (10, 1, 1); and that
/// reservation's release, carrying the same resource, at (11, 1, 1). Returns the name and the
/// ENSv1 and released ENSv2 resources.
async fn seed(pool: &PgPool) -> Result<(String, String, String)> {
    earlier_block(pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 11, '2026-08-26T00:00:12Z', 'canonical')")
        .bind(CHAIN).bind(BLOCK_11_HASH).execute(pool).await?;
    let logical = surface(pool, 102, "reserved-resource.eth", &[]).await?;
    let v1_resource = open_binding(pool, &logical, 102, "ens_v1", 1).await?;
    event(
        pool,
        "reservation-resource-v1-grant",
        &logical,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 1,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    let (v2_resource, _) = closed_binding_at_block_9(pool, &logical, 102, "ens_v2").await?;
    for (identity, kind, log, after) in [
        (
            "reservation-resource-v2-grant",
            "RegistrationGranted",
            2,
            json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (
            "reservation-resource-v2-release",
            "RegistrationReleased",
            3,
            json!({"source_event":"LabelUnregistered","status":"released"}),
        ),
    ] {
        event(
            pool,
            identity,
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
        sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity = $2")
            .bind(EARLIER_HASH).bind(identity).execute(pool).await?;
    }
    let reservation_resource = uuid(21, 102);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&reservation_resource).bind(CHAIN).bind(HASH).execute(pool).await?;
    event(
        pool,
        "reservation-resource-reserve",
        &logical,
        Some(&reservation_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReserved",
            log: 1,
            after: json!({"source_event":"LabelReserved","expiry":4_000_000_000_i64,"status":"reserved","reservation_resource":true}),
        },
    )
    .await?;
    let release = event(
        pool,
        "reservation-resource-release",
        &logical,
        Some(&reservation_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReleased",
            log: 1,
            after: json!({"source_event":"LabelUnregistered","status":"released"}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET block_number = 11, block_hash = $2 WHERE normalized_event_id = $1")
        .bind(release).bind(BLOCK_11_HASH).execute(pool).await?;
    Ok((logical, v1_resource, v2_resource))
}

async fn selection(
    pool: &PgPool,
    logical: &str,
) -> Result<(Option<String>, Option<String>, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT provenance #>> '{authority_selection,authority_arm}',
                provenance #>> '{authority_selection,lifecycle_state}',
                resource_id::text
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical)
    .fetch_one(pool)
    .await?)
}

async fn project(
    pool: &PgPool,
    target: i64,
    previous: Option<bigname_project::Marker>,
) -> Result<bigname_project::Marker> {
    Ok(Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: previous.as_ref().map_or(9, |marker| marker.number + 1),
            affected_to_block: target,
            resume_current: previous,
            mode: RunMode::Normal,
        })
        .await?
        .current)
}

#[tokio::test]
async fn the_release_of_a_reservation_with_its_own_resource_restores_the_v2_tombstone() -> Result<()>
{
    // Two incremental batches: block 10 holds the reservation, block 11 its release.
    let (incremental_db, incremental) = database("reservation_resource_incremental").await?;
    let (logical, v1_resource, v2_resource) = seed(&incremental).await?;
    let first = project(&incremental, 10, None).await?;
    assert_eq!(
        selection(&incremental, &logical).await?,
        (
            Some("ens_v1".into()),
            Some("registered".into()),
            Some(v1_resource)
        ),
        "the reservation with its own resource hands the name to the live ENSv1 lease"
    );
    project(&incremental, 11, Some(first)).await?;
    let tombstone = (
        Some("ens_v2".into()),
        Some("unregistered".into()),
        Some(v2_resource),
    );
    assert_eq!(
        selection(&incremental, &logical).await?,
        tombstone,
        "the reservation's release restores the released ENSv2 tombstone"
    );
    incremental_db.cleanup().await?;
    // One batch over the same rows agrees.
    let (full_db, full) = database("reservation_resource_full").await?;
    seed(&full).await?;
    project(&full, 11, None).await?;
    assert_eq!(selection(&full, &logical).await?, tombstone);
    full_db.cleanup().await?;
    Ok(())
}

/// Block 10's timestamp in seconds: `database` writes it as 2026-08-26T00:00:00Z.
const BLOCK_10_SECONDS: i64 = 1_787_702_400;

// A version-zero reservation with its own resource that is already expired when written (TYR-36
// step 6, adversarial review of e18e509e, N2). The rows are Interpret's for a named entry: the
// reservation at its log with the name and its own resource, and the state-derived release at the
// block boundary with the name, the same resource and no transaction or log index
// (crates/adapters/src/schema_v2/protocol/v2_registry/topology.rs, `append_removed_name`; the
// detached form is pinned in crates/adapters/src/schema_v2/tests.rs,
// `already_expired_detached_reservation_emits_resource_scoped_release`). The shape needs a
// replacement registry for the name, which the raw-log harness cannot set up, so the rows are
// written here directly. The reservation is never live, so the released ENSv2 tombstone stands.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452-L454 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L628-L630 @ ens_v2@a971bd64)
async fn seed_expired_at_write(pool: &PgPool) -> Result<(String, String)> {
    earlier_block(pool).await?;
    let logical = surface(pool, 104, "expired-resource.eth", &[]).await?;
    let v1_resource = open_binding(pool, &logical, 104, "ens_v1", 1).await?;
    event(
        pool,
        "expired-resource-v1-grant",
        &logical,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 1,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    let (v2_resource, _) = closed_binding_at_block_9(pool, &logical, 104, "ens_v2").await?;
    for (identity, kind, log, after) in [
        (
            "expired-resource-v2-grant",
            "RegistrationGranted",
            2,
            json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (
            "expired-resource-v2-release",
            "RegistrationReleased",
            3,
            json!({"source_event":"LabelUnregistered","status":"released"}),
        ),
    ] {
        event(
            pool,
            identity,
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
        sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity = $2")
            .bind(EARLIER_HASH).bind(identity).execute(pool).await?;
    }
    let reservation_resource = uuid(21, 104);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&reservation_resource).bind(CHAIN).bind(HASH).execute(pool).await?;
    let reserve = event(
        pool,
        "expired-resource-reserve",
        &logical,
        Some(&reservation_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReserved",
            log: 1,
            after: json!({"source_event":"LabelReserved","expiry":BLOCK_10_SECONDS,"status":"reserved","reservation_resource":true}),
        },
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET transaction_index = 1 WHERE normalized_event_id = $1",
    )
    .bind(reserve)
    .execute(pool)
    .await?;
    let release = event(
        pool,
        "expired-resource-boundary-release",
        &logical,
        Some(&reservation_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReleased",
            log: 0,
            after: json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","expiry":BLOCK_10_SECONDS,"status":"released","released_at":BLOCK_10_SECONDS}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL, before_state = $2 WHERE normalized_event_id = $1")
        .bind(release)
        .bind(json!({"status":"reserved","expiry":BLOCK_10_SECONDS,"registrant":null}))
        .execute(pool).await?;
    Ok((logical, v2_resource))
}

#[tokio::test]
async fn a_version_zero_reservation_expired_when_written_leaves_the_v2_tombstone() -> Result<()> {
    let tombstone = |v2_resource: String| {
        (
            Some("ens_v2".to_owned()),
            Some("unregistered".to_owned()),
            Some(v2_resource),
        )
    };
    let (full_db, full) = database("expired_resource_reservation_full").await?;
    let (logical, v2_resource) = seed_expired_at_write(&full).await?;
    project(&full, 10, None).await?;
    assert_eq!(
        selection(&full, &logical).await?,
        tombstone(v2_resource.clone()),
        "a reservation expired when written does not take the name"
    );
    full_db.cleanup().await?;
    let (incremental_db, incremental) =
        database("expired_resource_reservation_incremental").await?;
    seed_expired_at_write(&incremental).await?;
    let first = project(&incremental, 9, None).await?;
    project(&incremental, 10, Some(first)).await?;
    assert_eq!(
        selection(&incremental, &logical).await?,
        tombstone(v2_resource)
    );
    incremental_db.cleanup().await?;
    Ok(())
}

// Which reservation a resource-carrying release ends (TYR-36 step 6, Pro review of e18e509e,
// question 3). A release on a resource other than the one the name was last bound to counts only
// when it ends the name's current reservation on that resource. A version-zero reservation that
// is then registered keeps its resource, since the registry bumps the token version only when it
// replaces an owner, so its registration's later release carries the same resource and is not the
// end of a reservation. A reservation of the name on another resource after it is also not ended
// by it.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L435-L471 @ ens_v2@a971bd64)

/// One extra normalized row for the name: kind, resource (`None` for none), block, log and
/// after-state.
type Fact = (
    &'static str,
    &'static str,
    Option<&'static str>,
    i64,
    i64,
    Value,
);

/// The resource of registry A's version-zero token.
const A0: &str = "00000016-0000-0000-0000-000000000001";

/// A live ENSv1 lease and the name's last bound ENSv2 registration B0, granted and unregistered in
/// block 9, then `facts`. Returns the name and the ENSv1 and B0 resources.
async fn seed_sequence(
    pool: &PgPool,
    index: u16,
    name: &str,
    facts: &[Fact],
) -> Result<(String, String, String)> {
    earlier_block(pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 11, '2026-08-26T00:00:12Z', 'canonical')")
        .bind(CHAIN).bind(BLOCK_11_HASH).execute(pool).await?;
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 9, 'canonical')")
        .bind(A0).bind(CHAIN).bind(EARLIER_HASH).execute(pool).await?;
    let logical = surface(pool, index, name, &[]).await?;
    let v1_resource = open_binding(pool, &logical, index, "ens_v1", 1).await?;
    event(
        pool,
        &format!("sequence-{index}-v1-grant"),
        &logical,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 1,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    let (b0, _) = closed_binding_at_block_9(pool, &logical, index, "ens_v2").await?;
    let owned: [Fact; 2] = [
        (
            "b0-grant",
            "RegistrationGranted",
            None,
            9,
            2,
            json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (
            "b0-release",
            "RegistrationReleased",
            None,
            9,
            3,
            json!({"source_event":"LabelUnregistered","status":"released"}),
        ),
    ];
    for (position, (identity, kind, resource, block, log, after)) in
        owned.into_iter().chain(facts.iter().cloned()).enumerate()
    {
        // The owned B0 facts carry B0's resource.
        let resource = if position < 2 {
            Some(b0.as_str())
        } else {
            resource
        };
        let id = event(
            pool,
            &format!("sequence-{index}-{identity}"),
            &logical,
            resource,
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
        let hash = match block {
            9 => EARLIER_HASH,
            10 => HASH,
            _ => BLOCK_11_HASH,
        };
        sqlx::query("UPDATE normalized_events SET block_number = $2, block_hash = $3 WHERE normalized_event_id = $1")
            .bind(id).bind(block).bind(hash).execute(pool).await?;
    }
    Ok((logical, v1_resource, b0))
}

/// Projects the sequence in one batch to block 11 and as batches to 9, 10 and 11, checks that both
/// agree, and returns the selection at block 10 and at block 11.
async fn sequence_selections(
    prefix: &str,
    index: u16,
    name: &str,
    facts: &[Fact],
) -> Result<(
    String,
    (Option<String>, Option<String>, Option<String>),
    (Option<String>, Option<String>, Option<String>),
)> {
    let (incremental_db, incremental) = database(&format!("{prefix}_incremental")).await?;
    let (logical, v1_resource, _) = seed_sequence(&incremental, index, name, facts).await?;
    let at_9 = project(&incremental, 9, None).await?;
    let at_10 = project(&incremental, 10, Some(at_9)).await?;
    let block_10 = selection(&incremental, &logical).await?;
    project(&incremental, 11, Some(at_10)).await?;
    let block_11 = selection(&incremental, &logical).await?;
    incremental_db.cleanup().await?;
    let (full_db, full) = database(&format!("{prefix}_full")).await?;
    seed_sequence(&full, index, name, facts).await?;
    project(&full, 11, None).await?;
    assert_eq!(
        selection(&full, &logical).await?,
        block_11,
        "{prefix}: one batch and incremental batches agree"
    );
    full_db.cleanup().await?;
    Ok((v1_resource, block_10, block_11))
}

fn live_reservation(
    identity: &'static str,
    resource: Option<&'static str>,
    block: i64,
    log: i64,
) -> Fact {
    (
        identity,
        "RegistrationReserved",
        resource,
        block,
        log,
        json!({"source_event":"LabelReserved","expiry":4_000_000_000_i64,"status":"reserved"}),
    )
}

fn release(identity: &'static str, resource: Option<&'static str>, block: i64, log: i64) -> Fact {
    (
        identity,
        "RegistrationReleased",
        resource,
        block,
        log,
        json!({"source_event":"LabelUnregistered","status":"released"}),
    )
}

// Negative control: a live reservation B1 hands the name to ENSv1, then the label is reserved at
// version zero on registry A (resource A0) and registered there, and that registration is later
// unregistered. A0 carried a reservation of the name, but its release ends the registration that
// replaced it, not a reservation, so ENSv1 stays selected while B1 is live.
// The rows are hand-built to isolate the registration check: Interpret would not write them in
// this order. It names a reservation only while its registry is on the name's path, and a grant on
// the path opens a binding, so in Interpret output A0's named facts would come before B0, as they
// do in the two-registry test below.
#[tokio::test]
async fn a_registrations_release_does_not_end_the_reservation_it_replaced() -> Result<()> {
    let facts = [
        live_reservation("b1-reserve", None, 10, 1),
        live_reservation("a0-reserve", Some(A0), 10, 2),
        (
            "a0-grant",
            "RegistrationGranted",
            Some(A0),
            10,
            3,
            json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000003"}),
        ),
        release("a0-release", Some(A0), 11, 1),
    ];
    let (v1_resource, block_10, block_11) = sequence_selections(
        "registration_release_control",
        105,
        "reserve-register.eth",
        &facts,
    )
    .await?;
    let ensv1 = (
        Some("ens_v1".to_owned()),
        Some("registered".to_owned()),
        Some(v1_resource),
    );
    assert_eq!(block_10, ensv1);
    assert_eq!(
        block_11, ensv1,
        "the release of A0's registration ends no reservation"
    );
    Ok(())
}

// The two-registry sequence: A0 was reserved and registered for the name in registry A, whose path
// was later detached; B0 in registry B became the last bound registration and was released; a
// live reservation B1 hands the name to ENSv1; then A0's registration is unregistered. The name
// stays with ENSv1 while B1 is live.
#[tokio::test]
async fn a_detached_registrations_release_does_not_end_a_live_reservation_elsewhere() -> Result<()>
{
    let facts = [
        live_reservation("a0-reserve", Some(A0), 9, 0),
        (
            "a0-grant",
            "RegistrationGranted",
            Some(A0),
            9,
            1,
            json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000003"}),
        ),
        live_reservation("b1-reserve", None, 10, 1),
        release("a0-release", Some(A0), 11, 1),
    ];
    let (v1_resource, block_10, block_11) =
        sequence_selections("two_registry_release", 106, "two-registry.eth", &facts).await?;
    let ensv1 = (
        Some("ens_v1".to_owned()),
        Some("registered".to_owned()),
        Some(v1_resource),
    );
    assert_eq!(block_10, ensv1);
    assert_eq!(block_11, ensv1, "B1 is still live");
    Ok(())
}

// A reservation on A0 that was never registered, then a later live reservation B1 of the name on
// another resource, then A0's reservation ends. The release ends A0's reservation, not B1's, so
// ENSv1 stays selected while B1 is live.
#[tokio::test]
async fn a_release_on_one_resource_does_not_end_a_later_reservation_on_another() -> Result<()> {
    let facts = [
        live_reservation("a0-reserve", Some(A0), 9, 0),
        live_reservation("b1-reserve", None, 10, 1),
        release("a0-release", Some(A0), 11, 1),
    ];
    let (v1_resource, block_10, block_11) = sequence_selections(
        "foreign_resource_release",
        107,
        "foreign-release.eth",
        &facts,
    )
    .await?;
    let ensv1 = (
        Some("ens_v1".to_owned()),
        Some("registered".to_owned()),
        Some(v1_resource),
    );
    assert_eq!(block_10, ensv1);
    assert_eq!(block_11, ensv1, "B1 is still live");
    Ok(())
}
