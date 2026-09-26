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

/// One extra normalized row for the name: kind, resource (`None` for none), block, log (negative
/// for a block-boundary row with no transaction or log index) and after-state.
type Fact = (
    &'static str,
    &'static str,
    Option<&'static str>,
    i64,
    i64,
    Value,
);

/// Sentinel `released_at` values on hand-built releases, so a served release time names the row
/// it came from.
const B0_RELEASED_AT: i64 = 9_001;
/// B0's grant expiry, a sentinel as well; `LabelRegistered` always carries one.
const B0_EXPIRY: i64 = 9_002;
const CT_RELEASED_AT: i64 = 11_001;
const A0_RELEASED_AT: i64 = 11_002;

/// Seeds the sequence once more, projects it in one batch to block 11, and returns what the name
/// serves.
async fn served_at_11(prefix: &str, index: u16, name: &str, facts: &[Fact]) -> Result<Value> {
    let (db, pool) = database(&format!("{prefix}_served")).await?;
    let (logical, _, _) = seed_sequence(&pool, index, name, facts).await?;
    project(&pool, 11, None).await?;
    let served = served(&pool, &logical).await?;
    db.cleanup().await?;
    Ok(served)
}

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
            json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000002","expiry":B0_EXPIRY}),
        ),
        (
            "b0-release",
            "RegistrationReleased",
            None,
            9,
            3,
            // A sentinel release time, so a served one cannot be taken for B0's by mistake.
            json!({"source_event":"LabelUnregistered","status":"released","released_at":B0_RELEASED_AT}),
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
                log: log.max(0),
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
        // A negative log stands for a block-boundary row with no transaction or log index.
        if log < 0 {
            sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE normalized_event_id = $1")
                .bind(id).execute(pool).await?;
        }
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
    let served_11 = served(&incremental, &logical).await?;
    incremental_db.cleanup().await?;
    let (full_db, full) = database(&format!("{prefix}_full")).await?;
    seed_sequence(&full, index, name, facts).await?;
    project(&full, 11, None).await?;
    assert_eq!(
        selection(&full, &logical).await?,
        block_11,
        "{prefix}: one batch and incremental batches agree"
    );
    assert_eq!(
        served(&full, &logical).await?,
        served_11,
        "{prefix}: one batch and incremental batches serve the same fields"
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

// A topology reservation as the witness (TYR-36 step 6, adversarial review of 3be64c73). When a
// renewal revives an expired version-zero reservation and restores its name, Interpret writes the
// reservation again from topology, with `source_event` `ExpiryUpdated` and the reservation's own
// resource. That row is the reservation a later release on the resource ends, whether it sits at
// a log position or at the block boundary with no transaction or log index. It hands the name to
// ENSv1 at block 10, and its release at block 11 restores the released ENSv2 tombstone.
#[tokio::test]
async fn a_topology_reservation_is_ended_by_a_later_release_on_its_resource() -> Result<()> {
    for (case, index, name, log) in [
        ("topology_witness_indexed", 113, "topology-indexed.eth", 1),
        (
            "topology_witness_boundary",
            114,
            "topology-boundary.eth",
            -1,
        ),
    ] {
        let facts = [
            (
                "a0-topology-reserve",
                "RegistrationReserved",
                Some(A0),
                10,
                log,
                json!({"source_event":"ExpiryUpdated","status":"reserved","expiry":4_000_000_000_i64,"reservation_resource":true}),
            ),
            (
                "a0-release",
                "RegistrationReleased",
                Some(A0),
                11,
                1,
                json!({"source_event":"LabelUnregistered","status":"released","released_at":A0_RELEASED_AT}),
            ),
        ];
        let (v1_resource, block_10, block_11) =
            sequence_selections(case, index, name, &facts).await?;
        assert_eq!(
            block_10,
            (
                Some("ens_v1".to_owned()),
                Some("registered".to_owned()),
                Some(v1_resource)
            ),
            "{case}: the topology reservation hands the name to ENSv1"
        );
        assert_eq!(
            block_11,
            (
                Some("ens_v2".to_owned()),
                Some("unregistered".to_owned()),
                Some(uuid(15, index))
            ),
            "{case}: its release restores the tombstone on B0"
        );
        // The tombstone serves the reservation's end, on B0's resource and binding. The end is an
        // explicit release, which serves no expiry: not B0's grant expiry and not the
        // reservation's far-future one.
        let served = served_at_11(case, index, name, &facts).await?;
        assert_eq!(
            (
                served["resource_id"].as_str(),
                served["surface_binding_id"].as_str(),
                served["registration"]["status"].as_str(),
                served["control"]["status"].as_str(),
                served["registration"]["released_at"].as_i64(),
                served["registration"]["expiry"].as_i64(),
            ),
            (
                Some(uuid(15, index).as_str()),
                Some(uuid(16, index).as_str()),
                Some("released"),
                Some("unregistered"),
                Some(A0_RELEASED_AT),
                None,
            ),
            "{case}: {served}"
        );
    }
    Ok(())
}

// Which reservation a resource-less release ends (TYR-36 step 6, Pro review of 3be64c73,
// question 1). A versioned reservation carries no resource, and neither does its end, so a
// resource cannot tell which reservation a resource-less release ends. Interpret writes the
// registry instance and token id on both, and the release ends the name's current reservation
// only when that reservation has the same registry instance and token id. After a later
// reservation of the name elsewhere, the old reservation's end changes nothing.

/// A resource-less reservation of the name by registry instance `registry` and token `token`.
fn versioned_reservation(
    identity: &'static str,
    registry: &'static str,
    token: &'static str,
    block: i64,
    log: i64,
) -> Fact {
    (
        identity,
        "RegistrationReserved",
        None,
        block,
        log,
        json!({"source_event":"LabelReserved","expiry":4_000_000_000_i64,"status":"reserved","registry_contract_instance_id":registry,"token_id":token}),
    )
}

/// The end of the reservation by `registry` and `token`, by name and without a resource.
fn versioned_release(
    identity: &'static str,
    registry: &'static str,
    token: &'static str,
    block: i64,
    log: i64,
) -> Fact {
    (
        identity,
        "RegistrationReleased",
        None,
        block,
        log,
        json!({"source_event":"LabelUnregistered","status":"released","registry_contract_instance_id":registry,"token_id":token}),
    )
}

const REGISTRY_A: &str = "0000000a-0000-0000-0000-00000000000a";
const REGISTRY_B: &str = "0000000b-0000-0000-0000-00000000000b";
const REGISTRY_C: &str = "0000000c-0000-0000-0000-00000000000c";
const TOKEN_B1: &str = "0x00000000000000000000000000000000000000000000000000000000000000b1";
/// A version-zero token: the low 32 bits hold the token version, and Interpret gives a
/// reservation its own resource only at version zero.
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L649-L651 @ ens_v2@a971bd64)
const TOKEN_A0: &str = "0x000000000000000000000000000000000000000000000000000000a000000000";

// (a) The resource-less reservation B1 in registry B, then a live reservation A0 with its own
// resource in registry A after the parent points there, then B1 is unregistered. A0 is live, so
// ENSv1 stays selected.
#[tokio::test]
async fn a_resourceless_release_does_not_end_a_later_reservation_with_a_resource() -> Result<()> {
    let facts = [
        versioned_reservation("b1-reserve", REGISTRY_B, TOKEN_B1, 10, 1),
        (
            "a0-reserve",
            "RegistrationReserved",
            Some(A0),
            10,
            2,
            json!({"source_event":"LabelReserved","expiry":4_000_000_000_i64,"status":"reserved","reservation_resource":true,"registry_contract_instance_id":REGISTRY_A,"token_id":TOKEN_A0}),
        ),
        versioned_release("b1-release", REGISTRY_B, TOKEN_B1, 11, 1),
    ];
    let (v1_resource, block_10, block_11) = sequence_selections(
        "resourceless_release_after_a0",
        115,
        "resourceless-a0.eth",
        &facts,
    )
    .await?;
    let ensv1 = (
        Some("ens_v1".to_owned()),
        Some("registered".to_owned()),
        Some(v1_resource),
    );
    assert_eq!(block_10, ensv1);
    assert_eq!(block_11, ensv1, "A0 is still live");
    Ok(())
}

// (b) Two resource-less reservations of the name from different registry instances with the same
// token id: B1, then C1 after the parent points at registry C, then B1 is unregistered. A token id
// is the labelhash with the token version in its low bits, so the same label at the same version
// has the same token id in both registries, and only the registry instance tells the two apart.
// Neither the reservations nor the release have a resource. C1 is live, so ENSv1 stays selected.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L649-L651 @ ens_v2@a971bd64)
#[tokio::test]
async fn a_resourceless_release_does_not_end_another_resourceless_reservation() -> Result<()> {
    let facts = [
        versioned_reservation("b1-reserve", REGISTRY_B, TOKEN_B1, 10, 1),
        versioned_reservation("c1-reserve", REGISTRY_C, TOKEN_B1, 10, 2),
        versioned_release("b1-release", REGISTRY_B, TOKEN_B1, 11, 1),
    ];
    let (v1_resource, block_10, block_11) = sequence_selections(
        "resourceless_release_after_c1",
        116,
        "resourceless-c1.eth",
        &facts,
    )
    .await?;
    let ensv1 = (
        Some("ens_v1".to_owned()),
        Some("registered".to_owned()),
        Some(v1_resource),
    );
    assert_eq!(block_10, ensv1);
    assert_eq!(block_11, ensv1, "C1 is still live");
    Ok(())
}

// The positive case with the same rows: B1 alone, then its release, restores the released ENSv2
// tombstone on B0.
#[tokio::test]
async fn a_resourceless_release_ends_its_own_reservation() -> Result<()> {
    let facts = [
        versioned_reservation("b1-reserve", REGISTRY_B, TOKEN_B1, 10, 1),
        versioned_release("b1-release", REGISTRY_B, TOKEN_B1, 11, 1),
    ];
    let (v1_resource, block_10, block_11) = sequence_selections(
        "resourceless_release_own",
        117,
        "resourceless-own.eth",
        &facts,
    )
    .await?;
    assert_eq!(
        block_10,
        (
            Some("ens_v1".to_owned()),
            Some("registered".to_owned()),
            Some(v1_resource)
        )
    );
    assert_eq!(
        block_11,
        (
            Some("ens_v2".to_owned()),
            Some("unregistered".to_owned()),
            Some(uuid(15, 117))
        )
    );
    Ok(())
}

const TOKEN_B2: &str = "0x00000000000000000000000000000000000000000000000000000000000000b2";

// Same registry, different token (Pro review of 211cbaf0, question 4): the reservation is B1 in
// registry B, and the resource-less release at block 11 is for token B2 in the same registry.
// Only the token id tells them apart, so the release does not end B1 and ENSv1 stays selected.
#[tokio::test]
async fn a_resourceless_release_of_another_token_in_the_same_registry_ends_nothing() -> Result<()> {
    let facts = [
        versioned_reservation("b1-reserve", REGISTRY_B, TOKEN_B1, 10, 1),
        versioned_release("b2-release", REGISTRY_B, TOKEN_B2, 11, 1),
    ];
    let (v1_resource, block_10, block_11) = sequence_selections(
        "resourceless_other_token",
        118,
        "resourceless-token.eth",
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

// Both reservations and the release in one block at indexed positions: B1 at (10, 0, 1), a later
// reservation in registry C with the same token id at (10, 0, 2), and B1's release at (10, 0, 3).
// The later reservation is the name's current one, so the release ends nothing.
#[tokio::test]
async fn a_resourceless_release_in_the_same_block_does_not_end_the_later_reservation() -> Result<()>
{
    let facts = [
        versioned_reservation("b1-reserve", REGISTRY_B, TOKEN_B1, 10, 1),
        versioned_reservation("c1-reserve", REGISTRY_C, TOKEN_B1, 10, 2),
        versioned_release("b1-release", REGISTRY_B, TOKEN_B1, 10, 3),
    ];
    let (v1_resource, block_10, block_11) = sequence_selections(
        "resourceless_same_block",
        119,
        "resourceless-block.eth",
        &facts,
    )
    .await?;
    let ensv1 = (
        Some("ens_v1".to_owned()),
        Some("registered".to_owned()),
        Some(v1_resource),
    );
    assert_eq!(block_10, ensv1, "the later reservation is live");
    assert_eq!(block_11, ensv1);
    Ok(())
}

// Unknown identity never matches: a release without the registry instance and token id, and a
// reservation and release that both carry them as JSON null. Neither release ends the reservation,
// so ENSv1 stays selected at block 11.
#[tokio::test]
async fn a_resourceless_release_without_a_known_identity_ends_nothing() -> Result<()> {
    let missing = [
        versioned_reservation("b1-reserve", REGISTRY_B, TOKEN_B1, 10, 1),
        (
            "keyless-release",
            "RegistrationReleased",
            None,
            11,
            1,
            json!({"source_event":"LabelUnregistered","status":"released"}),
        ),
    ];
    let null = [
        (
            "null-reserve",
            "RegistrationReserved",
            None,
            10,
            1,
            json!({"source_event":"LabelReserved","expiry":4_000_000_000_i64,"status":"reserved","registry_contract_instance_id":null,"token_id":null}),
        ),
        (
            "null-release",
            "RegistrationReleased",
            None,
            11,
            1,
            json!({"source_event":"LabelUnregistered","status":"released","registry_contract_instance_id":null,"token_id":null}),
        ),
    ];
    for (case, index, name, facts) in [
        (
            "resourceless_missing_keys",
            120,
            "resourceless-missing.eth",
            &missing,
        ),
        (
            "resourceless_null_keys",
            121,
            "resourceless-null.eth",
            &null,
        ),
    ] {
        let (v1_resource, block_10, block_11) =
            sequence_selections(case, index, name, facts).await?;
        let ensv1 = (
            Some("ens_v1".to_owned()),
            Some("registered".to_owned()),
            Some(v1_resource),
        );
        assert_eq!(block_10, ensv1, "{case}");
        assert_eq!(
            block_11, ensv1,
            "{case}: an unknown identity is not a match"
        );
    }
    Ok(())
}

// One selection, not two (Pro review of b83f829c, question 1): after B0's release, B/T is reserved
// in registry B, then C/T with the same token in registry C, then C/T is unregistered. Authority
// selection's latest fact is C/T's end, which restores the released ENSv2 tombstone on B0. The
// registration section serves that same fact on B0's resource and binding: before, its own fold
// kept B/T's older reservation, served `reserved` and dropped the binding.
#[tokio::test]
async fn a_tombstone_restored_by_a_resourceless_release_serves_that_release() -> Result<()> {
    let facts = [
        versioned_reservation("bt-reserve", REGISTRY_B, TOKEN_B1, 10, 1),
        versioned_reservation("ct-reserve", REGISTRY_C, TOKEN_B1, 10, 2),
        (
            "ct-release",
            "RegistrationReleased",
            None,
            11,
            1,
            json!({"source_event":"LabelUnregistered","status":"released","released_at":CT_RELEASED_AT,"registry_contract_instance_id":REGISTRY_C,"token_id":TOKEN_B1}),
        ),
    ];
    let (_, _, block_11) = sequence_selections(
        "tombstone_after_ct_release",
        122,
        "tombstone-ct.eth",
        &facts,
    )
    .await?;
    assert_eq!(
        block_11,
        (
            Some("ens_v2".to_owned()),
            Some("unregistered".to_owned()),
            Some(uuid(15, 122))
        )
    );
    let served = served_at_11(
        "tombstone_after_ct_release",
        122,
        "tombstone-ct.eth",
        &facts,
    )
    .await?;
    assert_eq!(
        (
            served["resource_id"].as_str(),
            served["surface_binding_id"].as_str(),
            served["registration"]["status"].as_str(),
            served["registration"]["latest_event_kind"].as_str(),
            served["control"]["status"].as_str(),
            served["registration"]["released_at"].as_i64(),
        ),
        (
            Some(uuid(15, 122).as_str()),
            Some(uuid(16, 122).as_str()),
            Some("released"),
            Some("RegistrationReleased"),
            Some("unregistered"),
            Some(CT_RELEASED_AT),
        ),
        "the tombstone serves C/T's end, not B0's release: {served}"
    );
    Ok(())
}

/// Block 11's timestamp in seconds: `seed_sequence` writes it as 2026-08-26T00:00:12Z.
const BLOCK_11_SECONDS: i64 = 1_787_702_412;

// A lapsed reservation's end as the deciding fact (adversarial review of 503387dc, finding 1).
// After B0's release, B/T is reserved at block 10 with no resource, expiring at block 11's time,
// and Interpret writes its lapse at the start of block 11 by name, with no resource, and with the
// reservation's expiry and the lapse time. That end restores the released ENSv2 tombstone on B0.
// The registration section serves the fact that decided the tombstone, so its `expiry` and
// `released_at` are the reservation end's, on B0's resource and binding.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L628-L630 @ ens_v2@a971bd64)
#[tokio::test]
async fn a_lapsed_reservations_end_gives_the_tombstone_its_expiry_and_release_time() -> Result<()> {
    let facts = [
        (
            "bt-reserve",
            "RegistrationReserved",
            None,
            10,
            1,
            json!({"source_event":"LabelReserved","expiry":BLOCK_11_SECONDS,"status":"reserved","registry_contract_instance_id":REGISTRY_B,"token_id":TOKEN_B1}),
        ),
        (
            "bt-lapse",
            "RegistrationReleased",
            None,
            11,
            -1,
            json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released","expiry":BLOCK_11_SECONDS,"released_at":BLOCK_11_SECONDS,"registry_contract_instance_id":REGISTRY_B,"token_id":TOKEN_B1}),
        ),
    ];
    let (_, block_10, block_11) = sequence_selections(
        "lapsed_reservation_end",
        125,
        "lapsed-reservation.eth",
        &facts,
    )
    .await?;
    assert_eq!(
        block_10.0.as_deref(),
        Some("ens_v1"),
        "B/T is live at block 10"
    );
    assert_eq!(
        block_11,
        (
            Some("ens_v2".to_owned()),
            Some("unregistered".to_owned()),
            Some(uuid(15, 125))
        )
    );
    let (db, pool) = database("lapsed_reservation_end_served").await?;
    let (logical, _, _) = seed_sequence(&pool, 125, "lapsed-reservation.eth", &facts).await?;
    project(&pool, 11, None).await?;
    let served = served(&pool, &logical).await?;
    assert_eq!(
        (
            served["resource_id"].as_str(),
            served["surface_binding_id"].as_str(),
            served["registration"]["status"].as_str(),
            served["control"]["status"].as_str(),
            served["registration"]["expiry"].as_i64(),
            served["registration"]["released_at"].as_i64(),
        ),
        (
            Some(uuid(15, 125).as_str()),
            Some(uuid(16, 125).as_str()),
            Some("released"),
            Some("unregistered"),
            Some(BLOCK_11_SECONDS),
            Some(BLOCK_11_SECONDS),
        ),
        "{served}"
    );
    db.cleanup().await?;
    Ok(())
}
