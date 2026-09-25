use super::*;

// A reservation observed already expired (TYR-36 step 6, Pro review of 6ecb5edd, question 1).
// An ownerless `register` may write a nonzero expiry that is already at or before its block's
// timestamp, and the registry reports such an entry as available, never reserved. Interpret
// keeps the raw reservation at its log position and writes its state-derived release in the same
// block at the block boundary, with no transaction or log index
// (crates/adapters/src/schema_v2/protocol/v2_registry.rs, `immediate_expiry`). The reservation
// must not outrank that release and hand a released ENSv2 name to a live ENSv1 lease.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452-L454 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L628-L630 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L654-L660 @ ens_v2@a971bd64)

/// Block 10's timestamp in seconds: `database` writes it as 2026-08-26T00:00:00Z.
const BLOCK_10_SECONDS: i64 = 1_787_702_400;

/// What the fixture writes besides the ENSv1 lease, the ENSv2 grant and the reservation.
#[derive(Clone, Copy)]
struct Shape {
    /// Interpret's block-boundary release of the reservation at (10, NULL, NULL), by name, as it
    /// writes one for a reservation expired when written.
    derived_release: bool,
    /// The reservation and its derived release carry the reservation's own resource, as a
    /// version-zero reservation does; otherwise neither has a resource, as a versioned one.
    own_resource: bool,
    /// The ENSv2 registration is unregistered in block 9. Without it, the derived release is the
    /// only release, so the tombstone shows that it is kept.
    owned_release: bool,
}

/// The registry instance Interpret writes on the reservation and its derived release.
const EXPIRED_REGISTRY: &str = "0000000e-0000-0000-0000-00000000000e";

/// The reservation's token id, written on the reservation and its derived release.
fn expired_token(index: u16) -> String {
    format!("0x{index:064x}")
}

const RESOURCELESS: Shape = Shape {
    derived_release: true,
    own_resource: false,
    owned_release: true,
};

/// A live ENSv1 lease; an ENSv2 registration granted, and unless `shape` says otherwise
/// unregistered, in block 9; and in block 10 a reservation of the name at (10, 1, 1) whose expiry
/// is `expiry`, with what `shape` adds. Returns the name and the ENSv1 and ENSv2 resources.
async fn seed(
    pool: &PgPool,
    index: u16,
    name: &str,
    expiry: i64,
    shape: Shape,
) -> Result<(String, String, String)> {
    earlier_block(pool).await?;
    let logical = surface(pool, index, name, &[]).await?;
    let v1_resource = open_binding(pool, &logical, index, "ens_v1", 1).await?;
    event(
        pool,
        "expired-reservation-v1-grant",
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
    let (v2_resource, _) = closed_binding_at_block_9(pool, &logical, index, "ens_v2").await?;
    for (identity, kind, log, after) in [
        (
            "expired-reservation-v2-grant",
            "RegistrationGranted",
            2,
            json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (
            "expired-reservation-v2-release",
            "RegistrationReleased",
            3,
            json!({"source_event":"LabelUnregistered","status":"released"}),
        ),
    ]
    .into_iter()
    .filter(|(_, kind, _, _)| shape.owned_release || *kind != "RegistrationReleased")
    {
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
    let own_resource = uuid(21, index);
    if shape.own_resource {
        sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
            .bind(&own_resource).bind(CHAIN).bind(HASH).execute(pool).await?;
    }
    let reservation_resource = shape.own_resource.then_some(own_resource.as_str());
    event(
        pool,
        "expired-reservation-reserve",
        &logical,
        reservation_resource,
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReserved",
            log: 1,
            after: json!({"source_event":"LabelReserved","expiry":expiry,"status":"reserved","registry_contract_instance_id":EXPIRED_REGISTRY,"token_id":expired_token(index)}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET transaction_index = 1 WHERE event_identity = 'expired-reservation-reserve'")
        .execute(pool).await?;
    if shape.derived_release {
        event(
            pool,
            "expired-reservation-derived-release",
            &logical,
            reservation_resource,
            Event {
                family: "ens_v2_registry_l1",
                kind: "RegistrationReleased",
                log: 0,
                after: json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","expiry":expiry,"status":"released","registry_contract_instance_id":EXPIRED_REGISTRY,"token_id":expired_token(index)}),
            },
        )
        .await?;
        sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE event_identity = 'expired-reservation-derived-release'")
            .execute(pool).await?;
    }
    Ok((logical, v1_resource, v2_resource))
}

/// The selected arm, lifecycle state and resource.
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

/// Seeds the case twice and projects it once in one batch to block 10 and once to block 9 and
/// then incrementally to block 10. Asserts both give the same selection and returns it with the
/// ENSv1 and ENSv2 resources.
async fn full_and_incremental(
    prefix: &str,
    index: u16,
    name: &str,
    expiry: i64,
    shape: Shape,
) -> Result<(
    (Option<String>, Option<String>, Option<String>),
    String,
    String,
)> {
    let (full_db, full) = database(&format!("{prefix}_full")).await?;
    let (logical, v1_resource, v2_resource) = seed(&full, index, name, expiry, shape).await?;
    run(&full).await?;
    let full_selection = selection(&full, &logical).await?;
    full_db.cleanup().await?;
    let (incremental_db, incremental) = database(&format!("{prefix}_incremental")).await?;
    seed(&incremental, index, name, expiry, shape).await?;
    let first = Engine::new(incremental.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 9,
            affected_from_block: 9,
            affected_to_block: 9,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?
        .current;
    Engine::new(incremental.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 10,
            affected_from_block: 10,
            affected_to_block: 10,
            resume_current: Some(first),
            mode: RunMode::Normal,
        })
        .await?;
    let incremental_selection = selection(&incremental, &logical).await?;
    incremental_db.cleanup().await?;
    assert_eq!(
        incremental_selection, full_selection,
        "{prefix}: incremental and full runs agree"
    );
    Ok((full_selection, v1_resource, v2_resource))
}

// (a) The reservation's expiry equals its block's timestamp: expired from the start.
#[tokio::test]
async fn a_reservation_expiring_at_its_own_block_leaves_the_v2_tombstone() -> Result<()> {
    let (selected, _, v2_resource) = full_and_incremental(
        "expired_reservation_at_block",
        99,
        "expired-at-block.eth",
        BLOCK_10_SECONDS,
        RESOURCELESS,
    )
    .await?;
    assert_eq!(
        selected,
        (
            Some("ens_v2".into()),
            Some("unregistered".into()),
            Some(v2_resource)
        ),
        "a reservation expired when written is available, not reserved"
    );
    Ok(())
}

// (b) The reservation's expiry is before its block's timestamp.
#[tokio::test]
async fn a_reservation_expired_before_its_own_block_leaves_the_v2_tombstone() -> Result<()> {
    let (selected, _, v2_resource) = full_and_incremental(
        "expired_reservation_before_block",
        100,
        "expired-before-block.eth",
        BLOCK_10_SECONDS - 1,
        RESOURCELESS,
    )
    .await?;
    assert_eq!(
        selected,
        (
            Some("ens_v2".into()),
            Some("unregistered".into()),
            Some(v2_resource)
        ),
        "a reservation expired when written is available, not reserved"
    );
    Ok(())
}

// (c) The converse: the reservation is live at its block, so Interpret writes no release for it,
// and it hands the name to the live ENSv1 lease.
#[tokio::test]
async fn a_live_reservation_in_the_same_shape_hands_the_name_to_ensv1() -> Result<()> {
    let (selected, v1_resource, _) = full_and_incremental(
        "live_reservation_same_shape",
        101,
        "live-reservation.eth",
        BLOCK_10_SECONDS + 1,
        Shape {
            derived_release: false,
            ..RESOURCELESS
        },
    )
    .await?;
    assert_eq!(
        (selected.0.as_deref(), selected.2.as_deref()),
        (Some("ens_v1"), Some(v1_resource.as_str())),
        "a live reservation defers to the live ENSv1 lease"
    );
    Ok(())
}

// The same expired-at-write reservation at version zero (Pro review of e18e509e, question 4): the
// reservation and its derived release both carry the reservation's own resource, as Interpret
// writes them for a version-zero token. With the expiry equal to the block time and before it,
// the reservation stays out and the name is the released ENSv2 tombstone on the old bound
// resource. Without the owned release in block 9, the derived release is the only release, so the
// tombstone also shows that it is kept.
#[tokio::test]
async fn a_version_zero_reservation_expired_when_written_keeps_its_release_and_the_tombstone()
-> Result<()> {
    for (case, index, name, expiry, owned_release) in [
        (
            "v0_expired_at_block",
            108,
            "v0-at-block.eth",
            BLOCK_10_SECONDS,
            true,
        ),
        (
            "v0_expired_before_block",
            109,
            "v0-before-block.eth",
            BLOCK_10_SECONDS - 1,
            true,
        ),
        (
            "v0_expired_only_release",
            110,
            "v0-only-release.eth",
            BLOCK_10_SECONDS,
            false,
        ),
        (
            "v0_expired_before_only_release",
            111,
            "v0-before-only.eth",
            BLOCK_10_SECONDS - 1,
            false,
        ),
    ] {
        let (selected, _, v2_resource) = full_and_incremental(
            case,
            index,
            name,
            expiry,
            Shape {
                derived_release: true,
                own_resource: true,
                owned_release,
            },
        )
        .await?;
        assert_eq!(
            selected,
            (
                Some("ens_v2".into()),
                Some("unregistered".into()),
                Some(v2_resource)
            ),
            "{case}"
        );
    }
    Ok(())
}

// The converse boundary in one block: the old registration's expiry release at the start of block
// 10, with no transaction or log index, and a reservation of the name later in that block whose
// numeric expiry is still ahead of the block's timestamp. The reservation is live, so the
// already-expired rule leaves it in, and it is the later fact: the name goes to its live ENSv1
// lease. Run in one batch and as two incremental batches.
#[tokio::test]
async fn a_live_reservation_after_a_start_of_block_release_hands_the_name_to_ensv1() -> Result<()> {
    async fn seed_boundary(pool: &PgPool) -> Result<(String, String)> {
        earlier_block(pool).await?;
        let logical = surface(pool, 103, "boundary-live.eth", &[]).await?;
        let v1_resource = open_binding(pool, &logical, 103, "ens_v1", 1).await?;
        event(
            pool,
            "boundary-live-v1-grant",
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
        let (v2_resource, _) = closed_binding_at_block_9(pool, &logical, 103, "ens_v2").await?;
        event(
            pool,
            "boundary-live-v2-grant",
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind: "RegistrationGranted",
                log: 2,
                after: json!({"source_event":"LabelRegistered","status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
            },
        )
        .await?;
        sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity = 'boundary-live-v2-grant'")
            .bind(EARLIER_HASH).execute(pool).await?;
        event(
            pool,
            "boundary-live-v2-expiry",
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind: "RegistrationReleased",
                log: 0,
                after: json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released"}),
            },
        )
        .await?;
        sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE event_identity = 'boundary-live-v2-expiry'")
            .execute(pool).await?;
        event(
            pool,
            "boundary-live-reserve",
            &logical,
            None,
            Event {
                family: "ens_v2_registry_l1",
                kind: "RegistrationReserved",
                log: 1,
                after: json!({"source_event":"LabelReserved","expiry":BLOCK_10_SECONDS + 3_600,"status":"reserved"}),
            },
        )
        .await?;
        sqlx::query("UPDATE normalized_events SET transaction_index = 1 WHERE event_identity = 'boundary-live-reserve'")
            .execute(pool).await?;
        Ok((logical, v1_resource))
    }

    let (full_db, full) = database("boundary_live_reservation_full").await?;
    let (logical, v1_resource) = seed_boundary(&full).await?;
    run(&full).await?;
    let full_selection = selection(&full, &logical).await?;
    full_db.cleanup().await?;
    assert_eq!(
        (
            full_selection.0.as_deref(),
            full_selection.1.as_deref(),
            full_selection.2.as_deref()
        ),
        (
            Some("ens_v1"),
            Some("registered"),
            Some(v1_resource.as_str())
        ),
        "the live reservation is later than the start-of-block release"
    );
    let (incremental_db, incremental) = database("boundary_live_reservation_incremental").await?;
    seed_boundary(&incremental).await?;
    let first = Engine::new(incremental.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 9,
            affected_from_block: 9,
            affected_to_block: 9,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?
        .current;
    Engine::new(incremental.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 10,
            affected_from_block: 10,
            affected_to_block: 10,
            resume_current: Some(first),
            mode: RunMode::Normal,
        })
        .await?;
    assert_eq!(selection(&incremental, &logical).await?, full_selection);
    incremental_db.cleanup().await?;
    Ok(())
}
