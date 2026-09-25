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
