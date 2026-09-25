use super::*;

// A path-expiry release without a name (TYR-36 step 6, adversarial review F3). When Interpret
// retires an ENSv2 token that has no name any more, it writes the release at the block boundary on
// the token's resource with no name and no transaction or log index
// (crates/adapters/src/schema_v2/protocol/v2_registry/expiry.rs). The release must still count as
// the latest fact of the registration the name was last bound to, so the expired registration
// stays with ENSv2 as a released tombstone beside a live ENSv1 lease (product ruling of
// 2026-09-25).
// The row shape is Interpret's. The sequence is synthetic: on Interpret output the path cut that
// takes the token's name also writes a named release first (see production_interpret
// `a_detached_child_expiry_is_released_without_a_name_and_stays_a_v2_tombstone`), and this case
// leaves that release out so the nameless one decides alone.
// The name's registration section follows the same selection (Tate's ruling of 2026-09-26): it
// reads the nameless release on the resource the name was last bound to, so it serves the release
// too, not the old grant.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
#[tokio::test]
async fn a_nameless_path_expiry_release_keeps_the_name_on_its_v2_tombstone() -> Result<()> {
    let (db, pool) = database("nameless_path_expiry").await?;
    earlier_block(&pool).await?;
    let logical = surface(&pool, 97, "nameless-expiry.eth", &[]).await?;
    let v1_resource = open_binding(&pool, &logical, 97, "ens_v1", 1).await?;
    event(
        &pool,
        "nameless-expiry-v1-grant",
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
    let (v2_resource, v2_binding) =
        closed_binding_at_block_9(&pool, &logical, 97, "ens_v2").await?;
    event(
        &pool,
        "nameless-expiry-v2-grant",
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationGranted",
            log: 2,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity = 'nameless-expiry-v2-grant'")
        .bind(EARLIER_HASH).execute(&pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state) VALUES ('nameless-expiry-v2-release', 'ens', NULL, $1::uuid, 'RegistrationReleased', 'ens_v2_registry_l1', 1, $2, 10, $3, NULL, NULL, NULL, 'ens_v2_registry_resource_surface', 'canonical', $4)")
        .bind(&v2_resource).bind(CHAIN).bind(HASH)
        .bind(json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released"}))
        .execute(&pool).await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?.0.as_deref(),
        Some("ens_v2"),
        "the nameless expiry release keeps the name with ENSv2"
    );
    assert_eq!(
        lifecycle_state(&pool, &logical).await?.as_deref(),
        Some("unregistered")
    );
    type Served = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let served: Served = sqlx::query_as(
        "SELECT resource_id::text, surface_binding_id::text,
                declared_summary #>> '{registration,status}',
                declared_summary #>> '{registration,latest_event_kind}',
                declared_summary #>> '{control,status}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (
            served.0.as_deref(),
            served.1.as_deref(),
            served.2.as_deref(),
            served.3.as_deref(),
            served.4.as_deref(),
        ),
        (
            Some(v2_resource.as_str()),
            Some(v2_binding.as_str()),
            Some("released"),
            Some("RegistrationReleased"),
            Some("unregistered"),
        ),
        "the registration section serves the nameless release, as authority selection does"
    );
    db.cleanup().await?;
    Ok(())
}

// A block-boundary release and a later indexed reservation in the same block (review thread on
// the latest-fact ordering). Interpret writes the path-expiry release with no transaction or log
// index at the start of the block, and a `LabelReserved` later in that block carries its log
// position. The reservation is the later fact and hands the name to its live ENSv1 lease; with
// descending NULLS FIRST ordering the release would have sorted after it.
#[tokio::test]
async fn a_reservation_after_a_block_boundary_release_in_the_same_block_is_the_later_fact()
-> Result<()> {
    let (db, pool) = database("boundary_release_then_reservation").await?;
    earlier_block(&pool).await?;
    let logical = surface(&pool, 98, "boundary-reserve.eth", &[]).await?;
    let v1_resource = open_binding(&pool, &logical, 98, "ens_v1", 1).await?;
    event(
        &pool,
        "boundary-reserve-v1-grant",
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
    let (v2_resource, _) = closed_binding_at_block_9(&pool, &logical, 98, "ens_v2").await?;
    event(
        &pool,
        "boundary-reserve-v2-grant",
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationGranted",
            log: 2,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity = 'boundary-reserve-v2-grant'")
        .bind(EARLIER_HASH).execute(&pool).await?;
    // The expiry release at the start of block 10, named, on the registration's resource.
    event(
        &pool,
        "boundary-reserve-v2-release",
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
    sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE event_identity = 'boundary-reserve-v2-release'")
        .execute(&pool).await?;
    // The reservation later in block 10, by name and without a resource.
    event(
        &pool,
        "boundary-reserve-v2-reservation",
        &logical,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReserved",
            log: 5,
            after: json!({"status":"reserved"}),
        },
    )
    .await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?.0.as_deref(),
        Some("ens_v1"),
        "the indexed reservation is later than the block-boundary release"
    );
    let resource: Option<String> =
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id = $1")
            .bind(&logical)
            .fetch_one(&pool)
            .await?;
    assert_eq!(resource.as_deref(), Some(v1_resource.as_str()));
    db.cleanup().await?;
    Ok(())
}

// Two derived releases of the same registration at the same block-boundary position (Pro review
// of e18e509e, question 5). When Interpret retires the name's path and the token's own expiry in
// the same block, it writes the named path release and the nameless expiry release both at
// (10, NULL, NULL) on the registration's resource. They tie on position and fall to
// normalized_event_id, so either can be the latest fact; both end the same registration, so the
// name is the released ENSv2 tombstone either way. The case runs with each release written first.
#[tokio::test]
async fn two_boundary_releases_of_one_registration_give_the_same_tombstone_either_way() -> Result<()>
{
    for named_first in [true, false] {
        let (db, pool) = database(&format!("two_boundary_releases_{named_first}")).await?;
        earlier_block(&pool).await?;
        let logical = surface(&pool, 112, "two-releases.eth", &[]).await?;
        let v1_resource = open_binding(&pool, &logical, 112, "ens_v1", 1).await?;
        event(
            &pool,
            "two-releases-v1-grant",
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
        let (v2_resource, v2_binding) =
            closed_binding_at_block_9(&pool, &logical, 112, "ens_v2").await?;
        event(
            &pool,
            "two-releases-v2-grant",
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind: "RegistrationGranted",
                log: 2,
                after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
            },
        )
        .await?;
        sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity = 'two-releases-v2-grant'")
            .bind(EARLIER_HASH).execute(&pool).await?;
        let named = |pool: PgPool, logical: String, resource: String| async move {
            sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state) VALUES ('two-releases-named', 'ens', $1, $2::uuid, 'RegistrationReleased', 'ens_v2_registry_l1', 1, $3, 10, $4, NULL, NULL, NULL, 'ens_v2_registry_resource_surface', 'canonical', $5)")
                .bind(logical).bind(resource).bind(CHAIN).bind(HASH)
                .bind(json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released"}))
                .execute(&pool).await
        };
        let nameless = |pool: PgPool, resource: String| async move {
            sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state) VALUES ('two-releases-nameless', 'ens', NULL, $1::uuid, 'RegistrationReleased', 'ens_v2_registry_l1', 1, $2, 10, $3, NULL, NULL, NULL, 'ens_v2_registry_resource_surface', 'canonical', $4)")
                .bind(resource).bind(CHAIN).bind(HASH)
                .bind(json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released"}))
                .execute(&pool).await
        };
        if named_first {
            named(pool.clone(), logical.clone(), v2_resource.clone()).await?;
            nameless(pool.clone(), v2_resource.clone()).await?;
        } else {
            nameless(pool.clone(), v2_resource.clone()).await?;
            named(pool.clone(), logical.clone(), v2_resource.clone()).await?;
        }
        run(&pool).await?;
        assert_eq!(
            authority(&pool, &logical).await?.0.as_deref(),
            Some("ens_v2"),
            "named release written first: {named_first}"
        );
        assert_eq!(
            lifecycle_state(&pool, &logical).await?.as_deref(),
            Some("unregistered"),
            "named release written first: {named_first}"
        );
        let (resource, binding): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT resource_id::text, surface_binding_id::text
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            (resource.as_deref(), binding.as_deref()),
            (Some(v2_resource.as_str()), Some(v2_binding.as_str())),
            "named release written first: {named_first}"
        );
        db.cleanup().await?;
    }
    Ok(())
}
