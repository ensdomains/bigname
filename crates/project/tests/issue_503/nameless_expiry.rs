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
// The name's registration section follows the same selection (product ruling of 2026-09-26): it
// serves the release authority selection chose, not the old grant, as batches at blocks 9 and 10
// and as one batch.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
//
/// The synthetic nameless-release shape: a live ENSv1 lease, an ENSv2 grant and resolver in block 9
/// on a binding closed before block 10, and the release at the start of block 10 on that
/// resource without a name.
async fn seed_nameless_release(pool: PgPool) -> Result<String> {
    let pool = &pool;
    earlier_block(pool).await?;
    let logical = surface(pool, 97, "nameless-expiry.eth", &[]).await?;
    let v1_resource = open_binding(pool, &logical, 97, "ens_v1", 1).await?;
    event(
        pool,
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
    let (v2_resource, _) = closed_binding_at_block_9(pool, &logical, 97, "ens_v2").await?;
    event(
        pool,
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
    // A resolver the registration set, so the released row's resolver suppression is visible.
    event(
        pool,
        "nameless-expiry-v2-resolver",
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "ResolverChanged",
            log: 3,
            after: json!({"resolver":"0x0000000000000000000000000000000000000bee"}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity IN ('nameless-expiry-v2-grant', 'nameless-expiry-v2-resolver')")
        .bind(EARLIER_HASH).execute(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state) VALUES ('nameless-expiry-v2-release', 'ens', NULL, $1::uuid, 'RegistrationReleased', 'ens_v2_registry_l1', 1, $2, 10, $3, NULL, NULL, NULL, 'ens_v2_registry_resource_surface', 'canonical', $4)")
        .bind(&v2_resource).bind(CHAIN).bind(HASH)
        .bind(json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released"}))
        .execute(pool).await?;
    Ok(logical)
}

#[tokio::test]
async fn a_nameless_path_expiry_release_keeps_the_name_on_its_v2_tombstone() -> Result<()> {
    let served = served_both_ways("nameless_path_expiry", seed_nameless_release).await?;
    assert_eq!(
        (
            served["authority_arm"].as_str(),
            served["lifecycle_state"].as_str(),
            served["resource_id"].as_str(),
            served["surface_binding_id"].as_str(),
            served["registration"]["status"].as_str(),
            served["registration"]["latest_event_kind"].as_str(),
            served["control"]["status"].as_str(),
        ),
        (
            Some("ens_v2"),
            Some("unregistered"),
            Some(uuid(15, 97).as_str()),
            Some(uuid(16, 97).as_str()),
            Some("released"),
            Some("RegistrationReleased"),
            Some("unregistered"),
        ),
        "the registration section serves the nameless release, as authority selection does: {served}"
    );
    // The released row serves no registrant and no resolver, as for any released ENSv2 row.
    assert_eq!(
        (
            served["registration"]["registrant"].as_str(),
            served["resolver"]["chain_id"].as_str(),
            served["resolver"]["address"].as_str(),
        ),
        (None, None, None),
        "{served}"
    );
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

async fn project_at(
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

/// Seeds the same rows twice and projects them once as batches at blocks 9 and 10 and once as
/// one batch at block 10. Both must serve the same fields, which are returned.
async fn served_both_ways<F, Fut>(prefix: &str, seed: F) -> Result<Value>
where
    F: Fn(PgPool) -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    let (stepped_db, stepped) = database(&format!("{prefix}_stepped")).await?;
    let logical = seed(stepped.clone()).await?;
    let at_9 = project_at(&stepped, 9, None).await?;
    project_at(&stepped, 10, Some(at_9)).await?;
    let stepped_served = served(&stepped, &logical).await?;
    stepped_db.cleanup().await?;
    let (one_db, one) = database(&format!("{prefix}_one")).await?;
    let logical = seed(one.clone()).await?;
    project_at(&one, 10, None).await?;
    assert_eq!(
        served(&one, &logical).await?,
        stepped_served,
        "{prefix}: one batch and batches at blocks 9 and 10 serve the same fields"
    );
    one_db.cleanup().await?;
    Ok(stepped_served)
}

/// A release at the start of block 10, with no transaction or log index, on `resource`, by
/// `logical` or without a name, with Interpret's path-expiry markers.
async fn boundary_release(
    pool: &PgPool,
    identity: &str,
    logical: Option<&str>,
    resource: &str,
    extra: Value,
) -> Result<()> {
    let mut after = json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released"});
    if let (Some(after), Some(extra)) = (after.as_object_mut(), extra.as_object()) {
        after.extend(extra.clone());
    }
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state) VALUES ($1, 'ens', $2, $3::uuid, 'RegistrationReleased', 'ens_v2_registry_l1', 1, $4, 10, $5, NULL, NULL, NULL, 'ens_v2_registry_resource_surface', 'canonical', $6)")
        .bind(identity).bind(logical).bind(resource).bind(CHAIN).bind(HASH).bind(after)
        .execute(pool).await?;
    Ok(())
}

// A block-boundary release and an indexed regrant on the same resource later in the same block
// (Pro review of b83f829c, question 2). The rows are hand-built: the regrant keeps the released
// registration's resource and opens a binding on it at (10, 0, 1). Authority selection orders by
// block, transaction, log and event id with the boundary release first in its block, so the
// regrant is the later fact and the name is registered. The registration section must agree
// whichever row has the higher event id; before, its fold ordered by block and event id only and
// served the release when it was written second. The case runs with the release named, as
// Interpret writes it for a named entry, and without a name, and with each row written first.
#[tokio::test]
async fn an_indexed_regrant_after_a_boundary_release_in_the_same_block_is_the_later_fact()
-> Result<()> {
    for named in [true, false] {
        for release_first in [true, false] {
            let seed = |pool: PgPool| async move {
                earlier_block(&pool).await?;
                let logical = surface(&pool, 123, "boundary-regrant.eth", &[]).await?;
                let (resource, _) =
                    closed_binding_at_block_9(&pool, &logical, 123, "ens_v2").await?;
                event(
                    &pool,
                    "boundary-regrant-grant",
                    &logical,
                    Some(&resource),
                    Event {
                        family: "ens_v2_registry_l1",
                        kind: "RegistrationGranted",
                        log: 2,
                        after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
                    },
                )
                .await?;
                sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity = 'boundary-regrant-grant'")
                    .bind(EARLIER_HASH).execute(&pool).await?;
                sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2', '2026-08-26T00:00:00Z', $4, $5, 10, '{\"transaction_index\":0,\"log_index\":1}', 'canonical')")
                    .bind(uuid(17, 123)).bind(&logical).bind(&resource).bind(CHAIN).bind(HASH)
                    .execute(&pool).await?;
                let release = |pool: PgPool, logical: String, resource: String| async move {
                    boundary_release(
                        &pool,
                        "boundary-regrant-release",
                        named.then_some(logical.as_str()),
                        &resource,
                        json!({}),
                    )
                    .await
                };
                if release_first {
                    release(pool.clone(), logical.clone(), resource.clone()).await?;
                }
                event(
                    &pool,
                    "boundary-regrant-regrant",
                    &logical,
                    Some(&resource),
                    Event {
                        family: "ens_v2_registry_l1",
                        kind: "RegistrationGranted",
                        log: 1,
                        after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000003"}),
                    },
                )
                .await?;
                if !release_first {
                    release(pool.clone(), logical.clone(), resource.clone()).await?;
                }
                Ok(logical)
            };
            let served =
                served_both_ways(&format!("boundary_regrant_{named}_{release_first}"), seed)
                    .await?;
            assert_eq!(
                (
                    served["resource_id"].as_str(),
                    served["surface_binding_id"].as_str(),
                    served["registration"]["status"].as_str(),
                    served["registration"]["latest_event_kind"].as_str(),
                    served["control"]["status"].as_str(),
                ),
                (
                    Some(uuid(15, 123).as_str()),
                    Some(uuid(17, 123).as_str()),
                    Some("active"),
                    Some("RegistrationGranted"),
                    Some("registered"),
                ),
                "named release: {named}, release written first: {release_first}: {served}"
            );
        }
    }
    Ok(())
}

/// Block 10's timestamp in seconds: `database` writes it as 2026-08-26T00:00:00Z.
const BLOCK_10_TIME: i64 = 1_787_702_400;

// A version-zero reservation already expired when written and its release at the start of the
// same block, for a name with no ENSv2 binding and nothing on ENSv1 (Pro review of b83f829c,
// question 2). The rows are Interpret's shape for a named entry. The reservation is never live, so
// the registration section's fold leaves it out, as authority selection does, and serves the
// release whichever row has the higher event id. Without that, ordering by position would make the
// indexed reservation the later fact.
#[tokio::test]
async fn a_reservation_expired_when_written_does_not_outrank_its_boundary_release() -> Result<()> {
    for reservation_first in [true, false] {
        let seed = |pool: PgPool| async move {
            earlier_block(&pool).await?;
            let logical = surface(&pool, 124, "expired-fold.eth", &[]).await?;
            let resource = uuid(21, 124);
            sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
                .bind(&resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
            let reserve = |pool: PgPool, logical: String, resource: String| async move {
                event(
                    &pool,
                    "expired-fold-reserve",
                    &logical,
                    Some(&resource),
                    Event {
                        family: "ens_v2_registry_l1",
                        kind: "RegistrationReserved",
                        log: 1,
                        after: json!({"source_event":"LabelReserved","expiry":BLOCK_10_TIME,"status":"reserved","reservation_resource":true}),
                    },
                )
                .await
            };
            if reservation_first {
                reserve(pool.clone(), logical.clone(), resource.clone()).await?;
            }
            boundary_release(
                &pool,
                "expired-fold-release",
                Some(&logical),
                &resource,
                json!({"expiry":BLOCK_10_TIME,"released_at":BLOCK_10_TIME}),
            )
            .await?;
            if !reservation_first {
                reserve(pool.clone(), logical.clone(), resource.clone()).await?;
            }
            Ok(logical)
        };
        let served = served_both_ways(&format!("expired_fold_{reservation_first}"), seed).await?;
        assert_eq!(
            (
                served["registration"]["status"].as_str(),
                served["registration"]["latest_event_kind"].as_str(),
            ),
            (Some("released"), Some("RegistrationReleased")),
            "reservation written first: {reservation_first}: {served}"
        );
    }
    Ok(())
}

/// The nameless-release shape with `release` merged into the release's payload, the release
/// written by the name when `named`, and a later `ExpiryChanged` to 40 by the name on the same
/// resource in block 10.
async fn seed_release_before_a_named_expiry_change(
    pool: PgPool,
    release: Value,
    named: bool,
) -> Result<String> {
    let logical = seed_nameless_release(pool.clone()).await?;
    sqlx::query(
        "UPDATE normalized_events
         SET after_state = after_state || $2, logical_name_id = CASE WHEN $3 THEN $1 END
         WHERE event_identity = 'nameless-expiry-v2-release'",
    )
    .bind(&logical)
    .bind(release)
    .bind(named)
    .execute(&pool)
    .await?;
    event(
        &pool,
        "nameless-expiry-v2-expiry-change",
        &logical,
        Some(&uuid(15, 97)),
        Event {
            family: "ens_v2_registry_l1",
            kind: "ExpiryChanged",
            log: 5,
            after: json!({"expiry":40}),
        },
    )
    .await?;
    Ok(logical)
}

// Which expiry a released tombstone serves when a named expiry row follows its release (Pro review
// of 56825409, question 4). A path-expiry release that carries its expiry serves it, 30, over the
// later named `ExpiryChanged` to 40. When the release carries no expiry, or a JSON null, the
// name's expiry rows decide, 40. An explicit release serves no expiry at all.
#[tokio::test]
async fn a_path_expiry_releases_own_expiry_precedes_a_later_named_expiry_change() -> Result<()> {
    for (case, release, named, expiry) in [
        ("release_expiry", json!({"expiry":30}), false, Some(30)),
        ("release_without_expiry", json!({}), false, Some(40)),
        (
            "release_null_expiry",
            json!({"expiry":null}),
            false,
            Some(40),
        ),
        (
            "explicit_release",
            json!({"source_event":"LabelUnregistered","released_at":1_787_702_400_i64}),
            true,
            None,
        ),
    ] {
        let served = served_both_ways(&format!("expiry_precedence_{case}"), |pool| {
            seed_release_before_a_named_expiry_change(pool, release.clone(), named)
        })
        .await?;
        assert_eq!(
            (
                served["authority_arm"].as_str(),
                served["resource_id"].as_str(),
                served["registration"]["status"].as_str(),
                served["registration"]["expiry"].as_i64(),
            ),
            (
                Some("ens_v2"),
                Some(uuid(15, 97).as_str()),
                Some("released"),
                expiry,
            ),
            "{case}: {served}"
        );
    }
    Ok(())
}
