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
    let (resource, binding, status): (Option<String>, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT resource_id::text, surface_binding_id::text,
                    declared_summary #>> '{registration,status}'
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical)
        .fetch_one(&pool)
        .await?;
    // Known gap: the served registration block still reads ENSv2 lifecycle events by name, so
    // it does not see the nameless release and serves the grant. How that block counts an event
    // with a resource and no name is the open question the TYR-36 step 3 report left for a ruling
    // (`unnamed_resource_event_in_key_state`). On Interpret output a named release comes first,
    // so the block serves `released` there.
    assert_eq!(
        (resource.as_deref(), binding.as_deref(), status.as_deref()),
        (
            Some(v2_resource.as_str()),
            Some(v2_binding.as_str()),
            Some("active")
        )
    );
    db.cleanup().await?;
    Ok(())
}
