use anyhow::{Context, Result};
use sqlx::types::Uuid;

use super::support;
use crate::harness::responses::{exact_name, pointer};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const GRACE_PERIOD: i64 = 90 * 24 * 60 * 60;
const CANNOT_UNWRAP: u16 = 1;
const CANNOT_TRANSFER: u16 = 4;
const CANNOT_SET_RESOLVER: u16 = 8;
const PARENT_CANNOT_CONTROL: u32 = 1 << 16;
const IS_DOT_ETH: u32 = 1 << 17;
const LOCKED_PARENT_FUSES: u32 = IS_DOT_ETH
    | PARENT_CANNOT_CONTROL
    | CANNOT_UNWRAP as u32
    | CANNOT_TRANSFER as u32
    | CANNOT_SET_RESOLVER as u32;

async fn resource_token_lineage(
    run: &support::PipelineRun,
    resource_id: Uuid,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar("SELECT token_lineage_id FROM resources WHERE resource_id = $1")
        .bind(resource_id)
        .fetch_one(&run.db.pool)
        .await
        .context("resource token lineage lookup failed")
}

async fn authority_resource(
    run: &support::PipelineRun,
    logical_name_id: &str,
    authority_kind: &str,
) -> Result<Uuid> {
    sqlx::query_scalar(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = $1
           AND after_state->>'authority_kind' = $2
           AND resource_id IS NOT NULL
           AND canonicality_state = 'canonical'
         ORDER BY block_number, log_index, normalized_event_id
         LIMIT 1",
    )
    .bind(logical_name_id)
    .bind(authority_kind)
    .fetch_one(&run.db.pool)
    .await
    .with_context(|| format!("missing {authority_kind} resource for {logical_name_id}"))
}

/// One wrapper-heavy chain covers the phase-4 matrix:
/// - `locked.eth` stays wrapped, burns fuses, and creates wrapped children.
/// - `restore.eth` wraps and then unwraps before lease end, so the second
///   ingest confirms the prior registrar anchor and token lineage reactivate.
#[tokio::test]
async fn wrapper_wrap_fuses_subnames_and_unwrap_restore_identity() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob, carol) = (accounts[1], accounts[2], accounts[3]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "locked", alice, YEAR, resolver).await?;
    ens_v1::register_eth_name(&rpc, &deployment, "restore", alice, YEAR, resolver).await?;

    ens_v1::wrap_eth_2ld(&rpc, &deployment, alice, "locked", bob, 0, resolver).await?;
    ens_v1::wrap_eth_2ld(&rpc, &deployment, alice, "restore", bob, 0, resolver).await?;
    ens_v1::set_wrapper_fuses(
        &rpc,
        &deployment,
        bob,
        "locked.eth",
        CANNOT_UNWRAP | CANNOT_TRANSFER | CANNOT_SET_RESOLVER,
    )
    .await?;
    ens_v1::set_wrapped_subnode_owner(
        &rpc,
        &deployment,
        bob,
        ens_v1::WrappedSubnodeOwner {
            parent: "locked.eth",
            label: "kid",
            owner: carol,
            fuses: PARENT_CANNOT_CONTROL,
            expiry: u64::MAX,
        },
    )
    .await?;
    ens_v1::set_wrapped_subnode_record(
        &rpc,
        &deployment,
        bob,
        ens_v1::WrappedSubnodeRecord {
            parent: "locked.eth",
            label: "record",
            owner: carol,
            resolver,
            fuses: PARENT_CANNOT_CONTROL,
            expiry: u64::MAX,
        },
    )
    .await?;

    let ready_sql = format!(
        "SELECT EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'ens:0x669e8ea725c56427a7bca9ffaed126a8922a2b2baf4ed71a3fe74d871d0dd25b'
               AND event_kind = 'PermissionScopeChanged'
               AND canonicality_state = 'canonical'
               AND after_state->>'fuses' = '{LOCKED_PARENT_FUSES}'
         ) AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'ens:0xcc64a0f57a49fdd7ecb6d34cb1ab8ee14ee4686529ec038f47b31dfd1abf8a14'
               AND event_kind = 'PermissionScopeChanged'
               AND canonicality_state = 'canonical'
               AND after_state->>'fuses' = '{PARENT_CANNOT_CONTROL}'
         ) AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'ens:0x1b4c8e619992e455d6bcbe34df7b61027846e7699cf23cc5a9fe883e52f495fa'
               AND event_kind = 'ResolverChanged'
               AND canonicality_state = 'canonical'
         )"
    );
    let wrapped = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let locked_registrar_resource = authority_resource(
        &wrapped,
        "ens:0x669e8ea725c56427a7bca9ffaed126a8922a2b2baf4ed71a3fe74d871d0dd25b",
        "registrar",
    )
    .await?;
    let locked_wrapper_resource = authority_resource(
        &wrapped,
        "ens:0x669e8ea725c56427a7bca9ffaed126a8922a2b2baf4ed71a3fe74d871d0dd25b",
        "wrapper",
    )
    .await?;
    let locked_registrar_lineage =
        resource_token_lineage(&wrapped, locked_registrar_resource).await?;
    let locked_wrapper_lineage = resource_token_lineage(&wrapped, locked_wrapper_resource).await?;
    assert!(
        locked_registrar_lineage.is_some(),
        "registrar lease should mint a token lineage"
    );
    assert!(
        locked_wrapper_lineage.is_some(),
        "wrapped position should mint a wrapper token lineage"
    );
    assert_ne!(
        locked_registrar_lineage, locked_wrapper_lineage,
        "wrap must rotate token lineage rather than reusing the registrar lineage"
    );

    let locked_body = exact_name(&wrapped.api, "ens", "locked.eth").await?;
    let locked_wrapper_resource_string = locked_wrapper_resource.to_string();
    let locked_wrapper_lineage_string = locked_wrapper_lineage.map(|lineage| lineage.to_string());
    assert_eq!(
        pointer(&locked_body, "/data/resource_id").as_str(),
        Some(locked_wrapper_resource_string.as_str()),
        "wrapped name should be bound to the wrapper resource; body: {locked_body}"
    );
    assert_eq!(
        pointer(&locked_body, "/data/token_lineage_id").as_str(),
        locked_wrapper_lineage_string.as_deref(),
        "wrapped name should expose the wrapper token lineage; body: {locked_body}"
    );
    assert_eq!(
        pointer(&locked_body, "/declared_state/registration/registrant"),
        format!("{bob:#x}"),
        "current token-control subject should be the wrapped holder; body: {locked_body}"
    );
    assert_eq!(
        pointer(&locked_body, "/declared_state/control/registrant"),
        format!("{bob:#x}"),
        "wrapped-flow reconciliation should route control to the holder; body: {locked_body}"
    );
    let authority_key = pointer(&locked_body, "/declared_state/registration/authority_key");
    assert!(
        authority_key
            .as_str()
            .is_some_and(|key| key.starts_with("wrapper:")),
        "registration authority must follow the wrapper resource; body: {locked_body}"
    );
    let wrapper_owner_transfer: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'ens:0x669e8ea725c56427a7bca9ffaed126a8922a2b2baf4ed71a3fe74d871d0dd25b' AND event_kind = 'AuthorityTransferred' \
         AND canonicality_state = 'canonical' AND lower(after_state->>'owner') = $1",
    )
    .bind(format!("{:#x}", deployment.name_wrapper.address))
    .fetch_one(&wrapped.db.pool)
    .await?;
    assert!(
        wrapper_owner_transfer >= 1,
        "the wrap's registry-owner transfer to the NameWrapper must derive canonically"
    );

    let registrar_expiry: i64 = sqlx::query_scalar(
        "SELECT (after_state->>'expiry')::BIGINT
         FROM normalized_events
         WHERE logical_name_id = 'ens:0x669e8ea725c56427a7bca9ffaed126a8922a2b2baf4ed71a3fe74d871d0dd25b'
           AND event_kind = 'RegistrationGranted'
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&wrapped.db.pool)
    .await?;
    let wrapper_expiry: i64 = sqlx::query_scalar(
        "SELECT (after_state->>'expiry')::BIGINT
         FROM normalized_events
         WHERE logical_name_id = 'ens:0x669e8ea725c56427a7bca9ffaed126a8922a2b2baf4ed71a3fe74d871d0dd25b'
           AND event_kind = 'ExpiryChanged'
           AND source_family = 'ens_v1_wrapper_l1'
           AND canonicality_state = 'canonical'
         ORDER BY block_number DESC, log_index DESC
         LIMIT 1",
    )
    .fetch_one(&wrapped.db.pool)
    .await?;
    assert_eq!(
        wrapper_expiry,
        registrar_expiry + GRACE_PERIOD,
        "NameWrapper.wrapETH2LD should project wrapper expiry as registrar expiry plus grace"
    );
    assert_eq!(
        pointer(&locked_body, "/declared_state/registration/expiry"),
        registrar_expiry,
        "exact-name registration expiry should follow the registrar lease"
    );
    assert_eq!(
        pointer(&locked_body, "/declared_state/wrapper_state"),
        "locked",
        "locked.eth should expose its effective NameWrapper state"
    );

    let fuse_scopes: Vec<(i64, String)> = sqlx::query_as(
        "SELECT (after_state->>'fuses')::BIGINT, after_state->>'wrapper_state'
         FROM normalized_events \
         WHERE logical_name_id = 'ens:0x669e8ea725c56427a7bca9ffaed126a8922a2b2baf4ed71a3fe74d871d0dd25b' \
         AND event_kind = 'PermissionScopeChanged' \
         AND source_family = 'ens_v1_wrapper_l1' \
         AND canonicality_state = 'canonical' \
         ORDER BY normalized_event_id",
    )
    .fetch_all(&wrapped.db.pool)
    .await?;
    assert_eq!(
        fuse_scopes,
        vec![(196_608, "emancipated".into()), (196_621, "locked".into())],
        "wrap then setFuses should derive the expected lifecycle states"
    );
    let wrapper_subject_grants: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM permissions_current \
         WHERE subject = $1 AND scope = 'resource' \
         AND effective_powers ? 'resource_control'",
    )
    .bind(format!("{:#x}", deployment.name_wrapper.address))
    .fetch_one(&wrapped.db.pool)
    .await?;
    assert!(
        wrapper_subject_grants >= 1,
        "the NameWrapper contract should hold the registrar-anchor resource_control grant"
    );
    let locked_wrapper_permissions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1")
            .bind(locked_wrapper_resource)
            .fetch_one(&wrapped.db.pool)
            .await?;
    assert_eq!(
        locked_wrapper_permissions, 0,
        "wrapper resources without subject-grant events must not invent holder grants"
    );
    let locked_holder_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current \
         WHERE lower(address) = $1 AND namespace = 'ens' \
           AND raw_name = 'locked.eth' AND resource_id = $2",
    )
    .bind(format!("{bob:#x}"))
    .bind(locked_wrapper_resource)
    .fetch_one(&wrapped.db.pool)
    .await?;
    assert!(
        locked_holder_rows >= 1,
        "locked.eth must remain in the wrapper holder's schema-v2 address-name projection"
    );
    let locked_controller_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current
         WHERE raw_name = 'locked.eth' AND resource_id = $1
           AND relation = 'effective_controller'",
    )
    .bind(locked_wrapper_resource)
    .fetch_one(&wrapped.db.pool)
    .await?;
    assert_eq!(locked_controller_rows, 0);

    let kid_body = exact_name(&wrapped.api, "ens", "kid.locked.eth").await?;
    assert_eq!(
        pointer(&kid_body, "/declared_state/control/registrant"),
        format!("{carol:#x}"),
        "wrapper-born child control should follow its holder; body: {kid_body}"
    );
    assert_eq!(
        pointer(&kid_body, "/declared_state/registration/authority_kind"),
        "wrapper",
        "wrapper-created child should have wrapper authority; body: {kid_body}"
    );
    assert_eq!(
        pointer(&kid_body, "/declared_state/registration/registrant"),
        format!("{carol:#x}"),
        "wrapped child holder should be the owner passed to setSubnodeOwner"
    );
    assert_eq!(
        pointer(&kid_body, "/declared_state/wrapper_state"),
        "emancipated"
    );
    let kid_resource: Uuid = pointer(&kid_body, "/data/resource_id")
        .as_str()
        .context("kid.locked.eth resource_id missing")?
        .parse()
        .context("kid.locked.eth resource_id should be a UUID")?;
    assert_ne!(
        kid_resource, locked_wrapper_resource,
        "wrapped child should have its own resource_id"
    );
    let kid_permissions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1")
            .bind(kid_resource)
            .fetch_one(&wrapped.db.pool)
            .await?;
    assert_eq!(
        kid_permissions, 0,
        "PARENT_CANNOT_CONTROL wrapper children must not publish parent or holder grants"
    );

    let record_body = exact_name(&wrapped.api, "ens", "record.locked.eth").await?;
    assert_eq!(
        pointer(&record_body, "/declared_state/resolver/address"),
        format!("{resolver:#x}"),
        "setSubnodeRecord should project the child resolver; body: {record_body}"
    );
    assert_ne!(
        pointer(&record_body, "/data/resource_id"),
        pointer(&locked_body, "/data/resource_id"),
        "record.locked.eth should be a separate wrapped child resource"
    );
    assert_eq!(
        pointer(&record_body, "/declared_state/wrapper_state"),
        "emancipated"
    );

    wrapped.db.cleanup().await?;

    ens_v1::unwrap_eth_2ld(&rpc, &deployment, bob, "restore", alice, alice).await?;
    let unwrapped = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (
                 SELECT 1 FROM normalized_events
                 WHERE logical_name_id = 'ens:0xa7ca555cc876d903448642a3291764a22342c0bd09c7283ca5007923875f79a5'
                   AND event_kind = 'AuthorityEpochChanged'
                   AND canonicality_state = 'canonical'
                   AND before_state->>'authority_kind' = 'wrapper'
                   AND after_state->>'authority_kind' = 'registrar'
             )",
        ),
    )
    .await?;

    let restore_registrar_resource = authority_resource(
        &unwrapped,
        "ens:0xa7ca555cc876d903448642a3291764a22342c0bd09c7283ca5007923875f79a5",
        "registrar",
    )
    .await?;
    let restore_wrapper_resource = authority_resource(
        &unwrapped,
        "ens:0xa7ca555cc876d903448642a3291764a22342c0bd09c7283ca5007923875f79a5",
        "wrapper",
    )
    .await?;
    let restore_registrar_lineage =
        resource_token_lineage(&unwrapped, restore_registrar_resource).await?;
    let restore_wrapper_lineage =
        resource_token_lineage(&unwrapped, restore_wrapper_resource).await?;
    assert!(restore_registrar_lineage.is_some());
    assert!(restore_wrapper_lineage.is_some());
    assert_ne!(
        restore_registrar_lineage, restore_wrapper_lineage,
        "wrapper token lineage should remain distinct from the registrar lineage"
    );

    let restore_body = exact_name(&unwrapped.api, "ens", "restore.eth").await?;
    let restore_registrar_resource_string = restore_registrar_resource.to_string();
    let restore_registrar_lineage_string =
        restore_registrar_lineage.map(|lineage| lineage.to_string());
    assert_eq!(
        pointer(&restore_body, "/data/resource_id").as_str(),
        Some(restore_registrar_resource_string.as_str()),
        "unwrap before lease end should reactivate the prior registrar resource; body: {restore_body}"
    );
    assert_eq!(
        pointer(&restore_body, "/data/token_lineage_id").as_str(),
        restore_registrar_lineage_string.as_deref(),
        "unwrap should reactivate the prior registrar token lineage; body: {restore_body}"
    );
    assert_eq!(
        pointer(&restore_body, "/declared_state/registration/registrant"),
        format!("{alice:#x}"),
        "registrar token should return to the requested registrant on unwrap"
    );
    assert_eq!(
        pointer(&restore_body, "/declared_state/control/registry_owner"),
        format!("{alice:#x}"),
        "registry controller should return to the requested owner on unwrap"
    );

    let event_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind
         FROM normalized_events
         WHERE logical_name_id = 'ens:0xa7ca555cc876d903448642a3291764a22342c0bd09c7283ca5007923875f79a5'
           AND canonicality_state = 'canonical'",
    )
    .fetch_all(&unwrapped.db.pool)
    .await?;
    for expected in [
        "SurfaceBound",
        "SurfaceUnbound",
        "AuthorityTransferred",
        "TokenControlTransferred",
    ] {
        assert!(
            event_kinds.iter().any(|kind| kind == expected),
            "restore.eth should carry {expected} through wrap/unwrap identity transitions; saw {event_kinds:?}"
        );
    }

    unwrapped.db.cleanup().await?;
    Ok(())
}
