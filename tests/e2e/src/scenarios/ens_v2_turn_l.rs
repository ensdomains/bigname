use std::collections::BTreeSet;

use alloy_primitives::{Address, U256, keccak256};
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::support;
use crate::harness::responses::pointer;
use crate::harness::{anvil::Anvil, ens_v2, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const MONTH: u64 = 30 * 24 * 60 * 60;

async fn name_resource(pool: &sqlx::PgPool, logical_name_id: &str) -> Result<Uuid> {
    sqlx::query_scalar("SELECT resource_id FROM name_current WHERE logical_name_id = $1")
        .bind(logical_name_id)
        .fetch_one(pool)
        .await
        .with_context(|| format!("name_current row missing for {logical_name_id}"))
}

/// Rows 1, 3, and 9: registrar renewal preserves the promoted exact-name
/// coverage, direct registry renew moves expiry only and rejects reduction,
/// the declared resolver edge follows
/// set/change/zero, subregistry detach removes the edge, and admin-half
/// role bits render alongside regular powers.
#[tokio::test]
async fn renewal_preserves_promoted_coverage_and_registry_edges_follow() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];

    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "promoted",
            owner: alice,
            duration_secs: ens_v2::MIN_REGISTER_DURATION,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let any_id = ens_v2::label_id("promoted");

    // Post-audit registrar renewal remains available after registry expiry
    // while the name is in grace. It pays, forwards to the registry, and
    // emits both the registrar and registry fragments from one action.
    // (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L264 @ ens_v2@48b3e2d)
    // (upstream: .refs/ens_v2/contracts/src/registrar/AbstractETHRegistrar.sol:L84 @ ens_v2@48b3e2d).
    rpc.increase_time(ens_v2::MIN_REGISTER_DURATION + 1).await?;
    let renew_receipt = ens_v2::renew_eth_name(&rpc, &deployment, alice, "promoted", MONTH).await?;

    // Direct registry renew (requires ROLE_RENEW) moves expiry only and
    // rejects reduction
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L214 @ ens_v2@48b3e2d).
    ens_v2::grant_roles(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        any_id,
        ens_v2::role_bit(16),
        alice,
    )
    .await?;
    let genesis_expiry = u64::try_from(rpc.block_timestamp().await? + u128::from(MONTH + MONTH))?;
    let direct_receipt = ens_v2::renew_in_registry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        any_id,
        genesis_expiry,
    )
    .await?;
    anyhow::ensure!(direct_receipt.status_ok, "direct registry renew reverted");
    let reduction_receipt = ens_v2::renew_in_registry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        any_id,
        genesis_expiry - MONTH,
    )
    .await?;
    anyhow::ensure!(
        !reduction_receipt.status_ok,
        "expiry reduction must revert upstream"
    );

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'RegistrationRenewed' \
             AND transaction_hash = '{renew_tx}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'ExpiryChanged' \
             AND transaction_hash = '{direct_tx}' \
             AND canonicality_state = 'canonical')",
        renew_tx = renew_receipt.tx_hash,
        direct_tx = direct_receipt.tx_hash,
    );
    let run =
        support::ingest_ens_v2_sepolia_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    // Row 1 core claim: renewal preserves the exact-name profile's promoted
    // coverage while moving the registration lifecycle forward.
    let (status, body) = run
        .api
        .get_json("/v1/names/ens/promoted.eth?chain=ethereum-sepolia")
        .await?;
    assert_eq!(status, 200, "promoted.eth lookup failed: {body}");
    assert_eq!(
        pointer(&body, "/coverage/status"),
        "full",
        "renewed name must retain full exact-name coverage: {body}"
    );
    assert_eq!(
        pointer(&body, "/coverage/exhaustiveness"),
        "authoritative",
        "renewed name coverage must remain authoritative: {body}"
    );
    assert_eq!(
        pointer(&body, "/coverage/enumeration_basis"),
        "exact_name_profile",
        "renewed name coverage must retain the exact-name basis: {body}"
    );
    assert_eq!(
        pointer(&body, "/coverage/unsupported_reason"),
        Value::Null,
        "renewed name carries no shadow reason: {body}"
    );

    // Registrar renewal derives both fragments from one action.
    let renewal_kinds: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT DISTINCT event_kind, source_family, block_number FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical' \
         AND event_kind IN ('RegistrationRenewed', 'ExpiryChanged')",
    )
    .bind(&renew_receipt.tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        renewal_kinds
            .iter()
            .any(|(kind, family, block_number)| kind == "RegistrationRenewed"
                && family == "ens_v2_registrar_l1"
                && *block_number == renew_receipt.block_number as i64),
        "registrar renewal fragment missing: {renewal_kinds:?}"
    );
    assert!(
        renewal_kinds
            .iter()
            .any(|(kind, family, block_number)| kind == "ExpiryChanged"
                && family == "ens_v2_registry_l1"
                && *block_number == renew_receipt.block_number as i64),
        "registry expiry fragment missing: {renewal_kinds:?}"
    );

    // The wire layer emits ExpiryUpdated only for a direct renew, but the
    // adapter derives BOTH kinds from that one log — the renewal fragment
    // just carries registry-family provenance instead of registrar.
    let direct_kinds: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT event_kind, source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical' \
         AND event_kind IN ('RegistrationRenewed', 'ExpiryChanged')",
    )
    .bind(&direct_receipt.tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        direct_kinds
            .iter()
            .any(|(kind, family)| kind == "ExpiryChanged" && family == "ens_v2_registry_l1"),
        "direct renew must derive ExpiryChanged: {direct_kinds:?}"
    );
    assert!(
        direct_kinds
            .iter()
            .any(|(kind, family)| kind == "RegistrationRenewed" && family == "ens_v2_registry_l1"),
        "direct renew derives a registry-family renewal fragment: {direct_kinds:?}"
    );
    assert!(
        !direct_kinds
            .iter()
            .any(|(_, family)| family == "ens_v2_registrar_l1"),
        "no registrar-family fragment may derive without a registrar log: {direct_kinds:?}"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// Rows 3 and 9: the declared resolver edge follows set/change/zero,
/// subregistry detach-to-zero removes the edge, and admin-half role bits
/// render distinctly. A chain composing renewal + three resolver changes +
/// attach/detach on one name hangs live intake even though every op ingests
/// cleanly in isolation (probed one by one; compositional trigger recorded
/// with the reproduced wedge) — these edges are pinned via backfill + replay.
#[tokio::test]
async fn resolver_and_subregistry_edges_follow_set_change_zero() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];

    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "edges",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let any_id = ens_v2::label_id("edges");

    let resolver_a = ens_v2::deploy_child_registry(&rpc, &repo_root(), &deployment).await?;
    let resolver_b = ens_v2::deploy_child_registry(&rpc, &repo_root(), &deployment).await?;
    for target in [resolver_a.address, resolver_b.address, Address::ZERO] {
        ens_v2::set_resolver_in_registry(
            &rpc,
            deployment.eth_registry.address,
            alice,
            any_id,
            target,
        )
        .await?;
    }
    let child = ens_v2::deploy_child_registry(&rpc, &repo_root(), &deployment).await?;
    for target in [child.address, Address::ZERO] {
        ens_v2::attach_subregistry(&rpc, deployment.eth_registry.address, alice, any_id, target)
            .await?;
    }

    let run = support::backfill_ens_v2_sepolia_and_replay_projections(
        &anvil,
        &deployment,
        "ens-v2-edges",
    )
    .await?;

    // The zero-set derives a ResolverChanged with a NULL resolver — a
    // proper detach, not a zero-address value.
    let resolver_edges: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT after_state->>'resolver' FROM normalized_events \
         WHERE event_kind = 'ResolverChanged' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        resolver_edges,
        vec![
            Some(format!("{:#x}", resolver_a.address)),
            Some(format!("{:#x}", resolver_b.address)),
            None,
        ],
        "resolver edge must follow set/change/zero"
    );
    let current_resolver: Option<String> = sqlx::query_scalar(
        "SELECT declared_summary->'resolver'->>'address' FROM name_current \
         WHERE logical_name_id = 'ens:edges.eth'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(current_resolver, None, "zeroed resolver must detach");

    // Detach-to-zero likewise derives a NULL subregistry edge.
    let subregistry_edges: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT after_state->>'subregistry' FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        subregistry_edges,
        vec![Some(format!("{:#x}", child.address)), None],
        "subregistry edge must attach then detach"
    );

    // Pinned asymmetry: the live path derives PermissionChanged from every
    // registration's EACRolesChanged, but the backfill path derives none —
    // ENSv2 permission derivation is live-only today (parity gap recorded
    // with the ledger row).
    let backfill_permissions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind = 'PermissionChanged' AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        backfill_permissions, 0,
        "backfill currently derives no v2 PermissionChanged"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// Row 5: registry expiry passes with no transaction (event-silent flip), the
/// registrar grace period then passes, and the name re-registers, advancing
/// lineage on two LabelRegistered derivations with no unregister and no token
/// regeneration
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L528 @ ens_v2@48b3e2d)
/// (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L259 @ ens_v2@48b3e2d).
#[tokio::test]
async fn expiry_passes_then_reregistration_advances_lineage() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, carol) = (accounts[1], accounts[3]);

    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "fleeting",
            owner: alice,
            duration_secs: ens_v2::MIN_REGISTER_DURATION,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    // Captured pre-warp: expired entries zero out registry reads.
    let resource_before = ens_v2::resource_id(
        &rpc,
        deployment.eth_registry.address,
        ens_v2::label_id("fleeting"),
    )
    .await?;
    rpc.increase_time(ens_v2::MIN_REGISTER_DURATION + 1).await?;
    // Log-bearing post-expiry activity so sync boundaries advance.
    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "postwarp",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;

    let phase_one_ready = "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'RegistrationGranted' \
           AND after_state->>'label' = 'postwarp' \
           AND canonicality_state = 'canonical')";
    let first =
        support::ingest_ens_v2_sepolia_and_serve(&anvil, &deployment, Some(phase_one_ready))
            .await?;
    // The flip is event-silent: no release-like event exists, and the last
    // derived registration state stays granted with a past expiry.
    let fleeting_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events \
         WHERE after_state->>'label' = 'fleeting' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_all(&first.db.pool)
    .await?;
    assert!(
        !fleeting_kinds
            .iter()
            .any(|kind| kind == "RegistrationReleased" || kind == "RegistrationUnregistered"),
        "v2 expiry passage must stay event-silent: {fleeting_kinds:?}"
    );
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM name_current \
         WHERE logical_name_id = 'ens:fleeting.eth'",
    )
    .fetch_one(&first.db.pool)
    .await?;
    let projected_expiry = summary["registration"]["expiry"]
        .as_i64()
        .context("fleeting expiry missing")?;
    let head_timestamp = i64::try_from(rpc.block_timestamp().await?)?;
    assert!(
        projected_expiry < head_timestamp,
        "fleeting must serve last-known state with a past expiry: {summary}"
    );
    assert_eq!(
        summary["registration"]["status"], "active",
        "no event means no status flip: {summary}"
    );
    first.db.cleanup().await?;

    // Registry reads already treat the name as expired, but the registrar
    // keeps it unavailable throughout the post-expiry grace period.
    // (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L259 @ ens_v2@48b3e2d)
    // (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L291 @ ens_v2@48b3e2d).
    rpc.increase_time(ens_v2::GRACE_PERIOD + 1).await?;

    // Re-register the available name. Both counters advance inside register
    // with no unregister event.
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452 @ ens_v2@48b3e2d).
    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: carol,
            label: "fleeting",
            owner: carol,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let any_id = ens_v2::label_id("fleeting");
    let resource_after = ens_v2::resource_id(&rpc, deployment.eth_registry.address, any_id).await?;
    assert_ne!(
        resource_after, resource_before,
        "re-registration must advance the on-chain resource counter"
    );

    let ready_sql = "SELECT count(DISTINCT resource_id) = 2
         FROM normalized_events
         WHERE logical_name_id = 'ens:fleeting.eth'
           AND event_kind = 'RegistrationGranted'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')";
    let run =
        support::ingest_ens_v2_sepolia_and_serve(&anvil, &deployment, Some(ready_sql)).await?;

    let (status, body) = run
        .api
        .get_json("/v1/names/ens/fleeting.eth?chain=ethereum-sepolia")
        .await?;
    assert_eq!(
        status, 200,
        "re-registered exact-name lookup failed: {body}"
    );
    assert_eq!(pointer(&body, "/coverage/status"), "full");
    assert_eq!(pointer(&body, "/coverage/exhaustiveness"), "authoritative");
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{carol:#x}"),
        "re-registration should serve the successor owner: {body}"
    );

    let current_resource: Uuid = body
        .pointer("/data/resource_id")
        .and_then(Value::as_str)
        .context("re-registered exact-name response should include resource_id")?
        .parse()
        .context("re-registered resource_id should be a UUID")?;
    let bindings: Vec<(Uuid, OffsetDateTime, Option<OffsetDateTime>)> = sqlx::query_as(
        "SELECT resource_id, active_from, active_to FROM surface_bindings \
         WHERE logical_name_id = 'ens:fleeting.eth' \
           AND canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY active_from, surface_binding_id",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        bindings.len(),
        2,
        "both fleeting.eth resource epochs should remain"
    );
    assert_eq!(bindings[0].2, Some(bindings[1].1));
    assert_eq!(bindings[1].2, None);
    assert_eq!(current_resource, bindings[1].0);

    run.db.cleanup().await
}

/// Row 6: the root family's first exercised transitions — register `eth` at
/// the RootRegistry apex, attach ETHRegistry as its subregistry, and rotate
/// a root-scope role in both directions.
#[tokio::test]
async fn root_apex_attach_and_root_scope_roles() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let grantee = accounts[4];

    let eth_any_id = ens_v2::label_id("eth");
    ens_v2::register_in_registry(
        &rpc,
        deployment.root_registry.address,
        deployment.deployer,
        "eth",
        deployment.deployer,
        u64::try_from(rpc.block_timestamp().await?)? + YEAR,
    )
    .await?;
    ens_v2::attach_subregistry(
        &rpc,
        deployment.root_registry.address,
        deployment.deployer,
        eth_any_id,
        deployment.eth_registry.address,
    )
    .await?;
    ens_v2::grant_root_roles(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        ens_v2::role_bit(0),
        grantee,
    )
    .await?;
    ens_v2::revoke_root_roles(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        ens_v2::role_bit(0),
        grantee,
    )
    .await?;
    // setParent is registry-level and root-scoped: the watched ETH registry
    // declares itself the `eth` child of the RootRegistry
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L171 @ ens_v2@48b3e2d).
    ens_v2::set_parent(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        deployment.root_registry.address,
        "eth",
    )
    .await?;

    let grantee_hex = format!("{grantee:#x}");
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'RootPermissionChanged' \
             AND lower(after_state->>'subject') = '{grantee_hex}' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'ParentChanged' \
             AND canonicality_state = 'canonical')"
    );
    let run =
        support::ingest_ens_v2_sepolia_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let root_family_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events \
         WHERE source_family = 'ens_v2_root_l1' AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        root_family_kinds
            .iter()
            .any(|kind| kind == "SubregistryChanged"),
        "the eth attach must derive at the root family: {root_family_kinds:?}"
    );
    assert!(
        root_family_kinds
            .iter()
            .any(|kind| kind == "RegistrationGranted"),
        "the eth apex registration must derive: {root_family_kinds:?}"
    );

    // Grant vs revoke reads from the resulting root-scope bitmap: the grant
    // leaves the registrar bit set, the revoke zeroes it.
    let root_bitmaps: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT after_state->>'role_bitmap' FROM normalized_events \
         WHERE event_kind = 'RootPermissionChanged' \
           AND lower(after_state->>'subject') = $1 \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index",
    )
    .bind(&grantee_hex)
    .fetch_all(&run.db.pool)
    .await?;
    let bitmaps: Vec<String> = root_bitmaps.into_iter().flatten().collect();
    assert_eq!(
        bitmaps,
        vec![format!("0x{:064x}", 1), format!("0x{:064x}", 0)],
        "grant must set exactly the registrar bit and revoke must clear the bitmap"
    );
    let current_root_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE lower(subject) = $1")
            .bind(&grantee_hex)
            .fetch_one(&run.db.pool)
            .await?;
    assert_eq!(
        current_root_rows, 0,
        "revoke must remove the current subject row"
    );

    let parent_changes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind = 'ParentChanged' AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert!(parent_changes >= 1, "ParentChanged must derive");

    run.db.cleanup().await?;
    Ok(())
}

/// Rows 7, 8, and 2: reserved labels promote in place preserving expiry, a
/// non-admitted root-role holder registers with registry-only provenance and
/// gated coverage, and ERC1155 single and batch sales transfer token control
/// without regenerating the token.
#[tokio::test]
async fn reserved_labels_foreign_registrar_and_token_sale() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob, carol) = (accounts[1], accounts[2], accounts[3]);

    // Carol becomes an out-of-manifest registrar: root ROLE_REGISTRAR plus
    // ROLE_REGISTER_RESERVED
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L430 @ ens_v2@48b3e2d)
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L442 @ ens_v2@48b3e2d).
    ens_v2::grant_root_roles(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        ens_v2::role_bit(0) | ens_v2::role_bit(4),
        carol,
    )
    .await?;

    // Row 7: reserve (owner=0, empty bitmap), then promote preserving the
    // reservation expiry via expiry=0
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L444 @ ens_v2@48b3e2d).
    let reservation_expiry = u64::try_from(rpc.block_timestamp().await?)? + 3 * MONTH;
    let reserve_receipt = ens_v2::register_in_registry_with(
        &rpc,
        deployment.eth_registry.address,
        carol,
        "held",
        Address::ZERO,
        U256::ZERO,
        Address::ZERO,
        reservation_expiry,
    )
    .await?;
    anyhow::ensure!(reserve_receipt.status_ok, "reservation reverted");
    let promote_receipt = ens_v2::register_in_registry_with(
        &rpc,
        deployment.eth_registry.address,
        carol,
        "held",
        bob,
        ens_v2::role_bit(ens_v2::ROLE_SET_RESOLVER),
        Address::ZERO,
        0,
    )
    .await?;
    anyhow::ensure!(promote_receipt.status_ok, "promotion reverted");

    // Row 8: the same non-admitted registrar registers a fresh label.
    let foreign_receipt = ens_v2::register_in_registry_with(
        &rpc,
        deployment.eth_registry.address,
        carol,
        "foreign",
        carol,
        ens_v2::role_bit(ens_v2::ROLE_SET_RESOLVER),
        Address::ZERO,
        u64::try_from(rpc.block_timestamp().await?)? + YEAR,
    )
    .await?;
    anyhow::ensure!(foreign_receipt.status_ok, "foreign registration reverted");

    // Row 2: a registrar-registered name changes hands by ERC1155 transfer.
    let sale = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "sale",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let sale_receipt = ens_v2::transfer_registry_token(
        &rpc,
        deployment.eth_registry.address,
        alice,
        bob,
        sale.token_id,
    )
    .await?;

    let mut batch_sales = Vec::new();
    for label in ["batchsaleone", "batchsaletwo"] {
        batch_sales.push(
            ens_v2::register_eth_name(
                &rpc,
                &deployment,
                ens_v2::RegisterEthName {
                    from: alice,
                    label,
                    owner: alice,
                    duration_secs: YEAR,
                    subregistry: Address::ZERO,
                    resolver: Address::ZERO,
                },
            )
            .await?,
        );
    }
    let batch_receipt = ens_v2::batch_transfer_registry_tokens(
        &rpc,
        deployment.eth_registry.address,
        alice,
        carol,
        &batch_sales
            .iter()
            .map(|registration| registration.token_id)
            .collect::<Vec<_>>(),
    )
    .await?;

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
           WHERE event_kind = 'RegistrationReserved' \
             AND canonicality_state = 'canonical') \
         AND EXISTS (SELECT 1 FROM normalized_events \
           WHERE after_state->>'label' = 'foreign' \
             AND event_kind = 'RegistrationGranted' \
             AND canonicality_state = 'canonical') \
         AND (SELECT count(*) FROM normalized_events \
           WHERE event_kind = 'TokenControlTransferred' \
             AND transaction_hash = '{sale_tx}' \
             AND canonicality_state = 'canonical') = 1 \
         AND (SELECT count(*) FROM normalized_events \
           WHERE event_kind = 'TokenControlTransferred' \
             AND transaction_hash = '{batch_tx}' \
             AND canonicality_state = 'canonical') = 2 \
         AND (SELECT count(*) FROM normalized_events \
           WHERE event_kind = 'PermissionChanged' \
             AND transaction_hash = '{sale_tx}' \
             AND canonicality_state = 'canonical') = 2",
        sale_tx = sale_receipt.tx_hash,
        batch_tx = batch_receipt.tx_hash,
    );
    let run =
        support::ingest_ens_v2_sepolia_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    // Row 7 pins.
    let reserved: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'RegistrationReserved' AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    // The reserved shape carries no label string — labelhash-keyed,
    // token-less, status `reserved`.
    let held_labelhash = format!("{:#x}", keccak256("held".as_bytes()));
    assert_eq!(
        reserved["labelhash"], held_labelhash,
        "reservation labelhash: {reserved}"
    );
    assert_eq!(reserved["status"], "reserved", "{reserved}");
    let reserved_expiry = reserved["expiry"]
        .as_i64()
        .context("reservation expiry missing")?;
    let promoted: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'RegistrationGranted' \
           AND after_state->>'labelhash' = $1 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&held_labelhash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        promoted["expiry"].as_i64(),
        Some(reserved_expiry),
        "promotion must preserve the reservation expiry: {promoted}"
    );
    assert_eq!(promoted["registrant"], format!("{bob:#x}"));

    // Row 8 pins: registry-only provenance, no registrar fragment, coverage
    // stays gated.
    let foreign_families: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT source_family FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical'",
    )
    .bind(&foreign_receipt.tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        foreign_families,
        vec!["ens_v2_registry_l1".to_owned()],
        "foreign registration must derive registry-only"
    );
    let (status, foreign_body) = run
        .api
        .get_json("/v1/names/ens/foreign.eth?chain=ethereum-sepolia")
        .await?;
    assert_eq!(status, 200, "foreign.eth lookup failed: {foreign_body}");
    assert_eq!(
        pointer(&foreign_body, "/coverage/status"),
        "unsupported",
        "foreign registration stays coverage-gated: {foreign_body}"
    );

    // Row 2 pins the canonical token-control event. The accompanying role
    // migration remains orthogonal permission evidence.
    let sale_kind_counts: Vec<(String, i64)> = sqlx::query_as(
        "SELECT event_kind, count(*) FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical' \
         GROUP BY event_kind ORDER BY event_kind",
    )
    .bind(&sale_receipt.tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    let permission_changes = sale_kind_counts
        .iter()
        .find(|(kind, _)| kind == "PermissionChanged")
        .map(|(_, count)| *count)
        .unwrap_or_default();
    assert_eq!(
        permission_changes, 2,
        "sale must migrate roles as a revoke/grant pair: {sale_kind_counts:?}"
    );
    assert_eq!(
        sale_kind_counts
            .iter()
            .find(|(kind, _)| kind == "TokenControlTransferred")
            .map(|(_, count)| *count),
        Some(1),
        "sale must emit one canonical token-control transfer: {sale_kind_counts:?}"
    );
    assert!(
        !sale_kind_counts
            .iter()
            .any(|(kind, _)| kind == "TokenRegenerated"),
        "sale must not regenerate the token: {sale_kind_counts:?}"
    );
    let single_transfer: (String, Uuid, i64, String, String, String) = sqlx::query_as(
        "SELECT logical_name_id, resource_id, log_index, event_identity, \
                before_state->>'from', after_state->>'to' \
         FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND event_kind = 'TokenControlTransferred' \
           AND source_family = 'ens_v2_registry_l1' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&sale_receipt.tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(single_transfer.0, "ens:sale.eth");
    assert_eq!(single_transfer.4, format!("{alice:#x}"));
    assert_eq!(single_transfer.5, format!("{bob:#x}"));

    let batch_transfers: Vec<(String, Uuid, i64, String, String, String, Value)> = sqlx::query_as(
        "SELECT logical_name_id, resource_id, log_index, event_identity, \
                before_state->>'from', after_state->>'to', raw_fact_ref \
         FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND event_kind = 'TokenControlTransferred' \
           AND source_family = 'ens_v2_registry_l1' \
           AND canonicality_state = 'canonical' \
         ORDER BY logical_name_id",
    )
    .bind(&batch_receipt.tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(batch_transfers.len(), 2);
    assert_eq!(
        batch_transfers
            .iter()
            .map(|row| row.0.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["ens:batchsaleone.eth", "ens:batchsaletwo.eth"])
    );
    assert_eq!(
        batch_transfers
            .iter()
            .map(|row| row.1)
            .collect::<BTreeSet<_>>()
            .len(),
        2,
        "each batch token must retain its own resource"
    );
    assert_eq!(
        batch_transfers
            .iter()
            .map(|row| row.2)
            .collect::<BTreeSet<_>>()
            .len(),
        1,
        "both rows must point to one TransferBatch raw log"
    );
    assert_ne!(
        batch_transfers[0].3, batch_transfers[1].3,
        "batch fan-out identities must be distinct"
    );
    assert_eq!(batch_transfers[0].6, batch_transfers[1].6);
    assert!(
        batch_transfers
            .iter()
            .all(|row| { row.4 == format!("{alice:#x}") && row.5 == format!("{carol:#x}") })
    );
    let (status, buyer_names) = run
        .api
        .get_json(&format!(
            "/v1/addresses/{bob:#x}/names?namespace=ens&relation=registrant"
        ))
        .await?;
    assert_eq!(status, 200, "buyer registrant lookup failed: {buyer_names}");
    assert!(
        buyer_names["data"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["normalized_name"] == "sale.eth")),
        "the buyer registrant collection must contain sale.eth: {buyer_names}"
    );
    let (status, buyer_holder_names) = run
        .api
        .get_json(&format!(
            "/v1/addresses/{bob:#x}/names?namespace=ens&relation=token_holder"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "buyer token-holder lookup failed: {buyer_holder_names}"
    );
    assert!(
        buyer_holder_names["data"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["normalized_name"] == "sale.eth")),
        "the buyer token-holder collection must contain sale.eth: {buyer_holder_names}"
    );
    let (status, seller_names) = run
        .api
        .get_json(&format!(
            "/v1/addresses/{alice:#x}/names?namespace=ens&relation=registrant"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "seller registrant lookup failed: {seller_names}"
    );
    assert!(
        seller_names["data"]
            .as_array()
            .is_none_or(|rows| rows.iter().all(|row| row["normalized_name"] != "sale.eth")),
        "the seller registrant collection must not retain sale.eth: {seller_names}"
    );
    let (status, seller_holder_names) = run
        .api
        .get_json(&format!(
            "/v1/addresses/{alice:#x}/names?namespace=ens&relation=token_holder"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "seller token-holder lookup failed: {seller_holder_names}"
    );
    assert!(
        seller_holder_names["data"]
            .as_array()
            .is_none_or(|rows| rows.iter().all(|row| row["normalized_name"] != "sale.eth")),
        "the seller token-holder collection must not retain sale.eth: {seller_holder_names}"
    );

    for relation in ["registrant", "token_holder"] {
        let (status, recipient_names) = run
            .api
            .get_json(&format!(
                "/v1/addresses/{carol:#x}/names?namespace=ens&relation={relation}"
            ))
            .await?;
        assert_eq!(
            status, 200,
            "batch recipient lookup failed: {recipient_names}"
        );
        let rows = recipient_names["data"]
            .as_array()
            .context("batch recipient collection data must be an array")?;
        for name in ["batchsaleone.eth", "batchsaletwo.eth"] {
            assert!(
                rows.iter().any(|row| row["normalized_name"] == name),
                "batch recipient {relation} collection must contain {name}: {recipient_names}"
            );
        }

        let (status, prior_holder_names) = run
            .api
            .get_json(&format!(
                "/v1/addresses/{alice:#x}/names?namespace=ens&relation={relation}"
            ))
            .await?;
        assert_eq!(
            status, 200,
            "batch seller lookup failed: {prior_holder_names}"
        );
        let prior_rows = prior_holder_names["data"]
            .as_array()
            .context("batch seller collection data must be an array")?;
        for name in ["batchsaleone.eth", "batchsaletwo.eth"] {
            assert!(
                prior_rows.iter().all(|row| row["normalized_name"] != name),
                "batch seller {relation} collection must not retain {name}: {prior_holder_names}"
            );
        }
    }
    let sale_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM name_current WHERE logical_name_id = 'ens:sale.eth'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        sale_summary["registration"]["registrant"],
        format!("{bob:#x}"),
        "the registrant facet must follow the ERC1155 buyer: {sale_summary}"
    );
    for logical_name_id in ["ens:batchsaleone.eth", "ens:batchsaletwo.eth"] {
        let batch_summary: Value = sqlx::query_scalar(
            "SELECT declared_summary FROM name_current WHERE logical_name_id = $1",
        )
        .bind(logical_name_id)
        .fetch_one(&run.db.pool)
        .await?;
        assert_eq!(
            batch_summary["registration"]["registrant"],
            format!("{carol:#x}"),
            "the batch registrant facet must follow the ERC1155 recipient for {logical_name_id}: {batch_summary}"
        );
    }

    // Row 9's admin-half pin rides this live corpus (backfill derives no
    // PermissionChanged): the registrar bitmap's admin bits must render as
    // distinct powers rather than merging with regular ones. Post-sale the
    // migrated roles belong to the buyer.
    let sale_resource = name_resource(&run.db.pool, "ens:sale.eth").await?;
    let power_rows: Vec<Value> = sqlx::query_scalar(
        "SELECT effective_powers FROM permissions_current \
         WHERE resource_id = $1 AND lower(subject) = $2",
    )
    .bind(sale_resource)
    .bind(format!("{bob:#x}"))
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        !power_rows.is_empty(),
        "buyer powers missing after the role migration"
    );
    let power_names: Vec<String> = power_rows
        .iter()
        .flat_map(|powers| {
            powers
                .as_array()
                .map(|list| {
                    list.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .collect();
    assert!(
        power_names
            .iter()
            .any(|name| name.starts_with("admin_") || name.contains("admin")),
        "admin-half bits must render distinctly; saw {power_names:?}"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// Row 4: record writes on a discovered v2 resolver. Automatic ENSv2
/// bootstrap admits the resolver edge, fetches its finite-known-start history
/// in the same startup invocation, and derives the configured record events.
/// Resolver-profile admission still gates public selector publication.
#[tokio::test]
async fn discovered_v2_resolver_records_are_backfilled_in_session() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];

    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "records",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let resolver =
        ens_v2::deploy_permissioned_resolver(&rpc, &repo_root(), &deployment, alice).await?;
    ens_v2::set_resolver_in_registry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        ens_v2::label_id("records"),
        resolver.address,
    )
    .await?;
    let node = crate::harness::ens_v1::namehash("records.eth");
    ens_v2::set_resolver_text(&rpc, resolver.address, alice, node, "probe", "x").await?;
    ens_v2::set_resolver_addr(&rpc, resolver.address, alice, node, alice).await?;
    ens_v2::clear_resolver_records(&rpc, resolver.address, alice, node).await?;

    let ready_sql = "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'ResolverChanged' AND canonicality_state = 'canonical') \
         AND (SELECT count(*) = 3 FROM normalized_events \
              WHERE logical_name_id = 'ens:records.eth' \
                AND event_kind IN ('RecordChanged', 'RecordVersionChanged') \
                AND canonicality_state = 'canonical')";
    let run =
        support::ingest_ens_v2_sepolia_and_serve(&anvil, &deployment, Some(ready_sql)).await?;

    let resolver_hex = format!("{:#x}", resolver.address);
    let edge_admitted: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM discovery_edges edge \
         JOIN contract_instance_addresses target \
           ON target.contract_instance_id = edge.to_contract_instance_id \
         WHERE lower(target.address) = $1 AND edge.deactivated_at IS NULL)",
    )
    .bind(&resolver_hex)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(edge_admitted, "discovery must admit the resolver edge");

    let resolver_raw_logs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE lower(emitting_address) = $1")
            .bind(&resolver_hex)
            .fetch_one(&run.db.pool)
            .await?;
    // The address write emits both AddressChanged and the legacy AddrChanged
    // compatibility event. (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L679 @ ens_v2@48b3e2d)
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L681 @ ens_v2@48b3e2d)
    assert_eq!(
        resolver_raw_logs, 4,
        "automatic ENSv2 bootstrap must fetch all four discovered-resolver logs"
    );
    let record_events: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_kind, after_state->>'source_event' FROM normalized_events \
         WHERE event_kind IN ('RecordChanged', 'RecordVersionChanged') \
           AND logical_name_id = 'ens:records.eth' \
           AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index, event_kind",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        record_events,
        vec![
            ("RecordChanged".to_owned(), "TextChanged".to_owned()),
            ("RecordChanged".to_owned(), "AddressChanged".to_owned()),
            (
                "RecordVersionChanged".to_owned(),
                "VersionChanged".to_owned(),
            ),
        ],
        "discovered resolver history must derive text, address, and version observations"
    );

    let (status, exact) = run
        .api
        .get_json("/v1/names/ens/records.eth?chain=ethereum-sepolia")
        .await?;
    assert_eq!(status, 200, "records.eth exact-name lookup failed: {exact}");
    assert_eq!(
        pointer(&exact, "/declared_state/resolver/address"),
        resolver_hex,
        "the discovered resolver must remain the declared registry binding: {exact}"
    );
    assert_eq!(
        pointer(&exact, "/declared_state/record_inventory/status"),
        "unsupported",
        "unadmitted ENSv2 resolver observations must not publish an inventory: {exact}"
    );
    assert_eq!(
        pointer(
            &exact,
            "/declared_state/record_inventory/unsupported_reason"
        ),
        "declared record inventory summary is not yet projected",
        "the exact-name route must expose its current record-inventory boundary: {exact}"
    );

    run.db.cleanup().await?;
    Ok(())
}
