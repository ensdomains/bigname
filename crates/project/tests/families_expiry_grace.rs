//! The `.eth` expiry and grace a composed name serves around the Universal Resolver cutover
//! (TYR-90, TYR-282). A chain is cut over while its deployment profile admits an ENSv2 root
//! registry. On such a chain a `.eth` name that ENSv1 still decides and that ENSv2 premigration
//! reserved serves the reservation's expiry, and a `.eth` name that ENSv1 decides without a live
//! ENSv2 entry, and every name below it, serve no resolver. On a chain with no admitted root
//! registry the ENSv1 lease's expiry and the 90-day ENSv1 grace apply. The client-facing
//! Universal Resolver proxy's `Upgraded` events are kept for monitoring and move no name. These
//! synthetic ENSv2 entries use an arbitrary registry and no admitted ETHRegistry declaration, so
//! their canonical grace end equals expiry. The `.eth` suffix and the cutover do not establish
//! registrar grace. The registration time of such a name is its ENSv1 registration's through
//! renewals, the reservation and the ENSv1→ENSv2 migration. Only a new registration after a
//! release starts a new one (TYR-131).
#[path = "families_expiry_grace/declared_path.rs"]
mod declared_path;
#[path = "families_support/mod.rs"]
mod support;

use alloy_primitives::{B256, keccak256};
use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use bigname_storage::families::name::load_family_name;
use serde_json::{Value, json};
use support::{CHAIN, Event, Fixture, hash, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
// A generic registry, deliberately distinct from the admitted Sepolia ETHRegistry.
const V2_REGISTRY: &str = "0x00000000000000000000000000000000000000e6";
const NAME_WRAPPER: &str = "0x00000000000000000000000000000000000000e7";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const TOP_PROXY: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const MANAGED_PROXY: &str = "0x6d80f2172cfdec5730fe683860c33d26fc42e6f1";
const ADMITTED: &str = "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3";
const OLD_IMPLEMENTATION: &str = "0x2f8a1806000000000000000000000000000000aa";
const DAY: u64 = 86_400;
const LEASE_EXPIRY: u64 = 2_000_000_000;
/// Premigration reserves the lease's expiry plus the 62-day continuity bonus.
const RESERVED_EXPIRY: u64 = LEASE_EXPIRY + 62 * DAY;

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |parent, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(parent.as_slice());
        input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        keccak256(input)
    })
}

/// The surface of `name` with its real namehash and label hashes, so the reader places it
/// relative to `.eth`. Returns its logical name id.
async fn surface(fixture: &Fixture, name: &str) -> Result<String> {
    let node = format!("{:#x}", namehash(name));
    let logical_name_id = format!("ens:{node}");
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let labelhashes: Vec<String> = normalized
        .normalized_labels
        .iter()
        .map(|label| format!("{:#x}", keccak256(label.as_bytes())))
        .collect();
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', $5, $6, $7, $2, $8, $9, 'active', $3, $4, 0, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(&node)
    .bind(CHAIN)
    .bind(hash(0))
    .bind(normalized.normalized_name)
    .bind(normalized.normalized_labels)
    .bind(normalized.dns_encoded_name)
    .bind(labelhashes)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .execute(&fixture.pool)
    .await?;
    Ok(logical_name_id)
}

/// `name`'s ENSv1 lease `n`, bound at `block` under ens_v1 and granted one block later until
/// `LEASE_EXPIRY`, with a resolver set in the ENSv1 registry at `block + 2`.
async fn leased(fixture: &Fixture, name: &str, n: u32, block: i64) -> Result<String> {
    let logical_name_id = surface(fixture, name).await?;
    lease(fixture, &logical_name_id, n, block, LEASE_EXPIRY).await?;
    Ok(logical_name_id)
}

/// Lease `n` of the surface `logical_name_id`, as `leased` writes it, granted until `expiry`.
async fn lease(
    fixture: &Fixture,
    logical_name_id: &str,
    n: u32,
    block: i64,
    expiry: u64,
) -> Result<()> {
    let lease = uuid(0x1000 + n);
    fixture
        .binding(
            &uuid(100 + n),
            logical_name_id,
            &lease,
            "ens_v1",
            block,
            0,
            None,
        )
        .await?;
    fixture
        .write(
            block,
            0,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(logical_name_id),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            block + 1,
            0,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(logical_name_id),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": expiry}),
            REGISTRAR,
        )
        .await?;
    resolver(fixture, logical_name_id, block + 2).await
}

async fn resolver(fixture: &Fixture, logical_name_id: &str, block: i64) -> Result<()> {
    let node = logical_name_id.trim_start_matches("ens:");
    let identity = format!("resolver-{logical_name_id}-{block}");
    fixture
        .event(
            Event::new(&identity, block, 5, "ResolverChanged", "ens_v1_registry_l1")
                .name(logical_name_id)
                .after(json!({"resolver": RESOLVER, "node": node}))
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    Ok(())
}

/// A synthetic premigration-shaped reservation of `logical_name_id` in a generic ENSv2
/// registry. The string identifier `eth` is not proof of admitted registrar grace.
async fn reserved(fixture: &Fixture, logical_name_id: &str, n: u32, block: i64) -> Result<()> {
    fixture
        .write(
            block,
            1,
            "RegistrationReserved",
            "ens_v2_registry_l1",
            Some(logical_name_id),
            Some(&uuid(0x2000 + n)),
            json!({"registry_contract_instance_id": "eth", "token_id": format!("{n}"),
                   "status": "reserved", "expiry": RESERVED_EXPIRY}),
            V2_REGISTRY,
        )
        .await?;
    Ok(())
}

/// The manifest-sync input Project captures before publishing a block. Reusing its id rotates
/// the declaration while historical proxy upgrades remain readable.
async fn execution_manifest(
    fixture: &Fixture,
    manifest_id: Option<i64>,
    block: i64,
    top: &str,
    start: i64,
) -> Result<i64> {
    let payload = json!({"contracts": [
        {"role": "universal_resolver", "address": top, "start_block": start},
        {"role": "universal_resolver_managed", "address": MANAGED_PROXY, "start_block": 0}
    ], "universal_resolver_implementations": [ADMITTED]});
    let id = match manifest_id {
        Some(id) => id,
        None => {
            sqlx::query_scalar(
                "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                 chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                 manifest_payload)
             VALUES (1, 'ens', 'ens_execution', $1, 'fixture', 'active', 'fixture',
                 'fixture/ens_execution.toml', $2) RETURNING manifest_id",
            )
            .bind(CHAIN)
            .bind(&payload)
            .fetch_one(&fixture.pool)
            .await?
        }
    };
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state)
         VALUES ($1, 'ens', 'SourceManifestUpdated', 'ens_execution', 1, $2, $3, $4, $5,
             'manifest_sync', 'canonical',
             jsonb_build_object('rollout_status', 'active', 'manifest_payload', $6::jsonb))",
    )
    .bind(format!("execution-manifest:{id}:{block}"))
    .bind(id)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(payload)
    .execute(&fixture.pool)
    .await?;
    Ok(id)
}

/// An `Upgraded` of a declared Universal Resolver proxy, as the adapter classifies it.
async fn upgraded(
    fixture: &Fixture,
    block: i64,
    proxy: &str,
    role: &str,
    implementation: &str,
    kind: &str,
) -> Result<()> {
    let identity = format!("upgraded-{block}-{proxy}");
    fixture
        .event(
            Event::new(&identity, block, 9, "Upgraded", "ens_execution")
                .after(json!({"source_event": "Upgraded", "proxy_address": proxy,
                              "proxy_role": role, "implementation": implementation,
                              "implementation_kind": kind}))
                .raw(json!({"emitting_address": proxy})),
        )
        .await?;
    Ok(())
}

/// The served expiry, the ENSv1 lease's own expiry, grace end, resolver address and
/// resolution reasons of a composed name.
async fn served(fixture: &Fixture, logical_name_id: &str) -> Result<Value> {
    let row = load_family_name(&fixture.pool, logical_name_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no composed row"))?;
    let summary = &row.declared_summary;
    Ok(json!({
        "arm": row.provenance["authority_selection"]["authority_arm"],
        "expiry": summary["registration"]["expiry"],
        "ens_v1_expiry": summary["registration"]["ens_v1_expiry"],
        "grace_ends_at": summary["registration"]["grace_ends_at"],
        "resolver": summary["resolver"]["address"],
        "unresolvable_reason": summary.get("unresolvable_reason").cloned().unwrap_or(Value::Null),
        "resolution_unsupported_reason": summary.get("resolution_unsupported_reason").cloned().unwrap_or(Value::Null),
    }))
}

async fn summary_expiry(fixture: &Fixture, logical_name_id: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT expires_at::bigint FROM project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_one(&fixture.pool)
    .await?)
}

fn expect(expiry: u64, grace_days: u64, resolver: Option<&str>, reason: Option<&str>) -> Value {
    json!({
        "arm": "ens_v1",
        "expiry": expiry.to_string(),
        // The lease's own date, whichever expiry the name serves.
        "ens_v1_expiry": LEASE_EXPIRY.to_string(),
        "grace_ends_at": (expiry + grace_days * DAY).to_string(),
        "resolver": resolver,
        "unresolvable_reason": reason,
        "resolution_unsupported_reason": Value::Null,
    })
}

/// The declared Universal Resolver proxies' rows, as monitoring reads them: address, role and
/// the classification of the implementation.
async fn proxy_rows(fixture: &Fixture) -> Result<Vec<(String, Option<String>, String)>> {
    Ok(sqlx::query_as(
        "SELECT proxy_address, proxy_role, implementation_kind
         FROM project_universal_resolver_proxy WHERE chain_id = $1 ORDER BY proxy_address",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?)
}

#[tokio::test]
async fn an_admitted_ens_v2_root_registry_cuts_the_chain_over_with_no_proxy_event() -> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_admitted", 12).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    reserved(&fixture, &alice, 1, 4).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    ensure!(proxy_rows(&fixture).await?.is_empty());
    // The reservation keeps resolving through the ENSv1 resolver.
    let after = expect(RESERVED_EXPIRY, 0, Some(RESOLVER), None);
    ensure!(
        served(&fixture, &alice).await? == after,
        "{}",
        served(&fixture, &alice).await?
    );
    ensure!(summary_expiry(&fixture, &alice).await? == Some(RESERVED_EXPIRY as i64));
    let selected = [(
        alice.clone(),
        RESERVED_EXPIRY.to_string(),
        Some("ens_v1".to_owned()),
    )];
    ensure!(fixture.assert_expiry_selector().await? == selected);

    fixture.assert_undo_restores(5).await?;
    fixture.assert_rebuild_equal(5).await?;
    ensure!(fixture.assert_expiry_selector().await? == selected);
    fixture.cleanup().await
}

/// A chain whose profile admits no ENSv2 root registry keeps the ENSv1 schedule and resolver,
/// even when its client-facing proxy forwards to a listed UniversalResolverV2.
#[tokio::test]
async fn a_universal_resolver_upgrade_without_an_ens_v2_root_registry_cuts_nothing_over()
-> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_proxy_only", 12).await?;
    execution_manifest(&fixture, None, 0, TOP_PROXY, 0).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    reserved(&fixture, &alice, 1, 4).await?;
    let bob = leased(&fixture, "bob.eth", 2, 2).await?;
    upgraded(
        &fixture,
        5,
        TOP_PROXY,
        "universal_resolver",
        ADMITTED,
        "admitted_universal_resolver",
    )
    .await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    ensure!(proxy_rows(&fixture).await?.len() == 1);
    let lease = expect(LEASE_EXPIRY, 90, Some(RESOLVER), None);
    for name in [&alice, &bob] {
        ensure!(
            served(&fixture, name).await? == lease,
            "{}",
            served(&fixture, name).await?
        );
    }
    ensure!(summary_expiry(&fixture, &alice).await? == Some(LEASE_EXPIRY as i64));
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn a_rollback_to_an_unlisted_implementation_keeps_the_chain_cut_over() -> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_rollback", 12).await?;
    declared_path::root_eth_entry(&fixture).await?;
    execution_manifest(&fixture, None, 0, TOP_PROXY, 0).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    reserved(&fixture, &alice, 1, 4).await?;

    // The client-facing proxy points at the managed proxy, which has no `Upgraded` yet.
    upgraded(
        &fixture,
        6,
        TOP_PROXY,
        "universal_resolver",
        MANAGED_PROXY,
        "universal_resolver_proxy",
    )
    .await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    let hop_only = served(&fixture, &alice).await?;

    upgraded(
        &fixture,
        7,
        MANAGED_PROXY,
        "universal_resolver_managed",
        ADMITTED,
        "admitted_universal_resolver",
    )
    .await?;
    fixture.apply(7, FamilyMode::Normal).await?;
    let reservation = expect(RESERVED_EXPIRY, 0, Some(RESOLVER), None);
    ensure!(
        served(&fixture, &alice).await? == reservation,
        "{}",
        served(&fixture, &alice).await?
    );

    upgraded(
        &fixture,
        8,
        MANAGED_PROXY,
        "universal_resolver_managed",
        OLD_IMPLEMENTATION,
        "other",
    )
    .await?;
    fixture.apply(8, FamilyMode::Normal).await?;
    ensure!(
        proxy_rows(&fixture).await?[0]
            == (
                MANAGED_PROXY.into(),
                Some("universal_resolver_managed".into()),
                "other".into()
            ),
        "{:?}",
        proxy_rows(&fixture).await?
    );
    ensure!(
        served(&fixture, &alice).await? == reservation,
        "a rollback moved the name: {}",
        served(&fixture, &alice).await?
    );
    ensure!(summary_expiry(&fixture, &alice).await? == Some(RESERVED_EXPIRY as i64));
    ensure!(
        hop_only == reservation,
        "a managed proxy with no Upgraded moved the name: {hop_only}"
    );

    fixture.assert_undo_restores(8).await?;
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn on_an_admitted_chain_an_eth_name_without_a_live_entry_and_its_subnames_do_not_resolve()
-> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_unresolvable", 12).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let bob = leased(&fixture, "bob.eth", 2, 1).await?;
    let sub = surface(&fixture, "sub.bob.eth").await?;
    let sub_lease = uuid(0x3000);
    fixture
        .binding(&uuid(300), &sub, &sub_lease, "ens_v1", 1, 1, None)
        .await?;
    fixture
        .write(
            1,
            1,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            Some(&sub),
            Some(&sub_lease),
            json!({"authority_kind": "registry", "owner": OWNER, "registry_owner": OWNER,
                   "node": sub.trim_start_matches("ens:")}),
            REGISTRY,
        )
        .await?;
    resolver(&fixture, &sub, 3).await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    declared_path::assert_deployed_path(&fixture).await?;
    // ENSv1 still decides the name and its lease stands, but resolution never reaches it: no
    // proxy `Upgraded` is needed.
    ensure!(
        served(&fixture, &bob).await?
            == expect(LEASE_EXPIRY, 90, None, Some("no_live_ens_v2_entry")),
        "{}",
        served(&fixture, &bob).await?
    );
    let sub_row = served(&fixture, &sub).await?;
    ensure!(
        sub_row["resolver"].is_null()
            && sub_row["unresolvable_reason"] == json!("no_live_ens_v2_entry")
            && sub_row["resolution_unsupported_reason"].is_null(),
        "{sub_row}"
    );
    ensure!(proxy_rows(&fixture).await?.is_empty());

    fixture.assert_undo_restores(4).await?;
    fixture.assert_rebuild_equal(4).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn a_generic_ens_v2_registration_has_no_inferred_grace() -> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_native", 12).await?;
    let carol = surface(&fixture, "carol.eth").await?;
    let resource = uuid(0x4000);
    fixture
        .binding(&uuid(400), &carol, &resource, "ens_v2", 2, 1, None)
        .await?;
    fixture
        .write(
            2,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&carol),
            Some(&resource),
            json!({"registry_contract_instance_id": "eth", "token_id": "4",
                   "status": "registered", "registrant": OWNER, "owner": OWNER,
                   "expiry": RESERVED_EXPIRY}),
            V2_REGISTRY,
        )
        .await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    let row = served(&fixture, &carol).await?;
    ensure!(
        row["arm"] == json!("ens_v2")
            && row["expiry"] == json!(RESERVED_EXPIRY.to_string())
            && row["ens_v1_expiry"].is_null()
            && row["grace_ends_at"] == json!(RESERVED_EXPIRY.to_string()),
        "{row}"
    );
    fixture.cleanup().await
}

/// The proxy family still follows the declarations for monitoring. Rotating the declared
/// client-facing proxy moves `proxy_role` and `implementation_kind` on the retained rows at the
/// block that publishes it, and moves no name.
#[tokio::test]
async fn rotating_the_declared_proxy_reclassifies_retained_upgrades_for_monitoring() -> Result<()> {
    const SUCCESSOR: &str = "0x00000000000000000000000000000000000000f1";
    let fixture = Fixture::new("families_expiry_grace_rotation", 12).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let manifest = execution_manifest(&fixture, None, 0, TOP_PROXY, 0).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    reserved(&fixture, &alice, 1, 4).await?;
    upgraded(
        &fixture,
        5,
        TOP_PROXY,
        "universal_resolver",
        ADMITTED,
        "admitted_universal_resolver",
    )
    .await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    let client_facing = Some("universal_resolver".to_owned());
    let admitted = "admitted_universal_resolver".to_owned();
    ensure!(
        proxy_rows(&fixture).await?
            == [(TOP_PROXY.into(), client_facing.clone(), admitted.clone())]
    );
    let summaries = fixture.rows("project_name_summary").await?;
    let unmoved = || async {
        ensure!(served(&fixture, &alice).await?["expiry"] == json!(RESERVED_EXPIRY.to_string()));
        ensure!(
            fixture.rows("project_name_summary").await? == summaries,
            "a declaration rotation rewrote a name summary"
        );
        Ok(())
    };
    unmoved().await?;

    execution_manifest(&fixture, Some(manifest), 7, SUCCESSOR, 7).await?;
    // Rotation retains the retired proxy's replayable upgrade alongside the successor's.
    upgraded(
        &fixture,
        7,
        SUCCESSOR,
        "universal_resolver",
        OLD_IMPLEMENTATION,
        "other",
    )
    .await?;
    fixture.apply(7, FamilyMode::Normal).await?;
    ensure!(
        proxy_rows(&fixture).await?
            == [
                (SUCCESSOR.into(), client_facing.clone(), "other".into()),
                (TOP_PROXY.into(), None, admitted.clone()),
            ],
        "the retired proxy kept its role: {:?}",
        proxy_rows(&fixture).await?
    );
    unmoved().await?;
    fixture.assert_undo_restores(7).await?;
    fixture.assert_rebuild_equal(7).await?;

    // A declaration change with no new upgrade reclassifies the retained rows only once that
    // declaration starts.
    execution_manifest(&fixture, Some(manifest), 8, TOP_PROXY, 9).await?;
    fixture.apply(8, FamilyMode::Normal).await?;
    ensure!(
        proxy_rows(&fixture).await?
            == [
                (SUCCESSOR.into(), None, "other".into()),
                (TOP_PROXY.into(), None, admitted.clone()),
            ],
        "{:?}",
        proxy_rows(&fixture).await?
    );
    fixture.apply(9, FamilyMode::Normal).await?;
    ensure!(
        proxy_rows(&fixture).await?
            == [
                (SUCCESSOR.into(), None, "other".into()),
                (TOP_PROXY.into(), client_facing, admitted),
            ],
        "{:?}",
        proxy_rows(&fixture).await?
    );
    unmoved().await?;
    fixture.assert_undo_restores(9).await?;
    fixture.assert_rebuild_equal(9).await?;
    fixture.cleanup().await
}

/// The block time `Fixture::lineage` gives `block`, in Unix seconds.
fn block_time(block: i64) -> i64 {
    1_800_000_000 + block * 12
}

/// A composed name's registration time and migration time, in Unix seconds, as the name summary
/// and the name state hold them.
async fn times(fixture: &Fixture, logical_name_id: &str) -> Result<(Option<i64>, Option<i64>)> {
    let registered: Option<i64> = sqlx::query_scalar(
        "SELECT extract(epoch FROM registered_at)::bigint FROM project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_one(&fixture.pool)
    .await?;
    let migrated: Option<Option<i64>> = sqlx::query_scalar(
        "SELECT extract(epoch FROM migrated_at)::bigint FROM project_name_state
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_optional(&fixture.pool)
    .await?;
    Ok((registered, migrated.flatten()))
}

/// A registrar event of `name`'s lease `n` at `block`.
async fn lease_event(
    fixture: &Fixture,
    name: &str,
    n: u32,
    block: i64,
    kind: &str,
    expiry: u64,
) -> Result<()> {
    fixture
        .write(
            block,
            1,
            kind,
            "ens_v1_registrar_l1",
            Some(name),
            Some(&uuid(0x1000 + n)),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": expiry}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// An ENSv2 `eth` registry entry `entry` with token `token`, registered to `OWNER` at `block`
/// and bound under ens_v2 there.
async fn v2_registered(
    fixture: &Fixture,
    name: &str,
    binding: &str,
    entry: &str,
    token: &str,
    block: i64,
) -> Result<()> {
    fixture
        .binding(binding, name, entry, "ens_v2", block, 1, None)
        .await?;
    fixture
        .write(
            block,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(name),
            Some(entry),
            json!({"registry_contract_instance_id": "eth", "token_id": token,
                   "status": "registered", "registrant": OWNER, "owner": OWNER,
                   "expiry": RESERVED_EXPIRY}),
            V2_REGISTRY,
        )
        .await?;
    Ok(())
}

/// `name`'s lease `n` migrates at `block`: its ENSv1 binding closes, and in one transaction the
/// reserved ENSv2 entry is registered and bound. The MigrationApplied takes the ENSv2 grant's
/// exact position, as the adapter's boundary event copies it from the correlated registration
/// (`crates/adapters/src/schema_v2/migration/support.rs`, `boundary_event`).
async fn migrated(fixture: &Fixture, name: &str, n: u32, block: i64) -> Result<()> {
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(1800000000 + $2 * 12)
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(uuid(100 + n))
    .bind(block)
    .execute(&fixture.pool)
    .await?;
    let entry = uuid(0x2000 + n);
    v2_registered(
        fixture,
        name,
        &uuid(200 + n),
        &entry,
        &format!("{n}"),
        block,
    )
    .await?;
    fixture
        .write(
            block,
            1,
            "MigrationApplied",
            "ens_v2_migration_l1",
            Some(name),
            Some(&entry),
            json!({"migration_path": "unwrapped"}),
            V2_REGISTRY,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn renewals_the_reservation_and_the_migration_keep_the_registration_time() -> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_registered_at", 12).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    let granted = Some(block_time(2));
    lease_event(
        &fixture,
        &alice,
        1,
        3,
        "RegistrationRenewed",
        LEASE_EXPIRY + 365 * DAY,
    )
    .await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    ensure!(times(&fixture, &alice).await? == (granted, None));

    reserved(&fixture, &alice, 1, 4).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    let reserved_row = served(&fixture, &alice).await?;
    ensure!(
        reserved_row["expiry"] == json!(RESERVED_EXPIRY.to_string()),
        "{reserved_row}"
    );
    ensure!(times(&fixture, &alice).await? == (granted, None));

    migrated(&fixture, &alice, 1, 7).await?;
    fixture.apply(7, FamilyMode::Normal).await?;
    ensure!(served(&fixture, &alice).await?["arm"] == json!("ens_v2"));
    let migrated_times = times(&fixture, &alice).await?;
    ensure!(
        migrated_times == (granted, Some(block_time(7))),
        "{migrated_times:?}"
    );

    fixture.assert_undo_restores(7).await?;
    fixture.assert_rebuild_equal(7).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn only_a_registration_after_a_release_starts_a_new_registration_time() -> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_registered_again", 14).await?;
    // A family run needs every block between two of its blocks and walks at most 256 per run, so
    // this fixture cannot span the months a lease and its grace take at 12 seconds a block. The
    // first lease's expiry is instead set past enough that its 90-day grace ends before block 4;
    // the lifecycle fold reads the expiry and the release, not the time between blocks.
    let first_expiry = u64::try_from(block_time(4)).expect("block time") - 90 * DAY - 1;
    let dave = surface(&fixture, "dave.eth").await?;
    lease(&fixture, &dave, 2, 1, first_expiry).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    ensure!(times(&fixture, &dave).await? == (Some(block_time(2)), None));

    // At block 4 the interpreter releases the old lease from the block, as it does at the first
    // block past the grace; the registration that follows is a new lease with its own resource
    // and binding.
    fixture
        .event(
            Event::new(
                "dave-lease-released",
                4,
                0,
                "RegistrationReleased",
                "ens_v1_registrar_l1",
            )
            .name(&dave)
            .resource(&uuid(0x1002))
            .synthesised()
            .before(json!({"registrant": OWNER}))
            .after(json!({"expiry": first_expiry, "released_at": block_time(4),
                          "source_event": "RegistrationReleased"}))
            .raw(json!({"kind": "raw_block", "emitting_address": REGISTRAR})),
        )
        .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(1800000000 + 4 * 12)
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(uuid(102))
    .execute(&fixture.pool)
    .await?;
    lease(&fixture, &dave, 3, 4, LEASE_EXPIRY).await?;
    reserved(&fixture, &dave, 3, 7).await?;
    migrated(&fixture, &dave, 3, 8).await?;
    fixture.apply(8, FamilyMode::Normal).await?;
    let migrated_times = times(&fixture, &dave).await?;
    ensure!(
        migrated_times == (Some(block_time(5)), Some(block_time(8))),
        "{migrated_times:?}"
    );

    // The migrated registration is unregistered, and the label is registered again in ENSv2.
    fixture
        .event(
            Event::new(
                "dave-unregistered",
                10,
                1,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .name(&dave)
            .resource(&uuid(0x2003))
            .before(json!({"registrant": OWNER}))
            .after(
                json!({"registry_contract_instance_id": "eth", "token_id": "3",
                              "released_at": block_time(10)}),
            )
            .raw(json!({"emitting_address": V2_REGISTRY})),
        )
        .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(1800000000 + 10 * 12)
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(uuid(203))
    .execute(&fixture.pool)
    .await?;
    v2_registered(&fixture, &dave, &uuid(213), &uuid(0x2103), "4", 12).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let registered_again = times(&fixture, &dave).await?;
    ensure!(
        registered_again == (Some(block_time(12)), Some(block_time(8))),
        "{registered_again:?}"
    );

    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

/// A registrar renewal of a wrapped `.eth` lease: the BaseRegistrar's own `NameRenewed`, as the
/// adapter writes it on the lease
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L168 @ ens_v1@91c966f),
/// plus the NameWrapper expiry derived from it (registrar family, `authority_kind = wrapper`,
/// the lease expiry plus 90 days). The admitted wrapped controller renews through
/// `NameWrapper.renew`, which renews the lease on the BaseRegistrar and then stores the
/// returned expiry plus its 90-day grace for a wrapped name
/// (upstream: .refs/basenames/lib/ens-contracts/contracts/ethregistrar/ETHRegistrarController.sol:L210-L226 @ basenames@1809bbc)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L318 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L332-L337 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f).
async fn renewed_wrapped(
    fixture: &Fixture,
    logical_name_id: &str,
    lease: &str,
    wrapper: &str,
    block: i64,
    expiry: u64,
) -> Result<()> {
    let registrar = json!({"source_event": "NameRenewed", "authority_kind": "registrar",
                           "registrant": OWNER, "expiry": expiry});
    for (log, kind) in [(1, "RegistrationRenewed"), (2, "ExpiryChanged")] {
        fixture
            .write(
                block,
                log,
                kind,
                "ens_v1_registrar_l1",
                Some(logical_name_id),
                Some(lease),
                registrar.clone(),
                REGISTRAR,
            )
            .await?;
    }
    fixture
        .write(
            block,
            3,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            Some(logical_name_id),
            Some(wrapper),
            json!({"source_event": "NameRenewed", "authority_kind": "wrapper",
                   "expiry": expiry + 90 * DAY, "registrar_expiry": expiry}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_wrapped_eth_name_renewed_through_the_base_registrar_serves_the_renewed_lease_date()
-> Result<()> {
    const RENEWED: u64 = LEASE_EXPIRY + 365 * DAY;
    const RENEWED_AGAIN: u64 = RENEWED + 365 * DAY;
    let fixture = Fixture::new("families_expiry_grace_wrapped_renewal", 12).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let nick = surface(&fixture, "nick.eth").await?;
    let (lease, wrapper) = (uuid(0x5001), uuid(0x5002));
    fixture
        .binding(&uuid(501), &nick, &lease, "ens_v1", 1, 0, Some(2))
        .await?;
    fixture
        .write(
            1,
            0,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(&nick),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            1,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&nick),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": LEASE_EXPIRY}),
            REGISTRAR,
        )
        .await?;
    // Wrapped emancipated at block 2.
    fixture
        .binding(&uuid(502), &nick, &wrapper, "ens_v1", 2, 0, None)
        .await?;
    fixture
        .write(
            2,
            0,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            Some(&nick),
            Some(&wrapper),
            json!({"source_event": "NameWrapped", "authority_kind": "wrapper",
                   "wrapped_registrar_resource_id": lease,
                   "node": nick.trim_start_matches("ens:")}),
            NAME_WRAPPER,
        )
        .await?;
    fixture
        .write(
            2,
            1,
            "AuthorityEpochChanged",
            "ens_v1_wrapper_l1",
            Some(&nick),
            Some(&wrapper),
            json!({"source_event": "NameWrapped", "authority_kind": "wrapper", "owner": OWNER}),
            NAME_WRAPPER,
        )
        .await?;
    // PARENT_CANNOT_CONTROL | IS_DOT_ETH.
    fixture
        .write(
            2,
            3,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            None,
            Some(&wrapper),
            json!({"source_event": "NameWrapped", "wrapper_state": "emancipated",
                   "fuses": 196_608}),
            NAME_WRAPPER,
        )
        .await?;
    fixture
        .write(
            2,
            2,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            Some(&nick),
            Some(&wrapper),
            json!({"source_event": "NameWrapped", "expiry": LEASE_EXPIRY + 90 * DAY}),
            NAME_WRAPPER,
        )
        .await?;
    resolver(&fixture, &nick, 2).await?;
    reserved(&fixture, &nick, 5, 3).await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    let row = served(&fixture, &nick).await?;
    ensure!(
        row["expiry"] == json!(RESERVED_EXPIRY.to_string())
            && row["ens_v1_expiry"] == json!(LEASE_EXPIRY.to_string()),
        "before renewal: {row}"
    );

    // ETHRegistrarController.renew on the wrapped name.
    renewed_wrapped(&fixture, &nick, &lease, &wrapper, 5, RENEWED).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    let row = served(&fixture, &nick).await?;
    ensure!(
        row["ens_v1_expiry"] == json!(RENEWED.to_string()),
        "after the controller renewal: {row}"
    );
    ensure!(
        row["expiry"] == json!(RESERVED_EXPIRY.to_string()),
        "the reservation still decides the served expiry: {row}"
    );

    // ETHRenewerV1 after the cutover: the ENSv2 entry's expiry moves, then the BaseRegistrar
    // renews the lease and the NameWrapper expiry follows it
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/AbstractETHRegistrar.sol:L132-L133 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRenewerV1.sol:L153-L155 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L168 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRenewerV1.sol:L119-L124 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRenewerV1.sol:L140-L146 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L318 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L332-L337 @ ens_v1@91c966f).
    const RESERVED_AGAIN: u64 = RENEWED_AGAIN + 62 * DAY;
    fixture
        .write(
            6,
            0,
            "ExpiryChanged",
            "ens_v2_registry_l1",
            Some(&nick),
            Some(&uuid(0x2005)),
            json!({"registry_contract_instance_id": "eth", "token_id": "5",
                   "expiry": RESERVED_AGAIN}),
            V2_REGISTRY,
        )
        .await?;
    renewed_wrapped(&fixture, &nick, &lease, &wrapper, 6, RENEWED_AGAIN).await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    let row = served(&fixture, &nick).await?;
    ensure!(
        row["ens_v1_expiry"] == json!(RENEWED_AGAIN.to_string()),
        "after the ENSv2 renewer: {row}"
    );
    ensure!(
        row["expiry"] == json!(RESERVED_AGAIN.to_string()),
        "the renewed reservation decides the served expiry: {row}"
    );

    fixture.assert_undo_restores(6).await?;
    fixture.assert_rebuild_equal(6).await?;
    fixture.cleanup().await
}

/// The ENSv2 owner's BatchRegistrar can extend a reservation without the BaseRegistrar
/// (`BatchRegistrar.batchRegister` renews a RESERVED entry to a later expiry)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/BatchRegistrar.sol:L66-L70 @ ens_v2_sepolia_20260916@366de741). The served expiry
/// and canonical grace end follow the reservation while the lease date stays. This generic
/// registry has no admitted registrar grace, so G = E both before and after extension,
/// independently of the ENSv1 lease's own grace deadline (lease + 90 days).
#[tokio::test]
async fn extending_only_the_reservation_moves_its_deadline_and_keeps_the_lease_date() -> Result<()>
{
    const EXTENDED: u64 = RESERVED_EXPIRY + 30 * DAY;
    let fixture = Fixture::new("families_expiry_grace_batch_extension", 12).await?;
    declared_path::root_eth_entry(&fixture).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    reserved(&fixture, &alice, 1, 4).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    let before = served(&fixture, &alice).await?;
    ensure!(
        before["expiry"] == json!(RESERVED_EXPIRY.to_string())
            && before["grace_ends_at"] == json!(RESERVED_EXPIRY.to_string())
            && before["ens_v1_expiry"] == json!(LEASE_EXPIRY.to_string()),
        "the generic reservation's deadline is independent of the ENSv1 lease: {before}"
    );

    fixture
        .write(
            6,
            0,
            "ExpiryChanged",
            "ens_v2_registry_l1",
            Some(&alice),
            Some(&uuid(0x2001)),
            json!({"registry_contract_instance_id": "eth", "token_id": "1", "expiry": EXTENDED}),
            V2_REGISTRY,
        )
        .await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    let row = served(&fixture, &alice).await?;
    ensure!(
        row["expiry"] == json!(EXTENDED.to_string())
            && row["grace_ends_at"] == json!(EXTENDED.to_string())
            && row["ens_v1_expiry"] == json!(LEASE_EXPIRY.to_string()),
        "{row}"
    );
    ensure!(
        row["grace_ends_at"] != json!((LEASE_EXPIRY + 90 * DAY).to_string()),
        "the lease's grace deadline and the served one differ: {row}"
    );
    fixture.assert_undo_restores(6).await?;
    fixture.assert_rebuild_equal(6).await?;
    fixture.cleanup().await
}

/// The families block `block` journalled with their row counts, leaving out the two markers
/// every block journals.
async fn journalled(fixture: &Fixture, block: i64) -> Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT family, count(*) FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2
           AND family NOT IN ('marker', 'project_history_catalogue_marker')
         GROUP BY family ORDER BY family",
    )
    .bind(CHAIN)
    .bind(block)
    .fetch_all(&fixture.pool)
    .await?)
}

/// A Universal Resolver upgrade on an admitted chain writes its own proxy row and nothing else:
/// no name summary is composed again and none is journalled, whichever implementation the
/// proxy moves to.
#[tokio::test]
async fn a_universal_resolver_upgrade_after_admission_writes_no_summary_row() -> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_upgrade_only", 8).await?;
    declared_path::root_eth_entry(&fixture).await?;
    execution_manifest(&fixture, None, 0, TOP_PROXY, 0).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    reserved(&fixture, &alice, 1, 4).await?;
    let bob = leased(&fixture, "bob.eth", 2, 2).await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    let before = fixture.rows("project_name_summary").await?;
    ensure!(before.len() == 2, "{}", before.len());

    for (block, implementation, kind) in [
        (5, ADMITTED, "admitted_universal_resolver"),
        (6, OLD_IMPLEMENTATION, "other"),
    ] {
        upgraded(
            &fixture,
            block,
            TOP_PROXY,
            "universal_resolver",
            implementation,
            kind,
        )
        .await?;
        fixture.apply(block, FamilyMode::Normal).await?;
        ensure!(
            proxy_rows(&fixture).await?
                == [(
                    TOP_PROXY.into(),
                    Some("universal_resolver".into()),
                    kind.into()
                )],
            "block {block}: {:?}",
            proxy_rows(&fixture).await?
        );
        ensure!(
            journalled(&fixture, block).await? == [("project_universal_resolver_proxy".into(), 1)],
            "block {block}: a proxy upgrade journalled {:?}",
            journalled(&fixture, block).await?
        );
        ensure!(
            fixture.rows("project_name_summary").await? == before,
            "block {block}: a proxy upgrade rewrote a name summary"
        );
    }
    ensure!(served(&fixture, &alice).await?["expiry"] == json!(RESERVED_EXPIRY.to_string()));
    ensure!(served(&fixture, &bob).await?["unresolvable_reason"] == json!("no_live_ens_v2_entry"));
    fixture.apply(7, FamilyMode::Normal).await?;
    ensure!(
        journalled(&fixture, 7).await?.is_empty(),
        "an empty block journals only what `journalled` leaves out"
    );

    fixture.assert_undo_restores(7).await?;
    fixture.assert_rebuild_equal(7).await?;
    fixture.cleanup().await
}
