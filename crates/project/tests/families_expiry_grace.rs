//! The `.eth` expiry and grace a composed name serves around the Universal Resolver cutover
//! (TYR-90). A `.eth` name that ENSv1 still decides and that ENSv2 premigration reserved serves
//! its ENSv1 lease's expiry and the 90-day ENSv1 grace until the client-facing Universal
//! Resolver proxy's chain ends at an admitted UniversalResolverV2 implementation, and the
//! reservation's expiry and the 28-day ENSv2 grace while it does. After the cutover a `.eth`
//! name that ENSv1 decides without a live ENSv2 entry, and every name below it, serve no
//! resolver. A block that changes a proxy recomposes the summaries of the reserved names, and
//! undo and rebuild restore them.
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
const V2_REGISTRY: &str = "0x00000000000000000000000000000000000000e6";
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
    let lease = uuid(0x1000 + n);
    fixture
        .binding(
            &uuid(100 + n),
            &logical_name_id,
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
            Some(&logical_name_id),
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
            Some(&logical_name_id),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": LEASE_EXPIRY}),
            REGISTRAR,
        )
        .await?;
    resolver(fixture, &logical_name_id, block + 2).await?;
    Ok(logical_name_id)
}

async fn resolver(fixture: &Fixture, logical_name_id: &str, block: i64) -> Result<()> {
    let node = logical_name_id.trim_start_matches("ens:");
    let identity = format!("resolver-{logical_name_id}");
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

/// Premigration's reservation of `logical_name_id` in the ENSv2 `eth` registry.
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

/// The served expiry, grace end, resolver address and unresolvable reason of a composed name.
async fn served(fixture: &Fixture, logical_name_id: &str) -> Result<Value> {
    let row = load_family_name(&fixture.pool, logical_name_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no composed row"))?;
    let summary = &row.declared_summary;
    Ok(json!({
        "arm": row.provenance["authority_selection"]["authority_arm"],
        "expiry": summary["registration"]["expiry"],
        "grace_ends_at": summary["registration"]["grace_ends_at"],
        "resolver": summary["resolver"]["address"],
        "unresolvable_reason": summary.get("unresolvable_reason").cloned().unwrap_or(Value::Null),
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
        "grace_ends_at": (expiry + grace_days * DAY).to_string(),
        "resolver": resolver,
        "unresolvable_reason": reason,
    })
}

#[tokio::test]
async fn a_reserved_eth_name_serves_the_reservation_expiry_only_while_cut_over() -> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_reserved", 12).await?;
    execution_manifest(&fixture, None, 0, TOP_PROXY, 0).await?;
    let alice = leased(&fixture, "alice.eth", 1, 1).await?;
    reserved(&fixture, &alice, 1, 4).await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    let before = expect(LEASE_EXPIRY, 90, Some(RESOLVER), None);
    ensure!(
        served(&fixture, &alice).await? == before,
        "{}",
        served(&fixture, &alice).await?
    );
    ensure!(summary_expiry(&fixture, &alice).await? == Some(LEASE_EXPIRY as i64));

    // The client-facing proxy points at the managed proxy, which has no Upgraded yet: its
    // constructor's implementation is not admitted, so the chain is not cut over.
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
    ensure!(served(&fixture, &alice).await? == before);

    // The managed proxy moves to the admitted UniversalResolverV2: cut over.
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
    let after = expect(RESERVED_EXPIRY, 28, Some(RESOLVER), None);
    ensure!(
        served(&fixture, &alice).await? == after,
        "{}",
        served(&fixture, &alice).await?
    );
    ensure!(
        summary_expiry(&fixture, &alice).await? == Some(RESERVED_EXPIRY as i64),
        "the cutover block did not recompose the reserved name's summary"
    );

    // A rollback to an unadmitted implementation ends the cutover.
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
    ensure!(served(&fixture, &alice).await? == before);
    ensure!(summary_expiry(&fixture, &alice).await? == Some(LEASE_EXPIRY as i64));

    fixture.assert_undo_restores(8).await?;
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn after_the_cutover_an_eth_name_without_a_live_entry_and_its_subnames_do_not_resolve()
-> Result<()> {
    let fixture = Fixture::new("families_expiry_grace_unresolvable", 12).await?;
    execution_manifest(&fixture, None, 0, TOP_PROXY, 0).await?;
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
    ensure!(
        served(&fixture, &bob).await? == expect(LEASE_EXPIRY, 90, Some(RESOLVER), None),
        "{}",
        served(&fixture, &bob).await?
    );
    let sub_before = served(&fixture, &sub).await?;
    ensure!(sub_before["resolver"] == json!(RESOLVER), "{sub_before}");

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
    // ENSv1 still decides the name and its lease stands, but resolution no longer reaches it.
    ensure!(
        served(&fixture, &bob).await?
            == expect(LEASE_EXPIRY, 90, None, Some("no_live_ens_v2_entry")),
        "{}",
        served(&fixture, &bob).await?
    );
    let sub_after = served(&fixture, &sub).await?;
    ensure!(
        sub_after["resolver"].is_null()
            && sub_after["unresolvable_reason"] == json!("no_live_ens_v2_entry"),
        "{sub_after}"
    );

    fixture.assert_undo_restores(5).await?;
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn an_ens_v2_registration_serves_its_expiry_and_the_ens_v2_grace_before_any_cutover()
-> Result<()> {
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
            && row["grace_ends_at"] == json!((RESERVED_EXPIRY + 28 * DAY).to_string()),
        "{row}"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn rotating_the_declared_proxy_reclassifies_retained_upgrades_at_publication() -> Result<()> {
    const SUCCESSOR: &str = "0x00000000000000000000000000000000000000f1";
    let fixture = Fixture::new("families_expiry_grace_rotation", 12).await?;
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
    ensure!(served(&fixture, &alice).await?["expiry"] == json!(RESERVED_EXPIRY.to_string()));
    ensure!(summary_expiry(&fixture, &alice).await? == Some(RESERVED_EXPIRY as i64));

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
    // Sync cannot change the previously published result before Project consumes its input.
    ensure!(served(&fixture, &alice).await?["expiry"] == json!(RESERVED_EXPIRY.to_string()));
    fixture.apply(7, FamilyMode::Normal).await?;
    ensure!(
        served(&fixture, &alice).await?["expiry"] == json!(LEASE_EXPIRY.to_string()),
        "a retired proxy still determines the expiry after declaration rotation"
    );
    ensure!(summary_expiry(&fixture, &alice).await? == Some(LEASE_EXPIRY as i64));
    fixture.assert_undo_restores(7).await?;
    fixture.assert_rebuild_equal(7).await?;

    // A declaration change with no new upgrade reuses retained implementation evidence only
    // once that declaration starts. The start block itself must publish the changed expiry.
    execution_manifest(&fixture, Some(manifest), 8, TOP_PROXY, 9).await?;
    fixture.apply(8, FamilyMode::Normal).await?;
    ensure!(served(&fixture, &alice).await?["expiry"] == json!(LEASE_EXPIRY.to_string()));
    fixture.apply(9, FamilyMode::Normal).await?;
    ensure!(served(&fixture, &alice).await?["expiry"] == json!(RESERVED_EXPIRY.to_string()));
    ensure!(summary_expiry(&fixture, &alice).await? == Some(RESERVED_EXPIRY as i64));
    fixture.assert_undo_restores(9).await?;
    fixture.assert_rebuild_equal(9).await?;
    fixture.cleanup().await
}
