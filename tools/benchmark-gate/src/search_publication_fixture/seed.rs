use super::{inputs::*, *};
use serde_json::json;

const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const HOLDER: &str = "0x00000000000000000000000000000000000000a2";

pub async fn initial(pool: &PgPool) -> Result<()> {
    for (ns, chain, family) in [
        ("ens", ETH, "ens_v1_registry_l1"),
        ("basenames", BASE, "basenames_base_registry"),
    ] {
        manifest(
            pool,
            ns,
            chain,
            family,
            1,
            json!({"manifest_version":1,"namespace":ns,"source_family":family,
            "chain":chain,"deployment_epoch":"basenames_v1","rollout_status":"active",
            "normalizer_version":bigname_domain::normalization::ENS_NORMALIZER_VERSION,
            "capability_flags":{},"roots":[],"contracts":[],"discovery_rules":[]}),
        )
        .await?;
    }
    let plain = name(pool, "gate1-plain.eth", "ens", 0xa000, true, false).await?;
    registration(pool, &plain, 4_000_000_000).await?;
    let registry_only = name(pool, "gate1-registry-only.eth", "ens", 0xa005, true, false).await?;
    event(
        pool,
        &registry_only,
        0,
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        true,
        json!({"source_event":"Transfer","node":registry_only.node,"owner":OWNER}),
    )
    .await?;
    let extreme = name(pool, "gate1-extreme.eth", "ens", 0xa010, true, false).await?;
    registration(pool, &extreme, 253_402_300_800).await?;
    for (offset, state, fuses, expiry) in [
        (0, "wrapped", 0, 4_000_000_000u64),
        (1, "emancipated", 65536, 4_000_000_000),
        (2, "locked", 196609, 4_000_000_000),
        (3, "grace", 196609, (CLOCK + 100) as u64),
        (4, "masked", 65537, (CLOCK - 21) as u64),
        (5, "not-set", 0, 0),
        (6, "no-expiry", 65537, u64::MAX),
    ] {
        let spelling = if matches!(state, "locked" | "grace") {
            format!("gate1-{state}.eth")
        } else {
            format!("gate1-{state}.parent.eth")
        };
        let item = name(pool, &spelling, "ens", 0xa100 + offset * 16, true, false).await?;
        // Named wrapper inputs are the same event families used by v2_name_record's
        // wrapped fixture. Project applies wrapper clock masks; no public fields are seeded.
        event(
            pool,
            &item,
            0,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            true,
            json!({"node":item.node,"owner":OWNER}),
        )
        .await?;
        event(
            pool,
            &item,
            1,
            "AuthorityEpochChanged",
            "ens_v1_wrapper_l1",
            true,
            json!({"source_event":"NameWrapped","authority_kind":"wrapper","owner":HOLDER}),
        )
        .await?;
        event(
            pool,
            &item,
            2,
            "TokenControlTransferred",
            "ens_v1_wrapper_l1",
            true,
            json!({"source_event":"NameWrapped","owner":HOLDER}),
        )
        .await?;
        event(pool,&item,3,"PermissionScopeChanged","ens_v1_wrapper_l1",true,json!({"source_event":"NameWrapped","wrapper_state":if matches!(state,"grace"|"masked"|"no-expiry") {"locked"} else if state=="not-set" {"wrapped"}else{state},"fuses":fuses})).await?;
        event(
            pool,
            &item,
            4,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            true,
            json!({"source_event":"NameWrapped","expiry":expiry}),
        )
        .await?;
    }
    name(pool, "gate1-empty.eth", "ens", 0xa200, false, false).await?;
    name(pool, "gate1-fallback.eth", "ens", 0xa240, true, false).await?;
    name(
        pool,
        "gate1-fallback.base.eth",
        "basenames",
        0xa250,
        true,
        false,
    )
    .await?;
    name(
        pool,
        "gate1-empty.base.eth",
        "basenames",
        0xa210,
        false,
        false,
    )
    .await?;
    let base = name(
        pool,
        "gate1-declared.base.eth",
        "basenames",
        0xa220,
        true,
        false,
    )
    .await?;
    registration(pool, &base, 4_000_000_000).await?;
    let transport = name(
        pool,
        "gate1-transport.base.eth",
        "basenames",
        0xa230,
        true,
        false,
    )
    .await?;
    // An unnamed pointer input supplies topology while leaving name-history creation absent.
    event(
        pool,
        &transport,
        0,
        "ResolverChanged",
        "basenames_base_registry",
        false,
        json!({"resolver":"0x000000000000000000000000000000000000fafa"}),
    )
    .await?;
    for (name_text, seed, structural) in [
        ("☀.gate1-raw.eth", 0xa300, false),
        ("☀.gate1-structural.eth", 0xa310, true),
    ] {
        let emoji = name(pool, name_text, "ens", seed, true, structural).await?;
        registration(pool, &emoji, 4_000_000_000).await?;
    }
    let absent = name(pool, "gate1-absent.eth", "ens", 0xa400, true, false).await?;
    registration(pool, &absent, 4_000_000_000).await?;
    // Retained identity input with unreadable token lineage: a missing composed row must not enter search.
    sqlx::query(
        "UPDATE token_lineages SET canonicality_state='observed' WHERE token_lineage_id=$1",
    )
    .bind(uuid::Uuid::from_u128(absent.resource.as_u128() + 1))
    .execute(pool)
    .await?;
    Ok(())
}

async fn registration(pool: &PgPool, name: &Name, expiry: u64) -> Result<()> {
    let (registry, registrar) = if name.namespace == "basenames" {
        ("basenames_base_registry", "basenames_base_registrar")
    } else {
        ("ens_v1_registry_l1", "ens_v1_registrar_l1")
    };
    event(
        pool,
        name,
        0,
        "AuthorityTransferred",
        registry,
        true,
        json!({"source_event":"Transfer","node":name.node,"owner":OWNER}),
    )
    .await?;
    event(pool,name,1,"RegistrationGranted",registrar,true,json!({"source_event":"NameRegistered","authority_kind":"registrar","registrant":HOLDER,"expiry":expiry})).await?;
    Ok(())
}

pub async fn execution_context(pool: &PgPool) -> Result<()> {
    lineage(pool, ETH, 90, CLOCK - 20).await?;
    let payload = json!({"manifest_version":2,"namespace":"basenames","source_family":"basenames_execution",
        "chain":ETH,"deployment_epoch":"basenames_v1","rollout_status":"active",
        "normalizer_version":bigname_domain::normalization::ENS_NORMALIZER_VERSION,
        "capability_flags":{"verified_resolution":{"status":"supported"}},
        "contracts":[{"role":"l1_resolver","address":"0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31"}]});
    let id = manifest(
        pool,
        "basenames",
        ETH,
        "basenames_execution",
        2,
        payload.clone(),
    )
    .await?;
    sqlx::query("INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,raw_fact_ref,derivation_kind,canonicality_state,after_state)
        VALUES('gate1-execution-manifest','basenames','SourceManifestUpdated','basenames_execution',2,$1,$2,$3,'manifest_sync','finalized',$4)")
        .bind(id).bind(ETH).bind(json!({"deployment_epoch":"basenames_v1"}))
        .bind(json!({"rollout_status":"active","manifest_payload":payload})).execute(pool).await?;
    Ok(())
}
