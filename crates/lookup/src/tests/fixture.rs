//! Lookup fixtures publish real identity, declaration and record inputs through Project.
use super::*;
use bigname_project::families::{FamilyMode, FamilyOptions};

pub(super) async fn setup_fixture(kind: FixtureKind, indexed_value: &str) -> AnyResult<Fixture> {
    setup_fixture_inner(kind, indexed_value, true).await
}

pub(super) async fn setup_unpublished_fixture() -> AnyResult<Fixture> {
    setup_fixture_inner(FixtureKind::Ens, INDEXED_VALUE, false).await
}

async fn setup_fixture_inner(
    kind: FixtureKind,
    indexed_value: &str,
    publish: bool,
) -> AnyResult<Fixture> {
    let database = TestDatabase::create_from_template(
        TestDatabaseConfig::new("bigname_lookup").pool_max_connections(6),
        "lookup_phase",
        &PHASE_BASELINE.map(str::as_bytes),
        |pool| async move { install_baseline(&pool).await },
    )
    .await?;
    let pool = database.pool();
    apply_baseline(pool).await?;
    seed_heads(pool, kind).await?;
    let (
        namespace,
        name,
        chain,
        hash,
        entrypoint,
        role,
        execution,
        registrar,
        registry,
        resolver_family,
    ) = match kind {
        FixtureKind::Ens => (
            ENS_NAMESPACE,
            "alice.eth",
            ETHEREUM,
            ETHEREUM_HASH,
            UNIVERSAL_RESOLVER,
            "universal_resolver",
            "ens_execution",
            "ens_v1_registrar_l1",
            "ens_v1_registry_l1",
            "ens_v1_resolver_l1",
        ),
        FixtureKind::EnsV2Arm => (
            ENS_NAMESPACE,
            "alice.eth",
            ETHEREUM,
            ETHEREUM_HASH,
            UNIVERSAL_RESOLVER,
            "universal_resolver",
            "ens_execution",
            "ens_v2_registrar_l1",
            "ens_v2_registry_l1",
            "ens_v2_resolver_l1",
        ),
        FixtureKind::Basenames => (
            BASENAMES_NAMESPACE,
            "alice.base.eth",
            BASE,
            BASE_HASH,
            BASE_L1_RESOLVER,
            "l1_resolver",
            "basenames_execution",
            "basenames_base_registrar",
            "basenames_base_registry",
            "basenames_base_resolver",
        ),
    };
    seed_manifest(
        pool,
        namespace,
        execution,
        role,
        entrypoint,
        "00000000-0000-0000-0000-000000000103",
    )
    .await?;
    if matches!(kind, FixtureKind::EnsV2Arm) {
        sqlx::query("UPDATE manifest_versions SET manifest_payload = manifest_payload || '{\"verified_authority_arms\":[\"ens_v1\",\"ens_v2\"]}'::jsonb WHERE source_family = 'ens_execution'").execute(pool).await?;
    }
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let node = hex_string(&namehash(&normalized.normalized_name)?);
    let logical_name_id = format!("{namespace}:{node}");
    let resource = "00000000-0000-0000-0000-000000000101";
    let binding = "00000000-0000-0000-0000-000000000102";
    let resolver = "0x1000000000000000000000000000000000000001";
    let owner = "0x000000000000000000000000000000000000beef";
    sqlx::query("INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')")
        .bind(resource).bind(chain).bind(hash).execute(pool).await?;
    sqlx::query("INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid,$1::uuid,$2,$3,10,'canonical')")
        .bind(resource).bind(chain).bind(hash).execute(pool).await?;
    let labelhashes: Vec<_> = normalized
        .normalized_labels
        .iter()
        .map(|label| format!("{:#x}", keccak256(label.as_bytes())))
        .collect();
    sqlx::query("INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'active',$9,$10,10,'canonical')")
        .bind(&logical_name_id).bind(namespace).bind(normalized.normalized_name).bind(normalized.normalized_labels).bind(normalized.dns_encoded_name).bind(&node).bind(labelhashes).bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION).bind(chain).bind(hash).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path',$4,'2026-08-03T00:00:00Z',$5,$6,10,'canonical')")
        .bind(binding).bind(&logical_name_id).bind(resource).bind(kind.authority_arm()).bind(chain).bind(hash).execute(pool).await?;
    let resolver_role = if matches!(kind, FixtureKind::EnsV2Arm) {
        "public_resolver_v2"
    } else {
        "resolver"
    };
    let payload = json!({"contracts":[{"role":resolver_role,"address":resolver,"proxy_kind":"none","start_block":0,"read_features":[]}]});
    let manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,$1,$2,$3,'lookup-fixture','active',$4,'test/lookup-resolver.toml',$5) RETURNING manifest_id")
        .bind(namespace).bind(resolver_family).bind(chain).bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ('lookup-declaration',$1,'SourceManifestUpdated',$2,1,$3,$4,'manifest_sync','canonical',$5)")
        .bind(namespace).bind(resolver_family).bind(manifest).bind(chain).bind(json!({"rollout_status":"active","normalizer_version":bigname_domain::normalization::ENS_NORMALIZER_VERSION,"manifest_payload":payload})).execute(pool).await?;
    for (log, kind, family, named, after, emitter) in [
        (
            0i64,
            "RegistrationGranted",
            registrar,
            true,
            json!({"authority_kind":"registrar","registrant":owner,"expiry":2_000_000_000}),
            "0x0000000000000000000000000000000000000100",
        ),
        (
            1,
            "AuthorityTransferred",
            registry,
            true,
            json!({"node":node,"owner":owner}),
            ENS_REGISTRY,
        ),
        (
            2,
            "ResolverChanged",
            registry,
            true,
            json!({"node":node,"resolver":resolver}),
            ENS_REGISTRY,
        ),
        (
            3,
            "RecordChanged",
            resolver_family,
            false,
            json!({"source_event":"TextChanged","node":node,"resolver":resolver,"record_key":"text:url","record_family":"text","selector_key":"url","value":indexed_value}),
            resolver,
        ),
    ] {
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state) VALUES ($1,$2,$3,$4::uuid,$5,$6,1,$7,$8,10,$9,'0xlookupfixture',0,$10,$11,'ens_v1_unwrapped_authority','canonical',$12)")
            .bind(format!("lookup-input-{log}")).bind(namespace).bind(named.then_some(logical_name_id.as_str())).bind(named.then_some(resource)).bind(kind).bind(family).bind((kind=="RecordChanged").then_some(manifest)).bind(chain).bind(hash).bind(log).bind(json!({"kind":"raw_log","emitting_address":emitter,"transaction_index":0,"block_timestamp":"2026-08-03T00:00:00Z"})).bind(after).execute(pool).await?;
    }
    if publish {
        publish_lookup_families(pool, chain, 10, FamilyMode::Normal).await?;
        if chain != ETHEREUM {
            publish_lookup_families(pool, ETHEREUM, 10, FamilyMode::Normal).await?;
        }
    }
    Ok(Fixture {
        database,
        logical_name_id,
    })
}

pub(super) async fn publish_lookup_families(
    pool: &PgPool,
    chain: &str,
    target: i64,
    mode: FamilyMode,
) -> AnyResult<()> {
    let hash = sqlx::query_scalar("SELECT block_hash FROM chain_lineage WHERE chain_id=$1 AND block_number=$2 AND canonicality_state IN ('canonical','safe','finalized')")
        .bind(chain).bind(target).fetch_one(pool).await?;
    let token = bigname_project::families::input_token(pool, chain).await?;
    let outcome = bigname_project::families::apply(
        pool,
        chain,
        &bigname_project::Marker {
            number: target,
            hash,
        },
        mode,
        &token,
        &FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    anyhow::ensure!(
        outcome.marker.as_ref().map(|m| m.number) == Some(target),
        "family publication did not reach {chain}:{target}: {outcome:?}"
    );
    Ok(())
}

/// A declared ENSIP19 resolver and its actual AddressChanged default record.
pub(super) async fn seed_ensip19_default(fixture: &Fixture, value: &str) -> AnyResult<()> {
    let pool = fixture.pool();
    sqlx::query("UPDATE manifest_versions SET manifest_payload = jsonb_set(manifest_payload,'{contracts,0,read_features}','[\"ensip19_default_address\"]'::jsonb) WHERE file_path='test/lookup-resolver.toml'").execute(pool).await?;
    sqlx::query("UPDATE normalized_events declaration SET after_state = jsonb_set(declaration.after_state,'{manifest_payload}',manifest.manifest_payload) FROM manifest_versions manifest WHERE declaration.source_manifest_id=manifest.manifest_id AND declaration.event_kind='SourceManifestUpdated' AND manifest.file_path='test/lookup-resolver.toml'").execute(pool).await?;
    let node: String =
        sqlx::query_scalar("SELECT namehash FROM name_surfaces WHERE logical_name_id=$1")
            .bind(&fixture.logical_name_id)
            .fetch_one(pool)
            .await?;
    let payload = json!({"source_event":"AddressChanged", "node":node,
        "resolver":"0x1000000000000000000000000000000000000001",
        "record_key":"addr:2147483648", "record_family":"addr", "selector_key":"2147483648",
        "coin_type":"2147483648", "address_bytes_hex":value, "value_retained":false});
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state)
        SELECT 'lookup-default-address',namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,4,raw_fact_ref,derivation_kind,canonicality_state,$1 FROM normalized_events WHERE event_identity='lookup-input-3'").bind(payload).execute(pool).await?;
    publish_lookup_families(pool, ETHEREUM, 10, FamilyMode::Rebuild).await
}

/// An observed wildcard binding resolves through an actual bound ancestor pointer.
pub(super) async fn seed_wildcard_ancestor(fixture: &Fixture) -> AnyResult<()> {
    let pool = fixture.pool();
    let normalized = bigname_domain::normalization::normalize_name("eth")?;
    let node = hex_string(&namehash("eth")?);
    let logical = format!("ens:{node}");
    let resource = "00000000-0000-0000-0000-000000000202";
    let binding = "00000000-0000-0000-0000-000000000203";
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')").bind(resource).bind(ETHEREUM).bind(ETHEREUM_HASH).execute(pool).await?;
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens','eth',$2,$3,$4,$5,$6,'active',$7,$8,10,'canonical')")
        .bind(&logical).bind(normalized.normalized_labels).bind(normalized.dns_encoded_name).bind(&node).bind(vec![format!("{:#x}",keccak256(b"eth"))]).bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION).bind(ETHEREUM).bind(ETHEREUM_HASH).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v1','2026-08-03T00:00:00Z',$4,$5,10,'canonical')")
        .bind(binding).bind(&logical).bind(resource).bind(ETHEREUM).bind(ETHEREUM_HASH).execute(pool).await?;
    sqlx::query("UPDATE surface_bindings SET binding_kind='observed_wildcard_path' WHERE logical_name_id=$1").bind(&fixture.logical_name_id).execute(pool).await?;
    for (source, log) in [("lookup-input-1", 5i64), ("lookup-input-2", 6)] {
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state)
            SELECT $1,namespace,$2,$3::uuid,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,$4,raw_fact_ref,derivation_kind,canonicality_state,jsonb_set(after_state,'{node}',to_jsonb($5::text)) FROM normalized_events WHERE event_identity=$6")
            .bind(format!("wildcard-ancestor-{log}")).bind(&logical).bind(resource).bind(log).bind(&node).bind(source).execute(pool).await?;
    }
    sqlx::query("UPDATE normalized_events SET after_state=jsonb_set(after_state,'{resolver}',to_jsonb('0x0000000000000000000000000000000000000000'::text)) WHERE event_identity='lookup-input-2'").execute(pool).await?;
    publish_lookup_families(pool, ETHEREUM, 10, FamilyMode::Rebuild).await?;
    let row = bigname_storage::families::name::load_family_name(pool, &fixture.logical_name_id)
        .await?
        .context("wildcard name")?;
    anyhow::ensure!(
        row.declared_summary["topology"]["wildcard"]["source"]["logical_name_id"] == logical,
        "actual wildcard ancestor: {:?}",
        row.declared_summary
    );
    Ok(())
}
