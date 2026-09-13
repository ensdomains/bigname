//! A name whose current ENSv2 resolver is a declared ENSv1 mirror resolver serves the same
//! name's ENSv1 inventory (`docs/projections.md` § Resolver and records, ENSv1 mirror resolver).

use anyhow::{Context, Result};
use bigname_project::{BatchOutcome, BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const V1_REGISTRY: &str = "0x4444444444444444444444444444444444444401";
const V1_RESOLVER: &str = "0x1111111111111111111111111111111111111111";
const MIRROR: &str = "0x1010101010101010101010101010101010101010";
const ZERO20: &str = "0x0000000000000000000000000000000000000000";
const ADDRESS: &str = "0x2222222222222222222222222222222222222222";
const V1_RESOURCE: &str = "69100000-0000-0000-0000-000000000001";
const V2_RESOURCE: &str = "69100000-0000-0000-0000-000000000002";
const V1_BINDING: &str = "69100000-0000-0000-0000-000000000101";
const V2_BINDING: &str = "69100000-0000-0000-0000-000000000102";
const ROOT_INSTANCE: &str = "69100000-0000-0000-0000-000000000201";
const MIRROR_INSTANCE: &str = "69100000-0000-0000-0000-000000000202";
const NAME: &str = "mirror.fixture";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum V1Side {
    /// The ENSv1 registry points the same name at a declared ENSv1 resolver with records.
    Projected,
    /// The ENSv1 registry never selected a resolver for the name.
    Absent,
    /// The ENSv1 registry cleared its resolver at block `base + 2`.
    Cleared,
}

#[derive(Clone, Debug)]
struct Fixture {
    id: &'static str,
    base: i64,
    mirror: &'static str,
    v2_payload: Option<Value>,
    v1_side: V1Side,
}

impl Fixture {
    fn declared(id: &'static str, v1_side: V1Side) -> Self {
        Self {
            id,
            base: 10,
            mirror: MIRROR,
            v2_payload: None,
            v1_side,
        }
    }
    fn target(&self) -> i64 {
        self.base + 3
    }
    fn v2_payload(&self) -> Value {
        self.v2_payload.clone().unwrap_or_else(|| {
            json!({
                "deployment_epoch": "fixture",
                "correlation_addresses": {"ens_v1_registry": V1_REGISTRY},
                "contracts": [{
                    "role": "ensv1_mirror_resolver", "address": self.mirror,
                    "proxy_kind": "none", "start_block": 0
                }],
                "capability_flags": {}
            })
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Execution {
    FromZero,
    PerBlock,
    TwoByTwo,
    Idempotent,
    RedoLastBlock,
}

#[tokio::test]
async fn mirrored_name_serves_the_ensv1_inventory_with_mirror_provenance() -> Result<()> {
    let fixture = Fixture::declared("mirror_projected", V1Side::Projected);
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v1 = inventory(&pool, V1_RESOURCE).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v1["support_status"], "supported", "{v1}");
    assert_eq!(v2["support_status"], "supported", "{v2}");
    assert_eq!(v2["unsupported_reason"], Value::Null);
    assert_eq!(v2["entries"], v1["entries"], "{v2}");
    assert_eq!(v2["selectors"], v1["selectors"]);
    assert_eq!(v2["last_change"], v1["last_change"]);
    assert_eq!(v2["unsupported_families"], json!([]));
    let keys: Vec<_> = v2["entries"]
        .as_array()
        .context("entries")?
        .iter()
        .map(|entry| entry["record_key"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(keys, ["addr:60", "text:url"]);
    assert_eq!(v2["provenance"]["resolver_address"], MIRROR);
    assert_eq!(
        v2["provenance"]["read_rules"],
        json!([{"kind": "ensip19_default_address", "source_record_key": "addr:2147483648"}])
    );
    assert_eq!(
        v2["provenance"]["record_event_ids"],
        v1["provenance"]["record_event_ids"]
    );
    assert_eq!(
        v2["provenance"]["attributed_event_ids"],
        v1["provenance"]["attributed_event_ids"]
    );
    let v1_pointer = v1["provenance"]["resolver_pointer_event_id"].clone();
    assert_eq!(
        v2["provenance"]["mirror"],
        json!({
            "resolver_address": MIRROR,
            "mirrored_source_family": "ens_v1_resolver_l1",
            "mirrored_registry_source_family": "ens_v1_registry_l1",
            "mirrored_registry_address": V1_REGISTRY,
            "mirrored_resolver_address": V1_RESOLVER,
            "mirrored_resource_id": V1_RESOURCE,
            "mirrored_pointer_event_id": v1_pointer,
            "mirrored_pointer_source_family": "ens_v1_registry_l1"
        }),
        "{v2}"
    );
    assert_ne!(
        v2["provenance"]["resolver_pointer_event_id"],
        v1["provenance"]["resolver_pointer_event_id"]
    );
    assert_eq!(v2["record_version_boundary"]["resource_id"], V2_RESOURCE);
    assert_eq!(
        v2["record_version_boundary"]["chain_position"],
        v1["record_version_boundary"]["chain_position"]
    );
    assert_eq!(
        v2["record_version_boundary_key"],
        boundary_key(&v2["record_version_boundary"], CHAIN)
    );
    assert_eq!(
        v2["chain_positions"]["block_number"],
        json!(fixture.base + 3)
    );

    let resolver = resolver_current(&pool, MIRROR).await?;
    assert_eq!(resolver["support_status"], "supported", "{resolver}");
    assert_eq!(
        resolver["declared_summary"]["classification"],
        json!({
            "source_family": "ens_v2_resolver_l1",
            "role": "ensv1_mirror_resolver",
            "basis": "manifest_declared_address",
            "read_features": [],
            "mirror": {
                "mirrored_source_family": "ens_v1_resolver_l1",
                "mirrored_registry_source_family": "ens_v1_registry_l1",
                "mirrored_registry_address": V1_REGISTRY
            }
        }),
        "{resolver}"
    );
    assert_eq!(
        resolver["declared_summary"]["bindings"]["status"],
        "supported"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mirrored_inventory_converges_across_incremental_and_redo_execution() -> Result<()> {
    let fixture = Fixture::declared("mirror_replay", V1Side::Projected);
    let mut previous: Option<(Value, Value)> = None;
    for execution in [
        Execution::FromZero,
        Execution::PerBlock,
        Execution::TwoByTwo,
        Execution::Idempotent,
        Execution::RedoLastBlock,
    ] {
        let (database, pool) = project(&fixture, fixture.target(), execution).await?;
        let rows = (
            inventory(&pool, V2_RESOURCE).await?,
            resolver_current(&pool, MIRROR).await?,
        );
        assert_eq!(
            rows.0["support_status"], "supported",
            "{execution:?}: {}",
            rows.0
        );
        // The ENSv1 write at base + 3 must have rebuilt the mirrored row, not only the ENSv1 row.
        assert_eq!(
            rows.0["last_change"]["chain_position"]["block_number"],
            json!(fixture.base + 3),
            "{execution:?}: {}",
            rows.0
        );
        if let Some(previous) = &previous {
            assert_eq!(&rows, previous, "{execution:?} drifted");
        }
        previous = Some(rows);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn mirror_without_a_projected_ensv1_resolver_is_explicitly_unsupported() -> Result<()> {
    for (id, v1_side, target_offset, expected_mirrored_resolver) in [
        ("mirror_absent", V1Side::Absent, 3, None),
        ("mirror_cleared", V1Side::Cleared, 3, None),
        ("mirror_before_clear", V1Side::Cleared, 1, Some(V1_RESOLVER)),
    ] {
        let fixture = Fixture::declared(id, v1_side);
        let (database, pool) =
            project(&fixture, fixture.base + target_offset, Execution::FromZero).await?;
        let v2 = inventory(&pool, V2_RESOURCE).await?;
        if let Some(resolver) = expected_mirrored_resolver {
            assert_eq!(v2["support_status"], "supported", "{id}: {v2}");
            assert_eq!(
                v2["provenance"]["mirror"]["mirrored_resolver_address"],
                resolver
            );
            database.cleanup().await?;
            continue;
        }
        assert_eq!(v2["support_status"], "unsupported", "{id}: {v2}");
        assert_eq!(v2["unsupported_reason"], "mirrored_resolver_not_projected");
        assert_eq!(
            v2["unsupported_families"],
            json!([{
                "record_family": "resolver_classification",
                "unsupported_reason": "mirrored_resolver_not_projected"
            }])
        );
        assert_eq!(v2["entries"], json!([]));
        assert_eq!(v2["selectors"], json!([]));
        assert_eq!(v2["provenance"]["resolver_address"], MIRROR);
        assert_eq!(v2["provenance"]["read_rules"], json!([]));
        assert_eq!(
            v2["provenance"]["mirror"],
            json!({
                "resolver_address": MIRROR,
                "mirrored_source_family": "ens_v1_resolver_l1",
                "mirrored_registry_source_family": "ens_v1_registry_l1",
                "mirrored_registry_address": V1_REGISTRY
            }),
            "{id}: {v2}"
        );
        assert_eq!(
            v2["record_version_boundary"]["normalized_event_id"],
            Value::Null
        );
        assert_eq!(v2["record_version_boundary"]["event_kind"], Value::Null);
        assert_eq!(
            v2["record_version_boundary"]["chain_position"]["block_number"],
            json!(fixture.base)
        );
        assert_eq!(
            v2["record_version_boundary_key"],
            boundary_key(&v2["record_version_boundary"], CHAIN)
        );
        assert_eq!(v2["last_change"]["event_kind"], "ResolverChanged");
        let resolver = resolver_current(&pool, MIRROR).await?;
        assert_eq!(resolver["support_status"], "supported", "{id}: {resolver}");
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn undeclared_ensv2_resolver_without_upgrade_history_is_unchanged() -> Result<()> {
    let mut fixture = Fixture::declared("mirror_undeclared", V1Side::Projected);
    fixture.v2_payload = Some(json!({
        "deployment_epoch": "fixture", "contracts": [], "capability_flags": {}
    }));
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "unsupported", "{v2}");
    assert_eq!(v2["unsupported_reason"], "resolver_upgrade_not_observed");
    assert!(v2["provenance"].get("mirror").is_none(), "{v2}");
    let resolver = resolver_current(&pool, MIRROR).await?;
    assert_eq!(resolver["support_status"], "unsupported");
    assert_eq!(
        resolver["unsupported_reason"],
        "resolver_upgrade_not_observed"
    );
    assert_eq!(
        resolver["declared_summary"]["classification"]["basis"], "erc1967_upgraded_history",
        "{resolver}"
    );
    assert!(
        resolver["declared_summary"]["classification"]
            .get("mirror")
            .is_none()
    );
    let v1 = inventory(&pool, V1_RESOURCE).await?;
    assert_eq!(v1["support_status"], "supported", "{v1}");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn hackathon_manifest_declares_the_mirror_and_classifies_it() -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let repository = bigname_manifests::load_repository(root.join("manifests/sepolia-hackathon"))?;
    let manifest = &repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v2_resolver_l1")
        .context("hackathon ens_v2_resolver_l1 manifest")?
        .manifest;
    let mirror = manifest
        .contracts
        .iter()
        .find(|contract| contract.role == bigname_manifests::ENSV1_MIRROR_RESOLVER_ROLE)
        .context("hackathon mirror declaration")?;
    assert_eq!(
        mirror.address.to_ascii_lowercase(),
        "0x10107255fda20ab6c37a0efca1e9465f25066a00"
    );
    assert_eq!(mirror.start_block, Some(11_626_641));
    assert_eq!(mirror.proxy_kind, "none");
    let registry = manifest.correlation_addresses
        [bigname_manifests::ENSV1_MIRROR_REGISTRY_CORRELATION_KEY]
        .to_ascii_lowercase();
    assert_eq!(registry, "0x82080cc8ca78597bde586a003d0a080c79a1814b");

    let address: &'static str = Box::leak(mirror.address.to_ascii_lowercase().into_boxed_str());
    let fixture = Fixture {
        id: "mirror_hackathon",
        base: i64::try_from(mirror.start_block.unwrap())? + 1,
        mirror: address,
        v2_payload: Some(serde_json::to_value(manifest)?),
        v1_side: V1Side::Projected,
    };
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let resolver = resolver_current(&pool, address).await?;
    assert_eq!(resolver["support_status"], "supported", "{resolver}");
    assert_eq!(
        resolver["declared_summary"]["classification"]["role"],
        "ensv1_mirror_resolver"
    );
    assert_eq!(
        resolver["declared_summary"]["classification"]["mirror"]["mirrored_registry_address"],
        registry
    );
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "supported", "{v2}");
    assert_eq!(
        v2["provenance"]["mirror"]["mirrored_registry_address"],
        registry
    );
    assert_eq!(
        v2["provenance"]["mirror"]["mirrored_resolver_address"],
        V1_RESOLVER
    );
    database.cleanup().await?;
    Ok(())
}

fn boundary_key(boundary: &Value, chain_id: &str) -> String {
    let part = |value: &str| format!("{}:{value};", value.len());
    let text = |value: &Value| match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    [
        text(&boundary["logical_name_id"]),
        text(&boundary["resource_id"]),
        text(&boundary["normalized_event_id"]),
        text(&boundary["event_kind"]),
        chain_id.to_owned(),
        text(&boundary["chain_position"]["block_number"]),
        text(&boundary["chain_position"]["block_hash"]),
        text(&boundary["chain_position"]["timestamp"]),
    ]
    .iter()
    .map(|value| part(value))
    .collect()
}

async fn inventory(pool: &PgPool, resource: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' \
         FROM record_inventory_current row WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_one(pool)
    .await?)
}

async fn resolver_current(pool: &PgPool, address: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' \
         FROM resolver_current row WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(CHAIN)
    .bind(address)
    .fetch_one(pool)
    .await?)
}

async fn project(
    fixture: &Fixture,
    target: i64,
    execution: Execution,
) -> Result<(TestDatabase, PgPool)> {
    let (database, pool) = database(&format!("{}_{execution:?}", fixture.id)).await?;
    seed(&pool, fixture).await?;
    let base = fixture.base;
    match execution {
        Execution::FromZero => {
            run(&pool, target, 0, target, None, RunMode::Normal).await?;
        }
        Execution::PerBlock => {
            run(&pool, base, 0, base, None, RunMode::Normal).await?;
            for block in base + 1..=target {
                run(&pool, block, block, block, Some(block - 1), RunMode::Normal).await?;
            }
        }
        Execution::TwoByTwo => {
            run(&pool, base + 1, 0, base + 1, None, RunMode::Normal).await?;
            run(
                &pool,
                target,
                base + 2,
                target,
                Some(base + 1),
                RunMode::Normal,
            )
            .await?;
        }
        Execution::Idempotent => {
            run(&pool, target - 1, 0, target - 1, None, RunMode::Normal).await?;
            run(
                &pool,
                target,
                target,
                target,
                Some(target - 1),
                RunMode::Normal,
            )
            .await?;
            run(
                &pool,
                target,
                target,
                target,
                Some(target - 1),
                RunMode::Normal,
            )
            .await?;
        }
        Execution::RedoLastBlock => {
            run(&pool, target, 0, target, None, RunMode::Normal).await?;
            run(&pool, target, target, target, Some(target), RunMode::Redo).await?;
        }
    }
    let raw_count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_logs")
        .fetch_one(&pool)
        .await?;
    assert_eq!(raw_count, 0, "Project wrote raw facts");
    Ok((database, pool))
}

async fn run(
    pool: &PgPool,
    target_block: i64,
    affected_from_block: i64,
    affected_to_block: i64,
    resume_current: Option<i64>,
    mode: RunMode,
) -> Result<BatchOutcome> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block,
            affected_from_block,
            affected_to_block,
            resume_current: resume_current.map(|number| Marker {
                number,
                hash: block_hash(number),
            }),
            mode,
        })
        .await?;
    assert!(outcome.complete);
    assert_eq!(outcome.target.number, target_block);
    Ok(outcome)
}

async fn manifest(
    pool: &PgPool,
    fixture: &Fixture,
    source_family: &str,
    payload: &Value,
) -> Result<i64> {
    let manifest_id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens',$1,$2,'fixture','active','fixture',$3,$4) RETURNING manifest_id")
        .bind(source_family).bind(CHAIN)
        .bind(format!("fixture/{}/{source_family}.toml", fixture.id)).bind(payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','SourceManifestUpdated',$2,1,$3,$4,'manifest_sync','canonical',$5)")
        .bind(format!("manifest:{}:{source_family}", fixture.id)).bind(source_family)
        .bind(manifest_id).bind(CHAIN)
        .bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload}))
        .execute(pool).await?;
    Ok(manifest_id)
}

async fn seed(pool: &PgPool, fixture: &Fixture) -> Result<()> {
    let base = fixture.base;
    for number in base..=base + 3 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($4),'canonical')")
            .bind(CHAIN).bind(block_hash(number)).bind(number).bind(1_800_000_000 + number).execute(pool).await?;
    }
    let v2_manifest = manifest(pool, fixture, "ens_v2_resolver_l1", &fixture.v2_payload()).await?;
    let v1_manifest = manifest(
        pool,
        fixture,
        "ens_v1_resolver_l1",
        &json!({"deployment_epoch":"fixture","contracts":[{
            "role":"public_resolver","address":V1_RESOLVER,"proxy_kind":"none","start_block":0,
            "read_features":["ensip19_default_address"]
        }]}),
    )
    .await?;
    let root_manifest = manifest(pool, fixture, "ens_v2_root_l1", &json!({})).await?;
    let _ = v2_manifest;
    for instance in [ROOT_INSTANCE, MIRROR_INSTANCE] {
        sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')")
            .bind(instance).bind(CHAIN).execute(pool).await?;
    }
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,active_from_block_hash,source_manifest_id) VALUES ($1::uuid,$2,$3,$4,$5,$6)")
        .bind(MIRROR_INSTANCE).bind(CHAIN).bind(fixture.mirror).bind(base).bind(block_hash(base))
        .bind(root_manifest).execute(pool).await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state) VALUES ($1,'resolver',$2::uuid,$3::uuid,'fixture','fixture',$4,$5,$6,'canonical')")
        .bind(CHAIN).bind(ROOT_INSTANCE).bind(MIRROR_INSTANCE)
        .bind(root_manifest).bind(base).bind(block_hash(base)).execute(pool).await?;

    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    let logical_name_id = format!("ens:{node}");
    let labelhashes: Vec<_> = NAME
        .split('.')
        .map(|label| format!("{:#x}", alloy_primitives::keccak256(label)))
        .collect();
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens',$2,string_to_array($2,'.'),$3,$4,$5,'fixture','active',$6,$7,$8,'canonical')")
        .bind(&logical_name_id).bind(NAME).bind(b"\x06mirror\x07fixture\0".as_slice()).bind(&node)
        .bind(labelhashes).bind(CHAIN).bind(block_hash(base)).bind(base).execute(pool).await?;
    for (resource, binding, arm) in [
        (V1_RESOURCE, V1_BINDING, "ens_v1"),
        (V2_RESOURCE, V2_BINDING, "ens_v2"),
    ] {
        sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,$4,'canonical')")
            .bind(resource).bind(CHAIN).bind(block_hash(base)).bind(base).execute(pool).await?;
        sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path',$4,to_timestamp($5),$6,$7,$8,'canonical')")
            .bind(binding).bind(&logical_name_id).bind(resource).bind(arm)
            .bind(1_800_000_000 + base).bind(CHAIN).bind(block_hash(base)).bind(base).execute(pool).await?;
    }

    let mut events = vec![Event {
        identity: "v2-pointer",
        logical_name_id: Some(logical_name_id.clone()),
        resource_id: Some(V2_RESOURCE),
        kind: "ResolverChanged",
        source_family: "ens_v2_root_l1",
        manifest_id: Some(root_manifest),
        block: base,
        log_index: 0,
        emitter: V1_REGISTRY,
        after_state: json!({"node": node, "resolver": fixture.mirror}),
    }];
    if fixture.v1_side != V1Side::Absent {
        events.push(Event {
            identity: "v1-pointer",
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(V1_RESOURCE),
            kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            manifest_id: None,
            block: base,
            log_index: 1,
            emitter: V1_REGISTRY,
            after_state: json!({"node": node, "resolver": V1_RESOLVER}),
        });
        events.push(record(
            "v1-text",
            base + 1,
            &node,
            v1_manifest,
            json!({"record_key": "text:url", "record_family": "text", "selector_key": "url",
                   "source_event": "TextChanged", "value": "https://one.example"}),
        ));
        events.push(record(
            "v1-addr",
            base + 3,
            &node,
            v1_manifest,
            json!({"record_key": "addr:60", "record_family": "addr", "selector_key": "60",
                   "source_event": "AddrChanged", "value": ADDRESS}),
        ));
    }
    if fixture.v1_side == V1Side::Cleared {
        events.push(Event {
            identity: "v1-clear",
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(V1_RESOURCE),
            kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            manifest_id: None,
            block: base + 2,
            log_index: 0,
            emitter: V1_REGISTRY,
            after_state: json!({"node": node, "resolver": ZERO20}),
        });
    }
    for event in events {
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES ($1,'ens',$2,$3::uuid,$4,$5,1,$6,$7,$8,$9,$10,0,$11,'ens_v1_unwrapped_authority','canonical',$12,$13)")
            .bind(format!("{}:{}", fixture.id, event.identity)).bind(event.logical_name_id)
            .bind(event.resource_id).bind(event.kind).bind(event.source_family).bind(event.manifest_id)
            .bind(CHAIN).bind(event.block).bind(block_hash(event.block))
            .bind(format!("0x{:064x}", event.block * 10 + event.log_index)).bind(event.log_index)
            .bind(event.after_state).bind(json!({"emitting_address": event.emitter}))
            .execute(pool).await?;
    }
    Ok(())
}

struct Event {
    identity: &'static str,
    logical_name_id: Option<String>,
    resource_id: Option<&'static str>,
    kind: &'static str,
    source_family: &'static str,
    manifest_id: Option<i64>,
    block: i64,
    log_index: i64,
    emitter: &'static str,
    after_state: Value,
}

fn record(
    identity: &'static str,
    block: i64,
    node: &str,
    manifest_id: i64,
    mut after: Value,
) -> Event {
    after["node"] = json!(node);
    after["resolver"] = json!(V1_RESOLVER);
    Event {
        identity,
        logical_name_id: None,
        resource_id: None,
        kind: "RecordChanged",
        source_family: "ens_v1_resolver_l1",
        manifest_id: Some(manifest_id),
        block,
        log_index: 1,
        emitter: V1_RESOLVER,
        after_state: after,
    }
}

fn block_hash(number: i64) -> String {
    format!("0x{number:064x}")
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new(format!("mirror_{name}")).pool_max_connections(1),
    )
    .await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public; SET LOCAL search_path TO bigname_phase, public", database_name.replace('"', "\"\""))).execute(&mut *transaction).await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connection = pool.acquire().await?;
    sqlx::query("SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    drop(connection);
    Ok((database, pool))
}
