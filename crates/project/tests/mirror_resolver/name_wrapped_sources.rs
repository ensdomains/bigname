//! The one known pair of sources that write one registry-node pointer at one log
//! (docs/glossary.md#emission-ordinal): a NameWrapped log, interpreted by the real adapter,
//! writes the name's ENSv1 registry pointer from the wrapper and from the registry-read surface
//! materialization, with the same resolver. The served mirror selector reads the registry-side
//! pointer the mirror walk would call (`crates/project/src/builders/record_inventory/mirror.rs`)
//! and keeps the fact inserted last; the owned key families keep the fact with the higher
//! emission ordinal. The resolver value agrees; the resource, source family and event
//! attribution differ, and that is the whole of the disclosed difference.
use super::*;
use alloy_primitives::{Address, B256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, NormalizedEvent, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use time::OffsetDateTime;
use uuid::Uuid;

#[path = "../support/adapter_output.rs"]
mod adapter_output;

sol! {
    event Transfer(bytes32 indexed node, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
}

const WRAPPED_NAME: &str = "pointer.eth";
const OWNER: &str = "0x00000000000000000000000000000000000000a3";
const WRAPPED_OWNER: &str = "0x00000000000000000000000000000000000000a5";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
const WRAPPER: &str = "0x00000000000000000000000000000000000000a6";
const WRAPPED_V2_RESOURCE: &str = "69100000-0000-0000-0000-000000000005";
const WRAPPED_V2_BINDING: &str = "69100000-0000-0000-0000-000000000105";

fn timestamp(block: i64) -> Result<OffsetDateTime> {
    Ok(OffsetDateTime::from_unix_timestamp(1_800_000_000 + block)?)
}

fn raw_log(data: alloy_primitives::LogData, block: i64, emitter: &str) -> Result<RawLogInput> {
    Ok(RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: block_hash(block),
        block_number: block,
        block_timestamp: timestamp(block)?,
        canonicality_state: "canonical".to_owned(),
        transaction_hash: format!("0x{:064x}", 7_000 + block),
        transaction_index: 0,
        log_index: 0,
        emitting_address: emitter.to_owned(),
        topics: data
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: data.data.to_vec(),
    })
}

/// The checked-in Sepolia ENSv1 registry, registrar and wrapper manifests, registered in the
/// fixture database the way the harness registers its own, so the adapter and Project cite the
/// same manifest ids.
async fn manifests(pool: &PgPool, fixture: &Fixture) -> Result<Vec<ManifestInput>> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let mut inputs = Vec::new();
    for source_family in [
        "ens_v1_registry_l1",
        "ens_v1_registrar_l1",
        "ens_v1_wrapper_l1",
    ] {
        let loaded = repository
            .manifests()
            .iter()
            .find(|loaded| {
                loaded.manifest.chain == CHAIN
                    && loaded.manifest.source_family == source_family
                    && loaded.version_tag == "v1"
            })
            .with_context(|| format!("the checked-in {source_family} v1 manifest"))?;
        let payload = serde_json::to_value(&loaded.manifest)?;
        let manifest_id = manifest(pool, fixture, source_family, &payload).await?;
        inputs.push(ManifestInput {
            manifest_id,
            manifest_version: 1,
            namespace: loaded.manifest.namespace.clone(),
            source_family: source_family.to_owned(),
            chain_id: CHAIN.to_owned(),
            deployment_label: "fixture".to_owned(),
            normalizer_version: loaded.manifest.normalizer_version.clone(),
            payload_json: payload.to_string(),
        });
    }
    Ok(inputs)
}

fn admission(
    manifest: &ManifestInput,
    instance: u128,
    role: &str,
    address: &str,
) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: address.to_owned(),
        contract_instance_id: Uuid::from_u128(instance),
        source_manifest_id: Some(manifest.manifest_id),
        role: Some(role.to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

/// The registry gives the node an owner at `base`, selects `V1_RESOLVER` for it at `base + 1`,
/// and the NameWrapper wraps it at `base + 2`, as in the adapter's own
/// `wrapper_first_surface_links_the_retained_registry_read_resource`
/// (`crates/adapters/src/schema_v2/tests/v1_pre_surface_resolver.rs`).
fn interpret(manifests: Vec<ManifestInput>, base: i64, node: B256) -> Result<BatchOutput> {
    let admissions = vec![
        admission(&manifests[0], 9_401, "registry", V1_REGISTRY),
        admission(&manifests[1], 9_402, "registrar", REGISTRAR),
        admission(&manifests[2], 9_403, "name_wrapper", WRAPPER),
    ];
    let mut dns = Vec::new();
    for label in WRAPPED_NAME.split('.') {
        dns.push(u8::try_from(label.len())?);
        dns.extend_from_slice(label.as_bytes());
    }
    dns.push(0);
    let raw_logs = vec![
        raw_log(
            Transfer {
                node,
                owner: OWNER.parse::<Address>()?,
            }
            .encode_log_data(),
            base,
            V1_REGISTRY,
        )?,
        raw_log(
            NewResolver {
                node,
                resolver: V1_RESOLVER.parse::<Address>()?,
            }
            .encode_log_data(),
            base + 1,
            V1_REGISTRY,
        )?,
        raw_log(
            NameWrapped {
                node,
                name: dns.into(),
                owner: WRAPPED_OWNER.parse::<Address>()?,
                fuses: 1,
                expiry: 9_999,
            }
            .encode_log_data(),
            base + 2,
            WRAPPER,
        )?,
    ];
    let blocks = (base..=base + 2)
        .map(|block| {
            Ok(RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: block_hash(block),
                block_number: block,
                block_timestamp: timestamp(block)?,
                canonicality_state: "canonical".to_owned(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests,
            discovery_rules: Vec::new(),
            admissions,
            prior_events: Vec::new(),
            blocks,
            raw_logs,
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(Vec::new())?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    Ok(output)
}

/// Point the wrapped name's ENSv2 resource at the mirror, so the served build runs the mirror
/// walk over the name's ENSv1 registry pointer.
async fn point_ensv2_at_mirror(
    pool: &PgPool,
    fixture: &Fixture,
    logical_name_id: &str,
    node: &str,
) -> Result<()> {
    let block = fixture.base + 3;
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,$4,'canonical')")
        .bind(WRAPPED_V2_RESOURCE).bind(CHAIN).bind(block_hash(block)).bind(block).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v2',to_timestamp($4),$5,$6,$7,'canonical')")
        .bind(WRAPPED_V2_BINDING).bind(logical_name_id).bind(WRAPPED_V2_RESOURCE)
        .bind(1_800_000_000 + block).bind(CHAIN).bind(block_hash(block)).bind(block).execute(pool).await?;
    insert_event(
        pool,
        fixture,
        Event {
            identity: "wrapped-v2-pointer",
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(WRAPPED_V2_RESOURCE),
            kind: "ResolverChanged",
            source_family: "ens_v2_root_l1",
            manifest_id: Some(manifest_id(pool, "ens_v2_root_l1").await?),
            block,
            log_index: 0,
            emitter: V1_REGISTRY,
            after_state: json!({"node": node, "resolver": fixture.mirror}),
        },
    )
    .await?;
    Ok(())
}

fn pointer_facts(output: &BatchOutput, block: i64) -> Vec<&NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "ResolverChanged" && event.block_number == Some(block))
        .collect()
}

#[tokio::test]
async fn name_wrapped_pointer_differs_from_the_served_mirror_in_attribution_only() -> Result<()> {
    let fixture = Fixture::declared("name_wrapped_sources", V1Side::Absent);
    let (database, pool) = database(fixture.id).await?;
    seed(&pool, &fixture).await?;
    let node_hex = bigname_lookup::ens_namehash_hex(WRAPPED_NAME)?;
    let node: B256 = node_hex.parse()?;
    let logical_name_id = format!("ens:{node_hex}");
    let wrapped_block = fixture.base + 2;

    let output = interpret(manifests(&pool, &fixture).await?, fixture.base, node)?;
    let facts = pointer_facts(&output, wrapped_block);
    assert_eq!(facts.len(), 2, "{facts:#?}");
    let (wrapper, registry) = (facts[0], facts[1]);
    assert_eq!(wrapper.source_family, "ens_v1_wrapper_l1", "{wrapper:#?}");
    assert_eq!(
        registry.source_family, "ens_v1_registry_l1",
        "{registry:#?}"
    );
    assert_eq!(registry.after_state["surface_materialization"], true);
    for fact in [wrapper, registry] {
        assert_eq!(
            (fact.transaction_index, fact.log_index),
            (Some(0), Some(0)),
            "both facts come from the NameWrapped log"
        );
        assert_eq!(fact.after_state["resolver"], V1_RESOLVER);
    }
    assert_ne!(wrapper.resource_id, registry.resource_id);
    let ordinal = |fact: &NormalizedEvent| {
        families::emission_ordinal(fact.transaction_index, fact.log_index, &fact.event_identity)
    };
    assert_eq!(ordinal(registry), Some(0), "{}", registry.event_identity);
    assert!(
        matches!(ordinal(wrapper), Some(4 | 5)),
        "the wrapper fact follows its epoch, and a SurfaceBound when one is emitted: {}",
        wrapper.event_identity
    );

    adapter_output::persist_output(&pool, &output).await?;
    point_ensv2_at_mirror(&pool, &fixture, &logical_name_id, &node_hex).await?;
    let target = fixture.target();
    let id_of = |identity: &str| {
        sqlx::query_scalar::<_, i64>(
            "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
        )
        .bind(identity.to_owned())
        .fetch_one(&pool)
    };
    let wrapper_id = id_of(&wrapper.event_identity).await?;
    let registry_id = id_of(&registry.event_identity).await?;
    assert!(
        registry_id > wrapper_id,
        "Interpret inserts the sourced batch last"
    );
    // Step 2's disclosed attribution difference, as step 4's shadow harness reports it: the
    // mirrored resolver agrees, and the family keeps the wrapper fact (the higher emission
    // ordinal at one log, then identity bytes) where today keeps the registry fact.
    let expected = Expectations {
        differences: vec![ExpectedDifference {
            target,
            key: format!("record_inventory {WRAPPED_V2_RESOURCE}"),
            fields: vec![
                (
                    "provenance.mirror.mirrored_pointer_event_id".into(),
                    Some(json!(registry_id)),
                    Some(json!(wrapper_id)),
                ),
                (
                    "provenance.mirror.mirrored_pointer_source_family".into(),
                    Some(json!("ens_v1_registry_l1")),
                    Some(json!("ens_v1_wrapper_l1")),
                ),
                (
                    "provenance.mirror.mirrored_resource_id".into(),
                    Some(json!(registry.resource_id)),
                    Some(json!(wrapper.resource_id)),
                ),
            ],
            times: 1,
        }],
        ..Expectations::none()
    };
    run_expecting(&pool, target, 0, target, None, RunMode::Normal, &expected).await?;
    expected.finish()?;

    let served = inventory(&pool, WRAPPED_V2_RESOURCE).await?;
    let mirror = &served["provenance"]["mirror"];
    assert_eq!(
        json!({
            "resolver": mirror["mirrored_resolver_address"],
            "resource": mirror["mirrored_resource_id"],
            "source_family": mirror["mirrored_pointer_source_family"],
            "event": mirror["mirrored_pointer_event_id"],
        }),
        json!({
            "resolver": V1_RESOLVER,
            "resource": registry.resource_id,
            "source_family": "ens_v1_registry_l1",
            "event": registry_id,
        }),
        "{served}"
    );

    let marker = Marker {
        number: target,
        hash: block_hash(target),
    };
    let token = families::input_token(&pool, CHAIN).await?;
    families::apply(
        &pool,
        CHAIN,
        &marker,
        FamilyMode::Normal,
        &token,
        &FamilyOptions::new("name-wrapped-sources"),
    )
    .await;
    let family: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('resolver', resolver_address, 'resource', resource_id,
             'source_family', source_family, 'event', normalized_event_id,
             'identity', event_identity)
         FROM project_registry_pointer
         WHERE chain_id = $1 AND namespace = 'ens' AND node = $2",
    )
    .bind(CHAIN)
    .bind(&node_hex)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        family,
        json!({
            "resolver": V1_RESOLVER,
            "resource": wrapper.resource_id,
            "source_family": "ens_v1_wrapper_l1",
            "event": wrapper_id,
            "identity": wrapper.event_identity,
        })
    );
    assert_eq!(family["resolver"], mirror["mirrored_resolver_address"]);
    for (field, served_field) in [
        ("resource", "mirrored_resource_id"),
        ("source_family", "mirrored_pointer_source_family"),
        ("event", "mirrored_pointer_event_id"),
    ] {
        assert_ne!(family[field], mirror[served_field], "{field}");
    }
    database.cleanup().await
}
