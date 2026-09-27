//! A reverse node reclaimed after a wrap and an unwrap, interpreted by the real adapter: PR 955
//! thread 8, taken as a declared difference with the family key kept child first. The claim sets
//! the resolver, the wrap makes the reverse name's surface known
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L374 @ ens_v1@91c966f), the
//! unwrap releases the wrapper authority
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f), and
//! the registry owner is then set to zero with the resolver kept. The reclaim is `NewOwner` under
//! addr.reverse, with no `NewResolver` when the resolver does not change
//! (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74-L86 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L182 @ ens_v1@91c966f).
//! It reactivates the registry authority, and the adapter emits a state-derived `ResolverChanged`
//! from the `NewOwner` observation, with addr.reverse in `node` and the reverse node in
//! `child_node` (crates/adapters/src/schema_v2/protocol/v1/authority_transition.rs:489-509).
//! Today's reverse claim matches `ResolverChanged` by `node` only and skips it; the family keys
//! the registry pointer child first (crates/project/src/families/keys.rs:151-156) and takes it.
//! The resolver is the same, so the claim agrees and only the resolver event differs. A reclaim
//! that changes the resolver emits a `NewResolver` at the reverse node after the `NewOwner`, and
//! both readers take that.
use super::{database, family_shadow, hash};
use alloy_primitives::{Address, B256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, NormalizedEvent, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::{BatchRequest, Engine, RunMode};
use family_shadow::{Expectations, ExpectedDifference};
use serde_json::{Value, json};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

#[path = "../support/adapter_output.rs"]
mod adapter_output;

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(bytes32 indexed node, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
}

const CHAIN: &str = "ethereum-sepolia";
const CLAIMANT: &str = "0x00000000000000000000000000000000000000a1";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const WRAPPER: &str = "0x00000000000000000000000000000000000000e3";
const REVERSE_REGISTRAR: &str = "0x00000000000000000000000000000000000000f1";
const RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const OTHER_RESOLVER: &str = "0x00000000000000000000000000000000000000d2";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
/// Claim, wrap, unwrap, relinquish, reclaim: one block each.
const RECLAIM: i64 = 5;

struct Names {
    parent: B256,
    label: B256,
    node: B256,
    parent_hex: String,
    node_hex: String,
    label_text: String,
}

fn names() -> Result<Names> {
    let label_text = CLAIMANT.trim_start_matches("0x").to_owned();
    let parent_hex = bigname_lookup::ens_namehash_hex("addr.reverse")?;
    let node_hex = bigname_lookup::ens_namehash_hex(&format!("{label_text}.addr.reverse"))?;
    Ok(Names {
        parent: parent_hex.parse()?,
        label: alloy_primitives::keccak256(label_text.as_bytes()),
        node: node_hex.parse()?,
        parent_hex,
        node_hex,
        label_text,
    })
}

fn timestamp(block: i64) -> Result<OffsetDateTime> {
    Ok(OffsetDateTime::from_unix_timestamp(1_800_000_000 + block)?)
}

fn raw_log(
    data: alloy_primitives::LogData,
    block: i64,
    log: i64,
    emitter: &str,
) -> Result<RawLogInput> {
    Ok(RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: hash(block),
        block_number: block,
        block_timestamp: timestamp(block)?,
        canonicality_state: "canonical".to_owned(),
        transaction_hash: format!("0x{:064x}", 8_000 + block),
        transaction_index: 0,
        log_index: log,
        emitting_address: emitter.to_owned(),
        topics: data
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: data.data.to_vec(),
    })
}

async fn manifest(pool: &PgPool, source_family: &str, payload: &Value) -> Result<i64> {
    let manifest_id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens',$1,$2,'fixture','active','fixture',$3,$4) RETURNING manifest_id")
        .bind(source_family).bind(CHAIN)
        .bind(format!("fixture/reclaim/{source_family}.toml")).bind(payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','SourceManifestUpdated',$2,1,$3,$4,'manifest_sync','canonical',$5)")
        .bind(format!("manifest:reclaim:{source_family}")).bind(source_family)
        .bind(manifest_id).bind(CHAIN)
        .bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload}))
        .execute(pool).await?;
    Ok(manifest_id)
}

/// The checked-in Sepolia ENSv1 registry, wrapper and reverse registrar manifests, registered in
/// the fixture database so the adapter and Project cite the same manifest ids.
async fn manifests(pool: &PgPool) -> Result<Vec<ManifestInput>> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let mut inputs = Vec::new();
    for source_family in [
        "ens_v1_registry_l1",
        "ens_v1_wrapper_l1",
        "ens_v1_reverse_l1",
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
        let manifest_id = manifest(pool, source_family, &payload).await?;
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

/// The claim with `RESOLVER` (block 1), the wrap (2), the unwrap (3), the registry owner set to
/// zero with the resolver kept (4), and the reclaim (5), which sets `reclaim_resolver` through
/// `setSubnodeRecord` and so emits `NewResolver` only when it differs from `RESOLVER`.
fn interpret(
    manifests: Vec<ManifestInput>,
    names: &Names,
    reclaim_resolver: &str,
) -> Result<BatchOutput> {
    let admissions = vec![
        admission(&manifests[0], 9_501, "registry", REGISTRY),
        admission(&manifests[1], 9_502, "name_wrapper", WRAPPER),
        admission(&manifests[2], 9_503, "reverse_registrar", REVERSE_REGISTRAR),
    ];
    let claimant: Address = CLAIMANT.parse()?;
    let mut dns = Vec::new();
    for label in [names.label_text.as_str(), "addr", "reverse"] {
        dns.push(u8::try_from(label.len())?);
        dns.extend_from_slice(label.as_bytes());
    }
    dns.push(0);
    let claim = |block: i64| -> Result<Vec<RawLogInput>> {
        Ok(vec![
            raw_log(
                ReverseClaimed {
                    addr: claimant,
                    node: names.node,
                }
                .encode_log_data(),
                block,
                0,
                REVERSE_REGISTRAR,
            )?,
            raw_log(
                NewOwner {
                    node: names.parent,
                    label: names.label,
                    owner: claimant,
                }
                .encode_log_data(),
                block,
                1,
                REGISTRY,
            )?,
        ])
    };
    let transfer = |block: i64, owner: &str| -> Result<RawLogInput> {
        raw_log(
            Transfer {
                node: names.node,
                owner: owner.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            REGISTRY,
        )
    };
    let new_resolver = |block: i64, resolver: &str| -> Result<RawLogInput> {
        raw_log(
            NewResolver {
                node: names.node,
                resolver: resolver.parse()?,
            }
            .encode_log_data(),
            block,
            2,
            REGISTRY,
        )
    };
    let mut raw_logs = claim(1)?;
    raw_logs.push(new_resolver(1, RESOLVER)?);
    raw_logs.push(transfer(2, WRAPPER)?);
    raw_logs.push(raw_log(
        NameWrapped {
            node: names.node,
            name: dns.into(),
            owner: claimant,
            fuses: 0,
            expiry: 0,
        }
        .encode_log_data(),
        2,
        1,
        WRAPPER,
    )?);
    raw_logs.push(transfer(3, CLAIMANT)?);
    raw_logs.push(raw_log(
        NameUnwrapped {
            node: names.node,
            owner: claimant,
        }
        .encode_log_data(),
        3,
        1,
        WRAPPER,
    )?);
    raw_logs.push(transfer(4, ZERO)?);
    raw_logs.extend(claim(RECLAIM)?);
    if reclaim_resolver != RESOLVER {
        raw_logs.push(new_resolver(RECLAIM, reclaim_resolver)?);
    }
    let blocks = (1..=RECLAIM)
        .map(|block| {
            Ok(RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: hash(block),
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

/// Lineage for every block, the adapter's output, and a name record at each resolver for the
/// reverse node (block 1, log 9), so the selected resolver decides the claim.
async fn persist(pool: &PgPool, output: &BatchOutput, node: &str) -> Result<()> {
    for block in 1..=RECLAIM {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($4),'canonical')")
            .bind(CHAIN).bind(hash(block)).bind(block).bind(1_800_000_000 + block).execute(pool).await?;
    }
    // Block by block, as Interpret commits them, so each block's binding closures meet only the
    // bindings of earlier blocks.
    for block in 1..=RECLAIM {
        let at = BatchOutput {
            normalized_events: output
                .normalized_events
                .iter()
                .filter(|event| event.block_number == Some(block))
                .cloned()
                .collect(),
            name_surfaces: output
                .name_surfaces
                .iter()
                .filter(|surface| surface.block_number == block)
                .cloned()
                .collect(),
            token_lineages: output
                .token_lineages
                .iter()
                .filter(|lineage| lineage.block_number == block)
                .cloned()
                .collect(),
            resources: output
                .resources
                .iter()
                .filter(|resource| resource.block_number == block)
                .cloned()
                .collect(),
            surface_bindings: output
                .surface_bindings
                .iter()
                .filter(|binding| binding.block_number == block)
                .cloned()
                .collect(),
            binding_closures: output
                .binding_closures
                .iter()
                .filter(|closure| closure.block_number == block)
                .cloned()
                .collect(),
            ..BatchOutput::default()
        };
        adapter_output::persist_output(pool, &at).await?;
    }
    for (resolver, value) in [(RESOLVER, "alice.eth"), (OTHER_RESOLVER, "other.eth")] {
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','RecordChanged','ens_v1_resolver_l1',1,$2,1,$3,$3,1,9,'ens_v1_unwrapped_authority','canonical',$4)")
            .bind(format!("reclaim-name:{resolver}")).bind(CHAIN).bind(hash(1))
            .bind(json!({
                "source_event":"NameChanged", "node":node, "resolver":resolver,
                "record_key":"name", "record_family":"name", "raw_name":value
            }))
            .execute(pool).await?;
    }
    Ok(())
}

async fn event_id(pool: &PgPool, event: &NormalizedEvent) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&event.event_identity)
    .fetch_one(pool)
    .await?)
}

fn pointers_at(output: &BatchOutput, block: i64) -> Vec<&NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "ResolverChanged" && event.block_number == Some(block))
        .collect()
}

/// Run Project over every block, then compare the family reads with today's, expecting `shadow`.
async fn run(pool: &PgPool, shadow: &Expectations) -> Result<()> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: RECLAIM,
            affected_from_block: 0,
            affected_to_block: RECLAIM,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    family_shadow::compare_family_reads_at(pool, &outcome.current, shadow).await?;
    Ok(())
}

/// Today's claim and the family's, each as (resolver, resolver event id, claim event id, name).
async fn claims(pool: &PgPool) -> Result<[Value; 2]> {
    let served: Value = sqlx::query_scalar("SELECT to_jsonb(row) FROM primary_names_current row WHERE address=$1 AND coin_type='60' AND namespace='ens'")
        .bind(CLAIMANT).fetch_one(pool).await?;
    let family = bigname_storage::families::records::load_family_reverse_claim(
        pool, CHAIN, CLAIMANT, "ens", "60",
    )
    .await?
    .context("a family claim")?;
    assert!(!family.node_claim_at_other_resolver);
    let row = family.snapshot.row;
    let pick = |provenance: &Value, name: Value| {
        json!([
            provenance["resolver_address"],
            provenance["resolver_event_id"],
            provenance["claim_event_id"],
            name,
        ])
    };
    Ok([
        pick(
            &served["claim_provenance"],
            served["raw_claim_name"].clone(),
        ),
        pick(&row.claim_provenance, json!(row.raw_claim_name)),
    ])
}

#[tokio::test]
async fn a_reclaim_after_unwrap_differs_from_today_in_the_resolver_event_only() -> Result<()> {
    let (database, pool) = database("reclaim_after_unwrap").await?;
    let names = names()?;
    let output = interpret(manifests(&pool).await?, &names, RESOLVER)?;

    // The reclaim emits one ResolverChanged: the state-derived one, from the NewOwner under
    // addr.reverse, keyed by the parent in `node` and the reverse node in `child_node`.
    let reclaim = pointers_at(&output, RECLAIM);
    assert_eq!(reclaim.len(), 1, "{reclaim:#?}");
    let derived = reclaim[0];
    assert_eq!(derived.source_family, "ens_v1_registry_l1");
    assert_eq!(derived.after_state["source_event"], "AuthorityEpochChanged");
    assert_eq!(derived.after_state["node"], names.parent_hex);
    assert_eq!(derived.after_state["child_node"], names.node_hex);
    assert_eq!(derived.after_state["resolver"], RESOLVER);
    assert_eq!(
        derived.logical_name_id.as_deref(),
        Some(format!("ens:{}", names.node_hex).as_str())
    );
    // The resource is the reverse node's registry resource, the one the claim's NewResolver was
    // written on.
    let claimed = pointers_at(&output, 1);
    assert_eq!(claimed.len(), 1, "{claimed:#?}");
    assert_eq!(claimed[0].after_state["node"], names.node_hex);
    assert!(derived.resource_id.is_some());
    assert_eq!(derived.resource_id, claimed[0].resource_id);
    // Today's latest pointer matched by `node` is one the wrap wrote at the reverse node; at one
    // log today's reader takes the higher generated id.
    let wrapped: Vec<_> = pointers_at(&output, 2)
        .into_iter()
        .filter(|event| event.after_state["node"] == names.node_hex)
        .collect();
    assert!(
        !wrapped.is_empty(),
        "the wrap writes a pointer at the reverse node"
    );

    persist(&pool, &output, &names.node_hex).await?;
    let mut served_id = None;
    for event in wrapped {
        let key = (event.log_index, event_id(&pool, event).await?);
        served_id = served_id.max(Some(key));
    }
    let served_id = served_id.context("a served pointer")?.1;
    let derived_id = event_id(&pool, derived).await?;
    let shadow = Expectations {
        differences: vec![ExpectedDifference {
            target: RECLAIM,
            key: format!("primary_name {CLAIMANT} ens 60"),
            fields: vec![(
                "claim_provenance.resolver_event_id".into(),
                Some(json!(served_id)),
                Some(json!(derived_id)),
            )],
            times: 1,
        }],
        ..Expectations::none()
    };
    run(&pool, &shadow).await?;
    shadow.finish()?;
    let [served, family] = claims(&pool).await?;
    assert_eq!(served[0], RESOLVER);
    assert_eq!(served[1], served_id);
    assert_eq!(family[1], derived_id);
    assert_eq!(
        (&served[0], &served[2], &served[3]),
        (&family[0], &family[2], &family[3])
    );
    assert_eq!(served[3], "alice.eth");
    database.cleanup().await
}

#[tokio::test]
async fn a_reclaim_with_a_new_resolver_matches_today() -> Result<()> {
    let (database, pool) = database("reclaim_new_resolver").await?;
    let names = names()?;
    let output = interpret(manifests(&pool).await?, &names, OTHER_RESOLVER)?;
    let reclaim = pointers_at(&output, RECLAIM);
    let new_resolver = *reclaim
        .iter()
        .find(|event| event.after_state["source_event"] == "NewResolver")
        .context("the reclaim's NewResolver")?;
    assert_eq!(new_resolver.after_state["node"], names.node_hex);
    assert_eq!(new_resolver.after_state["resolver"], OTHER_RESOLVER);
    assert!(
        reclaim
            .iter()
            .all(|event| event.log_index <= new_resolver.log_index),
        "{reclaim:#?}"
    );

    persist(&pool, &output, &names.node_hex).await?;
    let new_resolver_id = event_id(&pool, new_resolver).await?;
    run(&pool, &Expectations::none()).await?;
    let [served, family] = claims(&pool).await?;
    assert_eq!(served, family);
    assert_eq!(
        served,
        json!([OTHER_RESOLVER, new_resolver_id, served[2], "other.eth"])
    );
    database.cleanup().await
}
