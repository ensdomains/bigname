//! A live reservation resolves ENSv1 names until its expiry or explicit unregister.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L48-L71 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20261001@07e55a05)
use super::*;
use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};

const PROXY: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const IMPLEMENTATION: &str = "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3";
const ROOT: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
const REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const MIRROR: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
const BATCH_REGISTRAR: &str = "0x4a4c8b7cdab6b19dc2cdb417cdb53a2ccbaf5322";
const RESOLVER: &str = "0x1000000000000000000000000000000000000001";
const START: i64 = 1_800_000_000;
sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
}

fn at(block: i64) -> String {
    format!("0xrelease{block}")
}
fn time_text(block: i64) -> String {
    OffsetDateTime::from_unix_timestamp(START + block)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}
fn token(label: &str) -> U256 {
    let mut bytes = *keccak256(label);
    bytes[28..].fill(0);
    U256::from_be_bytes(bytes)
}

/// Admitted root/ETH path and mirrored reservations, followed by the adapter's
/// block-boundary expiry or LabelUnregistered. The second reservation stays live.
/// This scales the October 1 deployment to local block 200 on two chains for isolation;
/// it does not claim that Mainnet has this deployment or these deployment starts.
fn producer(
    chain: &str,
    explicit: bool,
) -> Result<(Vec<ManifestInput>, bigname_adapters::schema_v2::BatchOutput)> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let mut manifests = Vec::new();
    let mut discovery_rules = Vec::new();
    for (id, family) in [
        (988100, "ens_v2_registry_l1"),
        (988101, "ens_v2_root_l1"),
        (988102, "ens_v2_resolver_l1"),
    ] {
        let mut manifest = repository
            .manifests()
            .iter()
            .find(|m| m.manifest.source_family == family)
            .unwrap()
            .manifest
            .clone();
        manifest.chain = chain.into();
        for root in &mut manifest.roots {
            root.start_block = Some(0);
        }
        for contract in &mut manifest.contracts {
            contract.start_block = Some(0);
        }
        // This direct adapter batch has only declared registry emitters, without the
        // factory discovery that fills dynamic emitter roles in the full runner.
        manifest.abi.events.retain(|event| {
            matches!(
                event.name.as_str(),
                "LabelRegistered"
                    | "LabelReserved"
                    | "LabelUnregistered"
                    | "TokenResource"
                    | "TransferSingle"
                    | "SubregistryUpdated"
                    | "ResolverUpdated"
            )
        });
        discovery_rules.extend(
            manifest
                .discovery_rules
                .iter()
                .map(|rule| DiscoveryRuleInput {
                    manifest_id: id,
                    edge_kind: rule.edge_kind.clone(),
                    from_role: Some(rule.from_role.clone()),
                    admission: rule.admission.clone(),
                }),
        );
        manifests.push(ManifestInput {
            manifest_id: id,
            manifest_version: manifest.manifest_version as i64,
            namespace: manifest.namespace.clone(),
            source_family: family.into(),
            chain_id: chain.into(),
            deployment_label: manifest.deployment_epoch.clone(),
            normalizer_version: manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(&manifest)?,
        });
    }
    let raw = |data: alloy_primitives::LogData, emitter: &str, block, tx, index| RawLogInput {
        chain_id: chain.into(),
        block_hash: at(block),
        block_number: block,
        block_timestamp: timestamp(START + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:062x}{tx:02x}"),
        transaction_index: tx,
        log_index: index,
        emitting_address: emitter.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    };
    let owner = FAMILY_ALICE.parse()?;
    let eth = token("eth");
    let mut logs = vec![
        raw(
            LabelRegistered {
                tokenId: eth,
                labelHash: keccak256("eth"),
                label: "eth".into(),
                owner,
                expiry: u64::MAX,
                sender: owner,
            }
            .encode_log_data(),
            ROOT,
            200,
            0,
            0,
        ),
        raw(
            TransferSingle {
                operator: owner,
                from: Address::ZERO,
                to: owner,
                id: eth,
                value: U256::from(1),
            }
            .encode_log_data(),
            ROOT,
            200,
            0,
            1,
        ),
        raw(
            TokenResource {
                tokenId: eth,
                resource: eth,
            }
            .encode_log_data(),
            ROOT,
            200,
            0,
            2,
        ),
        raw(
            SubregistryUpdated {
                tokenId: eth,
                subregistry: REGISTRY.parse()?,
                sender: owner,
            }
            .encode_log_data(),
            ROOT,
            200,
            0,
            3,
        ),
    ];
    for (i, label) in ["ledger", "live"].into_iter().enumerate() {
        logs.push(raw(
            LabelReserved {
                tokenId: token(label),
                labelHash: keccak256(label),
                label: label.into(),
                expiry: (START
                    + if label == "ledger" && !explicit {
                        203
                    } else {
                        1000
                    }) as u64,
                sender: BATCH_REGISTRAR.parse()?,
            }
            .encode_log_data(),
            REGISTRY,
            200,
            1,
            4 + i as i64 * 2,
        ));
        logs.push(raw(
            ResolverUpdated {
                tokenId: token(label),
                resolver: MIRROR.parse()?,
                sender: BATCH_REGISTRAR.parse()?,
            }
            .encode_log_data(),
            REGISTRY,
            200,
            1,
            5 + i as i64 * 2,
        ));
    }
    if explicit {
        logs.push(raw(
            LabelUnregistered {
                tokenId: token("ledger"),
                sender: FAMILY_ALICE.parse()?,
            }
            .encode_log_data(),
            REGISTRY,
            203,
            0,
            0,
        ));
    }
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: chain.into(),
            manifests: manifests.clone(),
            discovery_rules,
            admissions: [
                (REGISTRY, 988100, "registry"),
                (ROOT, 988101, "root_registry"),
                (MIRROR, 988102, "ensv1_mirror_resolver"),
            ]
            .into_iter()
            .map(|(address, id, role)| AddressAdmissionInput {
                address: address.into(),
                contract_instance_id: Uuid::from_u128(id as u128),
                source_manifest_id: Some(id),
                role: Some(role.into()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            })
            .collect(),
            prior_events: vec![],
            blocks: (200..=203)
                .map(|n| RawBlockInput {
                    chain_id: chain.into(),
                    block_hash: at(n),
                    block_number: n,
                    block_timestamp: timestamp(START + n),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs: logs,
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    let release = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "RegistrationReleased" && e.block_number == Some(203))
        .context("real release")?;
    assert_eq!(
        release.logical_name_id.as_deref(),
        Some(format!("ens:{}", bigname_lookup::ens_namehash_hex("ledger.eth")?).as_str())
    );
    assert_eq!(
        release.after_state["source_event"],
        if explicit {
            "LabelUnregistered"
        } else {
            "RegistryPathExpired"
        }
    );
    Ok((manifests, output))
}

async fn child(
    database: &TestDatabase,
    chain: &str,
    name: &str,
    seed: u128,
    manifest: i64,
) -> Result<String> {
    let resource = Uuid::from_u128(seed);
    let id = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        chain,
        200,
        &at(200),
        resource,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v1",
    )
    .await?;
    let node = id.trim_start_matches("ens:");
    let mut events = Vec::new();
    // ENSv1 registry-only subname authority, not a synthetic registrar lease for a child.
    for (index, kind, family, after) in [
        (
            10,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            json!({"authority_kind":"registry", "owner":FAMILY_ALICE,"registry_owner":FAMILY_ALICE,"node":node}),
        ),
        (
            11,
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"node":node,"resolver":RESOLVER}),
        ),
        (
            12,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({"source_event":"AddressChanged","node":node,"resolver":RESOLVER,"record_key":"addr:60","record_family":"addr","selector_key":"60","value":FAMILY_ALICE}),
        ),
    ] {
        let mut e = history_event(
            &format!("{name}-{kind}"),
            Some(&id),
            Some(resource),
            Some(chain),
            Some(200),
            Some(&at(200)),
            Some("0xchild"),
            Some(index),
            CanonicalityState::Canonical,
        );
        e.event_kind = kind.into();
        e.source_family = family.into();
        e.after_state = after;
        e.manifest_version = 1;
        e.source_manifest_id = (kind == "RecordChanged").then_some(manifest);
        e.raw_fact_ref = json!({"emitting_address":RESOLVER,"transaction_index":0});
        events.push(e);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(id)
}

#[tokio::test]
async fn parent_release_retires_only_affected_descendant_evidence() -> Result<()> {
    for chain in ["ethereum-mainnet", "ethereum-sepolia"] {
        for explicit in [false, true] {
            let database = TestDatabase::new_migrated().await?;
            for n in 200..=203 {
                seed_schema_v2_lookup_head(&database.pool, chain, n, &at(n), &time_text(n)).await?;
            }
            // Lookup starts at 200 even though all canonical inputs are retained for the later publish.
            seed_schema_v2_lookup_head(&database.pool, chain, 200, &at(200), &time_text(200))
                .await?;
            seed_schema_v2_ens_manifest_on_chain(
                &database.pool,
                chain,
                "ens_execution",
                "universal_resolver",
                PROXY,
                Uuid::from_u128(988001),
                true,
            )
            .await?;
            let (manifest, mut payload): (i64, Value) = sqlx::query_as("SELECT manifest_id, manifest_payload FROM manifest_versions WHERE source_family='ens_execution' AND chain_id=$1").bind(chain).fetch_one(&database.pool).await?;
            payload["universal_resolver_implementations"] = json!([IMPLEMENTATION]);
            payload["capability_flags"]["verified_resolution"]["status"] = json!("shadow");
            sqlx::query("UPDATE manifest_versions SET manifest_payload=$2 WHERE manifest_id=$1")
                .bind(manifest)
                .bind(&payload)
                .execute(&database.pool)
                .await?;
            seed_fixture_manifest_update(
                &database.pool,
                manifest,
                chain,
                "ens",
                "ens_execution",
                &payload,
            )
            .await?;
            let (manifests, output) = producer(chain, explicit)?;
            super::v2_history_bounded_rebinding::persist_with_manifests(
                &database.pool,
                &manifests,
                &output,
            )
            .await?;
            for declaration in &manifests {
                seed_fixture_manifest_update(
                    &database.pool,
                    declaration.manifest_id,
                    chain,
                    &declaration.namespace,
                    &declaration.source_family,
                    &serde_json::from_str(&declaration.payload_json)?,
                )
                .await?;
            }
            let resolver_manifest = declare_family_fixture_resolver(
                &database.pool,
                "ens",
                chain,
                "ens_v1_resolver_l1",
                RESOLVER,
            )
            .await?;
            let target = child(
                &database,
                chain,
                "sub.ledger.eth",
                988200,
                resolver_manifest,
            )
            .await?;
            let live = child(&database, chain, "sub.live.eth", 988300, resolver_manifest).await?;
            let mut upgrade = history_event(
                "release-cutover",
                None,
                None,
                Some(chain),
                Some(200),
                Some(&at(200)),
                Some("0xupgrade"),
                Some(20),
                CanonicalityState::Canonical,
            );
            upgrade.event_kind = "Upgraded".into();
            upgrade.source_family = "ens_execution".into();
            upgrade.after_state = json!({"proxy_address":PROXY,"implementation":IMPLEMENTATION});
            bigname_storage::insert_normalized_event_fixtures(&database.pool, &[upgrade]).await?;
            publish_test_families_on(&database.pool, chain, 200).await?;
            for id in [&target, &live] {
                let row = bigname_storage::families::name::load_family_name(&database.pool, id)
                    .await?
                    .context("child before release")?;
                assert_eq!(row.declared_summary["resolver"]["address"], RESOLVER);
                assert_eq!(row.unresolvable_reason(), None);
                assert_eq!(row.resolution_unsupported_reason(), None);
                let inventory = bigname_storage::families::records::load_family_record_inventory(
                    &database.pool,
                    chain,
                    row.record_serving_resource_id()
                        .context("retained child resource")?,
                )
                .await?
                .context("indexed child records before divergence")?;
                let address = inventory
                    .entries
                    .as_array()
                    .context("inventory entries")?
                    .iter()
                    .find(|entry| entry["record_key"] == "addr:60")
                    .context("indexed child address")?;
                assert_eq!(address["status"], "success");
                assert_eq!(address["value"], FAMILY_ALICE);
            }
            assert_eq!(
                cutover_lookup(&database, chain, &target, FAMILY_BOB).await?,
                bigname_lookup::LedgerAction::Written
            );
            assert_eq!(
                cutover_lookup(&database, chain, &target, FAMILY_ALICE).await?,
                bigname_lookup::LedgerAction::Cleared
            );
            seed_schema_v2_lookup_head(&database.pool, chain, 202, &at(202), &time_text(202))
                .await?;
            publish_test_families_on(&database.pool, chain, 202).await?;
            assert_eq!(
                cutover_lookup(&database, chain, &target, FAMILY_BOB).await?,
                bigname_lookup::LedgerAction::Written
            );
            assert_eq!(
                cutover_lookup(&database, chain, &live, FAMILY_BOB).await?,
                bigname_lookup::LedgerAction::Written
            );
            let other = if chain == "ethereum-mainnet" {
                "ethereum-sepolia"
            } else {
                "ethereum-mainnet"
            };
            seed_schema_v2_lookup_head(&database.pool, other, 202, &at(202), &time_text(202))
                .await?;
            sqlx::query("INSERT INTO resolution_divergences (logical_name_id,resolver_chain_id,resolver_address,request_kind,observed_positions,indexed_result,live_result,first_observed_at,last_observed_at) SELECT logical_name_id,$1,resolver_address,request_kind,
            jsonb_build_object(CASE WHEN $1='ethereum-mainnet' THEN 'ethereum' ELSE 'ethereum-sepolia' END,
                jsonb_build_object('chain_id',$1::text,'block_number',202,'block_hash',$4::text,'timestamp',$5::text)),
            indexed_result,live_result,first_observed_at,last_observed_at FROM resolution_divergences WHERE logical_name_id=$2 AND resolver_chain_id=$3 AND cleared_at IS NULL")
            .bind(other).bind(&target).bind(chain).bind(at(202)).bind(time_text(202)).execute(&database.pool).await?;
            let rows = "SELECT to_jsonb(d) FROM resolution_divergences d ORDER BY logical_name_id,resolver_chain_id,observed_positions::text";
            let before: Vec<Value> = sqlx::query_scalar(rows).fetch_all(&database.pool).await?;
            seed_schema_v2_lookup_head(&database.pool, chain, 203, &at(203), &time_text(203))
                .await?;
            sqlx::raw_sql("CREATE FUNCTION refuse_release() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.current_block_number=203 THEN RAISE EXCEPTION 'publication refused'; END IF; RETURN NEW; END $$; CREATE TRIGGER refuse_release BEFORE UPDATE ON project_family_marker FOR EACH ROW EXECUTE FUNCTION refuse_release();").execute(&database.pool).await?;
            assert!(
                publish_test_families_on(&database.pool, chain, 203)
                    .await
                    .is_err()
            );
            assert_eq!(
                sqlx::query_scalar::<_, Value>(rows)
                    .fetch_all(&database.pool)
                    .await?,
                before
            );
            sqlx::raw_sql("DROP TRIGGER refuse_release ON project_family_marker")
                .execute(&database.pool)
                .await?;
            publish_test_families_on(&database.pool, chain, 203).await?;
            let row = bigname_storage::families::name::load_family_name(&database.pool, &target)
                .await?
                .context("child")?;
            assert_eq!(
                row.declared_summary["unresolvable_reason"],
                "no_live_ens_v2_entry"
            );
            let after: Vec<Value> = sqlx::query_scalar(rows).fetch_all(&database.pool).await?;
            assert_eq!(before.len(), after.len());
            for (previous, mut current) in before.into_iter().zip(after.clone()) {
                if previous["logical_name_id"] == target
                    && previous["resolver_chain_id"] == chain
                    && previous["cleared_at"].is_null()
                {
                    assert!(
                        current["cleared_at"].is_string(),
                        "parent release left evidence active: {current}"
                    );
                    current["cleared_at"] = Value::Null;
                }
                assert_eq!(
                    current, previous,
                    "history, live-parent and other-chain observations are unchanged"
                );
            }
            for mode in [
                bigname_project::families::FamilyMode::Redo { from: 203, to: 203 },
                bigname_project::families::FamilyMode::Rebuild,
            ] {
                let token = bigname_project::families::input_token(&database.pool, chain).await?;
                let outcome = bigname_project::families::apply(
                    &database.pool,
                    chain,
                    &bigname_project::Marker {
                        number: 203,
                        hash: at(203),
                    },
                    mode,
                    &token,
                    &bigname_project::families::FamilyOptions::new(
                        bigname_content_hash::INTERPRETER_CONTENT_HASH,
                    ),
                )
                .await?;
                assert_eq!(outcome.marker.map(|m| m.number), Some(203));
                assert_eq!(
                    sqlx::query_scalar::<_, Value>(rows)
                        .fetch_all(&database.pool)
                        .await?,
                    after,
                    "replay preserves durable observations and never reactivates retired evidence"
                );
            }
            database.cleanup().await?;
        }
    }
    Ok(())
}
