//! The reviewed deployment rows: a declaration is read only under its own chain's epoch, a
//! factory origin is supported only for its own deployment's factory and implementations, and
//! the departure probe of each implementation reads its own sparse index.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgConnection, raw_sql, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{DEPARTURES, Declaration, Declarations, Model, load};
use crate::families::{
    id_index_plan_tests::{PLAN_MODES, Probe, explain_execute, missing_probes, with_database},
    name::FamilyPublication,
};

const SEPOLIA: &str = "ethereum-sepolia";
const MAINNET: &str = "ethereum-mainnet";
const SEPOLIA_EPOCH: &str = "ens_v2_sepolia_20261001";
const MAINNET_EPOCH: &str = "ens_v2_mainnet_20261002";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/VerifiableFactory.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_FACTORY: &str = "0x77510ba162dfea3d8989e3f0928a691b345730d2";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_WRAPPER: &str = "0xcf3edb3f5501de63b57254d910b6782ee370039b";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_USER: &str = "0x75ff9adbcfce2469b097ac68f6ae650eae1351bb";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_ETH: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/RootRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_ROOT: &str = "0x2553c09be6447beffdb86fa622ad41fb8f77a91e";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ENSV1Resolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET_MIRROR: &str = "0x394d8eb4a4fdf599384deab25f31d111d755c398";
const SEPOLIA_FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
const SEPOLIA_WRAPPER: &str = "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880";
const SEPOLIA_ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const SEPOLIA_ROOT: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
const SEPOLIA_MIRROR: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
/// An archived mainnet WrapperRegistry implementation, outside the reviewed row.
const ARCHIVED_WRAPPER: &str = "0x93b83f7861306dffa2176fd534f9729aa38e89e1";
const OTHER_REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const BLOCK: i64 = 100;

fn contract(role: &str, address: &str) -> Value {
    json!({"role": role, "address": address, "proxy_kind": "none", "start_block": 0})
}

/// The retained declaration of one active manifest, as `Declarations::load` reads it.
fn declaration(id: i64, family: &str, chain: &str, epoch: &str, contracts: Value) -> Declaration {
    Declaration {
        source_manifest_id: id,
        manifest_version: 1,
        namespace: "ens".into(),
        source_family: family.into(),
        after_state: json!({
            "rollout_status": "active",
            "manifest_payload": {
                "manifest_version": 1, "rollout_status": "active", "chain": chain,
                "namespace": "ens", "source_family": family, "deployment_epoch": epoch,
                "contracts": contracts,
            },
        }),
    }
}

fn publication(chain: &str) -> Result<FamilyPublication> {
    Ok(FamilyPublication {
        chain_id: chain.into(),
        block_number: BLOCK,
        block_hash: format!("block-{BLOCK}"),
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
        block_timestamp_json: json!("2027-01-15T08:00:00Z"),
        admission: None,
        admission_manifests: None,
    })
}

#[test]
fn a_registry_or_root_declaration_is_read_under_its_own_chains_epoch() -> Result<()> {
    let instance = Uuid::from_u128(7);
    // (case, publication chain, manifest chain, epoch, family, role, declared, read, supported)
    let cases = [
        (
            "mainnet ETHRegistry",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            MAINNET_ETH,
            true,
        ),
        (
            "mainnet RootRegistry",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_root_l1",
            "root_registry",
            MAINNET_ROOT,
            MAINNET_ROOT,
            true,
        ),
        (
            "Sepolia ETHRegistry",
            SEPOLIA,
            SEPOLIA,
            SEPOLIA_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            SEPOLIA_ETH,
            SEPOLIA_ETH,
            true,
        ),
        (
            "Sepolia RootRegistry",
            SEPOLIA,
            SEPOLIA,
            SEPOLIA_EPOCH,
            "ens_v2_root_l1",
            "root_registry",
            SEPOLIA_ROOT,
            SEPOLIA_ROOT,
            true,
        ),
        (
            "a registry manifest's own registry",
            SEPOLIA,
            SEPOLIA,
            SEPOLIA_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            OTHER_REGISTRY,
            OTHER_REGISTRY,
            true,
        ),
        (
            "a mainnet manifest under the Sepolia epoch",
            MAINNET,
            MAINNET,
            SEPOLIA_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            MAINNET_ETH,
            false,
        ),
        (
            "a Sepolia manifest under the mainnet epoch",
            SEPOLIA,
            SEPOLIA,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            SEPOLIA_ETH,
            SEPOLIA_ETH,
            false,
        ),
        (
            "a manifest of another chain",
            SEPOLIA,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            MAINNET_ETH,
            false,
        ),
        (
            "the root role in the registry family",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "root_registry",
            MAINNET_ETH,
            MAINNET_ETH,
            false,
        ),
        (
            "the registry role in the root family",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_root_l1",
            "registry",
            MAINNET_ROOT,
            MAINNET_ROOT,
            false,
        ),
        (
            "an undeclared registry",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_registry_l1",
            "registry",
            MAINNET_ETH,
            OTHER_REGISTRY,
            false,
        ),
        (
            "a declared factory",
            MAINNET,
            MAINNET,
            MAINNET_EPOCH,
            "ens_v2_migration_l1",
            "verifiable_factory",
            MAINNET_FACTORY,
            MAINNET_FACTORY,
            false,
        ),
    ];
    let mut failures = Vec::new();
    for (case, chain, manifest_chain, epoch, family, role, declared, read, supported) in cases {
        let declarations = Declarations(vec![declaration(
            1,
            family,
            manifest_chain,
            epoch,
            json!([contract(role, declared)]),
        )]);
        let publication = publication(chain)?;
        let model = declarations
            .declared(&publication, read, instance)
            .map(|registry| registry.model);
        if model != supported.then_some(Model::Declared) {
            failures.push(format!("{case}: {model:?}"));
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn the_mirror_is_the_one_its_deployments_resolver_manifest_declares() -> Result<()> {
    let mirror = |address: &str| contract("ensv1_mirror_resolver", address);
    let mut proxied = mirror(MAINNET_MIRROR);
    proxied["proxy_kind"] = json!("transparent");
    let mut later = mirror(MAINNET_MIRROR);
    later["start_block"] = json!(BLOCK + 1);
    let resolver = |chain: &str, epoch: &str, contracts: Value| {
        declaration(1, "ens_v2_resolver_l1", chain, epoch, contracts)
    };
    // (case, publication chain, declarations, mirror)
    let cases = [
        (
            "the mainnet mirror",
            MAINNET,
            vec![resolver(
                MAINNET,
                MAINNET_EPOCH,
                json!([mirror(MAINNET_MIRROR)]),
            )],
            Some(MAINNET_MIRROR),
        ),
        (
            "the Sepolia mirror",
            SEPOLIA,
            vec![resolver(
                SEPOLIA,
                SEPOLIA_EPOCH,
                json!([mirror(SEPOLIA_MIRROR)]),
            )],
            Some(SEPOLIA_MIRROR),
        ),
        ("no resolver manifest", MAINNET, Vec::new(), None),
        (
            "a mainnet mirror under the Sepolia epoch",
            MAINNET,
            vec![resolver(
                MAINNET,
                SEPOLIA_EPOCH,
                json!([mirror(MAINNET_MIRROR)]),
            )],
            None,
        ),
        (
            "another chain's mirror",
            MAINNET,
            vec![resolver(
                SEPOLIA,
                SEPOLIA_EPOCH,
                json!([mirror(SEPOLIA_MIRROR)]),
            )],
            None,
        ),
        (
            "a mirror role outside the resolver family",
            MAINNET,
            vec![declaration(
                1,
                "ens_v2_registry_l1",
                MAINNET,
                MAINNET_EPOCH,
                json!([mirror(MAINNET_MIRROR)]),
            )],
            None,
        ),
        (
            "two mirrors",
            MAINNET,
            vec![resolver(
                MAINNET,
                MAINNET_EPOCH,
                json!([mirror(MAINNET_MIRROR), mirror(SEPOLIA_MIRROR)]),
            )],
            None,
        ),
        (
            "a proxied mirror",
            MAINNET,
            vec![resolver(MAINNET, MAINNET_EPOCH, json!([proxied]))],
            None,
        ),
        (
            "a mirror started after the publication",
            MAINNET,
            vec![resolver(MAINNET, MAINNET_EPOCH, json!([later]))],
            None,
        ),
    ];
    let mut failures = Vec::new();
    for (case, chain, declarations, expected) in cases {
        let declared = Declarations(declarations).mirror_resolver(&publication(chain)?);
        if declared.as_deref() != expected {
            failures.push(format!("{case}: {declared:?}"));
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// One manifest catalog row, which the retained facts reference.
async fn manifest(connection: &mut PgConnection, chain: &str, family: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', $1, $2, 'fixture', 'shadow', 'fixture', $2 || '/' || $1, '{}')
         RETURNING manifest_id",
    )
    .bind(family)
    .bind(chain)
    .fetch_one(&mut *connection)
    .await?)
}

/// A canonical fact of `registry` at `block`, under the chain's manifest of its family.
async fn fact(
    connection: &mut PgConnection,
    chain: &str,
    manifest: i64,
    (kind, family, derivation): (&str, &str, &str),
    block: i64,
    after: Value,
    emitter: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state, raw_fact_ref)
         VALUES (gen_random_uuid()::text, 'ens', $1, $2, 1, $3, $4, $5, $9,
             $6, 'canonical', $7, jsonb_build_object('emitting_address', $8::text))",
    )
    .bind(kind)
    .bind(family)
    .bind(manifest)
    .bind(chain)
    .bind(block)
    .bind(derivation)
    .bind(after)
    .bind(emitter)
    .bind(format!("block-{block}"))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

#[tokio::test]
async fn a_factory_origin_is_supported_only_for_its_own_deployments_row() -> Result<()> {
    with_database("registry_support_rows", async |connection| {
        let mut manifests = Vec::new();
        for chain in [SEPOLIA, MAINNET] {
            raw_sql(&format!(
                "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                     canonicality_state)
                 SELECT '{chain}', 'block-' || n, n, to_timestamp(n), 'canonical'
                 FROM generate_series(1, {BLOCK}) n"
            ))
            .execute(&mut *connection)
            .await?;
            manifests.push((
                chain,
                manifest(connection, chain, "ens_v2_migration_l1").await?,
                manifest(connection, chain, "ens_v2_registry_l1").await?,
            ));
        }
        let ids = |chain: &str| {
            manifests
                .iter()
                .find(|(manifest_chain, ..)| *manifest_chain == chain)
                .map(|(_, migration, registry)| (*migration, *registry))
                .expect("the chain's manifests")
        };
        // (case, chain, epoch, factory, implementation, upgrade to, model)
        let cases = [
            (
                "mainnet WrapperRegistry",
                MAINNET,
                MAINNET_EPOCH,
                MAINNET_FACTORY,
                MAINNET_WRAPPER,
                None,
                Some(Model::Wrapper),
            ),
            (
                "mainnet UserRegistry",
                MAINNET,
                MAINNET_EPOCH,
                MAINNET_FACTORY,
                MAINNET_USER,
                None,
                Some(Model::User),
            ),
            (
                "a same-code mainnet upgrade",
                MAINNET,
                MAINNET_EPOCH,
                MAINNET_FACTORY,
                MAINNET_WRAPPER,
                Some(MAINNET_WRAPPER),
                Some(Model::Wrapper),
            ),
            (
                "a mainnet departure",
                MAINNET,
                MAINNET_EPOCH,
                MAINNET_FACTORY,
                MAINNET_WRAPPER,
                Some(MAINNET_USER),
                None,
            ),
            (
                "an archived mainnet implementation",
                MAINNET,
                MAINNET_EPOCH,
                MAINNET_FACTORY,
                ARCHIVED_WRAPPER,
                None,
                None,
            ),
            (
                "the Sepolia factory on mainnet",
                MAINNET,
                MAINNET_EPOCH,
                SEPOLIA_FACTORY,
                MAINNET_WRAPPER,
                None,
                None,
            ),
            (
                "the Sepolia implementation on mainnet",
                MAINNET,
                MAINNET_EPOCH,
                MAINNET_FACTORY,
                SEPOLIA_WRAPPER,
                None,
                None,
            ),
            (
                "mainnet manifests under the Sepolia epoch",
                MAINNET,
                SEPOLIA_EPOCH,
                MAINNET_FACTORY,
                MAINNET_WRAPPER,
                None,
                None,
            ),
            (
                "Sepolia WrapperRegistry",
                SEPOLIA,
                SEPOLIA_EPOCH,
                SEPOLIA_FACTORY,
                SEPOLIA_WRAPPER,
                None,
                Some(Model::Wrapper),
            ),
            (
                "the mainnet row on Sepolia",
                SEPOLIA,
                SEPOLIA_EPOCH,
                MAINNET_FACTORY,
                MAINNET_WRAPPER,
                None,
                None,
            ),
        ];
        let mut failures = Vec::new();
        for (index, (case, chain, epoch, factory, implementation, upgrade, expected)) in
            cases.into_iter().enumerate()
        {
            let registry = format!("0x{:040x}", 0xa00 + index);
            let instance = Uuid::from_u128(0xa00 + index as u128);
            let (migration, announcing) = ids(chain);
            // The manifests declare exactly what the origin names, so only the reviewed row
            // can refuse it.
            let declarations = Declarations(vec![
                declaration(
                    migration,
                    "ens_v2_migration_l1",
                    chain,
                    epoch,
                    json!([
                        contract("verifiable_factory", factory),
                        contract("wrapper_registry_implementation", implementation),
                        contract("user_registry_implementation", implementation),
                    ]),
                ),
                declaration(announcing, "ens_v2_registry_l1", chain, epoch, json!([])),
            ]);
            fact(
                connection,
                chain,
                migration,
                (
                    "ContractDiscovered",
                    "ens_v2_migration_l1",
                    "ens_v2_migration",
                ),
                10,
                json!({"source_event": "ContractDiscovered", "proxy_address": registry,
                    "implementation": implementation}),
                factory,
            )
            .await?;
            fact(
                connection,
                chain,
                announcing,
                (
                    "RegistryCreated",
                    "ens_v2_registry_l1",
                    "ens_v2_registry_resource_surface",
                ),
                11,
                json!({"source_event": "RegistryCreated", "registry": registry,
                    "contract_instance_id": instance.to_string()}),
                &registry,
            )
            .await?;
            if let Some(upgrade) = upgrade {
                fact(
                    connection,
                    chain,
                    announcing,
                    ("Upgraded", "ens_v2_registry_l1", "proxy_upgrade"),
                    12,
                    json!({"proxy_address": registry, "implementation": upgrade}),
                    &registry,
                )
                .await?;
            }
            let publication = publication(chain)?;
            let model = load(
                connection,
                &publication,
                &declarations,
                &registry,
                Some(instance),
            )
            .await?
            .map(|supported| supported.model);
            if model != expected {
                failures.push(format!("{case}: {model:?}, not {expected:?}"));
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn each_implementations_departure_probe_reads_its_own_sparse_index() -> Result<()> {
    with_database("registry_support_departure_plan", async |connection| {
        // Twenty proxies with a thousand same-code upgrades each, so a plan that reads the
        // resolver upgrade history visits a thousand rows for a proxy the sparse index skips.
        raw_sql(&format!(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                 canonicality_state)
             SELECT '{MAINNET}', 'block-' || n, n, to_timestamp(n), 'canonical'
             FROM generate_series(1, 1000) n;

             INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash, derivation_kind,
                 canonicality_state, after_state)
             SELECT 'upgrade:' || proxy || ':' || n, 'ens', 'Upgraded', 'ens_v2_registry_l1', 1,
                    '{MAINNET}', n, 'block-' || n, 'proxy_upgrade', 'canonical',
                    jsonb_build_object('proxy_address', '0x' || lpad(to_hex(proxy), 40, '0'),
                        'implementation', CASE WHEN proxy % 2 = 1 THEN '{MAINNET_WRAPPER}'
                                               ELSE '{MAINNET_USER}' END)
             FROM generate_series(1, 20) proxy CROSS JOIN generate_series(1, 1000) n;

             ANALYZE normalized_events;"
        ))
        .execute(&mut *connection)
        .await?;
        let mut failures = Vec::new();
        for (statement, implementation, proxy, index) in [
            ("wrapper_departure", MAINNET_WRAPPER, 1, "normalized_events_mainnet_wrapper_departure_idx"),
            ("user_departure", MAINNET_USER, 2, "normalized_events_mainnet_user_registry_departure_idx"),
        ] {
            raw_sql(&format!(
                "PREPARE {statement} (text, text, bigint, bigint) AS {DEPARTURES}'{implementation}')"
            ))
            .execute(&mut *connection)
            .await?;
            let values = format!("'{MAINNET}', '0x{proxy:040x}', 1, 1000");
            let probes = [Probe {
                index,
                conditions: &["chain_id = ", "proxy_address"],
            }];
            for mode in PLAN_MODES {
                let plan = explain_execute(connection, mode, statement, &values).await?;
                failures.extend(missing_probes(&format!("{statement} {mode}"), &plan, &probes));
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        Ok(())
    })
    .await
}
