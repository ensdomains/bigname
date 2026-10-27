//! A factory origin is supported exactly for the factory and implementations the active
//! migration manifest declares, each from at or before the factory log.
use super::*;

#[test]
fn the_role_names_are_the_manifest_crates() {
    assert_eq!(
        WRAPPER_ROLE,
        bigname_manifests::WRAPPER_REGISTRY_IMPLEMENTATION_ROLE
    );
    assert_eq!(
        USER_ROLE,
        bigname_manifests::USER_REGISTRY_IMPLEMENTATION_ROLE
    );
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

/// One origin case: the migration manifest's epoch and declared contracts, the origin's factory
/// and implementation, an optional later upgrade, and the code model expected.
struct OriginCase {
    case: &'static str,
    chain: &'static str,
    epoch: &'static str,
    contracts: Value,
    factory: &'static str,
    implementation: &'static str,
    upgrade: Option<&'static str>,
    expected: Option<Model>,
}

/// The origin's block. Declarations starting after it do not hold it.
const ORIGIN_BLOCK: i64 = 10;

fn declared_at(role: &str, address: &str, start: i64) -> Value {
    json!({"role": role, "address": address, "proxy_kind": "none", "start_block": start})
}

fn origin_cases() -> Vec<OriginCase> {
    let factory = || contract("verifiable_factory", MAINNET_FACTORY);
    let current = || {
        json!([
            factory(),
            contract(WRAPPER_ROLE, MAINNET_WRAPPER),
            contract(USER_ROLE, MAINNET_USER),
        ])
    };
    let two_pairs = || {
        json!([
            factory(),
            contract(WRAPPER_ROLE, MAINNET_WRAPPER),
            contract(USER_ROLE, MAINNET_USER),
            contract(WRAPPER_ROLE, OLD_WRAPPER),
            contract(USER_ROLE, OLD_USER),
        ])
    };
    let mainnet = |case, contracts, implementation, upgrade, expected| OriginCase {
        case,
        chain: MAINNET,
        epoch: MAINNET_EPOCH,
        contracts,
        factory: MAINNET_FACTORY,
        implementation,
        upgrade,
        expected,
    };
    vec![
        // Row 1: the declared pair is the set.
        mainnet(
            "current WrapperRegistry",
            current(),
            MAINNET_WRAPPER,
            None,
            Some(Model::Wrapper),
        ),
        mainnet(
            "current UserRegistry",
            current(),
            MAINNET_USER,
            None,
            Some(Model::User),
        ),
        // Row 2: a further declared pair joins it.
        mainnet(
            "two pairs, current wrapper",
            two_pairs(),
            MAINNET_WRAPPER,
            None,
            Some(Model::Wrapper),
        ),
        mainnet(
            "two pairs, old wrapper",
            two_pairs(),
            OLD_WRAPPER,
            None,
            Some(Model::Wrapper),
        ),
        mainnet(
            "two pairs, old user",
            two_pairs(),
            OLD_USER,
            None,
            Some(Model::User),
        ),
        // Row 3: an implementation the manifest does not declare.
        mainnet(
            "an archived implementation",
            current(),
            ARCHIVED_WRAPPER,
            None,
            None,
        ),
        mainnet(
            "the old wrapper, undeclared",
            current(),
            OLD_WRAPPER,
            None,
            None,
        ),
        // Row 4: a declaration starting after the origin.
        mainnet(
            "implementation declared from the origin block",
            json!([
                factory(),
                declared_at(WRAPPER_ROLE, MAINNET_WRAPPER, ORIGIN_BLOCK)
            ]),
            MAINNET_WRAPPER,
            None,
            Some(Model::Wrapper),
        ),
        mainnet(
            "implementation declared after the origin block",
            json!([
                factory(),
                declared_at(WRAPPER_ROLE, MAINNET_WRAPPER, ORIGIN_BLOCK + 1)
            ]),
            MAINNET_WRAPPER,
            None,
            None,
        ),
        // Row 6: the factory.
        mainnet(
            "factory declared after the origin block",
            json!([
                declared_at("verifiable_factory", MAINNET_FACTORY, ORIGIN_BLOCK + 1),
                contract(WRAPPER_ROLE, MAINNET_WRAPPER),
            ]),
            MAINNET_WRAPPER,
            None,
            None,
        ),
        mainnet(
            "factory undeclared",
            json!([contract(WRAPPER_ROLE, MAINNET_WRAPPER)]),
            MAINNET_WRAPPER,
            None,
            None,
        ),
        OriginCase {
            factory: SEPOLIA_FACTORY,
            ..mainnet(
                "another emitter than the declared factory",
                current(),
                MAINNET_WRAPPER,
                None,
                None,
            )
        },
        // Row 7: the role decides the model.
        mainnet(
            "the wrapper address declared only as a user implementation",
            json!([factory(), contract(USER_ROLE, MAINNET_WRAPPER)]),
            MAINNET_WRAPPER,
            None,
            Some(Model::User),
        ),
        mainnet(
            "one address declared under both roles",
            json!([
                factory(),
                contract(WRAPPER_ROLE, MAINNET_WRAPPER),
                contract(USER_ROLE, MAINNET_WRAPPER)
            ]),
            MAINNET_WRAPPER,
            None,
            None,
        ),
        mainnet(
            "an address declared only under another role",
            json!([factory(), contract("migration_helper", MAINNET_WRAPPER)]),
            MAINNET_WRAPPER,
            None,
            None,
        ),
        // Row 13: upgrades. An upgrade to any other implementation is a departure, even one
        // the manifest declares, under the other role or under the same one.
        mainnet(
            "a same-code upgrade",
            current(),
            MAINNET_WRAPPER,
            Some(MAINNET_WRAPPER),
            Some(Model::Wrapper),
        ),
        mainnet(
            "a departure to an undeclared implementation",
            current(),
            MAINNET_WRAPPER,
            Some(ARCHIVED_WRAPPER),
            None,
        ),
        mainnet(
            "a departure to the other declared implementation",
            current(),
            MAINNET_WRAPPER,
            Some(MAINNET_USER),
            None,
        ),
        mainnet(
            "a departure within one role",
            two_pairs(),
            OLD_WRAPPER,
            Some(MAINNET_WRAPPER),
            None,
        ),
        // Row 16: the epoch gate.
        OriginCase {
            epoch: SEPOLIA_EPOCH,
            ..mainnet(
                "mainnet manifests under the Sepolia epoch",
                current(),
                MAINNET_WRAPPER,
                None,
                None,
            )
        },
        OriginCase {
            epoch: "ens_v2_mainnet_unreviewed",
            ..mainnet(
                "an unreviewed epoch",
                current(),
                MAINNET_WRAPPER,
                None,
                None,
            )
        },
        // Row 17: Sepolia reads its own declarations.
        OriginCase {
            case: "Sepolia WrapperRegistry",
            chain: SEPOLIA,
            epoch: SEPOLIA_EPOCH,
            contracts: json!([
                contract("verifiable_factory", SEPOLIA_FACTORY),
                contract(WRAPPER_ROLE, SEPOLIA_WRAPPER),
            ]),
            factory: SEPOLIA_FACTORY,
            implementation: SEPOLIA_WRAPPER,
            upgrade: None,
            expected: Some(Model::Wrapper),
        },
    ]
}

/// The invariant: a migration-created registry has the WrapperRegistry or UserRegistry model
/// exactly when the active migration manifest under a reviewed epoch declares its factory and
/// its exact implementation under that role, each from at or before the factory log.
#[tokio::test]
async fn a_factory_origin_is_supported_exactly_when_its_implementation_is_declared() -> Result<()> {
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
        let mut failures = Vec::new();
        for (index, case) in origin_cases().into_iter().enumerate() {
            let registry = format!("0x{:040x}", 0xa00 + index);
            let instance = Uuid::from_u128(0xa00 + index as u128);
            let (migration, announcing) = ids(case.chain);
            let declarations = Declarations(vec![
                declaration(
                    migration,
                    "ens_v2_migration_l1",
                    case.chain,
                    case.epoch,
                    case.contracts,
                ),
                declaration(
                    announcing,
                    "ens_v2_registry_l1",
                    case.chain,
                    case.epoch,
                    json!([]),
                ),
            ]);
            fact(
                connection,
                case.chain,
                migration,
                (
                    "ContractDiscovered",
                    "ens_v2_migration_l1",
                    "ens_v2_migration",
                ),
                ORIGIN_BLOCK,
                json!({"source_event": "ContractDiscovered", "proxy_address": registry,
                    "implementation": case.implementation}),
                case.factory,
            )
            .await?;
            fact(
                connection,
                case.chain,
                announcing,
                (
                    "RegistryCreated",
                    "ens_v2_registry_l1",
                    "ens_v2_registry_resource_surface",
                ),
                ORIGIN_BLOCK + 1,
                json!({"source_event": "RegistryCreated", "registry": registry,
                    "contract_instance_id": instance.to_string()}),
                &registry,
            )
            .await?;
            if let Some(upgrade) = case.upgrade {
                fact(
                    connection,
                    case.chain,
                    announcing,
                    ("Upgraded", "ens_v2_registry_l1", "proxy_upgrade"),
                    ORIGIN_BLOCK + 2,
                    json!({"proxy_address": registry, "implementation": upgrade}),
                    &registry,
                )
                .await?;
            }
            let model = load(
                connection,
                &publication(case.chain)?,
                &declarations,
                &registry,
                Some(instance),
            )
            .await?
            .map(|supported| supported.model);
            if model != case.expected {
                failures.push(format!("{}: {model:?}, not {:?}", case.case, case.expected));
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    })
    .await
}
