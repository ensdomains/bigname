use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::{
    StateCacheCapacity, prepare_schema_v2_batch_incremental,
    schema_v2::{
        inject_activated_transition_for_test,
        seam::{
            ARM_WIDE_BINDING_CLOSE_KEY, CLOSED_AUTHORITY_ARM_KEY, LOG_INDEX_KEY,
            PREIMAGE_OBSERVATION_EVENT_KIND, SURFACE_BINDING_ID_KEY, TRANSACTION_INDEX_KEY,
        },
    },
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, types::Uuid};

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const CHAIN: &str = "ethereum-sepolia";
const SETUP_BLOCK: i64 = 11_163_420;
const PREDECESSOR_BLOCK: i64 = SETUP_BLOCK + 1;
const MIGRATION_BLOCK: i64 = SETUP_BLOCK + 2;
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const UNLOCKED_CONTROLLER: &str = "0xd021a69db7f9e276a59cbbccf06e7f1e5434215c";
const LOCKED_CONTROLLER: &str = "0x681802eff57b83edce99d688c023ab1284495176";
const GRAVEYARD: &str = "0x6f4bf58ac55e0018589b2d9734ed8bb82740124d";
const ETH_REGISTRY: &str = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
const VERIFIABLE_FACTORY: &str = "0x118bc31a50d559f7015a8da26d54b3b030cdb70f";
const WRAPPER_REGISTRY_IMPLEMENTATION: &str = "0xcf9f4863a1b44216cfc0be65f4e47b2b9a043924";
const MIGRATION_REGISTRY: &str = "0x0000000000000000000000000000000000000771";
const OWNER: &str = "0x0000000000000000000000000000000000000051";

mod ens_registry {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(bytes32 indexed node, address owner);
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
    }
}

mod base_registrar {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }
}

sol! {
    event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
    event RegistryCreated();
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
}

#[path = "tests/unlocked_wrapped.rs"]
mod unlocked_wrapped;

#[path = "activation_tests/equivalence.rs"]
mod equivalence;

#[path = "activation_tests/alias_equivalence.rs"]
mod alias_equivalence;

/// Exercises the checked-in Sepolia manifests through the production adapter and transition
/// writer. Its BaseRegistrar address is pinned upstream here:
/// (upstream: .refs/ens_v1/deployments/sepolia/BaseRegistrarImplementation.json:L2 @ ens_v1@91c966f)
#[tokio::test]
async fn checked_in_sepolia_manifests_materialize_exactly_one_transition_predecessor() -> TestResult
{
    let database = database("interpret_sepolia_activation").await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;

    let label = b"activation-gate";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    seed_lineage(pool).await?;
    seed_predecessor_facts(pool, labelhash, namehash).await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: PREDECESSOR_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let logical_name_id = format!("ens:{namehash:#x}");
    let predecessor_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT surface_binding_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1' AND active_to IS NULL",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        predecessor_ids.len(),
        1,
        "the admitted BaseRegistrar facts must materialize one live ENSv1 predecessor"
    );

    seed_migration_facts(pool, label, labelhash).await?;
    let loaded = load::batch_input(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        MIGRATION_BLOCK,
        None,
        None,
        StateCacheCapacity::Entries(65_536),
    )
    .await?;
    let expected_orphaning_epoch = loaded.prior_cache.validated_orphaning_epoch;
    let prepared = prepare_schema_v2_batch_incremental(
        loaded.input,
        loaded.adapter_session,
        StateCacheCapacity::Entries(65_536),
    )?;
    let state_values = load::prior_state_values(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        prepared.state_value_requests(),
    )
    .await?;
    let (output, _) = prepared.finish(state_values)?;
    assert_eq!(output.migration_authority_transitions.len(), 1);
    assert_eq!(
        output.migration_authority_transitions[0].logical_name_id,
        logical_name_id
    );
    assert!(
        output.normalized_events.iter().all(|event| {
            ![
                ARM_WIDE_BINDING_CLOSE_KEY,
                CLOSED_AUTHORITY_ARM_KEY,
                SURFACE_BINDING_ID_KEY,
            ]
            .into_iter()
            .all(|key| event.after_state.get(key).is_some())
        }),
        "migration activation must not emit the complete arm-wide reassertion marker tuple"
    );

    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    write::batch(
        pool,
        CHAIN,
        None,
        false,
        true,
        expected_orphaning_epoch,
        &[(MIGRATION_BLOCK, block_hash(MIGRATION_BLOCK))],
        &output,
    )
    .await?;

    let closed_at: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE surface_binding_id = $1")
            .bind(predecessor_ids[0])
            .fetch_one(pool)
            .await?;
    assert_eq!(
        closed_at,
        Some(time::OffsetDateTime::from_unix_timestamp(MIGRATION_BLOCK)?),
        "the activated transition preserves the registrar cleanup-time close"
    );
    let successor_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v2' AND active_to IS NULL",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(successor_count, 1);

    // This reduced transition-writer fixture omits the ENSv1→ENSv2 migration transaction's
    // registry `NewOwner` reclaim, registry `Transfer` to the Graveyard, and
    // resolver-clear logs. It proves exactly-one predecessor materialization,
    // not a production publication path. The faithful path remains ignored
    // below until #<interpret-unwrapped-predecessor-issue> is resolved.

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "#<interpret-unwrapped-predecessor-issue>: activated migration boundary has 0 active ENSv1 predecessors matching its resource selector; expected exactly one"]
async fn faithful_unwrapped_migration_reaches_predecessor_refusal() -> TestResult {
    let database = database("interpret_faithful_unwrapped_predecessor").await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;

    let label = b"activation-gate";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    seed_lineage(pool).await?;
    seed_predecessor_facts(pool, labelhash, namehash).await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: PREDECESSOR_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let logical_name_id = format!("ens:{namehash:#x}");
    let predecessor_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1' AND active_to IS NULL",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(predecessor_count, 1);

    seed_faithful_unwrapped_migration(pool, label, labelhash, namehash).await?;
    let error = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: MIGRATION_BLOCK,
            to_block: MIGRATION_BLOCK,
            resume_current: Some(Marker {
                number: PREDECESSOR_BLOCK,
                hash: block_hash(PREDECESSOR_BLOCK),
            }),
            mode: RunMode::Normal,
        })
        .await
        .expect_err("the faithful unwrapped sequence currently reaches the known refusal");
    let message = error.to_string();
    assert!(
        message.contains(
            "has 0 active ENSv1 predecessors matching its resource selector; expected exactly one"
        ),
        "unexpected faithful-path failure: {message}"
    );
    database.cleanup().await?;
    Err(error.into())
}

#[tokio::test]
#[rustfmt::skip]
async fn cold_restore_retains_zero_clear_beside_later_state_tail() -> TestResult {
    let database = database("interpret_zero_clear_retention").await?;
    sync_schema_v2_repository(database.pool(), &load_repository(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"))?).await?;
    let resume_block = 11_163_500_i64;
    let token_id = format!("{:#066x}", U256::from(1));
    let state_key = format!("{ETH_REGISTRY}:-:{token_id}:-:SubregistryUpdated");
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) SELECT $1, 'zero-clear-' || n, n, to_timestamp(n), 'canonical' FROM generate_series($2 - 3, $2) n").bind(CHAIN).bind(resume_block).execute(database.pool()).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state, after_state) SELECT 'zero-clear-' || n, 'ens', 'SubregistryChanged', 'ens_v2_registry_l1', 2, $1, $2 - 4 + n, 'zero-clear-' || ($2 - 4 + n), 'tx-' || n, n, 0, jsonb_build_object($3, $4, $5, $4), 'ens_v2_registry_resource_surface', 'canonical', state FROM (VALUES (1, jsonb_build_object('source_event', 'SubregistryUpdated', 'token_id', $6::text, 'subregistry', '0x0000000000000000000000000000000000000011')), (2, jsonb_build_object('source_event', 'SubregistryUpdated', 'token_id', $6::text, 'subregistry', null, $7::text, jsonb_build_array($6::text))), (3, jsonb_build_object('source_event', 'SubregistryUpdated', 'token_id', $6::text, 'subregistry', '0x0000000000000000000000000000000000000012'))) rows(n, state)").bind(CHAIN).bind(resume_block).bind(bigname_adapters::schema_v2::seam::INTERPRETER_STATE_KEY).bind(&state_key).bind(bigname_adapters::schema_v2::seam::STATE_SCOPE_KEY).bind(&token_id).bind(bigname_adapters::schema_v2::seam::SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY).execute(database.pool()).await?;
    let mut loaded = crate::load::batch_input(database.pool(), CHAIN, resume_block, resume_block, None, None, StateCacheCapacity::Unlimited).await?;
    assert_eq!(loaded.restored_event_count, 2);
    let block = loaded.input.blocks[0].clone();
    let update = SubregistryUpdated { tokenId: U256::from(1), subregistry: "0x0000000000000000000000000000000000000013".parse()?, sender: Address::ZERO }.encode_log_data();
    loaded.input.raw_logs.push(bigname_adapters::schema_v2::RawLogInput { chain_id: CHAIN.to_owned(), block_hash: block.block_hash, block_number: resume_block, block_timestamp: block.block_timestamp, canonicality_state: "canonical".to_owned(), transaction_hash: "zero-clear-resume".to_owned(), transaction_index: 0, log_index: 0, emitting_address: ETH_REGISTRY.to_owned(), topics: update.topics().iter().map(|topic| format!("{topic:#x}")).collect(), data: update.data.to_vec() });
    let prepared = prepare_schema_v2_batch_incremental(loaded.input, loaded.adapter_session, StateCacheCapacity::Unlimited)?;
    let state_values = crate::load::prior_state_values(database.pool(), CHAIN, resume_block, prepared.state_value_requests()).await?;
    let (output, _) = prepared.finish(state_values)?;
    let update = output.normalized_events.iter().find(|event| event.event_kind == "SubregistryChanged").expect("resumed update");
    assert_eq!(update.before_state["subregistry"], serde_json::json!("0x0000000000000000000000000000000000000012"));
    database.cleanup().await?;
    Ok(())
}

async fn database(prefix: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
    for statement in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
        include_str!("../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../schema-v2/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
    ] {
        sqlx::raw_sql(statement).execute(database.pool()).await?;
    }
    Ok(database)
}

async fn stamp_interpreter_hash(pool: &PgPool, interpreter_content_hash: &str) -> TestResult {
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        let mut connection = pool.acquire().await?;
        sqlx::query("SELECT set_config('bigname.interpreter_content_hash', $1, false)")
            .bind(interpreter_content_hash)
            .execute(&mut *connection)
            .await?;
        connections.push(connection);
    }
    drop(connections);
    Ok(())
}

async fn seed_lineage(pool: &PgPool) -> TestResult {
    for number in SETUP_BLOCK..=MIGRATION_BLOCK {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind((number > SETUP_BLOCK).then(|| block_hash(number - 1)))
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_predecessor_facts(pool: &PgPool, labelhash: B256, namehash: B256) -> TestResult {
    let owner = OWNER.parse::<Address>()?;
    let controller = UNLOCKED_CONTROLLER.parse::<Address>()?;
    insert_transaction(pool, SETUP_BLOCK, ENS_REGISTRY).await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        0,
        ENS_REGISTRY,
        ens_registry::Transfer {
            node: namehash,
            owner: NAME_WRAPPER.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        2,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: namehash,
            label: keccak256(b"child"),
            owner,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        1,
        NAME_WRAPPER,
        NameWrapped {
            node: namehash,
            name: b"\x0factivation-gate\x03eth\0".to_vec().into(),
            owner,
            fuses: (1 << 16) | (1 << 17),
            expiry: 1_900_000_000,
        }
        .encode_log_data(),
    )
    .await?;

    insert_transaction(pool, PREDECESSOR_BLOCK, NAME_WRAPPER).await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        0,
        ENS_REGISTRY,
        ens_registry::Transfer {
            node: namehash,
            owner: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        1,
        NAME_WRAPPER,
        NameUnwrapped {
            node: namehash,
            owner: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        2,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: NAME_WRAPPER.parse()?,
            to: controller,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await
}

async fn seed_faithful_unwrapped_migration(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
) -> TestResult {
    let controller = UNLOCKED_CONTROLLER.parse::<Address>()?;
    let graveyard = GRAVEYARD.parse::<Address>()?;
    let owner = OWNER.parse::<Address>()?;
    let mut versioned = labelhash.0;
    versioned[28..].fill(0);
    let token = U256::from_be_bytes(versioned);
    insert_transaction(pool, MIGRATION_BLOCK, UNLOCKED_CONTROLLER).await?;

    // U-01's validated order follows the controller's reclaim, ENSv1 record
    // cleanup, registrar cleanup, and ENSv2 injection. The registry calls and
    // emitted events are fixed by the pinned contracts.
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L82 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L182-L208 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L250-L274 @ ens_v2@a971bd64)
    insert_log(
        pool,
        MIGRATION_BLOCK,
        0,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: owner,
            to: controller,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        1,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: eth_node(),
            label: labelhash,
            owner: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        2,
        ENS_REGISTRY,
        ens_registry::Transfer {
            node: namehash,
            owner: graveyard,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        3,
        ENS_REGISTRY,
        ens_registry::NewResolver {
            node: namehash,
            resolver: Address::ZERO,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        4,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: controller,
            to: graveyard,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        5,
        ETH_REGISTRY,
        LabelRegistered {
            tokenId: token,
            labelHash: labelhash,
            label: std::str::from_utf8(label)?.to_owned(),
            owner,
            expiry: 1_900_000_000,
            sender: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        6,
        ETH_REGISTRY,
        TransferSingle {
            operator: controller,
            from: Address::ZERO,
            to: owner,
            id: token,
            value: U256::from(1_u64),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        7,
        ETH_REGISTRY,
        TokenResource {
            tokenId: token,
            resource: token,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        8,
        ETH_REGISTRY,
        EACRolesChanged {
            resource: token,
            account: owner,
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: "97409655027181761882228017414928043062435250176".parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        9,
        ETH_REGISTRY,
        ResolverUpdated {
            tokenId: token,
            resolver: "0x922D6956C99E12DFeB3224DEA977D0939758A1Fe".parse()?,
            sender: controller,
        }
        .encode_log_data(),
    )
    .await
}

async fn seed_migration_facts(pool: &PgPool, label: &[u8], labelhash: B256) -> TestResult {
    let controller = UNLOCKED_CONTROLLER.parse::<Address>()?;
    let mut versioned = labelhash.0;
    versioned[28..].fill(0);
    let token = U256::from_be_bytes(versioned);
    insert_transaction(pool, MIGRATION_BLOCK, ETH_REGISTRY).await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        0,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: controller,
            to: GRAVEYARD.parse()?,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        1,
        ETH_REGISTRY,
        LabelRegistered {
            tokenId: token,
            labelHash: labelhash,
            label: std::str::from_utf8(label)?.to_owned(),
            owner: OWNER.parse()?,
            expiry: 1_900_000_000,
            sender: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        2,
        ETH_REGISTRY,
        TokenResource {
            tokenId: token,
            resource: token,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        3,
        MIGRATION_REGISTRY,
        RegistryCreated {}.encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        4,
        VERIFIABLE_FACTORY,
        ProxyDeployed {
            sender: LOCKED_CONTROLLER.parse()?,
            proxyAddress: MIGRATION_REGISTRY.parse()?,
            salt: U256::from_be_bytes(keccak256(b"activation-registry").0),
            implementation: WRAPPER_REGISTRY_IMPLEMENTATION.parse()?,
        }
        .encode_log_data(),
    )
    .await
}

async fn insert_transaction(pool: &PgPool, block_number: i64, to: &str) -> TestResult {
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address
         ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
    )
    .bind(CHAIN)
    .bind(block_hash(block_number))
    .bind(block_number)
    .bind(transaction_hash(block_number))
    .bind(OWNER)
    .bind(to)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_log(
    pool: &PgPool,
    block_number: i64,
    log_index: i64,
    emitting_address: &str,
    encoded: alloy_primitives::LogData,
) -> TestResult {
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data
         ) VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)",
    )
    .bind(CHAIN)
    .bind(block_hash(block_number))
    .bind(block_number)
    .bind(transaction_hash(block_number))
    .bind(log_index)
    .bind(emitting_address)
    .bind(
        encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>(),
    )
    .bind(encoded.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

fn eth_namehash(labelhash: B256) -> B256 {
    keccak256([eth_node().as_slice(), labelhash.as_slice()].concat())
}

fn eth_node() -> B256 {
    keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat())
}

fn block_hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}

fn transaction_hash(number: i64) -> String {
    format!("0x{:064x}", number + 10_000)
}
