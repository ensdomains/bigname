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
const SETUP_BLOCK: i64 = 11_709_100;
const PREDECESSOR_BLOCK: i64 = SETUP_BLOCK + 1;
const MIGRATION_BLOCK: i64 = SETUP_BLOCK + 2;
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const UNLOCKED_CONTROLLER: &str = "0x7ed171bb143a905f56105e4ea146543ecb122f55";
const LOCKED_CONTROLLER: &str = "0xab1b57c6ee5e91e6090595c0af14cb9b8bc7773f";
const GRAVEYARD: &str = "0x950b93885b33ce4c7e8571be2c88a1aa93d82f49";
const ETH_REGISTRY: &str = "0x657ea849311d3d5823348dded7c2aaafb3ede09e";
const VERIFIABLE_FACTORY: &str = "0x9e726eb570beb6bceb495ab8cda7df517d4e841c";
const WRAPPER_REGISTRY_IMPLEMENTATION: &str = "0x2741543c3b14640b97bc70a233318032f7e35bac";
const MIGRATION_REGISTRY: &str = "0x0000000000000000000000000000000000000771";
const OWNER: &str = "0x0000000000000000000000000000000000000051";
/// The BaseRegistrar transfer to the Graveyard in `seed_migration_facts`.
const MIGRATION_CLEANUP_LOG_INDEX: i64 = 3;

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

#[path = "tests/registry_only_handoff.rs"]
mod registry_only_handoff;

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

    seed_migration_facts(pool, label, labelhash, namehash).await?;
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
        Some(
            time::OffsetDateTime::from_unix_timestamp(MIGRATION_BLOCK)?
                + time::Duration::microseconds(MIGRATION_CLEANUP_LOG_INDEX)
        ),
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

    // This fixture drives the checked-in Sepolia manifests through the complete migration
    // transaction plus the `RegistryCreated` and `ProxyDeployed` logs absent from U-01. It proves
    // exactly-one predecessor materialization through the production writer, not a publication
    // path; the wrapped-then-unwrapped predecessor and Redo replay are covered below.

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn faithful_unwrapped_migration_retires_all_v1_bindings() -> TestResult {
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

    // The ENSv1→ENSv2 migration block has the same ordered ten-event shape as
    // U-01 logs 0-9, using the checked-in Sepolia deployment and fixture values.
    // The pre-state is a wrapped-then-unwrapped name held by the eventual
    // ENSv1→ENSv2 migration sender; U-01 instead uses a plain registration with resolver
    // state. The plain-registration path is covered separately; this test retains
    // the wrapped-then-unwrapped predecessor as an independent regression.
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    seed_faithful_unwrapped_migration(pool, label, labelhash, namehash).await?;
    for mode in [RunMode::Normal, RunMode::Redo] {
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: MIGRATION_BLOCK,
                to_block: MIGRATION_BLOCK,
                resume_current: Some(Marker {
                    number: PREDECESSOR_BLOCK,
                    hash: block_hash(PREDECESSOR_BLOCK),
                }),
                mode,
            })
            .await?;
        let (v1, v2): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE authority_arm = 'ens_v1'),
                    count(*) FILTER (WHERE authority_arm = 'ens_v2')
             FROM surface_bindings WHERE chain_id = $1 AND logical_name_id = $2
               AND active_to IS NULL AND canonicality_state = 'canonical'",
        )
        .bind(CHAIN)
        .bind(&logical_name_id)
        .fetch_one(pool)
        .await?;
        assert_eq!((v1, v2), (0, 1), "cleanup must retire every V1 binding");
        let (boundaries, transfers): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE event_kind = 'MigrationApplied' AND consumer_visibility = 'activated'),
                    count(*) FILTER (WHERE event_kind = $4 AND source_family = 'ens_v1_registrar_l1')
             FROM normalized_events WHERE chain_id = $1 AND logical_name_id = $2 AND block_number = $3",
        ).bind(CHAIN).bind(&logical_name_id).bind(MIGRATION_BLOCK)
        .bind(bigname_adapters::schema_v2::seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
        .fetch_one(pool).await?;
        assert_eq!(
            (boundaries, transfers),
            (1, 2),
            "both registrar observations remain auditable"
        );
    }
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[rustfmt::skip]
async fn cold_restore_retains_zero_clear_beside_later_state_tail() -> TestResult {
    let database = database("interpret_zero_clear_retention").await?;
    sync_schema_v2_repository(database.pool(), &load_repository(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"))?).await?;
    let resume_block = 11_709_200_i64;
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
            owner,
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
            owner,
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
            to: owner,
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

    // U-01's validated block-236 transaction order follows the controller's
    // reclaim, ENSv1 record cleanup, registrar cleanup, and ENSv2 injection.
    // The registry calls and emitted events are fixed by the pinned contracts.
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33-L41 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L82 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L186 @ ens_v1@91c966f)
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

/// The unlocked controller's complete `.eth` migration transaction with the migration registry
/// creation logs appended: registrar transfer to the controller, registry reclaim, registry
/// `setRecord` to the Graveyard, registrar transfer to the Graveyard, ENSv2 registration with its
/// mint, resource link and role grant. The predecessor here holds no resolver, so `setRecord`
/// emits no `NewResolver`.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L188 @ ens_v1@91c966f)
async fn seed_migration_facts(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
) -> TestResult {
    let owner = OWNER.parse::<Address>()?;
    let controller = UNLOCKED_CONTROLLER.parse::<Address>()?;
    let graveyard = GRAVEYARD.parse::<Address>()?;
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
        MIGRATION_CLEANUP_LOG_INDEX,
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
        4,
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
        5,
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
        6,
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
        7,
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
        8,
        MIGRATION_REGISTRY,
        RegistryCreated {}.encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        9,
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

#[tokio::test]
async fn resolver_creation_replay_preserves_records_without_requesting_another_ingest() -> TestResult
{
    sol! {
        event ResolverCreated();
        event TextUpdated(uint256 indexed recordId, string indexed keyHash, string key, string value);
    }
    let database = database("resolver_creation_replay").await?;
    let pool = database.pool();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(root)?).await?;
    seed_lineage(pool).await?;
    const CREATED: &str = "0x0000000000000000000000000000000000000c01";
    const POINTER_ONLY: &str = "0x0000000000000000000000000000000000000c02";
    insert_transaction(pool, SETUP_BLOCK, CREATED).await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        0,
        CREATED,
        ResolverCreated {}.encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        1,
        CREATED,
        TextUpdated {
            recordId: U256::from(1),
            keyHash: keccak256(b"url"),
            key: "url".to_owned(),
            value: "first".to_owned(),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        2,
        ETH_REGISTRY,
        ResolverUpdated {
            tokenId: U256::from(1),
            resolver: POINTER_ONLY.parse()?,
            sender: OWNER.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_transaction(pool, MIGRATION_BLOCK, CREATED).await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        0,
        CREATED,
        TextUpdated {
            recordId: U256::from(1),
            keyHash: keccak256(b"url"),
            key: "url".to_owned(),
            value: "second".to_owned(),
        }
        .encode_log_data(),
    )
    .await?;
    sqlx::query("INSERT INTO ingest_cursors (chain_id,source_key,source_kind,seed_basis,start_block_number,next_block_number,target_block_number,last_processed_block_number,last_processed_block_hash) VALUES ($1,'creation-test','rpc','ethereum_head',$2,$3+1,$3,$3,$4)").bind(CHAIN).bind(SETUP_BLOCK).bind(MIGRATION_BLOCK).bind(block_hash(MIGRATION_BLOCK)).execute(pool).await?;
    sqlx::query("INSERT INTO chain_phase_state (chain_id,phase_name,phase_status,current_block_number,current_block_hash,target_block_number,target_block_hash,started_at,finished_at) VALUES ($1,'ingest','completed',$2,$3,$2,$3,now(),now())").bind(CHAIN).bind(MIGRATION_BLOCK).bind(block_hash(MIGRATION_BLOCK)).execute(pool).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    let mut original = None;
    for mode in [RunMode::Normal, RunMode::Redo] {
        let result = Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: SETUP_BLOCK,
                to_block: MIGRATION_BLOCK,
                resume_current: None,
                mode,
            })
            .await?;
        assert!(result.complete);
        let records: Vec<(String, serde_json::Value, serde_json::Value)> = sqlx::query_as("SELECT event_identity,before_state,after_state FROM normalized_events WHERE chain_id=$1 AND event_kind='RecordChanged' ORDER BY block_number,log_index").bind(CHAIN).fetch_all(pool).await?;
        assert_eq!(records.len(), 2);
        if let Some(expected) = &original {
            assert_eq!(&records, expected);
        } else {
            original = Some(records);
        }
        let pending: bool = sqlx::query_scalar("SELECT redo_in_progress FROM chain_phase_state WHERE chain_id=$1 AND phase_name='ingest'").bind(CHAIN).fetch_one(pool).await?;
        assert!(
            !pending,
            "creation capture and pointer changes must not request a second fetch"
        );
        let addresses: Vec<String> = sqlx::query_scalar("SELECT DISTINCT address FROM discovery_watch_admissions WHERE chain_id=$1 ORDER BY address").bind(CHAIN).fetch_all(pool).await?;
        assert_eq!(addresses, [CREATED]);
    }
    database.cleanup().await?;
    Ok(())
}
