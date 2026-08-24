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
use bigname_project::{
    BatchRequest as ProjectBatchRequest, Engine as ProjectEngine, RunMode as ProjectRunMode,
};
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
    }
}

mod base_registrar {
    use alloy_sol_types::sol;

    sol! { event Transfer(address indexed from, address indexed to, uint256 indexed tokenId); }
}

sol! {
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event RegistryCreated();
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
}

#[path = "tests/unlocked_wrapped.rs"]
mod unlocked_wrapped;

#[path = "activation_tests/equivalence.rs"]
mod equivalence;

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

    ProjectEngine::new(pool.clone())
        .run_batch(ProjectBatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: MIGRATION_BLOCK,
            affected_from_block: SETUP_BLOCK,
            affected_to_block: MIGRATION_BLOCK,
            resume_current: None,
            mode: ProjectRunMode::Normal,
        })
        .await?;
    let projected: (Uuid, serde_json::Value) = sqlx::query_as(
        "SELECT surface_binding_id, provenance FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        projected.0,
        output.migration_authority_transitions[0].successor_surface_binding_id
    );
    assert_eq!(
        projected.1.pointer("/authority_selection/proof_kind"),
        Some(&serde_json::json!("migration_authority_transition")),
        "Project must publish the successor selected by the production ENSv1→ENSv2 migration authority proof"
    );

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
    let parent = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    keccak256([parent.as_slice(), labelhash.as_slice()].concat())
}

fn block_hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}

fn transaction_hash(number: i64) -> String {
    format!("0x{:064x}", number + 10_000)
}
