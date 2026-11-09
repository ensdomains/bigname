//! An ENSv2 registry that reserves a label drafts the reserved token's resource and its token
//! lineage. Registering the reserved label keeps that resource and lineage on the token. A
//! contract no manifest declares can admit itself with `RegistryCreated()`, reserve a label,
//! then register it without linking the token to a resource with `TokenResource`. The lineage
//! the reservation drafted must still be known after the adapter state is restored from stored
//! events, so a later token event on the label neither drafts it again nor drafts the resource
//! with a different lineage. That restore happens at every batch boundary under the lookahead
//! loader and at every process start under the full-state loader. A fresh Engine per block
//! stands in for both.
//! (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18-L39 @ ens_v2@a971bd64)
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L472 @ ens_v2@a971bd64)
use super::*;
use bigname_adapters::schema_v2::seam::{
    TOKEN_CONTROL_TRANSFERRED_EVENT_KIND, TOKEN_LINEAGE_ID_KEY,
};

sol! {
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
}

/// A contract no Sepolia manifest declares. It admits itself with `RegistryCreated()`.
const ANNOUNCED_REGISTRY: &str = "0x00000000000000000000000000000000000000a9";
const FIRST: i64 = SETUP_BLOCK;
const SECOND: i64 = SETUP_BLOCK + 1;
/// Far beyond every block timestamp here, which is the block number in seconds.
const LIVE_EXPIRY: u64 = 1_900_000_000;
const LABEL: &str = "reserved";

#[derive(Clone, Copy, Debug)]
enum Loader {
    Lookahead,
    FullState,
}

impl Loader {
    fn engine(self, pool: &PgPool) -> Engine {
        match self {
            Self::Lookahead => Engine::new(pool.clone()),
            Self::FullState => Engine::new(pool.clone()).with_full_state_loader_forced(true),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Lookahead => "lookahead",
            Self::FullState => "full-state",
        }
    }
}

/// The label's first token id: its label hash with a zero token version in the low 32 bits.
fn reserved_token() -> U256 {
    let mut word = *keccak256(LABEL.as_bytes());
    word[28..].copy_from_slice(&0_u32.to_be_bytes());
    U256::from_be_bytes(word)
}

fn owner() -> Address {
    OWNER.parse().expect("the owner is an address")
}

/// The account the registered token is transferred to after the restore.
fn holder() -> Address {
    Address::with_last_byte(0xd1)
}

async fn sepolia_database(prefix: &str) -> TestResult<TestDatabase> {
    let database = database(prefix).await?;
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(database.pool(), &load_repository(manifest_root)?).await?;
    seed_lineage(database.pool()).await?;
    Ok(database)
}

/// One transaction to `emitter` in `block`, holding `logs` in order.
async fn insert_block(
    pool: &PgPool,
    block: i64,
    emitter: &str,
    logs: Vec<alloy_primitives::LogData>,
) -> TestResult {
    insert_transaction(pool, block, emitter).await?;
    for (index, log) in logs.into_iter().enumerate() {
        insert_log(pool, block, index as i64, emitter, log).await?;
    }
    Ok(())
}

/// The first block: the registry announces itself, reserves the label, then registers it to
/// the owner and mints the token, with no `TokenResource`. The second block: the owner
/// transfers the token.
async fn insert_reserve_register_then_transfer(pool: &PgPool) -> TestResult {
    let token = reserved_token();
    let label_hash = keccak256(LABEL.as_bytes());
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            LabelReserved {
                tokenId: token,
                labelHash: label_hash,
                label: LABEL.to_owned(),
                expiry: LIVE_EXPIRY,
                sender: owner(),
            }
            .encode_log_data(),
            LabelRegistered {
                tokenId: token,
                labelHash: label_hash,
                label: LABEL.to_owned(),
                owner: owner(),
                expiry: LIVE_EXPIRY,
                sender: owner(),
            }
            .encode_log_data(),
            TransferSingle {
                operator: owner(),
                from: Address::ZERO,
                to: owner(),
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![
            TransferSingle {
                operator: owner(),
                from: owner(),
                to: holder(),
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
        ],
    )
    .await
}

fn request(from_block: i64, to_block: i64, resume_current: Option<Marker>) -> BatchRequest {
    BatchRequest {
        chain_id: CHAIN.to_owned(),
        from_block,
        to_block,
        resume_current,
        mode: RunMode::Normal,
    }
}

fn assert_loader(engine: &Engine, loader: Loader) -> TestResult {
    assert_eq!(
        engine
            .chosen_loader(CHAIN)?
            .map(|choice| choice.to_string()),
        Some(loader.name().to_owned()),
        "the batch did not run under the {} loader",
        loader.name()
    );
    Ok(())
}

/// Interprets the first block, then the second with a new Engine, so the second block's
/// adapter state is restored from the events the first block stored.
async fn run_block_by_block(pool: &PgPool, loader: Loader) -> TestResult {
    let first = loader.engine(pool);
    first.run_batch(request(FIRST, FIRST, None)).await?;
    assert_loader(&first, loader)?;
    let resume = Marker {
        number: FIRST,
        hash: block_hash(FIRST),
    };
    let second = loader.engine(pool);
    second
        .run_batch(request(SECOND, SECOND, Some(resume)))
        .await
        .map_err(|error| {
            format!(
                "the {} loader refused the second block ({:?}): {error}",
                loader.name(),
                error.kind()
            )
        })?;
    assert_loader(&second, loader)?;
    Ok(())
}

/// The resource and token lineage the reservation drafted.
async fn reservation_identity(pool: &PgPool) -> TestResult<(Uuid, Uuid)> {
    let (resource, lineage): (Option<Uuid>, Option<String>) = sqlx::query_as(
        "SELECT resource_id, after_state ->> $3 FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationReserved' AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(FIRST)
    .bind(TOKEN_LINEAGE_ID_KEY)
    .fetch_one(pool)
    .await?;
    let (Some(resource), Some(lineage)) = (resource, lineage) else {
        return Err("a reservation names its token's resource and token lineage".into());
    };
    Ok((resource, Uuid::parse_str(&lineage)?))
}

async fn reserved_then_registered_token_keeps_its_lineage(loader: Loader) -> TestResult {
    let prefix = format!("interpret_reserved_registered_lineage_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_reserve_register_then_transfer(pool).await?;
    run_block_by_block(pool, loader).await?;

    let (resource, lineage) = reservation_identity(pool).await?;
    let blocks: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM token_lineages
         WHERE chain_id = $1 AND token_lineage_id = $2",
    )
    .bind(CHAIN)
    .bind(lineage)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        blocks,
        vec![FIRST],
        "the token lineage must be drafted once, by the reservation"
    );
    let bound: Option<Uuid> = sqlx::query_scalar(
        "SELECT token_lineage_id FROM resources WHERE chain_id = $1 AND resource_id = $2",
    )
    .bind(CHAIN)
    .bind(resource)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        bound,
        Some(lineage),
        "the resource keeps the reservation's lineage"
    );
    let transfers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = $3 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(SECOND)
    .bind(TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        transfers, 1,
        "the transfer after the restore is interpreted"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reserved_then_registered_token_keeps_its_lineage_across_a_lookahead_reload() -> TestResult
{
    reserved_then_registered_token_keeps_its_lineage(Loader::Lookahead).await
}

#[tokio::test]
async fn reserved_then_registered_token_keeps_its_lineage_across_a_full_state_reload() -> TestResult
{
    reserved_then_registered_token_keeps_its_lineage(Loader::FullState).await
}
