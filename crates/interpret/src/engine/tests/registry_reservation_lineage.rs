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
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
}

/// A contract no Sepolia manifest declares. It admits itself with `RegistryCreated()`.
const ANNOUNCED_REGISTRY: &str = "0x00000000000000000000000000000000000000a9";
const FIRST: i64 = SETUP_BLOCK;
const SECOND: i64 = SETUP_BLOCK + 1;
const THIRD: i64 = SETUP_BLOCK + 2;
/// Far beyond every block timestamp here, which is the block number in seconds.
const LIVE_EXPIRY: u64 = 1_900_000_000;
/// One second before the first block's timestamp, so a reservation with it has already expired.
const EXPIRED: u64 = FIRST as u64 - 1;
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

/// The label's token after one regeneration: the same label hash with token version 1.
fn regenerated_token() -> U256 {
    reserved_token() + U256::from(1)
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

fn reserved_log(expiry: u64) -> alloy_primitives::LogData {
    LabelReserved {
        tokenId: reserved_token(),
        labelHash: keccak256(LABEL.as_bytes()),
        label: LABEL.to_owned(),
        expiry,
        sender: owner(),
    }
    .encode_log_data()
}

fn registered_log() -> alloy_primitives::LogData {
    LabelRegistered {
        tokenId: reserved_token(),
        labelHash: keccak256(LABEL.as_bytes()),
        label: LABEL.to_owned(),
        owner: owner(),
        expiry: LIVE_EXPIRY,
        sender: owner(),
    }
    .encode_log_data()
}

fn mint_log() -> alloy_primitives::LogData {
    TransferSingle {
        operator: owner(),
        from: Address::ZERO,
        to: owner(),
        id: reserved_token(),
        value: U256::from(1),
    }
    .encode_log_data()
}

/// The owner transfers `token` to the holder.
fn transfer_log(token: U256) -> alloy_primitives::LogData {
    TransferSingle {
        operator: owner(),
        from: owner(),
        to: holder(),
        id: token,
        value: U256::from(1),
    }
    .encode_log_data()
}

/// The registration's resource is the token id itself, as for every token version 0.
fn token_resource_log() -> alloy_primitives::LogData {
    TokenResource {
        tokenId: reserved_token(),
        resource: reserved_token(),
    }
    .encode_log_data()
}

/// The first block: the registry announces itself, reserves the label, then registers it to
/// the owner and mints the token, with no `TokenResource`. The second block: the owner
/// transfers the token.
async fn insert_reserve_register_then_transfer(pool: &PgPool) -> TestResult {
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            reserved_log(LIVE_EXPIRY),
            registered_log(),
            mint_log(),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
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

/// Interprets each block with a new Engine, so every block after the first restores its adapter
/// state from the events the earlier blocks stored.
async fn run_each_block(pool: &PgPool, loader: Loader, blocks: &[i64]) -> TestResult {
    let mut resume = None;
    for &block in blocks {
        let engine = loader.engine(pool);
        engine
            .run_batch(request(block, block, resume.take()))
            .await
            .map_err(|error| {
                format!(
                    "the {} loader refused block {block} ({:?}): {error}",
                    loader.name(),
                    error.kind()
                )
            })?;
        assert_loader(&engine, loader)?;
        resume = Some(Marker {
            number: block,
            hash: block_hash(block),
        });
    }
    Ok(())
}

/// The resource and token lineage the first block's `event_kind` row names.
async fn drafted_identity(pool: &PgPool, event_kind: &str) -> TestResult<(Uuid, Uuid)> {
    let (resource, lineage): (Option<Uuid>, Option<String>) = sqlx::query_as(
        "SELECT resource_id, after_state ->> $3 FROM normalized_events
         WHERE chain_id = $1 AND event_kind = $4 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(FIRST)
    .bind(TOKEN_LINEAGE_ID_KEY)
    .bind(event_kind)
    .fetch_one(pool)
    .await?;
    let (Some(resource), Some(lineage)) = (resource, lineage) else {
        return Err(format!("{event_kind} names its token's resource and token lineage").into());
    };
    Ok((resource, Uuid::parse_str(&lineage)?))
}

/// The resource and token lineage the reservation drafted.
async fn reservation_identity(pool: &PgPool) -> TestResult<(Uuid, Uuid)> {
    drafted_identity(pool, "RegistrationReserved").await
}

/// The lineage was drafted once, in the first block, and the resource is still bound to it.
async fn assert_one_lineage(pool: &PgPool, resource: Uuid, lineage: Uuid) -> TestResult {
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
        "the token lineage must be drafted once, in the first block"
    );
    let bound: Option<Uuid> = sqlx::query_scalar(
        "SELECT token_lineage_id FROM resources WHERE chain_id = $1 AND resource_id = $2",
    )
    .bind(CHAIN)
    .bind(resource)
    .fetch_one(pool)
    .await?;
    assert_eq!(bound, Some(lineage), "the resource keeps its first lineage");
    Ok(())
}

async fn transfers_at(pool: &PgPool, block: i64) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = $3 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(block)
    .bind(TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
    .fetch_one(pool)
    .await?)
}

/// The token lineage the one `event_kind` row in `block` names.
async fn lineage_named_by(pool: &PgPool, event_kind: &str, block: i64) -> TestResult<Option<Uuid>> {
    let lineage: Option<String> = sqlx::query_scalar(
        "SELECT after_state ->> $3 FROM normalized_events
         WHERE chain_id = $1 AND block_number = $2 AND event_kind = $4",
    )
    .bind(CHAIN)
    .bind(block)
    .bind(TOKEN_LINEAGE_ID_KEY)
    .bind(event_kind)
    .fetch_one(pool)
    .await?;
    Ok(lineage.as_deref().map(Uuid::parse_str).transpose()?)
}

/// The token lineage the registration `LabelRegistered` produced in `block` names. Rows the
/// adapter derives from registry state carry `derived_from` and are left out.
async fn registration_lineage(pool: &PgPool, block: i64) -> TestResult<Option<Uuid>> {
    let lineage: Option<String> = sqlx::query_scalar(
        "SELECT after_state ->> $3 FROM normalized_events
         WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'RegistrationGranted'
           AND after_state ->> 'source_event' = 'LabelRegistered'
           AND after_state ->> 'derived_from' IS NULL",
    )
    .bind(CHAIN)
    .bind(block)
    .bind(TOKEN_LINEAGE_ID_KEY)
    .fetch_one(pool)
    .await?;
    Ok(lineage.as_deref().map(Uuid::parse_str).transpose()?)
}

/// An announced registry is admitted forward from its announcement, so a registry that announces
/// itself in an earlier block than its reservation takes this same path.
async fn reserved_then_registered_token_keeps_its_lineage(loader: Loader) -> TestResult {
    let prefix = format!("interpret_reserved_registered_lineage_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_reserve_register_then_transfer(pool).await?;
    run_each_block(pool, loader, &[FIRST, SECOND]).await?;

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

/// The deployed registry links every registration to its resource with `TokenResource` and
/// grants the owner's roles on it. The link names the lineage, so a restore keeps it.
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L466-L472 @ ens_v2@a971bd64)
async fn deployed_registration_order_keeps_its_lineage(loader: Loader) -> TestResult {
    let prefix = format!("interpret_deployed_registration_lineage_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            registered_log(),
            mint_log(),
            token_resource_log(),
            EACRolesChanged {
                resource: reserved_token(),
                account: owner(),
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1),
            }
            .encode_log_data(),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    run_each_block(pool, loader, &[FIRST, SECOND]).await?;

    let (resource, lineage) = drafted_identity(pool, "TokenResourceLinked").await?;
    assert_one_lineage(pool, resource, lineage).await?;
    assert_eq!(
        lineage_named_by(pool, TOKEN_CONTROL_TRANSFERRED_EVENT_KIND, SECOND).await?,
        Some(lineage),
        "the transfer after the restore names the link's lineage"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn deployed_registration_order_keeps_its_lineage_across_a_lookahead_reload() -> TestResult {
    deployed_registration_order_keeps_its_lineage(Loader::Lookahead).await
}

#[tokio::test]
async fn deployed_registration_order_keeps_its_lineage_across_a_full_state_reload() -> TestResult {
    deployed_registration_order_keeps_its_lineage(Loader::FullState).await
}

/// A reserved label registered in the deployed order is linked to the reservation's resource,
/// and the link names the lineage the reservation drafted.
#[tokio::test]
async fn reserved_then_deployed_registration_keeps_one_lineage() -> TestResult {
    let database = sepolia_database("interpret_reserved_deployed_registration_lineage").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            reserved_log(LIVE_EXPIRY),
            registered_log(),
            mint_log(),
            token_resource_log(),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    run_each_block(pool, Loader::Lookahead, &[FIRST, SECOND]).await?;

    let (resource, lineage) = reservation_identity(pool).await?;
    assert_eq!(
        drafted_identity(pool, "TokenResourceLinked").await?,
        (resource, lineage),
        "the link confirms the reservation's resource and lineage"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    assert_eq!(
        transfers_at(pool, SECOND).await?,
        1,
        "the transfer after the restore is interpreted"
    );
    database.cleanup().await?;
    Ok(())
}

/// A reservation can expire before anyone registers its label. Registering it then keeps the
/// token id, because the reservation minted no token to burn, so the lineage continues.
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L455-L460 @ ens_v2@a971bd64)
#[tokio::test]
async fn expired_reservation_registered_again_keeps_its_lineage() -> TestResult {
    let database = sepolia_database("interpret_expired_reservation_lineage").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![RegistryCreated {}.encode_log_data(), reserved_log(EXPIRED)],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![registered_log(), mint_log()],
    )
    .await?;
    insert_block(
        pool,
        THIRD,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    run_each_block(pool, Loader::Lookahead, &[FIRST, SECOND, THIRD]).await?;

    let (resource, lineage) = reservation_identity(pool).await?;
    assert_eq!(
        registration_lineage(pool, SECOND).await?,
        Some(lineage),
        "the registration names the expired reservation's lineage"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    assert_eq!(
        transfers_at(pool, THIRD).await?,
        1,
        "the transfer after the restore is interpreted"
    );
    database.cleanup().await?;
    Ok(())
}

/// Regenerating a token moves its resource and lineage to the new token id.
#[tokio::test]
async fn regenerated_reserved_token_keeps_its_lineage_across_reloads() -> TestResult {
    let database = sepolia_database("interpret_regenerated_reservation_lineage").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            reserved_log(LIVE_EXPIRY),
            registered_log(),
            mint_log(),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![
            TokenRegenerated {
                oldTokenId: reserved_token(),
                newTokenId: regenerated_token(),
            }
            .encode_log_data(),
        ],
    )
    .await?;
    insert_block(
        pool,
        THIRD,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(regenerated_token())],
    )
    .await?;
    run_each_block(pool, Loader::Lookahead, &[FIRST, SECOND, THIRD]).await?;

    let (resource, lineage) = reservation_identity(pool).await?;
    assert_eq!(
        lineage_named_by(pool, "TokenRegenerated", SECOND).await?,
        Some(lineage),
        "the regeneration moves the reservation's lineage to the new token"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    assert_eq!(
        transfers_at(pool, THIRD).await?,
        1,
        "the transfer of the regenerated token is interpreted"
    );
    database.cleanup().await?;
    Ok(())
}

/// A manifest-declared registry interprets the same logs the same way.
#[tokio::test]
async fn declared_registry_reserved_then_registered_keeps_its_lineage() -> TestResult {
    let database = sepolia_database("interpret_declared_reservation_lineage").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ETH_REGISTRY,
        vec![reserved_log(LIVE_EXPIRY), registered_log(), mint_log()],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ETH_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    run_each_block(pool, Loader::Lookahead, &[FIRST, SECOND]).await?;

    let (resource, lineage) = reservation_identity(pool).await?;
    assert_eq!(
        registration_lineage(pool, FIRST).await?,
        Some(lineage),
        "the registration names the reservation's lineage"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    assert_eq!(
        transfers_at(pool, SECOND).await?,
        1,
        "the transfer after the restore is interpreted"
    );
    database.cleanup().await?;
    Ok(())
}

/// A reserved token with no registration has no registration row to restore.
#[tokio::test]
async fn reserved_token_transfer_after_a_reload_keeps_its_lineage() -> TestResult {
    let database = sepolia_database("interpret_reserved_transfer_lineage").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            reserved_log(LIVE_EXPIRY),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    run_each_block(pool, Loader::Lookahead, &[FIRST, SECOND]).await?;

    let (resource, lineage) = reservation_identity(pool).await?;
    assert_eq!(
        lineage_named_by(pool, TOKEN_CONTROL_TRANSFERRED_EVENT_KIND, SECOND).await?,
        Some(lineage),
        "the transfer names the reservation's lineage"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    database.cleanup().await?;
    Ok(())
}

/// With no batch boundary between the registration and the transfer there is no restore, so
/// the adapter state keeps the lineage.
#[tokio::test]
async fn reserved_then_registered_same_batch_keeps_one_lineage() -> TestResult {
    let database = sepolia_database("interpret_reserved_registered_same_batch_lineage").await?;
    let pool = database.pool();
    insert_reserve_register_then_transfer(pool).await?;
    Engine::new(pool.clone())
        .run_batch(request(FIRST, SECOND, None))
        .await?;

    let (resource, lineage) = reservation_identity(pool).await?;
    assert_one_lineage(pool, resource, lineage).await?;
    assert_eq!(
        transfers_at(pool, SECOND).await?,
        1,
        "the transfer is interpreted"
    );
    database.cleanup().await?;
    Ok(())
}

/// A label the registry did not reserve.
const OTHER_LABEL: &str = "other";

/// The reserved label's token id registered under another label. Nothing in the log ties the
/// token id to the label hash, so a registry with its own bytecode can emit it.
fn other_label_registered_log() -> alloy_primitives::LogData {
    LabelRegistered {
        tokenId: reserved_token(),
        labelHash: keccak256(OTHER_LABEL.as_bytes()),
        label: OTHER_LABEL.to_owned(),
        owner: owner(),
        expiry: LIVE_EXPIRY,
        sender: owner(),
    }
    .encode_log_data()
}

/// The token's resource word, as a transfer's `upstream_resource` names it.
fn token_word() -> String {
    format!("{:#066x}", reserved_token())
}

/// The resource and token lineage the registration `LabelRegistered` produced in `block` names.
async fn registration_identity(
    pool: &PgPool,
    block: i64,
) -> TestResult<(Option<Uuid>, Option<Uuid>)> {
    let (resource, lineage): (Option<Uuid>, Option<String>) = sqlx::query_as(
        "SELECT resource_id, after_state ->> $3 FROM normalized_events
         WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'RegistrationGranted'
           AND after_state ->> 'source_event' = 'LabelRegistered'
           AND after_state ->> 'derived_from' IS NULL",
    )
    .bind(CHAIN)
    .bind(block)
    .bind(TOKEN_LINEAGE_ID_KEY)
    .fetch_one(pool)
    .await?;
    Ok((
        resource,
        lineage.as_deref().map(Uuid::parse_str).transpose()?,
    ))
}

/// The resource, upstream resource word and token lineage the one transfer in `block` names.
async fn transfer_identity(
    pool: &PgPool,
    block: i64,
) -> TestResult<(Option<Uuid>, Option<String>, Option<Uuid>)> {
    let (resource, upstream, lineage): (Option<Uuid>, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT resource_id, after_state ->> 'upstream_resource', after_state ->> $3
             FROM normalized_events
             WHERE chain_id = $1 AND block_number = $2 AND event_kind = $4",
        )
        .bind(CHAIN)
        .bind(block)
        .bind(TOKEN_LINEAGE_ID_KEY)
        .bind(TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
        .fetch_one(pool)
        .await?;
    Ok((
        resource,
        upstream,
        lineage.as_deref().map(Uuid::parse_str).transpose()?,
    ))
}

/// The first block: the registry announces itself, links the token to its resource with
/// `TokenResource`, then registers the token's label to the owner and mints the token.
fn link_then_register_logs() -> Vec<alloy_primitives::LogData> {
    vec![
        RegistryCreated {}.encode_log_data(),
        token_resource_log(),
        registered_log(),
        mint_log(),
    ]
}

/// A registry with its own bytecode can link a token to its resource before it registers the
/// token's label. Nothing unbinds the resource, so the registration keeps it and so does the
/// transfer after it. The deployed registry emits `TokenResource` after `LabelRegistered`, in
/// the same call.
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L472 @ ens_v2@a971bd64)
#[tokio::test]
async fn token_resource_before_label_keeps_the_resource_in_one_batch() -> TestResult {
    let database = sepolia_database("interpret_link_before_label_one_batch").await?;
    let pool = database.pool();
    let mut logs = link_then_register_logs();
    logs.push(transfer_log(reserved_token()));
    insert_block(pool, FIRST, ANNOUNCED_REGISTRY, logs).await?;
    Engine::new(pool.clone())
        .run_batch(request(FIRST, FIRST, None))
        .await?;

    let (resource, lineage) = drafted_identity(pool, "TokenResourceLinked").await?;
    assert_eq!(
        registration_identity(pool, FIRST).await?,
        (Some(resource), Some(lineage)),
        "the registration keeps the linked resource and lineage"
    );
    assert_eq!(
        transfer_identity(pool, FIRST).await?,
        (Some(resource), Some(token_word()), Some(lineage)),
        "the transfer names the linked resource and lineage"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    database.cleanup().await?;
    Ok(())
}

/// The same logs with a restore before the transfer. The restore keeps the resource the link
/// attached, so the transfer names what the one-batch run names.
async fn token_resource_before_label_keeps_the_resource_across_a_reload(
    loader: Loader,
) -> TestResult {
    let prefix = format!("interpret_link_before_label_reload_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_block(pool, FIRST, ANNOUNCED_REGISTRY, link_then_register_logs()).await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    run_each_block(pool, loader, &[FIRST, SECOND]).await?;

    let (resource, lineage) = drafted_identity(pool, "TokenResourceLinked").await?;
    assert_eq!(
        transfer_identity(pool, SECOND).await?,
        (Some(resource), Some(token_word()), Some(lineage)),
        "the transfer after the restore names the linked resource and lineage"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn token_resource_before_label_keeps_the_resource_across_a_lookahead_reload() -> TestResult {
    token_resource_before_label_keeps_the_resource_across_a_reload(Loader::Lookahead).await
}

#[tokio::test]
async fn token_resource_before_label_keeps_the_resource_across_a_full_state_reload() -> TestResult {
    token_resource_before_label_keeps_the_resource_across_a_reload(Loader::FullState).await
}

/// The link in one batch and the label in the next. The registration is interpreted from the
/// restored link and keeps its resource.
#[tokio::test]
async fn token_resource_a_batch_before_its_label_keeps_the_resource() -> TestResult {
    let database = sepolia_database("interpret_link_a_batch_before_label").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![RegistryCreated {}.encode_log_data(), token_resource_log()],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![registered_log(), mint_log()],
    )
    .await?;
    insert_block(
        pool,
        THIRD,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    run_each_block(pool, Loader::Lookahead, &[FIRST, SECOND, THIRD]).await?;

    let (resource, lineage) = drafted_identity(pool, "TokenResourceLinked").await?;
    assert_eq!(
        registration_identity(pool, SECOND).await?,
        (Some(resource), Some(lineage)),
        "the registration keeps the restored link's resource and lineage"
    );
    assert_eq!(
        transfer_identity(pool, THIRD).await?,
        (Some(resource), Some(token_word()), Some(lineage)),
        "the transfer names the linked resource and lineage"
    );
    assert_one_lineage(pool, resource, lineage).await?;
    database.cleanup().await?;
    Ok(())
}

/// Registering the reserved token id under another label does not keep the reservation's
/// resource, in one batch or across restores.
async fn token_registered_under_another_label_drops_the_reserved_resource(
    reload: bool,
) -> TestResult {
    let prefix = format!("interpret_other_label_drops_reservation_{reload}");
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            reserved_log(LIVE_EXPIRY),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![other_label_registered_log(), mint_log()],
    )
    .await?;
    insert_block(
        pool,
        THIRD,
        ANNOUNCED_REGISTRY,
        vec![transfer_log(reserved_token())],
    )
    .await?;
    if reload {
        run_each_block(pool, Loader::Lookahead, &[FIRST, SECOND, THIRD]).await?;
    } else {
        Engine::new(pool.clone())
            .run_batch(request(FIRST, THIRD, None))
            .await?;
    }

    reservation_identity(pool).await?;
    assert_eq!(
        registration_identity(pool, SECOND).await?,
        (None, None),
        "the registration under another label names no resource"
    );
    assert_eq!(
        transfer_identity(pool, THIRD).await?,
        (None, None, None),
        "the transfer names no resource"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn token_registered_under_another_label_drops_the_reserved_resource_in_one_batch()
-> TestResult {
    token_registered_under_another_label_drops_the_reserved_resource(false).await
}

#[tokio::test]
async fn token_registered_under_another_label_drops_the_reserved_resource_across_a_reload()
-> TestResult {
    token_registered_under_another_label_drops_the_reserved_resource(true).await
}
