//! A registry on a named path links a registered token to its resource with `TokenResource`, then
//! emits `TokenResource` for the same token and resource again. The second link binds the same
//! name to the same resource it is already bound to, so Interpret must accept it in the same
//! batch, in a later batch after its adapter state is restored, and under either loader. The
//! deployed registry emits `TokenResource` once per token, in `register`. A registry with its own
//! bytecode on a named path can emit it again.
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L472 @ ens_v2@a971bd64)
use super::*;
use crate::ErrorKind;
use bigname_adapters::schema_v2::seam::SURFACE_BOUND_EVENT_KIND;

const FIRST: i64 = SETUP_BLOCK;
const SECOND: i64 = SETUP_BLOCK + 1;
const THIRD: i64 = SETUP_BLOCK + 2;
/// Far beyond every block timestamp here, which is the block number in seconds.
const LIVE_EXPIRY: u64 = 1_900_000_000;
const LABEL: &str = "relinked";
const PARENT_LABEL: &str = "parent";
/// A contract no Sepolia manifest declares. It admits itself with `RegistryCreated()`.
const ANNOUNCED_REGISTRY: &str = "0x00000000000000000000000000000000000000a9";

/// A label's token id at version 0, which is also its resource.
fn token(label: &str) -> U256 {
    let mut word = *keccak256(label.as_bytes());
    word[28..].copy_from_slice(&0_u32.to_be_bytes());
    U256::from_be_bytes(word)
}

fn owner() -> Address {
    OWNER.parse().expect("the owner is an address")
}

fn registered_until(label: &str, expiry: u64) -> alloy_primitives::LogData {
    LabelRegistered {
        tokenId: token(label),
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: owner(),
        expiry,
        sender: owner(),
    }
    .encode_log_data()
}

fn registered(label: &str) -> alloy_primitives::LogData {
    registered_until(label, LIVE_EXPIRY)
}

fn mint(label: &str) -> alloy_primitives::LogData {
    TransferSingle {
        operator: owner(),
        from: Address::ZERO,
        to: owner(),
        id: token(label),
        value: U256::from(1),
    }
    .encode_log_data()
}

fn link(label: &str) -> alloy_primitives::LogData {
    TokenResource {
        tokenId: token(label),
        resource: token(label),
    }
    .encode_log_data()
}

/// One transaction in `block`, holding `logs` from their emitters in order.
async fn insert_block(
    pool: &PgPool,
    block: i64,
    logs: Vec<(&str, alloy_primitives::LogData)>,
) -> TestResult {
    insert_transaction(pool, block, ETH_REGISTRY).await?;
    for (index, (emitter, log)) in logs.into_iter().enumerate() {
        insert_log(pool, block, index as i64, emitter, log).await?;
    }
    Ok(())
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

fn request(from_block: i64, to_block: i64, resume_current: Option<Marker>) -> BatchRequest {
    BatchRequest {
        chain_id: CHAIN.to_owned(),
        from_block,
        to_block,
        resume_current,
        mode: RunMode::Normal,
    }
}

#[derive(Clone, Copy, Debug)]
enum Run {
    /// A fresh lookahead Engine per block, so every later block restores its adapter state.
    EachBlock,
    /// A fresh full-state Engine per block.
    EachBlockFullState,
    /// One Engine and one batch over every block.
    OneBatch,
}

fn refused(block: i64) -> impl Fn(InterpretError) -> String {
    move |error| {
        format!(
            "Interpret refused the batch ending at block {block} ({:?}): {error}",
            error.kind()
        )
    }
}

async fn run(pool: &PgPool, run: Run, blocks: &[i64]) -> TestResult {
    if let Run::OneBatch = run {
        let last = *blocks.last().expect("a run has blocks");
        Engine::new(pool.clone())
            .run_batch(request(blocks[0], last, None))
            .await
            .map_err(refused(last))?;
        return Ok(());
    }
    let mut resume = None;
    for &block in blocks {
        let engine = Engine::new(pool.clone());
        let engine = match run {
            Run::EachBlockFullState => engine.with_full_state_loader_forced(true),
            _ => engine,
        };
        engine
            .run_batch(request(block, block, resume.take()))
            .await
            .map_err(refused(block))?;
        resume = Some(Marker {
            number: block,
            hash: block_hash(block),
        });
    }
    Ok(())
}

/// The resource the first link of `label`'s token names.
async fn linked_resource(pool: &PgPool, label: &str) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'TokenResourceLinked'
           AND after_state ->> 'upstream_resource' = $2
         ORDER BY block_number, log_index LIMIT 1",
    )
    .bind(CHAIN)
    .bind(format!("{:#066x}", token(label)))
    .fetch_one(pool)
    .await?)
}

/// The blocks of `label`'s links, and of the bindings and `SurfaceBound` and linked registration
/// rows for its resource.
async fn link_rows(pool: &PgPool, label: &str) -> TestResult<[Vec<i64>; 4]> {
    let resource = linked_resource(pool, label).await?;
    let links: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'TokenResourceLinked' AND resource_id = $2
         ORDER BY block_number",
    )
    .bind(CHAIN)
    .bind(resource)
    .fetch_all(pool)
    .await?;
    let bindings: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM surface_bindings WHERE chain_id = $1 AND resource_id = $2
         ORDER BY block_number",
    )
    .bind(CHAIN)
    .bind(resource)
    .fetch_all(pool)
    .await?;
    let bound: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
         WHERE chain_id = $1 AND event_kind = $3 AND resource_id = $2 ORDER BY block_number",
    )
    .bind(CHAIN)
    .bind(resource)
    .bind(SURFACE_BOUND_EVENT_KIND)
    .fetch_all(pool)
    .await?;
    let restated: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted' AND resource_id = $2
           AND after_state ->> 'derived_from' = 'registry_state' ORDER BY block_number",
    )
    .bind(CHAIN)
    .bind(resource)
    .fetch_all(pool)
    .await?;
    Ok([links, bindings, bound, restated])
}

/// Both links are interpreted, and only the first opens the binding, binds the name and restates
/// the registration.
async fn assert_linked_once(pool: &PgPool, label: &str, first: i64, second: i64) -> TestResult {
    assert_eq!(
        link_rows(pool, label).await?,
        [vec![first, second], vec![first], vec![first], vec![first]],
        "links, bindings, SurfaceBound rows and linked registration rows"
    );
    Ok(())
}

/// The declared registry registers and links the token in the first block and links it again in
/// the second.
async fn repeated_link_on_the_declared_registry(prefix: &str, mode: Run) -> TestResult {
    let database = sepolia_database(prefix).await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        vec![
            (ETH_REGISTRY, registered(LABEL)),
            (ETH_REGISTRY, mint(LABEL)),
            (ETH_REGISTRY, link(LABEL)),
        ],
    )
    .await?;
    insert_block(pool, SECOND, vec![(ETH_REGISTRY, link(LABEL))]).await?;
    run(pool, mode, &[FIRST, SECOND]).await?;
    assert_linked_once(pool, LABEL, FIRST, SECOND).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_repeated_link_in_a_later_batch_is_written() -> TestResult {
    repeated_link_on_the_declared_registry("interpret_registry_relink", Run::EachBlock).await
}

#[tokio::test]
async fn a_repeated_link_in_one_batch_is_written() -> TestResult {
    repeated_link_on_the_declared_registry("interpret_registry_relink_one_batch", Run::OneBatch)
        .await
}

#[tokio::test]
async fn a_repeated_link_after_a_full_state_reload_is_written() -> TestResult {
    repeated_link_on_the_declared_registry(
        "interpret_registry_relink_full_state",
        Run::EachBlockFullState,
    )
    .await
}

/// A registry that admits itself with `RegistryCreated()` and is mounted under a name the
/// declared registry holds, so its labels have names too. It registers and links its token in
/// the second block and links it again in the third.
#[tokio::test]
async fn a_self_announced_registry_repeated_link_is_written() -> TestResult {
    let database = sepolia_database("interpret_registry_relink_announced").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        vec![
            (ANNOUNCED_REGISTRY, RegistryCreated {}.encode_log_data()),
            (ETH_REGISTRY, registered(PARENT_LABEL)),
            (ETH_REGISTRY, mint(PARENT_LABEL)),
            (ETH_REGISTRY, link(PARENT_LABEL)),
            (
                ETH_REGISTRY,
                SubregistryUpdated {
                    tokenId: token(PARENT_LABEL),
                    subregistry: ANNOUNCED_REGISTRY.parse()?,
                    sender: owner(),
                }
                .encode_log_data(),
            ),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        vec![
            (ANNOUNCED_REGISTRY, registered(LABEL)),
            (ANNOUNCED_REGISTRY, mint(LABEL)),
            (ANNOUNCED_REGISTRY, link(LABEL)),
        ],
    )
    .await?;
    insert_block(pool, THIRD, vec![(ANNOUNCED_REGISTRY, link(LABEL))]).await?;
    run(pool, Run::EachBlock, &[FIRST, SECOND, THIRD]).await?;
    assert_linked_once(pool, LABEL, SECOND, THIRD).await?;
    database.cleanup().await?;
    Ok(())
}

sol! {
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
}

/// A registry admitted by `RegistryCreated()` alone has no name, so its links bind nothing.
#[tokio::test]
async fn a_bare_announced_registry_repeated_link_binds_nothing() -> TestResult {
    let database = sepolia_database("interpret_registry_relink_bare").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        vec![
            (ANNOUNCED_REGISTRY, RegistryCreated {}.encode_log_data()),
            (ANNOUNCED_REGISTRY, registered(LABEL)),
            (ANNOUNCED_REGISTRY, mint(LABEL)),
            (ANNOUNCED_REGISTRY, link(LABEL)),
        ],
    )
    .await?;
    insert_block(pool, SECOND, vec![(ANNOUNCED_REGISTRY, link(LABEL))]).await?;
    run(pool, Run::EachBlock, &[FIRST, SECOND]).await?;
    let bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings binding
         JOIN normalized_events link ON link.resource_id = binding.resource_id
         WHERE binding.chain_id = $1 AND link.event_kind = 'TokenResourceLinked'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    assert_eq!(bindings, 0, "an unnamed token binds no name");
    database.cleanup().await?;
    Ok(())
}

/// The binding `label`'s first link opened: its opening block and whether it is closed.
async fn first_binding(pool: &PgPool, label: &str) -> TestResult<Vec<(i64, bool)>> {
    Ok(sqlx::query_as(
        "SELECT binding.block_number, binding.active_to IS NOT NULL FROM surface_bindings binding
         WHERE binding.chain_id = $1 AND binding.resource_id = (
             SELECT resource_id FROM normalized_events
             WHERE chain_id = $1 AND event_kind = 'TokenResourceLinked'
               AND after_state ->> 'upstream_resource' = $2
             ORDER BY block_number, log_index LIMIT 1)
         ORDER BY binding.block_number",
    )
    .bind(CHAIN)
    .bind(format!("{:#066x}", token(label)))
    .fetch_all(pool)
    .await?)
}

/// After the name expires its binding closes. A link after that changes nothing.
#[tokio::test]
async fn a_repeated_link_after_the_name_expired_is_written() -> TestResult {
    let database = sepolia_database("interpret_registry_relink_expired").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        vec![
            (ETH_REGISTRY, registered_until(LABEL, SECOND as u64)),
            (ETH_REGISTRY, mint(LABEL)),
            (ETH_REGISTRY, link(LABEL)),
        ],
    )
    .await?;
    insert_block(pool, SECOND, vec![(ETH_REGISTRY, link(LABEL))]).await?;
    run(pool, Run::EachBlock, &[FIRST, SECOND]).await?;
    assert_eq!(
        first_binding(pool, LABEL).await?,
        vec![(FIRST, true)],
        "the expired name's one binding is closed"
    );
    database.cleanup().await?;
    Ok(())
}

/// A renewal after the name expired binds it again. A link after that changes nothing.
#[tokio::test]
async fn a_repeated_link_after_an_expired_name_is_renewed_is_written() -> TestResult {
    let database = sepolia_database("interpret_registry_relink_renewed").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        vec![
            (ETH_REGISTRY, registered_until(LABEL, SECOND as u64)),
            (ETH_REGISTRY, mint(LABEL)),
            (ETH_REGISTRY, link(LABEL)),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        vec![(
            ETH_REGISTRY,
            ExpiryUpdated {
                tokenId: token(LABEL),
                newExpiry: LIVE_EXPIRY,
                sender: owner(),
            }
            .encode_log_data(),
        )],
    )
    .await?;
    insert_block(pool, THIRD, vec![(ETH_REGISTRY, link(LABEL))]).await?;
    run(pool, Run::EachBlock, &[FIRST, SECOND, THIRD]).await?;
    let bindings = first_binding(pool, LABEL).await?;
    assert!(
        bindings.iter().all(|(block, _)| *block < THIRD),
        "the link after the renewal opens no binding: {bindings:?}"
    );
    database.cleanup().await?;
    Ok(())
}

/// A registry with its own bytecode releases the token, then registers the same token id again
/// and links it. The binding the first link opened is closed, and the new link's binding has the
/// same id with a later start, so the writer refuses it and Interpret stops. This test pins that
/// stop. A change that lets Interpret accept the order turns it red and replaces it.
#[tokio::test]
async fn a_token_registered_again_under_its_released_token_id_stops_interpret() -> TestResult {
    let database = sepolia_database("interpret_registry_relink_reregistered").await?;
    let pool = database.pool();
    insert_block(
        pool,
        FIRST,
        vec![
            (ETH_REGISTRY, registered(LABEL)),
            (ETH_REGISTRY, mint(LABEL)),
            (ETH_REGISTRY, link(LABEL)),
        ],
    )
    .await?;
    insert_block(
        pool,
        SECOND,
        vec![(
            ETH_REGISTRY,
            LabelUnregistered {
                tokenId: token(LABEL),
                sender: owner(),
            }
            .encode_log_data(),
        )],
    )
    .await?;
    insert_block(
        pool,
        THIRD,
        vec![
            (ETH_REGISTRY, registered(LABEL)),
            (ETH_REGISTRY, mint(LABEL)),
            (ETH_REGISTRY, link(LABEL)),
        ],
    )
    .await?;
    run(pool, Run::EachBlock, &[FIRST, SECOND]).await?;
    let error = Engine::new(pool.clone())
        .run_batch(request(
            THIRD,
            THIRD,
            Some(Marker {
                number: SECOND,
                hash: block_hash(SECOND),
            }),
        ))
        .await
        .expect_err("the writer refuses the reopened binding");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
    assert!(
        error
            .to_string()
            .contains("is already bound to different identity data"),
        "{error}"
    );
    database.cleanup().await?;
    Ok(())
}
