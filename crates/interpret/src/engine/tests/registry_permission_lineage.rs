//! A role change on an ENSv2 registry resource names that resource's token lineage, and the
//! lineage is drafted once, by the first event that names it. A contract no manifest declares
//! can admit itself with `RegistryCreated()` and change roles on a resource that no token was
//! ever linked to. The lineage its first role change drafted must still be known after the
//! adapter state is restored from stored events. That restore happens at every batch boundary
//! under the lookahead loader and at every process start under the full-state loader. A fresh
//! Engine per block stands in for both. Most cases run under each loader; the cases about the
//! state-value cache and the root resource run under the lookahead loader only.
use super::*;
use bigname_adapters::schema_v2::seam::TOKEN_LINEAGE_ID_KEY;

/// A contract no Sepolia manifest declares. It admits itself with `RegistryCreated()`.
const ANNOUNCED_REGISTRY: &str = "0x00000000000000000000000000000000000000a7";
const FIRST: i64 = SETUP_BLOCK;
const SECOND: i64 = SETUP_BLOCK + 1;
/// Far beyond every block timestamp here, which is the block number in seconds.
const LIVE_EXPIRY: u64 = 1_900_000_000;

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

/// A word whose high 224 bits are a label hash and whose low 32 bits are a version, as
/// registry token ids and role resources are.
fn versioned(label: &str, version: u32) -> U256 {
    let mut word = *keccak256(label.as_bytes());
    word[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(word)
}

/// The non-zero role resource of the self-announced registry cases.
fn announced_resource() -> U256 {
    versioned("announced", 1)
}

fn owner() -> Address {
    OWNER.parse().expect("the owner is an address")
}

/// Distinct accounts the self-announced registry grants roles to.
fn account(index: u8) -> Address {
    Address::with_last_byte(0xc0 + index)
}

fn role_change(resource: U256, account: Address, old: u64, new: u64) -> alloy_primitives::LogData {
    EACRolesChanged {
        resource,
        account,
        oldRoleBitmap: U256::from(old),
        newRoleBitmap: U256::from(new),
    }
    .encode_log_data()
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

/// The registry announces itself and grants a role on its resource in the first block.
async fn insert_announcement_and_grant(pool: &PgPool) -> TestResult {
    insert_block(
        pool,
        FIRST,
        ANNOUNCED_REGISTRY,
        vec![
            RegistryCreated {}.encode_log_data(),
            role_change(announced_resource(), owner(), 0, 1),
        ],
    )
    .await
}

/// The first block's announcement and grant, then a second block that widens the role.
async fn insert_self_announced_role_changes(pool: &PgPool) -> TestResult {
    insert_announcement_and_grant(pool).await?;
    insert_block(
        pool,
        SECOND,
        ANNOUNCED_REGISTRY,
        vec![role_change(announced_resource(), owner(), 1, 3)],
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

/// Interprets the first block, then the second with a new Engine from `engine`, so the second
/// block's adapter state is restored from the events the first block stored.
async fn run_block_by_block_with(loader: Loader, engine: impl Fn() -> Engine) -> TestResult {
    let first = engine();
    first.run_batch(request(FIRST, FIRST, None)).await?;
    assert_loader(&first, loader)?;
    let resume = Marker {
        number: FIRST,
        hash: block_hash(FIRST),
    };
    let second = engine();
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

async fn run_block_by_block(pool: &PgPool, loader: Loader) -> TestResult {
    run_block_by_block_with(loader, || loader.engine(pool)).await
}

/// The token lineage of the resource the first block's role change names.
async fn first_role_change_lineage(pool: &PgPool) -> TestResult<Uuid> {
    let lineage: Option<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT resources.token_lineage_id
         FROM normalized_events events
         JOIN resources
           ON resources.chain_id = events.chain_id
          AND resources.resource_id = events.resource_id
         WHERE events.chain_id = $1
           AND events.event_kind = 'PermissionChanged'
           AND events.block_number = $2",
    )
    .bind(CHAIN)
    .bind(FIRST)
    .fetch_one(pool)
    .await?;
    let Some(lineage) = lineage else {
        return Err("a role change on a non-zero resource names its token lineage".into());
    };
    Ok(lineage)
}

/// The lineage has exactly one row, drafted by the first block.
async fn assert_one_lineage_from_first_block(pool: &PgPool, lineage: Uuid) -> TestResult {
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
        "the token lineage must be drafted once, by the first block"
    );
    Ok(())
}

/// A restore rebuilds the drafted lineages from stored after_states, so every role change on
/// the resource carries the lineage it names: `count` rows, in block and log order.
async fn assert_role_changes_carry(pool: &PgPool, lineage: Uuid, count: usize) -> TestResult {
    let carried: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT after_state ->> $2 FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'PermissionChanged'
         ORDER BY block_number, log_index",
    )
    .bind(CHAIN)
    .bind(TOKEN_LINEAGE_ID_KEY)
    .fetch_all(pool)
    .await?;
    assert_eq!(carried, vec![Some(lineage.to_string()); count]);
    Ok(())
}

/// One prior role change on the resource, then a reload, then a second role change.
async fn one_prior_permission_change_keeps_the_lineage(loader: Loader) -> TestResult {
    let prefix = format!("interpret_announced_role_lineage_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_self_announced_role_changes(pool).await?;
    run_block_by_block(pool, loader).await?;

    let lineage = first_role_change_lineage(pool).await?;
    assert_one_lineage_from_first_block(pool, lineage).await?;
    assert_role_changes_carry(pool, lineage, 2).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn one_prior_permission_change_keeps_the_lineage_across_a_lookahead_reload() -> TestResult {
    one_prior_permission_change_keeps_the_lineage(Loader::Lookahead).await
}

#[tokio::test]
async fn one_prior_permission_change_keeps_the_lineage_across_a_full_state_reload() -> TestResult {
    one_prior_permission_change_keeps_the_lineage(Loader::FullState).await
}

/// The interpreter state-value cache, `StateCacheCapacity::Entries` (default
/// `DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES`), is the one bound a loader puts on restored
/// state: it evicts the least recently used values and reloads them on demand, while the
/// lookahead queries read every prior row. A role change's state is keyed by its account, so
/// granting more accounts on the resource than the cache holds restores more rows than it
/// keeps. The second role change after the reload must still find the first lineage.
const STATE_CACHE_ENTRIES: usize = 4;

#[tokio::test]
async fn many_prior_permission_changes_beyond_the_state_cache_keep_the_lineage_across_a_lookahead_reload()
-> TestResult {
    let database = sepolia_database("interpret_announced_role_lineage_beyond_cache").await?;
    let pool = database.pool();
    let resource = announced_resource();
    let mut first_block = vec![RegistryCreated {}.encode_log_data()];
    for index in 1..=2 * STATE_CACHE_ENTRIES as u8 {
        first_block.push(role_change(resource, account(index), 0, 1));
    }
    insert_block(pool, FIRST, ANNOUNCED_REGISTRY, first_block).await?;
    let widen = role_change(resource, account(1), 1, 3);
    insert_block(pool, SECOND, ANNOUNCED_REGISTRY, vec![widen]).await?;
    let engine = || Engine::with_state_cache_capacity(pool.clone(), STATE_CACHE_ENTRIES);
    run_block_by_block_with(Loader::Lookahead, engine).await?;

    let lineage = first_role_change_lineage(pool).await?;
    assert_one_lineage_from_first_block(pool, lineage).await?;
    assert_role_changes_carry(pool, lineage, 2 * STATE_CACHE_ENTRIES + 1).await?;
    database.cleanup().await?;
    Ok(())
}

/// A role change on the root resource names no token lineage: it drafts none, before or after
/// a reload, and its after_state carries the lineage key as null.
#[tokio::test]
async fn root_permission_changes_carry_a_null_lineage_and_draft_none() -> TestResult {
    let database = sepolia_database("interpret_announced_root_role_lineage").await?;
    let pool = database.pool();
    let grant = role_change(U256::ZERO, account(1), 0, 1);
    let announce = RegistryCreated {}.encode_log_data();
    insert_block(pool, FIRST, ANNOUNCED_REGISTRY, vec![announce, grant]).await?;
    let widen = role_change(U256::ZERO, account(1), 1, 3);
    insert_block(pool, SECOND, ANNOUNCED_REGISTRY, vec![widen]).await?;
    run_block_by_block(pool, Loader::Lookahead).await?;

    let lineages: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM token_lineages lineage
         JOIN resources
           ON resources.chain_id = lineage.chain_id
          AND resources.token_lineage_id = lineage.token_lineage_id
         JOIN normalized_events events
           ON events.chain_id = resources.chain_id
          AND events.resource_id = resources.resource_id
         WHERE lineage.chain_id = $1
           AND events.raw_fact_ref ->> 'emitting_address' = $2",
    )
    .bind(CHAIN)
    .bind(ANNOUNCED_REGISTRY)
    .fetch_one(pool)
    .await?;
    assert_eq!(lineages, 0, "a root role change drafts no token lineage");
    let carried: Vec<Option<serde_json::Value>> = sqlx::query_scalar(
        "SELECT after_state -> $2 FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RootPermissionChanged'
         ORDER BY block_number, log_index",
    )
    .bind(CHAIN)
    .bind(TOKEN_LINEAGE_ID_KEY)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        carried,
        vec![Some(serde_json::Value::Null); 2],
        "both root role changes carry the lineage key as null"
    );
    database.cleanup().await?;
    Ok(())
}

/// A declared registry's `register` links the token to its resource before it grants roles,
/// and that link carries the lineage across a restore. A later role change on the live token
/// finds the lineage already drafted.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L448-L514 @ ens_v2_sepolia_20261001@07e55a05)
async fn declared_registry_role_change_after_a_reload_keeps_one_lineage(
    loader: Loader,
) -> TestResult {
    let prefix = format!("interpret_declared_role_lineage_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    let label = "registered";
    let token = versioned(label, 0);
    let owner: Address = OWNER.parse()?;
    insert_block(
        pool,
        FIRST,
        ETH_REGISTRY,
        vec![
            LabelRegistered {
                tokenId: token,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry: LIVE_EXPIRY,
                sender: owner,
            }
            .encode_log_data(),
            TransferSingle {
                operator: owner,
                from: Address::ZERO,
                to: owner,
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
            TokenResource {
                tokenId: token,
                resource: token,
            }
            .encode_log_data(),
            role_change(token, owner, 0, 1),
        ],
    )
    .await?;
    let widen = role_change(token, owner, 1, 3);
    insert_block(pool, SECOND, ETH_REGISTRY, vec![widen]).await?;
    run_block_by_block(pool, loader).await?;

    let lineage = first_role_change_lineage(pool).await?;
    assert_one_lineage_from_first_block(pool, lineage).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn declared_registry_role_change_after_a_reload_keeps_one_lineage_under_lookahead()
-> TestResult {
    declared_registry_role_change_after_a_reload_keeps_one_lineage(Loader::Lookahead).await
}

#[tokio::test]
async fn declared_registry_role_change_after_a_reload_keeps_one_lineage_under_full_state()
-> TestResult {
    declared_registry_role_change_after_a_reload_keeps_one_lineage(Loader::FullState).await
}

/// Both role changes of the self-announced registry in one batch: the adapter state that
/// drafted the lineage is the one that reads the second role change.
async fn same_batch_role_changes_keep_one_lineage(loader: Loader) -> TestResult {
    let prefix = format!("interpret_same_batch_role_lineage_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_self_announced_role_changes(pool).await?;
    let engine = loader.engine(pool);
    let outcome = engine.run_batch(request(FIRST, SECOND, None)).await?;
    assert_loader(&engine, loader)?;
    assert!(outcome.complete, "one batch covers both blocks");
    assert_eq!(outcome.current.number, SECOND);

    let lineage = first_role_change_lineage(pool).await?;
    assert_one_lineage_from_first_block(pool, lineage).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn same_batch_role_changes_keep_one_lineage_under_lookahead() -> TestResult {
    same_batch_role_changes_keep_one_lineage(Loader::Lookahead).await
}

#[tokio::test]
async fn same_batch_role_changes_keep_one_lineage_under_full_state() -> TestResult {
    same_batch_role_changes_keep_one_lineage(Loader::FullState).await
}

/// The self-announced registry changes roles on its resource first and links a token to that
/// resource only after a restore. The link names the lineage the role change already drafted.
async fn token_link_after_a_role_change_and_a_reload_keeps_one_lineage(
    loader: Loader,
) -> TestResult {
    let prefix = format!("interpret_announced_link_lineage_{}", loader.name());
    let database = sepolia_database(&prefix).await?;
    let pool = database.pool();
    insert_announcement_and_grant(pool).await?;
    let link = TokenResource {
        tokenId: versioned("announced", 0),
        resource: announced_resource(),
    }
    .encode_log_data();
    insert_block(pool, SECOND, ANNOUNCED_REGISTRY, vec![link]).await?;
    run_block_by_block(pool, loader).await?;

    let lineage = first_role_change_lineage(pool).await?;
    assert_one_lineage_from_first_block(pool, lineage).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn token_link_after_a_role_change_and_a_reload_keeps_one_lineage_under_lookahead()
-> TestResult {
    token_link_after_a_role_change_and_a_reload_keeps_one_lineage(Loader::Lookahead).await
}

#[tokio::test]
async fn token_link_after_a_role_change_and_a_reload_keeps_one_lineage_under_full_state()
-> TestResult {
    token_link_after_a_role_change_and_a_reload_keeps_one_lineage(Loader::FullState).await
}
