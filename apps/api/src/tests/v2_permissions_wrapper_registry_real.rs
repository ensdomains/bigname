//! Production raw-log loading → adapter admission → Interpret persistence → Project → HTTP.
//! Logs are hand-encoded contract emission sequences, not EVM execution. The factory origin
//! precedes delayed admission, or follows initialization in its creation transaction.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1561 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1564 @ ens_v2_sepolia_20261001@07e55a05)
use super::*;
use alloy_primitives::{Address, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_interpret::{BatchRequest, Engine, RunMode};

#[path = "v2_permissions_wrapper_registry_role.rs"]
mod api_role;
#[path = "v2_permissions_wrapper_registry_history.rs"]
mod history;
#[path = "v2_permissions_wrapper_registry_lifecycle.rs"]
mod lifecycle;
#[path = "v2_permissions_wrapper_registry_proof.rs"]
mod proof;

const CHAIN: &str = "ethereum-sepolia";
const BASE: i64 = 11_820_500;
const LAST: i64 = BASE + 24;
const TIME: i64 = 1_800_000_000;
const FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
const LOCKED: &str = "0x6029a063d69b09d23c52a754a90e4fe43adac3a8";
const ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const USER_IMPL: &str = "0x9bd8a88719068d09ecee662f36c0e3856708366a";
const WRAPPER_IMPL: &str = "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880";
const UNKNOWN: &str = "0x0000000000000000000000000000000000000bad";
const PARENT: &str = "0x0000000000000000000000000000000000000d01";
const WRAPPER: &str = "0x0000000000000000000000000000000000000d02";
const EMPTY_PARENT: &str = "0x0000000000000000000000000000000000000d04";
const CHILD: &str = "0x0000000000000000000000000000000000000d03";
const ALICE: &str = "0x00000000000000000000000000000000000000a1";
const BOB: &str = "0x00000000000000000000000000000000000000b1";
const OPERATOR: &str = "0x00000000000000000000000000000000000000c1";
const ADMIN: &str = "0x00000000000000000000000000000000000000e1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const LABEL: &str = "holder";

sol! {
    event RegistryCreated();
    event Upgraded(address indexed implementation);
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
    event ParentUpdated(address indexed parent, string label, address indexed sender);
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event ApprovalForAll(address indexed account, address indexed operator, bool approved);
}

fn address(value: &str) -> Address {
    value.parse().unwrap()
}
fn bit(index: usize) -> U256 {
    U256::from(1) << index
}
fn bitmap() -> U256 {
    bit(0) | bit(8) | bit(16) | bit(156)
}
fn admin_bitmap() -> U256 {
    bit(0)
        | bit(8)
        | bit(124)
        | bit(128)
        | bit(136)
        | bit(144)
        | bit(148)
        | bit(152)
        | bit(156)
        | bit(252)
}
fn parent_admin() -> U256 {
    bit(0) | bit(8) | bit(124) | bit(16) | bit(20) | bit(128) | bit(144)
}
fn expected_powers() -> Value {
    json!(["registrar", "set_parent", "renew", "can_transfer_admin"])
}
fn id(label: &str) -> U256 {
    U256::from_be_bytes(keccak256(label).0) >> 32 << 32
}
fn block_hash(block: i64) -> String {
    format!("0x{block:064x}")
}
fn namehash(name: &str) -> alloy_primitives::B256 {
    name.rsplit('.')
        .fold(alloy_primitives::B256::ZERO, |node, label| {
            keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
        })
}
fn tx_hash(block: i64) -> String {
    format!("0x{:064x}", block + 1_000_000)
}
fn block_time(block: i64) -> time::OffsetDateTime {
    timestamp(TIME + block - BASE)
}

#[derive(Default)]
struct Logs(Vec<(i64, String, LogData)>);
impl Logs {
    fn push(&mut self, offset: i64, registry: &str, data: LogData) -> &mut Self {
        self.0.push((BASE + offset, registry.into(), data));
        self
    }
    fn roles(
        &mut self,
        offset: i64,
        registry: &str,
        resource: U256,
        subject: &str,
        old: U256,
        new: U256,
    ) -> &mut Self {
        self.push(
            offset,
            registry,
            EACRolesChanged {
                resource,
                account: address(subject),
                oldRoleBitmap: old,
                newRoleBitmap: new,
            }
            .encode_log_data(),
        )
    }
    fn upgrade(&mut self, offset: i64, registry: &str, implementation: &str) -> &mut Self {
        self.push(
            offset,
            registry,
            Upgraded {
                implementation: address(implementation),
            }
            .encode_log_data(),
        )
    }
    fn factory(
        &mut self,
        offset: i64,
        factory: &str,
        proxy: &str,
        sender: &str,
        implementation: &str,
    ) -> &mut Self {
        self.push(
            offset,
            factory,
            ProxyDeployed {
                sender: address(sender),
                proxyAddress: address(proxy),
                salt: U256::from_be_bytes(
                    if sender == LOCKED {
                        namehash("holder.eth")
                    } else if sender == WRAPPER {
                        namehash("child.holder.eth")
                    } else {
                        keccak256(proxy)
                    }
                    .0,
                ),
                implementation: address(implementation),
            }
            .encode_log_data(),
        )
    }
    fn parent(
        &mut self,
        offset: i64,
        registry: &str,
        parent: &str,
        label: &str,
        sender: &str,
    ) -> &mut Self {
        self.push(
            offset,
            registry,
            ParentUpdated {
                parent: address(parent),
                label: label.into(),
                sender: address(sender),
            }
            .encode_log_data(),
        )
    }
    // User initialization seeds the operational root account used by these scenarios. Wrapper
    // initialization alone seeds root admin bits; subsequent public grants can add only ordinary
    // root roles while the caller still has their corresponding initialized admin bits.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/UserRegistry.sol:L57-L69 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L125-L145 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L250-L265 @ ens_v2_sepolia_20261001@07e55a05)
    fn user(&mut self, offset: i64, registry: &str) -> &mut Self {
        self.upgrade(offset, registry, USER_IMPL)
            .push(offset, registry, RegistryCreated {}.encode_log_data())
            .roles(
                offset,
                registry,
                U256::ZERO,
                ADMIN,
                U256::ZERO,
                parent_admin(),
            )
            .factory(offset, FACTORY, registry, ADMIN, USER_IMPL)
    }
    fn initialize_wrapper(
        &mut self,
        offset: i64,
        registry: &str,
        parent: &str,
        label: &str,
        roles: U256,
    ) -> &mut Self {
        self.push(offset, registry, RegistryCreated {}.encode_log_data())
            .parent(offset, registry, parent, label, ZERO)
            .roles(offset, registry, U256::ZERO, parent, U256::ZERO, roles)
    }
    fn wrapper(
        &mut self,
        offset: i64,
        registry: &str,
        parent: &str,
        label: &str,
        sender: &str,
        roles: U256,
    ) -> &mut Self {
        self.upgrade(offset, registry, WRAPPER_IMPL)
            .initialize_wrapper(offset, registry, parent, label, roles)
            .factory(offset, FACTORY, registry, sender, WRAPPER_IMPL)
    }
    // `_register` may grant zero token roles. Transfers in lifecycle fixtures use a separately
    // initialized can_transfer_admin token, since the transfer gate reads the token owner's own
    // resource bitmap; no root permission substitutes for it.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L448-L514 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L528-L543 @ ens_v2_sepolia_20261001@07e55a05)
    fn register(
        &mut self,
        offset: i64,
        registry: &str,
        label: &str,
        owner: &str,
        expiry: u64,
        roles: U256,
    ) -> &mut Self {
        self.register_by(offset, registry, label, (owner, ADMIN), expiry, roles)
    }
    fn register_by(
        &mut self,
        offset: i64,
        registry: &str,
        label: &str,
        (owner, sender): (&str, &str),
        expiry: u64,
        roles: U256,
    ) -> &mut Self {
        let token = id(label);
        self.push(
            offset,
            registry,
            LabelRegistered {
                tokenId: token,
                labelHash: keccak256(label),
                label: label.into(),
                owner: address(owner),
                expiry,
                sender: address(sender),
            }
            .encode_log_data(),
        )
        .push(
            offset,
            registry,
            TransferSingle {
                operator: address(sender),
                from: Address::ZERO,
                to: address(owner),
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
        )
        .push(
            offset,
            registry,
            TokenResource {
                tokenId: token,
                resource: token,
            }
            .encode_log_data(),
        );
        if roles != U256::ZERO {
            self.roles(offset, registry, token, owner, U256::ZERO, roles);
        }
        self
    }
    fn approve(
        &mut self,
        offset: i64,
        registry: &str,
        owner: &str,
        operator: &str,
        approved: bool,
    ) -> &mut Self {
        self.push(
            offset,
            registry,
            ApprovalForAll {
                account: address(owner),
                operator: address(operator),
                approved,
            }
            .encode_log_data(),
        )
    }
    fn renew(&mut self, offset: i64, registry: &str, label: &str, expiry: u64) -> &mut Self {
        self.push(
            offset,
            registry,
            ExpiryUpdated {
                tokenId: id(label),
                newExpiry: expiry,
                sender: address(ADMIN),
            }
            .encode_log_data(),
        )
    }
}

fn manual_logs(token_roles: U256, expiry: u64) -> Logs {
    let mut logs = Logs::default();
    logs.user(0, PARENT)
        .register(1, PARENT, LABEL, ALICE, expiry, token_roles)
        .wrapper(2, WRAPPER, PARENT, LABEL, ADMIN, bitmap() | admin_bitmap())
        .roles(
            3,
            WRAPPER,
            U256::ZERO,
            ADMIN,
            U256::ZERO,
            bit(0) | bit(8) | bit(124),
        )
        .roles(3, WRAPPER, U256::ZERO, ALICE, U256::ZERO, bit(20))
        .roles(3, WRAPPER, U256::ZERO, OPERATOR, U256::ZERO, bit(24))
        .roles(
            3,
            WRAPPER,
            U256::ZERO,
            PARENT,
            bitmap() | admin_bitmap(),
            bitmap(),
        )
        .approve(4, PARENT, ALICE, OPERATOR, true)
        .approve(4, PARENT, ALICE, ALICE, true);
    logs
}

async fn setup(logs: &Logs) -> Result<TestDatabase> {
    let database = TestDatabase::new_migrated().await?;
    // The phase runner stamps every Interpret connection; the direct Engine fixture does too.
    database
        .pool
        .set_connect_options(database.pool.connect_options().as_ref().clone().options([(
            "bigname.interpreter_content_hash",
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        )]));
    let mut connections = Vec::new();
    for _ in 0..database.pool.options().get_max_connections() {
        let mut connection = database.pool.acquire().await?;
        sqlx::query("SELECT set_config('bigname.interpreter_content_hash', $1, false)")
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .execute(&mut *connection)
            .await?;
        connections.push(connection);
    }
    drop(connections);
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    bigname_manifests::sync_schema_v2_repository(&database.lookup_pool, &repository).await?;
    for block in BASE..=LAST {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                block_timestamp, canonicality_state) VALUES ($1, $2, $3, $4, $5, 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind(block_hash(block - 1))
        .bind(block)
        .bind(block_time(block))
        .execute(&database.pool)
        .await?;
    }
    let mut indices = std::collections::BTreeMap::<i64, i64>::new();
    for (block, registry, data) in &logs.0 {
        sqlx::query(
            "INSERT INTO raw_transactions (chain_id, block_hash, block_number, transaction_hash,
                transaction_index, from_address, to_address) VALUES ($1, $2, $3, $4, 0, $5, $6)
            ON CONFLICT DO NOTHING",
        )
        .bind(CHAIN)
        .bind(block_hash(*block))
        .bind(block)
        .bind(tx_hash(*block))
        .bind(ADMIN)
        .bind(FACTORY)
        .execute(&database.pool)
        .await?;
        let index = indices.entry(*block).or_default();
        sqlx::query(
            "INSERT INTO raw_logs (chain_id, block_hash, block_number, transaction_hash,
                transaction_index, log_index, emitting_address, topics, data)
            VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)",
        )
        .bind(CHAIN)
        .bind(block_hash(*block))
        .bind(block)
        .bind(tx_hash(*block))
        .bind(*index)
        .bind(registry)
        .bind(
            data.topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect::<Vec<_>>(),
        )
        .bind(data.data.to_vec())
        .execute(&database.pool)
        .await?;
        *index += 1;
    }
    Ok(database)
}

async fn interpret(database: &TestDatabase, split: bool, cold: bool) -> Result<()> {
    let make_engine = || {
        Engine::new(database.pool.clone())
            .with_blocks_per_batch(std::num::NonZeroU32::new(if split { 1 } else { 500 }).unwrap())
    };
    let mut engine = make_engine();
    let mut resume = None;
    loop {
        if cold {
            engine = make_engine();
        }
        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                from_block: BASE,
                to_block: LAST,
                resume_current: resume,
                mode: RunMode::Normal,
            })
            .await?;
        if outcome.complete {
            break;
        }
        resume = Some(outcome.current);
    }
    Ok(())
}

async fn publish(database: &TestDatabase, offset: i64) -> Result<()> {
    let block = BASE + offset;
    sqlx::query("INSERT INTO chain_phase_state (chain_id, phase_name, phase_status, current_block_number,
            current_block_hash, target_block_number, target_block_hash, input_content_hash,
            started_at, finished_at)
        VALUES ($1, 'interpret', 'completed', $2, $3, $2, $3, $4, now(), now()),
               ($1, 'project', 'completed', $2, $3, $2, $3, $4, now(), now())
        ON CONFLICT (chain_id, phase_name) DO UPDATE SET phase_status = 'completed',
            current_block_number = EXCLUDED.current_block_number, current_block_hash = EXCLUDED.current_block_hash,
            target_block_number = EXCLUDED.target_block_number, target_block_hash = EXCLUDED.target_block_hash,
            input_content_hash = EXCLUDED.input_content_hash")
        .bind(CHAIN).bind(block).bind(block_hash(block)).bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .execute(&database.pool).await?;
    publish_test_families_on(&database.pool, CHAIN, block).await?;
    head(database, offset).await
}

async fn head(database: &TestDatabase, offset: i64) -> Result<()> {
    let block = BASE + offset;
    // Keep this a normal canonical head: the history fixture must be able to orphan a
    // departure. The generic API selector seed finalizes every target and cannot model undo.
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
        VALUES ($1, $2, $3) ON CONFLICT (chain_id) DO UPDATE SET
        latest_block_hash = EXCLUDED.latest_block_hash,
        latest_block_number = EXCLUDED.latest_block_number",
    )
    .bind(CHAIN)
    .bind(block_hash(block))
    .bind(block)
    .execute(&database.pool)
    .await?;
    Ok(())
}

// The older generic test routers override ENS to mainnet. These fixtures use real
// Sepolia manifests, so use normal production namespace selection instead.
fn state(pool: sqlx::PgPool) -> AppState {
    AppState::new_with_rpc_urls(pool, bigname_lookup::ChainRpcUrls::default())
}
async fn response(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = app_router(state(database.lookup_pool.clone()))
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    Ok((response.status(), read_json(response).await?))
}
async fn payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = response(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}
async fn pages(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    let mut pages = Vec::new();
    let mut next: Option<String> = None;
    loop {
        let uri = match &next {
            Some(cursor) => format!("{uri}&cursor={cursor}"),
            None => uri.to_owned(),
        };
        let body = payload(database, &uri).await?;
        next = body["page"]["next_cursor"].as_str().map(str::to_owned);
        assert_eq!(body["page"]["has_more"], json!(next.is_some()), "{body:#}");
        pages.push(body);
        if next.is_none() {
            break;
        }
        anyhow::ensure!(pages.len() < 100, "too many permission pages");
    }
    Ok(pages)
}
async fn registry_page(database: &TestDatabase, registry: &str) -> Result<Value> {
    payload(
        database,
        &format!("/v1/permissions?registry=11155111:{registry}"),
    )
    .await
}
fn for_subject<'a>(page: &'a Value, subject: &str) -> Vec<&'a Value> {
    page["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["address"] == subject)
        .collect()
}
fn assert_derived(page: &Value, subject: &str, relation: &str, parent: &str, owner: &str) {
    assert_derived_powers(page, subject, relation, parent, owner, expected_powers());
}
fn assert_derived_powers(
    page: &Value,
    subject: &str,
    relation: &str,
    parent: &str,
    owner: &str,
    powers: Value,
) {
    let rows = for_subject(page, subject);
    assert_eq!(rows.len(), 1, "{page:#}");
    let row = rows[0];
    assert_eq!(row["grant_relation"], relation, "{row:#}");
    assert_eq!(row["powers"], powers, "{row:#}");
    if relation == "operator" {
        assert_eq!(
            row["grant_scope"],
            json!({"kind": "account", "detail": {
            "chain_id": 11155111, "authority_kind": "ens_v2_registry", "authority_contract": parent, "owner": owner}})
        );
    } else {
        assert_eq!(row["grant_scope"]["kind"], "root");
    }
}
