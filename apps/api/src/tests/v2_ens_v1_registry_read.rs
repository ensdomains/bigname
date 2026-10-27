//! An ENSv1 name serves the resolver its ENSv1 registry record holds, and that resolver's
//! records, on name detail and the records route, read at request time whichever resource
//! Interpret linked the registry's `NewResolver` to. ENSv1 clients read the resolver from the
//! registry at read time
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L141 @ ens_v1@91c966f) and
//! `setResolver` emits `NewResolver` on every write
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L95 @ ens_v1@91c966f).
//!
//! The cases run raw logs through Engine on the checked-in Sepolia profile, then move a registry
//! `NewResolver` off the lease onto the node's registry-only resource before Project runs, the
//! row Interpret wrote for Sepolia `phantombug01.eth` at block 11982720. The composed row then
//! keeps the lease's older pointer. Each misattributed case is compared with the same logs left
//! as Interpret wrote them.
use super::v2_continuing_lease::{ENS_REGISTRY, LEASE, Logs, NAME, OWNER, registered_with};
use super::v2_sepolia_redeploy::{
    CHAIN, HEAD, NEW_REGISTRY, checked_in_profile, complete_phases, get,
};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_project::families::FamilyMode;

sol! {
    event NewResolver(bytes32 indexed node, address resolver);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event AddrChanged(bytes32 indexed node, address a);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
}

const SEPOLIA: u64 = 11_155_111;
/// The resolver the lease points at first, the latest ENS Labs PublicResolver in the Sepolia
/// profile, and the one the registry points at next, an older admitted generation.
const FIRST: &str = "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5";
const NEXT: &str = "0x8948458626811dd0c23eb25cc74291247077cc51";
/// The ENSv1 mirror resolver the Sepolia profile declares, premigration's ENSv2 resolver.
const MIRROR: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
/// The coin-60 address only `NEXT` holds for the node.
const NEXT_HOLDER: &str = "0x00000000000000000000000000000000000000b2";
const KEYS: &str = "addr:60,addr:61,text:name,text:avatar";

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
    })
}

fn lease_node() -> B256 {
    namehash(NAME)
}

fn label() -> &'static str {
    NAME.strip_suffix(".eth").expect("a .eth name")
}

/// The child `sub` of the lease's node, created by its registry owner.
fn child_node() -> B256 {
    keccak256([lease_node().as_slice(), keccak256(b"sub").as_slice()].concat())
}

fn child_name() -> String {
    format!("[{:x}].{NAME}", keccak256(b"sub"))
}

fn logs(parts: Vec<Result<Logs>>) -> Result<Logs> {
    Ok(parts.into_iter().collect::<Result<Vec<_>>>()?.concat())
}

fn pointer(node: B256, resolver: &str) -> Result<Logs> {
    Ok(vec![(
        ENS_REGISTRY,
        NewResolver {
            node,
            resolver: resolver.parse()?,
        }
        .encode_log_data(),
    )])
}

/// `setText` for each text, then `setAddr` for coin 60, which emits `AddressChanged` and then
/// `AddrChanged`, and for coin 61 when given, which emits `AddressChanged` alone.
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59-L62 @ ens_v1@91c966f)
fn records(
    resolver: &'static str,
    node: B256,
    coin_60: &str,
    coin_61: Option<&str>,
    texts: &[(&str, &str)],
) -> Result<Logs> {
    let mut out: Logs = texts
        .iter()
        .map(|(key, value)| {
            (
                resolver,
                TextChanged {
                    node,
                    indexedKey: keccak256(key.as_bytes()),
                    key: (*key).into(),
                    value: (*value).into(),
                }
                .encode_log_data(),
            )
        })
        .collect();
    let address: Address = coin_60.parse()?;
    out.push((
        resolver,
        AddressChanged {
            node,
            coinType: U256::from(60),
            newAddress: address.to_vec().into(),
        }
        .encode_log_data(),
    ));
    out.push((resolver, AddrChanged { node, a: address }.encode_log_data()));
    if let Some(coin_61) = coin_61 {
        let address: Address = coin_61.parse()?;
        out.push((
            resolver,
            AddressChanged {
                node,
                coinType: U256::from(61),
                newAddress: address.to_vec().into(),
            }
            .encode_log_data(),
        ));
    }
    Ok(out)
}

/// What `FIRST` holds for `node`: the owner as its coin-60 address, a name and an avatar.
fn first_records(node: B256) -> Result<Logs> {
    records(
        FIRST,
        node,
        OWNER,
        None,
        &[("name", "Test"), ("avatar", "first.png")],
    )
}

/// What `NEXT` holds for `node`: another coin-60 address, the owner as its coin-61 address, and
/// a name, as `0x8948…` holds `addr(node, 61)` for `phantombug01.eth`.
fn next_records(node: B256) -> Result<Logs> {
    records(NEXT, node, NEXT_HOLDER, Some(OWNER), &[("name", "Test")])
}

/// Premigration's `register` with a resolver, in one transaction: the ENSv1 lease, the
/// registry's resolver `FIRST` and its records, then the ownerless reservation with the ENSv1
/// mirror as its ENSv2 resolver.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L177-L178 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L217-L229 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L249-L266 @ ens_v2_sepolia_20261001@07e55a05)
fn premigrated() -> Result<Logs> {
    let token = U256::from_be_bytes(keccak256(label().as_bytes()).0) >> 32 << 32;
    let mut out = registered_with(
        None,
        OWNER,
        LEASE,
        logs(vec![
            pointer(lease_node(), FIRST),
            first_records(lease_node()),
        ])?,
    )?;
    out.push((
        NEW_REGISTRY,
        ResolverUpdated {
            tokenId: token,
            resolver: MIRROR.parse()?,
            sender: OWNER.parse()?,
        }
        .encode_log_data(),
    ));
    Ok(out)
}

/// The lease owner's `setSubnodeOwner(node, keccak("sub"), owner)`.
fn child() -> Result<Logs> {
    Ok(vec![(
        ENS_REGISTRY,
        NewOwner {
            node: lease_node(),
            label: keccak256(b"sub"),
            owner: OWNER.parse()?,
        }
        .encode_log_data(),
    )])
}

fn block_hash(block: i64) -> String {
    format!("{CHAIN}-block-{block}")
}

/// One Engine batch per block, `premigrated` first and an empty block at `HEAD` last. Returns
/// the base block.
async fn interpret(database: &TestDatabase, after: Vec<Logs>) -> Result<i64> {
    let pool = &database.pool;
    bigname_manifests::sync_schema_v2_repository(
        pool,
        &bigname_manifests::load_repository(checked_in_profile())?,
    )
    .await?;
    let mut blocks = vec![premigrated()?];
    blocks.extend(after);
    blocks.push(Vec::new());
    let first = HEAD + 1 - blocks.len() as i64;
    for offset in 0..blocks.len() {
        let block = first + offset as i64;
        sqlx::query(
            "INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,
                 block_timestamp,canonicality_state)
             VALUES($1,$2,$3,$4,to_timestamp($5),'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind((offset > 0).then(|| block_hash(block - 1)))
        .bind(block)
        .bind((LEASE - 100 + 10 * offset as u64) as f64)
        .execute(pool)
        .await?;
    }
    let mut previous = None;
    for (offset, logs) in blocks.into_iter().enumerate() {
        let block = first + offset as i64;
        let transaction = format!("{CHAIN}-transaction-{block}");
        if !logs.is_empty() {
            sqlx::query(
                "INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,
                     transaction_index,from_address,to_address)
                 VALUES($1,$2,$3,$4,0,$5,$6)",
            )
            .bind(CHAIN)
            .bind(block_hash(block))
            .bind(block)
            .bind(&transaction)
            .bind(OWNER)
            .bind(NEW_REGISTRY)
            .execute(pool)
            .await?;
        }
        for (index, (emitter, data)) in logs.into_iter().enumerate() {
            let topics: Vec<String> = data
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect();
            sqlx::query(
                "INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,
                     transaction_index,log_index,emitting_address,topics,data)
                 VALUES($1,$2,$3,$4,0,$5,$6,$7,$8)",
            )
            .bind(CHAIN)
            .bind(block_hash(block))
            .bind(block)
            .bind(&transaction)
            .bind(index as i64)
            .bind(emitter)
            .bind(topics)
            .bind(data.data.as_ref())
            .execute(pool)
            .await?;
        }
        bigname_interpret::Engine::new(pool.clone())
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: previous,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
        previous = Some(bigname_interpret::Marker {
            number: block,
            hash: block_hash(block),
        });
    }
    sqlx::query(
        "INSERT INTO chain_heads(chain_id,latest_block_hash,latest_block_number) VALUES($1,$2,$3)",
    )
    .bind(CHAIN)
    .bind(block_hash(HEAD))
    .bind(HEAD)
    .execute(pool)
    .await?;
    complete_phases(pool).await?;
    Ok(first)
}

/// Moves every registry `NewResolver` of `block` off the resource Interpret linked it to and onto
/// the node's registry-only resource, where Interpret put its registry-read copy, and drops the
/// copy and the registrar authority kind, as Interpret wrote the Sepolia row at 11982720.
async fn misattribute(pool: &PgPool, block: i64) -> Result<()> {
    let moved: Vec<String> = sqlx::query_scalar(
        "WITH copy AS (
             DELETE FROM bigname_phase.normalized_events
             WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'ResolverChanged'
               AND source_family = 'ens_v1_registry_l1'
               AND event_identity LIKE '%:ResolverChanged:registry-read:%'
             RETURNING resource_id, log_index)
         UPDATE bigname_phase.normalized_events event
         SET resource_id = copy.resource_id, after_state = event.after_state - 'authority_kind'
         FROM copy
         WHERE event.chain_id = $1 AND event.block_number = $2
           AND event.event_kind = 'ResolverChanged' AND event.source_family = 'ens_v1_registry_l1'
           AND event.log_index = copy.log_index
           AND event.event_identity NOT LIKE '%:ResolverChanged:registry-read:%'
         RETURNING event.event_identity",
    )
    .bind(CHAIN)
    .bind(block)
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        !moved.is_empty(),
        "block {block} has no lease NewResolver with a registry-read copy"
    );
    Ok(())
}

/// Moves the stored head to `block`, so a publication there is the current one the reads serve.
async fn head_at(pool: &PgPool, block: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chain_heads SET latest_block_number = $2, latest_block_hash = $3
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(block)
    .bind(block_hash(block))
    .execute(pool)
    .await?;
    Ok(())
}

/// Publishes the families at `target`, with the head there.
async fn project(pool: &PgPool, target: i64, mode: FamilyMode) -> Result<()> {
    head_at(pool, target).await?;
    let token = bigname_project::families::input_token(pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        pool,
        CHAIN,
        &bigname_project::Marker {
            number: target,
            hash: block_hash(target),
        },
        mode,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::PROJECTION_CONTENT_HASH,
        ),
    )
    .await?;
    anyhow::ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(target),
        "the families published {target}: {outcome:?}"
    );
    assert_bound_fields_follow_the_publication(pool, CHAIN).await
}

/// `after` interpreted, with the registry `NewResolver`s of the blocks `misattributed` names (1
/// is the first block after the base) moved onto the registry-only resource, and the families
/// rebuilt at `HEAD`.
async fn published(after: Vec<Logs>, misattributed: &[i64]) -> Result<TestDatabase> {
    let database = TestDatabase::new_migrated().await?;
    let base = interpret(&database, after).await?;
    for offset in misattributed {
        misattribute(&database.pool, base + offset).await?;
    }
    project(&database.pool, HEAD, FamilyMode::Rebuild).await?;
    Ok(database)
}

async fn ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = get(database, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

/// What a name serves on name detail and the records route: its resolver, grouped records,
/// `ens_v1.resolver`, and the records route's answers for `KEYS` and for its default set.
async fn served_name(database: &TestDatabase, name: &str) -> Result<Value> {
    let path = name.replace('[', "%5B").replace(']', "%5D");
    let detail = ok(database, &format!("/v1/names/{path}?namespace=ens")).await?;
    let keyed = ok(
        database,
        &format!("/v1/names/{path}/records?namespace=ens&keys={KEYS}"),
    )
    .await?;
    let default = ok(database, &format!("/v1/names/{path}/records?namespace=ens")).await?;
    Ok(json!({
        "authority": detail["data"]["authority"],
        "resolver": detail["data"]["resolver"],
        "ens_v1_resolver": detail["data"]["ens_v1"]["resolver"],
        "unresolvable_reason": detail["data"]["unresolvable_reason"],
        "records": detail["data"]["records"],
        "keyed_resolver": keyed["data"]["resolver"],
        "keyed": keyed["data"]["records"],
        "default_resolver": default["data"]["resolver"],
        "default": default["data"]["records"],
    }))
}

const RESOLVER_FIELDS: [&str; 4] = [
    "resolver",
    "ens_v1_resolver",
    "keyed_resolver",
    "default_resolver",
];

/// The served view of `NEXT` with its records, on every mode.
fn assert_serves_next(served: &Value) {
    for field in RESOLVER_FIELDS {
        assert_eq!(served[field], resolver(NEXT), "{field}: {served:#}");
    }
    assert_eq!(
        served["records"]["seen_addresses"],
        json!(["60", "61"]),
        "{served:#}"
    );
    assert_eq!(
        served["records"]["texts"],
        json!({"name": "Test"}),
        "{served:#}"
    );
    assert_eq!(
        served["keyed"]["addr:60"],
        json!({"status": "ok", "value": NEXT_HOLDER}),
        "{served:#}"
    );
    assert_eq!(served["keyed"]["addr:61"]["status"], "ok", "{served:#}");
    assert_eq!(
        served["keyed"]["text:avatar"],
        json!({"status": "not_found"}),
        "{served:#}"
    );
    assert_eq!(
        served["default"]["addr:60"]["value"], NEXT_HOLDER,
        "{served:#}"
    );
    assert!(served["default"].get("text:avatar").is_none(), "{served:#}");
}

async fn served(database: &TestDatabase) -> Result<Value> {
    served_name(database, NAME).await
}

fn resolver(address: &str) -> Value {
    json!({"chain_id": SEPOLIA, "address": address})
}

/// The misattributed fixture and the same logs as Interpret wrote them, both rebuilt.
async fn both(
    after: impl Fn() -> Result<Vec<Logs>>,
    misattributed: &[i64],
) -> Result<(Value, Value)> {
    let database = published(after()?, misattributed).await?;
    let wrong = served(&database).await?;
    database.cleanup().await?;
    let reference = published(after()?, &[]).await?;
    let right = served(&reference).await?;
    reference.cleanup().await?;
    Ok((wrong, right))
}

/// Row 3 (brantly.eth shape): the registry points the node at `NEXT` through its registry-only
/// resource, and `NEXT`'s records follow. The composed row keeps the lease's `FIRST`. Name
/// detail and both records reads serve `NEXT` and its records, as the same logs on the lease do.
#[tokio::test]
async fn r3_a_stale_resource_pointer_serves_the_registry_resolver() -> Result<()> {
    let (wrong, right) = both(
        || {
            Ok(vec![
                pointer(lease_node(), NEXT)?,
                next_records(lease_node())?,
            ])
        },
        &[1],
    )
    .await?;
    assert_serves_next(&wrong);
    assert_eq!(wrong, right);
    Ok(())
}

/// Row 3b: `NEXT`'s records written before the pointer in the pointer's block are served.
#[tokio::test]
async fn r3b_records_before_the_pointer_in_its_block_are_served() -> Result<()> {
    let (wrong, right) = both(
        || {
            Ok(vec![logs(vec![
                next_records(lease_node()),
                pointer(lease_node(), NEXT),
            ])?])
        },
        &[1],
    )
    .await?;
    assert_serves_next(&wrong);
    assert_eq!(wrong, right);
    Ok(())
}

/// Row 4: the registry's pointer cleared through the registry-only resource. The name serves no
/// resolver and no records, as a clear on the lease does.
#[tokio::test]
async fn r4_a_registry_clear_clears_the_served_resolver() -> Result<()> {
    let (wrong, right) = both(|| Ok(vec![pointer(lease_node(), ZERO)?]), &[1]).await?;
    for field in RESOLVER_FIELDS {
        assert!(wrong[field].is_null(), "{field}: {wrong:#}");
    }
    assert_eq!(wrong, right);
    Ok(())
}

/// Row 5, regression pin: the registry's pointer on the lease, as Interpret links it. The
/// composed row already serves `NEXT`, and the read leaves it alone.
#[tokio::test]
async fn r5_a_registry_pointer_on_the_lease_serves_as_composed() -> Result<()> {
    let database = published(
        vec![pointer(lease_node(), NEXT)?, next_records(lease_node())?],
        &[],
    )
    .await?;
    let view = served(&database).await?;
    database.cleanup().await?;
    assert_serves_next(&view);
    Ok(())
}

/// Row 6, Sepolia pinned: a registry child below the reserved `.eth` name with its own pointer,
/// which no label-bearing event names, so bigname knows it only by its label hash. bigname's
/// composed row withholds it with `ens_v2_path_no_resolver`. This pins bigname's served answer,
/// which the registry read leaves alone because it skips a withheld row. It makes no claim about
/// what the chain resolves for the child.
#[tokio::test]
async fn r6_sepolia_keeps_withholding_a_child_below_the_reservation() -> Result<()> {
    let database = published(
        vec![logs(vec![
            child(),
            pointer(child_node(), NEXT),
            next_records(child_node()),
        ])?],
        &[],
    )
    .await?;
    let view = served_name(&database, &child_name()).await?;
    database.cleanup().await?;
    assert_eq!(view["ens_v1_resolver"], resolver(NEXT), "{view:#}");
    assert_eq!(
        view["unresolvable_reason"], "ens_v2_path_no_resolver",
        "{view:#}"
    );
    for field in ["resolver", "keyed_resolver", "default_resolver"] {
        assert!(view[field].is_null(), "{field}: {view:#}");
    }
    Ok(())
}

/// Row 2 (ens.eth shape): the resolver's event named `text:email` but bigname retained no value.
/// The default records read answers it through the verified read, and only it: the other keys
/// keep their indexed answers.
#[tokio::test]
async fn r2_the_default_read_answers_an_unretained_text_through_the_verified_read() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    replace_alice_record_inputs(
        &database,
        &[
            family_fixture_record_write(
                "addr:60",
                Some(json!("0x0000000000000000000000000000000000000def")),
            ),
            family_fixture_record_write("text:email", None),
        ],
    )
    .await?;
    enable_alice_ensip19_inputs(&database).await?;
    const HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    seed_schema_v2_ens_lookup_head(&database.pool, 21_000_004, HASH, "2026-04-17T00:00:04Z")
        .await?;
    seed_schema_v2_ens_manifest(
        &database.pool,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc301),
        true,
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_004, HASH).await?;
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_text_response(
            "alice@example.test",
        )])
        .await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;
    let response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(
        payload["data"]["records"]["text:email"],
        json!({"status": "ok", "value": "alice@example.test"}),
        "{payload:#}"
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({"status": "ok", "value": "0x0000000000000000000000000000000000000def"}),
        "{payload:#}"
    );
    assert_eq!(payload["meta"]["source"], "verified", "{payload:#}");
    let requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(requests.len(), 1, "{requests:#?}");

    // An explicit `source=indexed` read never reads the chain.
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=indexed")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"]["records"]["text:email"],
        json!({"status": "unsupported", "unsupported_reason": "value_not_retained"}),
        "{payload:#}"
    );
    assert_eq!(payload["meta"]["source"], "indexed", "{payload:#}");
    database.cleanup().await
}

/// An address no Sepolia resolver manifest declares.
const NOT_DECLARED: &str = "0x00000000000000000000000000000000000000c3";

/// The lease's pointer cleared on the lease, then the registry pointing the node at `resolver`
/// through the registry-only resource with `then`'s logs in the same block. The composed row
/// serves no resolver, and the registry names `resolver`.
fn cleared_then(resolver: &str, then: Result<Logs>) -> Result<Vec<Logs>> {
    Ok(vec![
        pointer(lease_node(), ZERO)?,
        logs(vec![pointer(lease_node(), resolver), then])?,
    ])
}

/// `NEXT`'s records with a fourth selector, so its record count differs from `FIRST`'s.
fn more_next_records(node: B256) -> Result<Logs> {
    records(
        NEXT,
        node,
        NEXT_HOLDER,
        Some(OWNER),
        &[("name", "Test"), ("url", "https://next.test")],
    )
}

/// `name` DNS-encoded, as the Universal Resolver's `ResolverNotFound` carries it.
fn dns_encoded(name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for label in name.split('.') {
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out
}

/// `uri` against the Sepolia lookup pointed at a mock RPC that answers `response` once. Returns
/// the status, the body, and whether the chain was read.
async fn with_chain(
    database: &TestDatabase,
    uri: &str,
    response: Value,
) -> Result<(StatusCode, Value, bool)> {
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![response]).await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("{CHAIN}={rpc_url}"),
        ])?)
        .await?;
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    // The mock task ends once it has answered its one request.
    let read_chain = tokio::time::timeout(std::time::Duration::from_secs(5), rpc_handle)
        .await
        .is_ok();
    Ok((status, body, read_chain))
}

/// Row A1: the lease's pointer cleared, then the registry pointing at `NEXT` through the
/// registry-only resource. The composed row serves no resolver, so the verified lookup, which
/// reloads it, runs Universal Resolver discovery. A `source=verified` read answers `200` from
/// the chain and serves `NEXT`, with no `409 stale`.
#[tokio::test]
async fn ra1_a_verified_read_of_a_cleared_lease_serving_a_registry_resolver_is_not_stale()
-> Result<()> {
    let database = published(cleared_then(NEXT, next_records(lease_node()))?, &[2]).await?;
    let (status, body, read_chain) = with_chain(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&source=verified&keys=text:name"),
        resolution_universal_resolver_text_response("Test"),
    )
    .await?;
    database.cleanup().await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"]["resolver"], resolver(NEXT), "{body:#}");
    assert_eq!(
        body["data"]["records"]["text:name"],
        json!({"status": "ok", "value": "Test"}),
        "{body:#}"
    );
    assert_eq!(body["meta"]["source"], "verified", "{body:#}");
    assert!(read_chain);
    Ok(())
}

/// Row A2: as A1, with the registry pointing at an address no resolver manifest declares.
#[tokio::test]
async fn ra2_a_verified_read_of_a_cleared_lease_serving_an_undeclared_resolver_is_not_stale()
-> Result<()> {
    let database = published(cleared_then(NOT_DECLARED, Ok(Vec::new()))?, &[2]).await?;
    let (status, body, read_chain) = with_chain(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&source=verified&keys=text:name"),
        resolution_universal_resolver_text_response("Test"),
    )
    .await?;
    database.cleanup().await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"]["resolver"], resolver(NOT_DECLARED), "{body:#}");
    assert_eq!(body["meta"]["source"], "verified", "{body:#}");
    assert!(read_chain);
    Ok(())
}

/// Row A3: the registry's pointer cleared through the registry-only resource while the composed
/// row keeps `FIRST`. The verified lookup reloads the composed row and runs its projected route.
/// A `source=verified` read answers `200` with no resolver and the chain's answer, here the
/// Universal Resolver's `ResolverNotFound`, with no `409 stale`.
#[tokio::test]
async fn ra3_a_verified_read_of_a_registry_clear_is_not_stale() -> Result<()> {
    let database = published(vec![pointer(lease_node(), ZERO)?], &[1]).await?;
    let (status, body, read_chain) = with_chain(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&source=verified&keys=text:name"),
        resolution_resolver_not_found_error(&dns_encoded(NAME)),
    )
    .await?;
    database.cleanup().await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert!(body["data"]["resolver"].is_null(), "{body:#}");
    assert_ne!(
        body["data"]["records"]["text:name"]["status"], "ok",
        "{body:#}"
    );
    assert_eq!(body["meta"]["source"], "verified", "{body:#}");
    assert!(read_chain);
    Ok(())
}

/// Row A4: as A3, read with `source=auto`. The registry clear serves no resolver, so the indexed
/// answer is `not_found` and the read never reaches the chain, with no `409 stale`.
#[tokio::test]
async fn ra4_an_auto_read_of_a_registry_clear_answers_from_the_index() -> Result<()> {
    let database = published(vec![pointer(lease_node(), ZERO)?], &[1]).await?;
    let (status, body, read_chain) = with_chain(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&source=auto&keys=text:name"),
        resolution_resolver_not_found_error(&dns_encoded(NAME)),
    )
    .await?;
    database.cleanup().await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert!(body["data"]["resolver"].is_null(), "{body:#}");
    assert_eq!(
        body["data"]["records"]["text:name"]["status"], "not_found",
        "{body:#}"
    );
    assert_eq!(body["meta"]["source"], "indexed", "{body:#}");
    assert!(!read_chain);
    Ok(())
}

/// Row A5, pin: the composed row keeps `FIRST` and the registry names `NEXT`. Both select the
/// projected route, so a `source=verified` read answers `200` and serves `NEXT`.
#[tokio::test]
async fn ra5_a_verified_read_of_a_stale_pointer_serves_the_registry_resolver() -> Result<()> {
    let database = published(
        vec![pointer(lease_node(), NEXT)?, next_records(lease_node())?],
        &[1],
    )
    .await?;
    let (status, body, read_chain) = with_chain(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&source=verified&keys=text:name"),
        resolution_universal_resolver_text_response("Test"),
    )
    .await?;
    database.cleanup().await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"]["resolver"], resolver(NEXT), "{body:#}");
    assert_eq!(
        body["data"]["records"]["text:name"],
        json!({"status": "ok", "value": "Test"}),
        "{body:#}"
    );
    assert!(read_chain);
    Ok(())
}

/// A `TextChanged` with no value, so bigname retains the key `email` but not its value. The
/// layout is the archived mainnet PublicResolver's
/// (upstream: .refs/ens_v1/deployments/archive/PublicResolver_mainnet_9412610.sol/PublicResolver_mainnet_9412610.json:L269-L293 @ ens_v1@91c966f).
/// The log is synthetic: `NEXT`'s own generation emits the value
/// (upstream: .refs/ens_v1_sepolia_8209157/contracts/resolvers/profiles/ITextResolver.sol:L5-L10 @ ens_v1_sepolia_8209157@8209157),
/// and the fixture emits the older layout from its address only to make a not-retained key. The
/// Sepolia resolver manifest admits that layout from every emitter, so the emitter does not
/// change how it decodes.
fn unretained_email(resolver: &'static str, node: B256) -> Result<Logs> {
    sol! {
        event TextChanged(bytes32 indexed node, string indexed indexedKey, string key);
    }
    Ok(vec![(
        resolver,
        TextChanged {
            node,
            indexedKey: keccak256(b"email"),
            key: "email".into(),
        }
        .encode_log_data(),
    )])
}

/// Row A6: as A1, read with `source=auto` and a key whose value bigname did not retain, so the
/// read takes the verified fallback for it. It answers `200` from the chain, with no `409 stale`.
#[tokio::test]
async fn ra6_an_auto_fallback_of_a_cleared_lease_serving_a_registry_resolver_is_not_stale()
-> Result<()> {
    let database = published(
        cleared_then(
            NEXT,
            logs(vec![
                next_records(lease_node()),
                unretained_email(NEXT, lease_node()),
            ]),
        )?,
        &[2],
    )
    .await?;
    let (status, body, read_chain) = with_chain(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&source=auto&keys=text:name,text:email"),
        resolution_universal_resolver_text_response("next@example.test"),
    )
    .await?;
    database.cleanup().await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"]["resolver"], resolver(NEXT), "{body:#}");
    assert_eq!(
        body["data"]["records"]["text:name"],
        json!({"status": "ok", "value": "Test"}),
        "{body:#}"
    );
    assert_eq!(
        body["data"]["records"]["text:email"],
        json!({"status": "ok", "value": "next@example.test"}),
        "{body:#}"
    );
    assert_eq!(body["meta"]["source"], "verified", "{body:#}");
    assert!(read_chain);
    Ok(())
}

/// Row A7: as A2, read with `source=auto`. The undeclared resolver's inventory answers every key
/// `unsupported`, so the read takes the verified fallback for `text:name`. It answers `200` from
/// the chain, with no `409 stale`.
#[tokio::test]
async fn ra7_an_auto_fallback_of_a_cleared_lease_serving_an_undeclared_resolver_is_not_stale()
-> Result<()> {
    let database = published(cleared_then(NOT_DECLARED, Ok(Vec::new()))?, &[2]).await?;
    let (status, body, read_chain) = with_chain(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&source=auto&keys=text:name"),
        resolution_universal_resolver_text_response("Test"),
    )
    .await?;
    database.cleanup().await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"]["resolver"], resolver(NOT_DECLARED), "{body:#}");
    assert_eq!(
        body["data"]["records"]["text:name"],
        json!({"status": "ok", "value": "Test"}),
        "{body:#}"
    );
    assert_eq!(body["meta"]["source"], "verified", "{body:#}");
    assert!(read_chain);
    Ok(())
}

/// A contenthash for `node` on `resolver`.
fn contenthash(resolver: &'static str, node: B256) -> Result<Logs> {
    sol! {
        event ContenthashChanged(bytes32 indexed node, bytes hash);
    }
    Ok(vec![(
        resolver,
        ContenthashChanged {
            node,
            hash: vec![0xe3, 0x01, 0x01, 0x70].into(),
        }
        .encode_log_data(),
    )])
}

/// Name detail with `include=counts`: its resolver and `record_count`.
async fn counted(database: &TestDatabase) -> Result<Value> {
    let detail = ok(
        database,
        &format!("/v1/names/{NAME}?namespace=ens&include=counts"),
    )
    .await?;
    Ok(json!({
        "resolver": detail["data"]["resolver"],
        "record_count": detail["data"]["record_count"],
    }))
}

/// `counted` on the misattributed fixture and on the same logs as Interpret wrote them.
async fn both_counted(
    after: impl Fn() -> Result<Vec<Logs>>,
    misattributed: &[i64],
) -> Result<(Value, Value)> {
    let database = published(after()?, misattributed).await?;
    let wrong = counted(&database).await?;
    database.cleanup().await?;
    let reference = published(after()?, &[]).await?;
    let right = counted(&reference).await?;
    reference.cleanup().await?;
    Ok((wrong, right))
}

/// Row B1: the registry points at `NEXT`, which holds four selectors, while the composed row
/// keeps `FIRST`, which holds three. `record_count` counts `NEXT`'s inventory, the one `records`
/// comes from, as the same logs on the lease do.
#[tokio::test]
async fn rb1_record_count_follows_the_registry_resolver() -> Result<()> {
    let (wrong, right) = both_counted(
        || {
            Ok(vec![
                pointer(lease_node(), NEXT)?,
                more_next_records(lease_node())?,
            ])
        },
        &[1],
    )
    .await?;
    assert_eq!(wrong["resolver"], resolver(NEXT), "{wrong:#}");
    assert_eq!(wrong["record_count"], 4, "{wrong:#}");
    assert_eq!(wrong, right);
    Ok(())
}

/// Row B2: a registry clear serves no resolver and no records, so `record_count` is omitted, as
/// for a clear on the lease, never `FIRST`'s count.
#[tokio::test]
async fn rb2_a_registry_clear_omits_record_count() -> Result<()> {
    let (wrong, right) = both_counted(|| Ok(vec![pointer(lease_node(), ZERO)?]), &[1]).await?;
    assert!(wrong["resolver"].is_null(), "{wrong:#}");
    assert!(wrong["record_count"].is_null(), "{wrong:#}");
    assert_eq!(wrong, right);
    Ok(())
}

/// Row B3: a registry pointer to an undeclared address counts that address's inventory, as the
/// same pointer on the lease does.
#[tokio::test]
async fn rb3_record_count_follows_an_undeclared_registry_resolver() -> Result<()> {
    let (wrong, right) =
        both_counted(|| Ok(vec![pointer(lease_node(), NOT_DECLARED)?]), &[1]).await?;
    assert_eq!(wrong["resolver"], resolver(NOT_DECLARED), "{wrong:#}");
    assert_eq!(wrong, right);
    Ok(())
}

/// Row B4: the lease's pointer cleared, then the registry pointing at `NEXT`. `record_count`
/// counts `NEXT`'s inventory, as the same logs on the lease do.
#[tokio::test]
async fn rb4_record_count_of_a_cleared_lease_follows_the_registry_resolver() -> Result<()> {
    let (wrong, right) =
        both_counted(|| cleared_then(NEXT, more_next_records(lease_node())), &[2]).await?;
    assert_eq!(wrong["resolver"], resolver(NEXT), "{wrong:#}");
    assert_eq!(wrong["record_count"], 4, "{wrong:#}");
    assert_eq!(wrong, right);
    Ok(())
}

/// Row B5, pin: the registry pointer on the lease. The registry read leaves the row as
/// composed, and `record_count` counts `NEXT`'s inventory.
#[tokio::test]
async fn rb5_record_count_of_a_composed_pointer_is_unchanged() -> Result<()> {
    let database = published(
        vec![
            pointer(lease_node(), NEXT)?,
            more_next_records(lease_node())?,
        ],
        &[],
    )
    .await?;
    let view = counted(&database).await?;
    database.cleanup().await?;
    assert_eq!(view["resolver"], resolver(NEXT), "{view:#}");
    assert_eq!(view["record_count"], 4, "{view:#}");
    Ok(())
}

/// Row B6: as B1, with `NEXT` also holding a text whose value bigname did not retain and a
/// contenthash. `record_count` counts the same selectors as the same logs on the lease.
#[tokio::test]
async fn rb6_record_count_parity_covers_an_unretained_text_and_a_contenthash() -> Result<()> {
    let (wrong, right) = both_counted(
        || {
            Ok(vec![
                pointer(lease_node(), NEXT)?,
                logs(vec![
                    more_next_records(lease_node()),
                    unretained_email(NEXT, lease_node()),
                    contenthash(NEXT, lease_node()),
                ])?,
            ])
        },
        &[1],
    )
    .await?;
    assert_eq!(wrong["resolver"], resolver(NEXT), "{wrong:#}");
    assert_eq!(wrong["record_count"], 6, "{wrong:#}");
    assert_eq!(wrong, right);
    Ok(())
}

/// Row B7: as B3, with the undeclared address holding records for the node. `record_count`
/// counts its inventory as the same pointer on the lease does.
#[tokio::test]
async fn rb7_record_count_parity_covers_an_undeclared_resolver_holding_records() -> Result<()> {
    let (wrong, right) = both_counted(
        || {
            Ok(vec![
                pointer(lease_node(), NOT_DECLARED)?,
                logs(vec![
                    records(
                        NOT_DECLARED,
                        lease_node(),
                        NEXT_HOLDER,
                        None,
                        &[("url", "https://undeclared.test")],
                    ),
                    contenthash(NOT_DECLARED, lease_node()),
                ])?,
            ])
        },
        &[1],
    )
    .await?;
    assert_eq!(wrong["resolver"], resolver(NOT_DECLARED), "{wrong:#}");
    assert_eq!(wrong, right);
    Ok(())
}
