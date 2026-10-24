//! An ENSv1 name serves the resolver its ENSv1 registry record holds, and that resolver's
//! records, whichever resource Interpret linked the registry's `NewResolver` to. ENSv1 clients
//! read the resolver from the registry at read time
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L141 @ ens_v1@91c966f) and
//! `setResolver` emits `NewResolver` on every write
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L95 @ ens_v1@91c966f).
//!
//! The Sepolia cases run raw logs through Engine on the checked-in profile, then move a registry
//! `NewResolver` off the lease onto the node's registry-only resource before Project runs. That is
//! the row Interpret wrote for Sepolia `phantombug01.eth` at block 11982720: one named event on
//! the registry-only resource, with no authority kind and no copy on the lease. Each misattributed
//! case is compared with the same logs left as Interpret wrote them.
use super::v2_continuing_lease::{ENS_REGISTRY, LEASE, Logs, NAME, OWNER, registered_with};
use super::v2_sepolia_redeploy::{
    CHAIN, HEAD, NEW_REGISTRY, checked_in_profile, complete_phases, get, lookup,
};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_project::families::FamilyMode;
use std::hash::{Hash, Hasher};

sol! {
    event NewResolver(bytes32 indexed node, address resolver);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event AddrChanged(bytes32 indexed node, address a);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
}
mod erc721 {
    alloy_sol_types::sol! { event Transfer(address indexed from, address indexed to, uint256 indexed tokenId); }
}

const SEPOLIA: u64 = 11_155_111;
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
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
const DAY: u64 = 86_400;

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

/// `wrapETH2LD` by the holder: the registrar transfer to the NameWrapper, its reclaim, the
/// ERC1155 mint and `NameWrapped`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1020 @ ens_v1@91c966f)
fn wrapped() -> Result<Logs> {
    let labelhash = keccak256(label().as_bytes());
    let owner: Address = OWNER.parse()?;
    let mut dns = vec![u8::try_from(label().len())?];
    dns.extend_from_slice(label().as_bytes());
    dns.extend_from_slice(b"\x03eth\0");
    Ok(vec![
        (
            BASE_REGISTRAR,
            erc721::Transfer {
                from: owner,
                to: NAME_WRAPPER.parse()?,
                tokenId: U256::from_be_bytes(labelhash.0),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewOwner {
                node: namehash("eth"),
                label: labelhash,
                owner: NAME_WRAPPER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: owner,
                from: Address::ZERO,
                to: owner,
                id: U256::from_be_bytes(lease_node().0),
                value: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node: lease_node(),
                name: dns.into(),
                owner,
                fuses: 0x30000,
                expiry: LEASE + 90 * DAY,
            }
            .encode_log_data(),
        ),
    ])
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

/// Every row of a paginated route, following its cursors.
async fn rows(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = match &cursor {
            None => ok(database, uri).await?,
            Some(cursor) => ok(database, &format!("{uri}&cursor={cursor}")).await?,
        };
        out.extend(page["data"].as_array().cloned().unwrap_or_default());
        cursor = page["page"]["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            return Ok(out);
        }
        anyhow::ensure!(out.len() < 10_000, "{uri}: too many rows");
    }
}

/// What a name serves: its resolver and grouped records on name detail and lookup,
/// `ens_v1.resolver`, and the records route's answers for `KEYS`.
async fn served_name(database: &TestDatabase, name: &str) -> Result<Value> {
    let path = name.replace('[', "%5B").replace(']', "%5D");
    let detail = ok(database, &format!("/v1/names/{path}?namespace=ens")).await?;
    let (status, looked) = lookup(
        database,
        json!({"profile": "detail", "inputs": [{"name": name}]}),
    )
    .await?;
    anyhow::ensure!(status == StatusCode::OK, "{looked:#}");
    let keyed = ok(
        database,
        &format!("/v1/names/{path}/records?namespace=ens&keys={KEYS}"),
    )
    .await?;
    let record = &looked["data"][0]["record"];
    Ok(json!({
        "authority": detail["data"]["authority"],
        "resolver": detail["data"]["resolver"],
        "ens_v1_resolver": detail["data"]["ens_v1"]["resolver"],
        "unresolvable_reason": detail["data"]["unresolvable_reason"],
        "records": detail["data"]["records"],
        "lookup_resolver": record["resolver"],
        "lookup_records": record["records"],
        "keyed_resolver": keyed["data"]["resolver"],
        "keyed": keyed["data"]["records"],
    }))
}

async fn served(database: &TestDatabase) -> Result<Value> {
    served_name(database, NAME).await
}

fn resolver(address: &str) -> Value {
    json!({"chain_id": SEPOLIA, "address": address})
}

/// The served view of `NEXT` with its records, on every mode.
fn assert_serves_next(served: &Value) {
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert_eq!(served[field], resolver(NEXT), "{field}: {served:#}");
    }
    for records in [&served["records"], &served["lookup_records"]] {
        assert_eq!(records["seen_addresses"], json!(["60", "61"]), "{served:#}");
        assert_eq!(records["texts"], json!({"name": "Test"}), "{served:#}");
    }
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
}

/// The served view of `FIRST` with its records, on every mode.
fn assert_serves_first(served: &Value) {
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert_eq!(served[field], resolver(FIRST), "{field}: {served:#}");
    }
    for records in [&served["records"], &served["lookup_records"]] {
        assert_eq!(records["seen_addresses"], json!(["60"]), "{served:#}");
    }
    assert_eq!(
        served["keyed"]["addr:60"],
        json!({"status": "ok", "value": OWNER}),
        "{served:#}"
    );
    assert_eq!(
        served["keyed"]["addr:61"],
        json!({"status": "not_found"}),
        "{served:#}"
    );
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

/// The misattributed fixture and the same logs as Interpret wrote them, each published one normal
/// Project run per block and read after the runs to each block of `reads` (0 is the base).
async fn stepwise(
    after: impl Fn() -> Result<Vec<Logs>>,
    misattributed: &[i64],
    reads: &[i64],
) -> Result<Vec<(Value, Value)>> {
    let mut sides = Vec::new();
    for misattributed in [misattributed, &[]] {
        let database = TestDatabase::new_migrated().await?;
        let base = interpret(&database, after()?).await?;
        for offset in misattributed {
            misattribute(&database.pool, base + offset).await?;
        }
        let mut views = Vec::new();
        let mut next = base;
        for read in reads {
            while next <= base + read {
                project(&database.pool, next, FamilyMode::Normal).await?;
                next += 1;
            }
            views.push(served(&database).await?);
        }
        database.cleanup().await?;
        sides.push(views);
    }
    let right = sides.pop().expect("reference");
    Ok(sides
        .pop()
        .expect("misattributed")
        .into_iter()
        .zip(right)
        .collect())
}

/// A stable digest of `value`, for comparing a row set across two builds in the test log.
fn digest(value: &Value) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.to_string().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// The Project rows the lookup routes and the summary serve the lease's name from, its inventory
/// dependencies left out.
async fn stored(database: &TestDatabase) -> Result<Value> {
    stored_node(database, lease_node()).await
}

/// [`stored`] for the name at `node`.
async fn stored_node(database: &TestDatabase, node: B256) -> Result<Value> {
    let logical = format!("ens:{node:#x}");
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'name', (SELECT to_jsonb(name) FROM bigname_phase.project_lookup_name name
                      WHERE name.chain_id = $1 AND name.logical_name_id = $2),
             'inventory', (SELECT jsonb_agg(to_jsonb(inventory) ORDER BY inventory.resource_id)
                           FROM bigname_phase.project_lookup_inventory inventory
                           JOIN bigname_phase.project_lookup_name name
                             ON name.chain_id = inventory.chain_id
                            AND name.record_serving_resource_id = inventory.resource_id
                           WHERE name.chain_id = $1 AND name.logical_name_id = $2),
             'records', (SELECT jsonb_agg(to_jsonb(record) ORDER BY record.record_key)
                         FROM bigname_phase.project_lookup_record record
                         JOIN bigname_phase.project_lookup_name name
                           ON name.chain_id = record.chain_id
                          AND name.record_serving_resource_id = record.resource_id
                         WHERE name.chain_id = $1 AND name.logical_name_id = $2),
             'summary', (SELECT to_jsonb(summary) FROM bigname_phase.project_name_summary summary
                         WHERE summary.chain_id = $1 AND summary.logical_name_id = $2))",
    )
    .bind(CHAIN)
    .bind(&logical)
    .fetch_one(&database.pool)
    .await?)
}

/// The names `relation=resolves_to` lists for `address` on coin 60, in the `ens` namespace or,
/// with `scope` empty, in every namespace.
async fn resolving_names_in(
    database: &TestDatabase,
    address: &str,
    scope: &str,
) -> Result<Vec<Value>> {
    let found = rows(
        database,
        &format!("/v1/addresses/{address}/names?{scope}relation=resolves_to&coin_type=60"),
    )
    .await?;
    Ok(found.iter().map(|row| row["name"].clone()).collect())
}

async fn resolving_names(database: &TestDatabase, address: &str) -> Result<Vec<Value>> {
    resolving_names_in(database, address, "namespace=ens&").await
}

/// The name's summary row is a bound name, not a serving one, and the `parent=eth` listing
/// shows it through its binding.
async fn assert_bound_and_listed(database: &TestDatabase) -> Result<()> {
    let serving: bool = sqlx::query_scalar(
        "SELECT serving FROM bigname_phase.project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(format!("ens:{:#x}", lease_node()))
    .fetch_one(&database.pool)
    .await?;
    assert!(!serving, "a lease-bound name has no serving resource");
    let listed = rows(
        database,
        "/v1/names?namespace=ens&parent=eth&expires_after=0&page_size=50",
    )
    .await?;
    assert!(
        listed.iter().any(|row| row["name"] == json!(NAME)),
        "{listed:#?}"
    );
    Ok(())
}

/// P1: the registry points the node at `NEXT` through its registry-only resource, and `NEXT`'s
/// records follow in a later block. Every name mode serves `NEXT` and its records, as the
/// same logs on the lease do, and `relation=resolves_to` lists the name under `NEXT`'s address. The name's summary stays a bound name, not a serving one, and
/// the `parent=eth` listing shows it. History and the resolver route read the same either way.
#[tokio::test]
async fn p1_a_registry_pointer_on_the_registry_only_resource_moves_every_mode() -> Result<()> {
    let after = || -> Result<Vec<Logs>> {
        Ok(vec![
            pointer(lease_node(), NEXT)?,
            next_records(lease_node())?,
        ])
    };
    let database = published(after()?, &[1]).await?;
    let wrong = served(&database).await?;
    assert_bound_and_listed(&database).await?;
    let resolver_route = ok(
        &database,
        &format!("/v1/resolvers/{SEPOLIA}/{NEXT}/records?name={NAME}&keys=addr:61"),
    )
    .await?;
    assert_eq!(
        resolver_route["data"]["records"]["addr:61"]["status"], "ok",
        "{resolver_route:#}"
    );
    let history = ok(
        &database,
        &format!("/v1/names/{NAME}/history?namespace=ens"),
    )
    .await?;
    eprintln!(
        "p1 history {}: {:#}",
        digest(&history["data"]),
        history["data"]
    );
    let next_holder = resolving_names(&database, NEXT_HOLDER).await?;
    let owner = resolving_names(&database, OWNER).await?;
    // A record written before Interpret surfaced the name keeps no name in the inverse index.
    // Such a row reaches the name through the node's registry pointer in each namespace the
    // request spans, a request with no namespace spanning every servable one.
    sqlx::query(
        "UPDATE bigname_phase.project_address_record_node_index SET logical_name_id = ''
         WHERE chain_id = $1 AND address = $2",
    )
    .bind(CHAIN)
    .bind(NEXT_HOLDER)
    .execute(&database.pool)
    .await?;
    let unnamed = resolving_names_in(&database, NEXT_HOLDER, "").await?;
    let unnamed_ens = resolving_names(&database, NEXT_HOLDER).await?;
    database.cleanup().await?;
    assert_serves_next(&wrong);
    // The name resolves to the coin-60 address only `NEXT` holds, and no longer to the one only
    // `FIRST` holds.
    assert_eq!(next_holder, [json!(NAME)]);
    assert!(owner.is_empty(), "{owner:?}");
    assert_eq!(unnamed, [json!(NAME)]);
    assert_eq!(unnamed_ens, [json!(NAME)]);

    let reference = published(after()?, &[]).await?;
    let right = served(&reference).await?;
    reference.cleanup().await?;
    assert_eq!(wrong, right);
    Ok(())
}

/// P2: the registry points back at `FIRST` through the registry-only resource. The name serves
/// `NEXT` in between and `FIRST` again after, at each step as the same logs on the lease do.
#[tokio::test]
async fn p2_moving_back_on_the_registry_only_resource_serves_the_first_resolver() -> Result<()> {
    let after = || {
        Ok(vec![
            pointer(lease_node(), NEXT)?,
            next_records(lease_node())?,
            pointer(lease_node(), FIRST)?,
        ])
    };
    let steps = stepwise(after, &[1, 3], &[2, 4]).await?;
    assert_serves_next(&steps[0].0);
    assert_serves_first(&steps[1].0);
    for (wrong, right) in steps {
        assert_eq!(wrong, right);
    }
    Ok(())
}

/// P3: `NEXT`'s records written before the pointer in the pointer's block are served: ENSv1
/// reads the resolver's storage at read time, so the pointer is no lower bound on the writes.
#[tokio::test]
async fn p3_records_written_before_the_pointer_in_its_block_are_served() -> Result<()> {
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

/// P4: the pointer and `NEXT`'s records in one block, the records after it.
#[tokio::test]
async fn p4_records_after_the_pointer_in_its_block_are_served() -> Result<()> {
    let (wrong, right) = both(
        || {
            Ok(vec![logs(vec![
                pointer(lease_node(), NEXT),
                next_records(lease_node()),
            ])?])
        },
        &[1],
    )
    .await?;
    assert_serves_next(&wrong);
    assert_eq!(wrong, right);
    Ok(())
}

/// P5: the registry's pointer cleared through the registry-only resource. The name serves no
/// resolver and no records, as a clear on the lease does, and `ens_v1.resolver` is null.
#[tokio::test]
async fn p5_a_clear_on_the_registry_only_resource_clears_the_served_resolver() -> Result<()> {
    let (wrong, right) = both(|| Ok(vec![pointer(lease_node(), ZERO)?]), &[1]).await?;
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert!(wrong[field].is_null(), "{field}: {wrong:#}");
    }
    assert_eq!(wrong, right);
    Ok(())
}

/// P6: the registry's pointer on the lease, as Interpret links it. The name serves `NEXT` and
/// stays a listed bound name, and the stored lookup and summary rows print a digest the two
/// builds can compare.
#[tokio::test]
async fn p6_a_registry_pointer_on_the_lease_serves_as_before() -> Result<()> {
    let database = published(
        vec![pointer(lease_node(), NEXT)?, next_records(lease_node())?],
        &[],
    )
    .await?;
    let view = served(&database).await?;
    assert_serves_next(&view);
    assert_bound_and_listed(&database).await?;
    let rows = stored(&database).await?;
    eprintln!(
        "p6 served {} stored {}: {rows:#}",
        digest(&view),
        digest(&rows)
    );
    database.cleanup().await
}

/// P7: a wrapped name whose registry pointer the NameWrapper's owner sets: Interpret links it to
/// the wrapper resource, and the node's registry pointer agrees.
#[tokio::test]
async fn p7_a_wrapped_name_serves_its_registry_pointer() -> Result<()> {
    let database = published(
        vec![
            wrapped()?,
            logs(vec![
                pointer(lease_node(), NEXT),
                next_records(lease_node()),
            ])?,
        ],
        &[],
    )
    .await?;
    let view = served(&database).await?;
    assert_serves_next(&view);
    database.cleanup().await
}

/// P8: a registry child below the `.eth` name with its own pointer, on its registry-only
/// resource, the child's own authority. Its registry fact is `NEXT`. The rule cannot change this
/// row: a registry-only resource's pointer and the node's registry pointer come from the same
/// registry `NewResolver`, so the two never name different resolvers. The Sepolia profile is cut
/// over, and the ENSv2 path gives a name below the reservation no resolver, so every mode
/// withholds it on either build. The composed row prints a digest the two builds can compare.
#[tokio::test]
async fn p8_a_child_with_its_own_registry_pointer_is_withheld_alike() -> Result<()> {
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
    assert_eq!(view["ens_v1_resolver"], resolver(NEXT), "{view:#}");
    assert_eq!(
        view["unresolvable_reason"], "ens_v2_path_no_resolver",
        "{view:#}"
    );
    for field in ["resolver", "lookup_resolver", "keyed_resolver"] {
        assert!(view[field].is_null(), "{field}: {view:#}");
    }
    let rows = stored_node(&database, child_node()).await?;
    eprintln!(
        "p8 served {} stored {}: {rows:#}",
        digest(&view),
        digest(&rows)
    );
    database.cleanup().await
}

/// P9: a registry child with no pointer of its own. ENSv1 has no wildcard, so the parent's
/// pointer does not make it resolve.
#[tokio::test]
async fn p9_a_child_without_its_own_registry_pointer_serves_no_resolver() -> Result<()> {
    let database = published(vec![child()?], &[]).await?;
    let view = served_name(&database, &child_name()).await?;
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert!(view[field].is_null(), "{field}: {view:#}");
    }
    database.cleanup().await
}

/// P10: an ENSv2 name whose resolver is the ENSv1 mirror serves what the mirror's registry walk
/// selects, after the node's ENSv1 registry pointer moved from one resolver to another.
#[tokio::test]
async fn p10_a_mirror_bound_name_follows_the_moved_registry_pointer() -> Result<()> {
    const CHAIN: &str = "ethereum-sepolia";
    const HASH: &str = "0xmirror";
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    const MOVED: &str = "0x2222222222222222222222222222222222222222";
    const V1_REGISTRY: &str = "0x4444444444444444444444444444444444444401";
    let database =
        v2_mirror_records_database("alice.eth", MIRROR, MirrorFixtureSource::Exact, "resolver")
            .await?;
    let node = bigname_lookup::ens_namehash_hex("alice.eth")?;
    declare_family_fixture_contract(
        &database.pool,
        "ens",
        CHAIN,
        "ens_v1_resolver_l1",
        "resolver",
        MOVED,
    )
    .await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        CHAIN,
        "alice.eth",
        MOVED,
        21000003,
        HASH,
        &[family_fixture_record_write(
            "addr:60",
            Some(json!("0x0000000000000000000000000000000000000fed")),
        )],
    )
    .await?;
    let mut moved = history_event(
        "mirror-v1-pointer-moved",
        None,
        None,
        Some(CHAIN),
        Some(21000003),
        Some(HASH),
        Some("0xmirror"),
        Some(5),
        CanonicalityState::Canonical,
    );
    moved.source_family = "ens_v1_registry_l1".into();
    moved.event_kind = "ResolverChanged".into();
    moved.before_state = json!({});
    moved.after_state = json!({"node": node, "resolver": MOVED});
    moved.raw_fact_ref = json!({"emitting_address": V1_REGISTRY});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[moved]).await?;
    rebuild_fixture_families(&database.pool, CHAIN, 21000003, HASH).await?;
    let payload =
        v2_name_record_payload_for_database(&database, "/v1/names/alice.eth/records?keys=addr:60")
            .await?;
    assert_eq!(
        payload["data"]["resolver"]["address"],
        json!(MIRROR),
        "{payload:#}"
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({"status": "ok", "value": "0x0000000000000000000000000000000000000fed"}),
        "{payload:#}"
    );
    database.cleanup().await
}

/// P11: an ENSv2 name at the mirror whose only ENSv1 registry pointer is its parent's: the walk
/// finds the ancestor's resolver, which the mirror does not extend to the name.
#[tokio::test]
async fn p11_a_mirror_bound_name_with_only_an_ancestor_pointer_stays_unsupported() -> Result<()> {
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    let payload = v2_mirror_records_payload(
        "/v1/names/alice.eth/records?keys=addr:60",
        "alice.eth",
        MIRROR,
        MirrorFixtureSource::Ancestor,
    )
    .await?;
    assert_eq!(
        payload["data"]["resolver"]["address"],
        json!(MIRROR),
        "{payload:#}"
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({"status": "unsupported", "unsupported_reason": "mirrored_resolver_not_projected"}),
        "{payload:#}"
    );
    Ok(())
}

/// P12: the pointer block and the records block published one Project block at a time. The
/// pointer block alone moves the name and its stored lookup inventory to `NEXT`, and the records
/// block fills it, at each step as the same logs on the lease do.
#[tokio::test]
async fn p12_the_pointer_block_moves_the_name_before_the_records_arrive() -> Result<()> {
    let after = || {
        Ok(vec![
            pointer(lease_node(), NEXT)?,
            next_records(lease_node())?,
        ])
    };
    let steps = stepwise(after, &[1], &[0, 1, 2]).await?;
    assert_serves_first(&steps[0].0);
    for field in [
        "resolver",
        "ens_v1_resolver",
        "lookup_resolver",
        "keyed_resolver",
    ] {
        assert_eq!(
            steps[1].0[field],
            resolver(NEXT),
            "{field}: {:#}",
            steps[1].0
        );
    }
    assert_eq!(
        steps[1].0["keyed"]["addr:61"],
        json!({"status": "not_found"}),
        "{:#}",
        steps[1].0
    );
    assert_serves_next(&steps[2].0);
    for (wrong, right) in steps {
        assert_eq!(wrong, right);
    }
    Ok(())
}

/// P13: undoing the pointer block restores `FIRST` from the undo journal, and replaying it moves
/// the name to `NEXT` again.
#[tokio::test]
async fn p13_undoing_the_pointer_block_restores_the_first_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let base = interpret(
        &database,
        vec![logs(vec![
            pointer(lease_node(), NEXT),
            next_records(lease_node()),
        ])?],
    )
    .await?;
    misattribute(&database.pool, base + 1).await?;
    project(&database.pool, base, FamilyMode::Normal).await?;
    let before = served(&database).await?;
    assert_serves_first(&before);
    let rows = stored(&database).await?;
    project(&database.pool, base + 1, FamilyMode::Normal).await?;
    assert_serves_next(&served(&database).await?);
    let undone = bigname_project::families::undo_to(&database.pool, CHAIN, base).await?;
    assert_eq!(undone, 1);
    head_at(&database.pool, base).await?;
    assert_eq!(served(&database).await?, before);
    assert_eq!(stored(&database).await?, rows);
    project(&database.pool, base + 1, FamilyMode::Normal).await?;
    assert_serves_next(&served(&database).await?);
    database.cleanup().await
}

/// P14: a full rebuild serves what the incremental runs published.
#[tokio::test]
async fn p14_a_rebuild_serves_what_the_incremental_runs_published() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let base = interpret(
        &database,
        vec![pointer(lease_node(), NEXT)?, next_records(lease_node())?],
    )
    .await?;
    misattribute(&database.pool, base + 1).await?;
    for block in base..=HEAD {
        project(&database.pool, block, FamilyMode::Normal).await?;
    }
    let incremental = served(&database).await?;
    let rows = stored(&database).await?;
    assert_serves_next(&incremental);
    project(&database.pool, HEAD, FamilyMode::Rebuild).await?;
    assert_eq!(served(&database).await?, incremental);
    assert_eq!(stored(&database).await?, rows);
    database.cleanup().await
}
