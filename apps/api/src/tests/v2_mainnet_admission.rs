//! TYR-363: the checked-in mainnet profile admits the official 2026-10-02 ENSv2 deployment. Raw
//! logs at the mainnet addresses run through Engine and Project, and the public routes serve the
//! admission and a premigrated name whose ENSv1 lease is live.
use super::v2_sepolia_redeploy::{get, lookup, token};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use axum::http::StatusCode;

sol! {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event NewResolver(bytes32 indexed node, address resolver);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
    event RegistryCreated();
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
}

mod controller {
    alloy_sol_types::sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
    }
}

const CHAIN: &str = "ethereum-mainnet";
const MAINNET: u64 = 1;
/// Above the admitted RootRegistry's start, 26183742.
const HEAD: i64 = 26_216_500;
const ROOT: &str = "0x2553c09be6447beffdb86fa622ad41fb8f77a91e";
const ROOT_START: i64 = 26_183_742;
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
/// The mainnet ENSv1 registrar's `legacy_registrar_controller`, whose `NameRegistered` carries
/// the label and makes the registration.
const CONTROLLER: &str = "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5";
/// The ENS Labs PublicResolver the mainnet ENSv1 resolver manifest declares.
pub(super) const PUBLIC_RESOLVER: &str = "0xf29100983e058b709f3d539b0c765937b804ac15";
const ETH_REGISTRY: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
const BATCH_REGISTRAR: &str = "0xf0ccfbf6dba1e01ccb833ced6d551a8f000e9721";
/// The ENSV1Resolver, the ENSv2 resolver premigration gives each reserved name.
const MIRROR_RESOLVER: &str = "0x394d8eb4a4fdf599384deab25f31d111d755c398";
pub(super) const OWNER: &str = "0x0000000000000000000000000000000000000051";
/// The RootRegistry the 2026-10-13 redeploy replaced. No manifest declares it.
const REPLACED_ROOT: &str = "0x51bbc58b3dae7d45b14fba8cf31b7c8f7904a750";
const ARCHIVED_OWNER: &str = "0x0000000000000000000000000000000000000052";
pub(super) const NAME: &str = "premigrated.eth";
pub(super) const REGISTERED: u64 = 1_700_000_000;
const RESERVED: u64 = 1_790_000_000;
const LEASE: u64 = 2_000_000_000;
/// The ETHRegistry reservation's expiry. It is below the ENSv1 lease's, so a served `expires_at`
/// of this value comes from the ENSv2 entry, neither from the lease nor from the later of the two.
const RESERVATION_EXPIRY: u64 = 1_950_000_000;
const DAY: u64 = 86_400;

pub(super) type Logs = Vec<(&'static str, alloy_primitives::LogData)>;

fn label() -> &'static str {
    NAME.strip_suffix(".eth").expect("a .eth name")
}

fn node() -> B256 {
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    keccak256([eth.as_slice(), keccak256(label().as_bytes()).as_slice()].concat())
}

/// An ENSv1 `.eth` registration through the controller, with the ENSv1 registry resolver set to
/// the PublicResolver.
pub(super) fn lease() -> Result<Logs> {
    let hash = keccak256(label().as_bytes());
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    let owner: Address = OWNER.parse()?;
    let token = U256::from_be_bytes(hash.0);
    Ok(vec![
        (
            BASE_REGISTRAR,
            Transfer {
                from: Address::ZERO,
                to: owner,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewOwner {
                node: eth,
                label: hash,
                owner,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            NameRegistered {
                id: token,
                owner,
                expires: U256::from(LEASE),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewResolver {
                node: node(),
                resolver: PUBLIC_RESOLVER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            CONTROLLER,
            controller::NameRegistered {
                name: label().into(),
                label: hash,
                owner,
                cost: U256::from(1),
                expires: U256::from(LEASE),
            }
            .encode_log_data(),
        ),
    ])
}

/// One text record on the PublicResolver.
pub(super) fn text() -> Result<Logs> {
    Ok(vec![(
        PUBLIC_RESOLVER,
        TextChanged {
            node: node(),
            indexedKey: keccak256(b"avatar"),
            key: "avatar".into(),
            value: "lease.png".into(),
        }
        .encode_log_data(),
    )])
}

/// Premigration's `batchRegister` reserves the label with no owner and the ENSV1Resolver as its
/// resolver: `LabelReserved`, then `ResolverUpdated`.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/registrar/BatchRegistrar.sol:L61-L71 @ ens_v2_mainnet_20261002@b1f97b91)
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/registry/PermissionedRegistry.sol:L498-L513 @ ens_v2_mainnet_20261002@b1f97b91)
fn reservation() -> Result<Logs> {
    let token = U256::from_be_bytes(keccak256(label().as_bytes()).0) >> 32 << 32;
    let sender: Address = BATCH_REGISTRAR.parse()?;
    Ok(vec![
        (
            ETH_REGISTRY,
            LabelReserved {
                tokenId: token,
                labelHash: keccak256(label().as_bytes()),
                label: label().into(),
                expiry: RESERVATION_EXPIRY,
                sender,
            }
            .encode_log_data(),
        ),
        (
            ETH_REGISTRY,
            ResolverUpdated {
                tokenId: token,
                resolver: MIRROR_RESOLVER.parse()?,
                sender,
            }
            .encode_log_data(),
        ),
    ])
}

/// The replaced RootRegistry announces itself and registers `eth` and a top-level `archived`,
/// as a root that was current before the redeploy would.
fn replaced_root() -> Result<Logs> {
    let owner: Address = ARCHIVED_OWNER.parse()?;
    let mut logs = vec![(REPLACED_ROOT, RegistryCreated {}.encode_log_data())];
    for label in ["eth", "archived"] {
        logs.push((
            REPLACED_ROOT,
            LabelRegistered {
                tokenId: token(label),
                labelHash: keccak256(label.as_bytes()),
                label: label.into(),
                owner,
                expiry: u64::MAX,
                sender: owner,
            }
            .encode_log_data(),
        ));
    }
    Ok(logs)
}

/// The checked-in mainnet profile synced, one Engine batch per block ending at `HEAD`, then a
/// full Project publication. Each block holds one transaction, sent to its last log's emitter.
pub(super) async fn publish(database: &TestDatabase, blocks: Vec<(u64, Logs)>) -> Result<()> {
    let pool = &database.pool;
    bigname_manifests::sync_schema_v2_repository(
        pool,
        &bigname_manifests::load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
        )?,
    )
    .await?;
    let block_hash = |block: i64| format!("{CHAIN}-block-{block}");
    let first = HEAD + 1 - blocks.len() as i64;
    let mut previous = None;
    for (offset, (time, logs)) in blocks.into_iter().enumerate() {
        let block = first + offset as i64;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number, block_timestamp,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind((offset > 0).then(|| block_hash(block - 1)))
        .bind(block)
        .bind(time as f64)
        .execute(pool)
        .await?;
        let transaction = format!("{CHAIN}-transaction-{block}");
        if let Some((to, _)) = logs.last() {
            sqlx::query(
                "INSERT INTO raw_transactions (
                     chain_id, block_hash, block_number, transaction_hash, transaction_index,
                     from_address, to_address
                 ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
            )
            .bind(CHAIN)
            .bind(block_hash(block))
            .bind(block)
            .bind(&transaction)
            .bind(OWNER)
            .bind(*to)
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
                "INSERT INTO raw_logs (
                     chain_id, block_hash, block_number, transaction_hash, transaction_index,
                     log_index, emitting_address, topics, data
                 ) VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)",
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
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(CHAIN)
    .bind(block_hash(HEAD))
    .bind(HEAD)
    .execute(pool)
    .await?;
    phase_runner::state::PhaseStore::new(pool.clone())
        .initialize_chain(CHAIN)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', started_at = now(), finished_at = now(),
             current_block_number = $2, current_block_hash = $3,
             live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
             input_content_hash = CASE phase_name WHEN 'interpret' THEN $4
                 WHEN 'project' THEN $5 END
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(block_hash(HEAD))
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .bind(bigname_content_hash::PROJECTION_CONTENT_HASH)
    .execute(pool)
    .await?;
    let input = bigname_project::families::input_token(pool, CHAIN).await?;
    let publication = bigname_project::families::apply(
        pool,
        CHAIN,
        &bigname_project::Marker {
            number: HEAD,
            hash: block_hash(HEAD),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &input,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::PROJECTION_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(publication.marker.map(|marker| marker.number), Some(HEAD));
    Ok(())
}

async fn ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = get(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

/// Rows 2 and 5 of the TYR-363 scope. The namespace reports the mainnet admission at the
/// RootRegistry's start. A name premigration reserved while its ENSv1 lease is live keeps the
/// lease's owner and registration time, takes its expiry and 28-day grace from the ENSv2 entry,
/// and is served through the ENSv1 registry's resolver. The ENSV1Resolver route walks to the same
/// records.
#[tokio::test]
async fn the_mainnet_admission_serves_a_premigrated_lease() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (REGISTERED, lease()?),
            (REGISTERED + 60, text()?),
            (RESERVED, reservation()?),
            (RESERVED + DAY, vec![]),
        ],
    )
    .await?;

    let namespace = ok(&database, "/v1/namespaces/ens").await?;
    let ethereum = namespace["data"]["networks"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|network| network["network"] == json!("ethereum"))
        .cloned()
        .with_context(|| format!("no ethereum network: {namespace:#}"))?;
    assert_eq!(
        ethereum["resolution"],
        json!({"protocol": "ens_v2", "since_block": ROOT_START}),
        "{namespace:#}"
    );
    let marker: (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT root_registry, since_block FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(marker, (Some(ROOT.to_owned()), Some(ROOT_START)));

    let detail = ok(&database, &format!("/v1/names/{NAME}?namespace=ens")).await?;
    let record = &detail["data"];
    assert!(record.get("unresolvable_reason").is_none(), "{detail:#}");
    assert_eq!(record["owner"], json!(OWNER), "{detail:#}");
    assert_eq!(
        record["registered_at"],
        REGISTERED.to_string(),
        "{detail:#}"
    );
    assert_eq!(
        record["expires_at"],
        RESERVATION_EXPIRY.to_string(),
        "{detail:#}"
    );
    assert_eq!(
        record["grace_ends_at"],
        (RESERVATION_EXPIRY + 28 * DAY).to_string(),
        "{detail:#}"
    );
    assert_eq!(
        record["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{detail:#}"
    );

    let served = json!({"text:avatar": {"status": "ok", "value": "lease.png"}});
    let name = ok(
        &database,
        &format!("/v1/names/{NAME}/records?namespace=ens&keys=text:avatar"),
    )
    .await?;
    assert_eq!(
        name["data"]["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{name:#}"
    );
    assert_eq!(name["data"]["records"], served, "{name:#}");
    let mirror = ok(
        &database,
        &format!("/v1/resolvers/{MAINNET}/{MIRROR_RESOLVER}/records?name={NAME}&keys=text:avatar"),
    )
    .await?;
    assert_eq!(mirror["data"]["records"], served, "{mirror:#}");
    database.cleanup().await
}

/// `body` with every spelling of the top-level `label` replaced by the matching spelling of
/// `neverseen`.
fn as_never_seen(body: &Value, label: &str) -> Result<Value> {
    let mut text = body.to_string();
    let node =
        |label: &str| keccak256([B256::ZERO.as_slice(), keccak256(label).as_slice()].concat());
    for (seen, never) in [
        (node(label), node("neverseen")),
        (keccak256(label.as_bytes()), keccak256(b"neverseen")),
    ] {
        text = text
            .replace(&format!("{seen:#x}"), &format!("{never:#x}"))
            .replace(&format!("{seen:x}"), &format!("{never:x}"));
    }
    Ok(serde_json::from_str(&text.replace(label, "neverseen"))?)
}

fn names(body: &Value) -> Vec<String> {
    body["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row["name"].as_str().map(str::to_owned))
        .collect()
}

/// The replaced RootRegistry's announcement is fetched, so its logs are indexed as a registry's,
/// but nothing admitted reaches it. No derived event names a name, and every route serves its
/// `archived` entry as a name never seen and lists nothing for its owner or for the registry.
#[tokio::test]
async fn the_replaced_root_registry_names_nothing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (REGISTERED - 60, replaced_root()?),
            (REGISTERED, lease()?),
            (RESERVED, reservation()?),
            (RESERVED + DAY, vec![]),
        ],
    )
    .await?;

    let (events, named): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(logical_name_id) FROM normalized_events
         WHERE chain_id = $1 AND lower(raw_fact_ref ->> 'emitting_address') = $2",
    )
    .bind(CHAIN)
    .bind(REPLACED_ROOT)
    .fetch_one(&database.pool)
    .await?;
    assert!(events > 0, "the replaced root's logs are indexed");
    assert_eq!(named, 0);

    let (never_status, never) = get(&database, "/v1/names/neverseen").await?;
    let (status, archived) = get(&database, "/v1/names/archived").await?;
    assert_eq!(
        (status, as_never_seen(&archived, "archived")?),
        (never_status, never),
        "{archived:#}"
    );
    let (_, eth) = get(&database, "/v1/names/eth?namespace=ens").await?;
    assert!(!eth.to_string().contains(ARCHIVED_OWNER), "{eth:#}");

    let mut collections = vec![
        "/v1/names?namespace=ens&expires_after=0&page_size=200".to_owned(),
        "/v1/search?q=archived&namespace=ens".to_owned(),
    ];
    for address in [ARCHIVED_OWNER, REPLACED_ROOT] {
        for relation in ["any", "role_holder", "former_owner", "resolves_to"] {
            collections.push(format!(
                "/v1/addresses/{address}/names?namespace=ens&relation={relation}"
            ));
        }
    }
    for uri in collections {
        let body = ok(&database, &uri).await?;
        assert!(
            !names(&body)
                .iter()
                .any(|name| name == "archived" || name == "eth")
                && !body.to_string().contains("archived"),
            "{uri}: {body:#}"
        );
    }
    let listing = ok(
        &database,
        "/v1/names?namespace=ens&expires_after=0&page_size=200",
    )
    .await?;
    assert!(
        names(&listing).iter().any(|name| name == NAME),
        "{listing:#}"
    );

    for profile in ["feed", "detail"] {
        let (status, answer) = lookup(
            &database,
            json!({"profile": profile, "inputs": [
                {"name": "archived"},
                {"name": "neverseen"},
                {"address": ARCHIVED_OWNER, "relation": "any"},
            ]}),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{answer:#}");
        let mut archived = answer["data"][0].clone();
        archived["input"] = Value::Null;
        let mut never = answer["data"][1].clone();
        never["input"] = Value::Null;
        assert_eq!(as_never_seen(&archived, "archived")?, never, "{answer:#}");
        let reverse = answer["data"][2].to_string();
        assert!(
            !reverse.contains("archived") && !reverse.contains("\"eth\""),
            "{answer:#}"
        );
    }
    database.cleanup().await
}
