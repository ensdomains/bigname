//! An unmigrated `.eth` lease outlived by its ownerless ENSv2 reservation, through Engine,
//! Project and the public routes. Premigration reserves with no owner and the continuity bonus.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L246-L268 @ ens_v2_sepolia_20261001@07e55a05)
use super::v2_sepolia_redeploy::{
    CHAIN, HEAD, NEW_REGISTRY, checked_in_profile, complete_phases, get, lookup,
};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};

const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const PROXY: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const IMPLEMENTATION: &str = "0x24e1d8e068620b647ca097f961a61055f4f42d72";
const OWNER: &str = "0x0000000000000000000000000000000000000051";
const NAME: &str = "continuinglease.eth";
const LEASE: u64 = 2_000_000_000;
const EXTENDED: u64 = LEASE + 92 * 86_400;
const GRACE: u64 = EXTENDED + 28 * 86_400;
sol! {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event Upgraded(address indexed implementation);
}

#[tokio::test]
async fn released_continuing_lease_keeps_its_former_holder_on_public_routes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let pool = &database.pool;
    bigname_manifests::sync_schema_v2_repository(
        pool,
        &bigname_manifests::load_repository(checked_in_profile())?,
    )
    .await?;
    let block_hash = |block: i64| format!("{CHAIN}-block-{block}");
    // The last block carries no log: the lease passes its 90-day grace by the clock alone,
    // one second before the extended reservation expires.
    let times = [LEASE - 100, LEASE - 90, LEASE - 80, EXTENDED - 1];
    let first = HEAD - 3;
    for (offset, time) in times.into_iter().enumerate() {
        let block = first + offset as i64;
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
            VALUES($1,$2,$3,$4,to_timestamp($5),'canonical')")
            .bind(CHAIN).bind(block_hash(block)).bind((offset > 0).then(|| block_hash(block - 1)))
            .bind(block).bind(time as f64).execute(pool).await?;
    }
    let label = NAME.strip_suffix(".eth").unwrap();
    let hash = keccak256(label.as_bytes());
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    let owner: Address = OWNER.parse()?;
    let lease_token = U256::from_be_bytes(hash.0);
    let token = lease_token >> 32 << 32;
    let blocks = [
        vec![
            (
                BASE_REGISTRAR,
                Transfer {
                    from: Address::ZERO,
                    to: owner,
                    tokenId: lease_token,
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
                    id: lease_token,
                    owner,
                    expires: U256::from(LEASE),
                }
                .encode_log_data(),
            ),
            (
                NEW_REGISTRY,
                LabelReserved {
                    tokenId: token,
                    labelHash: hash,
                    label: label.into(),
                    expiry: LEASE + 62 * 86_400,
                    sender: owner,
                }
                .encode_log_data(),
            ),
        ],
        vec![(
            PROXY,
            Upgraded {
                implementation: IMPLEMENTATION.parse()?,
            }
            .encode_log_data(),
        )],
        vec![(
            NEW_REGISTRY,
            ExpiryUpdated {
                tokenId: token,
                newExpiry: EXTENDED,
                sender: owner,
            }
            .encode_log_data(),
        )],
        vec![],
    ];
    let mut previous = None;
    for (offset, logs) in blocks.into_iter().enumerate() {
        let block = first + offset as i64;
        let transaction = format!("{CHAIN}-transaction-{block}");
        if !logs.is_empty() {
            sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address)
                VALUES($1,$2,$3,$4,0,$5,$6)")
                .bind(CHAIN).bind(block_hash(block)).bind(block).bind(&transaction).bind(OWNER).bind(NEW_REGISTRY).execute(pool).await?;
        }
        for (index, (emitter, data)) in logs.into_iter().enumerate() {
            let topics: Vec<String> = data
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect();
            sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data)
                VALUES($1,$2,$3,$4,0,$5,$6,$7,$8)")
                .bind(CHAIN).bind(block_hash(block)).bind(block).bind(&transaction).bind(index as i64).bind(emitter).bind(topics).bind(data.data.as_ref()).execute(pool).await?;
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
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(publication.marker.map(|marker| marker.number), Some(HEAD));

    let (status, detail) = get(&database, &format!("/v1/names/{NAME}?namespace=ens")).await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let (status, looked_up) = lookup(
        &database,
        json!({"profile":"detail","inputs":[{"name":NAME}]}),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{looked_up}");
    for record in [&detail["data"], &looked_up["data"][0]["record"]] {
        // The reservation's own schedule is still running and grants nobody the name.
        assert_eq!(record["status"], "active", "{record}");
        assert_eq!(record["expires_at"], EXTENDED.to_string(), "{record}");
        assert_eq!(record["grace_ends_at"], GRACE.to_string(), "{record}");
        assert!(record["registration_id"].is_string(), "{record}");
        assert_eq!(record["registered_at"], times[0].to_string(), "{record}");
        for current in ["owner", "manager"] {
            assert!(record.get(current).is_none(), "{current}: {record}");
        }
        let lapsed = &record["lapsed_registration"];
        assert_eq!(lapsed["owner"], OWNER, "{record}");
        assert_eq!(lapsed["held_through"], "registrar", "{record}");
        assert_eq!(lapsed["release_kind"], "expired", "{record}");
        assert_eq!(lapsed["released_at"], times[3].to_string(), "{record}");
    }
    let listed = |body: &Value| {
        body["data"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["name"] == NAME))
    };
    let (status, former) = get(
        &database,
        &format!("/v1/addresses/{OWNER}/names?namespace=ens&relation=former_owner"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{former}");
    assert!(listed(&former), "former holder lost the name: {former}");
    let (status, current) = get(
        &database,
        &format!("/v1/addresses/{OWNER}/names?namespace=ens&relation=owner,manager"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{current}");
    assert!(!listed(&current), "released lease still owned: {current}");
    database.cleanup().await
}
