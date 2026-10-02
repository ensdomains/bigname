//! TYR-183 through the public routes: after the checked-in Sepolia profile replaces the
//! 2026-09-15 ENSv2 deployment and the Interpret redo and Project run, a name only the dropped
//! registry named serves like a name never seen. The setup mirrors
//! apps/phase-runner/tests/production_interpret/sepolia_redeploy.rs.
use super::*;
use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_interpret::{BatchRequest, Engine, RunMode};

sol! {
    event Upgraded(address indexed implementation);
    event RegistryCreated();
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
}

const CHAIN: &str = "ethereum-sepolia";
const FIRST: i64 = 11_709_000;
const HEAD: i64 = 11_821_700;
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const CLIENT_PROXY: &str = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";
const MANAGED_PROXY: &str = "0x6d80F2172CFdEc5730fE683860C33d26fC42e6F1";
const OLD_REGISTRY: &str = "0x657ea849311d3d5823348dded7c2aaafb3ede09e";
const NEW_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const OLD_UNIVERSAL_RESOLVER: &str = "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3";
const NEW_UNIVERSAL_RESOLVER: &str = "0x24e1d8e068620b647ca097f961a61055f4f42d72";

/// (2026-10-01 address, 2026-09-15 address, 2026-10-01 start, 2026-09-15 start).
const REDEPLOYED: [(&str, &str, Option<u64>, Option<u64>); 15] = [
    (
        "0xb458d6a3a77919449d03e7a6903c26827c1ec43f",
        "0x9703dbd26dab89504490994138cf2c575251a9ce",
        Some(11_820_291),
        Some(11_708_988),
    ),
    (
        NEW_REGISTRY,
        OLD_REGISTRY,
        Some(11_820_399),
        Some(11_709_066),
    ),
    (
        "0xf633e7fc17e2bbe0d0965d18ec1821dcb754a3d3",
        "0xabe76f6c8dfced81aa5a2bb8034202a7136b94ca",
        Some(11_820_440),
        Some(11_709_083),
    ),
    (
        "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0",
        "0xb2bf4a9a86d29661ea93223582b9945943931e42",
        Some(11_820_288),
        Some(11_708_986),
    ),
    (
        "0xdc4a563d00f5c3012b699794eb9e13a561be386f",
        "0xd7e590ad0e92a6ac1d81f4483a9b951d3585a50f",
        Some(11_820_448),
        Some(11_709_089),
    ),
    (
        "0x2a35b94df22cc7354570be2284655e2cdc0e64a2",
        "0x7ed171bb143a905f56105e4ea146543ecb122f55",
        Some(11_820_436),
        Some(11_709_081),
    ),
    (
        "0x6029a063d69b09d23c52a754a90e4fe43adac3a8",
        "0xab1b57c6ee5e91e6090595c0af14cb9b8bc7773f",
        Some(11_820_452),
        Some(11_709_093),
    ),
    (
        "0xb58a90a39d13cce1d0e192b5da5c47640855b04d",
        "0x950b93885b33ce4c7e8571be2c88a1aa93d82f49",
        Some(11_820_435),
        Some(11_709_080),
    ),
    (
        "0x4a4c8b7cdab6b19dc2cdb417cdb53a2ccbaf5322",
        "0xbe68ff9afc7d5a1864ffef5c82de0a1c13e6b529",
        Some(11_820_450),
        Some(11_709_091),
    ),
    (
        "0xf2ece44980778966b8a0fccb3a9e339440f6e045",
        "0xd06e726e9bd8ac0f33a2a45f4cc28fe10d656a36",
        Some(11_820_442),
        Some(11_709_084),
    ),
    (
        "0xda70306c98e97ece36f997a21368e53298572991",
        "0x9e726eb570beb6bceb495ab8cda7df517d4e841c",
        Some(11_820_318),
        Some(11_708_995),
    ),
    (
        "0xa8f86ee5cdd28703bd876f3a8c10b1de70f36899",
        "0x58d12d60471b98f191856e4c2d56886e9c3ea573",
        Some(11_820_455),
        Some(11_709_095),
    ),
    (
        "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880",
        "0x2741543c3b14640b97bc70a233318032f7e35bac",
        Some(11_820_449),
        Some(11_709_090),
    ),
    (
        "0x115eb53f0c60696633855f90b138178fb40b2b2c",
        "0x14f09fd05d4585759e54844dc9b00147131cf243",
        None,
        None,
    ),
    (NEW_UNIVERSAL_RESOLVER, OLD_UNIVERSAL_RESOLVER, None, None),
];

const REDEPLOYED_FAMILIES: [&str; 6] = [
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_registrar_l1",
    "ens_v2_resolver_l1",
    "ens_v2_migration_l1",
    "ens_execution",
];

fn checked_in_profile() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia")
}

fn copy_dir(source: &std::path::Path, target: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            copy_dir(&path, &target.join(entry.file_name()))?;
        } else {
            std::fs::copy(&path, target.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn previous_profile() -> Result<std::path::PathBuf> {
    let root =
        std::env::temp_dir().join(format!("bigname-api-sepolia-20260915-{}", Uuid::new_v4()));
    copy_dir(&checked_in_profile(), &root)?;
    for family in REDEPLOYED_FAMILIES {
        let directory = root.join("ethereum/ens").join(family);
        let mut manifest = std::fs::read_to_string(directory.join("v2.toml"))?
            .replacen("manifest_version = 2", "manifest_version = 1", 1)
            .replacen(
                "deployment_epoch = \"ens_v2_sepolia_20261001\"",
                "deployment_epoch = \"ens_v2_sepolia_20260915\"",
                1,
            );
        for (new, old, new_start, old_start) in REDEPLOYED {
            manifest = manifest.replace(new, old);
            if let (Some(new_start), Some(old_start)) = (new_start, old_start) {
                manifest = manifest.replace(
                    &format!("start_block = {new_start}"),
                    &format!("start_block = {old_start}"),
                );
            }
        }
        std::fs::remove_file(directory.join("v2.toml"))?;
        std::fs::write(directory.join("v1.toml"), manifest)?;
    }
    Ok(root)
}

fn block_hash(number: i64) -> String {
    format!("{CHAIN}-block-{number}")
}

fn raw_namehash(labels: &[&str]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(node.as_slice());
        input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        keccak256(input)
    })
}

fn token(label: &str) -> U256 {
    let mut token = *keccak256(label.as_bytes());
    token[28..].copy_from_slice(&0_u32.to_be_bytes());
    U256::from_be_bytes(token)
}

fn reservation(label: &str, expiry: u64) -> Result<alloy_primitives::LogData> {
    Ok(LabelReserved {
        tokenId: token(label),
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        expiry,
        sender: SENDER.parse()?,
    }
    .encode_log_data())
}

async fn seed_both_generations(pool: &PgPool) -> Result<()> {
    let upgraded = |implementation: &str| -> Result<alloy_primitives::LogData> {
        Ok(Upgraded {
            implementation: implementation.parse()?,
        }
        .encode_log_data())
    };
    let registration = LabelRegistered {
        tokenId: token("kept"),
        labelHash: keccak256(b"kept"),
        label: "kept".to_owned(),
        owner: SENDER.parse()?,
        expiry: 1_900_000_000,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    let resource = TokenResource {
        tokenId: token("kept"),
        resource: U256::from(7_001),
    }
    .encode_log_data();
    let facts: [(i64, &str, alloy_primitives::LogData); 10] = [
        (FIRST, CLIENT_PROXY, upgraded(MANAGED_PROXY)?),
        (
            11_709_066,
            OLD_REGISTRY,
            RegistryCreated {}.encode_log_data(),
        ),
        (
            11_709_797,
            OLD_REGISTRY,
            reservation("nick", 1_801_817_044)?,
        ),
        (11_710_193, MANAGED_PROXY, upgraded(OLD_UNIVERSAL_RESOLVER)?),
        (
            11_820_399,
            NEW_REGISTRY,
            RegistryCreated {}.encode_log_data(),
        ),
        (
            11_821_474,
            NEW_REGISTRY,
            reservation("nick", 1_803_965_433)?,
        ),
        (11_821_680, MANAGED_PROXY, upgraded(NEW_UNIVERSAL_RESOLVER)?),
        (
            11_821_690,
            OLD_REGISTRY,
            reservation("later", 1_900_000_000)?,
        ),
        (11_821_695, OLD_REGISTRY, registration),
        (11_821_696, OLD_REGISTRY, resource),
    ];
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         )
         SELECT $1, $1 || '-block-' || height::text,
                CASE WHEN height > $2 THEN $1 || '-block-' || (height - 1)::text END,
                height, to_timestamp(height), 'canonical'::canonicality_state
         FROM generate_series($2::bigint, $3::bigint) AS height",
    )
    .bind(CHAIN)
    .bind(FIRST)
    .bind(HEAD)
    .execute(pool)
    .await?;
    for (number, emitter, fact) in facts {
        let transaction = format!("{CHAIN}-transaction-{number}");
        sqlx::query(
            "INSERT INTO raw_transactions (
                 chain_id, block_hash, block_number, transaction_hash, transaction_index,
                 from_address, to_address
             ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind(number)
        .bind(&transaction)
        .bind(SENDER)
        .bind(emitter)
        .execute(pool)
        .await?;
        let topics: Vec<String> = fact
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect();
        sqlx::query(
            "INSERT INTO raw_logs (
                 chain_id, block_hash, block_number, transaction_hash, transaction_index,
                 log_index, emitting_address, topics, data
             ) VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind(number)
        .bind(&transaction)
        .bind(emitter)
        .bind(topics)
        .bind(fact.data.as_ref())
        .execute(pool)
        .await?;
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
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'intake', 'drpc', 'ethereum_head', 0, $2 + 1, $2, $2, $3)",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(block_hash(HEAD))
    .execute(pool)
    .await?;
    Ok(())
}

async fn interpret_and_project(pool: &PgPool, mode: RunMode) -> Result<()> {
    let engine = Engine::new(pool.clone())
        .with_blocks_per_batch(std::num::NonZeroU32::new(200_000).expect("non-zero"));
    let mut resume_current = None;
    loop {
        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: FIRST,
                to_block: HEAD,
                resume_current,
                mode,
            })
            .await?;
        if outcome.complete {
            break;
        }
        resume_current = Some(outcome.current);
    }
    let token = bigname_project::families::input_token(pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        pool,
        CHAIN,
        &bigname_project::Marker {
            number: HEAD,
            hash: block_hash(HEAD),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        )
        .with_max_blocks_per_run(200_000),
    )
    .await?;
    anyhow::ensure!(
        !outcome.budget_exhausted
            && outcome.marker.as_ref().map(|marker| marker.number) == Some(HEAD),
        "{outcome:?}"
    );
    Ok(())
}

/// The public namespaces come from the synced manifests: the test override maps `ens` to Mainnet.
async fn send(database: &TestDatabase, request: Request<Body>) -> Result<(StatusCode, Value)> {
    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(request)
    .await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

async fn get(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    send(database, Request::builder().uri(uri).body(Body::empty())?).await
}

async fn lookup(database: &TestDatabase, body: Value) -> Result<(StatusCode, Value)> {
    send(
        database,
        Request::builder()
            .method("POST")
            .uri("/v1/lookup")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body)?))?,
    )
    .await
}

/// `body` with every spelling of `label`.eth replaced by the matching spelling of `neverseen.eth`.
fn as_never_seen(body: &Value, label: &str) -> Result<Value> {
    let mut text = body.to_string();
    for (seen, never) in [
        (
            raw_namehash(&[label, "eth"]),
            raw_namehash(&["neverseen", "eth"]),
        ),
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

const DROPPED: [&str; 2] = ["later", "kept"];
const LISTING: &str = "/v1/names?namespace=ens&expires_after=0&page_size=200";

fn address_name_routes() -> Vec<String> {
    [SENDER, OLD_REGISTRY, NEW_REGISTRY]
        .into_iter()
        .flat_map(|address| {
            ["any", "role_holder", "former_registrant", "resolves_to"].map(|relation| {
                format!("/v1/addresses/{address}/names?namespace=ens&relation={relation}")
            })
        })
        .collect()
}

fn lookup_body(profile: &str) -> Value {
    json!({"profile": profile, "inputs": [
        {"name": "later.eth"},
        {"name": "kept.eth"},
        {"name": "neverseen.eth"},
        {"address": SENDER, "relation": "any"},
    ]})
}

/// `later.eth` is the old registry's reservation after the repoint, as in the phase-runner test;
/// `kept.eth` is a registration with an owner, so the collections serve it before the swap.
#[tokio::test]
async fn sepolia_redeploy_serves_names_only_the_dropped_registry_named_as_never_seen() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let pool = &database.pool;
    let previous = previous_profile()?;
    bigname_manifests::sync_schema_v2_repository(
        pool,
        &bigname_manifests::load_repository(&previous)?,
    )
    .await?;
    std::fs::remove_dir_all(&previous)?;
    seed_both_generations(pool).await?;
    phase_runner::state::PhaseStore::new(pool.clone())
        .initialize_chain(CHAIN)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', started_at = now(), finished_at = now(),
             current_block_number = $2, current_block_hash = $3,
             input_content_hash = CASE WHEN phase_name IN ('interpret', 'project') THEN $4 END
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(block_hash(HEAD))
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    interpret_and_project(pool, RunMode::Normal).await?;

    for label in DROPPED {
        let (status, body) = get(&database, &format!("/v1/names/{label}.eth")).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "before the swap {label}.eth: {body:#}"
        );
    }
    let kept = vec!["kept.eth".to_owned()];
    for uri in [
        LISTING.to_owned(),
        "/v1/search?q=kept&namespace=ens".to_owned(),
        format!("/v1/addresses/{SENDER}/names?namespace=ens&relation=any"),
    ] {
        let (status, body) = get(&database, &uri).await?;
        assert_eq!(
            (status, names(&body)),
            (StatusCode::OK, kept.clone()),
            "before the swap {uri}: {body:#}"
        );
    }
    let (status, answer) = lookup(&database, lookup_body("feed")).await?;
    assert_eq!(status, StatusCode::OK, "{answer:#}");
    assert_eq!(
        answer["data"][1]["record"]["name"], "kept.eth",
        "{answer:#}"
    );
    assert!(
        answer["data"][3].to_string().contains("kept.eth"),
        "{answer:#}"
    );

    bigname_manifests::sync_schema_v2_repository(
        pool,
        &bigname_manifests::load_repository(checked_in_profile())?,
    )
    .await?;
    interpret_and_project(pool, RunMode::Redo).await?;

    let (status, nick) = get(&database, "/v1/names/nick.eth").await?;
    assert_eq!(status, StatusCode::OK, "{nick:#}");
    assert_eq!(nick["data"]["created_at"], json!("11821474"), "{nick:#}");
    assert_eq!(nick["data"]["expires_at"], json!("1803965433"), "{nick:#}");

    let (never_status, never) = get(&database, "/v1/names/neverseen.eth").await?;
    for label in DROPPED {
        let (status, body) = get(&database, &format!("/v1/names/{label}.eth")).await?;
        assert_eq!(
            (status, as_never_seen(&body, label)?),
            (never_status, never.clone()),
            "GET /v1/names/{label}.eth: {body:#}"
        );
    }

    let mut collections = vec![LISTING.to_owned()];
    collections.extend(DROPPED.map(|label| format!("/v1/search?q={label}&namespace=ens")));
    collections.extend(address_name_routes());
    for uri in collections {
        let (status, body) = get(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
        assert!(
            DROPPED
                .iter()
                .all(|label| !body.to_string().contains(&format!("{label}.eth"))),
            "{uri}: {body:#}"
        );
    }

    for profile in ["feed", "detail"] {
        let (status, answer) = lookup(&database, lookup_body(profile)).await?;
        assert_eq!(status, StatusCode::OK, "{answer:#}");
        let mut never = answer["data"][2].clone();
        never["input"] = Value::Null;
        for (index, label) in DROPPED.into_iter().enumerate() {
            let mut result = answer["data"][index].clone();
            result["input"] = Value::Null;
            assert_eq!(
                as_never_seen(&result, label)?,
                never,
                "POST /v1/lookup profile={profile} {label}.eth: {answer:#}"
            );
        }
        assert!(
            DROPPED.iter().all(|label| !answer["data"][3]
                .to_string()
                .contains(&format!("{label}.eth"))),
            "POST /v1/lookup profile={profile} reverse {SENDER}: {answer:#}"
        );
    }

    database.cleanup().await
}
