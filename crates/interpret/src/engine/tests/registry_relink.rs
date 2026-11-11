//! A registry on a named path links a registered token to its resource with `TokenResource`, then
//! emits `TokenResource` for the same token and resource again in a later block. The second link
//! binds the same name to the same resource, so Interpret must write it in a later batch, after
//! its adapter state is restored, as it would in one batch. The deployed registry emits
//! `TokenResource` once per token, in `register`; a registry with its own bytecode mounted on a
//! named path can emit it again.
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L472 @ ens_v2@a971bd64)
use super::*;

const FIRST: i64 = SETUP_BLOCK;
const SECOND: i64 = SETUP_BLOCK + 1;
/// Far beyond every block timestamp here, which is the block number in seconds.
const LIVE_EXPIRY: u64 = 1_900_000_000;
const LABEL: &str = "relinked";

/// The label's token id at version 0, which is also its resource.
fn token() -> U256 {
    let mut word = *keccak256(LABEL.as_bytes());
    word[28..].copy_from_slice(&0_u32.to_be_bytes());
    U256::from_be_bytes(word)
}

fn owner() -> Address {
    OWNER.parse().expect("the owner is an address")
}

fn link() -> alloy_primitives::LogData {
    TokenResource {
        tokenId: token(),
        resource: token(),
    }
    .encode_log_data()
}

async fn insert_block(
    pool: &PgPool,
    block: i64,
    logs: Vec<alloy_primitives::LogData>,
) -> TestResult {
    insert_transaction(pool, block, ETH_REGISTRY).await?;
    for (index, log) in logs.into_iter().enumerate() {
        insert_log(pool, block, index as i64, ETH_REGISTRY, log).await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_repeated_link_in_a_later_batch_is_written() -> TestResult {
    let database = database("interpret_registry_relink").await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;
    seed_lineage(pool).await?;
    insert_block(
        pool,
        FIRST,
        vec![
            LabelRegistered {
                tokenId: token(),
                labelHash: keccak256(LABEL.as_bytes()),
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
                id: token(),
                value: U256::from(1),
            }
            .encode_log_data(),
            link(),
        ],
    )
    .await?;
    insert_block(pool, SECOND, vec![link()]).await?;

    let mut resume = None;
    for block in [FIRST, SECOND] {
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: block,
                to_block: block,
                resume_current: resume.take(),
                mode: RunMode::Normal,
            })
            .await
            .map_err(|error| {
                format!(
                    "Interpret refused block {block} ({:?}): {error}",
                    error.kind()
                )
            })?;
        resume = Some(Marker {
            number: block,
            hash: block_hash(block),
        });
    }

    let links: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'TokenResourceLinked'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    assert_eq!(links, 2, "both links are interpreted");
    database.cleanup().await?;
    Ok(())
}
