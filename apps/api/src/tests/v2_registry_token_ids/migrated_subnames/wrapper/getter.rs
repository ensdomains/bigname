//! The original ENSv1 getter controls eligibility; the rebound node supplies the record.
use super::*;

async fn transferred_original_owner(recipient: &str, expected: bool, reason: &str) -> Result<()> {
    eprintln!(
        "TYR105 getter fingerprint {}",
        bigname_content_hash::INTERPRETER_CONTENT_HASH
    );
    let (database, resolver) = unwrapped_rebound().await?;
    assert_name_consumers(&database, NEW_CHILD, GRANTEE, true, resolver, None).await?;
    let before = replay::families(&database).await?;
    let original: B256 = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    // ENSRegistry.setOwner has no recipient callback. Graveyard is a literal nonzero getter;
    // only assigning the registry's own address makes this getter zero-equivalent.
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L68 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
    let transfer = transaction(
        125,
        0,
        vec![(
            V1.parse()?,
            registry::Transfer {
                node: original,
                owner: recipient.parse()?,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(&database, &transfer, 125, 125, &[(125, 0, GRANTEE)], None).await?;
    let event: Value = sqlx::query_scalar(
        "SELECT to_jsonb(event) FROM project_registry_owner_event event
         WHERE chain_id=$1 AND node=$2 AND source_family='ens_v1_registry_l1'
         ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1",
    )
    .bind(PATH_CHAIN)
    .bind(format!("{original:#x}"))
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        event["owner_getter"],
        if expected {
            recipient
        } else {
            "0x0000000000000000000000000000000000000000"
        }
    );
    assert_eq!(event["owner_getter_reason"], reason, "{event:#}");
    let original_detail = path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    assert!(
        original_detail["data"]["owner"].is_null(),
        "public owner presentation must remain empty: {original_detail:#}"
    );
    let rebound = path_get(&database, &format!("/v1/names/{NEW_CHILD}")).await?;
    if expected {
        assert_eq!(
            rebound["data"]["primary_address"], GRANTEE,
            "Nonzero Graveyard getter incorrectly withdrew the rebound requested-node record: {rebound:#}"
        );
    } else {
        assert_eq!(
            rebound["data"]["unresolvable_reason"], "ens_v2_path_no_resolver",
            "{rebound:#}"
        );
    }
    assert_name_consumers(&database, NEW_CHILD, GRANTEE, expected, resolver, None).await?;
    let after = replay::families(&database).await?;
    assert_eq!(
        bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 124).await?,
        1
    );
    assert_eq!(
        replay::families(&database).await?,
        before,
        "owner block undo"
    );
    assert_name_consumers(&database, NEW_CHILD, GRANTEE, true, resolver, None).await?;
    publish(&database, 125).await?;
    assert_eq!(
        replay::families(&database).await?,
        after,
        "owner block reapply"
    );
    assert_name_consumers(&database, NEW_CHILD, GRANTEE, expected, resolver, None).await?;
    replay::assert_rebuild(&database, 125).await?;
    assert_name_consumers(&database, NEW_CHILD, GRANTEE, expected, resolver, None).await?;
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_pro_graveyard_getter_retains_rebound_record() -> Result<()> {
    transferred_original_owner(GRAVE, true, "graveyard").await
}

#[tokio::test]
async fn migrated_subname_pro_registry_self_getter_withdraws_rebound_record() -> Result<()> {
    transferred_original_owner(V1, false, "registry_self").await
}
