//! A valid ENSv1 pointer does not prove that its target's execution is modeled.
use super::*;

const CUSTOM: &str = "0x0000000000000000000000000000000000000bad";

async fn summaries(database: &TestDatabase) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_agg(to_jsonb(summary) ORDER BY logical_name_id)
         FROM project_name_summary summary",
    )
    .fetch_one(&database.pool)
    .await?)
}

/// `summary` with `CHILD`'s stored `ens_v1.resolver` naming `pointer`. A registry pointer write
/// changes those bytes of the summary and no others.
fn with_pointer(summary: &Value, pointer: &str) -> Value {
    let child = bigname_storage::logical_name_id_for_name("ens", CHILD);
    let mut summary = summary.clone();
    for row in summary.as_array_mut().expect("summary rows") {
        if row["logical_name_id"] == child.as_str() {
            row["search_fields"]["ens_v1"]["resolver"] =
                json!({"chain_id": 11_155_111, "address": pointer});
        }
    }
    summary
}

async fn assert_unknown(database: &TestDatabase, resolver: Address, before: &Value) -> Result<()> {
    let detail = path_get(database, &format!("/v1/names/{CHILD}")).await?;
    assert_eq!(
        detail["data"]["resolution_unsupported_reason"], "ens_v2_path_not_projected",
        "An undeclared mirror target was incorrectly treated as a known different target: {detail:#}"
    );
    assert!(
        detail["data"]["unresolvable_reason"].is_null(),
        "{detail:#}"
    );
    for field in ["resolver", "records", "primary_address"] {
        assert!(detail["data"][field].is_null(), "{field}: {detail:#}");
    }
    for field in [
        "owner",
        "authority",
        "status",
        "expires_at",
        "grace_ends_at",
    ] {
        assert_eq!(detail["data"][field], before["data"][field], "{field}");
    }
    // `ens_v1.resolver` is the registry's pointer, so it names the custom target.
    let mut ens_v1 = before["data"]["ens_v1"].clone();
    ens_v1["resolver"] = json!({"chain_id": 11_155_111, "address": CUSTOM});
    assert_eq!(detail["data"]["ens_v1"], ens_v1, "{detail:#}");
    assert_consumers(database, false, resolver, Some("ens_v2_path_not_projected")).await
}

#[tokio::test]
async fn migrated_subname_mirror_custom_target_is_unknown_and_restoration_retains_records()
-> Result<()> {
    eprintln!(
        "TYR105 mirror fingerprint {}",
        bigname_content_hash::INTERPRETER_CONTENT_HASH
    );
    let (database, logs, resolver) = setup().await?;
    let initial: Vec<_> = logs
        .into_iter()
        .filter(|log| log.block_number <= BASE + 121)
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    assert_consumers(&database, true, resolver, None).await?;
    let before = path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    assert!(!before["data"]["owner"].is_null());
    assert!(!before["data"]["authority"].is_null());
    let summary = summaries(&database).await?;
    let families_before = replay::families(&database).await?;
    let v1: Address = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let node = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    // The retained child's owner can set any resolver address. This changes the pointer only;
    // no custom ABI, declaration or Project classification is fabricated.
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f)
    // The admitted ENSV1Resolver follows this nearest pointer for the requested child.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/ENSV1Resolver.sol:L40-L42 @ ens_v2_sepolia_20261001@07e55a05)
    let custom = emitted(
        NewResolver {
            node,
            resolver: CUSTOM.parse()?,
        }
        .encode_log_data(),
        v1,
        122,
        0,
    );
    seed_and_run_with(&database, &[custom], 122, 122, &[(122, 0, GRANTEE)], None).await?;
    let pointed = with_pointer(&summary, CUSTOM);
    assert_eq!(
        summaries(&database).await?,
        pointed,
        "pointer classification changes only the stored registry pointer, not the clocks"
    );
    assert_unknown(&database, resolver, &before).await?;
    let families_unknown = replay::families(&database).await?;
    assert_eq!(
        bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 121).await?,
        1
    );
    assert_eq!(replay::families(&database).await?, families_before);
    assert_eq!(summaries(&database).await?, summary);
    assert_consumers(&database, true, resolver, None).await?;
    publish(&database, 122).await?;
    assert_eq!(replay::families(&database).await?, families_unknown);
    assert_unknown(&database, resolver, &before).await?;
    replay::assert_rebuild(&database, 122).await?;
    assert_eq!(summaries(&database).await?, pointed);
    assert_unknown(&database, resolver, &before).await?;
    let restore = emitted(NewResolver { node, resolver }.encode_log_data(), v1, 123, 0);
    seed_and_run_with(&database, &[restore], 123, 123, &[(123, 0, GRANTEE)], None).await?;
    assert_eq!(summaries(&database).await?, summary);
    assert_consumers(&database, true, resolver, None).await?;
    replay::assert_rebuild(&database, 123).await?;
    assert_eq!(summaries(&database).await?, summary);
    assert_consumers(&database, true, resolver, None).await?;
    database.cleanup().await
}
