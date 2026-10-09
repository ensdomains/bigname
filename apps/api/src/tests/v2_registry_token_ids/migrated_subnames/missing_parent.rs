//! A missing parent on a chain whose profile admits an ENSv2 root registry, without a
//! reservation or synthetic state deletion.
use super::*;

#[tokio::test]
async fn migrated_subname_actual_producer_missing_parent_has_no_live_entry() -> Result<()> {
    eprintln!(
        "TYR105 missing-parent fingerprint {}",
        bigname_content_hash::INTERPRETER_CONTENT_HASH
    );
    let (database, mut logs, resolver) = setup().await?;
    // Keep the root eth deployment and ENSv1 parent/child logs. Omit the whole parent
    // reservation transaction (LabelReserved + mirror ResolverUpdated). The Sepolia profile
    // admits the root registry, so the chain is cut over before the Universal Resolver proxy
    // upgrades. That upgrade is replayed last and must move nothing.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L39-L51 @ ens_v2_sepolia_20261001@07e55a05)
    let mut upgrade = logs
        .iter()
        .find(|log| log.block_number == BASE + 120 && log.transaction_index == 0)
        .context("Universal Resolver upgrade")?
        .clone();
    upgrade.block_number = BASE + 122;
    upgrade.block_hash = format!("0xhistory{}", BASE + 122);
    upgrade.transaction_hash = format!("0x{:064x}", (BASE + 122) * 100);
    logs.retain(|log| {
        log.block_number <= BASE + 121
            && !(log.block_number == BASE + 120 && matches!(log.transaction_index, 0 | 4))
    });
    logs.extend(transaction(
        120,
        5,
        vec![(
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?,
            NewResolver {
                node: bigname_lookup::ens_namehash_hex(NAME)?.parse()?,
                resolver,
            }
            .encode_log_data(),
        )],
    ));
    seed_and_run(&database, &logs, 120, 121).await?;
    assert_consumers(&database, false, resolver, None).await?;
    let parent_before = path_get(&database, &format!("/v1/names/{NAME}")).await?;
    let child_before = path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    assert_eq!(parent_before["data"]["authority"], "ens_v1");
    assert_eq!(child_before["data"]["authority"], "ens_v1");
    assert_eq!(child_before["data"]["owner"], GRANTEE);
    for before in [&parent_before, &child_before] {
        assert_eq!(
            before["data"]["unresolvable_reason"], "no_live_ens_v2_entry",
            "{before:#}"
        );
        assert!(
            before["data"]["resolution_unsupported_reason"].is_null(),
            "{before:#}"
        );
        for field in ["resolver", "records", "primary_address"] {
            assert!(before["data"][field].is_null(), "{field}: {before:#}");
        }
        for field in ["owner", "registration_id"] {
            assert!(before["data"][field].is_string(), "{field}: {before:#}");
        }
    }
    assert!(parent_before["data"]["expires_at"].is_string());

    let entries: Vec<(String, String)> = sqlx::query_as(
        "SELECT registry, status FROM project_ens_v2_entry_owner ORDER BY registry, entry_key",
    )
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(
        entries,
        vec![(
            "0xb458d6a3a77919449d03e7a6903c26827c1ec43f".into(),
            "registered".into()
        )],
        "only the deployed root eth entry exists; no parent reservation"
    );
    let root_subregistry: String = sqlx::query_scalar(
        "SELECT after_state->>'subregistry' FROM normalized_events
         WHERE event_kind='SubregistryChanged' AND source_family='ens_v2_root_l1'
           AND consumer_visibility='activated'
         ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        root_subregistry,
        "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4"
    );

    seed_and_run(&database, &[upgrade], 122, 122).await?;
    assert_consumers(&database, false, resolver, None).await?;
    for (name, before) in [(NAME, parent_before), (CHILD, child_before)] {
        let after = path_get(&database, &format!("/v1/names/{name}")).await?;
        assert_eq!(
            after["data"], before["data"],
            "a Universal Resolver upgrade moved {name}"
        );
    }
    database.cleanup().await
}
