use super::*;
sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(bytes32 indexed node, address owner);
}

#[tokio::test]
async fn history_actions_handoff_enriches_first_current_owner_log_without_a_new_row() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let registry =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 1003).await?;
    let old = role_address(&registry, "registry_old");
    let current = role_address(&registry, "registry");
    let parent = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
    let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let logs = vec![
        emitted(
            NewOwner {
                node: parent,
                label: keccak256(LABEL),
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            old,
            120,
            0,
        ),
        emitted(
            NewOwner {
                node: parent,
                label: keccak256("current-only"),
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            current,
            120,
            1,
        ),
        emitted(
            NewOwner {
                node: parent,
                label: keccak256(LABEL),
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            current,
            121,
            0,
        ),
        emitted(
            Transfer {
                node,
                owner: GRANTEE.parse()?,
            }
            .encode_log_data(),
            current,
            121,
            1,
        ),
        emitted(
            Transfer {
                node: alloy_primitives::B256::ZERO,
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            old,
            121,
            2,
        ),
        emitted(
            Transfer {
                node: alloy_primitives::B256::ZERO,
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            current,
            121,
            3,
        ),
        emitted(
            Transfer {
                node,
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            old,
            122,
            0,
        ),
    ];
    history_v1_payments::seed_and_run(&database, CHAIN, &logs, 122).await?;
    let body = page(
        &database,
        "/v1/events?namespace=ens&kind=AuthorityTransferred,SubregistryChanged",
    )
    .await?;
    let handoffs = action_rows(&body, "registry_handoff");
    assert_eq!(handoffs.len(), 1, "{body:#}");
    let row = handoffs[0];
    assert_eq!(row["block_number"], 121);
    assert_eq!(row["log_index"], 0);
    assert_eq!(row["type"], "subregistry");
    assert_eq!(row["data"]["node"], format!("{node:#x}"));
    assert_eq!(row["data"]["owner"], HOLDER);
    assert_eq!(
        row["data"]["from_registry"],
        json!({"chain_id":1,"address":format!("{old:#x}")})
    );
    assert_eq!(
        row["data"]["to_registry"],
        json!({"chain_id":1,"address":format!("{current:#x}")})
    );
    assert!(row["data"].get("migration_path").is_none());
    let identity:String=sqlx::query_scalar("SELECT event_identity FROM normalized_events WHERE block_number=121 AND log_index=0 AND event_kind='SubregistryChanged'").fetch_one(&database.pool).await?;
    use sha2::Digest;
    assert_eq!(
        row["id"],
        hex::encode(sha2::Sha256::digest(identity.as_bytes()))
    );
    let exact = page(
        &database,
        &format!("/v1/events?contract_address={current:#x}&kind=SubregistryChanged"),
    )
    .await?;
    let matching = action_rows(&exact, "registry_handoff");
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0]["id"], row["id"]);
    database.cleanup().await
}
