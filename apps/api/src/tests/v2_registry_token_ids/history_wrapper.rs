use super::compatibility::{admit_family_from, role_address};
use super::*;
sol! {event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);}

#[tokio::test]
async fn v2_history_wrapper_single_and_batch_keep_only_retained_operators() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let wrapper = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 980).await?;
    let wrapper_address = role_address(&wrapper, "name_wrapper");
    let node: alloy_primitives::B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let id = U256::from_be_bytes(*node);
    let owner = HOLDER.parse()?;
    let recipient = GRANTEE.parse()?;
    let operator: Address = "0x00000000000000000000000000000000000000ab".parse()?;
    let mut dns = vec![LABEL.len() as u8];
    dns.extend_from_slice(LABEL.as_bytes());
    dns.extend_from_slice(b"\x03eth\x00");
    let mut logs = vec![
        raw(
            TransferSingle {
                operator,
                from: Address::ZERO,
                to: owner,
                id,
                value: U256::from(1),
            }
            .encode_log_data(),
            120,
            0,
        ),
        raw(
            NameWrapped {
                node,
                name: dns.into(),
                owner,
                fuses: 0,
                expiry: 1_900_000_000,
            }
            .encode_log_data(),
            120,
            1,
        ),
        raw(
            TransferSingle {
                operator,
                from: owner,
                to: recipient,
                id,
                value: U256::from(1),
            }
            .encode_log_data(),
            121,
            0,
        ),
        raw(
            TransferBatch {
                operator,
                from: recipient,
                to: owner,
                ids: vec![id],
                values: vec![U256::from(1)],
            }
            .encode_log_data(),
            122,
            0,
        ),
        raw(
            TransferSingle {
                operator,
                from: owner,
                to: recipient,
                id,
                value: U256::ZERO,
            }
            .encode_log_data(),
            122,
            1,
        ),
    ];
    for log in &mut logs {
        log.emitting_address = format!("{wrapper_address:#x}");
    }
    super::history_v1_payments::seed_and_run(&database, CHAIN, &logs, 122).await?;
    let retained:Vec<(i64,Value)>=sqlx::query_as("SELECT block_number,after_state FROM normalized_events WHERE event_kind='TokenControlTransferred' ORDER BY block_number,log_index").fetch_all(&database.pool).await?;
    assert_eq!(retained.len(), 3, "{retained:#?}");
    assert!(retained[0].1.get("operator").is_none());
    for (_, after) in retained.iter().skip(1) {
        assert_eq!(after["operator"], format!("{operator:#x}"));
    }
    for route in [
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/events?contract_address={wrapper_address:#x}"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let (status, body) = read_family_response(
            &database,
            &format!("{route}&include=data,raw&order=asc&page_size=200"),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        let rows = body["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["kind"] == "TokenControlTransferred")
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 3, "{body:#}");
        for row in rows {
            if row["block_number"] == 120 {
                assert!(row["data"].get("operator").is_none());
            } else {
                assert_eq!(row["data"]["operator"], format!("{operator:#x}"));
            }
            assert!(row["data"].get("canonical_id").is_none());
            assert!(row["data"].get("token_id").is_none());
        }
    }
    database.cleanup().await
}
