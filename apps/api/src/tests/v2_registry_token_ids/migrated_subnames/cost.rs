//! Opt-in first-cost fixture. Retains only its own disposable database when an evidence
//! directory is supplied, so EXPLAIN can measure the exact producer-created family rows.
use super::*;
use std::{path::PathBuf, time::Instant};

#[tokio::test]
#[ignore = "manual TYR-105 bounded cost evidence on dedicated disposable PostgreSQL"]
async fn migrated_subname_path_cost_fixture() -> Result<()> {
    let count: usize = std::env::var("BIGNAME_TYR105_COST_NAMES")
        .unwrap_or_else(|_| "1000".into())
        .parse()?;
    let (database, logs, resolver) = setup().await?;
    let initial: Vec<_> = logs
        .iter()
        .filter(|log| log.block_number < BASE + 122)
        .cloned()
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    let parent = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let registry = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let owner = HOLDER.parse()?;
    let mut children = Vec::new();
    let mut ids = Vec::new();
    for index in 0..count {
        let label = format!("path-cost-{index}");
        let name = format!("{label}.{NAME}");
        let node: alloy_primitives::B256 = bigname_lookup::ens_namehash_hex(&name)?.parse()?;
        ids.push(format!("ens:{node:#x}"));
        children.extend(transaction(
            122,
            index as i64 * 3,
            vec![
                (
                    registry,
                    NewOwner {
                        node: parent,
                        label: keccak256(&label),
                        owner,
                    }
                    .encode_log_data(),
                ),
                (registry, NewResolver { node, resolver }.encode_log_data()),
            ],
        ));
        children.extend(transaction(
            122,
            index as i64 * 3 + 1,
            vec![
                (resolver, NameChanged { node, name }.encode_log_data()),
                (
                    resolver,
                    AddressChanged {
                        node,
                        coinType: U256::from(60),
                        newAddress: owner.to_vec().into(),
                    }
                    .encode_log_data(),
                ),
            ],
        ));
        children.extend(transaction(
            122,
            index as i64 * 3 + 2,
            vec![(registry, NewResolver { node, resolver }.encode_log_data())],
        ));
    }
    for (index, log) in children.iter_mut().enumerate() {
        log.log_index = index as i64;
    }
    let started = Instant::now();
    seed_and_run(&database, &children, 122, 122).await?;
    let seed_ms = started.elapsed().as_millis();
    let started = Instant::now();
    let before = bigname_storage::families::name::load_family_names_by_logical_name_ids(
        &database.pool,
        &ids,
    )
    .await?;
    let before_read_ms = started.elapsed().as_millis();
    assert_eq!(before.len(), count);
    assert!(
        before
            .values()
            .all(|row| row.declared_summary["resolver"]["address"].is_string())
    );
    let later: Vec<_> = logs
        .into_iter()
        .filter(|log| log.block_number == BASE + 122)
        .map(|mut log| {
            log.block_number = BASE + 123;
            log.block_hash = format!("0xhistory{}", BASE + 123);
            log.transaction_hash =
                format!("0x{:064x}", log.block_number * 100 + log.transaction_index);
            log
        })
        .collect();
    let started = Instant::now();
    seed_and_run(&database, &later, 123, 123).await?;
    let migration_ms = started.elapsed().as_millis();
    let started = Instant::now();
    let after = bigname_storage::families::name::load_family_names_by_logical_name_ids(
        &database.pool,
        &ids,
    )
    .await?;
    let after_read_ms = started.elapsed().as_millis();
    let retained = after
        .values()
        .filter(|row| row.declared_summary["resolver"]["address"].is_string())
        .count();
    let records = transaction(
        124,
        0,
        vec![(
            resolver,
            AddressChanged {
                node: bigname_lookup::ens_namehash_hex(&format!("path-cost-0.{NAME}"))?.parse()?,
                coinType: U256::from(60),
                newAddress: GRANTEE.parse::<Address>()?.to_vec().into(),
            }
            .encode_log_data(),
        )],
    );
    let started = Instant::now();
    seed_and_run(&database, &records, 124, 124).await?;
    let record_edit_ms = started.elapsed().as_millis();
    let receipt = json!({"names":count,"database":database.database_name,"fingerprint":bigname_content_hash::INTERPRETER_CONTENT_HASH,"seed_and_project_ms":seed_ms,"before_batch_read_ms":before_read_ms,"migration_and_project_ms":migration_ms,"after_batch_read_ms":after_read_ms,"retained_after_migration":retained,"ordinary_record_edit_and_project_ms":record_edit_ms});
    eprintln!("TYR105 cost {receipt}");
    if let Ok(output) = std::env::var("BIGNAME_TYR105_COST_DIR") {
        let path = PathBuf::from(output);
        std::fs::create_dir_all(&path)?;
        std::fs::write(
            path.join("cost-fixture.json"),
            serde_json::to_vec_pretty(&receipt)?,
        )?;
        database.pool.close().await;
        database.lookup_pool.close().await;
        Ok(())
    } else {
        database.cleanup().await
    }
}
