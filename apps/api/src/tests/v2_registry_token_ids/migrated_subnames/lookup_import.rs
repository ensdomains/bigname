//! Node-only ABI observations are Project-produced before the actual operator label import.
//! Imported spelling must be visible immediately without rebuilding prepared lookup components.
use super::*;

#[tokio::test]
async fn lookup_precomputation_uses_current_imported_spelling_without_republication() -> Result<()>
{
    const LABEL_TO_IMPORT: &str = "unknown-before-import";
    const IMPORT_OWNER: &str = "0x0000000000000000000000000000000000000259";
    let (database, logs, resolver) = setup().await?;
    let initial: Vec<_> = logs
        .into_iter()
        .filter(|log| log.block_number <= BASE + 121)
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    let name = format!("{LABEL_TO_IMPORT}.{NAME}");
    let node = bigname_lookup::ens_namehash_hex(&name)?.parse()?;
    let registry: Address = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let owner: Address = IMPORT_OWNER.parse()?;
    // ENSRegistry.setSubnodeRecord emits the child node/label hashes, not the spelling.
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L57 @ ens_v1@91c966f)
    let mut child = transaction(
        122,
        0,
        vec![
            (
                registry,
                NewOwner {
                    node: bigname_lookup::ens_namehash_hex(NAME)?.parse()?,
                    label: keccak256(LABEL_TO_IMPORT),
                    owner,
                }
                .encode_log_data(),
            ),
            (registry, NewResolver { node, resolver }.encode_log_data()),
        ],
    );
    child.extend(transaction(
        122,
        1,
        vec![(
            resolver,
            AddressChanged {
                node,
                coinType: U256::from(60),
                newAddress: owner.to_vec().into(),
            }
            .encode_log_data(),
        )],
    ));
    for (index, log) in child.iter_mut().enumerate() {
        log.log_index = index as i64;
    }
    seed_and_run_with(&database, &child, 122, 122, &[(122, 1, IMPORT_OWNER)], None).await?;
    let id = format!("ens:{node:#x}");
    let raw: Option<String> =
        sqlx::query_scalar("SELECT raw_name FROM name_surfaces WHERE logical_name_id=$1")
            .bind(&id)
            .fetch_one(&database.pool)
            .await?;
    assert!(
        raw.is_none(),
        "the producer must have no spelling to persist"
    );
    let placeholder = format!("[{}].{NAME}", hex::encode(keccak256(LABEL_TO_IMPORT)));
    let before = lookup_publication::assert_name_prepared_parity(&database, &name).await?;
    assert_eq!(before["name"], placeholder);
    assert_eq!(before["primary_address"], IMPORT_OWNER);
    let snapshot = lookup_publication::components(&database).await?;
    let marker: Value =
        sqlx::query_scalar("SELECT to_jsonb(m) FROM project_family_marker m WHERE chain_id=$1")
            .bind(PATH_CHAIN)
            .fetch_one(&database.pool)
            .await?;
    let request = json!({"profile":"detail","inputs":[
        {"address":IMPORT_OWNER,"relation":"any","page_size":1}
    ]});
    let reverse_before = path_lookup(&database, request.clone()).await?;
    assert_eq!(reverse_before["data"][0]["records"][0]["name"], placeholder);

    sqlx::query("INSERT INTO ens_names(hash,name) VALUES ($1,$2)")
        .bind(format!("{:#x}", keccak256(LABEL_TO_IMPORT)))
        .bind(LABEL_TO_IMPORT)
        .execute(&database.pool)
        .await?;
    let imported =
        bigname_storage::import_label_preimages_from_ens_names_table(&database.pool, None, None)
            .await?;
    assert_eq!(
        (
            imported.scanned_row_count,
            imported.retained_row_count,
            imported.rejected_row_count
        ),
        (1, 1, 0)
    );
    let after = lookup_publication::assert_name_prepared_parity(&database, &name).await?;
    assert_eq!(after["name"], name);
    assert_eq!(after["primary_address"], IMPORT_OWNER);
    let reverse_after = path_lookup(&database, request).await?;
    assert_eq!(reverse_after["data"][0]["records"][0]["name"], name);
    assert_eq!(
        reverse_after["meta"]["as_of"],
        reverse_before["meta"]["as_of"]
    );
    assert_eq!(lookup_publication::components(&database).await?, snapshot);
    let after_marker: Value =
        sqlx::query_scalar("SELECT to_jsonb(m) FROM project_family_marker m WHERE chain_id=$1")
            .bind(PATH_CHAIN)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(after_marker, marker, "import does not republish Project");
    let repeated =
        bigname_storage::import_label_preimages_from_ens_names_table(&database.pool, None, None)
            .await?;
    assert_eq!(repeated.retained_row_count, 0);
    assert_eq!(lookup_publication::components(&database).await?, snapshot);
    database.cleanup().await
}
