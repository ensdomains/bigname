//! Real admitted registry/resolver logs flow through Interpret and Project before HTTP/storage
//! parity and changed-key assertions. No prepared lookup row is inserted by this fixture.
use super::*;
use bigname_storage::families::{
    name::load_family_name,
    records::{FamilyAttribution, load_family_record_inventory_detail},
};

sol! {
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5-L10 @ ens_v1@91c966f)
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
}

async fn components(database: &TestDatabase) -> Result<Value> {
    let mut out = serde_json::Map::new();
    for table in [
        "project_lookup_name",
        "project_lookup_relation",
        "project_lookup_inventory",
        "project_lookup_record",
        "project_lookup_dependency",
    ] {
        let rows: Vec<Value> = sqlx::query_scalar(&format!(
            "SELECT to_jsonb(row) FROM {table} row ORDER BY to_jsonb(row)::text"
        ))
        .fetch_all(&database.pool)
        .await?;
        out.insert(table.into(), json!(rows));
    }
    Ok(Value::Object(out))
}

async fn assert_full_parity(database: &TestDatabase) -> Result<Value> {
    let detail = path_get(database, &format!("/v1/names/{CHILD}")).await?;
    let lookup = path_lookup(
        database,
        json!({"profile":"detail","inputs":[{"name":CHILD}]}),
    )
    .await?;
    assert_eq!(lookup["data"][0]["status"], "ok", "{lookup:#}");
    assert_eq!(
        lookup["data"][0]["record"], detail["data"],
        "full detail record and null/absence semantics"
    );
    assert_eq!(lookup["meta"]["as_of"], detail["meta"]["as_of"]);
    let id = format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?);
    let row = load_family_name(&database.pool, &id)
        .await?
        .context("composed child")?;
    let mut stored =
        bigname_storage::load_phase_identity_records_by_ids(&database.pool, &[id]).await?;
    let stored = stored.pop().context("stored child")?;
    match row.record_serving_resource_id() {
        None => assert!(stored.record_inventory_current.is_none()),
        Some(resource) => {
            let expected = load_family_record_inventory_detail(
                &database.pool,
                PATH_CHAIN,
                resource,
                FamilyAttribution::Omit,
            )
            .await?
            .context("composed inventory")?;
            let actual = stored
                .record_inventory_current
                .context("stored inventory")?;
            assert_eq!(
                actual.record_version_boundary_key,
                expected.record_version_boundary_key
            );
            assert_eq!(actual.selectors, expected.row.selectors);
            assert_eq!(actual.entries, expected.row.entries);
            assert_eq!(actual.provenance, expected.row.provenance);
            assert_eq!(
                actual.unsupported_families,
                expected.row.unsupported_families
            );
            assert_eq!(actual.chain_positions, row.chain_positions);
            assert_eq!(actual.last_recomputed_at, expected.row.last_recomputed_at);
        }
    }
    Ok(detail["data"].clone())
}

fn text(resolver: Address, block: i64, key: &str, value: &str) -> Result<Vec<RawLogInput>> {
    Ok(transaction(
        block,
        0,
        vec![(
            resolver,
            TextChanged {
                node: bigname_lookup::ens_namehash_hex(CHILD)?.parse()?,
                indexedKey: keccak256(key),
                key: key.into(),
                value: value.into(),
            }
            .encode_log_data(),
        )],
    ))
}

#[tokio::test]
async fn lookup_precomputation_produced_full_records_incremental_undo_and_empty_block() -> Result<()>
{
    let (database, logs, resolver) = setup().await?;
    let initial: Vec<_> = logs
        .into_iter()
        .filter(|log| log.block_number <= BASE + 121)
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    let before = assert_full_parity(&database).await?;
    assert_eq!(
        before["primary_address"], HOLDER,
        "the ordinary and stored paths must both serve"
    );
    let first = text(resolver, 122, "display name,a", "first")?;
    seed_and_run_with(&database, &first, 122, 122, &[(122, 0, GRANTEE)], None).await?;
    let first = assert_full_parity(&database).await?;
    assert_eq!(first["records"]["texts"]["display name,a"], "first");
    let second = text(resolver, 123, "kept", "unchanged")?;
    seed_and_run_with(&database, &second, 123, 123, &[(123, 0, GRANTEE)], None).await?;
    let before_update = components(&database).await?;
    let changed = text(resolver, 124, "display name,a", "second")?;
    seed_and_run_with(&database, &changed, 124, 124, &[(124, 0, GRANTEE)], None).await?;
    let after = assert_full_parity(&database).await?;
    assert_eq!(after["records"]["texts"]["display name,a"], "second");
    assert_eq!(after["records"]["texts"]["kept"], "unchanged");
    let journal: Vec<(String,String)> = sqlx::query_as("SELECT family,key FROM project_family_undo
        WHERE chain_id=$1 AND block_number=$2 AND family LIKE 'project_lookup_%' ORDER BY family,key")
        .bind(PATH_CHAIN).bind(BASE+124).fetch_all(&database.pool).await?;
    assert_eq!(
        journal
            .iter()
            .filter(|(family, _)| family == "project_lookup_record")
            .count(),
        1,
        "{journal:?}"
    );
    assert!(
        journal
            .iter()
            .all(|(family, _)| family == "project_lookup_record"
                || family == "project_lookup_inventory"),
        "a value edit does not fan out to aliases/relations/dependencies: {journal:?}"
    );
    let before_empty = components(&database).await?;
    seed_and_run(&database, &[], 125, 125).await?;
    assert_eq!(components(&database).await?, before_empty);
    let empty_writes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_family_undo
        WHERE chain_id=$1 AND block_number=$2 AND family LIKE 'project_lookup_%'",
    )
    .bind(PATH_CHAIN)
    .bind(BASE + 125)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(empty_writes, 0);
    bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 123).await?;
    assert_eq!(
        components(&database).await?,
        before_update,
        "undo restores exact factored components"
    );
    publish(&database, 125).await?;
    assert_full_parity(&database).await?;
    assert_eq!(
        components(&database).await?,
        before_empty,
        "replay restores the same selected values"
    );

    // Duplicate inputs straddle the 32-address request chunk. Full output, count and order are
    // identical for each duplicate, and feed must omit payloads that detail serves.
    for profile in ["feed", "detail"] {
        let inputs: Vec<_> = (0..65)
            .map(|_| json!({"address":GRANTEE,"relation":"any","page_size":1}))
            .collect();
        let response = path_lookup(&database, json!({"profile":profile,"inputs":inputs})).await?;
        let answers = response["data"].as_array().context("lookup results")?;
        assert_eq!(answers.len(), 65);
        for answer in answers {
            assert_eq!(answer, &answers[0]);
            let record = &answer["records"][0];
            assert_eq!(record["name"], CHILD, "{answer:#}");
            assert_eq!(record.get("records").is_some(), profile == "detail");
        }
    }
    database.cleanup().await
}
