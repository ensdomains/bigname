//! A manifest sync reaches the ENSv2 path walk only through the redo that republishes
//! (`docs/glossary.md` § Universal Resolver cutover).
use super::*;

/// What a client sees of the child through the walk: name detail and its indexed `addr:60`.
async fn child_view(database: &TestDatabase) -> Result<Value> {
    let detail = path_get(database, &format!("/v1/names/{CHILD}")).await?;
    let records = path_get(
        database,
        &format!("/v1/names/{CHILD}/records?source=indexed&keys=addr:60"),
    )
    .await?;
    let mut view = json!({"addr:60": records["data"]["records"]["addr:60"]["status"]});
    for field in [
        "resolver",
        "primary_address",
        "authority",
        "unresolvable_reason",
        "resolution_unsupported_reason",
    ] {
        view[field] = detail["data"][field].clone();
    }
    Ok(view)
}

/// Publish the child resolving through the walk, let manifest sync turn the Sepolia `family`
/// manifest to `shadow`, and check that the child is served unchanged until the redo. After the
/// redo the walk no longer reaches it.
async fn served_unchanged_until_the_redo(family: &str) -> Result<()> {
    let (database, logs, resolver) = setup().await?;
    let initial: Vec<_> = logs
        .iter()
        .filter(|log| log.block_number <= BASE + 121)
        .cloned()
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    assert_consumers(&database, true, resolver, None).await?;
    let before = child_view(&database).await?;
    assert_eq!(before["addr:60"], "ok", "{before:#}");

    shadow_by_manifest_sync(&database.pool, PATH_CHAIN, family, &[PATH_CHAIN]).await?;
    assert_eq!(
        child_view(&database).await?,
        before,
        "between the manifest sync and the redo"
    );

    adopt_this_build(&database.pool, &[PATH_CHAIN]).await?;
    rebuild_fixture_families(
        &database.pool,
        PATH_CHAIN,
        BASE + 121,
        &format!("0xhistory{}", BASE + 121),
    )
    .await?;
    assert_eq!(
        child_view(&database).await?,
        json!({
            "addr:60": "unsupported",
            "authority": "ens_v1",
            "primary_address": null,
            "resolution_unsupported_reason": "ens_v2_path_not_projected",
            "resolver": null,
            "unresolvable_reason": null,
        }),
        "after the redo"
    );
    database.cleanup().await
}

/// R20. The ENSv2 registry manifest turns `shadow`. The walk reads the registry declarations of
/// the manifest set the publication recorded, so the child is served until the redo. After it
/// the registry is no longer declared and the walk does not reach the child.
#[tokio::test]
async fn migrated_subname_walk_serves_the_published_registry_declarations_until_the_redo()
-> Result<()> {
    served_unchanged_until_the_redo("ens_v2_registry_l1").await
}

/// R22. The child's ENSv1 resolver manifest turns `shadow`. The walk reads the resolver's
/// declaration from the manifest set the publication recorded, so the child is served until the
/// redo. After it the resolver is no longer admitted and the walk stops at it.
#[tokio::test]
async fn migrated_subname_walk_serves_the_published_resolver_declaration_until_the_redo()
-> Result<()> {
    served_unchanged_until_the_redo("ens_v1_resolver_l1").await
}
