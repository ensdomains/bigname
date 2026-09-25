use super::*;
use bigname_storage::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    load_address_names_current_page_filtered, load_name_migration_transition_timestamps,
};

// `migrated_at` and the `is_migrated` filter read the activated migration only while ENSv2 is the
// selected arm (Pro review of PR 953, question 6). These cases read both storage readers directly
// after Project: a migrated name whose ENSv2 registration was released stays with ENSv2 and keeps
// both, also beside a later live ENSv1 lease (product ruling of 2026-09-25), and a migrated name
// whose selection changes to ENSv1 through a later ENSv2 reservation loses both.

const V1_REGISTRANT: &str = "0x0000000000000000000000000000000000000071";
const V2_REGISTRANT: &str = "0x0000000000000000000000000000000000000072";

/// A migrated name: an ENSv2 registration at log 2 whose binding has closed, its activated
/// migration at log 3, and its release at log 4. Returns (logical, ENSv2 resource).
async fn released_migrated_name(
    pool: &PgPool,
    index: u16,
    raw_name: &str,
) -> Result<(String, String)> {
    let logical = surface(pool, index, raw_name, &[]).await?;
    let v2_resource = closed_v2_binding_at(pool, &logical, index, 2, 0).await?;
    event(
        pool,
        &format!("readers-{index}-grant"),
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationGranted",
            log: 2,
            after: json!({"status":"registered","registrant":V2_REGISTRANT}),
        },
    )
    .await?;
    event(
        pool,
        &format!("readers-{index}-migration"),
        &logical,
        None,
        Event {
            family: "ens_v2_migration_l1",
            kind: "MigrationApplied",
            log: 3,
            after: json!({"migration_path":"unwrapped","successor_binding":{"binding_id":uuid(13, index),"resource_id":v2_resource}}),
        },
    )
    .await?;
    event(
        pool,
        &format!("readers-{index}-release"),
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReleased",
            log: 4,
            after: json!({"status":"unregistered"}),
        },
    )
    .await?;
    Ok((logical, v2_resource))
}

/// A live ENSv1 lease on the name: an open ENSv1 binding and its grant at `log`.
async fn live_v1_lease(pool: &PgPool, logical: &str, index: u16, log: i64) -> Result<String> {
    let resource = open_binding(pool, logical, index, "ens_v1", log).await?;
    // The registrant relation is a token relation, so the lease resource carries its token.
    let lineage = uuid(17, index);
    sqlx::query("INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&lineage).bind(CHAIN).bind(HASH).execute(pool).await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $2::uuid WHERE resource_id = $1::uuid")
        .bind(&resource)
        .bind(&lineage)
        .execute(pool)
        .await?;
    event(
        pool,
        &format!("readers-{index}-v1-grant"),
        logical,
        Some(&resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log,
            after: json!({"status":"registered","registrant":V1_REGISTRANT}),
        },
    )
    .await?;
    Ok(resource)
}

async fn is_migrated_names(pool: &PgPool, address: &str, is_migrated: bool) -> Result<Vec<String>> {
    let page = load_address_names_current_page_filtered(
        pool,
        address,
        None,
        None,
        AddressNamesCurrentDedupe::Surface,
        None,
        None,
        Some(is_migrated),
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        None,
        50,
    )
    .await?;
    Ok(page
        .entries
        .into_iter()
        .map(|entry| entry.logical_name_id)
        .collect())
}

#[tokio::test]
async fn migration_readers_follow_the_selected_arm_after_a_v2_release() -> Result<()> {
    let (db, pool) = database("migration_readers_release").await?;
    // Retained: the release is the latest lifecycle fact and ENSv2 stays selected.
    let (retained, _) = released_migrated_name(&pool, 81, "retained.eth").await?;
    // Re-reserved: a later reservation of the ENSv2 label defers to the live ENSv1 lease. As
    // Interpret writes it after an `unregister`, the reservation names the label but carries no
    // resource, since the token version was bumped.
    let (reserved, _) = released_migrated_name(&pool, 82, "rereserved.eth").await?;
    live_v1_lease(&pool, &reserved, 82, 1).await?;
    event(
        &pool,
        "readers-82-reservation",
        &reserved,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReserved",
            log: 5,
            after: json!({"status":"reserved"}),
        },
    )
    .await?;
    // Relet: an ENSv1 lease granted after the release is live; the release still holds.
    let (relet, _) = released_migrated_name(&pool, 83, "relet.eth").await?;
    live_v1_lease(&pool, &relet, 83, 5).await?;
    // Active: a migrated name whose ENSv2 registration is current, for the positive filter.
    let active = surface(&pool, 84, "active.eth", &[]).await?;
    let active_v2 = open_binding(&pool, &active, 84, "ens_v2", 2).await?;
    let lineage = uuid(17, 84);
    sqlx::query("INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&lineage).bind(CHAIN).bind(HASH).execute(&pool).await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $2::uuid WHERE resource_id = $1::uuid")
        .bind(&active_v2)
        .bind(&lineage)
        .execute(&pool)
        .await?;
    for (log, family, kind, after) in [
        (
            2,
            "ens_v2_registry_l1",
            "RegistrationGranted",
            json!({"status":"registered","registrant":V2_REGISTRANT}),
        ),
        (
            3,
            "ens_v2_migration_l1",
            "MigrationApplied",
            json!({"migration_path":"unwrapped","successor_binding":{"binding_id":uuid(9, 84),"resource_id":active_v2}}),
        ),
    ] {
        let resource = (kind != "MigrationApplied").then_some(active_v2.as_str());
        event(
            &pool,
            &format!("readers-84-{kind}"),
            &active,
            resource,
            Event {
                family,
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    run(&pool).await?;

    for (logical, arm) in [
        (&retained, "ens_v2"),
        (&reserved, "ens_v1"),
        (&relet, "ens_v2"),
    ] {
        assert_eq!(
            authority(&pool, logical).await?.0.as_deref(),
            Some(arm),
            "{logical}"
        );
    }
    let names = vec![
        retained.clone(),
        reserved.clone(),
        relet.clone(),
        active.clone(),
    ];
    let timestamps = load_name_migration_transition_timestamps(&pool, &names).await?;
    let mut expected_migrated = vec![retained.clone(), active.clone(), relet.clone()];
    expected_migrated.sort();
    assert_eq!(
        timestamps.keys().cloned().collect::<Vec<_>>(),
        expected_migrated,
        "only the names still selected under ENSv2 serve migrated_at"
    );
    assert_eq!(
        is_migrated_names(&pool, V2_REGISTRANT, true).await?,
        vec![active.clone()]
    );
    assert!(
        is_migrated_names(&pool, V2_REGISTRANT, false)
            .await?
            .is_empty()
    );
    // The ENSv1-selected name is held by its live lease, so the ownership reader lists it for the
    // lease registrant, and only under is_migrated=false. The relet name is released under ENSv2,
    // so its lease registrant holds no current relation to it.
    assert_eq!(
        is_migrated_names(&pool, V1_REGISTRANT, false).await?,
        vec![reserved.clone()]
    );
    assert!(
        is_migrated_names(&pool, V1_REGISTRANT, true)
            .await?
            .is_empty()
    );
    // The retained tombstone is released, so no address holds it and neither filter lists it;
    // the V2_REGISTRANT lists above contain only the active name.
    db.cleanup().await?;
    Ok(())
}
