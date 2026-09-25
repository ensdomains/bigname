use super::*;
use bigname_storage::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    load_address_names_current_page_filtered, load_name_migration_transition_timestamps,
};

// `migrated_at` and the `is_migrated` filter read the activated migration only while ENSv2 is the
// selected arm (Pro review of PR 953, question 6). These cases read both storage readers directly
// after Project: a migrated name whose ENSv2 registration was released stays with ENSv2 and keeps
// both, also beside a later live ENSv1 lease (product ruling of 2026-09-25), and a migrated name
// whose selection changes to ENSv1 through a later ENSv2 reservation loses both while that
// reservation is live and gets them back when it ends.

const V1_REGISTRANT: &str = "0x0000000000000000000000000000000000000071";
const V2_REGISTRANT: &str = "0x0000000000000000000000000000000000000072";
/// Block 10's timestamp in seconds, where every `MigrationApplied` here is written: `database`
/// writes it as 2026-08-26T00:00:00Z.
const MIGRATION_BLOCK_SECONDS: i64 = 1_787_702_400;
const BLOCK_11_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000511";

/// `migrated_at` as each name's block time in seconds.
async fn migrated_at(pool: &PgPool, names: &[String]) -> Result<Vec<(String, i64)>> {
    Ok(load_name_migration_transition_timestamps(pool, names)
        .await?
        .into_iter()
        .map(|(name, at)| (name, at.unix_timestamp()))
        .collect())
}

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
    let first = ops_project(&pool, 10, None, RunMode::Normal).await?;

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
    let mut expected_migrated = vec![
        (retained.clone(), MIGRATION_BLOCK_SECONDS),
        (active.clone(), MIGRATION_BLOCK_SECONDS),
        (relet.clone(), MIGRATION_BLOCK_SECONDS),
    ];
    expected_migrated.sort();
    assert_eq!(
        migrated_at(&pool, &names).await?,
        expected_migrated,
        "only the names still selected under ENSv2 serve migrated_at, at the MigrationApplied block time"
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

    // The reservation ends in block 11. Interpret writes its end by name and without a resource,
    // and the released ENSv2 tombstone returns for the re-reserved name.
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 11, '2026-08-26T00:00:12Z', 'canonical')")
        .bind(CHAIN).bind(BLOCK_11_HASH).execute(&pool).await?;
    let ended = event(
        &pool,
        "readers-82-reservation-end",
        &reserved,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReleased",
            log: 0,
            after: json!({"source_event":"LabelUnregistered","status":"released"}),
        },
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET block_number = 11, block_hash = $2 WHERE normalized_event_id = $1")
        .bind(ended).bind(BLOCK_11_HASH).execute(&pool).await?;
    ops_project(&pool, 11, Some(first), RunMode::Normal).await?;
    assert_eq!(
        authority(&pool, &reserved).await?.0.as_deref(),
        Some("ens_v2"),
        "the released ENSv2 tombstone returns once the reservation ends"
    );
    // migrated_at comes back with the same MigrationApplied block time, not the block that ended
    // the reservation.
    let mut restored = vec![
        (retained.clone(), MIGRATION_BLOCK_SECONDS),
        (reserved.clone(), MIGRATION_BLOCK_SECONDS),
        (active.clone(), MIGRATION_BLOCK_SECONDS),
        (relet.clone(), MIGRATION_BLOCK_SECONDS),
    ];
    restored.sort();
    assert_eq!(migrated_at(&pool, &names).await?, restored);
    // The tombstone is released, so its former lease registrant no longer holds it under either
    // filter, and it is not an observable migrated ownership row either: only the active name is.
    assert!(
        is_migrated_names(&pool, V1_REGISTRANT, false)
            .await?
            .is_empty()
    );
    assert!(
        is_migrated_names(&pool, V1_REGISTRANT, true)
            .await?
            .is_empty()
    );
    assert_eq!(
        is_migrated_names(&pool, V2_REGISTRANT, true).await?,
        vec![active.clone()]
    );
    db.cleanup().await?;
    Ok(())
}
