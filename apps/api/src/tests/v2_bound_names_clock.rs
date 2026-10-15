// The bound-name inputs a stored name summary carries must never go stale by the clock alone.
// A name whose composed resolver, registration, control, serving pointer or support changes
// with no fact changing must be composed again by the family step at the first block past the
// change: its stored `recompose_at` must be due by then. Each step below publishes a block with
// no event at a later time and compares the name's composition at that block with the one the
// summary last stored.

/// The composed-row fields the bound-name rules read: the served resolver, the registration and
/// control fields of the listing rules, the serving pointer, the support status and the
/// selected binding.
fn bound_rule_inputs(row: Option<&bigname_storage::NameCurrentRow>) -> Value {
    let Some(row) = row else {
        return Value::Null;
    };
    let summary = &row.declared_summary;
    json!({
        "resolver": [summary.pointer("/resolver/chain_id"), summary.pointer("/resolver/address")],
        "registration": [summary.pointer("/registration/status"),
                         summary.pointer("/registration/released_at"),
                         summary.pointer("/registration/authority_kind")],
        "control": [summary.pointer("/control/owner"), summary.pointer("/control/registry_owner")],
        "read_reachability": [row.provenance.pointer("/read_reachability/basis"),
                              row.provenance.pointer("/read_reachability/serving_resource_id")],
        "coverage": [row.coverage.get("status"), row.coverage.get("unsupported_reason")],
        "binding": [row.surface_binding_id, row.resource_id, row.serving_resource_id,
                    row.binding_kind.map(|kind| kind.as_str())],
        "namespace": row.namespace,
    })
}

async fn stored_recompose_at(database: &TestDatabase, name: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT recompose_at FROM project_name_summary
         WHERE chain_id = 'ethereum-mainnet' AND logical_name_id = $1",
    )
    .bind(name)
    .fetch_one(&database.pool)
    .await?)
}

/// A registry subname of alice.eth with its own Transfer and resolver.
async fn seed_alice_registry_subname(database: &TestDatabase) -> Result<String> {
    let chain = "ethereum-mainnet";
    let resource = Uuid::from_u128(0x2210);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        "sub.alice.eth",
        chain,
        21_000_003,
        "0xbinding",
        resource,
        Uuid::from_u128(0x1110),
        Uuid::from_u128(0x3310),
        "ens_v1",
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex("sub.alice.eth")?;
    let mut events = Vec::new();
    for (log, kind, after) in [
        (
            40,
            "AuthorityTransferred",
            json!({"source_event":"Transfer", "node":node,
                   "owner":"0x00000000000000000000000000000000000000cc"}),
        ),
        (
            41,
            "ResolverChanged",
            json!({"node":node, "resolver":"0x0000000000000000000000000000000000000abd"}),
        ),
    ] {
        let mut event = history_event(
            &format!("sub-alice-{kind}"),
            Some(&logical),
            Some(resource),
            Some(chain),
            Some(21_000_003),
            Some("0xbinding"),
            Some("0xsub-alice"),
            Some(log),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = "ens_v1_registry_l1".into();
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(logical)
}

/// A `.eth` second-level name decided by ENSv1 beside a live ENSv2 reservation on a cut-over
/// chain, and a registry subname below it, across the lease expiry, the reservation expiry, the
/// lease's grace end and the wrapper expiry, each passed by a block with no event.
#[tokio::test]
async fn stored_bound_fields_follow_a_second_level_entry_lapsing_by_the_clock() -> Result<()> {
    const LEASE_EXPIRY: i64 = ALICE_LEASE_EXPIRY as i64;
    const DAY: i64 = 86_400;
    let database = TestDatabase::new_migrated().await?;
    seed_alice_wrapped_reserved_on_an_admitted_chain(&database).await?;
    let sub = seed_alice_registry_subname(&database).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let names = [bigname_storage::logical_name_id_for_name("ens", "alice.eth"), sub];

    // What each name's summary was last composed from, and its stored boundary.
    let mut stored = Vec::new();
    for name in &names {
        let row = bigname_storage::families::name::load_family_name(&database.pool, name).await?;
        stored.push((
            bound_rule_inputs(row.as_ref()),
            stored_recompose_at(&database, name).await?,
        ));
    }
    let mut timeline = vec![format!("21000003: {stored:?}")];
    let mut stale = Vec::new();
    let steps = [
        LEASE_EXPIRY - DAY,
        LEASE_EXPIRY + 1,
        LEASE_EXPIRY + 62 * DAY - 1,
        LEASE_EXPIRY + 62 * DAY + 1,
        LEASE_EXPIRY + 90 * DAY - 1,
        LEASE_EXPIRY + 90 * DAY + 1,
        LEASE_EXPIRY + 400 * DAY,
    ];
    for (offset, seconds) in steps.into_iter().enumerate() {
        let block = 21_000_004 + offset as i64;
        let hash = format!("0xclock-{block}");
        let time: String = sqlx::query_scalar(
            "SELECT to_char(to_timestamp($1) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
        )
        .bind(seconds as f64)
        .fetch_one(&database.pool)
        .await?;
        seed_schema_v2_lookup_head(&database.pool, "ethereum-mainnet", block, &hash, &time)
            .await?;
        publish_test_families_on(&database.pool, "ethereum-mainnet", block).await?;
        for (name, (inputs, due)) in names.iter().zip(stored.iter_mut()) {
            let row =
                bigname_storage::families::name::load_family_name(&database.pool, name).await?;
            let fresh = bound_rule_inputs(row.as_ref());
            let recomposed = due.is_some_and(|due| due <= seconds);
            if recomposed {
                *inputs = fresh.clone();
            } else if *inputs != fresh {
                stale.push(format!(
                    "{name} at block {block} ({seconds}): stored {inputs} fresh {fresh}, \
                     recompose_at {due:?}"
                ));
            }
            *due = stored_recompose_at(&database, name).await?;
            timeline.push(format!("{block} ({seconds}) {name}: {fresh} due {due:?}"));
        }
    }
    eprintln!("bound-name clock timeline:\n{}", timeline.join("\n"));
    anyhow::ensure!(
        stale.is_empty(),
        "bound-name inputs changed by the clock without a recomposition:\n{}",
        stale.join("\n")
    );
    database.cleanup().await
}
