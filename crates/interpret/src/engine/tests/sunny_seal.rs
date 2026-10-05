use alloy_sol_types::SolCall;
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, TOKEN_CONTROL_TRANSFERRED_EVENT_KIND, TOKEN_LINEAGE_ID_KEY,
};
use serde_json::Value;

use super::*;

const INCIDENT_BLOCK: i64 = 11_849_653;
const LOGICAL: &str = "ens:0xe5700a9afdd4b3a7d54407ce2c473ca3e4183a1c1a63144e0b694bcd815e6343";
const LEASE: &str = "850cba17-9ad5-5308-ac30-f6320ead6deb";
const TRANSIENT_BINDING: &str = "a72322fd-2dbe-52d8-87cd-f492bcdb2e0c";

/// Recorded Sepolia transaction and bounded retained state, including the prior ENSv2
/// reservation's preimage and an ENSv1 lease that had never acquired a readable binding.
#[tokio::test]
async fn sunny_seal_recorded_migration_commits_without_a_cleanup_binding() -> TestResult {
    let database = database("interpret_sunny_seal_recorded").await?;
    let pool = database.pool();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/sunny-seal-migration.json"
    ))?;
    seed_recorded_state(pool, &fixture).await?;
    let older = older_history(pool).await?;
    let result = run_block(&Engine::new(pool.clone()), &fixture, INCIDENT_BLOCK).await;
    if let Err(error) = &result {
        let message = error.to_string();
        assert!(message.contains(TRANSIENT_BINDING), "{message}");
        assert!(
            message.contains("leaves 1 ENSv1 bindings open"),
            "{message}"
        );
        let persisted: i64 =
            sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE block_number = $1")
                .bind(INCIDENT_BLOCK)
                .fetch_one(pool)
                .await?;
        assert_eq!(persisted, 0, "failed migration must roll back atomically");
        eprintln!("recorded cleanup failure and atomic rollback confirmed: {message}");
    }
    result?;
    assert_eq!(
        older_history(pool).await?,
        older,
        "older unnamed observations stay immutable"
    );
    assert_committed(pool, &fixture, INCIDENT_BLOCK).await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sunny_seal_warm_cold_combined_and_redo_preserve_the_same_current_authority() -> TestResult
{
    let recorded: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/sunny-seal-migration.json"
    ))?;
    let mut snapshots = Vec::new();
    for mode in ["cold", "continuing", "combined"] {
        let database = database("interpret_sunny_seal_restore").await?;
        let pool = database.pool();
        seed_recorded_state(pool, &recorded).await?;
        let fixture = seed_generated_renewal(pool, &recorded).await?;
        let older = older_history(pool).await?;
        match mode {
            "continuing" => {
                let engine = Engine::new(pool.clone())
                    .with_blocks_per_batch(std::num::NonZeroU32::new(1).unwrap());
                let mut request = BatchRequest {
                    chain_id: CHAIN.to_owned(),
                    from_block: INCIDENT_BLOCK - 1,
                    to_block: INCIDENT_BLOCK + 1,
                    resume_current: None,
                    mode: RunMode::Normal,
                };
                for block in INCIDENT_BLOCK - 1..=INCIDENT_BLOCK + 1 {
                    let outcome = engine.run_batch(request.clone()).await?;
                    assert_eq!(outcome.current.number, block);
                    assert_eq!(outcome.complete, block == INCIDENT_BLOCK + 1);
                    request.resume_current = Some(outcome.current);
                    if block == INCIDENT_BLOCK {
                        assert_committed(pool, &fixture, INCIDENT_BLOCK).await?;
                    }
                }
            }
            "combined" => {
                let outcome = Engine::new(pool.clone())
                    .run_batch(BatchRequest {
                        chain_id: CHAIN.to_owned(),
                        from_block: INCIDENT_BLOCK,
                        to_block: INCIDENT_BLOCK + 1,
                        resume_current: None,
                        mode: RunMode::Normal,
                    })
                    .await?;
                assert_eq!(outcome.current.number, INCIDENT_BLOCK + 1);
                assert!(outcome.complete);
            }
            _ => {
                run_block(&Engine::new(pool.clone()), &fixture, INCIDENT_BLOCK).await?;
                assert_committed(pool, &fixture, INCIDENT_BLOCK).await?;
                run_block(&Engine::new(pool.clone()), &fixture, INCIDENT_BLOCK + 1).await?;
            }
        }
        assert_eq!(older_history(pool).await?, older);
        assert_committed(pool, &fixture, INCIDENT_BLOCK + 1).await?;
        assert_renewed(pool, INCIDENT_BLOCK + 1, 1_810_091_605).await?;
        let before_redo = migration_snapshot(pool).await?;
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: INCIDENT_BLOCK,
                to_block: INCIDENT_BLOCK + 1,
                resume_current: None,
                mode: RunMode::Redo,
            })
            .await?;
        let after_redo = migration_snapshot(pool).await?;
        assert!(
            after_redo == before_redo,
            "{mode}: {}",
            equivalence::first_json_difference(&before_redo, &after_redo, "$")
        );
        assert_eq!(older_history(pool).await?, older);
        assert_committed(pool, &fixture, INCIDENT_BLOCK + 1).await?;
        assert_renewed(pool, INCIDENT_BLOCK + 1, 1_810_091_605).await?;
        snapshots.push(before_redo);
        database.cleanup().await?;
    }
    for other in &snapshots[1..] {
        assert!(
            snapshots[0] == *other,
            "cold, retained-session and combined output: {}",
            equivalence::first_json_difference(&snapshots[0], other, "$")
        );
    }
    Ok(())
}

async fn assert_renewed(pool: &PgPool, block: i64, expiry: i64) -> TestResult {
    let renewed: (Uuid, Value) = sqlx::query_as(
        "SELECT resource_id,after_state FROM normalized_events
         WHERE block_number=$1 AND event_kind='RegistrationRenewed'
           AND source_family='ens_v1_registrar_l1' AND log_index=0",
    )
    .bind(block)
    .fetch_one(pool)
    .await?;
    assert_eq!(renewed.0.to_string(), LEASE);
    assert_eq!(renewed.1["registrant"], GRAVEYARD);
    assert_eq!(renewed.1["expiry"], expiry);
    assert_eq!(
        renewed.1["registrar_surface_evidence"]["retirement"]["state"]["registrar_surface_retired"],
        true,
    );
    Ok(())
}

#[tokio::test]
async fn sunny_seal_new_numeric_grant_and_its_renewal_keep_the_new_lease_binding() -> TestResult {
    sol! {
        function registerOnly(uint256 id, address owner, uint256 duration);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
    let recorded: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/sunny-seal-migration.json"
    ))?;
    let database = database("interpret_sunny_seal_new_lease").await?;
    let pool = database.pool();
    seed_recorded_state(pool, &recorded).await?;
    run_block(&Engine::new(pool.clone()), &recorded, INCIDENT_BLOCK).await?;
    assert_committed(pool, &recorded, INCIDENT_BLOCK).await?;

    // Construct a new authorized numeric registerOnly after the old lease and grace period.
    // The ERC721 burn/mint precede NameRegistered; the registry record stays independent.
    // This is generated boundary coverage, not a captured transaction.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L168 @ ens_v1@91c966f)
    let later = INCIDENT_BLOCK + 1;
    let timestamp = 1_810_091_604_i64 + 90 * 24 * 60 * 60 + 1;
    let expiry = timestamp + 3600;
    let token = U256::from_be_bytes(keccak256(b"sunny-seal").0);
    let owner: Address = "0x0000000000000000000000000000000000000052".parse()?;
    let mut fixture = recorded.clone();
    let previous = fixture["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["block_number"] == INCIDENT_BLOCK)
        .unwrap();
    let block = serde_json::json!({
        "chain_id":CHAIN, "block_number":later, "block_hash":block_hash(later),
        "parent_hash":previous["block_hash"], "canonicality_state":"canonical",
        "block_timestamp":time::OffsetDateTime::from_unix_timestamp(timestamp)?
            .format(&time::format_description::well_known::Rfc3339)?,
    });
    sqlx::query(
        "INSERT INTO chain_lineage
         (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
         SELECT chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state
         FROM jsonb_populate_record(NULL::chain_lineage,$1)",
    )
    .bind(&block)
    .execute(pool)
    .await?;
    fixture["blocks"].as_array_mut().unwrap().push(block);
    insert_transaction(pool, later, BASE_REGISTRAR).await?;
    sqlx::query("UPDATE raw_transactions SET input=$1 WHERE transaction_hash=$2")
        .bind(
            registerOnlyCall {
                id: token,
                owner,
                duration: U256::from(3600),
            }
            .abi_encode(),
        )
        .bind(transaction_hash(later))
        .execute(pool)
        .await?;
    for (index, from, to) in [
        (0, GRAVEYARD.parse()?, Address::ZERO),
        (1, Address::ZERO, owner),
    ] {
        insert_log(
            pool,
            later,
            index,
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from,
                to,
                tokenId: token,
            }
            .encode_log_data(),
        )
        .await?;
    }
    insert_log(
        pool,
        later,
        2,
        BASE_REGISTRAR,
        NameRegistered {
            id: token,
            owner,
            expires: U256::from(expiry),
        }
        .encode_log_data(),
    )
    .await?;
    run_block(&Engine::new(pool.clone()), &fixture, later).await?;
    let grant: (Uuid, Value) = sqlx::query_as(
        "SELECT resource_id,after_state FROM normalized_events WHERE block_number=$1
         AND event_kind='RegistrationGranted' AND source_family='ens_v1_registrar_l1'",
    )
    .bind(later)
    .fetch_one(pool)
    .await?;
    assert_ne!(grant.0.to_string(), LEASE);
    assert_eq!(grant.1["registrant"], format!("{owner:#x}"));
    let original_binding: Uuid = sqlx::query_scalar(
        "SELECT surface_binding_id FROM surface_bindings WHERE resource_id=$1
         AND authority_arm='ens_v1' AND active_to IS NULL",
    )
    .bind(grant.0)
    .fetch_one(pool)
    .await?;
    let fixture = seed_renewal_at(
        pool,
        &fixture,
        later + 1,
        timestamp + 12,
        (expiry + 1) as u64,
    )
    .await?;
    run_block(&Engine::new(pool.clone()), &fixture, later + 1).await?;
    let bindings: Vec<Uuid> = sqlx::query_scalar(
        "SELECT surface_binding_id FROM surface_bindings WHERE resource_id=$1
         AND authority_arm='ens_v1' AND active_to IS NULL",
    )
    .bind(grant.0)
    .fetch_all(pool)
    .await?;
    assert_eq!(bindings, vec![original_binding]);
    let renewed: (Uuid, Value) = sqlx::query_as(
        "SELECT resource_id,after_state FROM normalized_events WHERE block_number=$1
         AND event_kind='RegistrationRenewed' AND source_family='ens_v1_registrar_l1' AND log_index=0",
    ).bind(later + 1).fetch_one(pool).await?;
    assert_eq!(renewed.0, grant.0);
    assert_eq!(renewed.1["expiry"], expiry + 1);
    assert_eq!(renewed.1["registrant"], format!("{owner:#x}"));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sunny_seal_incomplete_and_ordinary_transfers_gain_no_migration_attribution() -> TestResult
{
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/sunny-seal-migration.json"
    ))?;
    let database = database("interpret_sunny_seal_incomplete").await?;
    let pool = database.pool();
    seed_recorded_state(pool, &fixture).await?;
    let complete = prepare_recorded(pool, None, true).await?;
    let incoming = transfer(&complete, 86);
    assert_eq!(incoming.logical_name_id.as_deref(), Some(LOGICAL));
    let cleanup = transfer(&complete, 90);
    assert_eq!(cleanup.logical_name_id.as_deref(), Some(LOGICAL));
    assert_eq!(
        incoming.raw_fact_ref[INTERPRETER_STATE_KEY],
        cleanup.raw_fact_ref[INTERPRETER_STATE_KEY]
    );
    assert_eq!(cleanup.after_state["registrar_surface_retired"], true);
    let block = fixture["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["block_number"] == INCIDENT_BLOCK)
        .unwrap();
    let folded = bigname_adapters::schema_v2::seam::fold_prior_events(
        Vec::new(),
        &complete.normalized_events,
        &[bigname_adapters::schema_v2::RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: block["block_hash"].as_str().unwrap().to_owned(),
            block_number: INCIDENT_BLOCK,
            block_timestamp: time::OffsetDateTime::from_unix_timestamp(1_791_213_960)?,
            canonicality_state: block["canonicality_state"].as_str().unwrap().to_owned(),
        }],
    )?;
    let retained: Vec<_> = folded
        .iter()
        .filter(|event| {
            event.event_kind == TOKEN_CONTROL_TRANSFERRED_EVENT_KIND
                && event.resource_id == Some(LEASE.parse().unwrap())
        })
        .collect();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].after_state["to"], GRAVEYARD);
    assert_eq!(retained[0].after_state["registrar_surface_retired"], true);

    // Remove reclaim, registry cleanup, resolver clear, token cleanup, registration, mint,
    // resource link and initial roles separately. The final case is the ordinary incoming
    // transfer with no later migration evidence. All are counterfactual evidence subsets.
    for missing in [87, 88, 89, 90, 91, 92, 93, 94, -1] {
        let output = prepare_recorded(pool, Some(missing), true).await?;
        let ordinary = transfer(&output, 86);
        let control = prepare_recorded(pool, Some(missing), false).await?;
        assert_eq!(
            ordinary,
            transfer(&control, 86),
            "incomplete proof {missing} must preserve ordinary registrar output"
        );
        assert_eq!(ordinary.event_identity, incoming.event_identity);
        assert_eq!(ordinary.resource_id, incoming.resource_id);
        assert_eq!(ordinary.before_state, incoming.before_state);
        assert_eq!(ordinary.after_state, incoming.after_state);
        let mut raw = ordinary.raw_fact_ref.clone();
        let mut named_raw = incoming.raw_fact_ref.clone();
        raw.as_object_mut().unwrap().remove(INTERPRETER_STATE_KEY);
        named_raw
            .as_object_mut()
            .unwrap()
            .remove(INTERPRETER_STATE_KEY);
        assert_eq!(raw, named_raw);
        if missing == -1 {
            assert!(output.migration_authority_transitions.is_empty());
        }
    }
    database.cleanup().await?;
    Ok(())
}

fn transfer(
    output: &bigname_adapters::schema_v2::BatchOutput,
    log: i64,
) -> &bigname_adapters::schema_v2::NormalizedEvent {
    output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == TOKEN_CONTROL_TRANSFERRED_EVENT_KIND
                && event.source_family == "ens_v1_registrar_l1"
                && event.log_index == Some(log)
        })
        .unwrap()
}

async fn prepare_recorded(
    pool: &PgPool,
    missing_log: Option<i64>,
    migration_enabled: bool,
) -> TestResult<bigname_adapters::schema_v2::BatchOutput> {
    let mut loaded = load::batch_input(
        pool,
        CHAIN,
        INCIDENT_BLOCK,
        INCIDENT_BLOCK,
        None,
        None,
        StateCacheCapacity::Unlimited,
    )
    .await?;
    if let Some(log) = missing_log {
        loaded.input.raw_logs.retain(|raw| {
            if log == -1 {
                raw.log_index <= 86
            } else {
                raw.log_index != log
            }
        });
    }
    if !migration_enabled {
        loaded
            .input
            .manifests
            .retain(|source| source.source_family != "ens_v2_migration_l1");
        loaded
            .input
            .admissions
            .retain(|admission| admission.source_manifest_id != Some(1811));
    }
    let prepared = prepare_schema_v2_batch_incremental(
        loaded.input,
        loaded.adapter_session,
        StateCacheCapacity::Unlimited,
    )?;
    let state_values =
        load::prior_state_values(pool, CHAIN, INCIDENT_BLOCK, prepared.state_value_requests())
            .await?;
    Ok(prepared.finish(state_values)?.0)
}

async fn older_history(pool: &PgPool) -> TestResult<Value> {
    let rows: Value = sqlx::query_scalar(
        "SELECT jsonb_agg(to_jsonb(event) - 'normalized_event_id' - 'observed_at' ORDER BY event_identity)
         FROM normalized_events event WHERE source_family = 'ens_v1_registrar_l1'
           AND block_number < 11821508",
    ).fetch_one(pool).await?;
    let rows_array = rows.as_array().ok_or("older registrar history")?;
    assert!(!rows_array.is_empty());
    assert!(
        rows_array
            .iter()
            .all(|row| row["logical_name_id"].is_null())
    );
    Ok(rows)
}

async fn migration_snapshot(pool: &PgPool) -> TestResult<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
         'events',(SELECT jsonb_agg(to_jsonb(e)-'normalized_event_id'-'observed_at' ORDER BY event_identity)
                   FROM normalized_events e WHERE block_number >= $1),
         'bindings',(SELECT jsonb_agg(to_jsonb(b)-'observed_at'-'inserted_at' ORDER BY surface_binding_id)
                     FROM surface_bindings b WHERE logical_name_id = $2))",
    ).bind(INCIDENT_BLOCK).bind(LOGICAL).fetch_one(pool).await?)
}

async fn assert_committed(pool: &PgPool, fixture: &Value, project_block: i64) -> TestResult {
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE authority_arm = 'ens_v1'),
                count(*) FILTER (WHERE authority_arm = 'ens_v2')
         FROM surface_bindings WHERE chain_id = $1 AND logical_name_id = $2
           AND active_to IS NULL AND canonicality_state IN ('canonical','safe','finalized')",
    )
    .bind(CHAIN)
    .bind(LOGICAL)
    .fetch_one(pool)
    .await?;
    assert_eq!(counts, (0, 1), "Project target {project_block}");
    let successor: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings WHERE chain_id=$1 AND logical_name_id=$2
         AND authority_arm='ens_v2' AND active_to IS NULL",
    )
    .bind(CHAIN)
    .bind(LOGICAL)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        successor.to_string(),
        "d130a2d8-3b46-5df7-9f7b-f7ce4a288679"
    );
    let boundaries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE chain_id = $1 AND logical_name_id = $2
           AND event_kind = 'MigrationApplied' AND consumer_visibility = 'activated'",
    )
    .bind(CHAIN)
    .bind(LOGICAL)
    .fetch_one(pool)
    .await?;
    assert_eq!(boundaries, 1);
    let transient: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings WHERE surface_binding_id = $1
            OR (authority_arm = 'ens_v1' AND block_number >= $2)
            OR active_from >= active_to",
    )
    .bind(TRANSIENT_BINDING.parse::<Uuid>()?)
    .bind(INCIDENT_BLOCK)
    .fetch_one(pool)
    .await?;
    assert_eq!(transient, 0);
    let transfers: Vec<(i64, String, Uuid, Value, Value)> = sqlx::query_as(&format!(
        "SELECT log_index,logical_name_id,resource_id,before_state,after_state
         FROM normalized_events WHERE block_number = $1 AND source_family = 'ens_v1_registrar_l1'
           AND event_kind = '{TOKEN_CONTROL_TRANSFERRED_EVENT_KIND}' ORDER BY log_index"
    ))
    .bind(INCIDENT_BLOCK)
    .fetch_all(pool)
    .await?;
    assert_eq!(transfers.len(), 2);
    for (transfer, log, from, to) in [
        (
            &transfers[0],
            86,
            "0xcc692d6e11268b40a1e3c58e3d86fc4caab9b77a",
            UNLOCKED_CONTROLLER,
        ),
        (&transfers[1], 90, UNLOCKED_CONTROLLER, GRAVEYARD),
    ] {
        assert_eq!(transfer.0, log);
        assert_eq!(transfer.1, LOGICAL);
        assert_eq!(transfer.2.to_string(), LEASE);
        assert_eq!(transfer.3["from"], from);
        assert_eq!(transfer.4["to"], to);
        assert_eq!(
            transfer.4[TOKEN_LINEAGE_ID_KEY],
            "0393e96d-b357-58af-bed2-68962338e9f7"
        );
    }
    assert_eq!(transfers[1].4["registrar_surface_retired"], true);
    let later_records: Vec<(i64, String)> = sqlx::query_as(
        "SELECT log_index,event_kind FROM normalized_events WHERE block_number=$1
           AND log_index >= 95 ORDER BY log_index,event_kind",
    )
    .bind(INCIDENT_BLOCK)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        later_records,
        [
            (95, "ResolverChanged".to_owned()),
            (96, PREIMAGE_OBSERVATION_EVENT_KIND.to_owned()),
            (96, "ResolverRecordLinked".to_owned()),
            (97, "RecordChanged".to_owned()),
        ]
    );
    let block = fixture["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["block_number"] == project_block)
        .unwrap();
    let marker = bigname_project::Marker {
        number: project_block,
        hash: block["block_hash"].as_str().unwrap().to_owned(),
    };
    let token = bigname_project::families::input_token(pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        pool,
        CHAIN,
        &marker,
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(outcome.marker, Some(marker));
    let summary = bigname_storage::families::name::load_family_name(pool, LOGICAL)
        .await?
        .ok_or("published sunny-seal.eth")?
        .declared_summary;
    assert_eq!(
        summary["registration"]["authority_kind"], "ens_v2_registry",
        "{summary:#}"
    );
    assert_eq!(
        summary["control"]["registry_owner"], "0xcc692d6e11268b40a1e3c58e3d86fc4caab9b77a",
        "{summary:#}"
    );
    Ok(())
}

async fn seed_generated_renewal(pool: &PgPool, recorded: &Value) -> TestResult<Value> {
    seed_renewal_at(
        pool,
        recorded,
        INCIDENT_BLOCK + 1,
        1_791_213_972,
        1_810_091_605,
    )
    .await
}

async fn seed_renewal_at(
    pool: &PgPool,
    recorded: &Value,
    later: i64,
    timestamp: i64,
    expiry: u64,
) -> TestResult<Value> {
    sol! { event NameRenewed(uint256 indexed id, uint256 expires); }
    mod controller {
        use alloy_sol_types::sol;
        sol! {
            function renew(string name, uint256 duration);
            event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
        }
    }
    const CONTROLLER: &str = "0xfed6a969aaa60e4961fcd3ebf1a2e8913ac65b72";
    let mut fixture = recorded.clone();
    let previous = fixture["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["block_number"] == later - 1)
        .unwrap()
        .clone();
    // This follow-up is generated, not part of the recorded transaction. BaseRegistrar renew
    // extends the current lease without transferring its owner, including a Graveyard holder.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L168 @ ens_v1@91c966f)
    let block = serde_json::json!({
        "chain_id": CHAIN, "block_number": later, "block_hash": block_hash(later),
        "parent_hash": previous["block_hash"], "canonicality_state": "canonical",
        "block_timestamp": time::OffsetDateTime::from_unix_timestamp(timestamp)?
            .format(&time::format_description::well_known::Rfc3339)?,
    });
    sqlx::query(
        "INSERT INTO chain_lineage
         (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
         SELECT chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state
         FROM jsonb_populate_record(NULL::chain_lineage,$1)",
    )
    .bind(&block)
    .execute(pool)
    .await?;
    fixture["blocks"].as_array_mut().unwrap().push(block);
    // Use the complete admitted wrapped-controller receipt. A nonwrapped token returns from
    // NameWrapper.renew without wrapper logs; the controller emits its text renewal after the
    // BaseRegistrar's numeric renewal. The generated EOA overpayment refund emits no log.
    // (upstream: .refs/basenames/lib/ens-contracts/contracts/ethregistrar/ETHRegistrarController.sol:L210-L226 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/ens-contracts/contracts/wrapper/NameWrapper.sol:L351-L384 @ basenames@1809bbc)
    insert_transaction(pool, later, CONTROLLER).await?;
    sqlx::query(
        "UPDATE raw_transactions SET input=$1,value=$2::text::numeric WHERE transaction_hash=$3",
    )
    .bind(
        controller::renewCall {
            name: "sunny-seal".to_owned(),
            duration: U256::from(1),
        }
        .abi_encode(),
    )
    .bind("1000000000000000000")
    .bind(transaction_hash(later))
    .execute(pool)
    .await?;
    insert_log(
        pool,
        later,
        0,
        BASE_REGISTRAR,
        NameRenewed {
            id: U256::from_be_bytes(keccak256(b"sunny-seal").0),
            expires: U256::from(expiry),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        later,
        1,
        CONTROLLER,
        controller::NameRenewed {
            name: "sunny-seal".to_owned(),
            label: keccak256(b"sunny-seal"),
            cost: U256::from(1_000_000_000_000_000_000_u64),
            expires: U256::from(expiry),
        }
        .encode_log_data(),
    )
    .await?;
    Ok(fixture)
}

async fn seed_recorded_state(pool: &PgPool, fixture: &Value) -> TestResult {
    let repository = load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    // Retained state contains database-local manifest IDs. Seed only those recorded IDs,
    // then let the normal sync install the checked-in declarations and compiled watch plan.
    for (family, id) in [
        ("ens_v1_registrar_l1", 2_i64),
        ("ens_v1_registry_l1", 3),
        ("ens_v2_migration_l1", 1811),
        ("ens_v2_registry_l1", 1813),
    ] {
        let loaded = repository
            .manifests()
            .iter()
            .find(|loaded| loaded.manifest.source_family == family)
            .ok_or("recorded manifest family")?;
        let manifest = &loaded.manifest;
        sqlx::query(
            "INSERT INTO manifest_versions
             (manifest_id, manifest_version, namespace, source_family, chain_id,
              deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
             OVERRIDING SYSTEM VALUE VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(id)
        .bind(i64::try_from(manifest.manifest_version)?)
        .bind(&manifest.namespace)
        .bind(family)
        .bind(&manifest.chain)
        .bind(&manifest.deployment_epoch)
        .bind(manifest.rollout_status.as_db_value())
        .bind(&manifest.normalizer_version)
        .bind(loaded.relative_path.to_string_lossy().as_ref())
        .bind(serde_json::to_value(manifest)?)
        .execute(pool)
        .await?;
    }
    sqlx::query("SELECT setval(pg_get_serial_sequence('manifest_versions','manifest_id'), 1813)")
        .execute(pool)
        .await?;
    sync_schema_v2_repository(pool, &repository).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    seed_raw(pool, fixture).await?;
    // Materialize identity prerequisites from the recorded numeric lease and raw observations.
    // This is a bounded reconstruction, not an export of the full historical database.
    for block in [10_836_561, 10_839_948, 11_821_508, 11_849_641] {
        run_block(&Engine::new(pool.clone()), fixture, block).await?;
    }
    let lease: Uuid =
        sqlx::query_scalar("SELECT resource_id FROM resources WHERE resource_id = $1")
            .bind(LEASE.parse::<Uuid>()?)
            .fetch_one(pool)
            .await?;
    assert_eq!(lease.to_string(), LEASE);
    // The captured retained state has no committed ENSv1 binding. Keep that observed snapshot
    // shape, and replace locally regenerated tails with the exact recorded normalized rows.
    sqlx::query("DELETE FROM surface_bindings WHERE chain_id = $1 AND logical_name_id = $2")
        .bind(CHAIN)
        .bind(LOGICAL)
        .execute(pool)
        .await?;
    sqlx::query(
        "DELETE FROM normalized_events WHERE chain_id = $1 AND block_number IN (11821508,11849641)",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    for event in fixture["retained_events"]
        .as_array()
        .ok_or("retained events")?
    {
        sqlx::query(
            "INSERT INTO normalized_events
             (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,
              manifest_version,source_manifest_id,chain_id,block_number,block_hash,
              transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,
              canonicality_state,before_state,after_state,migration_correlation_ids,consumer_visibility)
             SELECT event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,
                    manifest_version,source_manifest_id,chain_id,block_number,block_hash,
                    transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,
                    canonicality_state,before_state,after_state,migration_correlation_ids,consumer_visibility
             FROM jsonb_populate_record(NULL::normalized_events,$1)",
        )
        .bind(event)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_raw(pool: &PgPool, fixture: &Value) -> TestResult {
    for block in fixture["blocks"].as_array().ok_or("blocks")? {
        sqlx::query(
            "INSERT INTO chain_lineage
             (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
             SELECT chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state
             FROM jsonb_populate_record(NULL::chain_lineage,$1)",
        )
        .bind(block)
        .execute(pool)
        .await?;
    }
    for transaction in fixture["transactions"].as_array().ok_or("transactions")? {
        sqlx::query(
            "INSERT INTO raw_transactions
             (chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address,input,value)
             SELECT chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address,
                    decode($1->>'input_hex','hex'),value
             FROM jsonb_populate_record(NULL::raw_transactions,$1)",
        )
        .bind(transaction)
        .execute(pool)
        .await?;
    }
    for log in fixture["raw_logs"].as_array().ok_or("logs")? {
        sqlx::query(
            "INSERT INTO raw_logs
             (chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data)
             SELECT chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,
                    decode($1->>'data_hex','hex') FROM jsonb_populate_record(NULL::raw_logs,$1)",
        )
        .bind(log)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn run_block(engine: &Engine, fixture: &Value, block: i64) -> TestResult {
    let outcome = engine
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: block,
            to_block: block,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let recorded = fixture["blocks"]
        .as_array()
        .ok_or("blocks")?
        .iter()
        .find(|row| row["block_number"] == block)
        .ok_or("recorded block")?;
    assert_eq!(outcome.current.number, block);
    assert_eq!(
        outcome.current.hash,
        recorded["block_hash"].as_str().unwrap()
    );
    Ok(())
}
