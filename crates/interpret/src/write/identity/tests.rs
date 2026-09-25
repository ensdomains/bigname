use bigname_adapters::schema_v2::{
    BindingClosure, MigrationCandidateEffect, NormalizedEvent, SurfaceBinding,
    seam::{
        MIGRATION_APPLIED_EVENT_KIND, SURFACE_BINDING_ID_KEY, SURFACE_BOUND_EVENT_KIND,
        SURFACE_UNBOUND_EVENT_KIND, TOKEN_CONTROL_TRANSFERRED_EVENT_KIND, raw_block_provenance,
    },
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const NAME: &str = "ens:0xname";
const CHILD: &str = "ens:0xchild";
const WRAPPER: &str = "0x0000000000000000000000000000000000000202";

async fn setup(pool: &sqlx::PgPool) -> TestResult {
    sqlx::raw_sql(include_str!(
        "../../../../../schema-v2/baseline/01_chain.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../../../schema-v2/baseline/03_identity.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../../../schema-v2/baseline/04_manifests.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../../../schema-v2/baseline/05_normalized_events.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES
             ('ethereum', '0x01', 1, '1970-01-01 00:00:01Z', 'canonical'),
             ('ethereum', '0x02', 2, '1970-01-01 00:00:02Z', 'canonical'),
             ('ethereum', '0x03', 3, '1970-01-01 00:00:03Z', 'canonical'),
             ('ethereum', '0x04', 4, '1970-01-01 00:00:04Z', 'canonical');
         INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state, chain_id,
             block_hash, block_number, canonicality_state
         ) VALUES
             ('ens:0xname', 'ens', 'name.eth', ARRAY['name','eth'], '\\x'::bytea,
              '0xname', ARRAY['0xlabel','0xeth'], 'test', 'active', 'ethereum',
              '0x01', 1, 'canonical'),
             ('ens:0xchild', 'ens', 'child.name.eth', ARRAY['child','name','eth'], '\\x'::bytea,
              '0xchild', ARRAY['0xchild','0xlabel','0xeth'], 'test', 'active', 'ethereum',
              '0x01', 1, 'canonical');
         INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) SELECT id, 'ethereum', '0x01', 1, 'canonical'
           FROM unnest(ARRAY[
             '00000000-0000-0000-0000-000000000001'::uuid,
             '00000000-0000-0000-0000-000000000002'::uuid,
             '00000000-0000-0000-0000-000000000003'::uuid,
             '00000000-0000-0000-0000-000000000004'::uuid,
             '00000000-0000-0000-0000-000000000005'::uuid,
             '00000000-0000-0000-0000-000000000006'::uuid
           ]) id;",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn database() -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new("interpret_authority")).await?;
    setup(database.pool()).await?;
    Ok(database)
}

async fn insert_binding(
    pool: &sqlx::PgPool,
    id: u128,
    name: &str,
    resource: u128,
    arm: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number,
             provenance, canonicality_state
         ) VALUES ($1, $2, $3, 'declared_registry_path', $4,
                   '1970-01-01 00:00:01Z', 'ethereum', '0x01', 1,
                   $5, 'canonical')",
    )
    .bind(Uuid::from_u128(id))
    .bind(name)
    .bind(Uuid::from_u128(resource))
    .bind(arm)
    .bind(json!({(TRANSACTION_INDEX_KEY):0,(LOG_INDEX_KEY):0}))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_registrar_contract(pool: &sqlx::PgPool) -> TestResult {
    let instance = Uuid::from_u128(101);
    let address = "0x0000000000000000000000000000000000000101";
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, 'ethereum', 'contract') ON CONFLICT DO NOTHING",
    )
    .bind(instance)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number
         ) SELECT $1, 'ethereum', $2, 0 WHERE NOT EXISTS (
             SELECT 1 FROM contract_instance_addresses WHERE contract_instance_id = $1
         )",
    )
    .bind(instance)
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_registrar_evidence(
    pool: &sqlx::PgPool,
    resource: u128,
    token_id: &str,
) -> TestResult {
    insert_registrar_contract(pool).await?;
    let address = "0x0000000000000000000000000000000000000101";
    sqlx::query(&format!(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, after_state
         ) VALUES ($1, 'ens', $2, $3, '{TOKEN_CONTROL_TRANSFERRED_EVENT_KIND}', 'test', 1,
                   'ethereum', 1, '0x01', jsonb_build_object('emitting_address', $4::text),
                   'ens_v1_unwrapped_authority', 'canonical',
                   jsonb_build_object('token_id', $5::text))",
    ))
    .bind(format!("registrar-evidence-{resource}"))
    .bind(NAME)
    .bind(Uuid::from_u128(resource))
    .bind(address)
    .bind(token_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn predecessor_evidence(
    event_identity: &str,
    resource: u128,
    emitting_address: &str,
    after_state: serde_json::Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(NAME.to_owned()),
        resource_id: Some(Uuid::from_u128(resource)),
        event_kind: TOKEN_CONTROL_TRANSFERRED_EVENT_KIND.to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: "ethereum".to_owned(),
        block_number: Some(1),
        block_hash: Some("0x01".to_owned()),
        transaction_hash: Some("0xprior".to_owned()),
        transaction_index: Some(0),
        log_index: Some(0),
        raw_fact_ref: json!({"emitting_address":emitting_address}),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: "canonical".to_owned(),
        before_state: json!({}),
        after_state,
        migration_correlation_ids: vec![],
        consumer_visibility: "activated".to_owned(),
        before_state_explicit: false,
    }
}

fn ordinary_open(id: u128, resource: u128, arm: &str, block: i64) -> BatchOutput {
    let binding_id = Uuid::from_u128(id);
    let open_time =
        time::OffsetDateTime::from_unix_timestamp(block).unwrap() + time::Duration::microseconds(1);
    BatchOutput {
        surface_bindings: vec![SurfaceBinding {
            surface_binding_id: binding_id,
            logical_name_id: NAME.to_owned(),
            resource_id: Uuid::from_u128(resource),
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: arm.to_owned(),
            active_from: open_time,
            chain_id: "ethereum".to_owned(),
            block_hash: format!("0x0{block}"),
            block_number: block,
            provenance: json!({(TRANSACTION_INDEX_KEY):0,(LOG_INDEX_KEY):1}),
            canonicality_state: "canonical".to_owned(),
        }],
        binding_closures: vec![BindingClosure {
            logical_name_id: NAME.to_owned(),
            authority_arm: arm.to_owned(),
            chain_id: "ethereum".to_owned(),
            except_surface_binding_id: Some(binding_id),
            active_to: open_time,
            block_number: block,
            transaction_index: 0,
            log_index: 1,
        }],
        ..BatchOutput::default()
    }
}

const REGISTRAR_CLEANUP_LOG_INDEX: i64 = 0;
const REGISTRAR_BOUNDARY_LOG_INDEX: i64 = 1;

fn registrar_selector() -> serde_json::Value {
    json!({
        "authority_epoch":"ens_v1",
        "logical_name_id":NAME,
        "selection":"active_immediately_before_predecessor_cleanup",
        "predecessor_cleanup":{
            "event_identity":"registrar-cleanup",
            "source_event":"Transfer",
            "block_number":2,
            (TRANSACTION_INDEX_KEY):0,
            (LOG_INDEX_KEY):REGISTRAR_CLEANUP_LOG_INDEX
        },
        "resource":{
            "anchor_kind":"registrar_backed_registration",
            "contract_instance_id":Uuid::from_u128(101).to_string(),
            "token_id":"0xexpected",
            "labelhash":"0xexpected",
            "selection":"current_registrar_resource_immediately_before_predecessor_cleanup"
        }
    })
}

fn legacy_registrar_selector() -> serde_json::Value {
    json!({
        "authority_epoch":"ens_v1",
        "logical_name_id":NAME,
        "selection":"active_immediately_before_boundary",
        "resource":{
            "anchor_kind":"registrar_backed_registration",
            "contract_instance_id":Uuid::from_u128(101).to_string(),
            "token_id":"0xexpected",
            "labelhash":"0xexpected",
            "selection":"current_registrar_resource_immediately_before_boundary"
        }
    })
}

fn registrar_cleanup_event() -> NormalizedEvent {
    let mut event = predecessor_evidence(
        "registrar-cleanup",
        1,
        "0x0000000000000000000000000000000000000101",
        json!({"source_event":"Transfer","token_id":"0xexpected"}),
    );
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.block_number = Some(2);
    event.block_hash = Some("0x02".to_owned());
    event.transaction_hash = Some("0xtx".to_owned());
    event.log_index = Some(REGISTRAR_CLEANUP_LOG_INDEX);
    event
}

fn wrapper_selector(contract_address: &str) -> serde_json::Value {
    json!({
        "authority_epoch":"ens_v1",
        "logical_name_id":NAME,
        "selection":"active_immediately_before_boundary",
        "resource":{
            "anchor_kind":"wrapper_backed_control",
            "contract_address":contract_address,
            "wrapper_token_id":"0xname",
            "namehash":"0xname",
            "selection":"current_wrapper_resource_immediately_before_boundary"
        }
    })
}

fn activate(output: &mut BatchOutput) -> TestResult {
    activate_with_selector(output, registrar_selector())?;
    output.normalized_events.push(registrar_cleanup_event());
    boundary_at_log_index(output, REGISTRAR_BOUNDARY_LOG_INDEX);
    Ok(())
}

fn activate_with_selector(
    output: &mut BatchOutput,
    predecessor_selector: serde_json::Value,
) -> TestResult {
    let successor = &output.surface_bindings[0];
    let correlation_id = "migration-correlation".to_owned();
    let proposed_effect = json!({
        "logical_name_id":NAME,
        "predecessor_binding":predecessor_selector,
        "successor_binding":{
            "authority_epoch":"ens_v2",
            "binding_id":successor.surface_binding_id.to_string(),
            "resource_id":successor.resource_id.to_string()
        }
    });
    output.normalized_events.push(NormalizedEvent {
        event_identity: "migration-boundary".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(NAME.to_owned()),
        resource_id: Some(successor.resource_id),
        event_kind: MIGRATION_APPLIED_EVENT_KIND.to_owned(),
        source_family: "ens_v2_migration_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: successor.chain_id.clone(),
        block_number: Some(successor.block_number),
        block_hash: Some("0x02".to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        transaction_index: Some(0),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: "ens_v2_migration".to_owned(),
        canonicality_state: "canonical".to_owned(),
        before_state: json!({}),
        after_state: proposed_effect.clone(),
        migration_correlation_ids: vec![correlation_id.clone()],
        consumer_visibility: "candidate".to_owned(),
        before_state_explicit: false,
    });
    output
        .migration_candidate_identity_effects
        .push(MigrationCandidateEffect {
            effect_identity: "migration-effect".to_owned(),
            migration_correlation_ids: vec![correlation_id],
            correlation_kind: "authority_transition".to_owned(),
            effect_kind: "surface_binding_transition".to_owned(),
            proposed_effect,
            evidence_refs: json!([]),
            chain_id: successor.chain_id.clone(),
            block_number: successor.block_number,
            block_hash: "0x02".to_owned(),
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 0,
            log_index: 0,
            canonicality_state: "canonical".to_owned(),
            consumer_visibility: "candidate".to_owned(),
        });
    bigname_adapters::schema_v2::inject_activated_transition_for_test(output)?;
    Ok(())
}

fn select_wrapper(output: &mut BatchOutput, contract_address: &str) {
    let selector = wrapper_selector(contract_address);
    output.migration_authority_transitions[0].predecessor_selector = selector.clone();
    output
        .normalized_events
        .iter_mut()
        .find(|event| event.event_kind == MIGRATION_APPLIED_EVENT_KIND)
        .expect("activated migration boundary")
        .after_state["predecessor_binding"] = selector;
}

async fn apply(pool: &sqlx::PgPool, output: &BatchOutput) -> crate::Result<()> {
    let mut write_output = output.clone();
    // Identity tests exercise the production writer composition but leave diagnostic-effect
    // persistence to migration writer tests, which provide the process content-hash stamp.
    write_output.migration_event_associations.clear();
    write_output.migration_discovery_associations.clear();
    write_output.migration_candidate_identity_effects.clear();
    write_output.migration_candidate_discovery_effects.clear();
    let expected_lineage = write_output
        .surface_bindings
        .iter()
        .map(|binding| (binding.block_number, binding.block_hash.clone()))
        .chain(
            write_output
                .normalized_events
                .iter()
                .filter_map(|event| Some((event.block_number?, event.block_hash.clone()?))),
        )
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    crate::write::batch(
        pool,
        "ethereum",
        None,
        false,
        false,
        0,
        &expected_lineage,
        &write_output,
    )
    .await
    .map(|_| ())
}

async fn close_binding(pool: &sqlx::PgPool, id: u128, micros: i64) -> TestResult {
    sqlx::query(
        "UPDATE surface_bindings
         SET active_to = timestamptz '1970-01-01 00:00:02Z' + $2 * interval '1 microsecond'
         WHERE surface_binding_id = $1",
    )
    .bind(Uuid::from_u128(id))
    .bind(micros)
    .execute(pool)
    .await?;
    Ok(())
}

/// Moves the boundary to a later log of its own transaction, so a predecessor closed earlier in
/// that same transaction is representable.
fn boundary_at_log_index(output: &mut BatchOutput, log_index: i64) {
    output.migration_authority_transitions[0].log_index = log_index;
    output
        .normalized_events
        .iter_mut()
        .find(|event| event.event_kind == MIGRATION_APPLIED_EVENT_KIND)
        .expect("activated migration boundary")
        .log_index = Some(log_index);
}

/// The writer resolves the predecessor as a binding still open at the boundary's own position:
/// `active_from` before it, and `active_to` unset or at/after it. A binding closed earlier in the
/// boundary's own transaction — which is what an emancipated child's ENSv1 unwrap does to its
/// wrapper binding — resolves to nothing, and the writer treats that as a data-integrity failure
/// rather than a no-op. This is why a child boundary records the position of its ENSv1 cleanup:
/// the binding slice 3B has to close is the one active immediately before that cleanup, not the one
/// active immediately before the registration.
#[tokio::test]
async fn a_predecessor_closed_earlier_in_the_boundary_transaction_resolves_to_none() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    output.normalized_events.push(predecessor_evidence(
        "same-batch-wrapper-evidence",
        1,
        WRAPPER,
        json!({"authority_kind":"wrapper","node":"0xname"}),
    ));
    activate_with_selector(&mut output, wrapper_selector(WRAPPER))?;
    boundary_at_log_index(&mut output, 5);

    close_binding(pool, 11, 2).await?;
    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(
        error.contains("0 active ENSv1 predecessors"),
        "a predecessor closed before the boundary is not resolvable: {error}"
    );

    // Closed at the boundary itself — the locked shape, whose wrapper token moves without the
    // binding ever closing early — still resolves.
    close_binding(pool, 11, 5).await?;
    apply(pool, &output).await?;
    assert_eq!(
        active_to(pool, 11).await?,
        Some(time::OffsetDateTime::from_unix_timestamp(2)? + time::Duration::microseconds(5)),
        "the resolved predecessor is closed at the boundary"
    );
    database.cleanup().await?;
    Ok(())
}

async fn active_to(pool: &sqlx::PgPool, id: u128) -> TestResult<Option<time::OffsetDateTime>> {
    Ok(
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE surface_binding_id = $1")
            .bind(Uuid::from_u128(id))
            .fetch_one(pool)
            .await?,
    )
}

async fn insert_close_event(
    pool: &sqlx::PgPool,
    kind: &str,
    opened: Option<u128>,
    log_index: i64,
    migration_shape: bool,
) -> TestResult {
    sqlx::query(&format!(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, after_state
         ) VALUES ('close-event', 'ens', $1, $2, 'test', 1, 'ethereum', 2,
                   '0x02', '0xtx', 0, $4, 'ens_v2_migration', 'canonical',
                   CASE WHEN $5 THEN jsonb_build_object(
                       'successor_binding', jsonb_build_object('binding_id', $3::text),
                       'predecessor_binding', jsonb_build_object('authority_epoch', 'ens_v1')
                   ) ELSE jsonb_build_object('{SURFACE_BINDING_ID_KEY}', $3::text) END)",
    ))
    .bind(NAME)
    .bind(kind)
    .bind(opened.map(Uuid::from_u128))
    .bind(log_index)
    .bind(migration_shape)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn candidate_migration_keeps_ordinary_cross_arm_bindings_inert() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    let output = ordinary_open(12, 2, "ens_v2", 2);
    assert!(output.migration_authority_transitions.is_empty());
    apply(pool, &output).await?;
    assert_eq!(active_to(pool, 11).await?, None);
    let active_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings WHERE logical_name_id = $1 AND active_to IS NULL",
    )
    .bind(NAME)
    .fetch_one(pool)
    .await?;
    assert_eq!(active_count, 2, "ENSv1 and ENSv2 bindings must coexist");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn activated_boundary_closes_exactly_one_ens_v1_predecessor() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_binding(pool, 13, CHILD, 3, "ens_v1").await?;
    insert_registrar_contract(pool).await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    output.normalized_events.push(predecessor_evidence(
        "same-batch-registrar-evidence",
        1,
        "0x0000000000000000000000000000000000000101",
        json!({"token_id":"0xexpected"}),
    ));
    activate(&mut output)?;
    // Restart before the successful registration write commits.
    let mut interrupted = pool.begin().await?;
    write_bindings(&mut interrupted, &output, false).await?;
    interrupted.rollback().await?;
    assert_eq!(active_to(pool, 11).await?, None);

    // Restart at the successful registration boundary: identity rows and current-batch
    // normalized evidence were attempted, but the atomic transaction never reached the close.
    let mut at_boundary = pool.begin().await?;
    write_rows(&mut at_boundary, &output, false).await?;
    crate::write::normalized::events(&mut at_boundary, &output.normalized_events).await?;
    let closed_in_flight: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE surface_binding_id = $1")
            .bind(Uuid::from_u128(11))
            .fetch_one(&mut *at_boundary)
            .await?;
    assert_eq!(closed_in_flight, None);
    at_boundary.rollback().await?;
    assert_eq!(active_to(pool, 11).await?, None);

    // Restart after the transaction: replay is idempotent and keeps the exact successor open.
    apply(pool, &output).await?;
    apply(pool, &output).await?;
    assert_eq!(active_to(pool, 11).await?.unwrap().unix_timestamp(), 2);
    assert_eq!(active_to(pool, 12).await?, None);
    assert_eq!(active_to(pool, 13).await?, None, "child authority changed");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn registrar_boundary_relative_selector_is_rejected() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_contract(pool).await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    output.normalized_events.push(predecessor_evidence(
        "same-batch-registrar-evidence",
        1,
        "0x0000000000000000000000000000000000000101",
        json!({"token_id":"0xexpected"}),
    ));
    activate_with_selector(&mut output, legacy_registrar_selector())?;

    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("invalid authority selector"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn registrar_boundary_refuses_inexact_cleanup_evidence() -> TestResult {
    for case in [
        "event identity",
        "source event",
        "position",
        "event kind",
        "emitter",
        "contract instance",
    ] {
        let database = database().await?;
        let pool = database.pool();
        insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
        insert_registrar_contract(pool).await?;
        let mut output = ordinary_open(12, 2, "ens_v2", 2);
        output.normalized_events.push(predecessor_evidence(
            "same-batch-registrar-evidence",
            1,
            "0x0000000000000000000000000000000000000101",
            json!({"token_id":"0xexpected"}),
        ));
        activate(&mut output)?;

        if case == "contract instance" {
            let wrong_instance = Uuid::from_u128(202).to_string();
            output.migration_authority_transitions[0].predecessor_selector["resource"]["contract_instance_id"] =
                json!(wrong_instance.clone());
            output
                .normalized_events
                .iter_mut()
                .find(|event| event.event_kind == MIGRATION_APPLIED_EVENT_KIND)
                .expect("activated migration boundary")
                .after_state["predecessor_binding"]["resource"]["contract_instance_id"] =
                json!(wrong_instance);
        } else {
            let cleanup = output
                .normalized_events
                .iter_mut()
                .find(|event| event.event_identity == "registrar-cleanup")
                .expect("registrar cleanup event");
            match case {
                "event identity" => cleanup.event_identity = "other-cleanup".to_owned(),
                "source event" => cleanup.after_state["source_event"] = json!("NameUnwrapped"),
                "position" => cleanup.log_index = Some(REGISTRAR_BOUNDARY_LOG_INDEX),
                "event kind" => cleanup.event_kind = SURFACE_UNBOUND_EVENT_KIND.to_owned(),
                "emitter" => {
                    cleanup.raw_fact_ref["emitting_address"] =
                        json!("0x0000000000000000000000000000000000000bad");
                }
                _ => unreachable!(),
            }
        }

        let error = apply(pool, &output).await.unwrap_err().to_string();
        assert!(
            error.contains("no exact ENSv1 predecessor cleanup"),
            "{case}: {error}"
        );
        assert_eq!(
            active_to(pool, 11).await?,
            None,
            "{case} must not close the ENSv1 predecessor"
        );
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn activated_boundary_rejects_zero_and_multiple_predecessors() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    // A binding alone is no lease: resource 1 has no registrar lifecycle event before the cleanup,
    // so the cleanup transfer, which every migration emits on the lease resource, vouches only
    // for a fallback binding positioned at the cleanup itself. An earlier grant of some other
    // resource of the name does not vouch for this one either.
    insert_registrar_contract(pool).await?;
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_lease_event(
        pool,
        "registrar-grant-6",
        6,
        "RegistrationGranted",
        0,
        None,
        json!({"source_event":"NameRegistered","namehash":"0xname","labelhash":"0xexpected"}),
    )
    .await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    let zero = apply(pool, &output).await.unwrap_err().to_string();
    assert!(zero.contains("0 active ENSv1 predecessors"), "{zero}");
    assert_eq!(active_to(pool, 11).await?, None);

    sqlx::query("ALTER TABLE surface_bindings DROP CONSTRAINT surface_bindings_no_overlap")
        .execute(pool)
        .await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    insert_binding(pool, 13, NAME, 3, "ens_v1").await?;
    insert_registrar_evidence(pool, 3, "0xexpected").await?;
    let multiple = apply(pool, &output).await.unwrap_err().to_string();
    assert!(
        multiple.contains("2 active ENSv1 predecessors"),
        "{multiple}"
    );
    database.cleanup().await?;
    Ok(())
}

/// A BaseRegistrar lifecycle event of `event_kind` on `resource` at block 1 log `log_index`,
/// emitted by the admitted registrar instance, with `after_state` merged over `token_id` when
/// one is given (a controller-derived grant on the Mainnet profile carries none).
async fn insert_lease_event(
    pool: &sqlx::PgPool,
    event_identity: &str,
    resource: u128,
    event_kind: &str,
    log_index: i64,
    token_id: Option<&str>,
    after_state: serde_json::Value,
) -> TestResult {
    insert_registrar_contract(pool).await?;
    let mut state = json!({});
    if let Some(token_id) = token_id {
        state["token_id"] = json!(token_id);
    }
    state
        .as_object_mut()
        .unwrap()
        .extend(after_state.as_object().unwrap().clone());
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             after_state
         ) VALUES ($1, 'ens', $2, $3, $4, 'ens_v1_registrar_l1', 1, 'ethereum', 1, '0x01',
                   '0xlease', 0, $5,
                   jsonb_build_object('emitting_address', '0x0000000000000000000000000000000000000101'),
                   'ens_v1_unwrapped_authority', 'canonical', $6)",
    )
    .bind(event_identity)
    .bind(NAME)
    .bind(Uuid::from_u128(resource))
    .bind(event_kind)
    .bind(log_index)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// A binding positioned at block 1 log `log_index`, active over `[from, to)` given in microseconds
/// after block 1.
async fn insert_binding_span(
    pool: &sqlx::PgPool,
    id: u128,
    resource: u128,
    log_index: i64,
    from_micros: i64,
    to_micros: Option<i64>,
) -> TestResult {
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, active_to, chain_id, block_hash, block_number,
             provenance, canonicality_state
         ) VALUES ($1, $2, $3, 'declared_registry_path', 'ens_v1',
                   timestamptz '1970-01-01 00:00:01Z' + $4 * interval '1 microsecond',
                   timestamptz '1970-01-01 00:00:01Z' + $5 * interval '1 microsecond',
                   'ethereum', '0x01', 1, $6, 'canonical')",
    )
    .bind(Uuid::from_u128(id))
    .bind(NAME)
    .bind(Uuid::from_u128(resource))
    .bind(from_micros)
    .bind(to_micros)
    .bind(json!({(TRANSACTION_INDEX_KEY):0,(LOG_INDEX_KEY):log_index}))
    .execute(pool)
    .await?;
    Ok(())
}

/// The lease of a registered `.eth` name ends only with the token. A BaseRegistrar transfer
/// without `reclaim` leaves the registry owner in place, so ordinary ENSv1 interpretation closes
/// the lease binding and binds the name to a registry-only resource while the lease goes on
/// under it (crates/project/src/builders/name_authority/stage.rs). The unlocked controller later
/// receives that token and reclaims the registry record for itself before parking both in the
/// Graveyard, so the token, not the registry-owner record, is what the boundary migrates.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L121 @ ens_v2@a971bd6)
///
/// The writer therefore finds the lease by its own token evidence, accepts that its binding was
/// closed by the handoff, and closes whatever ENSv1 binding is still open at the cleanup: here
/// the registry-only one.
#[tokio::test]
async fn a_registrar_boundary_resolves_the_lease_behind_a_registry_only_handoff() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    // The lease binding, closed by the handoff half a second after it opened.
    insert_binding_span(pool, 11, 1, 0, 0, Some(500_000)).await?;
    // The registry-only binding the handoff opened, still current at the cleanup.
    insert_binding_span(pool, 14, 4, 1, 500_000, None).await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;

    apply(pool, &output).await?;
    let handoff = time::OffsetDateTime::from_unix_timestamp(1)? + time::Duration::milliseconds(500);
    assert_eq!(
        active_to(pool, 11).await?,
        Some(handoff),
        "the lease binding keeps the close the handoff gave it"
    );
    assert_eq!(
        active_to(pool, 14).await?,
        Some(
            time::OffsetDateTime::from_unix_timestamp(2)?
                + time::Duration::microseconds(REGISTRAR_CLEANUP_LOG_INDEX)
        ),
        "the registry-only binding closes at the recorded registrar cleanup"
    );
    assert_eq!(active_to(pool, 12).await?, None, "the successor stays open");

    apply(pool, &output).await?;
    assert_eq!(active_to(pool, 11).await?, Some(handoff));
    database.cleanup().await?;
    Ok(())
}

/// A token released before the cleanup is no lease any more, whatever bindings say: the boundary
/// has no predecessor and the batch stops.
#[tokio::test]
async fn a_lease_released_before_the_cleanup_is_not_a_predecessor() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    insert_lease_event(
        pool,
        "registrar-release-1",
        1,
        "RegistrationReleased",
        1,
        Some("0xexpected"),
        json!({}),
    )
    .await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;

    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("0 active ENSv1 predecessors"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

/// A lapsed lease can be granted again with `registerOnly`, which mints a new token and writes
/// the expiry without touching the registry. Under a registry-only binding that successor lease
/// never gets a binding of its own (`crates/project/src/builders/name_authority/stage.rs`), and
/// the replaced lease was released before it. The successor is the token the controller receives,
/// so it is the predecessor: found by its evidence alone, with no binding to point at it.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
#[tokio::test]
async fn a_register_only_successor_lease_without_a_binding_is_the_predecessor() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    // The replaced lease: bound, handed off without reclaim, then released.
    insert_binding_span(pool, 11, 1, 0, 0, Some(500_000)).await?;
    insert_binding_span(pool, 14, 4, 1, 500_000, None).await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    insert_lease_event(
        pool,
        "registrar-release-1",
        1,
        "RegistrationReleased",
        1,
        Some("0xexpected"),
        json!({}),
    )
    .await?;
    // The successor lease, granted with `registerOnly` on a new resource and never bound.
    insert_lease_event(
        pool,
        "registrar-grant-5",
        5,
        "RegistrationGranted",
        2,
        Some("0xexpected"),
        json!({"source_event":"NameRegistered","namehash":"0xname","labelhash":"0xexpected"}),
    )
    .await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;

    apply(pool, &output).await?;
    assert_eq!(
        active_to(pool, 11).await?,
        Some(time::OffsetDateTime::from_unix_timestamp(1)? + time::Duration::milliseconds(500)),
        "the replaced lease binding keeps its handoff close"
    );
    assert_eq!(
        active_to(pool, 14).await?,
        Some(
            time::OffsetDateTime::from_unix_timestamp(2)?
                + time::Duration::microseconds(REGISTRAR_CLEANUP_LOG_INDEX)
        ),
        "the registry-only binding closes at the recorded registrar cleanup"
    );
    assert_eq!(active_to(pool, 12).await?, None);
    database.cleanup().await?;
    Ok(())
}

/// On the Mainnet deployment profile the controller's `RegistrationGranted` lands on the lease
/// but carries no token id, and the mint transfer from the zero address is not indexed, so a name
/// registered straight into the NameWrapper has no token-bearing event until `unwrapETH2LD` sends
/// the token from the NameWrapper to the Graveyard in the migration transaction itself. That
/// cleanup transfer is admissible evidence because the lease was observed before the transaction:
/// the earlier grant, token id or not, is what separates it from a lease the cleanup alone would
/// have to vouch for (`activated_boundary_rejects_zero_and_multiple_predecessors`).
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L128-L150 @ ens_v2@a971bd6)
#[tokio::test]
async fn a_lease_granted_without_a_token_id_is_found_by_its_cleanup_transfer() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_lease_event(
        pool,
        "registrar-grant-1",
        1,
        "RegistrationGranted",
        0,
        None,
        json!({"source_event":"NameRegistered","namehash":"0xname","labelhash":"0xexpected"}),
    )
    .await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;

    apply(pool, &output).await?;
    assert_eq!(
        active_to(pool, 11).await?,
        Some(
            time::OffsetDateTime::from_unix_timestamp(2)?
                + time::Duration::microseconds(REGISTRAR_CLEANUP_LOG_INDEX)
        ),
        "the lease binding closes at the recorded registrar cleanup"
    );
    assert_eq!(active_to(pool, 12).await?, None, "the successor stays open");

    apply(pool, &output).await?;
    assert_eq!(active_to(pool, 12).await?, None);
    database.cleanup().await?;
    Ok(())
}

/// An ENSv1 binding opened at the cleanup instant itself cannot be closed there, so it would
/// outlive the boundary. Ordinary interpretation of a complete migration transaction opens none
/// (the adapters reconcile the transaction first), so this shape is only constructible directly;
/// the writer must still refuse it rather than publish two current authorities.
#[tokio::test]
async fn a_binding_opened_at_the_cleanup_instant_stops_the_boundary() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    // Pre-write the successor and close the lease at the cleanup, as the boundary would, then
    // open another ENSv1 binding of the name at exactly that instant and position.
    apply(pool, &output).await?;
    sqlx::query(&format!(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
             active_from, chain_id, block_hash, block_number, provenance, canonicality_state
         ) VALUES ($1, $2, $3, 'declared_registry_path', 'ens_v1',
                   timestamptz '1970-01-01 00:00:02Z' + $4 * interval '1 microsecond',
                   'ethereum', '0x02', 2,
                   jsonb_build_object('{TRANSACTION_INDEX_KEY}', 0, '{LOG_INDEX_KEY}', $4),
                   'canonical')",
    ))
    .bind(Uuid::from_u128(15))
    .bind(NAME)
    .bind(Uuid::from_u128(1))
    .bind(REGISTRAR_CLEANUP_LOG_INDEX)
    .execute(pool)
    .await?;

    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(
        error.contains("leaves 1 ENSv1 bindings open at its predecessor cleanup"),
        "{error}"
    );
    assert_eq!(
        active_to(pool, 15).await?,
        None,
        "the writer does not clamp it shut"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn activated_boundary_rejects_a_wrong_singleton_selector() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xwrong").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("0 active ENSv1 predecessors"), "{error}");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn transition_requires_its_exact_activated_boundary() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    output.normalized_events[0].consumer_visibility = "candidate".to_owned();
    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(
        error.contains("exact activated MigrationApplied"),
        "{error}"
    );
    assert_eq!(active_to(pool, 11).await?, None);

    output.normalized_events[0].consumer_visibility = "activated".to_owned();
    output.normalized_events[0].after_state["successor_binding"]["authority_epoch"] =
        json!("ens_v1");
    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(
        error.contains("exact activated MigrationApplied"),
        "{error}"
    );
    assert_eq!(active_to(pool, 11).await?, None);

    output.normalized_events[0].after_state["successor_binding"]["authority_epoch"] =
        json!("ens_v2");
    output.migration_authority_transitions[0].predecessor_selector["authority_epoch"] =
        json!("ens_v2");
    output.normalized_events[0].after_state["predecessor_binding"]["authority_epoch"] =
        json!("ens_v2");
    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("invalid authority selector"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn activated_boundary_requires_its_exact_transition() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    output.migration_authority_transitions.clear();

    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("exact authority transitions"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn activated_boundary_rejects_duplicate_exact_transitions() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    output
        .migration_authority_transitions
        .push(output.migration_authority_transitions[0].clone());

    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("2 exact authority transitions"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn transition_requires_activated_exact_name_predecessor_evidence() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;

    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['candidate-predecessor-evidence']
         WHERE event_identity = 'registrar-evidence-1'",
    )
    .execute(pool)
    .await?;
    let candidate = apply(pool, &output).await.unwrap_err().to_string();
    assert!(
        candidate.contains("0 active ENSv1 predecessors"),
        "{candidate}"
    );
    assert_eq!(active_to(pool, 11).await?, None);

    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'activated',
             migration_correlation_ids = '{}',
             logical_name_id = $1
         WHERE event_identity = 'registrar-evidence-1'",
    )
    .bind(CHILD)
    .execute(pool)
    .await?;
    let other_name = apply(pool, &output).await.unwrap_err().to_string();
    assert!(
        other_name.contains("0 active ENSv1 predecessors"),
        "{other_name}"
    );
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn transition_rejects_predecessor_evidence_after_boundary() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    sqlx::query(
        "UPDATE normalized_events
         SET block_number = 3, block_hash = '0x03'
         WHERE event_identity = 'registrar-evidence-1'",
    )
    .execute(pool)
    .await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;

    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("0 active ENSv1 predecessors"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn transition_orders_predecessor_evidence_by_full_same_block_position() -> TestResult {
    for (evidence_log_index, eligible) in [(0_i64, true), (1, false), (2, false)] {
        let database = database().await?;
        let pool = database.pool();
        insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
        insert_registrar_evidence(pool, 1, "0xexpected").await?;
        sqlx::query(
            "UPDATE normalized_events
             SET block_number = 2,
                 block_hash = '0x02',
                 transaction_hash = '0xprior',
                 transaction_index = 1,
                 log_index = $1
             WHERE event_identity = 'registrar-evidence-1'",
        )
        .bind(evidence_log_index)
        .execute(pool)
        .await?;
        let mut output = ordinary_open(12, 2, "ens_v2", 2);
        output.surface_bindings[0].provenance[TRANSACTION_INDEX_KEY] = json!(1);
        activate(&mut output)?;
        output.migration_authority_transitions[0].transaction_index = 1;
        output.migration_authority_transitions[0].log_index = 2;
        output.migration_authority_transitions[0].predecessor_selector["predecessor_cleanup"]
            [TRANSACTION_INDEX_KEY] = json!(1);
        output.migration_authority_transitions[0].predecessor_selector["predecessor_cleanup"]
            [LOG_INDEX_KEY] = json!(1);
        let cleanup = output
            .normalized_events
            .iter_mut()
            .find(|event| event.event_identity == "registrar-cleanup")
            .expect("registrar cleanup event");
        cleanup.transaction_index = Some(1);
        cleanup.log_index = Some(1);
        let boundary = output
            .normalized_events
            .iter_mut()
            .find(|event| event.event_kind == MIGRATION_APPLIED_EVENT_KIND)
            .expect("activated migration boundary");
        boundary.transaction_index = Some(1);
        boundary.log_index = Some(2);
        boundary.after_state["predecessor_binding"]["predecessor_cleanup"][TRANSACTION_INDEX_KEY] =
            json!(1);
        boundary.after_state["predecessor_binding"]["predecessor_cleanup"][LOG_INDEX_KEY] =
            json!(1);

        let result = apply(pool, &output).await;
        if eligible {
            result?;
            assert!(active_to(pool, 11).await?.is_some());
        } else {
            let error = result.unwrap_err().to_string();
            assert!(error.contains("0 active ENSv1 predecessors"), "{error}");
            assert_eq!(active_to(pool, 11).await?, None);
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn transition_rejects_predecessor_evidence_on_orphaned_lineage() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum' AND block_number = 1",
    )
    .execute(pool)
    .await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;

    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("0 active ENSv1 predecessors"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn wrapper_selector_is_enforced_by_the_transition_writer() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    output.normalized_events.push(predecessor_evidence(
        "same-batch-wrapper-evidence",
        1,
        WRAPPER,
        json!({"authority_kind":"wrapper","node":"0xname"}),
    ));
    activate_with_selector(&mut output, wrapper_selector(WRAPPER))?;
    select_wrapper(&mut output, "0x0000000000000000000000000000000000000bad");
    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("0 active ENSv1 predecessors"), "{error}");
    assert_eq!(active_to(pool, 11).await?, None);

    select_wrapper(&mut output, WRAPPER);
    apply(pool, &output).await?;
    assert!(active_to(pool, 11).await?.is_some());
    database.cleanup().await?;
    Ok(())
}

const CLEANUP_LOG_INDEX: i64 = 3;
const BOUNDARY_LOG_INDEX: i64 = 5;

/// The shape slice 3A records for a direct child: its own anchor in the ENSv1 NameWrapper, and a
/// predecessor selected against the child's ENSv1 cleanup rather than the ENSv2 registration.
fn child_selector() -> serde_json::Value {
    child_selector_for("NameUnwrapped")
}

/// `locked_child` parks the wrapper token, which closes nothing; `emancipated_child` unwraps the
/// node, which closes the ENSv1 wrapper binding at that same log.
fn child_selector_for(cleanup_source_event: &str) -> serde_json::Value {
    json!({
        "authority_epoch":"ens_v1",
        "logical_name_id":NAME,
        "selection":"active_immediately_before_predecessor_cleanup",
        "predecessor_cleanup":{
            "event_identity":"child-cleanup",
            "source_event":cleanup_source_event,
            "block_number":2,
            (TRANSACTION_INDEX_KEY):0,
            (LOG_INDEX_KEY):CLEANUP_LOG_INDEX
        },
        "resource":{
            "anchor_kind":"wrapper_backed_child_control",
            "contract_address":WRAPPER,
            "wrapper_token_id":"0xname",
            "namehash":"0xname",
            "parent_namehash":"0xparent",
            "labelhash":"0xlabel",
            "parent_migration_correlation_id":"0xparentcorrelation",
            "selection":"current_wrapper_resource_immediately_before_predecessor_cleanup"
        }
    })
}

fn cleanup_event(source_event: &str, log_index: i64) -> NormalizedEvent {
    let mut event = predecessor_evidence(
        "child-cleanup",
        1,
        WRAPPER,
        json!({"source_event":source_event,"authority_kind":"wrapper","node":"0xname"}),
    );
    event.block_number = Some(2);
    event.block_hash = Some("0x02".to_owned());
    event.transaction_hash = Some("0xtx".to_owned());
    event.log_index = Some(log_index);
    event
}

/// One activated child boundary with its recorded cleanup, the ENSv1 wrapper evidence its anchor
/// resolves through, and the ENSv2 successor the registration opens.
fn child_activation(selector: serde_json::Value) -> TestResult<BatchOutput> {
    child_activation_for(selector, "NameUnwrapped")
}

fn child_activation_for(
    selector: serde_json::Value,
    cleanup_source_event: &str,
) -> TestResult<BatchOutput> {
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    output.normalized_events.push(predecessor_evidence(
        "same-batch-wrapper-evidence",
        1,
        WRAPPER,
        json!({"authority_kind":"wrapper","node":"0xname"}),
    ));
    output
        .normalized_events
        .push(cleanup_event(cleanup_source_event, CLEANUP_LOG_INDEX));
    activate_with_selector(&mut output, selector)?;
    boundary_at_log_index(&mut output, BOUNDARY_LOG_INDEX);
    Ok(output)
}

/// A child's ENSv1 authority ends at its cleanup, which precedes the registration in the same
/// transaction. The activated transition therefore resolves and closes the predecessor there: the
/// emancipated shape's unwrap already closed the wrapper binding at that log, and closing at the
/// later registration would re-extend it.
#[tokio::test]
async fn an_activated_child_boundary_closes_its_predecessor_at_the_recorded_cleanup() -> TestResult
{
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    let output = child_activation(child_selector())?;

    // Closed exactly at the cleanup still resolves; one microsecond earlier does not.
    close_binding(pool, 11, CLEANUP_LOG_INDEX - 1).await?;
    let error = apply(pool, &output).await.unwrap_err().to_string();
    assert!(error.contains("0 active ENSv1 predecessors"), "{error}");

    close_binding(pool, 11, CLEANUP_LOG_INDEX).await?;
    apply(pool, &output).await?;
    assert_eq!(
        active_to(pool, 11).await?,
        Some(
            time::OffsetDateTime::from_unix_timestamp(2)?
                + time::Duration::microseconds(CLEANUP_LOG_INDEX)
        ),
        "the predecessor closes at the cleanup, not at the boundary"
    );
    database.cleanup().await?;
    Ok(())
}

/// The recorded cleanup is the wire input the whole child rule rests on, so every field of it has
/// to match an event that exists exactly as described. "Some earlier wrapper event" is not
/// equivalent evidence, and neither selector may be resolved through the other's rule.
#[tokio::test]
async fn a_child_boundary_refuses_inexact_cleanup_evidence() -> TestResult {
    let boundary_relative = {
        let mut selector = child_selector();
        selector["selection"] = json!("active_immediately_before_boundary");
        selector
    };
    let second_level_anchor = {
        let mut selector = child_selector();
        selector["resource"]["anchor_kind"] = json!("wrapper_backed_control");
        selector
    };
    let cases: Vec<(&str, serde_json::Value, &str)> = vec![
        (
            "boundary-relative child anchor",
            boundary_relative,
            "invalid authority selector",
        ),
        (
            "second-level anchor with a cleanup",
            second_level_anchor,
            "invalid authority selector",
        ),
        (
            "cleanup at the registration",
            mutate(|selector| {
                selector["predecessor_cleanup"][LOG_INDEX_KEY] = json!(BOUNDARY_LOG_INDEX);
            }),
            "invalid predecessor resource selector",
        ),
        (
            "cleanup in another transaction",
            mutate(|selector| {
                selector["predecessor_cleanup"][TRANSACTION_INDEX_KEY] = json!(1);
            }),
            "invalid predecessor resource selector",
        ),
        (
            "missing parent evidence",
            mutate(|selector| {
                selector["resource"]["parent_migration_correlation_id"] = json!("");
            }),
            "invalid predecessor resource selector",
        ),
        (
            "wrong cleanup event identity",
            mutate(|selector| {
                selector["predecessor_cleanup"]["event_identity"] = json!("other-cleanup");
            }),
            "no exact ENSv1 predecessor cleanup",
        ),
        (
            "wrong cleanup source event",
            mutate(|selector| {
                selector["predecessor_cleanup"]["source_event"] = json!("TransferSingle");
            }),
            "no exact ENSv1 predecessor cleanup",
        ),
        (
            "wrong cleanup log index",
            mutate(|selector| {
                selector["predecessor_cleanup"][LOG_INDEX_KEY] = json!(CLEANUP_LOG_INDEX - 1);
            }),
            "no exact ENSv1 predecessor cleanup",
        ),
        (
            "wrong wrapper anchor",
            mutate(|selector| {
                selector["resource"]["contract_address"] =
                    json!("0x0000000000000000000000000000000000000bad");
            }),
            "no exact ENSv1 predecessor cleanup",
        ),
    ];
    for (case, selector, expected) in cases {
        let database = database().await?;
        let pool = database.pool();
        insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
        let output = child_activation(selector)?;
        let error = apply(pool, &output).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{case}: {error}");
        assert_eq!(
            active_to(pool, 11).await?,
            None,
            "{case} must not close the ENSv1 predecessor"
        );
        database.cleanup().await?;
    }
    Ok(())
}

fn mutate(edit: impl FnOnce(&mut serde_json::Value)) -> serde_json::Value {
    let mut selector = child_selector();
    edit(&mut selector);
    selector
}

#[tokio::test]
async fn ordinary_opens_close_only_their_own_arm() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_binding(pool, 21, NAME, 3, "ens_v2").await?;
    apply(pool, &ordinary_open(12, 2, "ens_v1", 2)).await?;
    assert!(active_to(pool, 11).await?.is_some());
    assert_eq!(active_to(pool, 21).await?, None, "ENSv1 open closed ENSv2");
    apply(pool, &ordinary_open(22, 4, "ens_v2", 3)).await?;
    assert!(active_to(pool, 21).await?.is_some());
    assert_eq!(active_to(pool, 12).await?, None, "ENSv2 open closed ENSv1");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn redo_reopens_an_activated_migration_predecessor() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    apply(pool, &output).await?;
    insert_close_event(pool, MIGRATION_APPLIED_EVENT_KIND, Some(12), 0, true).await?;

    let mut transaction = pool.begin().await?;
    crate::write::orphan_bindings_started_in_range(&mut transaction, "ethereum", 2, 2).await?;
    crate::write::reopen_bindings_closed_in_range(&mut transaction, "ethereum", 2, 2).await?;
    transaction.commit().await?;
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn partial_redo_ignores_a_surviving_successor_in_another_arm() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    apply(pool, &ordinary_open(12, 2, "ens_v1", 2)).await?;
    apply(pool, &ordinary_open(21, 3, "ens_v2", 3)).await?;
    insert_close_event(pool, SURFACE_BOUND_EVENT_KIND, Some(12), 1, false).await?;

    let mut transaction = pool.begin().await?;
    crate::write::orphan_bindings_started_in_range(&mut transaction, "ethereum", 2, 2).await?;
    crate::write::reopen_bindings_closed_in_range(&mut transaction, "ethereum", 2, 2).await?;
    transaction.commit().await?;
    assert_eq!(active_to(pool, 11).await?, None);
    database.cleanup().await?;
    Ok(())
}

/// A redo reopen undoes the close a closing event caused, so it has to look for that close where
/// the event actually made it. A child migration boundary closes its ENSv1 predecessor at the
/// cleanup it records, earlier in its own transaction, not at its own log.
///
/// The `locked_child` shape is the one that proves it: parking the wrapper token closes nothing, so
/// the activated transition is the only close, and a reopen keyed on the boundary's own log finds
/// no row to undo. `emancipated_child` survives a boundary-keyed reopen only incidentally, through
/// the unwrap's own `SurfaceUnbound` at that same cleanup log — so both shapes are pinned here.
#[tokio::test]
async fn redo_reopens_a_child_predecessor_closed_at_its_cleanup() -> TestResult {
    for (case, cleanup_source_event, unbind_at_cleanup) in [
        ("locked_child", "TransferSingle", false),
        ("emancipated_child", "NameUnwrapped", true),
    ] {
        let database = database().await?;
        let pool = database.pool();
        insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
        let output = child_activation_for(
            child_selector_for(cleanup_source_event),
            cleanup_source_event,
        )?;
        close_binding(pool, 11, CLEANUP_LOG_INDEX).await?;
        apply(pool, &output).await?;
        let closed_at = active_to(pool, 11).await?.expect("the predecessor closed");
        assert_eq!(
            closed_at,
            time::OffsetDateTime::from_unix_timestamp(2)?
                + time::Duration::microseconds(CLEANUP_LOG_INDEX),
            "{case} closes at its cleanup"
        );
        if unbind_at_cleanup {
            insert_unbind_at(pool, 1, CLEANUP_LOG_INDEX).await?;
        }

        let mut transaction = pool.begin().await?;
        crate::write::orphan_bindings_started_in_range(&mut transaction, "ethereum", 2, 2).await?;
        crate::write::reopen_bindings_closed_in_range(&mut transaction, "ethereum", 2, 2).await?;
        transaction.commit().await?;
        assert_eq!(
            active_to(pool, 11).await?,
            None,
            "{case} predecessor must reopen when its boundary is redone"
        );
        database.cleanup().await?;
    }
    Ok(())
}

/// Re-deriving the same corpus after a redo has to land on the same close, so a redo that reopens
/// and re-applies converges instead of drifting.
#[tokio::test]
async fn a_child_close_survives_reopen_and_identical_redo() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    let output = child_activation_for(child_selector_for("TransferSingle"), "TransferSingle")?;
    close_binding(pool, 11, CLEANUP_LOG_INDEX).await?;
    apply(pool, &output).await?;
    let first = active_to(pool, 11).await?;

    let mut transaction = pool.begin().await?;
    crate::write::orphan_bindings_started_in_range(&mut transaction, "ethereum", 2, 2).await?;
    crate::write::reopen_bindings_closed_in_range(&mut transaction, "ethereum", 2, 2).await?;
    transaction.commit().await?;
    assert_eq!(active_to(pool, 11).await?, None);

    apply(pool, &output).await?;
    assert_eq!(
        active_to(pool, 11).await?,
        first,
        "an identical re-derivation converges on the same close"
    );
    database.cleanup().await?;
    Ok(())
}

/// The ENSv1 unwrap the emancipated shape performs, which closes the wrapper binding at the cleanup
/// log on its own.
async fn insert_unbind_at(pool: &sqlx::PgPool, resource: u128, log_index: i64) -> TestResult {
    sqlx::query(&format!(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             after_state
         ) VALUES ('child-unbind', 'ens', $1, $2, '{SURFACE_UNBOUND_EVENT_KIND}',
                   'ens_v1_wrapper_l1', 1, 'ethereum', 2, '0x02', '0xtx', 0, $3, '{{}}'::jsonb,
                   'ens_v1_unwrapped_authority', 'canonical',
                   jsonb_build_object('source_event', 'NameUnwrapped'))",
    ))
    .bind(NAME)
    .bind(Uuid::from_u128(resource))
    .bind(log_index)
    .execute(pool)
    .await?;
    Ok(())
}

/// A redo reopen undoes the close a closing event caused, so it must match the arm that event's own
/// evidence names. Two bindings of one name closed at the same instant on different arms are
/// distinguishable only by that predicate ([#471](https://github.com/ensdomains/bigname/issues/471)).
#[tokio::test]
async fn redo_reopen_matches_the_closing_event_arm() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    insert_binding(pool, 21, NAME, 3, "ens_v2").await?;
    // One ENSv1 open closes the ENSv1 predecessor; the ENSv2 binding is closed at the same instant
    // by hand, which is the coincidence the arm-blind join could not tell apart.
    apply(pool, &ordinary_open(12, 2, "ens_v1", 2)).await?;
    let clamp = active_to(pool, 11)
        .await?
        .expect("ENSv1 predecessor closed");
    sqlx::query("UPDATE surface_bindings SET active_to = $2 WHERE surface_binding_id = $1")
        .bind(Uuid::from_u128(21))
        .bind(clamp)
        .execute(pool)
        .await?;
    insert_close_event(pool, SURFACE_BOUND_EVENT_KIND, Some(12), 1, false).await?;

    let mut transaction = pool.begin().await?;
    crate::write::orphan_bindings_started_in_range(&mut transaction, "ethereum", 2, 2).await?;
    crate::write::reopen_bindings_closed_in_range(&mut transaction, "ethereum", 2, 2).await?;
    transaction.commit().await?;
    assert_eq!(
        active_to(pool, 11).await?,
        None,
        "the closing event's own arm reopens"
    );
    assert_eq!(
        active_to(pool, 21).await?,
        Some(clamp),
        "the other arm's binding stays closed"
    );
    database.cleanup().await?;
    Ok(())
}

/// The binding upsert's identity guard is loud, not silent: a same-identifier row whose name,
/// resource, kind, or arm disagrees updates nothing and the writer raises rather than continuing
/// ([#471](https://github.com/ensdomains/bigname/issues/471)'s second finding).
#[tokio::test]
async fn a_conflicting_binding_identity_fails_loudly() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
    let error = apply(pool, &ordinary_open(11, 1, "ens_v2", 2))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("already bound to different identity data"),
        "{error}"
    );
    let arm: String = sqlx::query_scalar(
        "SELECT authority_arm FROM surface_bindings WHERE surface_binding_id=$1",
    )
    .bind(Uuid::from_u128(11))
    .fetch_one(pool)
    .await?;
    assert_eq!(arm, "ens_v1", "the conflicting write changed nothing");
    database.cleanup().await?;
    Ok(())
}

#[test]
fn raw_block_binding_open_orders_before_the_first_log() {
    let binding = SurfaceBinding {
        surface_binding_id: Uuid::nil(),
        logical_name_id: "ens:0x00".to_owned(),
        resource_id: Uuid::nil(),
        binding_kind: "declared_registry_path".to_owned(),
        authority_arm: "ens_v1".to_owned(),
        active_from: time::OffsetDateTime::UNIX_EPOCH,
        chain_id: "chain".to_owned(),
        block_hash: "block".to_owned(),
        block_number: 7,
        provenance: raw_block_provenance(),
        canonicality_state: "canonical".to_owned(),
    };
    let closure = BindingClosure {
        logical_name_id: binding.logical_name_id.clone(),
        authority_arm: binding.authority_arm.clone(),
        chain_id: binding.chain_id.clone(),
        except_surface_binding_id: None,
        active_to: time::OffsetDateTime::UNIX_EPOCH,
        block_number: 7,
        transaction_index: 0,
        log_index: 0,
    };
    assert!(
        BindingOperation::Open(&binding).order_key()
            < BindingOperation::Close(&closure).order_key()
    );
}

use bigname_adapters::schema_v2 as adapter_api;
#[path = "../../../../adapters/tests/fixtures/interpreters/numeric_short_lease.rs"]
mod numeric_short_lease_fixture;

mod numeric_short_lease_connected {
    use super::numeric_short_lease_fixture as fixture;
    use anyhow::{Context, Result};
    use bigname_adapters::schema_v2::{
        self as adapter, BatchInput, BatchOutput, StateCacheCapacity,
    };
    use bigname_project::{BatchRequest, Engine, Marker, RunMode};
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::Value;
    use sqlx::{PgPool, types::Uuid};

    async fn database(input: &BatchInput) -> Result<TestDatabase> {
        let database = TestDatabase::create(TestDatabaseConfig::new("numeric_short_lease")).await?;
        let pool = database.pool();
        sqlx::raw_sql("CREATE SCHEMA bigname_phase")
            .execute(pool)
            .await?;
        pool.set_connect_options(
            pool.connect_options()
                .as_ref()
                .clone()
                .options([("search_path", "bigname_phase,public")]),
        );
        let mut connections = Vec::new();
        for _ in 0..pool.options().get_max_connections() {
            let mut connection = pool.acquire().await?;
            sqlx::raw_sql("SET search_path TO bigname_phase, public")
                .execute(&mut *connection)
                .await?;
            connections.push(connection);
        }
        drop(connections);
        for script in [
            include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
            include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
            include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
            include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
            include_str!("../../../../../schema-v2/baseline/08_heartbeats.sql"),
            include_str!("../../../../../schema-v2/baseline/09_divergence.sql"),
            include_str!("../../../../../schema-v2/baseline/10_phase_state.sql"),
            include_str!(
                "../../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"
            ),
            include_str!("../../../../../schema-v2/baseline/12_project_generation_failures.sql"),
            include_str!("../../../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
            include_str!("../../../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
        ] {
            sqlx::raw_sql(script).execute(pool).await?;
        }
        for block in &input.blocks {
            sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,$4,$5::canonicality_state)")
                .bind(&block.chain_id).bind(&block.block_hash).bind(block.block_number)
                .bind(block.block_timestamp).bind(&block.canonicality_state).execute(pool).await?;
        }
        // Only declared fixture inputs are seeded. Identity, events and projections come from adapters.
        for manifest in &input.manifests {
            let payload: Value = serde_json::from_str(&manifest.payload_json)?;
            sqlx::query("INSERT INTO manifest_versions (manifest_id,manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) OVERRIDING SYSTEM VALUE VALUES ($1,$2,$3,$4,$5,$6,'active',$7,$8,$9)")
                .bind(manifest.manifest_id).bind(manifest.manifest_version).bind(&manifest.namespace)
                .bind(&manifest.source_family).bind(&manifest.chain_id).bind(&manifest.deployment_label)
                .bind(&manifest.normalizer_version).bind(format!("fixture/{}",manifest.source_family))
                .bind(payload).execute(pool).await?;
        }
        for admission in &input.admissions {
            sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1,$2,'contract')")
                .bind(admission.contract_instance_id).bind(fixture::CHAIN).execute(pool).await?;
            sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,source_manifest_id) VALUES ($1,$2,$3,0,$4)")
                .bind(admission.contract_instance_id).bind(fixture::CHAIN).bind(&admission.address)
                .bind(admission.source_manifest_id).execute(pool).await?;
        }
        Ok(database)
    }

    async fn write(pool: &PgPool, input: &BatchInput, output: &BatchOutput) -> Result<()> {
        let mut output = output.clone();
        // Existing identity-writer tests isolate diagnostic persistence in the same way.
        output.migration_event_associations.clear();
        output.migration_discovery_associations.clear();
        output.migration_candidate_identity_effects.clear();
        output.migration_candidate_discovery_effects.clear();
        let lineage = input
            .blocks
            .iter()
            .map(|block| (block.block_number, block.block_hash.clone()))
            .collect::<Vec<_>>();
        crate::write::batch(
            pool,
            fixture::CHAIN,
            None,
            false,
            false,
            0,
            &lineage,
            &output,
        )
        .await?;
        Ok(())
    }

    async fn project(
        pool: &PgPool,
        from: i64,
        target: i64,
        previous: Option<Marker>,
    ) -> Result<Marker> {
        Ok(Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: fixture::CHAIN.to_owned(),
                target_block: target,
                affected_from_block: from,
                affected_to_block: target,
                resume_current: previous,
                mode: RunMode::Normal,
            })
            .await?
            .current)
    }

    async fn summary(pool: &PgPool, logical: &str) -> Result<Value> {
        Ok(
            sqlx::query_scalar(
                "SELECT declared_summary FROM name_current WHERE logical_name_id=$1",
            )
            .bind(logical)
            .fetch_one(pool)
            .await?,
        )
    }

    async fn assert_pre(pool: &PgPool, expected: &Value, resource: Uuid) -> Result<Value> {
        let logical = format!("ens:{}", expected["node"].as_str().unwrap());
        let summary = summary(pool, &logical).await?;
        assert_eq!(
            summary["registration"]["registrant"],
            expected["expected_owner"]
        );
        // The public name record's `owner` is read from `control.registry_owner`; a disclosed
        // numeric lease must publish the proven registry owner, not only the registrant.
        assert_eq!(
            summary["control"]["registry_owner"],
            expected["expected_owner"]
        );
        assert_eq!(summary["registration"]["expiry"], expected["v1_expiry"]);
        assert_eq!(
            summary
                .pointer("/resolver/address")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            expected["expected_resolver"]
        );
        let registered_at: i64 = sqlx::query_scalar("SELECT extract(epoch FROM (declared_summary #>> '{registration,registered_at}')::timestamptz)::bigint FROM name_current WHERE logical_name_id=$1")
            .bind(&logical).fetch_one(pool).await?;
        assert_eq!(
            registered_at,
            expected["registration_timestamp"].as_i64().unwrap()
        );
        let permissions: i64 = sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id=$1 AND lower(subject)=$2 AND jsonb_array_length(effective_powers)>0")
            .bind(resource).bind(expected["expected_owner"].as_str().unwrap()).fetch_one(pool).await?;
        assert!(
            permissions > 0,
            "current owner has no projected ownership permissions"
        );
        let registry_owner: String = sqlx::query_scalar(
            "SELECT registry_owner FROM permissions_current_resource_summary WHERE resource_id=$1",
        )
        .bind(resource)
        .fetch_one(pool)
        .await?;
        assert_eq!(registry_owner, expected["expected_owner"].as_str().unwrap());
        let inventory: Value =
            sqlx::query_scalar("SELECT entries FROM record_inventory_current WHERE resource_id=$1")
                .bind(resource)
                .fetch_one(pool)
                .await?;
        for (key, value) in expected["expected_records"].as_object().unwrap() {
            let record = inventory
                .as_array()
                .context("record inventory entries")?
                .iter()
                .find(|record| record["record_key"] == *key)
                .with_context(|| format!("record {key}"))?;
            assert_eq!(&record["value"], value, "pre-migration record {key}");
        }
        Ok(summary)
    }

    #[tokio::test]
    async fn declared_controller_enrichment_opens_one_registrar_binding_through_writer()
    -> Result<()> {
        use alloy_primitives::{U256, keccak256};
        use alloy_sol_types::{SolEvent, sol};
        use serde_json::json;
        sol! { event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires); }
        let captured = fixture::input()?;
        let expected = fixture::fixture()?;
        let mut input = fixture::range(&captured, 403, 407);
        // Explicit synthetic controller variant: retain numeric lease and registry setup,
        // replace the later V2 receipt with a declared readable controller observation.
        let mut raw = input
            .raw_logs
            .iter()
            .find(|raw| raw.block_number == 407)
            .unwrap()
            .clone();
        input.raw_logs.retain(|raw| raw.block_number < 407);
        let controller = "0x0000000000000000000000000000000000000099";
        let label = expected["name"]
            .as_str()
            .unwrap()
            .strip_suffix(".eth")
            .unwrap();
        let log = NameRegistered {
            name: label.to_owned(),
            label: keccak256(label.as_bytes()),
            owner: expected["expected_owner"].as_str().unwrap().parse()?,
            expires: U256::from(expected["v1_expiry"].as_i64().unwrap() as u64),
        }
        .encode_log_data();
        raw.emitting_address = controller.to_owned();
        raw.topics = log
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect();
        raw.data = log.data.to_vec();
        raw.log_index = 0;
        input.raw_logs.push(raw);
        let manifest = input
            .manifests
            .iter_mut()
            .find(|manifest| manifest.source_family == "ens_v1_registrar_l1")
            .unwrap();
        let mut payload: Value = serde_json::from_str(&manifest.payload_json)?;
        let mut contract = payload["contracts"][0].clone();
        contract["role"] = json!("controller");
        contract["address"] = json!(controller);
        payload["contracts"].as_array_mut().unwrap().push(contract);
        payload["abi"]["events"].as_array_mut().unwrap().push(json!({
            "name":"NameRegistered",
            "fragment":"event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            "emitter_roles":["controller"],"normalized_events":[adapter::seam::PREIMAGE_OBSERVATION_EVENT_KIND],
        }));
        manifest.payload_json = serde_json::to_string(&payload)?;
        let mut admission = input
            .admissions
            .iter()
            .find(|admission| admission.source_manifest_id == Some(manifest.manifest_id))
            .unwrap()
            .clone();
        admission.address = controller.to_owned();
        admission.role = Some("controller".to_owned());
        admission.contract_instance_id = Uuid::from_u128(999_999);
        input.admissions.push(admission);
        let (output, _) = adapter::prepare_schema_v2_batch_incremental(
            input.clone(),
            None,
            StateCacheCapacity::Unlimited,
        )?
        .finish(Vec::new())?;
        assert_eq!(
            output
                .normalized_events
                .iter()
                .filter(|event| event.event_kind == "RegistrationGranted")
                .count(),
            1
        );
        assert!(
            !output
                .normalized_events
                .iter()
                .any(|event| event.after_state["registrar_surface_snapshot"] == true)
        );
        let resource = fixture::registrar_grant(&output).resource_id.unwrap();
        assert_eq!(
            output
                .surface_bindings
                .iter()
                .filter(|binding| binding.resource_id == resource)
                .count(),
            1
        );
        let database = database(&input).await?;
        let result = async {
            write(database.pool(), &input, &output).await?;
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM surface_bindings WHERE resource_id=$1")
                    .bind(resource)
                    .fetch_one(database.pool())
                    .await?;
            assert_eq!(count, 1);
            Ok(())
        }
        .await;
        database.cleanup().await?;
        result
    }

    #[tokio::test]
    async fn actual_order_raw_receipts_write_project_and_cold_restore_through_old_expiry()
    -> Result<()> {
        let input = fixture::input()?;
        fixture::assert_receipt_order(&input);
        let expected = fixture::fixture()?;
        let logical = format!("ens:{}", expected["node"].as_str().unwrap());
        let mut mode_summaries = Vec::new();
        for cold in [false, true] {
            let database = database(&input).await?;
            let pool = database.pool();
            let mut session = None;
            let mut retained = Vec::new();
            let mut marker = None;
            let mut registrar_resource = None;
            let mut original_row = None;
            let mut summaries = Vec::new();
            for (from, to) in [(403, 403), (405, 406), (407, 414), (415, 415), (416, 416)] {
                let mut part = fixture::range(&input, from, to);
                if cold {
                    part.prior_events = retained.clone();
                }
                let (output, next) = adapter::prepare_schema_v2_batch_incremental(
                    part.clone(),
                    session.take(),
                    StateCacheCapacity::Unlimited,
                )?
                .finish(Vec::new())?;
                retained = adapter::seam::fold_prior_events(
                    retained,
                    &output.normalized_events,
                    &part.blocks,
                )?;
                if !cold {
                    session = Some(next);
                }
                if to == 403 {
                    registrar_resource = fixture::registrar_grant(&output).resource_id;
                }
                if to == 415 {
                    assert_eq!(
                        output.migration_authority_transitions.len(),
                        1,
                        "real migration must activate"
                    );
                    assert!(output.normalized_events.iter().any(|event| event.event_kind
                        == adapter::seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND
                        && event.log_index == Some(4)
                        && event.resource_id == registrar_resource));
                }
                write(pool, &part, &output).await?;
                if to == 403 {
                    original_row = Some(sqlx::query_scalar::<_,Value>("SELECT to_jsonb(event) - 'inserted_at' FROM normalized_events event WHERE event_identity=$1")
                        .bind(&fixture::registrar_grant(&output).event_identity).fetch_one(pool).await?);
                }
                marker = Some(project(pool, from, to, marker).await?);
                if to == 414 {
                    summaries.push(assert_pre(pool, &expected, registrar_resource.unwrap()).await?);
                }
                if to >= 415 {
                    let selected: (Uuid,String) = sqlx::query_as("SELECT current.resource_id,binding.authority_arm FROM name_current current JOIN surface_bindings binding ON binding.surface_binding_id=current.surface_binding_id WHERE current.logical_name_id=$1")
                        .bind(&logical).fetch_one(pool).await?;
                    assert_ne!(Some(selected.0), registrar_resource);
                    assert_eq!(selected.1, "ens_v2");
                    let current = summary(pool, &logical).await?;
                    assert_eq!(current["registration"]["expiry"], expected["v2_expiry"]);
                    summaries.push(current);
                    let closed: bool = sqlx::query_scalar("SELECT bool_and(active_to IS NOT NULL) FROM surface_bindings WHERE resource_id=$1 AND authority_arm='ens_v1'")
                        .bind(registrar_resource).fetch_one(pool).await?;
                    assert!(
                        closed,
                        "unchanged strict cleanup writer did not close prior V1 authority"
                    );
                }
            }
            let preserved: Value = sqlx::query_scalar("SELECT to_jsonb(event) - 'inserted_at' FROM normalized_events event WHERE source_family='ens_v1_registrar_l1' AND event_kind='RegistrationGranted' AND block_number=403")
                .fetch_one(pool).await?;
            assert_eq!(
                Some(preserved),
                original_row,
                "later readability rewrote a retained raw grant"
            );
            mode_summaries.push(summaries);
            database.cleanup().await?;
        }
        assert_eq!(
            mode_summaries[0], mode_summaries[1],
            "live and cold Project results differ"
        );
        Ok(())
    }

    // Interpret-to-Project coverage for a migrated name that is then released on ENSv2 (Pro review
    // of PR 953, question 5). The captured registration, TokenResource and migration receipts and a
    // synthetic `LabelUnregistered` go through the adapter and the writer, and nothing rewrites the
    // normalized rows. The successor identifiers, the predecessor closure, the successor binding
    // position and the release closure are checked in the written rows before Project runs.
    #[tokio::test]
    async fn raw_migration_then_v2_release_is_written_before_project_serves_it() -> Result<()> {
        use alloy_sol_types::{SolEvent, sol};
        use bigname_adapters::schema_v2::{RawBlockInput, RawLogInput};
        sol! {
            event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
            event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
            event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
        }
        const RELEASE_BLOCK: i64 = 417;
        let mut input = fixture::input()?;
        let expected = fixture::fixture()?;
        let logical = format!("ens:{}", expected["node"].as_str().unwrap());
        let topic = |hash: alloy_primitives::B256| format!("{hash:#x}");
        let migration_log = |signature| {
            input
                .raw_logs
                .iter()
                .find(|raw| {
                    raw.block_number == fixture::MIGRATION_BLOCK
                        && raw.topics.first() == Some(&topic(signature))
                })
                .cloned()
                .context("captured ENSv2 migration log")
        };
        let registered = migration_log(LabelRegistered::SIGNATURE_HASH)?;
        let linked = migration_log(TokenResource::SIGNATURE_HASH)?;
        let token: alloy_primitives::U256 = registered.topics[1].parse()?;
        let after_expiry = input
            .blocks
            .iter()
            .find(|block| block.block_number == fixture::AFTER_EXPIRY_BLOCK)
            .context("after-expiry block")?
            .clone();
        let release_block = RawBlockInput {
            block_hash: format!("0x{RELEASE_BLOCK:064x}"),
            block_number: RELEASE_BLOCK,
            block_timestamp: after_expiry.block_timestamp + time::Duration::seconds(12),
            ..after_expiry
        };
        let release = LabelUnregistered {
            tokenId: token,
            sender: expected["expected_owner"].as_str().unwrap().parse()?,
        }
        .encode_log_data();
        input.raw_logs.push(RawLogInput {
            chain_id: fixture::CHAIN.to_owned(),
            block_hash: release_block.block_hash.clone(),
            block_number: RELEASE_BLOCK,
            block_timestamp: release_block.block_timestamp,
            canonicality_state: "canonical".to_owned(),
            transaction_hash: format!("0x{:064x}", 0x417_u64),
            transaction_index: 0,
            log_index: 0,
            emitting_address: registered.emitting_address.clone(),
            topics: release.topics().iter().map(|hash| topic(*hash)).collect(),
            data: release.data.to_vec(),
        });
        input.blocks.push(release_block.clone());

        let database = database(&input).await?;
        let pool = database.pool();
        let result = async {
            let mut session = None;
            let mut marker = None;
            for (from, to) in [
                (403, 403),
                (405, 406),
                (407, 414),
                (415, 415),
                (416, 416),
                (RELEASE_BLOCK, RELEASE_BLOCK),
            ] {
                let part = fixture::range(&input, from, to);
                let (output, next) = adapter::prepare_schema_v2_batch_incremental(
                    part.clone(),
                    session.take(),
                    StateCacheCapacity::Unlimited,
                )?
                .finish(Vec::new())?;
                session = Some(next);
                write(pool, &part, &output).await?;
                if to == fixture::MIGRATION_BLOCK {
                    // Successor identifiers: the activated migration names the ENSv2 binding and
                    // resource the registration opened, at the TokenResource log.
                    let successor: (Uuid, Uuid, i64, i64, i64, Uuid) = sqlx::query_as(
                        "SELECT binding.surface_binding_id, binding.resource_id,
                                binding.block_number,
                                (binding.provenance ->> 'transaction_index')::bigint,
                                (binding.provenance ->> 'log_index')::bigint,
                                grant_event.resource_id
                         FROM surface_bindings binding
                         JOIN normalized_events grant_event
                           ON grant_event.logical_name_id = binding.logical_name_id
                          AND grant_event.event_kind = 'RegistrationGranted'
                          AND grant_event.source_family = 'ens_v2_registry_l1'
                         WHERE binding.logical_name_id = $1 AND binding.authority_arm = 'ens_v2'",
                    )
                    .bind(&logical)
                    .fetch_one(pool)
                    .await?;
                    assert_eq!(
                        successor.1, successor.5,
                        "the grant is on the bound resource"
                    );
                    assert_eq!(
                        (successor.2, successor.3, successor.4),
                        (
                            fixture::MIGRATION_BLOCK,
                            linked.transaction_index,
                            linked.log_index
                        ),
                        "the successor binding opens at the TokenResource log"
                    );
                    let migration: (String, Option<String>, Option<String>) = sqlx::query_as(
                        "SELECT consumer_visibility::text,
                                after_state #>> '{successor_binding,binding_id}',
                                after_state #>> '{successor_binding,resource_id}'
                         FROM normalized_events
                         WHERE logical_name_id = $1 AND event_kind = 'MigrationApplied'",
                    )
                    .bind(&logical)
                    .fetch_one(pool)
                    .await?;
                    assert_eq!(
                        migration,
                        (
                            "activated".to_owned(),
                            Some(successor.0.to_string()),
                            Some(successor.1.to_string()),
                        )
                    );
                    // Predecessor closure: no ENSv1 binding of the name stays open.
                    let open_v1: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM surface_bindings
                         WHERE logical_name_id = $1 AND authority_arm = 'ens_v1'
                           AND active_to IS NULL",
                    )
                    .bind(&logical)
                    .fetch_one(pool)
                    .await?;
                    assert_eq!(open_v1, 0, "the migration closes the ENSv1 predecessor");
                }
                if to == RELEASE_BLOCK {
                    // Release closure: the ENSv2 binding closes at the release, which is written
                    // on its resource at the unregister log.
                    let closure: (Option<time::OffsetDateTime>, i64, Option<i64>, Option<i64>) =
                        sqlx::query_as(
                            "SELECT binding.active_to, release.block_number,
                                    release.transaction_index, release.log_index
                             FROM surface_bindings binding
                             JOIN normalized_events release
                               ON release.resource_id = binding.resource_id
                              AND release.event_kind = 'RegistrationReleased'
                              AND release.after_state ->> 'source_event' = 'LabelUnregistered'
                             WHERE binding.logical_name_id = $1
                               AND binding.authority_arm = 'ens_v2'",
                        )
                        .bind(&logical)
                        .fetch_one(pool)
                        .await?;
                    assert_eq!(
                        closure,
                        (
                            Some(release_block.block_timestamp),
                            RELEASE_BLOCK,
                            Some(0),
                            Some(0)
                        )
                    );
                }
                marker = Some(project(pool, from, to, marker).await?);
            }
            // Project serves the released ENSv2 registration, keeps the migration as history and
            // starts the epoch at the successor binding.
            let served: (
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Value,
            ) = sqlx::query_as(
                "SELECT provenance #>> '{authority_selection,authority_arm}',
                        provenance #>> '{authority_selection,lifecycle_state}',
                        provenance #>> '{authority_selection,proof_kind}',
                        declared_summary #>> '{registration,status}',
                        provenance #> '{authority_selection,epoch_start_position}'
                 FROM name_current WHERE logical_name_id = $1",
            )
            .bind(&logical)
            .fetch_one(pool)
            .await?;
            assert_eq!(
                served,
                (
                    Some("ens_v2".to_owned()),
                    Some("unregistered".to_owned()),
                    Some("migration_authority_transition".to_owned()),
                    Some("released".to_owned()),
                    serde_json::json!({
                        "block_number": fixture::MIGRATION_BLOCK,
                        "transaction_index": linked.transaction_index,
                        "log_index": linked.log_index,
                    }),
                )
            );
            Ok(())
        }
        .await;
        database.cleanup().await?;
        result
    }

    #[tokio::test]
    async fn later_readable_timestamp_does_not_move_public_registered_at() -> Result<()> {
        let mut input = fixture::input()?;
        for raw in input
            .raw_logs
            .iter_mut()
            .filter(|raw| raw.block_number == 407)
        {
            raw.block_timestamp += time::Duration::seconds(5);
        }
        for block in input
            .blocks
            .iter_mut()
            .filter(|block| block.block_number == 407)
        {
            block.block_timestamp += time::Duration::seconds(5);
        }
        let input = fixture::range(&input, 403, 414);
        let database = database(&input).await?;
        let output = adapter::interpret_schema_v2_batch(input.clone())?;
        write(database.pool(), &input, &output).await?;
        project(database.pool(), 403, 414, None).await?;
        assert_pre(
            database.pool(),
            &fixture::fixture()?,
            fixture::registrar_grant(&output).resource_id.unwrap(),
        )
        .await?;
        database.cleanup().await?;
        Ok(())
    }
}
