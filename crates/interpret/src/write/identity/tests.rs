use bigname_adapters::schema_v2::{
    BindingClosure, MigrationCandidateEffect, NormalizedEvent, SurfaceBinding,
    seam::{
        MIGRATION_APPLIED_EVENT_KIND, SURFACE_BINDING_ID_KEY, SURFACE_BOUND_EVENT_KIND,
        raw_block_provenance,
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
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, after_state
         ) VALUES ($1, 'ens', $2, $3, 'TokenControlTransferred', 'test', 1,
                   'ethereum', 1, '0x01', jsonb_build_object('emitting_address', $4::text),
                   'ens_v1_unwrapped_authority', 'canonical',
                   jsonb_build_object('token_id', $5::text))",
    )
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
        event_kind: "TokenControlTransferred".to_owned(),
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

fn registrar_selector() -> serde_json::Value {
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
    activate_with_selector(output, registrar_selector())
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
async fn activated_boundary_rejects_zero_and_multiple_predecessors() -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    insert_registrar_evidence(pool, 1, "0xexpected").await?;
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    activate(&mut output)?;
    let zero = apply(pool, &output).await.unwrap_err().to_string();
    assert!(zero.contains("0 active ENSv1 predecessors"), "{zero}");

    sqlx::query("ALTER TABLE surface_bindings DROP CONSTRAINT surface_bindings_no_overlap")
        .execute(pool)
        .await?;
    insert_binding(pool, 11, NAME, 1, "ens_v1").await?;
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
        output.migration_authority_transitions[0].log_index = 1;
        let boundary = output
            .normalized_events
            .iter_mut()
            .find(|event| event.event_kind == MIGRATION_APPLIED_EVENT_KIND)
            .expect("activated migration boundary");
        boundary.transaction_index = Some(1);
        boundary.log_index = Some(1);

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
    json!({
        "authority_epoch":"ens_v1",
        "logical_name_id":NAME,
        "selection":"active_immediately_before_predecessor_cleanup",
        "predecessor_cleanup":{
            "event_identity":"child-cleanup",
            "source_event":"NameUnwrapped",
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
    let mut output = ordinary_open(12, 2, "ens_v2", 2);
    output.normalized_events.push(predecessor_evidence(
        "same-batch-wrapper-evidence",
        1,
        WRAPPER,
        json!({"authority_kind":"wrapper","node":"0xname"}),
    ));
    output
        .normalized_events
        .push(cleanup_event("NameUnwrapped", CLEANUP_LOG_INDEX));
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
