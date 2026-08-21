use alloy_primitives::keccak256;
use anyhow::Context;
use serde_json::{Value, json};

use super::registry::CORRELATION_KIND as REGISTRY_CORRELATION_KIND;
use super::{CANDIDATE, MIGRATION_FAMILY};
use crate::schema_v2::{
    BatchOutput, MigrationEventAssociation, NormalizedEvent, catalog::Catalog,
    model::DiscoveryEdge, protocol::MigrationObservation, state_key::interpreter_state_key,
};

pub(super) fn registrar_transfer_event_identity(observation: &MigrationObservation) -> String {
    super::super::normalized::raw_log_event_identity(
        &observation.source_family,
        observation.source_manifest_id,
        &observation.raw,
        "TokenControlTransferred",
        "TokenControlTransferred",
        0,
    )
}

#[derive(Clone)]
pub(super) struct RegistryGroup {
    pub(super) correlation_id: String,
    pub(super) logical_name_id: String,
    pub(super) registry_address: String,
    pub(super) evidence: Vec<Value>,
    pub(super) completion_log_index: i64,
    /// Block, transaction index, and log index of the factory log. Child correlation spans
    /// transactions and blocks, so it orders against the full position rather than the log index.
    pub(super) completion_position: (i64, i64, i64),
}

pub(super) fn associate_restored_registry_effects(
    catalog: &Catalog,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let associations = output
        .normalized_events
        .iter()
        .filter(|event| event.source_family != MIGRATION_FAMILY)
        .flat_map(|event| {
            let address = event
                .raw_fact_ref
                .get("emitting_address")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let correlations = event
                .block_number
                .map(|block| catalog.migration_registry_correlations(address, block))
                .unwrap_or_default();
            correlations
                .into_iter()
                .map(move |correlation| (event.event_identity.clone(), correlation))
        })
        .collect::<Vec<_>>();
    for (event_identity, correlation) in associations {
        associate_event(
            output,
            &event_identity,
            &correlation.id,
            REGISTRY_CORRELATION_KIND,
            correlation.evidence,
        )?;
    }
    Ok(())
}

pub(super) fn boundary_event(
    source: &crate::schema_v2::manifest::ManifestSource,
    registration: &NormalizedEvent,
    id: &str,
    before_state: Value,
    after_state: Value,
) -> anyhow::Result<NormalizedEvent> {
    let mut raw_fact_ref = registration.raw_fact_ref.clone();
    let state_scope = format!("migration-authority:{id}");
    let state_key = interpreter_state_key(
        &source.namespace,
        registration.logical_name_id.as_deref(),
        registration.resource_id,
        "MigrationApplied",
        MIGRATION_FAMILY,
        &state_scope,
    );
    let raw_ref = raw_fact_ref
        .as_object_mut()
        .context("registration raw fact reference is not an object")?;
    raw_ref.insert("state_scope".to_owned(), Value::String(state_scope));
    raw_ref.insert("interpreter_state_key".to_owned(), Value::String(state_key));
    Ok(NormalizedEvent {
        event_identity: format!(
            "ens_v2_migration:{}:{}:{id}:MigrationApplied",
            source.manifest_id, registration.chain_id
        ),
        namespace: source.namespace.clone(),
        logical_name_id: registration.logical_name_id.clone(),
        resource_id: registration.resource_id,
        event_kind: "MigrationApplied".to_owned(),
        source_family: MIGRATION_FAMILY.to_owned(),
        manifest_version: source.manifest_version,
        source_manifest_id: Some(source.manifest_id),
        chain_id: registration.chain_id.clone(),
        block_number: registration.block_number,
        block_hash: registration.block_hash.clone(),
        transaction_hash: registration.transaction_hash.clone(),
        transaction_index: registration.transaction_index,
        log_index: registration.log_index,
        raw_fact_ref,
        derivation_kind: "ens_v2_migration".to_owned(),
        canonicality_state: registration.canonicality_state.clone(),
        before_state,
        after_state,
        migration_correlation_ids: vec![id.to_owned()],
        consumer_visibility: CANDIDATE.to_owned(),
        before_state_explicit: true,
    })
}

pub(super) fn mark_direct_position(
    output: &mut BatchOutput,
    raw: &crate::schema_v2::RawLogInput,
    id: &str,
) {
    for event in &mut output.normalized_events {
        if event.source_family == MIGRATION_FAMILY && same_position(event, raw) {
            event.migration_correlation_ids.push(id.to_owned());
            event.migration_correlation_ids.sort();
            event.migration_correlation_ids.dedup();
            event.consumer_visibility = CANDIDATE.to_owned();
        }
    }
}

pub(super) fn mark_direct_historical(
    output: &mut BatchOutput,
    raw: &crate::schema_v2::RawLogInput,
    id: &str,
    lifecycle_classification: &str,
) {
    mark_direct_position(output, raw, id);
    for event in &mut output.normalized_events {
        if event.source_family == MIGRATION_FAMILY && same_position(event, raw) {
            let state = event
                .after_state
                .as_object_mut()
                .expect("migration event state is an object");
            state.insert(
                "lifecycle_classification".to_owned(),
                Value::String(lifecycle_classification.to_owned()),
            );
            state.insert("historical".to_owned(), Value::Bool(true));
            state.insert(
                "authority_effect".to_owned(),
                Value::String("none".to_owned()),
            );
        }
    }
}

pub(super) fn correlate_cleanups(
    observations: &[&MigrationObservation],
    graveyard: &str,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    const GRAVEYARD_CLEANUP_EXPIRY: u64 = 18_446_744_073_701_775_615;
    for observation in observations.iter().copied().filter(|observation| {
        observation.event_name == "NameRegistered"
            && super::is_v1_registrar_observation(observation)
            && observation
                .decoded
                .get("owner")
                .and_then(Value::as_str)
                .is_some_and(|owner| owner.eq_ignore_ascii_case(graveyard))
            && observation.decoded.get("expiry").and_then(Value::as_u64)
                == Some(GRAVEYARD_CLEANUP_EXPIRY)
    }) {
        let logical_name_id = logical_name_from_decoded(&observation.decoded)?;
        let evidence = vec![observation_evidence(observation)];
        let id = correlation_id("graveyard_cleanup", Some(&logical_name_id), &evidence);
        mark_direct_historical(output, &observation.raw, &id, "graveyard_cleanup");
    }
    Ok(())
}

pub(super) fn correlate_historical_renewals(
    observations: &[&MigrationObservation],
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for observation in observations.iter().copied().filter(|observation| {
        observation.event_name == "NameRenewed" && super::is_v1_registrar_observation(observation)
    }) {
        let already_correlated = output.normalized_events.iter().any(|event| {
            event.source_family == MIGRATION_FAMILY
                && same_position(event, &observation.raw)
                && !event.migration_correlation_ids.is_empty()
        });
        if already_correlated {
            continue;
        }
        let logical_name_id = logical_name_from_decoded(&observation.decoded)?;
        let evidence = vec![observation_evidence(observation)];
        let id = correlation_id("historical_renewal", Some(&logical_name_id), &evidence);
        mark_direct_historical(output, &observation.raw, &id, "historical_renewal");
    }
    Ok(())
}

pub(super) fn anchor_direct_position(
    output: &mut BatchOutput,
    raw: &crate::schema_v2::RawLogInput,
    resource_id: uuid::Uuid,
) {
    for event in &mut output.normalized_events {
        if event.source_family == MIGRATION_FAMILY && same_position(event, raw) {
            let Some(state_scope) = event
                .raw_fact_ref
                .get("state_scope")
                .and_then(Value::as_str)
                .map(str::to_owned)
            else {
                continue;
            };
            event.resource_id = Some(resource_id);
            let state_key = interpreter_state_key(
                &event.namespace,
                event.logical_name_id.as_deref(),
                event.resource_id,
                &event.event_kind,
                &event.source_family,
                &state_scope,
            );
            event
                .raw_fact_ref
                .as_object_mut()
                .expect("migration raw fact reference is an object")
                .insert("interpreter_state_key".to_owned(), Value::String(state_key));
        }
    }
}

pub(super) fn associate_event(
    output: &mut BatchOutput,
    event_identity: &str,
    correlation_id: &str,
    correlation_kind: &str,
    evidence: Vec<Value>,
) -> anyhow::Result<()> {
    if output
        .migration_event_associations
        .iter()
        .any(|association| {
            association.event_identity == event_identity
                && association.migration_correlation_id == correlation_id
        })
    {
        return Ok(());
    }
    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_identity == event_identity)
        .with_context(|| format!("migration association event {event_identity} is absent"))?;
    let (block_number, transaction_index, log_index) = required_position(event)?;
    output
        .migration_event_associations
        .push(MigrationEventAssociation {
            event_identity: event_identity.to_owned(),
            migration_correlation_id: correlation_id.to_owned(),
            correlation_kind: correlation_kind.to_owned(),
            evidence_refs: Value::Array(evidence),
            chain_id: event.chain_id.clone(),
            block_number,
            block_hash: event.block_hash.clone().expect("required position"),
            transaction_hash: event.transaction_hash.clone().expect("required position"),
            transaction_index,
            log_index,
            canonicality_state: event.canonicality_state.clone(),
            consumer_visibility: CANDIDATE.to_owned(),
        });
    Ok(())
}

pub(super) fn authority_transition_event(
    event: &NormalizedEvent,
    logical_name_id: &str,
    registry_address: &str,
    controller: &str,
    registration_log: i64,
) -> bool {
    event.source_family == "ens_v2_registry_l1"
        && event.logical_name_id.as_deref() == Some(logical_name_id)
        && event
            .log_index
            .is_some_and(|index| index >= registration_log)
        && event
            .raw_fact_ref
            .get("emitting_address")
            .and_then(Value::as_str)
            .is_some_and(|address| address.eq_ignore_ascii_case(registry_address))
        && event
            .after_state
            .get("source_event")
            .and_then(Value::as_str)
            .is_some_and(|source_event| {
                matches!(
                    source_event,
                    "LabelRegistered"
                        | "TokenResource"
                        | "TokenRegenerated"
                        | "EACRolesChanged"
                        | "SubregistryUpdated"
                        | "ResolverUpdated"
                )
            })
        && event
            .after_state
            .get("sender")
            .and_then(Value::as_str)
            .is_none_or(|sender| sender.eq_ignore_ascii_case(controller))
}

pub(super) fn insert_boundaries(output: &mut BatchOutput, boundaries: Vec<NormalizedEvent>) {
    for boundary in boundaries {
        let insertion = output
            .normalized_events
            .iter()
            .rposition(|event| same_event_position(event, &boundary))
            .map_or(output.normalized_events.len(), |index| index + 1);
        output.normalized_events.insert(insertion, boundary);
    }
}

pub(super) fn sort_and_deduplicate(output: &mut BatchOutput) {
    output.migration_event_associations.sort_by(|left, right| {
        (&left.event_identity, &left.migration_correlation_id)
            .cmp(&(&right.event_identity, &right.migration_correlation_id))
    });
    output.migration_event_associations.dedup_by(|left, right| {
        left.event_identity == right.event_identity
            && left.migration_correlation_id == right.migration_correlation_id
    });
    output
        .migration_discovery_associations
        .sort_by(|left, right| {
            (&left.logical_edge_identity, &left.migration_correlation_id).cmp(&(
                &right.logical_edge_identity,
                &right.migration_correlation_id,
            ))
        });
    output
        .migration_discovery_associations
        .dedup_by(|left, right| {
            left.logical_edge_identity == right.logical_edge_identity
                && left.migration_correlation_id == right.migration_correlation_id
        });
}

pub(super) fn matching_events(
    output: &BatchOutput,
    raw: &crate::schema_v2::RawLogInput,
    predicate: impl Fn(&NormalizedEvent) -> bool,
) -> Vec<NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| same_transaction(event, raw) && predicate(event))
        .cloned()
        .collect()
}

pub(super) fn same_transaction(
    event: &NormalizedEvent,
    raw: &crate::schema_v2::RawLogInput,
) -> bool {
    event.block_hash.as_deref() == Some(raw.block_hash.as_str())
        && event.transaction_hash.as_deref() == Some(raw.transaction_hash.as_str())
}

pub(super) fn same_position(event: &NormalizedEvent, raw: &crate::schema_v2::RawLogInput) -> bool {
    same_transaction(event, raw) && event.log_index == Some(raw.log_index)
}

fn same_event_position(left: &NormalizedEvent, right: &NormalizedEvent) -> bool {
    left.block_hash == right.block_hash
        && left.transaction_hash == right.transaction_hash
        && left.log_index == right.log_index
}

pub(super) fn logical_name_from_decoded(decoded: &Value) -> anyhow::Result<String> {
    Ok(format!("ens:{}", value_str(decoded, "namehash")?))
}

pub(super) fn value_str<'a>(value: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("migration evidence has no string {key}"))
}

pub(super) fn declared_address(catalog: &Catalog, role: &str) -> anyhow::Result<String> {
    Ok(catalog
        .declared_address_for_role(MIGRATION_FAMILY, role)
        .with_context(|| format!("migration manifest has no {role} declaration"))?
        .to_ascii_lowercase())
}

pub(super) fn observation_evidence(observation: &MigrationObservation) -> Value {
    json!({
        "kind":"raw_log",
        "source_family":observation.source_family,
        "event":observation.event_name,
        "emitter_role":observation.emitter_role,
        "contract_instance_id":observation.contract_instance_id,
        "chain_id":observation.raw.chain_id,
        "block_number":observation.raw.block_number,
        "block_hash":observation.raw.block_hash,
        "transaction_hash":observation.raw.transaction_hash,
        "transaction_index":observation.raw.transaction_index,
        "log_index":observation.raw.log_index,
        "emitting_address":observation.raw.emitting_address,
        "decoded":observation.decoded,
    })
}

pub(super) fn event_evidence(event: &NormalizedEvent) -> Value {
    json!({
        "kind":"normalized_event",
        "event_identity":event.event_identity,
        "event_kind":event.event_kind,
        "source_family":event.source_family,
        "chain_id":event.chain_id,
        "block_number":event.block_number,
        "block_hash":event.block_hash,
        "transaction_hash":event.transaction_hash,
        "transaction_index":event.transaction_index,
        "log_index":event.log_index,
    })
}

pub(super) fn correlation_id(kind: &str, subject: Option<&str>, evidence: &[Value]) -> String {
    let mut evidence = evidence.iter().map(Value::to_string).collect::<Vec<_>>();
    evidence.sort();
    evidence.dedup();
    let mut bytes = Vec::from(b"bigname:migration-correlation:v1\0".as_slice());
    for field in std::iter::once(kind)
        .chain(subject)
        .chain(evidence.iter().map(String::as_str))
    {
        let field = field.as_bytes();
        bytes.extend_from_slice(&u32::try_from(field.len()).unwrap_or(u32::MAX).to_be_bytes());
        bytes.extend_from_slice(field);
    }
    format!("{:#x}", keccak256(bytes))
}

pub(super) fn logical_edge_identity(
    edge: &DiscoveryEdge,
    source: &crate::schema_v2::manifest::ManifestSource,
) -> anyhow::Result<String> {
    let transaction_index = edge
        .provenance
        .get("transaction_index")
        .and_then(Value::as_i64)
        .context("registry-announcement provenance has no transaction index")?;
    let log_index = edge
        .provenance
        .get("log_index")
        .and_then(Value::as_i64)
        .context("registry-announcement provenance has no log index")?;
    let fields = [
        edge.chain_id.clone(),
        edge.edge_kind.clone(),
        edge.from_contract_instance_id
            .to_string()
            .to_ascii_lowercase(),
        edge.to_contract_instance_id
            .to_string()
            .to_ascii_lowercase(),
        edge.discovery_source.clone(),
        edge.admission_basis.clone(),
        source.namespace.clone(),
        source.source_family.clone(),
        source.chain_id.clone(),
        source.deployment_label.clone(),
        source.manifest_version.to_string(),
        edge.observation_key.clone(),
        edge.active_from_block_number.to_string(),
        edge.active_from_block_hash.to_ascii_lowercase(),
        transaction_index.to_string(),
        log_index.to_string(),
    ];
    let mut encoded = Vec::from(b"bigname:discovery-edge:v1\0".as_slice());
    for field in fields {
        let bytes = field.as_bytes();
        encoded.extend_from_slice(
            &u32::try_from(bytes.len())
                .context("logical discovery-edge field is too long")?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(bytes);
    }
    Ok(format!("{:#x}", keccak256(encoded)))
}

pub(super) fn required_position(event: &NormalizedEvent) -> anyhow::Result<(i64, i64, i64)> {
    Ok((
        event.block_number.context("event has no block number")?,
        event
            .transaction_index
            .context("event has no transaction index")?,
        event.log_index.context("event has no log index")?,
    ))
}

pub(super) fn position_key(raw: &crate::schema_v2::RawLogInput) -> (i64, i64, i64) {
    (raw.block_number, raw.transaction_index, raw.log_index)
}

#[cfg(any(test, feature = "test-activation"))]
pub fn inject_activated_transition_for_test(output: &mut BatchOutput) -> anyhow::Result<()> {
    let transitions = output
        .migration_candidate_identity_effects
        .iter()
        .filter(|effect| {
            effect.correlation_kind == "authority_transition"
                && effect.effect_kind == "surface_binding_transition"
        })
        .map(|effect| {
            anyhow::ensure!(
                effect.migration_correlation_ids.len() == 1,
                "authority transition must have one correlation id"
            );
            let correlation_id = effect.migration_correlation_ids[0].clone();
            let boundary = output
                .normalized_events
                .iter()
                .find(|event| {
                    event.event_kind == "MigrationApplied"
                        && event.migration_correlation_ids == [correlation_id.clone()]
                })
                .context("authority transition has no exact MigrationApplied boundary")?;
            let proposed = &effect.proposed_effect;
            let predecessor = proposed
                .get("predecessor_binding")
                .context("authority transition has no predecessor selector")?;
            let successor = proposed
                .get("successor_binding")
                .context("authority transition has no successor binding")?;
            Ok(crate::schema_v2::MigrationAuthorityTransition {
                boundary_event_identity: boundary.event_identity.clone(),
                migration_correlation_id: correlation_id,
                logical_name_id: proposed["logical_name_id"]
                    .as_str()
                    .context("authority transition has no logical name")?
                    .to_owned(),
                predecessor_selector: predecessor.clone(),
                expected_predecessor_arm: predecessor["authority_epoch"]
                    .as_str()
                    .context("authority transition has no predecessor arm")?
                    .to_owned(),
                successor_surface_binding_id: successor["binding_id"]
                    .as_str()
                    .context("authority transition has no successor binding id")?
                    .parse()
                    .context("authority transition successor binding id is malformed")?,
                successor_resource_id: successor["resource_id"]
                    .as_str()
                    .context("authority transition has no successor resource id")?
                    .parse()
                    .context("authority transition successor resource id is malformed")?,
                successor_arm: successor["authority_epoch"]
                    .as_str()
                    .context("authority transition has no successor arm")?
                    .to_owned(),
                chain_id: effect.chain_id.clone(),
                block_number: effect.block_number,
                transaction_index: effect.transaction_index,
                log_index: effect.log_index,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for transition in &transitions {
        output
            .normalized_events
            .iter_mut()
            .find(|event| event.event_identity == transition.boundary_event_identity)
            .context("activated authority transition lost its boundary event")?
            .consumer_visibility = "activated".to_owned();
    }
    output.migration_authority_transitions.extend(transitions);
    Ok(())
}
