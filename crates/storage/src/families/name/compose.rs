//! Assemble one composed name row from what `batch.rs` loaded and decided.
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::{
    CoverageShape, FamilyPublication, NameHistory,
    selection::NameSelection,
    serving::{PointerRow, ResolverScope, Serving, resolver_block},
};
use crate::{
    NameCurrentRow, SurfaceBindingKind,
    families::control::{
        lifecycle::{NameFacts, ShadowName, staged_as_own},
        position::Position,
        rows::WrapperRow,
        wrapper::effective_wrapper,
    },
};

/// The surface of one name.
#[derive(Clone, Debug)]
pub(super) struct Surface {
    pub(super) logical_name_id: String,
    pub(super) namespace: String,
    pub(super) raw_name: String,
    pub(super) namehash: String,
    /// The label hashes in name order.
    pub(super) labelhashes: Vec<String>,
    pub(super) chain_id: String,
    pub(super) block_number: i64,
}

/// Everything one row is assembled from.
pub(super) struct Parts<'a> {
    pub(super) surface: &'a Surface,
    pub(super) publication: &'a FamilyPublication,
    pub(super) facts: &'a NameFacts,
    pub(super) shadow: &'a ShadowName,
    pub(super) selection: &'a NameSelection,
    pub(super) history: Option<&'a NameHistory>,
    pub(super) serving: Option<&'a Serving>,
    pub(super) resource_pointer: Option<&'a PointerRow>,
    pub(super) node_pointer: Option<&'a PointerRow>,
    /// The token lineage of the row's event resource.
    pub(super) token_lineage_id: Option<Uuid>,
    /// The history heads (`heads.rs`), given the row's resource.
    pub(super) heads: &'a super::heads::Heads,
    /// Whether the name resolves to nothing through the Universal Resolver
    /// (`resolvability.rs`): its resolver and records are withheld.
    pub(super) unresolvable: bool,
}

fn trace_text<'a>(shadow: &'a ShadowName, key: &str) -> Option<&'a str> {
    shadow.trace.get(key).and_then(Value::as_str)
}

fn uuid(text: Option<&str>) -> Result<Option<Uuid>> {
    text.map(|text| Uuid::parse_str(text).with_context(|| format!("bad uuid {text}")))
        .transpose()
}

/// The chain positions slot of a chain.
fn slot(chain_id: &str) -> &str {
    match chain_id {
        "ethereum-mainnet" => "ethereum",
        "base-mainnet" => "base",
        other => other,
    }
}

/// The authority a released ENSv1 lease had when it lapsed: the latest
/// grant or authority epoch with a kind on the lapsed resource.
fn lapsed_authority(parts: &Parts<'_>) -> (Value, Value) {
    let shadow = parts.shadow;
    let resource = if shadow.trace.get("authority_context_kind") == Some(&json!("registry_only")) {
        trace_text(shadow, "selected_event").and_then(|identity| {
            parts
                .facts
                .events
                .iter()
                .find(|event| event.position.event_identity == identity)
                .and_then(|event| event.resource_id.as_deref())
        })
    } else {
        trace_text(shadow, "event_resource")
    };
    let Some(resource) = resource else {
        return (Value::Null, Value::Null);
    };
    let mut candidates: Vec<(Position, Option<String>, Option<String>)> = parts
        .facts
        .events
        .iter()
        .filter(|event| {
            event.event_kind == "RegistrationGranted"
                && event.resource_id.as_deref() == Some(resource)
                && event.authority_kind_raw.is_some()
        })
        .map(|event| {
            (
                event.position.clone(),
                event.authority_kind_raw.clone(),
                event.authority_key.clone(),
            )
        })
        .collect();
    if let Some(starts) = parts.facts.authority_starts.as_object() {
        for start in starts.values() {
            let kind = start.get("authority_kind").and_then(Value::as_str);
            if start.get("resource_id").and_then(Value::as_str) == Some(resource)
                && kind.is_some()
                && let Some(position) = Position::from_json(start)
            {
                candidates.push((
                    position,
                    kind.map(str::to_owned),
                    start
                        .get("authority_key")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                ));
            }
        }
    }
    candidates
        .into_iter()
        .max_by(|left, right| left.0.cmp(&right.0))
        .map_or((Value::Null, Value::Null), |(_, kind, key)| {
            (json!(kind), json!(key))
        })
}

/// The block time of the name's first event: the history's first block, or an earlier unnamed
/// registrar row the staging passes give the name, which the history cannot know when the row was
/// written, since the binding that names it can arrive later.
fn created_at(parts: &Parts<'_>) -> Value {
    let staged = parts
        .facts
        .events
        .iter()
        .filter(|event| {
            event.original_logical_name_id.is_none() && staged_as_own(parts.facts, event)
        })
        .map(|event| event.position.block_number)
        .min();
    let first = parts.history.map(|history| history.first_block_number);
    match (staged, first) {
        (Some(staged), first) if first.is_none_or(|first| staged < first) => parts
            .facts
            .block_timestamps
            .get(&staged)
            .cloned()
            .unwrap_or(Value::Null),
        _ => parts
            .history
            .map_or(Value::Null, |history| history.created_at.clone()),
    }
}

fn wrapper_fields(summary: &mut Map<String, Value>, row: Option<&WrapperRow>, clock: i64) {
    let Some(effective) = row.map(|row| effective_wrapper(row, clock)) else {
        return;
    };
    let Some(state) = effective.wrapper_state else {
        return;
    };
    let fuses = effective.fuses.unwrap_or_default();
    let bit = |mask: i64| fuses & mask != 0;
    summary.insert("wrapper_state".into(), json!(state));
    summary.insert("wrapper_in_grace".into(), json!(effective.in_grace));
    summary.insert(
        "wrapper_fuses".into(),
        json!({
            "fuses": effective.fuses,
            "cannot_unwrap": bit(1),
            "cannot_burn_fuses": bit(2),
            "cannot_transfer": bit(4),
            "cannot_set_resolver": bit(8),
            "cannot_set_ttl": bit(16),
            "cannot_create_subdomain": bit(32),
            "cannot_approve": bit(64),
            "parent_cannot_control": bit(65536),
            "is_dot_eth": bit(131072),
            "can_extend_expiry": bit(262144),
        }),
    );
}

/// The declared coverage.
fn declared_coverage(parts: &Parts<'_>) -> Value {
    let selection = &parts.selection.selection;
    let ens_v2 = match selection.authority_arm.as_deref() {
        Some(arm) => arm == "ens_v2",
        None => parts
            .history
            .is_some_and(|history| history.has_ens_v2_events),
    };
    let classes = if ens_v2 {
        json!([
            "ens_v2_root_l1",
            "ens_v2_registry_l1",
            "ens_v2_registrar_l1"
        ])
    } else if matches!(parts.surface.namespace.as_str(), "ens" | "basenames") {
        json!(["ensv1_registry_path"])
    } else {
        json!([])
    };
    let basis = if parts.serving.is_some() {
        "event_linked_registry_resolver"
    } else if ens_v2 {
        "exact_name_profile"
    } else {
        "exact_name"
    };
    json!({
        "status": "projected",
        "exhaustiveness": "not_asserted",
        "source_classes_considered": classes,
        "unsupported_reason": selection.unsupported_reason,
        "enumeration_basis": basis,
    })
}

pub(super) fn compose(parts: &Parts<'_>, shape: CoverageShape) -> Result<NameCurrentRow> {
    let shadow = parts.shadow;
    let selection = &parts.selection.selection;
    let binding = parts.selection.binding.as_ref();
    let mismatch = shadow.trace.get("identity_mismatch") == Some(&Value::Bool(true));
    let event_resource = trace_text(shadow, "event_resource");

    let mut registration = shadow.registration.clone();
    registration.insert("created_at".into(), created_at(parts));
    if !selection.is_v2()
        && let Some(Value::Object(lapsed)) = registration.get_mut("lapsed_registration")
    {
        let (kind, key) = lapsed_authority(parts);
        lapsed.insert("authority_kind".into(), kind);
        lapsed.insert("authority_key".into(), key);
    }
    let selected_kind = trace_text(shadow, "selected_kind");
    let scope = ResolverScope {
        name: &parts.surface.logical_name_id,
        chain_id: &parts.publication.chain_id,
        resource: if selection.is_v2() {
            selection
                .resource_id
                .as_deref()
                .filter(|resource| trace_text(shadow, "selected_key") == Some(*resource))
        } else {
            selection.resource_id.as_deref()
        },
        arm: selection.authority_arm.as_deref(),
        admits: selection.unsupported_reason.is_none(),
        withholds: (matches!(
            selected_kind,
            Some("RegistrationReleased" | "RegistrationReserved")
        ) && selection.authority_arm.as_deref() == Some("ens_v2"))
            || selection.released_tombstone,
        unresolvable: parts.unresolvable,
    };
    let (resolver, source_family) = resolver_block(
        &scope,
        parts.resource_pointer,
        parts.node_pointer,
        parts.serving,
    );
    let coverage_block = declared_coverage(parts);
    let mut summary = Map::new();
    summary.insert("registration".into(), Value::Object(registration));
    summary.insert("control".into(), Value::Object(shadow.control.clone()));
    summary.insert("resolver".into(), resolver);
    if parts.unresolvable {
        summary.insert(
            "unresolvable_reason".into(),
            json!(super::resolvability::NO_LIVE_ENS_V2_ENTRY),
        );
    }
    summary.insert("coverage".into(), coverage_block.clone());
    let staged: Vec<&str> = parts
        .facts
        .events
        .iter()
        .filter(|event| {
            event.original_logical_name_id.is_none() && staged_as_own(parts.facts, event)
        })
        .map(|event| event.position.event_identity.as_str())
        .collect();
    let row_resource = match binding {
        Some(binding) if !mismatch && selection.unsupported_reason.is_none() => {
            Some(binding.resource_id.as_str())
        }
        _ => None,
    };
    summary.insert(
        "history".into(),
        parts
            .heads
            .history(&parts.surface.logical_name_id, &staged, row_resource),
    );
    wrapper_fields(
        &mut summary,
        event_resource.and_then(|resource| parts.facts.wrappers.get(resource)),
        parts.publication.timestamp_seconds(),
    );

    let mut provenance = Map::new();
    provenance.insert("chain_id".into(), json!(parts.publication.chain_id));
    provenance.insert(
        "surface_block_number".into(),
        json!(parts.surface.block_number),
    );
    // Retained for address-history bounds, so a later acquisition cannot admit older history.
    if let Some(position) = shadow
        .trace
        .get("registrant_position")
        .filter(|value| !value.is_null())
    {
        provenance.insert("registrant_position".into(), position.clone());
    }
    provenance.insert("derivation_kind".into(), json!("name_current_rebuild"));
    provenance.insert("authority_selection".into(), parts.selection.provenance());
    provenance.insert(
        "read_reachability".into(),
        Serving::read_reachability(parts.serving),
    );
    if let Some(family) = source_family {
        provenance.insert("resolver_pointer_source_family".into(), json!(family));
    }

    let mut coverage = match &selection.unsupported_reason {
        None => json!({"status": "projected", "exhaustiveness": "not_asserted"}),
        Some(reason) => json!({"status": "unsupported", "exhaustiveness": "not_asserted",
                               "unsupported_reason": reason}),
    };
    if shape == CoverageShape::WithBasis
        && let Value::Object(coverage) = &mut coverage
    {
        for key in ["source_classes_considered", "enumeration_basis"] {
            if let Some(value) = coverage_block.get(key).filter(|value| !value.is_null()) {
                coverage.insert(key.into(), value.clone());
            }
        }
    }

    let publication = parts.publication;
    let (surface_binding_id, resource_id, binding_kind) = match binding {
        Some(binding) if !mismatch => (
            uuid(Some(&binding.surface_binding_id))?,
            uuid(Some(&binding.resource_id))?,
            binding
                .binding_kind
                .as_deref()
                .map(SurfaceBindingKind::parse)
                .transpose()?,
        ),
        _ => (None, None, None),
    };
    let normalized = bigname_domain::normalization::normalize_name(&parts.surface.raw_name)
        .with_context(|| {
            format!(
                "phase name row {} has an unreadable active raw_name",
                parts.surface.logical_name_id
            )
        })?;
    Ok(NameCurrentRow {
        logical_name_id: parts.surface.logical_name_id.clone(),
        namespace: parts.surface.namespace.clone(),
        canonical_display_name: normalized.canonical_display_name,
        normalized_name: normalized.normalized_name,
        namehash: parts.surface.namehash.clone(),
        surface_binding_id,
        token_lineage_id: resource_id.and(parts.token_lineage_id),
        resource_id,
        serving_resource_id: uuid(parts.serving.and_then(Serving::resource_id))?,
        binding_kind,
        declared_summary: Value::Object(summary),
        provenance: Value::Object(provenance),
        coverage,
        chain_positions: json!({
            slot(&publication.chain_id): {
                "chain_id": publication.chain_id,
                "block_number": publication.block_number,
                "block_hash": publication.block_hash,
                "timestamp": publication.block_timestamp_json,
            }
        }),
        canonicality_summary: json!({
            "state": "canonical_lineage",
            "target_block_number": publication.block_number,
            "target_block_hash": publication.block_hash,
        }),
        manifest_version: 1,
        last_recomputed_at: publication.block_timestamp,
    })
}
