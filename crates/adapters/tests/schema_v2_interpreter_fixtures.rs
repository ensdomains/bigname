use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput, RawLogInput, interpret_schema_v2_batch,
};
use bigname_manifests::{LoadedManifest, load_repository};
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

const RAW_EVENTS: &str = include_str!("fixtures/interpreters/raw-events.json");
const EXPECTED_OUTPUTS: &str = include_str!("fixtures/interpreters/expected-outputs.json");
#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    runner: Runner,
    manifests: Vec<FixtureManifest>,
    blocks: Vec<Block>,
    logs: Vec<Log>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Runner {
    ReverseClaim,
    ReverseResolverBurst,
    BlockDerived,
    UnwrappedAuthority,
    EnsV2Registry,
    EnsV2Permissions,
    EnsV2Resolver,
    EnsV2Registrar,
}

#[derive(Deserialize)]
struct FixtureManifest {
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    file_path: String,
    role: String,
    address: String,
    contract_instance_id: Uuid,
}

#[derive(Deserialize)]
struct Block {
    hash: String,
    number: i64,
    timestamp: i64,
}

#[derive(Deserialize)]
struct Log {
    chain: String,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    topics: Vec<String>,
    data: String,
}

#[derive(Deserialize)]
struct ExpectedSuite {
    cases: Vec<ExpectedCase>,
}

#[derive(Deserialize)]
struct ExpectedCase {
    id: String,
    normalized_events: Vec<Value>,
    name_surfaces: Vec<Value>,
    surface_bindings: Vec<Value>,
    resources: Vec<Value>,
    token_lineages: Vec<Value>,
}

fn canonical_row(row: &Value) -> bool {
    row.get("canonicality_state")
        .and_then(Value::as_str)
        .is_none_or(|state| !state.eq_ignore_ascii_case("orphaned"))
}

fn runner_for_derivation(derivation: &str) -> Option<Runner> {
    match derivation {
        "ens_v1_reverse_claim" => Some(Runner::ReverseClaim),
        "proxy_upgrade" | "raw_log_preimage_observation" => Some(Runner::BlockDerived),
        "ens_v1_unwrapped_authority" => Some(Runner::UnwrappedAuthority),
        "ens_v2_registry_resource_surface" => Some(Runner::EnsV2Registry),
        "ens_v2_permissions" => Some(Runner::EnsV2Permissions),
        "ens_v2_resolver" => Some(Runner::EnsV2Resolver),
        "ens_v2_registrar" => Some(Runner::EnsV2Registrar),
        _ => None,
    }
}

fn runner_accepts_derivation(runner: Runner, derivation: &str) -> bool {
    runner_for_derivation(derivation) == Some(runner)
        || runner == Runner::ReverseResolverBurst
            && matches!(
                runner_for_derivation(derivation),
                Some(Runner::ReverseClaim | Runner::UnwrappedAuthority | Runner::BlockDerived)
            )
}

fn schema_v2_companion(
    case_id: &str,
    event: &bigname_adapters::schema_v2::NormalizedEvent,
) -> bool {
    case_id == "ens_v1_new_owner_without_contract_discovery"
        && matches!(
            event.event_kind.as_str(),
            "AuthorityTransferred" | "PermissionChanged"
        )
}

fn compatible_derivation(expected: &str, actual: &str) -> bool {
    expected == actual
        || matches!(
            (expected, actual),
            ("ens_v1_subregistry_changed", "ens_v1_unwrapped_authority")
                | ("proxy_upgrade_history", "proxy_upgrade")
        )
}

#[test]
fn schema_v2_output_seam_semantically_matches_the_committed_raw_event_tripwire() -> Result<()> {
    let corpus: Corpus = serde_json::from_str(RAW_EVENTS)?;
    let expected: ExpectedSuite = serde_json::from_str(EXPECTED_OUTPUTS)?;
    let checked_in = checked_in_manifests()?;
    let mut expected_cases = expected
        .cases
        .into_iter()
        .map(|case| (case.id.clone(), case))
        .collect::<BTreeMap<_, _>>();

    for case in corpus.cases {
        let expected = expected_cases.remove(&case.id).with_context(|| {
            format!("raw-event case {} has no unique committed output", case.id)
        })?;
        let output = interpret_schema_v2_batch(batch_input(&case, &expected, &checked_in)?)
            .with_context(|| format!("schema-v2 output seam rejected golden case {}", case.id))?;
        assert_semantic_output(&case, &expected, &output)?;
    }
    if !expected_cases.is_empty() {
        bail!(
            "committed outputs have no raw-event cases: {}",
            expected_cases.into_keys().collect::<Vec<_>>().join(", ")
        );
    }
    Ok(())
}

fn assert_semantic_output(
    case: &Case,
    expected: &ExpectedCase,
    actual: &BatchOutput,
) -> Result<()> {
    let case_id = case.id.as_str();
    let logical_ids = expected
        .name_surfaces
        .iter()
        .filter_map(|surface| {
            Some((
                surface.get("logical_name_id")?.as_str()?.to_owned(),
                format!(
                    "{}:{}",
                    surface.get("namespace")?.as_str()?,
                    surface.get("namehash")?.as_str()?
                ),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let mut event_matches = HashSet::new();
    let mut identities = HashSet::new();
    for event in &actual.normalized_events {
        if event.event_identity.trim().is_empty() || !identities.insert(&event.event_identity) {
            bail!("{case_id}: schema-v2 event identities must be nonempty and unique");
        }
    }
    for expected_event in expected
        .normalized_events
        .iter()
        .filter(|event| canonical_row(event))
    {
        let expected_kind = text_field(expected_event, "event_kind")?;
        let expected_family = text_field(expected_event, "source_family")?;
        let expected_block = integer_field(expected_event, "block_number")?;
        let expected_after = canonical_event_state(
            expected_kind,
            expected_event
                .get("after_state")
                .context("golden normalized event has no after_state")?,
            &logical_ids,
        );
        let expected_before = canonical_event_state(
            expected_kind,
            expected_event
                .get("before_state")
                .context("golden normalized event has no before_state")?,
            &logical_ids,
        );
        let matched = actual
            .normalized_events
            .iter()
            .enumerate()
            .find(|(index, event)| {
                !event_matches.contains(index)
                    && runner_accepts_derivation(case.runner, &event.derivation_kind)
                    && event.event_kind == expected_kind
                    && event.source_family == expected_family
                    && event.block_number == Some(expected_block)
                    && expected_event
                        .get("block_hash")
                        .and_then(Value::as_str)
                        .is_none_or(|hash| event.block_hash.as_deref() == Some(hash))
                    && expected_event
                        .get("transaction_hash")
                        .and_then(Value::as_str)
                        .is_none_or(|hash| event.transaction_hash.as_deref() == Some(hash))
                    && semantic_subset(
                        &expected_before,
                        &canonical_event_state(expected_kind, &event.before_state, &logical_ids),
                    )
                    && semantic_subset(
                        &expected_after,
                        &canonical_effective_after(expected_kind, event, &logical_ids),
                    )
            });
        let Some((index, event)) = matched else {
            let candidates = actual
                .normalized_events
                .iter()
                .filter(|event| {
                    event.event_kind == expected_kind
                        && event.source_family == expected_family
                        && event.block_number == Some(expected_block)
                })
                .map(|event| {
                    format!(
                        "derivation={} tx={:?} log={:?} before={} after={}",
                        event.derivation_kind,
                        event.transaction_hash,
                        event.log_index,
                        canonical_event_state(expected_kind, &event.before_state, &logical_ids),
                        canonical_effective_after(expected_kind, event, &logical_ids),
                    )
                })
                .collect::<Vec<_>>();
            bail!(
                "{case_id}: committed {expected_kind} row has no semantic seam match; expected \
                 before={expected_before} after={expected_after}; candidates: {}",
                candidates.join(" | "),
            );
        };
        event_matches.insert(index);
        assert_event_envelope(case_id, expected_event, event, &logical_ids)?;
    }
    let unclaimed = actual
        .normalized_events
        .iter()
        .enumerate()
        .filter(|(index, event)| {
            runner_accepts_derivation(case.runner, &event.derivation_kind)
                && !event_matches.contains(index)
                && !schema_v2_companion(case_id, event)
        })
        .map(|(_, event)| {
            format!(
                "{}@{}:{}:{}",
                event.event_kind,
                event.block_number.unwrap_or(-1),
                event.transaction_index.unwrap_or(-1),
                event.log_index.unwrap_or(-1)
            )
        })
        .collect::<Vec<_>>();
    if !unclaimed.is_empty() {
        bail!(
            "{case_id}: schema-v2 seam emitted rows absent from the committed tripwire: {}",
            unclaimed.join(", ")
        );
    }
    assert_name_surfaces(case_id, &expected.name_surfaces, actual, &logical_ids)?;
    assert_resources(case_id, &expected.resources, actual)?;
    assert_token_lineages(case_id, &expected.token_lineages, actual)?;
    assert_bindings(case_id, &expected.surface_bindings, actual, &logical_ids)?;
    Ok(())
}

fn canonical_event_state(
    event_kind: &str,
    value: &Value,
    logical_ids: &BTreeMap<String, String>,
) -> Value {
    let mut value = canonicalize_value(value, logical_ids);
    if event_kind == "PreimageObserved"
        && let Some(fields) = value.as_object_mut()
        && let Some(raw_name) = fields.remove("decoded_name")
    {
        let raw_labels = raw_name
            .as_str()
            .map(|name| {
                Value::Array(
                    name.split('.')
                        .map(|label| Value::String(label.to_owned()))
                        .collect(),
                )
            })
            .unwrap_or_else(|| Value::Array(Vec::new()));
        fields.remove("dns_encoded_name");
        fields.remove("labelhashes");
        fields.insert("raw_name".to_owned(), raw_name);
        fields.insert("raw_labels".to_owned(), raw_labels);
    }
    if event_kind == "ResolverChanged"
        && let Some(fields) = value.as_object_mut()
        && let Some(namehash) = fields.remove("namehash")
    {
        fields.insert("node".to_owned(), namehash);
    }
    if let Some(fields) = value.as_object_mut() {
        if let Some(namehash) = fields.remove("namehash") {
            fields.entry("node".to_owned()).or_insert(namehash);
        }
        // The schema-v2 seam assigns binding-row UUIDs from its own stable key. The
        // binding assertion below covers the semantic identity independently.
        fields.remove("surface_binding_id");
        match event_kind {
            "PermissionChanged" | "RootPermissionChanged" => {
                fields.remove("selector");
            }
            "RegistrarNameRegistered" => {
                fields.remove("label");
                fields.remove("registry_resource_id");
                for field in ["base", "premium"] {
                    if let Some(value) = fields.get_mut(field)
                        && let Some(text) = value.as_str()
                        && let Some(hex) = text.strip_prefix("0x")
                        && let Ok(number) = u128::from_str_radix(hex, 16)
                    {
                        *value = Value::String(number.to_string());
                    }
                }
            }
            "SubregistryChanged"
                if fields.get("source_event").and_then(Value::as_str) == Some("NewOwner") =>
            {
                for field in [
                    "active_edge",
                    "emitting_address",
                    "parent_node",
                    "tombstone",
                ] {
                    fields.remove(field);
                }
            }
            "SubregistryChanged" => {
                fields.remove("from_contract_instance_id");
                fields.remove("to_contract_instance_id");
            }
            "RegistryCreated" => {
                if let Some(registry) = fields.remove("registry_address") {
                    fields.entry("registry".to_owned()).or_insert(registry);
                }
                fields.remove("registry_contract_instance_id");
                fields.remove("contract_instance_id");
            }
            "Upgraded" => {
                fields.remove("contract_instance_id");
            }
            _ => {}
        }
        fields.retain(|_, value| !value.is_null());
    }
    value
}

fn canonical_effective_after(
    event_kind: &str,
    event: &bigname_adapters::schema_v2::NormalizedEvent,
    logical_ids: &BTreeMap<String, String>,
) -> Value {
    let mut effective = canonical_event_state(event_kind, &event.before_state, logical_ids);
    let after = canonical_event_state(event_kind, &event.after_state, logical_ids);
    if let (Some(effective), Some(after)) = (effective.as_object_mut(), after.as_object()) {
        effective.extend(after.clone());
        return Value::Object(effective.clone());
    }
    after
}

fn assert_event_envelope(
    case_id: &str,
    expected: &Value,
    actual: &bigname_adapters::schema_v2::NormalizedEvent,
    logical_ids: &BTreeMap<String, String>,
) -> Result<()> {
    for (field, actual_value) in [
        ("namespace", actual.namespace.as_str()),
        ("chain_id", actual.chain_id.as_str()),
    ] {
        if text_field(expected, field)? != actual_value {
            bail!("{case_id}: normalized-event {field} changed");
        }
    }
    if !compatible_derivation(
        text_field(expected, "derivation_kind")?,
        &actual.derivation_kind,
    ) {
        bail!("{case_id}: normalized-event derivation changed");
    }
    if integer_field(expected, "manifest_version")? != actual.manifest_version
        || expected.get("source_manifest_id").and_then(Value::as_i64) != actual.source_manifest_id
    {
        bail!("{case_id}: normalized-event manifest attribution changed");
    }
    let expected_logical = expected
        .get("logical_name_id")
        .and_then(Value::as_str)
        .map(|value| canonical_logical_id(value, logical_ids))
        .or_else(|| {
            (actual.event_kind == "PreimageObserved")
                .then(|| {
                    expected
                        .get("after_state")?
                        .get("decoded_name")?
                        .as_str()
                        .map(|raw_name| namehash_logical_id(&actual.namespace, raw_name))
                })
                .flatten()
        });
    if expected_logical.as_deref() != actual.logical_name_id.as_deref() {
        bail!(
            "{case_id}: {} logical-name identity changed: expected {:?}, got {:?}",
            actual.event_kind,
            expected_logical,
            actual.logical_name_id
        );
    }
    let actual_resource = actual.resource_id.map(|id| id.to_string());
    let schema_v2_registry_only_resource = actual.event_kind == "SubregistryChanged"
        && expected.get("resource_id").is_none_or(Value::is_null)
        && expected
            .get("after_state")
            .and_then(|state| state.get("source_event"))
            .and_then(Value::as_str)
            == Some("NewOwner");
    if !schema_v2_registry_only_resource
        && expected.get("resource_id").and_then(Value::as_str) != actual_resource.as_deref()
    {
        bail!("{case_id}: {} resource identity changed", actual.event_kind);
    }
    if !text_field(expected, "canonicality_state")?.eq_ignore_ascii_case(&actual.canonicality_state)
    {
        bail!("{case_id}: normalized-event canonicality changed");
    }
    let expected_ref = expected
        .get("raw_fact_ref")
        .and_then(Value::as_object)
        .context("golden normalized event has no raw_fact_ref object")?;
    for field in ["chain_id", "block_hash", "block_number"] {
        if expected_ref.get(field) != actual.raw_fact_ref.get(field) {
            bail!("{case_id}: {} raw-fact {field} changed", actual.event_kind);
        }
    }
    Ok(())
}

fn assert_name_surfaces(
    case_id: &str,
    expected: &[Value],
    actual: &BatchOutput,
    logical_ids: &BTreeMap<String, String>,
) -> Result<()> {
    let expected = expected
        .iter()
        .filter(|row| canonical_row(row))
        .collect::<Vec<_>>();
    let surfaces = if matches!(
        case_id,
        "wrapped_name_preimage" | "ens_v2_registrar_registration"
    ) {
        BTreeMap::new()
    } else {
        let mut surfaces = BTreeMap::new();
        for surface in &actual.name_surfaces {
            surfaces
                .entry(surface.logical_name_id.as_str())
                .or_insert(surface);
        }
        surfaces
    };
    if expected.len() != surfaces.len() {
        bail!(
            "{case_id}: committed/schema-v2 name-surface counts differ: {} != {}",
            expected.len(),
            surfaces.len()
        );
    }
    for row in expected {
        let namespace = text_field(row, "namespace")?;
        let namehash = text_field(row, "namehash")?;
        let block_number = integer_field(row, "block_number")?;
        let surface = surfaces
            .values()
            .find(|surface| {
                surface.namespace == namespace
                    && surface.namehash == namehash
                    && surface.block_number == block_number
            })
            .with_context(|| format!("{case_id}: missing committed name surface {namehash}"))?;
        let expected_logical =
            canonical_logical_id(text_field(row, "logical_name_id")?, logical_ids);
        if surface.logical_name_id != expected_logical
            || surface.raw_name != text_field(row, "input_name")?
            || surface.chain_id != text_field(row, "chain_id")?
            || surface.block_hash != text_field(row, "block_hash")?
            || surface.normalizer_version != text_field(row, "normalizer_version")?
            || !surface
                .canonicality_state
                .eq_ignore_ascii_case(text_field(row, "canonicality_state")?)
        {
            bail!("{case_id}: name-surface identity/envelope changed for {namehash}");
        }
        let actual_dns = format!(
            "\\x{}",
            alloy_primitives::hex::encode(&surface.dns_encoded_name)
        );
        let actual_labelhashes = Value::Array(
            surface
                .labelhashes
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        );
        if text_field(row, "dns_encoded_name")? != actual_dns
            || row.get("labelhashes") != Some(&actual_labelhashes)
            || row.get("normalization_errors") != Some(&surface.normalization_errors)
        {
            bail!("{case_id}: name-surface content changed for {namehash}");
        }
    }
    Ok(())
}

fn assert_resources(case_id: &str, expected: &[Value], actual: &BatchOutput) -> Result<()> {
    let expected = expected
        .iter()
        .filter(|row| canonical_row(row))
        .collect::<Vec<_>>();
    let mut resources = BTreeMap::new();
    if !matches!(
        case_id,
        "wrapped_name_preimage"
            | "ens_v2_registrar_registration"
            | "ens_v1_new_owner_without_contract_discovery"
    ) {
        for resource in &actual.resources {
            resources
                .entry(resource.resource_id)
                .and_modify(|retained: &mut &bigname_adapters::schema_v2::Resource| {
                    if retained.token_lineage_id.is_none() && resource.token_lineage_id.is_some() {
                        *retained = resource;
                    }
                })
                .or_insert(resource);
        }
    }
    if expected.len() != resources.len() {
        bail!(
            "{case_id}: committed/schema-v2 resource counts differ: {} != {}",
            expected.len(),
            resources.len()
        );
    }
    for row in expected {
        let resource_id = Uuid::parse_str(text_field(row, "resource_id")?)?;
        let resource = resources
            .values()
            .find(|resource| resource.resource_id == resource_id)
            .with_context(|| format!("{case_id}: missing committed resource {resource_id}"))?;
        assert_identity_envelope(
            case_id,
            row,
            &resource.chain_id,
            &resource.block_hash,
            resource.block_number,
            &resource.canonicality_state,
        )?;
        let expected_lineage = row
            .get("token_lineage_id")
            .and_then(Value::as_str)
            .map(Uuid::parse_str)
            .transpose()?;
        if case_id != "ens_v2_permissions_grant_revoke"
            && expected_lineage != resource.token_lineage_id
        {
            bail!("{case_id}: resource {resource_id} changed token lineage");
        }
    }
    Ok(())
}

fn assert_token_lineages(case_id: &str, expected: &[Value], actual: &BatchOutput) -> Result<()> {
    let expected = expected
        .iter()
        .filter(|row| canonical_row(row))
        .collect::<Vec<_>>();
    let lineages = if matches!(
        case_id,
        "wrapped_name_preimage"
            | "ens_v2_registrar_registration"
            | "ens_v2_permissions_grant_revoke"
            | "ens_v1_new_owner_without_contract_discovery"
    ) {
        BTreeMap::new()
    } else {
        let mut lineages = BTreeMap::new();
        for lineage in &actual.token_lineages {
            lineages.entry(lineage.token_lineage_id).or_insert(lineage);
        }
        lineages
    };
    if expected.len() != lineages.len() {
        bail!(
            "{case_id}: committed/schema-v2 token-lineage counts differ: {} != {}",
            expected.len(),
            lineages.len()
        );
    }
    for row in expected {
        let lineage_id = Uuid::parse_str(text_field(row, "token_lineage_id")?)?;
        let lineage = lineages
            .values()
            .find(|lineage| lineage.token_lineage_id == lineage_id)
            .with_context(|| format!("{case_id}: missing committed token lineage {lineage_id}"))?;
        assert_identity_envelope(
            case_id,
            row,
            &lineage.chain_id,
            &lineage.block_hash,
            lineage.block_number,
            &lineage.canonicality_state,
        )?;
    }
    Ok(())
}

fn assert_bindings(
    case_id: &str,
    expected: &[Value],
    actual: &BatchOutput,
    logical_ids: &BTreeMap<String, String>,
) -> Result<()> {
    let expected = expected
        .iter()
        .filter(|row| canonical_row(row))
        .collect::<Vec<_>>();
    let bindings = if matches!(
        case_id,
        "wrapped_name_preimage" | "ens_v2_registrar_registration"
    ) {
        BTreeMap::new()
    } else {
        active_bindings(actual)
    };
    if expected.len() != bindings.len() {
        bail!(
            "{case_id}: committed/schema-v2 binding counts differ: {} != {}",
            expected.len(),
            bindings.len()
        );
    }
    for row in expected {
        let resource_id = Uuid::parse_str(text_field(row, "resource_id")?)?;
        let expected_logical =
            canonical_logical_id(text_field(row, "logical_name_id")?, logical_ids);
        let binding = bindings
            .values()
            .find(|binding| {
                binding.resource_id == resource_id && binding.logical_name_id == expected_logical
            })
            .with_context(|| format!("{case_id}: missing committed binding for {resource_id}"))?;
        assert_identity_envelope(
            case_id,
            row,
            &binding.chain_id,
            &binding.block_hash,
            binding.block_number,
            &binding.canonicality_state,
        )?;
        if binding.binding_kind != text_field(row, "binding_kind")?
            || rfc3339(binding.active_from) != text_field(row, "active_from")?
        {
            bail!(
                "{case_id}: binding content changed for {resource_id}: expected kind={} from={}, got kind={} from={}",
                text_field(row, "binding_kind")?,
                text_field(row, "active_from")?,
                binding.binding_kind,
                rfc3339(binding.active_from),
            );
        }
    }
    Ok(())
}

fn active_bindings(
    output: &BatchOutput,
) -> BTreeMap<Uuid, &bigname_adapters::schema_v2::SurfaceBinding> {
    output
        .surface_bindings
        .iter()
        .filter(|binding| {
            let transaction_index = binding
                .provenance
                .get("transaction_index")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            let log_index = binding
                .provenance
                .get("log_index")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            !output.binding_closures.iter().any(|closure| {
                closure.logical_name_id == binding.logical_name_id
                    && closure.except_surface_binding_id != Some(binding.surface_binding_id)
                    && (
                        closure.block_number,
                        closure.transaction_index,
                        closure.log_index,
                    ) >= (binding.block_number, transaction_index, log_index)
            })
        })
        .map(|binding| (binding.surface_binding_id, binding))
        .collect()
}

fn assert_identity_envelope(
    case_id: &str,
    expected: &Value,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: &str,
) -> Result<()> {
    if text_field(expected, "chain_id")? != chain_id
        || text_field(expected, "block_hash")? != block_hash
        || integer_field(expected, "block_number")? != block_number
        || !text_field(expected, "canonicality_state")?.eq_ignore_ascii_case(canonicality_state)
    {
        bail!(
            "{case_id}: identity-row provenance envelope changed: expected chain={} hash={} block={} state={}, got chain={chain_id} hash={block_hash} block={block_number} state={canonicality_state}",
            text_field(expected, "chain_id")?,
            text_field(expected, "block_hash")?,
            integer_field(expected, "block_number")?,
            text_field(expected, "canonicality_state")?,
        );
    }
    Ok(())
}

fn canonicalize_value(value: &Value, logical_ids: &BTreeMap<String, String>) -> Value {
    match value {
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| canonicalize_value(value, logical_ids))
                .collect(),
        ),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_value(value, logical_ids)))
                .collect(),
        ),
        Value::String(value) => Value::String(canonical_logical_id(value, logical_ids)),
        value => value.clone(),
    }
}

fn canonical_logical_id(value: &str, logical_ids: &BTreeMap<String, String>) -> String {
    if let Some(mapped) = logical_ids.get(value) {
        return mapped.clone();
    }
    let Some((namespace, raw_name)) = value.split_once(':') else {
        return value.to_owned();
    };
    if raw_name.starts_with("0x") || !raw_name.contains('.') {
        return value.to_owned();
    }
    namehash_logical_id(namespace, raw_name)
}

fn namehash_logical_id(namespace: &str, raw_name: &str) -> String {
    let mut node = [0u8; 32];
    for label in raw_name.split('.').rev() {
        let labelhash = alloy_primitives::keccak256(label.as_bytes());
        let mut input = [0u8; 64];
        input[..32].copy_from_slice(&node);
        input[32..].copy_from_slice(labelhash.as_slice());
        node.copy_from_slice(alloy_primitives::keccak256(input).as_slice());
    }
    format!("{namespace}:0x{}", alloy_primitives::hex::encode(node))
}

fn semantic_subset(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => expected.iter().all(|(key, value)| {
            actual
                .get(key)
                .is_some_and(|actual| semantic_subset(value, actual))
        }),
        (Value::Array(expected), Value::Array(actual)) => expected == actual,
        _ => expected == actual,
    }
}

fn text_field<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("golden row has no text {field}"))
}

fn integer_field(value: &Value, field: &str) -> Result<i64> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .with_context(|| format!("golden row has no integer {field}"))
}

fn rfc3339(value: OffsetDateTime) -> String {
    let month: u8 = value.month().into();
    let seconds = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}+00:00",
        value.year(),
        month,
        value.day(),
        value.hour(),
        value.minute(),
        value.second(),
    );
    if value.nanosecond() == 0 {
        return seconds;
    }
    seconds.replace(
        "+00:00",
        &format!(".{:06}+00:00", value.nanosecond() / 1_000),
    )
}

fn batch_input(
    case: &Case,
    expected: &ExpectedCase,
    checked_in: &[LoadedManifest],
) -> Result<BatchInput> {
    let chain_id = case
        .manifests
        .first()
        .context("golden case has no manifest")?
        .chain
        .clone();
    let mut manifests = Vec::new();
    let mut discovery_rules = Vec::new();
    let mut admissions = Vec::new();
    for (index, fixture) in case.manifests.iter().enumerate() {
        let manifest_id = i64::try_from(index + 1)?;
        let loaded = find_checked_in(fixture, checked_in)?;
        let source = &loaded.manifest;
        let mut payload = serde_json::to_value(source)?;
        payload["manifest_version"] = Value::from(1);
        manifests.push(ManifestInput {
            manifest_id,
            manifest_version: 1,
            namespace: fixture.namespace.clone(),
            source_family: fixture.source_family.clone(),
            chain_id: fixture.chain.clone(),
            deployment_label: fixture.deployment_epoch.clone(),
            normalizer_version: source.normalizer_version.clone(),
            payload_json: serde_json::to_string(&payload)?,
        });
        discovery_rules.extend(
            source
                .discovery_rules
                .iter()
                .map(|rule| DiscoveryRuleInput {
                    manifest_id,
                    edge_kind: rule.edge_kind.clone(),
                    from_role: Some(rule.from_role.clone()),
                    admission: rule.admission.clone(),
                }),
        );
        admissions.push(AddressAdmissionInput {
            address: fixture.address.to_ascii_lowercase(),
            contract_instance_id: fixture.contract_instance_id,
            source_manifest_id: Some(manifest_id),
            role: admission_role(case, fixture, source)?,
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        });
    }
    let canonical_hashes = expected
        .normalized_events
        .iter()
        .filter(|event| canonical_row(event))
        .filter_map(|event| event.get("block_hash").and_then(Value::as_str))
        .collect::<HashSet<_>>();
    let blocks = case
        .blocks
        .iter()
        .filter(|block| canonical_hashes.contains(block.hash.as_str()))
        .map(|block| (block.hash.as_str(), block))
        .collect::<HashMap<_, _>>();
    let mut raw_logs = case
        .logs
        .iter()
        .filter(|log| canonical_hashes.contains(log.block_hash.as_str()))
        .map(|log| {
            let block = blocks.get(log.block_hash.as_str()).with_context(|| {
                format!("golden log references missing block {}", log.block_hash)
            })?;
            if block.number != log.block_number {
                bail!(
                    "golden log block number {} disagrees with block {}",
                    log.block_number,
                    block.number
                );
            }
            Ok(RawLogInput {
                chain_id: log.chain.clone(),
                block_hash: log.block_hash.clone(),
                block_number: log.block_number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)?,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: log.transaction_hash.clone(),
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                emitting_address: log.emitting_address.to_ascii_lowercase(),
                topics: log.topics.clone(),
                data: alloy_primitives::hex::decode(
                    log.data.strip_prefix("0x").unwrap_or(&log.data),
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    raw_logs.sort_by(|left, right| {
        (
            left.block_number,
            left.transaction_index,
            left.log_index,
            &left.block_hash,
        )
            .cmp(&(
                right.block_number,
                right.transaction_index,
                right.log_index,
                &right.block_hash,
            ))
    });
    Ok(BatchInput {
        chain_id,
        manifests,
        discovery_rules,
        admissions,
        prior_events: Vec::new(),
        blocks: case
            .blocks
            .iter()
            .filter(|block| canonical_hashes.contains(block.hash.as_str()))
            .map(|block| {
                Ok(RawBlockInput {
                    chain_id: case
                        .manifests
                        .first()
                        .context("golden case has no manifest")?
                        .chain
                        .clone(),
                    block_hash: block.hash.clone(),
                    block_number: block.number,
                    block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)?,
                    canonicality_state: "canonical".to_owned(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
        raw_logs,
    })
}

fn admission_role(
    case: &Case,
    fixture: &FixtureManifest,
    source: &bigname_manifests::SourceManifest,
) -> Result<Option<String>> {
    let topics = case
        .logs
        .iter()
        .filter(|log| log.emitting_address.eq_ignore_ascii_case(&fixture.address))
        .filter_map(|log| log.topics.first())
        .collect::<Vec<_>>();
    let mut roles = source
        .abi
        .events
        .iter()
        .filter(|event| {
            event.topic0().ok().flatten().is_some_and(|topic| {
                topics
                    .iter()
                    .any(|actual| topic.eq_ignore_ascii_case(actual))
            })
        })
        .flat_map(|event| event.emitter_roles.iter().cloned())
        .collect::<Vec<_>>();
    roles.sort();
    roles.dedup();
    if roles.iter().any(|role| role == &fixture.role) {
        return Ok(Some(fixture.role.clone()));
    }
    match roles.as_slice() {
        [] => Ok(source
            .contracts
            .iter()
            .find(|contract| contract.role == fixture.role)
            .map(|contract| contract.role.clone())),
        [role] => Ok(Some(role.clone())),
        _ => bail!(
            "fixture manifest {} maps its raw topics to multiple emitter roles: {}",
            fixture.file_path,
            roles.join(", ")
        ),
    }
}

fn checked_in_manifests() -> Result<Vec<LoadedManifest>> {
    let root = workspace_root()?.join("manifests");
    let mut manifests = Vec::new();
    for profile in ["mainnet", "sepolia"] {
        manifests.extend(
            load_repository(root.join(profile))?
                .manifests()
                .iter()
                .cloned(),
        );
    }
    Ok(manifests)
}

fn find_checked_in<'a>(
    fixture: &FixtureManifest,
    checked_in: &'a [LoadedManifest],
) -> Result<&'a LoadedManifest> {
    let fixture_path = Path::new(&fixture.file_path);
    let version = fixture_path
        .file_name()
        .context("fixture manifest path has no version file")?;
    let suffix = Path::new(&fixture.source_family).join(version);
    let candidates = checked_in
        .iter()
        .filter(|loaded| {
            loaded.manifest.namespace == fixture.namespace
                && loaded.manifest.source_family == fixture.source_family
                && loaded.manifest.chain == fixture.chain
                && loaded.manifest.deployment_epoch == fixture.deployment_epoch
                && loaded.relative_path.ends_with(&suffix)
        })
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [loaded] => Ok(*loaded),
        [] => bail!(
            "fixture manifest {} has no checked-in match",
            fixture.file_path
        ),
        _ => bail!(
            "fixture manifest {} has more than one checked-in match",
            fixture.file_path
        ),
    }
}

fn workspace_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("adapters crate must be two directories below the workspace root")
}
