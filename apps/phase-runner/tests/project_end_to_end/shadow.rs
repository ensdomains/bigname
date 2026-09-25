//! Shadow comparison (TYR-36 step 3): after the owned key families have followed a publication,
//! the family readers of `bigname_storage::families::control` compute the registration and
//! control blocks of every served name, the permission rows, restriction block and registry
//! binding of every summarised resource, the registry-operator rows the effective-permission
//! reader adds, and every account approval, and each is compared with what the production
//! readers serve from today's tables at the same publication.
//!
//! A difference passes only as a disclosed D12 same-block delta (design section 7 item 5; the
//! brief's section 4.3): the keyed events include two lifecycle events in one block whose
//! generated-id order disagrees with the canonical order, or a synthesised event beside another
//! event of its block. A difference that comes from a named step 2 discrepancy (section
//! "Known discrepancies" below) is counted and printed as `known_discrepancy` with its name, so
//! the finding stays visible without failing the run. Anything else is a mismatch.
//!
//! Known discrepancies, each a place where the step 2 families and today's builders disagree on
//! what a value is read from, reported rather than patched (step 3 changes no reducer):
//! - `unnamed_resource_event_in_key_state`: the F2a key state of a resource counts every
//!   lifecycle event whose resource is that resource, named or not (crates/project/src/families/
//!   lifecycle.rs:71-76), as the design's key-state relation says, while today's ENSv2 membership
//!   reads only the events emitted with the name (build.sql:322, :366-367). The interpreter's
//!   RegistryPathExpired release carries its resource and no name
//!   (crates/adapters/src/schema_v2/protocol/v2_registry/expiry.rs:58-59), so the families put it
//!   in the name's candidate and five-kind latest and today's read never does. The design does not
//!   rule on a resource-bearing unnamed event, so this is reported as a design question, not
//!   patched. On the fixture corpus it shows as `registration/latest_event_kind` served
//!   RegistrationRenewed, shadow RegistrationReleased.
//! - `registry_owner_without_its_position`: the ENSv1 control block's registry owner and latest
//!   kind are the latest admitted AuthorityTransferred, AuthorityEpochChanged or transfer
//!   (build.sql:649-694). F2c sets the owner only on AuthorityTransferred
//!   (crates/project/src/families/registry.rs:69-94) but stamps the node row with the position of
//!   every event that writes it, SubregistryChanged included (:120), and keeps neither the
//!   AuthorityTransferred's own position nor its resource, so the shadow can neither order it
//!   against an AuthorityEpochChanged nor admit it. On the fixture corpus a registry-only name's
//!   AuthorityTransferred (log 9) and AuthorityEpochChanged (log 11) are followed in the same
//!   block by a SubregistryChanged of the same node (log 12): served `control/latest_event_kind`
//!   AuthorityEpochChanged, shadow AuthorityTransferred. For an ENSv1 name a difference in these
//!   two fields is reported under this name.
//! - `authority_kind_defaulted_to_registrar`: the retained lifecycle row stores
//!   `authority_kind` with a `registrar` default when the event's after-state has none
//!   (crates/project/src/families/lifecycle.rs:317-322), while the served registration block
//!   reads the raw after-state and serves null (build.sql:30 from :394 and :425). The shadow
//!   cannot tell the two apart, so a served null against a shadow `registrar` is this finding.
//! - `authority_key_not_retained`: the registration's `authority_key` is the winning grant's,
//!   AuthorityEpochChanged's or registry-only SurfaceBound's after-state key (build.sql:31,
//!   :393-420). Step 2 keeps none of them: the retained row has no `authority_key` column
//!   (crates/project/src/families/lifecycle.rs:306-380), the key state's `last_grant` omits it
//!   (:432-437), and `authority_start_positions` keeps only the kind and resource
//!   (crates/project/src/families/identity.rs:92-95). The reader reads each of those places, so
//!   the finding closes by itself once step 2 stores the key, and until then a served key
//!   against a shadow null, with the trace saying the key was not seen, is this finding.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::{
    EffectivePermissionScope,
    families::control::{
        compare::{Difference, differences, field, same},
        lifecycle::{
            AuthoritySelection, CONTROL_FIELDS, Clock, NameInput, REGISTRATION_FIELDS,
            load_shadow_names,
        },
        permissions::{
            ResourceInput, effective_operator_rows, grant_json, load_shadow_approvals,
            load_shadow_permissions,
        },
        registry::{
            NameAttribution, load_observations, load_registry_nodes, ownerless_registry,
            registry_bindings, registry_generation,
        },
    },
    load_effective_permissions_by_resource_ids, load_name_current_by_logical_name_ids,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const NAME_CHUNK: usize = 500;
/// Mismatch lines printed per run; the counters count all of them.
const PRINTED: usize = 40;

/// What one comparison saw.
#[derive(Debug, Default)]
pub struct Report {
    pub names: usize,
    pub resources: usize,
    pub accounts: usize,
    pub equal: usize,
    pub expected_delta: usize,
    pub known_discrepancy: BTreeMap<&'static str, usize>,
    pub mismatched: usize,
    pub lines: Vec<String>,
}

/// Why an item's differences pass, if they do.
#[derive(Clone, Copy)]
enum Excuse {
    None,
    SameBlockOrder,
    Known(&'static str),
}

impl Report {
    /// Count one compared item. `excuse` decides, per differing field, whether it passes.
    fn item(&mut self, key: &str, diffs: Vec<Difference>, excuse: impl Fn(&Difference) -> Excuse) {
        if diffs.is_empty() {
            self.equal += 1;
            return;
        }
        let excused: Vec<(Difference, Excuse)> = diffs
            .into_iter()
            .map(|diff| {
                let why = excuse(&diff);
                (diff, why)
            })
            .collect();
        if excused.iter().any(|(_, why)| matches!(why, Excuse::None)) {
            self.mismatched += 1;
        } else if excused
            .iter()
            .any(|(_, why)| matches!(why, Excuse::SameBlockOrder))
        {
            self.expected_delta += 1;
        } else {
            for (_, why) in &excused {
                if let Excuse::Known(name) = why {
                    *self.known_discrepancy.entry(name).or_default() += 1;
                }
            }
        }
        for (diff, why) in excused {
            let (kind, case) = match why {
                Excuse::SameBlockOrder => ("EXPECTED_DELTA", "same_block_order"),
                Excuse::Known(name) => ("KNOWN_DISCREPANCY", name),
                Excuse::None => ("MISMATCH", "none"),
            };
            if self.lines.len() < PRINTED {
                self.lines.push(format!(
                    "SEPOLIA_END_TO_END_SHADOW_{kind} case={case} key={key} field={} served={} \
                     shadow={}",
                    diff.field, diff.served, diff.shadow
                ));
            }
        }
    }

    pub fn print(&self, target: i64) {
        for line in &self.lines {
            eprintln!("{line}");
        }
        let list = |counts: &BTreeMap<&'static str, usize>| {
            counts
                .iter()
                .map(|(reason, count)| format!("{reason}:{count}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        eprintln!(
            "SEPOLIA_END_TO_END_SHADOW target={target} names={} resources={} accounts={} \
             equal={} expected_delta={} mismatched={} known_discrepancy={}",
            self.names,
            self.resources,
            self.accounts,
            self.equal,
            self.expected_delta,
            self.mismatched,
            list(&self.known_discrepancy)
        );
    }
}

/// One retained lifecycle event's key, names, resource, position, identity and generated id.
type RetainedPosition = (
    String,
    Option<String>,
    Option<String>,
    i64,
    Option<i64>,
    Option<i64>,
    String,
    Option<i64>,
);

/// One served resource summary: resource, authority kind, root, restrictions, registry owner and
/// contract, binding provenance, chain positions and clear event id.
type SummaryRow = (
    String,
    Option<String>,
    Option<String>,
    Option<Value>,
    Option<String>,
    Option<String>,
    Option<Value>,
    Option<Value>,
    Option<Value>,
);

/// Keys whose retained lifecycle events hold a same-block pair the D12 order and the generated-id
/// order disagree on, or a synthesised event beside another event of its block: names (decoded
/// or original) and resources.
async fn same_block_keys(pool: &PgPool, chain: &str) -> Result<BTreeSet<String>> {
    let rows: Vec<RetainedPosition> = sqlx::query_as(
        "SELECT state_key, COALESCE(original_logical_name_id, decoded_logical_name_id),
                    resource_id::text, block_number, transaction_index, log_index,
                    event_identity, normalized_event_id
             FROM project_lifecycle_event WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    // Two groupings: events of one key in one block (a key's own candidate, brief 4.3 items 1, 2
    // and 4), and events of one name in one block, which also catches two grants on different
    // resources racing for a triple's association (item 3).
    let mut groups: BTreeMap<(String, i64), Vec<_>> = BTreeMap::new();
    for row in &rows {
        groups
            .entry((format!("key:{}", row.0), row.3))
            .or_default()
            .push(row);
        if let Some(name) = &row.1 {
            groups
                .entry((format!("name:{name}"), row.3))
                .or_default()
                .push(row);
        }
    }
    let mut keys = BTreeSet::new();
    for events in groups.values().filter(|events| events.len() > 1) {
        let synthesised = events.iter().any(|event| event.4.is_none());
        let mut by_position = events.clone();
        by_position
            .sort_by(|left, right| (left.4, left.5, &left.6).cmp(&(right.4, right.5, &right.6)));
        let mut by_id = events.clone();
        by_id.sort_by_key(|event| event.7);
        let disagree = by_position
            .iter()
            .zip(&by_id)
            .any(|(left, right)| left.6 != right.6);
        if synthesised || disagree {
            for event in events {
                keys.extend(event.1.clone());
                keys.extend(event.2.clone());
            }
        }
    }
    Ok(keys)
}

pub async fn compare(pool: &PgPool, chain: &str, target: i64) -> Result<Report> {
    let mut report = Report::default();
    let timestamp: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM block_timestamp)::bigint FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .bind(target)
    .fetch_one(pool)
    .await
    .context("target clock")?;
    let clock = Clock {
        block_number: target,
        timestamp_seconds: timestamp,
    };
    let ambiguous = same_block_keys(pool, chain).await?;

    // Names: registration and control, registry generation and the ownerless profile.
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT logical_name_id FROM name_current ORDER BY 1")
            .fetch_all(pool)
            .await?;
    let mut attributions = BTreeMap::new();
    for chunk in keys.chunks(NAME_CHUNK) {
        let rows = load_name_current_by_logical_name_ids(pool, chunk).await?;
        let inputs: Vec<NameInput> = rows
            .values()
            .map(|row| NameInput {
                logical_name_id: row.logical_name_id.clone(),
                namehash: row.namehash.to_ascii_lowercase(),
                selection: AuthoritySelection::from_provenance(&row.provenance),
            })
            .collect();
        let shadows = load_shadow_names(pool, chain, &clock, &inputs).await?;
        let node_keys: Vec<(String, String)> = inputs
            .iter()
            .map(|input| {
                (
                    row_namespace(&input.logical_name_id),
                    input.namehash.clone(),
                )
            })
            .collect();
        let nodes = load_registry_nodes(pool, chain, &node_keys).await?;
        for input in &inputs {
            let row = &rows[&input.logical_name_id];
            attributions.insert(
                input.logical_name_id.clone(),
                NameAttribution {
                    current_resource_id: row.resource_id.map(|id| id.to_string()),
                    authority_arm: input.selection.authority_arm.clone(),
                },
            );
            let shadow = &shadows[&input.logical_name_id];
            let summary = &row.declared_summary;
            let mut diffs = differences(
                summary.get("registration").unwrap_or(&Value::Null),
                &Value::Object(shadow.registration.clone()),
                &REGISTRATION_FIELDS,
            );
            for diff in &mut diffs {
                diff.field = format!("registration/{}", diff.field);
            }
            let mut control = differences(
                summary.get("control").unwrap_or(&Value::Null),
                &Value::Object(shadow.control.clone()),
                &CONTROL_FIELDS,
            );
            for diff in &mut control {
                diff.field = format!("control/{}", diff.field);
            }
            diffs.extend(control);
            let selection = row
                .provenance
                .get("authority_selection")
                .cloned()
                .unwrap_or(Value::Null);
            let node = nodes.get(&(
                row_namespace(&input.logical_name_id),
                input.namehash.clone(),
            ));
            let (generation, handoff) =
                registry_generation(node, input.selection.authority_arm.as_deref());
            for (path, shadow) in [
                ("registry_generation", json!(generation)),
                ("registry_handoff_block_number", json!(handoff)),
            ] {
                let served = field(&selection, path);
                if !same(served, &shadow) {
                    diffs.push(Difference {
                        field: format!("authority_selection/{path}"),
                        served: served.clone(),
                        shadow,
                    });
                }
            }
            let ownerless = ownerless_registry(
                node,
                input.selection.surface_binding_id.as_deref(),
                input.selection.authority_arm.as_deref(),
            );
            if ownerless != input.selection.ownerless_registry {
                diffs.push(Difference {
                    field: "authority_selection/ownerless_registry".into(),
                    served: json!(input.selection.ownerless_registry),
                    shadow: json!(ownerless),
                });
            }
            if !diffs.is_empty() && report.lines.len() < PRINTED {
                report.lines.push(format!(
                    "SEPOLIA_END_TO_END_SHADOW_TRACE key={} selection={} trace={}",
                    input.logical_name_id,
                    selection,
                    Value::Object(shadow.trace.clone())
                ));
            }
            let same_block = ambiguous.contains(&input.logical_name_id);
            let foreign = shadow.trace.contains_key("foreign_members");
            let v1 = !input.selection.is_v2();
            let key_unseen = shadow.trace.get("authority_key_retained") == Some(&json!(false));
            report.item(&input.logical_name_id, diffs, |diff| {
                if same_block {
                    Excuse::SameBlockOrder
                } else if diff.field == "registration/authority_key"
                    && key_unseen
                    && diff.shadow.is_null()
                {
                    Excuse::Known("authority_key_not_retained")
                } else if diff.field == "registration/authority_kind"
                    && diff.served.is_null()
                    && diff.shadow == json!("registrar")
                {
                    Excuse::Known("authority_kind_defaulted_to_registrar")
                } else if foreign {
                    Excuse::Known("unnamed_resource_event_in_key_state")
                } else if v1
                    && matches!(
                        diff.field.as_str(),
                        "control/latest_event_kind" | "control/registry_owner"
                    )
                {
                    Excuse::Known("registry_owner_without_its_position")
                } else {
                    Excuse::None
                }
            });
            report.names += 1;
        }
    }

    // Resources: permission rows, restriction block, registry binding and operator rows.
    let summaries: Vec<SummaryRow> = sqlx::query_as(
        "SELECT resource_id::text, authority_kind, root_resource_id::text,
                    resource_restrictions, registry_owner, registry_contract,
                    registry_binding_provenance, registry_binding_chain_positions,
                    provenance -> 'registry_binding_clear_event_id'
             FROM permissions_current_resource_summary ORDER BY 1",
    )
    .fetch_all(pool)
    .await?;
    let mut resource_ids: BTreeSet<String> = summaries.iter().map(|row| row.0.clone()).collect();
    let served_grants: Vec<(String, Value)> = sqlx::query_as(
        "SELECT resource_id::text, jsonb_build_object(
                    'resource_id', resource_id::text, 'subject', subject, 'scope', scope,
                    'scope_kind', scope_kind, 'scope_detail', scope_detail,
                    'effective_powers', effective_powers, 'grant_source', grant_source,
                    'revocation_source', revocation_source, 'inheritance_path', inheritance_path,
                    'transfer_behavior', transfer_behavior)
         FROM permissions_current ORDER BY resource_id, subject, scope",
    )
    .fetch_all(pool)
    .await?;
    let mut grants_by_resource: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for (resource, row) in served_grants {
        resource_ids.insert(resource.clone());
        grants_by_resource.entry(resource).or_default().push(row);
    }
    let summary_by_resource: BTreeMap<String, _> =
        summaries.iter().map(|row| (row.0.clone(), row)).collect();
    let inputs: Vec<ResourceInput> = resource_ids
        .iter()
        .map(|resource| {
            let summary = summary_by_resource.get(resource);
            ResourceInput {
                resource_id: resource.clone(),
                authority_kind: summary.and_then(|row| row.1.clone()),
                root_resource_id: summary.and_then(|row| row.2.clone()),
            }
        })
        .collect();
    let shadows = load_shadow_permissions(pool, chain, &clock, &inputs).await?;
    let observations = load_observations(pool, chain).await?;
    let bindings = registry_bindings(&observations, &attributions);
    let approvals = load_shadow_approvals(pool, chain).await?;
    let ids: Vec<Uuid> = resource_ids
        .iter()
        .filter_map(|resource| resource.parse().ok())
        .collect();
    let mut served_operators: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
    for chunk in ids.chunks(NAME_CHUNK) {
        for row in load_effective_permissions_by_resource_ids(pool, chunk, None).await? {
            if matches!(row.scope, EffectivePermissionScope::Account { .. }) {
                served_operators
                    .entry(row.resource_id.to_string())
                    .or_default()
                    .insert((row.subject.clone(), row.scope.storage_key()));
            }
        }
    }
    for resource in &resource_ids {
        let shadow = &shadows[resource];
        let mut diffs = Vec::new();
        let served: Vec<Value> = grants_by_resource
            .get(resource)
            .cloned()
            .unwrap_or_default();
        let computed: Vec<Value> = shadow.grants.iter().map(grant_json).collect();
        if !same(
            &Value::Array(served.clone()),
            &Value::Array(computed.clone()),
        ) {
            diffs.push(Difference {
                field: "permissions_current".into(),
                served: Value::Array(served),
                shadow: Value::Array(computed),
            });
        }
        let summary = summary_by_resource.get(resource);
        if let Some(summary) = summary {
            let restrictions = summary.3.clone().unwrap_or(Value::Null);
            let computed = shadow.restrictions.clone().unwrap_or(Value::Null);
            if !same(&restrictions, &computed) {
                diffs.push(Difference {
                    field: "resource_restrictions".into(),
                    served: restrictions,
                    shadow: computed,
                });
            }
            let binding = bindings.get(resource).cloned().unwrap_or_default();
            let served_binding = json!({
                "registry_owner": summary.4, "registry_contract": summary.5,
                "event_ids": summary.6.as_ref().and_then(|provenance| provenance.get("normalized_event_ids")).cloned(),
                "block_number": summary.7.as_ref().and_then(|positions| positions.get("block_number")).cloned(),
                "transaction_index": summary.7.as_ref().and_then(|positions| positions.get("transaction_index")).cloned(),
                "log_index": summary.7.as_ref().and_then(|positions| positions.get("log_index")).cloned(),
                "clear_event_id": summary.8,
            });
            let applicable = binding.registry_owner.is_some();
            let shadow_binding = json!({
                "registry_owner": binding.registry_owner, "registry_contract": binding.registry_contract,
                "event_ids": applicable.then(|| json!([binding.normalized_event_id])),
                "block_number": binding.position.as_ref().map(|position| position.block_number),
                "transaction_index": binding.position.as_ref().and_then(|position| position.transaction_index),
                "log_index": binding.position.as_ref().and_then(|position| position.log_index),
                "clear_event_id": binding.clear_event_identity.as_ref().and(binding.normalized_event_id),
            });
            diffs.extend(
                differences(
                    &served_binding,
                    &shadow_binding,
                    &[
                        "registry_owner",
                        "registry_contract",
                        "event_ids",
                        "block_number",
                        "transaction_index",
                        "log_index",
                        "clear_event_id",
                    ],
                )
                .into_iter()
                .map(|diff| Difference {
                    field: format!("registry_binding/{}", diff.field),
                    ..diff
                }),
            );
            let computed: BTreeSet<(String, String)> =
                effective_operator_rows(chain, resource, &binding, &approvals)
                    .into_iter()
                    .map(|row| (row.subject, row.scope))
                    .collect();
            let served = served_operators.get(resource).cloned().unwrap_or_default();
            if served != computed {
                diffs.push(Difference {
                    field: "effective_operator_rows".into(),
                    served: json!(served),
                    shadow: json!(computed),
                });
            }
        }
        let same_block = ambiguous.contains(resource);
        report.item(resource, diffs, |_| {
            if same_block {
                Excuse::SameBlockOrder
            } else {
                Excuse::None
            }
        });
        report.resources += 1;
    }

    // Account approvals.
    let served_accounts: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('authority_kind', authority_kind,
                    'authority_contract', authority_contract,
                    'authority_contract_instance_id', authority_contract_instance_id::text,
                    'owner', owner, 'subject', subject, 'relation_kind', relation_kind,
                    'approved', approved, 'effective_powers', effective_powers,
                    'grant_source', grant_source, 'revocation_source', revocation_source,
                    'inheritance_path', inheritance_path, 'transfer_behavior', transfer_behavior)
         FROM account_permission_state_current WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    let key = |row: &Value| -> String {
        [
            "authority_kind",
            "authority_contract",
            "owner",
            "subject",
            "relation_kind",
        ]
        .iter()
        .map(|part| row.get(part).and_then(Value::as_str).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("|")
    };
    let mut served: BTreeMap<String, Value> = served_accounts
        .iter()
        .map(|row| (key(row), row.clone()))
        .collect();
    let computed: BTreeMap<String, Value> = approvals
        .iter()
        .map(|approval| {
            let row = json!({
                "authority_kind": approval.authority_kind,
                "authority_contract": approval.authority_contract,
                "authority_contract_instance_id": approval.authority_contract_instance_id,
                "owner": approval.owner, "subject": approval.subject,
                "relation_kind": approval.relation_kind, "approved": approval.approved,
                "effective_powers": approval.effective_powers, "grant_source": approval.grant_source,
                "revocation_source": approval.revocation_source,
                "inheritance_path": approval.inheritance_path,
                "transfer_behavior": approval.transfer_behavior,
            });
            (key(&row), row)
        })
        .collect();
    let account_keys: BTreeSet<String> = served.keys().chain(computed.keys()).cloned().collect();
    for account in account_keys {
        let left = served.remove(&account).unwrap_or(Value::Null);
        let right = computed.get(&account).cloned().unwrap_or(Value::Null);
        let diffs = if same(&left, &right) {
            Vec::new()
        } else {
            vec![Difference {
                field: "account_permission_state_current".into(),
                served: left,
                shadow: right,
            }]
        };
        report.item(&account, diffs, |_| Excuse::None);
        report.accounts += 1;
    }
    Ok(report)
}

fn row_namespace(name: &str) -> String {
    name.split_once(':')
        .map_or("ens", |(namespace, _)| namespace)
        .to_owned()
}
