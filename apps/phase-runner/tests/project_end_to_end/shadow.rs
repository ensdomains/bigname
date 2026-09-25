//! Shadow comparison (TYR-36 step 3): after the owned key families have followed a publication,
//! the family readers of `bigname_storage::families::control` compute the registration and
//! control blocks of every served name, the permission rows, admin powers, restriction block and
//! registry binding of every summarised resource, the registry-operator rows the
//! effective-permission reader adds (every semantic column), and every account approval, and each
//! is compared with what the production readers serve from today's tables at the same
//! publication. `created_at`, the lapsed registration's authority, the child rows and the
//! whole-history evidence columns are not compared.
//!
//! Every differing field of every item is decided on its own, at this publication, and passes
//! only when a named cause is shown to produce it; anything else is a mismatch and fails the run.
//! Each passing field is printed with its served and shadow values and counted by
//! `case:field`, and the tests assert those counts exactly.
//!
//! - `d12_same_block_order` (brief section 4.3, counted as `expected_delta`): the item's retained
//!   lifecycle events hold a block whose canonical order disagrees with the generated-id order,
//!   and reading the same families again in today's generated-id order where today's builders
//!   use it (ENSv2 membership by block and id, the laterals by block, transaction, log and id),
//!   every position kept so the authority admission is unchanged, the node's owner-setting
//!   events, F1's epoch starts and the binding candidates' SurfaceBounds ordered by their
//!   generated ids too, and the association winner of
//!   an affected triple moved only to a grant of the same name, registry and token
//!   (`v2_lifecycle_events.sql:10-23`), gives exactly the served value for the field. A
//!   `control/*` field also needs every owner event, epoch start and SurfaceBound owner the
//!   families hold for the name to equal its rebuild from the event log. For a
//!   resource's permission rows, admin powers and restriction block, the path-expiry drop rule of
//!   permissions.rs:111-133, :391-398 must keep the registration live in today's order and
//!   lapse it in the canonical order, the served value must not be empty, and the whole read in
//!   today's order must equal it. A served empty value against a canonical row is a mismatch.
//!   For the registry binding, the observations are rebuilt from the event log of the published
//!   blocks, independently of the families: for each observation identity (the name, else the
//!   resource) its latest producer event canonically (block, transaction, log, event identity)
//!   and in today's order (block, transaction, log, generated id), each derived as step 2
//!   derives it, its target read from the name's current binding at the publication. A
//!   `registry_binding/*` field passes only when every family observation reaching the resource
//!   is exactly its identity's canonical rebuild (no identity on one side only), the canonical
//!   binding equals the shadow one whole, today's binding equals the served one whole, and the
//!   two select different event identities. The registry-operator rows pass with the binding
//!   only when the rows computed from each rebuilt binding equal the shadow and served rows.
//!
//! Named causes, each a place where the families and today's builders disagree, reported
//! rather than patched (step 3 changes no reducer and no served table):
//! - `served_membership_skips_unnamed_path_expiry`: the interpreter's RegistryPathExpired
//!   release names its resource and no name (crates/adapters/src/schema_v2/protocol/v2_registry/
//!   expiry.rs:58-59). The F2a key state is keyed by the resource and counts it (design:40,
//!   decoder rule 1; crates/project/src/families/lifecycle.rs:71-76), and the chain treats the
//!   registration as over: a name is available once its expiry has passed, the registry reports
//!   no owner and no resolver for it, and unregistering sets the expiry to the current time
//!   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L36 @ ens_v2@a971bd64)
//!   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L206 @ ens_v2@a971bd64)
//!   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
//!   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L313-L316 @ ens_v2@a971bd64)
//!   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L627-L630 @ ens_v2@a971bd64).
//!   Tate's ruling: an expired or released ENSv2 registration stays ENSv2 and is served
//!   unregistered, and never falls back to an ENSv1 lease (only a RESERVED entry defers to
//!   ENSv1). So the reader selects the release, serves it released and closes the control
//!   block. Today's name-scoped membership (build.sql:322, :366-367) never sees the unnamed
//!   release and serves the registration active: a served-side bug, recorded here with the
//!   names and count this harness finds in that shape on the
//!   `SEPOLIA_END_TO_END_SHADOW_SERVED_SIDE_BUG` line, not a rule the reader copies. The
//!   fallback half of the ruling is not observable here: when the interpreter knows the name it
//!   also closes the ENSv2 binding, the served name authority then selects an open ENSv1 lease
//!   (name_authority/build.sql:611-618), and the shadow takes that selection as input and
//!   agrees; the lifecycle fixture `a_real_path_expiry_with_an_ensv1_lease_is_served_from_the_lease`
//!   pins it for step 6. Served code is not changed in this branch. A field passes only when the shadow
//!   selected that unnamed release and the field holds what the ENSv2 path-release presentation
//!   gives (build.sql:88-95, :101-103): status released, latest kind RegistrationReleased,
//!   the release's released_at, the expiry the reader's expiry rule gives (the name's latest
//!   admitted numeric expiry on the key, else the release's own), no registrant or authority,
//!   control unregistered with nothing else; and the served value must be what today's
//!   name-scoped membership gives, the families read in today's order without the unnamed
//!   release.
//! - `served_release_presentation_reads_the_raw_arm`: the selection reads a missing authority
//!   arm as ENSv2 (build.sql:347), so a name with no selected arm can select an ENSv2 release,
//!   but today's presentation compares the raw arm with 'ens_v2' (build.sql:89, :94, :103) and
//!   serves that release with its registrant, authority and expiry and a live control block.
//!   The reader decides both with the one resolved arm and presents the release whole (ruling
//!   R1). A field passes only when no arm is selected, the shadow presents the whole release,
//!   and the served value is what the reader traced for the raw-arm presentation.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use anyhow::{Context, Result};
use bigname_storage::{
    EffectivePermissionScope,
    families::control::{
        compare::{Difference, differences, field, same},
        lifecycle::view::registration_lapsed,
        lifecycle::{
            AuthoritySelection, CONTROL_FIELDS, Clock, NameFacts, NameInput, REGISTRATION_FIELDS,
            ShadowName, evaluate, load_name_facts, load_shadow_names, membership::maxima_of,
        },
        permissions::{
            ResourceInput, ServedApproval, ShadowPermissions, effective_operator_rows, grant_json,
            load_shadow_approvals, load_shadow_permissions, load_shadow_permissions_in,
        },
        position::{EventOrder, Position},
        registry::{
            NameAttribution, Observation, RegistryBinding, load_observations, load_registry_nodes,
            ownerless_registry, registry_bindings, registry_bindings_in, registry_generation,
        },
        rows::LifecycleEvent,
    },
    load_effective_permissions_by_resource_ids, load_name_current_by_logical_name_ids,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const NAME_CHUNK: usize = 500;
/// Difference lines printed per comparison; the counters count all of them.
const PRINTED: usize = 400;

/// The counted fields of every printed report in this process, by target, for the tests that
/// assert them exactly. It is process-wide: a test that reads it clears it first
/// (`take_reports`) and must not share its process with another comparing test, which holds for
/// the fixture-corpus test because every other comparing test in its binary is ignored.
static REPORTS: Mutex<Vec<Counted>> = Mutex::new(Vec::new());

/// One printed report's target, same-block delta fields and named-cause fields.
pub type Counted = (i64, BTreeMap<String, usize>, BTreeMap<String, usize>);

/// The counted fields of the reports printed so far, emptying the list.
pub fn take_reports() -> Vec<Counted> {
    REPORTS
        .lock()
        .map(|mut reports| std::mem::take(&mut *reports))
        .unwrap_or_default()
}

/// What one comparison saw.
#[derive(Debug, Default)]
pub struct Report {
    pub names: usize,
    pub resources: usize,
    pub accounts: usize,
    pub equal: usize,
    /// Items whose every difference is a same-block ordering delta or a named cause, with at
    /// least one same-block delta.
    pub expected_delta: usize,
    /// Same-block delta fields, by `d12_same_block_order:field`.
    pub expected_delta_fields: BTreeMap<String, usize>,
    /// Named-cause fields, by `case:field`.
    pub known_discrepancy: BTreeMap<String, usize>,
    pub mismatched: usize,
    /// Names the families serve released after the interpreter's path expiry and today's reader
    /// serves active: the served-side bug of `served_membership_skips_unnamed_path_expiry`.
    pub served_side_bug_names: Vec<String>,
    pub lines: Vec<String>,
}

/// Why one differing field passes, if it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Excuse {
    None,
    SameBlockOrder,
    Known(&'static str),
}

impl Report {
    /// Count one compared item from its differing fields, each with the cause shown for it.
    fn item(&mut self, key: &str, diffs: Vec<(Difference, Excuse)>) {
        if diffs.is_empty() {
            self.equal += 1;
            return;
        }
        if diffs.iter().any(|(_, why)| *why == Excuse::None) {
            self.mismatched += 1;
        } else if diffs.iter().any(|(_, why)| *why == Excuse::SameBlockOrder) {
            self.expected_delta += 1;
        }
        for (diff, why) in diffs {
            let (kind, case) = match why {
                Excuse::SameBlockOrder => ("EXPECTED_DELTA", "d12_same_block_order"),
                Excuse::Known(name) => ("KNOWN_DISCREPANCY", name),
                Excuse::None => ("MISMATCH", "none"),
            };
            let counted = format!("{case}:{}", diff.field);
            match why {
                Excuse::SameBlockOrder => {
                    *self.expected_delta_fields.entry(counted).or_default() += 1;
                }
                Excuse::Known(_) => *self.known_discrepancy.entry(counted).or_default() += 1,
                Excuse::None => {}
            }
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
        eprintln!(
            "SEPOLIA_END_TO_END_SHADOW_SERVED_SIDE_BUG case=served_membership_skips_unnamed_path_expiry \
             target={target} count={} names={}",
            self.served_side_bug_names.len(),
            self.served_side_bug_names.join(",")
        );
        let list = |counts: &BTreeMap<String, usize>| {
            counts
                .iter()
                .map(|(reason, count)| format!("{reason}:{count}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        eprintln!(
            "SEPOLIA_END_TO_END_SHADOW target={target} names={} resources={} accounts={} \
             equal={} expected_delta={} mismatched={} expected_delta_fields={} \
             known_discrepancy={}",
            self.names,
            self.resources,
            self.accounts,
            self.equal,
            self.expected_delta,
            self.mismatched,
            list(&self.expected_delta_fields),
            list(&self.known_discrepancy)
        );
        if let Ok(mut reports) = REPORTS.lock() {
            reports.push((
                target,
                self.expected_delta_fields.clone(),
                self.known_discrepancy.clone(),
            ));
        }
    }
}

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
            let excuses = name_excuses(pool, chain, &clock, input, shadow, &diffs).await?;
            if excuses.contains(&Excuse::Known(
                "served_membership_skips_unnamed_path_expiry",
            )) {
                report
                    .served_side_bug_names
                    .push(input.logical_name_id.clone());
            }
            report.item(
                &input.logical_name_id,
                diffs.into_iter().zip(excuses).collect(),
            );
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
    let inputs_by_resource: BTreeMap<&str, &ResourceInput> = inputs
        .iter()
        .map(|input| (input.resource_id.as_str(), input))
        .collect();
    let observations = load_observations(pool, chain).await?;
    let bindings = registry_bindings(&observations, &attributions);
    let approvals = load_shadow_approvals(pool, chain).await?;
    let ids: Vec<Uuid> = resource_ids
        .iter()
        .filter_map(|resource| resource.parse().ok())
        .collect();
    // Every semantic column of a registry-operator row, keyed by subject and scope.
    let mut served_operators: BTreeMap<String, BTreeMap<(String, String), Value>> = BTreeMap::new();
    for chunk in ids.chunks(NAME_CHUNK) {
        for row in load_effective_permissions_by_resource_ids(pool, chunk, None).await? {
            if let EffectivePermissionScope::Account {
                chain_id,
                authority_kind,
                authority_contract,
                owner,
            } = &row.scope
            {
                let scope = row.scope.storage_key();
                let value = json!({
                    "subject": row.subject, "scope": scope, "scope_kind": "account",
                    "scope_detail": {
                        "chain_id": chain_id, "authority_kind": authority_kind,
                        "authority_contract": authority_contract, "owner": owner,
                    },
                    "record_resource_selector": row.record_resource_selector,
                    "grant_relation": row.grant_relation.map(|_| "operator"),
                    "effective_powers": row.effective_powers, "grant_source": row.grant_source,
                    "revocation_source": row.revocation_source,
                    "inheritance_path": row.inheritance_path,
                    "transfer_behavior": row.transfer_behavior,
                });
                served_operators
                    .entry(row.resource_id.to_string())
                    .or_default()
                    .insert((row.subject.clone(), scope), value);
            }
        }
    }
    // The admin powers the served summary derives for each resource (resource_summary.rs
    // :272-297, `v2_admin_powers`): the distinct admin powers of its registry- or root-scoped
    // served rows.
    let served_admins: BTreeMap<String, Vec<String>> = sqlx::query_as(
        r"SELECT served.resource_id::text, array_agg(DISTINCT power.value ORDER BY power.value)
         FROM permissions_current served
         CROSS JOIN LATERAL jsonb_array_elements_text(served.effective_powers) power
         WHERE served.scope_kind IN ('registry', 'root')
           AND (power.value LIKE 'admin\_%' OR power.value = 'can_transfer_admin')
         GROUP BY 1",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();
    let mut binding_orders: Option<BindingOrders> = None;
    for resource in &resource_ids {
        let mut binding_values: Option<(Value, Value)> = None;
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
        let served_admin = json!(served_admins.get(resource).cloned().unwrap_or_default());
        let computed_admin = json!(shadow.admin_powers);
        if !same(&served_admin, &computed_admin) {
            diffs.push(Difference {
                field: "admin_powers".into(),
                served: served_admin,
                shadow: computed_admin,
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
            let shadow_binding = binding_json(&binding);
            binding_values = Some((served_binding.clone(), shadow_binding.clone()));
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
            let computed = operator_rows_json(chain, resource, &binding, &approvals);
            let served = served_operators.get(resource).cloned().unwrap_or_default();
            let served = Value::Array(served.into_values().collect());
            if !same(&served, &computed) {
                diffs.push(Difference {
                    field: "effective_operator_rows".into(),
                    served,
                    shadow: computed,
                });
            }
        }
        let mut excuses = resource_excuses(
            pool,
            chain,
            &clock,
            inputs_by_resource[resource.as_str()],
            &diffs,
        )
        .await?;
        // The registry binding rebuilt from the event log in both orders: the observations of
        // each identity tie-broken canonically and by generated id (permission_resources.rs
        // :41-57). Every `registry_binding/*` field passes as a same-block delta only when the
        // families' observations that reach the resource are exactly what the log gives, the
        // canonical rebuild equals the shadow binding, the today's-order rebuild equals the
        // served one, and the two select different events.
        // The registry-operator rows follow the binding's owner and contract, so they pass with
        // it only when the rows computed from each rebuilt binding equal the served and shadow
        // rows.
        let follows_binding = |field: &str| {
            field.starts_with("registry_binding/") || field == "effective_operator_rows"
        };
        if let Some((served_binding, shadow_binding)) = &binding_values
            && diffs
                .iter()
                .zip(&excuses)
                .any(|(diff, excuse)| follows_binding(&diff.field) && *excuse == Excuse::None)
        {
            if binding_orders.is_none() {
                binding_orders = Some(
                    rebuilt_bindings(pool, chain, &clock, &observations, &attributions).await?,
                );
            }
            let orders = binding_orders.as_ref().expect("rebuilt above");
            if orders.same_block_delta(resource, served_binding, shadow_binding) {
                let rows = |bindings: &BTreeMap<String, RegistryBinding>| {
                    let binding = bindings.get(resource).cloned().unwrap_or_default();
                    operator_rows_json(chain, resource, &binding, &approvals)
                };
                let (canonical_rows, legacy_rows) = (rows(&orders.canonical), rows(&orders.legacy));
                for (diff, excuse) in diffs.iter().zip(excuses.iter_mut()) {
                    if *excuse != Excuse::None {
                        continue;
                    }
                    let passes = if diff.field == "effective_operator_rows" {
                        same(&canonical_rows, &diff.shadow) && same(&legacy_rows, &diff.served)
                    } else {
                        diff.field.starts_with("registry_binding/")
                    };
                    if passes {
                        *excuse = Excuse::SameBlockOrder;
                    }
                }
            }
        }
        report.item(resource, diffs.into_iter().zip(excuses).collect());
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
        report.item(
            &account,
            diffs.into_iter().map(|diff| (diff, Excuse::None)).collect(),
        );
        report.accounts += 1;
    }
    Ok(report)
}

fn row_namespace(name: &str) -> String {
    name.split_once(':')
        .map_or("ens", |(namespace, _)| namespace)
        .to_owned()
}

/// The value of a `registration/...` or `control/...` field of a shadow read; `None` for any
/// other path, which the shadow read does not compute.
fn shadow_field(shadow: &ShadowName, path: &str) -> Option<Value> {
    let (block, rest) = path.split_once('/')?;
    let block = match block {
        "registration" => &shadow.registration,
        "control" => &shadow.control,
        _ => return None,
    };
    Some(field(&Value::Object(block.clone()), rest).clone())
}

/// Whether the shadow presents its selected registration as an ENSv2 release, whole: status
/// released, no registrant, authority kind or key, and a control block that is
/// `{status: unregistered}` with nothing else (build.sql:88-95, :101-103).
fn release_presented(shadow: &ShadowName) -> bool {
    let registration = |name: &str| {
        shadow
            .registration
            .get(name)
            .cloned()
            .unwrap_or(Value::Null)
    };
    registration("status") == json!("released")
        && registration("registrant").is_null()
        && registration("authority_kind").is_null()
        && registration("authority_key").is_null()
        && shadow.control.get("status") == Some(&json!("unregistered"))
        && shadow
            .control
            .iter()
            .all(|(name, value)| name == "status" || value.is_null())
}

/// Whether the shadow value of `diff` is what the ENSv2 path-release presentation gives, for a
/// name whose selected registration is the interpreter's unnamed path-expiry release. The whole
/// presentation must hold, not only the differing field. The expiry is the reader's expiry
/// rule: the expiry lateral's value (the latest admitted numeric expiry of the name on the
/// selected key) when it has one, else the release's own expiry; a later ExpiryChanged or
/// renewal of the name moves it past the release's own.
fn serves_the_unnamed_release(shadow: &ShadowName, diff: &Difference) -> bool {
    let trace = &shadow.trace;
    if trace.get("selected_unnamed_path_expiry") != Some(&json!(true)) || !release_presented(shadow)
    {
        return false;
    }
    let traced = |name: &str| trace.get(name).cloned().unwrap_or(Value::Null);
    let expiry = match traced("expiry_candidate") {
        Value::Null => traced("selected_expiry"),
        lateral => lateral,
    };
    let value = &diff.shadow;
    match diff.field.as_str() {
        "registration/status" => value == &json!("released"),
        "registration/latest_event_kind" => value == &json!("RegistrationReleased"),
        "registration/released_at" => same(value, &traced("selected_released_at")),
        "registration/expiry" => !value.is_null() && same(value, &expiry),
        "registration/authority_kind"
        | "registration/authority_key"
        | "registration/registrant"
        | "control/expiry"
        | "control/registrant"
        | "control/registry_owner"
        | "control/latest_event_kind"
        | "control/unsupported_reason" => value.is_null(),
        "control/status" => value == &json!("unregistered"),
        _ => false,
    }
}

/// Whether `diff` is the difference between the whole ENSv2 release the shadow presents for a
/// name with no selected arm and what today's presentation serves for it, which compares the raw
/// arm with 'ens_v2' (build.sql:89, :94, :103) and so neither clears the release nor closes the
/// control block. The served value must equal what the reader traced for that presentation.
fn serves_the_raw_arm_release(input: &NameInput, shadow: &ShadowName, diff: &Difference) -> bool {
    if input.selection.authority_arm.is_some() || !release_presented(shadow) {
        return false;
    }
    let Some(raw) = shadow.trace.get("raw_arm_presentation") else {
        return false;
    };
    let Some((block, rest)) = diff.field.split_once('/') else {
        return false;
    };
    matches!(block, "registration" | "control")
        && shadow_field(shadow, &diff.field).is_some_and(|value| same(&diff.shadow, &value))
        && same(&diff.served, field(&raw[block], rest))
}

/// The name's facts as today's name-scoped membership reads them: without the interpreter's
/// unnamed path-expiry releases, every key folded from its retained events in today's
/// generated-id order.
async fn without_unnamed_release(
    pool: &PgPool,
    chain: &str,
    facts: &NameFacts,
) -> Result<NameFacts> {
    let mut membership = facts.clone();
    membership.events.retain(|event| {
        !(event.original_logical_name_id.is_none()
            && event.event_kind == "RegistrationReleased"
            && event.is_path_expiry())
    });
    let identities: Vec<String> = membership
        .events
        .iter()
        .map(|event| event.position.event_identity.clone())
        .collect();
    membership.order = EventOrder::Generated(generated_ids(pool, chain, &identities).await?);
    Ok(membership)
}

/// The cause shown for each differing field of one name, in `diffs` order.
async fn name_excuses(
    pool: &PgPool,
    chain: &str,
    clock: &Clock,
    input: &NameInput,
    shadow: &ShadowName,
    diffs: &[Difference],
) -> Result<Vec<Excuse>> {
    if diffs.is_empty() {
        return Ok(Vec::new());
    }
    let Some(facts) = load_name_facts(pool, chain, std::slice::from_ref(input))
        .await?
        .pop()
    else {
        return Ok(vec![Excuse::None; diffs.len()]);
    };
    // The unnamed-release cause passes a field only when today's name-scoped membership gives
    // the served value: the families read in today's order without the unnamed path-expiry
    // release (build.sql:322, :366-367 build membership by name).
    let unnamed: Vec<bool> = diffs
        .iter()
        .map(|diff| serves_the_unnamed_release(shadow, diff))
        .collect();
    let without_release = if unnamed.contains(&true) {
        Some(evaluate(
            &without_unnamed_release(pool, chain, &facts).await?,
            clock,
        ))
    } else {
        None
    };
    let mut out = Vec::with_capacity(diffs.len());
    for (index, diff) in diffs.iter().enumerate() {
        let membership_gives_served = without_release
            .as_ref()
            .and_then(|membership| shadow_field(membership, &diff.field))
            .is_some_and(|value| same(&value, &diff.served));
        let excuse = if unnamed[index] && membership_gives_served {
            Excuse::Known("served_membership_skips_unnamed_path_expiry")
        } else if serves_the_raw_arm_release(input, shadow, diff) {
            Excuse::Known("served_release_presentation_reads_the_raw_arm")
        } else {
            Excuse::None
        };
        out.push(excuse);
    }
    if out.iter().all(|excuse| *excuse != Excuse::None) {
        return Ok(out);
    }

    let open = |out: &[Excuse], index: usize| out[index] == Excuse::None;
    // The same families read in today's generated-id order at the selectors that use it.
    if out.contains(&Excuse::None) {
        let identities: Vec<String> = facts
            .events
            .iter()
            .map(|event| event.position.event_identity.clone())
            .chain(
                control_positions(&facts)
                    .into_iter()
                    .map(|position| position.event_identity),
            )
            .collect();
        let ids = generated_ids(pool, chain, &identities).await?;
        let keys = association_keys(pool, chain, &identities).await?;
        if let Some(legacy) = legacy_facts(&facts, &ids, &keys) {
            let counterfactual = evaluate(&legacy, clock);
            let mut control_holds = None;
            for (index, diff) in diffs.iter().enumerate() {
                // Only the fields the counterfactual computes: an authority-selection field has
                // no today's-order value here and stays open.
                if !open(&out, index)
                    || !shadow_field(&counterfactual, &diff.field)
                        .is_some_and(|value| same(&value, &diff.served))
                {
                    continue;
                }
                // A control field passes only when the families' control facts are what the
                // event log gives, so a wrong value on the canonically selected event fails.
                if diff.field.starts_with("control/") {
                    if control_holds.is_none() {
                        control_holds = Some(
                            control_facts_hold(pool, chain, clock.block_number, &facts).await?,
                        );
                    }
                    if control_holds != Some(true) {
                        continue;
                    }
                }
                out[index] = Excuse::SameBlockOrder;
            }
        }
    }
    Ok(out)
}

/// The owner an authority event reports to the served control block, as step 2 stores it for
/// an epoch start and a binding candidate (crates/project/src/families/identity.rs
/// `control_owner`, name_current/build.sql:650-663).
fn reported_control_owner(after: &Value) -> Option<String> {
    let unmasked = match after.get("owner_word_unmasked") {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::String(text)) => text == "true",
        _ => false,
    };
    if unmasked {
        return None;
    }
    let lower = |name: &str| {
        after
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_lowercase)
    };
    lower("registry_owner").or_else(|| lower("owner"))
}

/// One event's log row, by identity: kind, name, resource, source family, block, transaction,
/// log and after-state.
type LogRow = (
    String,
    Option<String>,
    Option<String>,
    String,
    i64,
    Option<i64>,
    Option<i64>,
    Value,
);

/// Whether every control fact of the name the control block reads, beyond its lifecycle
/// events, is what the event log up to the publication gives: each owner-setting event of its
/// node, each epoch start and each binding candidate's SurfaceBound owner, found by identity at
/// its position among canonical events of the published blocks and rebuilt from it as step 2
/// derives it.
async fn control_facts_hold(
    pool: &PgPool,
    chain: &str,
    target: i64,
    facts: &NameFacts,
) -> Result<bool> {
    let identities: Vec<String> = control_positions(facts)
        .into_iter()
        .map(|position| position.event_identity)
        .collect();
    let rows: BTreeMap<String, LogRow> = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            i64,
            Option<i64>,
            Option<i64>,
            Value,
        ),
    >(
        "SELECT event_identity, event_kind, logical_name_id, resource_id::text, source_family,
                block_number, transaction_index, log_index, after_state
         FROM normalized_events WHERE chain_id = $1 AND event_identity = ANY($2)
           AND block_number <= $3
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .bind(&identities)
    .bind(target)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        (
            row.0,
            (row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8),
        )
    })
    .collect();
    let at = |position: &Position| {
        rows.get(&position.event_identity).filter(|row| {
            (row.4, row.5, row.6)
                == (
                    position.block_number,
                    position.transaction_index,
                    position.log_index,
                )
        })
    };
    let text =
        |after: &Value, name: &str| after.get(name).and_then(Value::as_str).map(str::to_owned);
    let lower = |after: &Value, name: &str| text(after, name).map(|value| value.to_lowercase());
    let owners_hold = facts
        .registry_node
        .iter()
        .flat_map(|node| &node.owner_events)
        .all(|event| {
            at(&event.position).is_some_and(|row| {
                let after = &row.7;
                row.0 == event.event_kind
                    && row.1 == event.logical_name_id
                    && row.2 == event.resource_id
                    && row.3 == event.source_family
                    && text(after, "authority_kind") == event.authority_kind
                    && lower(after, "owner") == event.owner
                    && lower(after, "registry_owner") == event.registry_owner
                    && after.get("owner_word_unmasked").and_then(Value::as_bool)
                        == event.owner_word_unmasked
                    && lower(after, "owner_getter") == event.owner_getter
            })
        });
    let starts_hold = facts
        .authority_starts
        .as_object()
        .into_iter()
        .flatten()
        .all(|(_, start)| {
            Position::from_json(start).is_some_and(|position| {
                at(&position).is_some_and(|row| {
                    let member = |name: &str| start.get(name).and_then(Value::as_str);
                    let after = &row.7;
                    member("owner").map(str::to_owned) == reported_control_owner(after)
                        && member("resource_id") == row.2.as_deref()
                        && member("authority_kind").map(str::to_owned)
                            == text(after, "authority_kind")
                        && member("authority_key").map(str::to_owned)
                            == text(after, "authority_key")
                })
            })
        });
    let bounds_hold = facts.candidates.iter().all(|candidate| {
        candidate
            .surface_bound_position
            .as_ref()
            .is_none_or(|position| {
                at(position)
                    .is_some_and(|row| candidate.bound_owner == reported_control_owner(&row.7))
            })
    });
    Ok(owners_hold && starts_hold && bounds_hold)
}

/// A resource's registry-operator rows as the effective-permission reader adds them, from its
/// registry binding and the account approvals, in (subject, scope) order.
fn operator_rows_json(
    chain: &str,
    resource: &str,
    binding: &RegistryBinding,
    approvals: &[ServedApproval],
) -> Value {
    let rows: BTreeMap<(String, String), Value> =
        effective_operator_rows(chain, resource, binding, approvals)
            .into_iter()
            .map(|row| {
                let value = json!({
                    "subject": row.subject, "scope": row.scope,
                    "scope_kind": "account", "scope_detail": row.scope_detail,
                    "record_resource_selector": null,
                    "grant_relation": "operator",
                    "effective_powers": row.effective_powers,
                    "grant_source": row.grant_source, "revocation_source": null,
                    "inheritance_path": row.inheritance_path,
                    "transfer_behavior": row.transfer_behavior,
                });
                ((row.subject, row.scope), value)
            })
            .collect();
    Value::Array(rows.into_values().collect())
}

/// A resource's registry binding as the summary serves it (permission_resources.rs:71-79).
pub fn binding_json(binding: &RegistryBinding) -> Value {
    let applicable = binding.registry_owner.is_some();
    json!({
        "registry_owner": binding.registry_owner, "registry_contract": binding.registry_contract,
        "event_ids": applicable.then(|| json!([binding.normalized_event_id])),
        "block_number": binding.position.as_ref().map(|position| position.block_number),
        "transaction_index": binding.position.as_ref().and_then(|position| position.transaction_index),
        "log_index": binding.position.as_ref().and_then(|position| position.log_index),
        "clear_event_id": binding.clear_event_identity.as_ref().and(binding.normalized_event_id),
    })
}

/// Whether a value is a lower-case 20-byte address, the served applicability test.
fn is_address(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value.len() == 42
            && value.starts_with("0x")
            && value[2..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

/// One producer event: observation identity, event identity, name,
/// resource, block, transaction, log, generated id, kind, derived contract and extras.
type RivalRow = (
    String,
    String,
    Option<String>,
    String,
    i64,
    Option<i64>,
    Option<i64>,
    i64,
    Option<String>,
    Option<String>,
    Value,
);

/// Every resource's registry binding rebuilt from the event log in the canonical order and in
/// today's order, with the resources an observation the log does not reproduce reaches.
pub struct BindingOrders {
    pub canonical: BTreeMap<String, RegistryBinding>,
    pub legacy: BTreeMap<String, RegistryBinding>,
    pub unverified: BTreeSet<String>,
}

impl BindingOrders {
    /// Whether a resource's binding difference is a same-block ordering delta: every family
    /// observation reaching it is what the log gives, the canonical rebuild equals the shadow
    /// binding, the today's-order rebuild equals the served one, and the two select different
    /// events.
    pub fn same_block_delta(&self, resource: &str, served: &Value, shadow: &Value) -> bool {
        if self.unverified.contains(resource) {
            return false;
        }
        let (Some(canonical), Some(legacy)) =
            (self.canonical.get(resource), self.legacy.get(resource))
        else {
            return false;
        };
        let selected = |binding: &RegistryBinding| {
            binding
                .position
                .as_ref()
                .map(|position| position.event_identity.clone())
                .or_else(|| binding.clear_event_identity.clone())
        };
        let (canonical_json, legacy_json) = (binding_json(canonical), binding_json(legacy));
        same(&canonical_json, shadow)
            && same(&legacy_json, served)
            && !same(&legacy_json, &canonical_json)
            && selected(canonical).is_some()
            && selected(legacy).is_some()
            && selected(canonical) != selected(legacy)
    }
}

/// The observation step 2 derives from one producer event (crates/project/src/families/
/// registry.rs `observations`). A named AuthorityTransferred or SubregistryChanged reaches the
/// name's current ENSv1 or Basenames resource at the publication, as the served summary reads
/// it (permission_resources.rs:36-40), else the event's own resource.
fn observation_of(row: &RivalRow, targets: &BTreeMap<String, String>) -> Observation {
    let through_name = row.2.is_some()
        && matches!(
            row.8.as_deref(),
            Some("AuthorityTransferred" | "SubregistryChanged")
        );
    let owner = (row.8.as_deref() != Some("SurfaceUnbound"))
        .then(|| row.10["owner_getter"].as_str().map(str::to_owned))
        .flatten();
    let applicable = is_address(owner.as_deref())
        && owner.as_deref() != Some("0x0000000000000000000000000000000000000000")
        && is_address(row.9.as_deref());
    Observation {
        resource_id: row.3.clone(),
        logical_name_id: row.2.clone(),
        attributed_via: if through_name { "name" } else { "own" }.to_owned(),
        target_resource_id: row
            .2
            .as_ref()
            .filter(|_| through_name)
            .and_then(|name| targets.get(name))
            .unwrap_or(&row.3)
            .clone(),
        position: Position {
            block_number: row.4,
            transaction_index: row.5,
            log_index: row.6,
            event_identity: row.1.clone(),
        },
        event_kind: row.8.clone().unwrap_or_default(),
        registry_owner: owner,
        registry_contract: row.9.clone(),
        provenance: json!({"raw_fact_ref": row.10["raw_fact_ref"]}),
        applicable,
        clear_event_identity: (!applicable).then(|| row.1.clone()),
        normalized_event_id: Some(row.7),
    }
}

/// Whether a family observation is exactly the one rebuilt from the event log, its target
/// included.
fn same_observation(family: &Observation, rebuilt: &Observation) -> bool {
    family.target_resource_id == rebuilt.target_resource_id
        && family.position == rebuilt.position
        && family.resource_id == rebuilt.resource_id
        && family.logical_name_id == rebuilt.logical_name_id
        && family.attributed_via == rebuilt.attributed_via
        && family.event_kind == rebuilt.event_kind
        && family.registry_owner == rebuilt.registry_owner
        && family.registry_contract == rebuilt.registry_contract
        && family.applicable == rebuilt.applicable
        && family.clear_event_identity == rebuilt.clear_event_identity
        && family.normalized_event_id == rebuilt.normalized_event_id
}

/// Every resource's registry binding rebuilt from the event log of the published blocks, in
/// both orders, independently of the family's choices. For each observation identity (the
/// name, else the resource) the canonical rebuild takes its latest producer event in the
/// canonical order (block, transaction, log, event identity), as F2c keeps it, and today's
/// rebuild the latest in (block, transaction, log, generated id), as the served summary does
/// (permission_resources.rs:10-11); each is derived as step 2 derives it, with its target
/// read from the name's current binding at the publication. A family observation that is not
/// exactly its identity's canonical rebuild, or an identity one side has and the other lacks,
/// marks the resources it reaches unverified. The resources are then chosen in the canonical
/// order and in (block, transaction, log, generated id) order.
async fn rebuilt_bindings(
    pool: &PgPool,
    chain: &str,
    clock: &Clock,
    observations: &[Observation],
    names: &BTreeMap<String, NameAttribution>,
) -> Result<BindingOrders> {
    let rows: Vec<RivalRow> = sqlx::query_as(
        r#"SELECT COALESCE(event.logical_name_id, event.resource_id::text), event.event_identity,
               event.logical_name_id, event.resource_id::text, event.block_number,
               event.transaction_index, event.log_index, event.normalized_event_id,
               event.event_kind,
               lower(CASE WHEN event.source_family IN (
                                  'ens_v1_registrar_l1', 'basenames_base_registrar')
                              OR (event.event_kind = 'SurfaceBound'
                                  AND event.after_state @> '{"state_derived":true,"authority_kind":"registry_only"}')
                          THEN event.after_state ->> 'registry_contract'
                          ELSE COALESCE(event.raw_fact_ref ->> 'emitting_address',
                                        event.after_state ->> 'registry_contract') END),
               jsonb_build_object('owner_getter', lower(event.after_state ->> 'owner_getter'),
                                  'raw_fact_ref', event.raw_fact_ref)
         FROM normalized_events event
         WHERE event.chain_id = $1 AND event.block_number <= $2
           AND event.event_kind IN ('AuthorityTransferred', 'SubregistryChanged', 'SurfaceBound',
                                    'SurfaceUnbound')
           AND (event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
                OR (event.event_kind IN ('SurfaceBound', 'SurfaceUnbound')
                    AND event.source_family IN ('ens_v1_registrar_l1',
                                                'basenames_base_registrar')))
           AND event.resource_id IS NOT NULL
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')"#,
    )
    .bind(chain)
    .bind(clock.block_number)
    .fetch_all(pool)
    .await?;
    let named: Vec<String> = rows
        .iter()
        .filter_map(|row| row.2.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let targets: BTreeMap<String, String> = sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT ON (binding.logical_name_id) binding.logical_name_id,
                binding.resource_id::text
         FROM surface_bindings binding
         WHERE binding.chain_id = $1 AND binding.logical_name_id = ANY($2)
           AND binding.authority_arm IN ('ens_v1', 'basenames')
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.block_number <= $3
           AND binding.active_from <= to_timestamp($4)
           AND (binding.active_to IS NULL OR binding.active_to > to_timestamp($4))
         ORDER BY binding.logical_name_id, binding.active_from DESC,
                  binding.surface_binding_id DESC",
    )
    .bind(chain)
    .bind(&named)
    .bind(clock.block_number)
    .bind(clock.timestamp_seconds as f64)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();
    let position = |row: &RivalRow| Position {
        block_number: row.4,
        transaction_index: row.5,
        log_index: row.6,
        event_identity: row.1.clone(),
    };
    let mut canonical_rows: BTreeMap<&str, &RivalRow> = BTreeMap::new();
    let mut legacy_rows: BTreeMap<&str, &RivalRow> = BTreeMap::new();
    for row in &rows {
        let canonical = canonical_rows.entry(row.0.as_str()).or_insert(row);
        if position(row) > position(canonical) {
            *canonical = row;
        }
        let legacy = legacy_rows.entry(row.0.as_str()).or_insert(row);
        if (row.4, row.5, row.6, row.7) > (legacy.4, legacy.5, legacy.6, legacy.7) {
            *legacy = row;
        }
    }
    let reached = |observation: &Observation| {
        registry_bindings_in(
            std::slice::from_ref(observation),
            names,
            &EventOrder::Canonical,
        )
        .into_keys()
        .next()
    };
    let canonical: Vec<Observation> = canonical_rows
        .values()
        .map(|row| observation_of(row, &targets))
        .collect();
    let legacy: Vec<Observation> = legacy_rows
        .values()
        .map(|row| observation_of(row, &targets))
        .collect();
    let key = |observation: &Observation| {
        observation
            .logical_name_id
            .clone()
            .unwrap_or_else(|| observation.resource_id.clone())
    };
    let family: BTreeMap<String, &Observation> = observations
        .iter()
        .map(|observation| (key(observation), observation))
        .collect();
    let rebuilt: BTreeMap<String, &Observation> = canonical
        .iter()
        .map(|observation| (key(observation), observation))
        .collect();
    let mut unverified = BTreeSet::new();
    for identity in family.keys().chain(rebuilt.keys()) {
        match (family.get(identity), rebuilt.get(identity)) {
            (Some(kept), Some(log)) if same_observation(kept, log) => {}
            (kept, log) => {
                unverified.extend(kept.and_then(|kept| reached(kept)));
                unverified.extend(log.and_then(|log| reached(log)));
            }
        }
    }
    let ids: BTreeMap<String, i64> = legacy
        .iter()
        .filter_map(|observation| {
            Some((
                observation.position.event_identity.clone(),
                observation.normalized_event_id?,
            ))
        })
        .collect();
    Ok(BindingOrders {
        canonical: registry_bindings_in(&canonical, names, &EventOrder::Canonical),
        legacy: registry_bindings_in(&legacy, names, &EventOrder::Generated(ids)),
        unverified,
    })
}

/// The positions of the control-block events the lifecycle family does not retain: the node's
/// owner-setting registry events (`project_registry_owner_event`), F1's epoch starts and the
/// binding candidates' SurfaceBounds, which feed the control owner as registry-only bindings.
pub fn control_positions(facts: &NameFacts) -> Vec<Position> {
    let owners = facts
        .registry_node
        .iter()
        .flat_map(|node| node.owner_events.iter().map(|event| event.position.clone()));
    let starts = facts
        .authority_starts
        .as_object()
        .into_iter()
        .flat_map(|starts| starts.values().filter_map(Position::from_json));
    let bounds = facts
        .candidates
        .iter()
        .filter_map(|candidate| candidate.surface_bound_position.clone());
    owners.chain(starts).chain(bounds).collect()
}

/// The generated ids of events, by identity.
pub async fn generated_ids(
    pool: &PgPool,
    chain: &str,
    identities: &[String],
) -> Result<BTreeMap<String, i64>> {
    Ok(sqlx::query_as::<_, (String, i64)>(
        "SELECT event_identity, normalized_event_id FROM normalized_events
         WHERE chain_id = $1 AND event_identity = ANY($2)",
    )
    .bind(chain)
    .bind(identities)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

/// The registry identifier and token id of events, by identity, as today's association keys
/// them (v2_lifecycle_events.sql:14-19).
pub async fn association_keys(
    pool: &PgPool,
    chain: &str,
    identities: &[String],
) -> Result<BTreeMap<String, (String, String)>> {
    Ok(
        sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
            "SELECT event_identity,
                COALESCE(after_state ->> 'registry_contract_instance_id',
                         raw_fact_ref ->> 'emitting_address', after_state ->> 'registry'),
                after_state ->> 'token_id'
         FROM normalized_events WHERE chain_id = $1 AND event_identity = ANY($2)",
        )
        .bind(chain)
        .bind(identities)
        .fetch_all(pool)
        .await?
        .into_iter()
        .filter_map(|(identity, registry, token)| Some((identity, (registry?, token?))))
        .collect(),
    )
}

/// The name's facts read in today's generated-id order where today's builders use it
/// (`EventOrder::Generated`: ENSv2 membership in block and generated id, build.sql:322-340; the
/// laterals in block, transaction, log and generated id, build.sql:307-308), with every event
/// kept at its own position, so the authority admission, whose bounds compare positions,
/// admits exactly what the canonical read admits. A triple whose association winner sits in a
/// block whose two orders disagree moves to the latest grant or reservation, in today's order,
/// of that block with the same name, registry identifier and token id: today's association
/// (v2_lifecycle_events.sql:10-23). None when no block of the name's events reads differently
/// in the two orders or an event has no generated id.
pub fn legacy_facts(
    facts: &NameFacts,
    ids: &BTreeMap<String, i64>,
    triple_keys: &BTreeMap<String, (String, String)>,
) -> Option<NameFacts> {
    let mut blocks: BTreeMap<i64, Vec<Position>> = BTreeMap::new();
    for event in &facts.events {
        ids.get(&event.position.event_identity)?;
        blocks
            .entry(event.position.block_number)
            .or_default()
            .push(event.position.clone());
    }
    // The owner events, epoch starts and SurfaceBounds the control block reads are ordered
    // too; one the event log does not name leaves the order unknown, so there is no reread.
    for position in control_positions(facts) {
        ids.get(&position.event_identity)?;
        blocks
            .entry(position.block_number)
            .or_default()
            .push(position);
    }
    let disagreeing: BTreeSet<i64> = blocks
        .iter()
        .filter(|(_, positions)| {
            let mut canonical: Vec<&Position> = positions.iter().collect();
            canonical.sort();
            canonical.dedup_by(|left, right| left.event_identity == right.event_identity);
            // Membership orders by generated id alone, the laterals by transaction, log and id.
            let mut membership = canonical.clone();
            membership.sort_by_key(|position| ids[&position.event_identity]);
            let mut lateral = canonical.clone();
            lateral.sort_by_key(|position| {
                (
                    position.transaction_index,
                    position.log_index,
                    ids[&position.event_identity],
                )
            });
            [membership, lateral].iter().any(|today| {
                canonical
                    .iter()
                    .zip(today)
                    .any(|(left, right)| left.event_identity != right.event_identity)
            })
        })
        .map(|(block, _)| *block)
        .collect();
    if disagreeing.is_empty() {
        return None;
    }
    let mut out = facts.clone();
    out.order = EventOrder::Generated(ids.clone());
    for triple in &mut out.triples {
        let Some(winner) = triple.target_position.clone() else {
            continue;
        };
        if !disagreeing.contains(&winner.block_number) {
            continue;
        }
        let key = (triple.key[1].clone(), triple.key[2].clone());
        let rival = facts
            .events
            .iter()
            .filter(|event| {
                event.state_kind == "resource"
                    && event.is_v2_family()
                    && event.resource_id.is_some()
                    && event.original_logical_name_id.as_deref() == Some(triple.key[0].as_str())
                    && matches!(
                        event.event_kind.as_str(),
                        "RegistrationGranted" | "RegistrationReserved"
                    )
                    && event.position.block_number == winner.block_number
                    && triple_keys.get(&event.position.event_identity) == Some(&key)
            })
            .max_by_key(|event| ids[&event.position.event_identity]);
        if let Some(rival) = rival {
            triple.target = rival.resource_id.clone();
            triple.target_position = Some(rival.position.clone());
        }
    }
    Some(out)
}

/// The cause shown for each differing field of one resource, in `diffs` order. The permissions
/// builder's path-expiry drop (permissions.rs:111-133, :391-398) takes the resource's latest
/// ENSv2 registration event in today's (block, generated id) order. A `permissions_current` or
/// `resource_restrictions` field passes as a same-block delta only in one direction: today's
/// order keeps the registration live while the canonical order lapses it, the served value is
/// not empty, the canonical read is empty, and the whole permission read of the resource taken
/// again from the families in today's order equals it. That read is compared whole, so a wrong subject, power, collision
/// row or restriction in the families fails. The shadow value is the canonical read itself, so
/// comparing it with the canonical read checks nothing and is not counted as evidence. The other
/// direction, today's order lapsing the registration, is left a mismatch: the read in that order
/// is empty, so matching it would only show that the served value is empty.
pub async fn resource_excuses(
    pool: &PgPool,
    chain: &str,
    clock: &Clock,
    input: &ResourceInput,
    diffs: &[Difference],
) -> Result<Vec<Excuse>> {
    let mut out = vec![Excuse::None; diffs.len()];
    if !diffs.iter().any(|diff| {
        matches!(
            diff.field.as_str(),
            "permissions_current" | "admin_powers" | "resource_restrictions"
        )
    }) {
        return Ok(out);
    }
    let rows: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(event) FROM project_lifecycle_event event
         WHERE event.chain_id = $1 AND event.state_kind = 'resource' AND event.state_key = $2",
    )
    .bind(chain)
    .bind(&input.resource_id)
    .fetch_all(pool)
    .await?;
    let events: Vec<LifecycleEvent> = rows.iter().filter_map(LifecycleEvent::from_row).collect();
    let identities: Vec<String> = events
        .iter()
        .map(|event| event.position.event_identity.clone())
        .collect();
    let ids = generated_ids(pool, chain, &identities).await?;
    if !events
        .iter()
        .all(|event| ids.contains_key(&event.position.event_identity))
    {
        return Ok(out);
    }
    let today = EventOrder::Generated(ids);
    let lapsed = |order: &EventOrder| registration_lapsed(&maxima_of(&events, true, order), order);
    if lapsed(&today) || !lapsed(&EventOrder::Canonical) {
        return Ok(out);
    }
    let read = |order: EventOrder| async move {
        load_shadow_permissions_in(pool, chain, clock, std::slice::from_ref(input), &order)
            .await
            .map(|mut reads| reads.remove(&input.resource_id).unwrap_or_default())
    };
    let legacy = read(today).await?;
    let canonical = read(EventOrder::Canonical).await?;
    let value = |read: &ShadowPermissions, field: &str| match field {
        "permissions_current" => Some(Value::Array(read.grants.iter().map(grant_json).collect())),
        "admin_powers" => Some(json!(read.admin_powers)),
        "resource_restrictions" => Some(read.restrictions.clone().unwrap_or(Value::Null)),
        _ => None,
    };
    for (index, diff) in diffs.iter().enumerate() {
        let (Some(legacy), Some(canonical)) =
            (value(&legacy, &diff.field), value(&canonical, &diff.field))
        else {
            continue;
        };
        let empty = |value: &Value| match value {
            Value::Null => true,
            Value::Array(rows) => rows.is_empty(),
            _ => false,
        };
        if !empty(&diff.served)
            && empty(&canonical)
            && same(&legacy, &diff.served)
            && !same(&legacy, &canonical)
        {
            out[index] = Excuse::SameBlockOrder;
        }
    }
    Ok(out)
}

/// Exactly one printed report for each expected target, and none for any other target.
pub fn one_report_per_target(reports: &[Counted], targets: &[i64]) -> Result<()> {
    let mut expected = targets.to_vec();
    expected.sort_unstable();
    let distinct = expected.windows(2).all(|pair| pair[0] != pair[1]);
    anyhow::ensure!(distinct, "the expected targets repeat: {expected:?}");
    let mut seen: Vec<i64> = reports.iter().map(|report| report.0).collect();
    seen.sort_unstable();
    anyhow::ensure!(
        seen == expected,
        "shadow reports for targets {seen:?}, expected one each for {expected:?}"
    );
    Ok(())
}

/// The fixture corpus's counted fields, asserted exactly against counts read from its event log
/// at each target without the family readers (crates/project/tests/rebuild_performance/seed.sql):
/// - an ENSv2 name whose interpreter path-expiry release (the `expired` rows, no name, the token
///   resource) is not followed on that resource by a grant, reservation or named release is served
///   active today and released by the families: eight fields each, and the two control-owner
///   fields again for those whose token was transferred before the target.
///
/// No same-block delta may pass on the corpus.
pub async fn assert_fixture_corpus_counts(pool: &PgPool, targets: &[i64]) -> Result<()> {
    let reports = take_reports();
    one_report_per_target(&reports, targets)?;
    for (target, delta, known) in reports {
        anyhow::ensure!(
            delta.is_empty(),
            "target {target}: same-block deltas {delta:?}"
        );
        let expired: Vec<(String, bool)> = sqlx::query_as(
            "SELECT name.logical_name_id, EXISTS (
                        SELECT 1 FROM normalized_events transfer
                        WHERE transfer.resource_id = release.resource_id
                          AND transfer.logical_name_id = name.logical_name_id
                          AND transfer.event_kind = 'TokenControlTransferred'
                          AND transfer.block_number <= $1)
             FROM name_current name
             JOIN normalized_events release ON release.resource_id = name.resource_id
             WHERE release.logical_name_id IS NULL
               AND release.event_kind = 'RegistrationReleased'
               AND release.after_state ->> 'source_event' = 'RegistryPathExpired'
               AND release.block_number <= $1
               AND NOT EXISTS (
                   SELECT 1 FROM normalized_events later
                   WHERE later.resource_id = release.resource_id
                     AND later.block_number > release.block_number
                     AND later.block_number <= $1
                     AND (later.event_kind IN ('RegistrationGranted', 'RegistrationReserved')
                          OR (later.event_kind = 'RegistrationReleased'
                              AND later.logical_name_id IS NOT NULL)))",
        )
        .bind(target)
        .fetch_all(pool)
        .await?;
        let cause = "served_membership_skips_unnamed_path_expiry";
        let transferred = expired
            .iter()
            .filter(|(_, transferred)| *transferred)
            .count();
        let mut expected: BTreeMap<String, usize> = BTreeMap::new();
        for field in [
            "registration/status",
            "registration/latest_event_kind",
            "registration/authority_kind",
            "registration/registrant",
            "registration/released_at",
            "control/status",
            "control/expiry",
            "control/registrant",
        ] {
            if !expired.is_empty() {
                expected.insert(format!("{cause}:{field}"), expired.len());
            }
        }
        for field in ["control/latest_event_kind", "control/registry_owner"] {
            if transferred > 0 {
                expected.insert(format!("{cause}:{field}"), transferred);
            }
        }
        anyhow::ensure!(
            known == expected,
            "target {target}: counted {known:?}, the event log gives {expected:?}"
        );
    }
    Ok(())
}
