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
//!   every position kept so the authority admission is unchanged, and the association winner of
//!   an affected triple moved only to a grant of the same name, registry and token
//!   (`v2_lifecycle_events.sql:10-23`), gives exactly the served value for the field. For a
//!   resource's permission rows and restriction block, the path-expiry drop rule of
//!   permissions.rs:111-133, :391-398 must keep the registration live in today's order and
//!   lapse it in the canonical order, the served value must not be empty, and the whole read in
//!   today's order must equal it. A served empty value against a canonical row is a mismatch.
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
//!   control unregistered with nothing else.
//! - `served_release_presentation_reads_the_raw_arm`: the selection reads a missing authority
//!   arm as ENSv2 (build.sql:347), so a name with no selected arm can select an ENSv2 release,
//!   but today's presentation compares the raw arm with 'ens_v2' (build.sql:89, :94, :103) and
//!   serves that release with its registrant, authority and expiry and a live control block.
//!   The reader decides both with the one resolved arm and presents the release whole (ruling
//!   R1). A field passes only when no arm is selected, the shadow presents the whole release,
//!   and the served value is what the reader traced for the raw-arm presentation.
//! - `binding_candidate_pairs_its_surface_bound_by_log`: step 2 pairs a binding with the
//!   SurfaceBound at the transaction and log index its provenance records
//!   (crates/project/src/families/identity.rs `opening_event`), and a binding with no such event
//!   gets a candidate with no wrapper metadata, so the NameWrapper staging pass cannot name the
//!   wrapped lease's unnamed registrar rows for it. Today's stage reads the NameWrapper
//!   SurfaceBound itself (name_authority/stage.rs:149-198). A field passes only when the
//!   candidate has no opening SurfaceBound, its block has a NameWrapper SurfaceBound of the same
//!   name and resource that recorded a lease, and the families loaded again with the candidate
//!   given that event's wrapper metadata give exactly the served value. Step 2's pairing is the
//!   chain's shape: the adapter pushes a binding and its SurfaceBound from one raw log, and the
//!   interpreter refuses a binding whose provenance lacks the indexes outside raw-block
//!   derivation. What is wrong is the corpus seed (rebuild_performance/seed.sql binds at log 1
//!   and wraps at log 5). The seed fix goes into step 2's branch; this cause and
//!   `load_name_facts_replacing` go at the next merge of step 2.
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
            ShadowName, evaluate, load_name_facts, load_name_facts_replacing, load_shadow_names,
            membership::maxima_of,
        },
        permissions::{
            ResourceInput, ShadowPermissions, effective_operator_rows, grant_json,
            load_shadow_approvals, load_shadow_permissions, load_shadow_permissions_in,
        },
        position::EventOrder,
        registry::{
            NameAttribution, load_observations, load_registry_nodes, ownerless_registry,
            registry_bindings, registry_generation,
        },
        rows::{BindingCandidate, LifecycleEvent},
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
            let computed: BTreeMap<(String, String), Value> =
                effective_operator_rows(chain, resource, &binding, &approvals)
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
            let served = served_operators.get(resource).cloned().unwrap_or_default();
            let (served, computed) = (
                Value::Array(served.into_values().collect()),
                Value::Array(computed.into_values().collect()),
            );
            if !same(&served, &computed) {
                diffs.push(Difference {
                    field: "effective_operator_rows".into(),
                    served,
                    shadow: computed,
                });
            }
        }
        let excuses = resource_excuses(
            pool,
            chain,
            &clock,
            inputs_by_resource[resource.as_str()],
            &diffs,
        )
        .await?;
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

/// The value of a `registration/...` or `control/...` field of a shadow read.
fn shadow_field(shadow: &ShadowName, path: &str) -> Value {
    let Some((block, rest)) = path.split_once('/') else {
        return Value::Null;
    };
    let block = match block {
        "registration" => &shadow.registration,
        "control" => &shadow.control,
        _ => return Value::Null,
    };
    field(&Value::Object(block.clone()), rest).clone()
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
        && same(&diff.shadow, &shadow_field(shadow, &diff.field))
        && same(&diff.served, field(&raw[block], rest))
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
    let mut out = Vec::with_capacity(diffs.len());
    for diff in diffs {
        let excuse = if serves_the_unnamed_release(shadow, diff) {
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
    let Some(facts) = load_name_facts(pool, chain, std::slice::from_ref(input))
        .await?
        .pop()
    else {
        return Ok(out);
    };

    // The same families with each binding candidate step 2 left unpaired given its block's
    // NameWrapper SurfaceBound of the same name and resource, as today's stage reads it.
    let open = |out: &[Excuse], index: usize| out[index] == Excuse::None;
    let paired = wrapper_bounds_at_another_log(pool, chain, &facts).await?;
    if !paired.is_empty() {
        let counterfactual =
            load_name_facts_replacing(pool, chain, std::slice::from_ref(input), &paired)
                .await?
                .pop()
                .map(|facts| evaluate(&facts, clock));
        if let Some(counterfactual) = counterfactual {
            for (index, diff) in diffs.iter().enumerate() {
                if open(&out, index)
                    && same(&shadow_field(&counterfactual, &diff.field), &diff.served)
                {
                    out[index] = Excuse::Known("binding_candidate_pairs_its_surface_bound_by_log");
                }
            }
        }
    }

    // The same families read in today's generated-id order at the selectors that use it.
    if out.contains(&Excuse::None) {
        let identities: Vec<String> = facts
            .events
            .iter()
            .map(|event| event.position.event_identity.clone())
            .collect();
        let ids = generated_ids(pool, chain, &identities).await?;
        let keys = association_keys(pool, chain, &identities).await?;
        if let Some(legacy) = legacy_facts(&facts, &ids, &keys) {
            let counterfactual = evaluate(&legacy, clock);
            for (index, diff) in diffs.iter().enumerate() {
                if open(&out, index)
                    && same(&shadow_field(&counterfactual, &diff.field), &diff.served)
                {
                    out[index] = Excuse::SameBlockOrder;
                }
            }
        }
    }
    Ok(out)
}

/// The name's binding candidates that step 2 wrote with no opening SurfaceBound, each given the
/// wrapper metadata of the latest NameWrapper SurfaceBound of its name and resource in its block,
/// read from the event log. Step 2 pairs a binding with the SurfaceBound at the transaction and
/// log index its provenance records (crates/project/src/families/identity.rs `opening_event`);
/// today's stage reads the NameWrapper SurfaceBound itself (name_authority/stage.rs:149-158).
async fn wrapper_bounds_at_another_log(
    pool: &PgPool,
    chain: &str,
    facts: &NameFacts,
) -> Result<Vec<BindingCandidate>> {
    let mut out = Vec::new();
    for candidate in &facts.candidates {
        if candidate.surface_bound_position.is_some()
            || candidate.wrapped_registrar_resource_id.is_some()
        {
            continue;
        }
        let row: Option<WrapperBound> = sqlx::query_as(
            "SELECT after_state ->> 'wrapped_registrar_resource_id',
                    lower(after_state ->> 'node'), transaction_hash,
                    lower(raw_fact_ref ->> 'emitting_address'),
                    NULLIF(after_state ->> 'authority_kind', ''),
                    (after_state ->> 'state_derived')::boolean
             FROM normalized_events
             WHERE chain_id = $1 AND event_kind = 'SurfaceBound'
               AND source_family = 'ens_v1_wrapper_l1'
               AND logical_name_id = $2 AND resource_id = $3::uuid AND block_number = $4
               AND after_state ->> 'wrapped_registrar_resource_id' IS NOT NULL
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
             ORDER BY transaction_index DESC NULLS LAST, log_index DESC NULLS LAST,
                      convert_to(event_identity, 'UTF8') DESC
             LIMIT 1",
        )
        .bind(chain)
        .bind(&candidate.logical_name_id)
        .bind(&candidate.resource_id)
        .bind(candidate.block_number)
        .fetch_optional(pool)
        .await?;
        if let Some((lease, node, transaction_hash, emitter, kind, state_derived)) = row {
            let mut paired = candidate.clone();
            paired.wrapped_registrar_resource_id = lease;
            paired.node = node;
            paired.transaction_hash = transaction_hash;
            paired.emitting_address = emitter;
            paired.authority_kind = kind;
            paired.state_derived = state_derived;
            out.push(paired);
        }
    }
    Ok(out)
}

type WrapperBound = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<bool>,
);

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
    let mut blocks: BTreeMap<i64, Vec<&LifecycleEvent>> = BTreeMap::new();
    for event in &facts.events {
        ids.get(&event.position.event_identity)?;
        blocks
            .entry(event.position.block_number)
            .or_default()
            .push(event);
    }
    let disagreeing: BTreeSet<i64> = blocks
        .iter()
        .filter(|(_, events)| {
            let mut canonical: Vec<&LifecycleEvent> = events.to_vec();
            canonical.sort_by(|left, right| left.position.cmp(&right.position));
            let mut today = canonical.clone();
            today.sort_by_key(|event| ids[&event.position.event_identity]);
            canonical
                .iter()
                .zip(&today)
                .any(|(left, right)| left.position.event_identity != right.position.event_identity)
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
/// not empty, and the whole permission read of the resource taken again from the families in
/// today's order equals it. That read is compared whole, so a wrong subject, power, collision
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
            "permissions_current" | "resource_restrictions"
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
        "resource_restrictions" => Some(read.restrictions.clone().unwrap_or(Value::Null)),
        _ => None,
    };
    for (index, diff) in diffs.iter().enumerate() {
        let (Some(legacy), Some(canonical)) =
            (value(&legacy, &diff.field), value(&canonical, &diff.field))
        else {
            continue;
        };
        let served_empty = match &diff.served {
            Value::Null => true,
            Value::Array(rows) => rows.is_empty(),
            _ => false,
        };
        if !served_empty && same(&legacy, &diff.served) && !same(&legacy, &canonical) {
            out[index] = Excuse::SameBlockOrder;
        }
    }
    Ok(out)
}

/// The fixture corpus's counted fields, asserted exactly against counts read from its event log
/// at each target without the family readers (crates/project/tests/rebuild_performance/seed.sql):
/// - an ENSv2 name whose interpreter path-expiry release (the `expired` rows, no name, the token
///   resource) is not followed on that resource by a grant, reservation or named release is served
///   active today and released by the families: eight fields each, and the two control-owner
///   fields again for those whose token was transferred before the target;
/// - a wrapped name whose surface binding records another log than its NameWrapper SurfaceBound
///   (the seed binds at log 1 and wraps at log 5) has a binding candidate with no wrapper
///   metadata, so the families leave its unnamed lease rows unstaged where today's stage names
///   them: the lease's resource, authority kind and registration time once it is granted, the
///   expiry and latest kind again once it is renewed, the registrant again once its token moved.
///
/// No same-block delta may pass on the corpus.
pub async fn assert_fixture_corpus_counts(pool: &PgPool) -> Result<()> {
    let reports = take_reports();
    anyhow::ensure!(!reports.is_empty(), "no shadow comparison ran");
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
        let (granted, renewed, moved): (i64, i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE lease.granted), count(*) FILTER (WHERE lease.renewed),
                    count(*) FILTER (WHERE lease.moved)
             FROM name_current name
             JOIN surface_bindings binding ON binding.logical_name_id = name.logical_name_id
             JOIN normalized_events bound
               ON bound.event_kind = 'SurfaceBound' AND bound.source_family = 'ens_v1_wrapper_l1'
              AND bound.logical_name_id = binding.logical_name_id
              AND bound.resource_id = binding.resource_id
              AND bound.block_number = binding.block_number
              AND bound.log_index IS DISTINCT FROM (binding.provenance ->> 'log_index')::bigint
             CROSS JOIN LATERAL (
                 SELECT bool_or(event.event_kind = 'RegistrationGranted') AS granted,
                        bool_or(event.event_kind = 'RegistrationRenewed') AS renewed,
                        bool_or(event.event_kind = 'TokenControlTransferred') AS moved
                 FROM normalized_events event
                 WHERE event.resource_id::text = bound.after_state ->> 'wrapped_registrar_resource_id'
                   AND event.logical_name_id IS NULL
                   AND event.source_family = 'ens_v1_registrar_l1'
                   AND event.block_number <= $1) lease
             WHERE binding.block_number <= $1",
        )
        .bind(target)
        .fetch_one(pool)
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
        let unpaired = "binding_candidate_pairs_its_surface_bound_by_log";
        for (fields, count) in [
            (
                &[
                    "registration/resource_id",
                    "registration/authority_kind",
                    "registration/registered_at",
                ][..],
                granted,
            ),
            (
                &["registration/expiry", "registration/latest_event_kind"][..],
                renewed,
            ),
            (&["registration/registrant"][..], moved),
        ] {
            for field in fields {
                if count > 0 {
                    expected.insert(format!("{unpaired}:{field}"), usize::try_from(count)?);
                }
            }
        }
        anyhow::ensure!(
            known == expected,
            "target {target}: counted {known:?}, the event log gives {expected:?}"
        );
    }
    Ok(())
}
