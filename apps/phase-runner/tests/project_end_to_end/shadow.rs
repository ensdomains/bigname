//! Shadow comparison (TYR-36 step 3): after the owned key families have followed a publication,
//! the family readers of `bigname_storage::families::control` compute the registration and
//! control blocks of every served name, the permission rows, admin powers, restriction block and
//! registry binding of every summarised resource, the registry-operator rows the
//! effective-permission reader adds (every semantic column), and every account approval, and each
//! is compared with what the production readers serve from today's tables at the same
//! publication. `created_at`, the lapsed registration's authority, the child rows and the
//! whole-history evidence columns are not compared.
//!
//! A comparison is for one publication, a height and its block's hash (`Publication`). Before
//! its first read and after its last it checks that the block is readable and that the
//! families' marker and Project's published position both stand on it, outside a redo, and it
//! refuses to report otherwise (`fence`).
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
//!   (`v2_lifecycle_events.sql:10-23`), gives exactly the served value for the field. Those
//!   reads take the name's retained lifecycle events rebuilt from the publication-visible log,
//!   not the family rows, and the same rebuild read in the canonical order must give the
//!   shadow value; every named cause below needs that too. Every name excuse, this one and
//!   the named causes, also needs each
//!   epoch start (its kind, name and arm) and binding candidate's SurfaceBound (its kind, name,
//!   resource, authority kind and key, state-derived flag and owner) the families hold for the
//!   name to equal its rebuild from that log, since the registration's authority kind and key
//!   read them too, and a `control/*` field every owner-setting event of its node. Before
//!   any of that, the families must hold exactly the retained facts the log gives the name
//!   under step 2's retention rules, in both directions (`retention.rs`): a fact they lack is
//!   read by neither rebuild, so it could not fail them. For a
//!   resource's permission rows, admin powers and restriction block, the path-expiry drop rule of
//!   permissions.rs:111-133, :391-398, read from exactly the retained events the log gives the
//!   resource, must keep the registration live in today's order and
//!   lapse it in the canonical order, the served value must not be empty, and the whole read in
//!   today's order must equal it. A served empty value against a canonical row is a mismatch.
//!   A live registration's restriction block also passes when only its registry root's lapse
//!   differs between the orders, the whole block read in today's order equals the served one
//!   and the canonical read the shadow one; the canonical side is the families against
//!   themselves, and a wrong root value is caught by the root's own `admin_powers` mismatch
//!   (`resource_excuses`).
//!   For the registry binding, the observations are rebuilt from the publication-visible event
//!   log (activated, canonical, at the canonical lineage's hash, at or below the target: the set
//!   family intake reads), independently of the families: for each observation identity (the name, else the
//!   resource) its latest producer event canonically (block, transaction, log, event identity)
//!   and in today's order (block, transaction, log, generated id), each derived as step 2
//!   derives it, its target read from the name's current binding at the publication. A
//!   `registry_binding/*` field passes only when every family observation reaching the resource
//!   is exactly its identity's canonical rebuild (no identity on one side only), the canonical
//!   binding equals the shadow one whole, today's binding equals the served one whole, and the
//!   two select different event identities. The registry-operator rows pass with the binding
//!   only when the rows computed from each rebuilt binding equal the shadow and served rows.
//!   That is the binding rebuilt from the log plus the family's approvals, not the operator
//!   result rebuilt from the log: an approval only the canonical binding uses is checked by the
//!   account comparison, which compares every approval with its served row and excuses nothing.
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
    ACCOUNT_APPROVAL_READ_FILTER, CURRENT_PERMISSION_SUMMARY_READ_FILTER,
    DEFAULT_PERMISSIONS_CURRENT_READ_FILTER, EffectivePermissionScope,
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

#[path = "retention.rs"]
pub mod retention;

const NAME_CHUNK: usize = 500;
/// The served names of one chain: a name_current row belongs to the chain its provenance
/// names, the chain its read filter checks its publication lineage on (name_current.rs
/// `DEFAULT_NAME_CURRENT_READ_FILTER`). The resource summaries and permission rows are scoped
/// by their provenance chain the same way (canonicality.rs).
const NAMES_OF_CHAIN: &str =
    "SELECT logical_name_id FROM name_current WHERE provenance ->> 'chain_id' = $1 ORDER BY 1";
/// Difference lines printed per comparison; the counters count all of them.
const PRINTED: usize = 400;

/// The counted fields of every printed report in this process, by target, for the tests that
/// assert them exactly. It is process-wide: a test that reads it clears it first
/// (`take_reports`) and must not share its process with another comparing test, which holds for
/// the fixture-corpus test because every other comparing test in its binary is ignored.
static REPORTS: Mutex<Vec<Counted>> = Mutex::new(Vec::new());

/// One printed report's target, same-block delta fields and named-cause fields, and the
/// fixture corpus's expected named-cause fields read at the same publication when the
/// comparison was asked for them (`Options::corpus`).
pub type Counted = (
    i64,
    BTreeMap<String, usize>,
    BTreeMap<String, usize>,
    Option<BTreeMap<String, usize>>,
);

/// The counted fields of the reports printed so far, emptying the list.
pub fn take_reports() -> Vec<Counted> {
    REPORTS
        .lock()
        .map(|mut reports| std::mem::take(&mut *reports))
        .unwrap_or_default()
}

/// What one comparison saw.
#[derive(Debug, Default, PartialEq)]
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
    /// `corpus_expectation` at this publication, when asked for (`Options::corpus`).
    pub corpus_expected: Option<BTreeMap<String, usize>>,
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
                self.corpus_expected.clone(),
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

/// How one comparison runs.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Names read at a time. Every name's result must not depend on which other names share
    /// its chunk; the fixtures compare chunkings to check that.
    pub chunk: usize,
    /// Also read the fixture corpus's expected named-cause counts (`corpus_expectation`) at
    /// this publication, into `Report::corpus_expected`.
    pub corpus: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            chunk: NAME_CHUNK,
            corpus: false,
        }
    }
}

/// The publication a comparison is for: a height and the hash of its block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    pub number: i64,
    pub hash: String,
}

impl Publication {
    /// The readable block at `number`, for a caller that holds no hash (the fixtures, which
    /// publish one block at a time). Only the Project fixture tests call it.
    #[allow(dead_code)]
    pub async fn readable(pool: &PgPool, chain: &str, number: i64) -> Result<Self> {
        let hash: String = sqlx::query_scalar(
            "SELECT block_hash FROM chain_lineage
             WHERE chain_id = $1 AND block_number = $2
               AND canonicality_state IN ('canonical', 'safe', 'finalized')",
        )
        .bind(chain)
        .bind(number)
        .fetch_one(pool)
        .await
        .with_context(|| format!("block {number} is not readable"))?;
        Ok(Self { number, hash })
    }
}

/// The publication fence of one comparison: its block must be readable, the families' marker
/// and the Project phase's published position must both stand on it, and Project must not be
/// in a redo. The comparison reads the served tables, the families and the log separately, so
/// it checks this before its first read and after its last and refuses to report if either
/// fails: nothing it read can then belong to another publication at the same height.
async fn fence(pool: &PgPool, chain: &str, publication: &Publication) -> Result<()> {
    let (readable, families, served): (bool, bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM chain_lineage
                        WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
                          AND canonicality_state IN ('canonical', 'safe', 'finalized')),
                EXISTS (SELECT 1 FROM project_family_marker
                        WHERE chain_id = $1 AND current_block_number = $2
                          AND current_block_hash = $3),
                EXISTS (SELECT 1 FROM chain_phase_state
                        WHERE chain_id = $1 AND phase_name = 'project'
                          AND current_block_number = $2 AND current_block_hash = $3
                          AND NOT redo_in_progress)",
    )
    .bind(chain)
    .bind(publication.number)
    .bind(&publication.hash)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        readable && families && served,
        "publication {} {} is not both sides' (readable {readable}, families {families}, \
         served {served})",
        publication.number,
        publication.hash
    );
    Ok(())
}

/// Compare at the readable block of `target`. Only the Project fixture tests call it.
#[allow(dead_code)]
pub async fn compare(pool: &PgPool, chain: &str, target: i64) -> Result<Report> {
    let publication = Publication::readable(pool, chain, target).await?;
    compare_with(pool, chain, &publication, Options::default()).await
}

/// `compare` with the names read `chunk` at a time. Only the Project fixture tests call it.
#[allow(dead_code)]
pub async fn compare_in_chunks(
    pool: &PgPool,
    chain: &str,
    target: i64,
    chunk: usize,
) -> Result<Report> {
    let publication = Publication::readable(pool, chain, target).await?;
    let options = Options {
        chunk,
        ..Options::default()
    };
    compare_with(pool, chain, &publication, options).await
}

/// Compare every served item with its shadow read at `publication`, inside its fence.
pub async fn compare_with(
    pool: &PgPool,
    chain: &str,
    publication: &Publication,
    options: Options,
) -> Result<Report> {
    fence(pool, chain, publication)
        .await
        .context("before the comparison")?;
    let report = compare_fenced(pool, chain, publication, options).await?;
    fence(pool, chain, publication)
        .await
        .context("after the comparison")?;
    Ok(report)
}

async fn compare_fenced(
    pool: &PgPool,
    chain: &str,
    publication: &Publication,
    options: Options,
) -> Result<Report> {
    let target = publication.number;
    let mut report = Report::default();
    let timestamp: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM block_timestamp)::bigint FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3",
    )
    .bind(chain)
    .bind(target)
    .bind(&publication.hash)
    .fetch_one(pool)
    .await
    .context("target clock")?;
    let clock = Clock {
        block_number: target,
        timestamp_seconds: timestamp,
    };

    // Names: registration and control, registry generation and the ownerless profile.
    let keys: Vec<String> = sqlx::query_scalar(NAMES_OF_CHAIN)
        .bind(chain)
        .fetch_all(pool)
        .await?;
    let mut attributions = BTreeMap::new();
    for names in keys.chunks(options.chunk) {
        let rows = load_name_current_by_logical_name_ids(pool, names).await?;
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
        let mut pending: Vec<(&NameInput, Vec<Difference>, Value)> = Vec::new();
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
            pending.push((input, diffs, selection));
        }
        // The excuse pass reads the chunk's differing names' facts and their log rows once for
        // the chunk, not once per name.
        let differing: Vec<NameInput> = pending
            .iter()
            .filter(|(_, diffs, _)| !diffs.is_empty())
            .map(|(input, _, _)| (*input).clone())
            .collect();
        let prefetched = ExcuseInputs::load(pool, chain, clock.block_number, &differing).await?;
        for (input, diffs, selection) in pending {
            let shadow = &shadows[&input.logical_name_id];
            if !diffs.is_empty() && report.lines.len() < PRINTED {
                report.lines.push(format!(
                    "SEPOLIA_END_TO_END_SHADOW_TRACE key={} selection={} trace={}",
                    input.logical_name_id,
                    selection,
                    Value::Object(shadow.trace.clone())
                ));
            }
            let excuses = name_excuses(&prefetched, &clock, input, shadow, &diffs);
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

    // Resources: permission rows, restriction block, registry binding and operator rows. Every
    // served row below is what the serving readers expose: each read takes that reader's own
    // canonicality predicate (canonicality.rs, effective.rs), so a stale row of an
    // orphaned publication is not compared. The names above come through the serving loader,
    // which applies name_current.rs `DEFAULT_NAME_CURRENT_READ_FILTER`.
    let summaries: Vec<SummaryRow> = sqlx::query_as(&format!(
        "SELECT summary.resource_id::text, summary.authority_kind,
                summary.root_resource_id::text, summary.resource_restrictions,
                summary.registry_owner, summary.registry_contract,
                summary.registry_binding_provenance, summary.registry_binding_chain_positions,
                summary.provenance -> 'registry_binding_clear_event_id'
         FROM permissions_current_resource_summary summary
         WHERE summary.provenance ->> 'chain_id' = $1
           AND {CURRENT_PERMISSION_SUMMARY_READ_FILTER} ORDER BY 1"
    ))
    .bind(chain)
    .fetch_all(pool)
    .await?;
    let mut resource_ids: BTreeSet<String> = summaries.iter().map(|row| row.0.clone()).collect();
    let served_grants: Vec<(String, Value)> = sqlx::query_as(&format!(
        "SELECT pc.resource_id::text, jsonb_build_object(
                    'resource_id', pc.resource_id::text, 'subject', pc.subject,
                    'scope', pc.scope, 'scope_kind', pc.scope_kind,
                    'scope_detail', pc.scope_detail, 'effective_powers', pc.effective_powers,
                    'grant_source', pc.grant_source, 'revocation_source', pc.revocation_source,
                    'inheritance_path', pc.inheritance_path,
                    'transfer_behavior', pc.transfer_behavior)
         FROM permissions_current pc
         WHERE pc.provenance ->> 'chain_id' = $1 {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
         ORDER BY pc.resource_id, pc.subject, pc.scope"
    ))
    .bind(chain)
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
    let served_admins: BTreeMap<String, Vec<String>> = sqlx::query_as(&format!(
        r"SELECT pc.resource_id::text, array_agg(DISTINCT power.value ORDER BY power.value)
         FROM permissions_current pc
         CROSS JOIN LATERAL jsonb_array_elements_text(pc.effective_powers) power
         WHERE pc.provenance ->> 'chain_id' = $1 AND pc.scope_kind IN ('registry', 'root')
           AND (power.value LIKE 'admin\_%' OR power.value = 'can_transfer_admin')
           {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
         GROUP BY 1"
    ))
    .bind(chain)
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

    // Account approvals, under the account half of the operator reader's filter
    // (`ACCOUNT_APPROVAL_READ_FILTER`). The operator query's other half is not the resource
    // summary's filter: it also requires the summary's registry binding lineage readable
    // (effective.rs `ACCOUNT_READ_FILTER`). The operator rows above come through that reader
    // itself, so the account comparison does not repeat it.
    let served_accounts: Vec<Value> = sqlx::query_scalar(&format!(
        "SELECT jsonb_build_object('authority_kind', authority_kind,
                    'authority_contract', authority_contract,
                    'authority_contract_instance_id', authority_contract_instance_id::text,
                    'owner', owner, 'subject', subject, 'relation_kind', relation_kind,
                    'approved', approved, 'effective_powers', effective_powers,
                    'grant_source', grant_source, 'revocation_source', revocation_source,
                    'inheritance_path', inheritance_path, 'transfer_behavior', transfer_behavior)
         FROM account_permission_state_current aps
         WHERE aps.chain_id = $1 {ACCOUNT_APPROVAL_READ_FILTER}"
    ))
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
    if options.corpus {
        report.corpus_expected = Some(corpus_expectation(pool, chain, target).await?);
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
fn without_unnamed_release(facts: &NameFacts, ids: &BTreeMap<String, i64>) -> NameFacts {
    let mut membership = facts.clone();
    membership.events.retain(|event| {
        !(event.original_logical_name_id.is_none()
            && event.event_kind == "RegistrationReleased"
            && event.is_path_expiry())
    });
    membership.order = EventOrder::Generated(ids.clone());
    membership
}

/// Whether `read` computes `value` for `field`.
fn gives(read: Option<&ShadowName>, field: &str, value: &Value) -> bool {
    read.and_then(|read| shadow_field(read, field))
        .is_some_and(|computed| same(&computed, value))
}

/// The cause shown for each differing field of one name, in `diffs` order. The families must
/// first hold exactly the retained facts the log gives the name (`retention::name_differs`).
/// Every cause then rests on the name's retained lifecycle events rebuilt from the
/// publication-visible log, not on the family rows: read in the canonical order (through the
/// refolding path, so the stored key states and triple summaries are not read either) they
/// must give the field's shadow value. Each retained row must also equal its rebuild whole, so
/// a wrong fact on a retained lifecycle row fails every field of the name. Facts the retention
/// check does not rebuild are listed in `retention.rs`.
fn name_excuses(
    prefetched: &ExcuseInputs,
    clock: &Clock,
    input: &NameInput,
    shadow: &ShadowName,
    diffs: &[Difference],
) -> Vec<Excuse> {
    let mut out = vec![Excuse::None; diffs.len()];
    if diffs.is_empty() {
        return out;
    }
    let Some(facts) = prefetched.facts.get(&input.logical_name_id) else {
        return out;
    };
    // The families must hold exactly the retained facts the log gives the name: a fact they
    // lack is read by neither rebuild below, so it could not fail them.
    if retention::name_differs(facts, &prefetched.retention).is_some() {
        return out;
    }
    let Some(from_log) = log_facts_in(facts, &prefetched.log, &prefetched.snapshots) else {
        return out;
    };
    // Every retained row must also equal its rebuild whole: the shadow read the family rows,
    // so a wrong payload the rebuild drops would otherwise leave a shadow value the log does
    // not give, reclassified rather than failed.
    if facts
        .events
        .iter()
        .zip(&from_log.events)
        .any(|(row, log)| format!("{row:?}") != format!("{log:?}"))
    {
        return out;
    }
    let canonical = evaluate(&in_canonical_ranks(&from_log), clock);
    // Every excuse also needs the families' identity facts, the epoch starts and the binding
    // candidates' SurfaceBounds, to be what the log gives, since both blocks read them, and a
    // control field the node's owner-setting events too.
    let held = control_fact_checks_in(facts, &prefetched.log);
    let log_gives_shadow: Vec<bool> = diffs
        .iter()
        .map(|diff| {
            held.identity
                && (held.owners || !diff.field.starts_with("control/"))
                && gives(Some(&canonical), &diff.field, &diff.shadow)
        })
        .collect();
    // The unnamed-release cause passes a field only when today's name-scoped membership gives
    // the served value: the events rebuilt from the log, read in today's order without the
    // unnamed path-expiry release (build.sql:322, :366-367 build membership by name).
    let unnamed: Vec<bool> = diffs
        .iter()
        .map(|diff| serves_the_unnamed_release(shadow, diff))
        .collect();
    let without_release = unnamed
        .contains(&true)
        .then(|| evaluate(&without_unnamed_release(&from_log, &prefetched.ids), clock));
    for (index, diff) in diffs.iter().enumerate() {
        if !log_gives_shadow[index] {
            continue;
        }
        out[index] = if unnamed[index] && gives(without_release.as_ref(), &diff.field, &diff.served)
        {
            Excuse::Known("served_membership_skips_unnamed_path_expiry")
        } else if serves_the_raw_arm_release(input, shadow, diff) {
            Excuse::Known("served_release_presentation_reads_the_raw_arm")
        } else {
            Excuse::None
        };
    }
    if !out
        .iter()
        .zip(&log_gives_shadow)
        .any(|(excuse, holds)| *excuse == Excuse::None && *holds)
    {
        return out;
    }

    // The same events read in today's generated-id order at the selectors that use it: a field
    // passes as a same-block delta only when that read gives the served value.
    let Some(legacy) = legacy_facts(&from_log, &prefetched.ids, &prefetched.keys) else {
        return out;
    };
    let today = evaluate(&legacy, clock);
    for (index, diff) in diffs.iter().enumerate() {
        // Only the fields both reads compute: an authority-selection field has no today's-order
        // value here and stays open.
        if out[index] != Excuse::None
            || !log_gives_shadow[index]
            || !gives(Some(&today), &diff.field, &diff.served)
        {
            continue;
        }
        out[index] = Excuse::SameBlockOrder;
    }
    out
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

/// One publication-visible event of the log.
pub struct LogEvent {
    /// The generated normalized event id.
    pub id: i64,
    pub kind: String,
    pub namespace: String,
    pub name: Option<String>,
    pub resource: Option<String>,
    pub family: String,
    pub position: Position,
    pub transaction_hash: Option<String>,
    /// The raw fact's emitting address, lower-cased, none when blank (step 2
    /// `BlockEvent::emitting_address`, crates/project/src/families/input.rs:84-86, :100-106).
    pub emitter: Option<String>,
    pub before: Value,
    pub after: Value,
}

/// The publication-visible events (`published`) of `identities`, by identity.
pub async fn published_log(
    pool: &PgPool,
    chain: &str,
    target: i64,
    identities: &[String],
) -> Result<BTreeMap<String, LogEvent>> {
    published_where(
        pool,
        chain,
        target,
        "event.event_identity = ANY($2)",
        identities,
    )
    .await
}

/// The publication-visible events (`published`) that also meet `condition`, which reads
/// `values` as the text array `$2`, by identity.
pub async fn published_where(
    pool: &PgPool,
    chain: &str,
    target: i64,
    condition: &str,
    values: &[String],
) -> Result<BTreeMap<String, LogEvent>> {
    type Row = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        i64,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Value,
        Value,
        i64,
    );
    let sql = format!(
        "SELECT event.event_identity, event.event_kind, event.namespace, event.logical_name_id,
                event.resource_id::text, event.source_family, event.block_number,
                event.transaction_index, event.log_index, event.transaction_hash,
                CASE WHEN jsonb_typeof(event.raw_fact_ref -> 'emitting_address')
                               IN ('string', 'number')
                          AND btrim(event.raw_fact_ref ->> 'emitting_address') <> ''
                     THEN lower(event.raw_fact_ref ->> 'emitting_address') END,
                event.before_state, event.after_state, event.normalized_event_id
         FROM normalized_events event
         WHERE event.chain_id = $1 AND ({condition}) AND {}",
        published("$3")
    );
    Ok(sqlx::query_as::<_, Row>(&sql)
        .bind(chain)
        .bind(values)
        .bind(target)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| {
            let position = Position {
                block_number: row.6,
                transaction_index: row.7,
                log_index: row.8,
                event_identity: row.0.clone(),
            };
            let event = LogEvent {
                id: row.13,
                kind: row.1,
                namespace: row.2,
                name: row.3,
                resource: row.4,
                family: row.5,
                position,
                transaction_hash: row.9,
                emitter: row.10,
                before: row.11,
                after: row.12,
            };
            (row.0, event)
        })
        .collect())
}

/// `after ->> field` as step 2 stores it: a string as it is, another value as its JSON text,
/// null as none (crates/project/src/families/reduce.rs:134-140).
fn raw_text(value: &Value, field: &str) -> Option<String> {
    match value.get(field)? {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

fn raw_lower(value: &Value, field: &str) -> Option<String> {
    raw_text(value, field).map(|text| text.to_ascii_lowercase())
}

/// A boolean or its text, as step 2 stores a flag (crates/project/src/families/lifecycle.rs
/// :291-299).
fn raw_flag(value: &Value, field: &str) -> Option<bool> {
    match value.get(field) {
        Some(Value::Bool(flag)) => Some(*flag),
        Some(Value::String(text)) if text == "true" || text == "false" => Some(text == "true"),
        _ => None,
    }
}

/// The expiry in seconds as step 2 converts it: an integral JSON number in range, else none
/// (crates/project/src/families/lifecycle.rs:302-316, build.sql:493-501).
fn expiry_seconds(after: &Value) -> Option<i64> {
    let Some(Value::Number(number)) = after.get("expiry") else {
        return None;
    };
    number
        .as_i64()
        .or_else(|| {
            number
                .as_f64()
                .filter(|value| value.fract() == 0.0 && value.abs() < 1e15)
                .map(|value| value as i64)
        })
        .filter(|value| (-377_705_116_800..=253_402_300_799).contains(value))
}

/// A retained lifecycle event rebuilt from its log row as step 2 writes it
/// (crates/project/src/families/lifecycle.rs:318-389, `retained_columns`), keeping only the
/// family's key and decoded name, which the log does not carry. None when the log row is not at
/// the event's position.
fn lifecycle_from_log(event: &LifecycleEvent, log: &LogEvent) -> Option<LifecycleEvent> {
    if log.position != event.position {
        return None;
    }
    let after = &log.after;
    let kind = log.kind.as_str();
    Some(LifecycleEvent {
        position: log.position.clone(),
        event_kind: log.kind.clone(),
        original_logical_name_id: log.name.clone(),
        resource_id: log.resource.clone(),
        source_family: log.family.clone(),
        authority_kind: raw_text(after, "authority_kind")
            .filter(|kind| !kind.is_empty())
            .unwrap_or_else(|| "registrar".into()),
        authority_kind_raw: raw_text(after, "authority_kind"),
        authority_key: raw_text(after, "authority_key"),
        transaction_hash: log.transaction_hash.clone(),
        to_address: (kind == "TokenControlTransferred")
            .then(|| raw_lower(after, "to"))
            .flatten(),
        namehash: raw_lower(after, "namehash"),
        registrant: raw_lower(after, "registrant"),
        before_registrant: (kind == "RegistrationReleased")
            .then(|| raw_lower(&log.before, "registrant"))
            .flatten(),
        expiry: after.get("expiry").cloned().unwrap_or(Value::Null),
        expiry_seconds: expiry_seconds(after),
        status: raw_text(after, "status"),
        released_at: after.get("released_at").cloned().unwrap_or(Value::Null),
        source_event: raw_text(after, "source_event"),
        derived_from: raw_text(after, "derived_from"),
        terminal_reason: raw_text(after, "terminal_reason"),
        revived_from_expiry: raw_flag(after, "revived_from_expiry"),
        state_derived: raw_flag(after, "state_derived"),
        surface_materialization: raw_flag(after, "surface_materialization"),
        registrar_surface_snapshot: raw_flag(after, "registrar_surface_snapshot"),
        original_registered_at: logged_registration_time(log),
        owner_getter: raw_lower(after, "owner_getter"),
        owner_word_unmasked: raw_flag(after, "owner_word_unmasked"),
        registry_owner: raw_lower(after, "registry_owner"),
        ..event.clone()
    })
}

/// `events` each rebuilt from its publication-visible log row; None when one is not there.
async fn events_from_log(
    pool: &PgPool,
    chain: &str,
    target: i64,
    events: &[LifecycleEvent],
) -> Result<Option<Vec<LifecycleEvent>>> {
    let identities: Vec<String> = events
        .iter()
        .map(|event| event.position.event_identity.clone())
        .collect();
    let log = published_log(pool, chain, target, &identities).await?;
    Ok(events
        .iter()
        .map(|event| lifecycle_from_log(event, log.get(&event.position.event_identity)?))
        .collect())
}

/// The name's facts with every retained lifecycle event rebuilt from its publication-visible
/// log row in `log`; None when one is not there. The snapshot registration times render
/// through `snapshots`, the conversions of the logged times, keeping exactly the rebuilt
/// events' own: the loader's map holds the family times of every name in its batch, so a
/// rebuilt time could otherwise render or not by which names share the chunk.
fn log_facts_in(
    facts: &NameFacts,
    log: &BTreeMap<String, LogEvent>,
    snapshots: &BTreeMap<i64, Value>,
) -> Option<NameFacts> {
    let events = facts
        .events
        .iter()
        .map(|event| lifecycle_from_log(event, log.get(&event.position.event_identity)?))
        .collect::<Option<Vec<_>>>()?;
    let mut out = facts.clone();
    out.snapshot_timestamps = events
        .iter()
        .filter_map(|event| {
            let seconds = event.original_registered_at?;
            Some((seconds, snapshots.get(&seconds)?.clone()))
        })
        .collect();
    out.events = events;
    Some(out)
}

/// The registration time of a retained event's log row as step 2 stores it
/// (`lifecycle_from_log`).
fn logged_registration_time(event: &LogEvent) -> Option<i64> {
    raw_text(&event.after, "original_registered_at").and_then(|value| value.parse().ok())
}

/// What the name excuses read, loaded once for a chunk of differing names: their facts, and the
/// publication-visible log rows, generated ids and association keys of every lifecycle and
/// control event those facts name.
pub struct ExcuseInputs {
    pub facts: BTreeMap<String, NameFacts>,
    pub log: BTreeMap<String, LogEvent>,
    pub ids: BTreeMap<String, i64>,
    pub keys: BTreeMap<String, (String, String)>,
    /// What the log gives the names under step 2's retention rules.
    pub retention: retention::RetentionLog,
    /// `to_jsonb(to_timestamp(seconds))` of every registration time in `log`, as the loader
    /// converts the family's (load.rs:255-270).
    pub snapshots: BTreeMap<i64, Value>,
}

impl ExcuseInputs {
    pub async fn load(
        pool: &PgPool,
        chain: &str,
        target: i64,
        names: &[NameInput],
    ) -> Result<Self> {
        let facts: BTreeMap<String, NameFacts> = if names.is_empty() {
            BTreeMap::new()
        } else {
            load_name_facts(pool, chain, names)
                .await?
                .into_iter()
                .map(|facts| (facts.input.logical_name_id.clone(), facts))
                .collect()
        };
        let identities: Vec<String> = facts
            .values()
            .flat_map(|facts| {
                facts
                    .events
                    .iter()
                    .map(|event| event.position.event_identity.clone())
                    .chain(
                        control_positions(facts)
                            .into_iter()
                            .map(|position| position.event_identity),
                    )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let log = published_log(pool, chain, target, &identities).await?;
        let ids = log
            .iter()
            .map(|(identity, event)| (identity.clone(), event.id))
            .collect();
        let keys = association_keys(pool, chain, target, &identities).await?;
        let retention = retention::RetentionLog::load(pool, chain, target, &facts).await?;
        let seconds: Vec<i64> = log
            .values()
            .filter_map(logged_registration_time)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let snapshots = sqlx::query_as::<_, (i64, Value)>(
            "SELECT seconds, to_jsonb(to_timestamp(seconds)) FROM unnest($1::bigint[]) seconds",
        )
        .bind(&seconds)
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect();
        Ok(Self {
            facts,
            log,
            ids,
            keys,
            retention,
            snapshots,
        })
    }
}

/// The facts read in the canonical order through the refolding path: every event and control
/// position ranked by (block, transaction, log, identity) and the rank given as its generated
/// id, so membership and the laterals fold the retained events themselves in the canonical
/// order rather than read the stored key states and triple summaries.
pub fn in_canonical_ranks(facts: &NameFacts) -> NameFacts {
    let mut positions: Vec<Position> = facts
        .events
        .iter()
        .map(|event| event.position.clone())
        .chain(control_positions(facts))
        .collect();
    positions.sort();
    positions.dedup_by(|left, right| left.event_identity == right.event_identity);
    let mut out = facts.clone();
    out.order = EventOrder::Generated(
        positions
            .into_iter()
            .enumerate()
            .map(|(rank, position)| (position.event_identity, rank as i64))
            .collect(),
    );
    out
}

/// The arm step 2 files an epoch start under (crates/project/src/families/identity.rs:29-37).
fn epoch_arm(source_family: &str) -> &'static str {
    if source_family.starts_with("basenames_") {
        "basenames"
    } else if source_family.starts_with("ens_v2_") {
        "ens_v2"
    } else {
        "ens_v1"
    }
}

/// Whether every control fact of the name the control block reads, beyond its lifecycle
/// events, is what the event log up to the publication gives: each owner-setting event of its
/// node, each epoch start and each binding candidate's SurfaceBound, found by identity at its
/// position among the publication-visible events (`published`) and rebuilt from it as step 2
/// derives it. An epoch start must be an AuthorityEpochChanged of the name filed under its
/// family's arm (identity.rs:88-101); a candidate's SurfaceBound must be of the candidate's
/// name and resource with its authority kind, key, state-derived flag and owner
/// (identity.rs:135-152, :380-400).
/// Only the Project fixture tests call it; the comparison uses `control_fact_checks_in`.
#[allow(dead_code)]
pub async fn control_facts_hold(
    pool: &PgPool,
    chain: &str,
    target: i64,
    facts: &NameFacts,
) -> Result<bool> {
    let identities: Vec<String> = control_positions(facts)
        .into_iter()
        .map(|position| position.event_identity)
        .collect();
    let log = published_log(pool, chain, target, &identities).await?;
    let checks = control_fact_checks_in(facts, &log);
    Ok(checks.owners && checks.identity)
}

/// The two halves of `control_facts_hold`: the node's owner-setting events, which only the
/// control block reads, and the identity facts (epoch starts and candidate SurfaceBounds),
/// which the registration's authority kind and key read too (laterals.rs
/// `authority_context`).
pub struct ControlFactChecks {
    pub owners: bool,
    pub identity: bool,
}

/// The checks of `control_facts_hold` against publication-visible log rows already loaded.
fn control_fact_checks_in(
    facts: &NameFacts,
    log: &BTreeMap<String, LogEvent>,
) -> ControlFactChecks {
    let at = |position: &Position| {
        log.get(&position.event_identity)
            .filter(|row| row.position == *position)
    };
    let lower = |after: &Value, name: &str| raw_lower(after, name);
    let owners_hold = facts
        .registry_node
        .iter()
        .flat_map(|node| &node.owner_events)
        .all(|event| {
            at(&event.position).is_some_and(|row| {
                let after = &row.after;
                row.kind == event.event_kind
                    && row.name == event.logical_name_id
                    && row.resource == event.resource_id
                    && row.family == event.source_family
                    && raw_text(after, "authority_kind") == event.authority_kind
                    && lower(after, "owner") == event.owner
                    && lower(after, "registry_owner") == event.registry_owner
                    && after.get("owner_word_unmasked").and_then(Value::as_bool)
                        == event.owner_word_unmasked
                    && lower(after, "owner_getter") == event.owner_getter
            })
        });
    let name = facts.input.logical_name_id.as_str();
    let starts_hold = facts
        .authority_starts
        .as_object()
        .into_iter()
        .flatten()
        .all(|(arm, start)| {
            Position::from_json(start).is_some_and(|position| {
                at(&position).is_some_and(|row| {
                    let member = |field: &str| start.get(field).and_then(Value::as_str);
                    let after = &row.after;
                    row.kind == "AuthorityEpochChanged"
                        && row.name.as_deref() == Some(name)
                        && epoch_arm(&row.family) == arm
                        && member("owner").map(str::to_owned) == reported_control_owner(after)
                        && member("resource_id") == row.resource.as_deref()
                        && member("authority_kind").map(str::to_owned)
                            == raw_text(after, "authority_kind")
                        && member("authority_key").map(str::to_owned)
                            == raw_text(after, "authority_key")
                })
            })
        });
    let bounds_hold = facts.candidates.iter().all(|candidate| {
        candidate
            .surface_bound_position
            .as_ref()
            .is_none_or(|position| {
                at(position).is_some_and(|row| {
                    let after = &row.after;
                    row.kind == "SurfaceBound"
                        && row.name.as_deref() == Some(candidate.logical_name_id.as_str())
                        && row.resource.as_deref() == Some(candidate.resource_id.as_str())
                        && raw_text(after, "authority_kind") == candidate.authority_kind
                        && raw_text(after, "authority_key") == candidate.authority_key
                        && after.get("state_derived").and_then(Value::as_bool)
                            == candidate.state_derived
                        && candidate.bound_owner == reported_control_owner(after)
                })
            })
    });
    ControlFactChecks {
        owners: owners_hold,
        identity: starts_hold && bounds_hold,
    }
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

/// Every resource's registry binding rebuilt from the publication-visible event log
/// (`published`), in both orders, independently of the family's choices. For each observation identity (the
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
    let rows: Vec<RivalRow> = sqlx::query_as(&format!(
        r#"SELECT COALESCE(event.logical_name_id, event.resource_id::text), event.event_identity,
               event.logical_name_id, event.resource_id::text, event.block_number,
               event.transaction_index, event.log_index, event.normalized_event_id,
               event.event_kind,
               lower(CASE WHEN event.source_family IN (
                                  'ens_v1_registrar_l1', 'basenames_base_registrar')
                              OR (event.event_kind = 'SurfaceBound'
                                  AND event.after_state @> '{{"state_derived":true,"authority_kind":"registry_only"}}')
                          THEN event.after_state ->> 'registry_contract'
                          ELSE COALESCE(event.raw_fact_ref ->> 'emitting_address',
                                        event.after_state ->> 'registry_contract') END),
               jsonb_build_object('owner_getter', lower(event.after_state ->> 'owner_getter'),
                                  'raw_fact_ref', event.raw_fact_ref)
         FROM normalized_events event
         WHERE event.chain_id = $1 AND {}
           AND event.event_kind IN ('AuthorityTransferred', 'SubregistryChanged', 'SurfaceBound',
                                    'SurfaceUnbound')
           AND (event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
                OR (event.event_kind IN ('SurfaceBound', 'SurfaceUnbound')
                    AND event.source_family IN ('ens_v1_registrar_l1',
                                                'basenames_base_registrar')))
           AND event.resource_id IS NOT NULL"#,
        published("$2")
    ))
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
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_number = binding.block_number
          AND lineage.block_hash = binding.block_hash
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
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

/// The publication-visible event log at or below the target bound `target` (a query
/// parameter): activated, canonical, and at the canonical lineage's hash for its height, the
/// set family intake reads (crates/project/src/families/input.rs:175-195, :309-319). `event` is
/// the normalized_events alias. Every log read an excuse rests on takes this predicate.
fn published(target: &str) -> String {
    published_as("event", target)
}

/// `published` for the normalized_events alias `event`.
fn published_as(event: &str, target: &str) -> String {
    format!(
        "{event}.block_number <= {target}
         AND {event}.consumer_visibility = 'activated'
         AND {event}.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND EXISTS (SELECT 1 FROM chain_lineage lineage
                     WHERE lineage.chain_id = {event}.chain_id
                       AND lineage.block_number = {event}.block_number
                       AND lineage.block_hash = {event}.block_hash
                       AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))"
    )
}

/// The generated ids of events, by identity, among the publication-visible events
/// (`published`). An event that is a candidate, off the canonical lineage or past the target
/// has no id here, so a read that needs it has no today's order.
pub async fn generated_ids(
    pool: &PgPool,
    chain: &str,
    target: i64,
    identities: &[String],
) -> Result<BTreeMap<String, i64>> {
    let sql = format!(
        "SELECT event.event_identity, event.normalized_event_id FROM normalized_events event
         WHERE event.chain_id = $1 AND event.event_identity = ANY($2) AND {}",
        published("$3")
    );
    Ok(sqlx::query_as::<_, (String, i64)>(&sql)
        .bind(chain)
        .bind(identities)
        .bind(target)
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect())
}

/// The registry identifier and token id of publication-visible events (`published`), by
/// identity, as today's association keys them (v2_lifecycle_events.sql:14-19).
pub async fn association_keys(
    pool: &PgPool,
    chain: &str,
    target: i64,
    identities: &[String],
) -> Result<BTreeMap<String, (String, String)>> {
    let sql = format!(
        "SELECT event.event_identity,
                COALESCE(event.after_state ->> 'registry_contract_instance_id',
                         event.raw_fact_ref ->> 'emitting_address',
                         event.after_state ->> 'registry'),
                event.after_state ->> 'token_id'
         FROM normalized_events event
         WHERE event.chain_id = $1 AND event.event_identity = ANY($2) AND {}",
        published("$3")
    );
    Ok(
        sqlx::query_as::<_, (String, Option<String>, Option<String>)>(&sql)
            .bind(chain)
            .bind(identities)
            .bind(target)
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

/// A resource's retained lifecycle events rebuilt from the publication-visible log, with their
/// generated ids: None unless the families hold exactly the retained events the log gives the
/// resource, each is at its logged position and filed under the resource, each family row
/// equals its rebuild from the log whole (today's-order reads refold the family rows, so a
/// wrong payload there could otherwise supply the served value), and the log names every
/// generated id.
async fn lapse_evidence(
    pool: &PgPool,
    chain: &str,
    target: i64,
    resource: &str,
) -> Result<Option<(Vec<LifecycleEvent>, BTreeMap<String, i64>)>> {
    let rows: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(event) FROM project_lifecycle_event event
         WHERE event.chain_id = $1 AND event.state_kind = 'resource' AND event.state_key = $2",
    )
    .bind(chain)
    .bind(resource)
    .fetch_all(pool)
    .await?;
    let events: Vec<LifecycleEvent> = rows.iter().filter_map(LifecycleEvent::from_row).collect();
    let family: BTreeSet<String> = events
        .iter()
        .map(|event| event.position.event_identity.clone())
        .collect();
    if family.len() != events.len()
        || !retention::resource_events_hold(pool, chain, target, resource, &family).await?
    {
        return Ok(None);
    }
    let Some(rebuilt) = events_from_log(pool, chain, target, &events).await? else {
        return Ok(None);
    };
    let placed = |event: &LifecycleEvent| {
        event.state_kind == "resource"
            && event.state_key == resource
            && event.resource_id.as_deref() == Some(resource)
    };
    if !rebuilt.iter().all(placed)
        || events
            .iter()
            .zip(&rebuilt)
            .any(|(row, log)| format!("{row:?}") != format!("{log:?}"))
    {
        return Ok(None);
    }
    let events = rebuilt;
    let identities: Vec<String> = family.into_iter().collect();
    let ids = generated_ids(pool, chain, target, &identities).await?;
    if !identities.iter().all(|identity| ids.contains_key(identity)) {
        return Ok(None);
    }
    Ok(Some((events, ids)))
}

/// The cause shown for each differing field of one resource, in `diffs` order. The permissions
/// builder's path-expiry drop (permissions.rs:111-133, :391-398) takes the resource's latest
/// ENSv2 registration event in today's (block, generated id) order. Every lapse below is decided
/// from the resource's retained events rebuilt from the publication-visible log, and only when
/// the families hold exactly the retained events the log gives it (`lapse_evidence`).
///
/// A `permissions_current`, `admin_powers` or `resource_restrictions` field passes as a
/// same-block delta in one direction: the rebuilt events keep the registration live in today's
/// order and lapse it in the canonical order, the served value is not empty, the canonical read
/// is empty, and the whole permission read of the resource taken again from the families in
/// today's order, its registry root's events in today's order too, equals it. That read is compared whole, so a wrong subject, power, collision
/// row or restriction in the families fails. The shadow value is the canonical read itself, so
/// comparing it with the canonical read checks nothing and is not counted as evidence. The
/// other direction, today's order lapsing the registration, is left a mismatch: the read in
/// that order is empty, so matching it would only show that the served value is empty.
///
/// A live registration's restriction block also reads its registry root's admin powers
/// (resource_summary.rs:272-325), which the root's own drop decides. When the resource lapses
/// the same way in both orders and its root lapses in one only, `resource_restrictions` passes
/// when the whole block read in today's order equals the served one, the whole block read in
/// the canonical order through the refolding path (both resources' events ranked canonically,
/// so the stored key states are not read) equals the shadow one, and the two differ. The
/// canonical side is the families read against themselves, so it is no evidence on its own.
/// The child's pass is sound because the rest is checked elsewhere: the child's own content is
/// in both reads and today's must equal the served block, and a wrong root value leaves the
/// root's own `admin_powers` difference a mismatch, which fails the run.
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
    let target = clock.block_number;
    let Some((events, mut ids)) = lapse_evidence(pool, chain, target, &input.resource_id).await?
    else {
        return Ok(out);
    };
    let lapsed = |events: &[LifecycleEvent], order: &EventOrder| {
        registration_lapsed(&maxima_of(events, true, order), order)
    };
    let own = (
        lapsed(&events, &EventOrder::Generated(ids.clone())),
        lapsed(&events, &EventOrder::Canonical),
    );
    let read = |order: EventOrder| async move {
        load_shadow_permissions_in(pool, chain, clock, std::slice::from_ref(input), &order)
            .await
            .map(|mut reads| reads.remove(&input.resource_id).unwrap_or_default())
    };
    let value = |read: &ShadowPermissions, field: &str| match field {
        "permissions_current" => Some(Value::Array(read.grants.iter().map(grant_json).collect())),
        "admin_powers" => Some(json!(read.admin_powers)),
        "resource_restrictions" => Some(read.restrictions.clone().unwrap_or(Value::Null)),
        _ => None,
    };
    if own == (false, true) {
        // The read also refolds the root's admin powers, so the root's events take their
        // generated ids too: today's order for both resources, and no excuse unless the
        // root's retained events also match the log.
        if let Some(root) = input.root_resource_id.as_deref() {
            let Some((_, root_ids)) = lapse_evidence(pool, chain, target, root).await? else {
                return Ok(out);
            };
            ids.extend(root_ids);
        }
        let legacy = read(EventOrder::Generated(ids)).await?;
        let canonical = read(EventOrder::Canonical).await?;
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
        return Ok(out);
    }
    // The restriction block through the root.
    let (Some(root), false) = (input.root_resource_id.as_deref(), own.0 != own.1) else {
        return Ok(out);
    };
    if !diffs
        .iter()
        .any(|diff| diff.field == "resource_restrictions")
    {
        return Ok(out);
    }
    let Some((root_events, root_ids)) = lapse_evidence(pool, chain, target, root).await? else {
        return Ok(out);
    };
    if lapsed(&root_events, &EventOrder::Generated(root_ids.clone()))
        == lapsed(&root_events, &EventOrder::Canonical)
    {
        return Ok(out);
    }
    let mut positions: Vec<Position> = events
        .iter()
        .chain(&root_events)
        .map(|event| event.position.clone())
        .collect();
    positions.sort();
    positions.dedup_by(|left, right| left.event_identity == right.event_identity);
    let ranks: BTreeMap<String, i64> = positions
        .into_iter()
        .enumerate()
        .map(|(rank, position)| (position.event_identity, rank as i64))
        .collect();
    ids.extend(root_ids);
    let legacy = read(EventOrder::Generated(ids)).await?;
    let canonical = read(EventOrder::Generated(ranks)).await?;
    for (index, diff) in diffs.iter().enumerate() {
        if diff.field != "resource_restrictions" {
            continue;
        }
        let (Some(legacy), Some(canonical)) =
            (value(&legacy, &diff.field), value(&canonical, &diff.field))
        else {
            continue;
        };
        if same(&legacy, &diff.served)
            && same(&canonical, &diff.shadow)
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

/// The fixture corpus's counted fields, asserted exactly against `corpus_expectation` read by
/// the same comparison at the same publication (`Options::corpus`). No same-block delta may
/// pass on the corpus.
pub fn assert_fixture_corpus_counts(targets: &[i64]) -> Result<()> {
    let reports = take_reports();
    one_report_per_target(&reports, targets)?;
    for (target, delta, known, expected) in reports {
        anyhow::ensure!(
            delta.is_empty(),
            "target {target}: same-block deltas {delta:?}"
        );
        let expected =
            expected.with_context(|| format!("target {target}: no corpus expectation read"))?;
        anyhow::ensure!(
            known == expected,
            "target {target}: counted {known:?}, the event log gives {expected:?}"
        );
    }
    Ok(())
}

/// The corpus's named-cause counts at `target`, read from its publication-visible event log
/// (`published`, every event reference) without the family readers
/// (crates/project/tests/rebuild_performance/seed.sql), over exactly the names the harness
/// compares: those the serving loader returns, with the resource it serves. An ENSv2 name whose
/// interpreter path-expiry release (the `expired` rows, no name, the token resource) is not
/// followed on that resource by a grant, reservation or named release is served active today
/// and released by the families: eight fields each, and the two control-owner fields again for
/// those whose token was transferred before the target. The oracle is the seed's: other
/// histories are not covered.
pub async fn corpus_expectation(
    pool: &PgPool,
    chain: &str,
    target: i64,
) -> Result<BTreeMap<String, usize>> {
    let keys: Vec<String> = sqlx::query_scalar(NAMES_OF_CHAIN)
        .bind(chain)
        .fetch_all(pool)
        .await?;
    let (mut names, mut resources): (Vec<String>, Vec<String>) = (Vec::new(), Vec::new());
    for chunk in keys.chunks(NAME_CHUNK) {
        for row in load_name_current_by_logical_name_ids(pool, chunk)
            .await?
            .values()
        {
            if let Some(resource) = row.resource_id {
                names.push(row.logical_name_id.clone());
                resources.push(resource.to_string());
            }
        }
    }
    let sql = format!(
        "SELECT name.logical_name_id, EXISTS (
                    SELECT 1 FROM normalized_events transfer
                    WHERE transfer.chain_id = $2
                      AND transfer.resource_id = release.resource_id
                      AND transfer.logical_name_id = name.logical_name_id
                      AND transfer.event_kind = 'TokenControlTransferred'
                      AND {transfer})
         FROM unnest($3::text[], $4::uuid[]) name(logical_name_id, resource_id)
         JOIN normalized_events release ON release.resource_id = name.resource_id
         WHERE release.chain_id = $2
           AND release.logical_name_id IS NULL
           AND release.event_kind = 'RegistrationReleased'
           AND release.after_state ->> 'source_event' = 'RegistryPathExpired'
           AND {release}
           AND NOT EXISTS (
               SELECT 1 FROM normalized_events later
               WHERE later.chain_id = $2
                 AND later.resource_id = release.resource_id
                 AND later.block_number > release.block_number
                 AND {later}
                 AND (later.event_kind IN ('RegistrationGranted', 'RegistrationReserved')
                      OR (later.event_kind = 'RegistrationReleased'
                          AND later.logical_name_id IS NOT NULL)))",
        transfer = published_as("transfer", "$1"),
        release = published_as("release", "$1"),
        later = published_as("later", "$1"),
    );
    let expired: Vec<(String, bool)> = sqlx::query_as(&sql)
        .bind(target)
        .bind(chain)
        .bind(&names)
        .bind(&resources)
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
    Ok(expected)
}
