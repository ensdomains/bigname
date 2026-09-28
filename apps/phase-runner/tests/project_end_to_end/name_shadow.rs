//! Name composition comparison (TYR-36 step 7b): after the owned key families have followed a
//! publication, the composed name reader (`bigname_storage::families::name`) must serve, for
//! every name of the chain, the fields the API and the verified lookup read from the served
//! `name_current` row: the identity columns, the coverage column, the registration, control,
//! resolver and coverage blocks with the NameWrapper state and fuses, and the authority
//! selection, read reachability, resolver pointer family and chain of the provenance, and the
//! history heads (`declared_summary.history`) the binding diagnostics route serves, and the
//! complete declared resolution topology consumed by verified lookup and record readback.
//!
//! Not compared, because no route reads them: the whole-history evidence
//! (`selected_event_ids`, `raw_fact_refs`, `manifest_versions`, `registrant_event_id`), the
//! selection's `lifecycle_state`, and the row metadata
//! (`chain_positions`, `canonicality_summary`, `manifest_version`, `last_recomputed_at`), which a
//! composed row takes from the publication. Basenames execution admission and both chain
//! positions are checked by the dedicated cross-chain projection tests.
//!
//! A registration or control field that differs passes, counted as `covered_by_control`, only
//! when the composed authority selection equals the served one: both blocks are then the
//! control shadow read of the same selection, which the control comparison (`shadow.rs`) checks
//! field by field with its named causes, and the harness requires it clean. `created_at` and
//! the lapsed registration's authority are compared here strictly. The resolver block passes the
//! same way, together with absent resolution topology, only when the composed side withholds
//! it because of a registration status that
//! differs and that the control comparison decides (a released or reserved ENSv2 registration
//! serves no resolver, name_current/build.sql:141-169): the one known case is the control
//! comparison's `served_membership_skips_unnamed_path_expiry`. Every other difference, and a name
//! only one side serves, is a mismatch and fails the run.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use anyhow::{Context, Result};
use bigname_storage::{
    NameCurrentRow,
    families::{
        control::{compare::same, lifecycle::AuthoritySelection},
        name::{load_family_name, load_family_names_by_logical_name_ids},
    },
    load_name_current, load_name_current_by_logical_name_ids,
};
use serde_json::{Value, json};
use sqlx::PgPool;

const CHUNK: usize = 500;
const PRINTED: usize = 200;
/// Every name either side can serve at the publication: the served rows of the chain and every
/// readable active surface of the chain at or below the family marker.
const NAMES: &str = "
    SELECT logical_name_id FROM name_current WHERE provenance ->> 'chain_id' = $1
    UNION
    SELECT surface.logical_name_id FROM name_surfaces surface
    JOIN chain_lineage lineage
      ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
    WHERE surface.chain_id = $1 AND surface.block_number <= $2
      AND surface.visibility_state = 'active' AND surface.raw_name <> ''
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    ORDER BY 1";
/// Selection keys the comparison leaves out (no route reads them).
const UNREAD_SELECTION: [&str; 1] = ["lifecycle_state"];

/// What one name comparison saw.
#[derive(Debug, Default)]
pub struct NameReport {
    pub target: i64,
    pub names: usize,
    pub equal: usize,
    /// Registration or control fields left to the control comparison, by field.
    pub covered_by_control: BTreeMap<String, usize>,
    /// Mismatched fields by field, `presence` for a name only one side serves.
    pub mismatched_fields: BTreeMap<String, usize>,
    pub mismatched: usize,
    /// The names with a field left to the control comparison: their served and composed rows
    /// differ by a cause that comparison decides, so a listing that orders or filters by those
    /// fields may place them differently.
    pub covered_names: BTreeSet<String>,
    pub lines: Vec<String>,
    /// Read time in microseconds, served then composed: every name read alone (name detail's
    /// read), and every chunk of names read at once (the batch readers).
    pub single_us: (u128, u128),
    pub batch_us: (u128, u128),
    pub chunks: usize,
}

impl NameReport {
    pub fn print(&self) {
        let per = |total: u128, count: usize| total / (count.max(1) as u128);
        println!(
            "SEPOLIA_END_TO_END_NAME_SHADOW target={} names={} equal={} covered_by_control={:?} \
             mismatched={} mismatched_fields={:?} single_served_us_per_name={} \
             single_composed_us_per_name={} batch_served_us_per_chunk={} \
             batch_composed_us_per_chunk={}",
            self.target,
            self.names,
            self.equal,
            self.covered_by_control,
            self.mismatched,
            self.mismatched_fields,
            per(self.single_us.0, self.names),
            per(self.single_us.1, self.names),
            per(self.batch_us.0, self.chunks),
            per(self.batch_us.1, self.chunks),
        );
        for line in self.lines.iter().take(PRINTED) {
            println!("  {line}");
        }
    }

    pub fn require_clean(&self) -> Result<()> {
        anyhow::ensure!(
            self.mismatched == 0,
            "the composed name rows differ from the served rows at {} for {} names: {:?}; first \
             lines: {:#?}",
            self.target,
            self.mismatched,
            self.mismatched_fields,
            self.lines.iter().take(20).collect::<Vec<_>>()
        );
        Ok(())
    }
}

fn text<T: ToString>(value: Option<T>) -> Value {
    value.map_or(Value::Null, |value| Value::String(value.to_string()))
}

/// The compared projection of one row.
pub fn projection(row: &NameCurrentRow) -> Value {
    let summary = &row.declared_summary;
    let provenance = &row.provenance;
    let mut selection = provenance
        .get("authority_selection")
        .cloned()
        .unwrap_or(Value::Null);
    if let Value::Object(selection) = &mut selection {
        for key in UNREAD_SELECTION {
            selection.remove(key);
        }
    }
    let pick = |value: &Value, key: &str| value.get(key).cloned().unwrap_or(Value::Null);
    json!({
        "columns": {
            "namespace": row.namespace,
            "canonical_display_name": row.canonical_display_name,
            "normalized_name": row.normalized_name,
            "namehash": row.namehash,
            "surface_binding_id": text(row.surface_binding_id),
            "resource_id": text(row.resource_id),
            "serving_resource_id": text(row.serving_resource_id),
            "token_lineage_id": text(row.token_lineage_id),
            "binding_kind": text(row.binding_kind.map(|kind| kind.as_str())),
            "coverage": row.coverage,
        },
        "declared_summary": {
            "registration": pick(summary, "registration"),
            "control": pick(summary, "control"),
            "resolver": pick(summary, "resolver"),
            "topology": pick(summary, "topology"),
            "coverage": pick(summary, "coverage"),
            "wrapper_state": pick(summary, "wrapper_state"),
            "wrapper_fuses": pick(summary, "wrapper_fuses"),
            "history": pick(summary, "history"),
        },
        "provenance": {
            "chain_id": pick(provenance, "chain_id"),
            "authority_selection": selection,
            "read_reachability": pick(provenance, "read_reachability"),
            "resolver_pointer_source_family": pick(provenance, "resolver_pointer_source_family"),
        },
    })
}

/// Every leaf of a JSON object by its `/` path; an array is a leaf.
fn leaves(value: &Value, prefix: &str, out: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}/{key}")
                };
                leaves(value, &path, out);
            }
        }
        other => {
            out.insert(prefix.to_owned(), other.clone());
        }
    }
}

/// The paths whose values differ, a missing key read as null.
pub fn differing(served: &Value, composed: &Value) -> Vec<(String, Value, Value)> {
    let (mut left, mut right) = (BTreeMap::new(), BTreeMap::new());
    leaves(served, "", &mut left);
    leaves(composed, "", &mut right);
    let paths: BTreeSet<&String> = left.keys().chain(right.keys()).collect();
    paths
        .into_iter()
        .filter_map(|path| {
            let served = left.get(path).cloned().unwrap_or(Value::Null);
            let composed = right.get(path).cloned().unwrap_or(Value::Null);
            (!same(&served, &composed)).then(|| (path.clone(), served, composed))
        })
        .collect()
}

/// Whether a differing path is one the control comparison decides.
fn control_path(path: &str) -> bool {
    (path.starts_with("declared_summary/registration/")
        || path.starts_with("declared_summary/control/"))
        && path != "declared_summary/registration/created_at"
        && !path.starts_with("declared_summary/registration/lapsed_registration/authority_")
}

/// Compare every name of `chain` at the publication the families stand on, `target`.
pub async fn compare(pool: &PgPool, chain: &str, target: i64) -> Result<NameReport> {
    let names: Vec<String> = sqlx::query_scalar(NAMES)
        .bind(chain)
        .bind(target)
        .fetch_all(pool)
        .await
        .context("the names to compare")?;
    let mut report = NameReport {
        target,
        ..NameReport::default()
    };
    for chunk in names.chunks(CHUNK) {
        let started = Instant::now();
        let served = load_name_current_by_logical_name_ids(pool, chunk).await?;
        report.batch_us.0 += started.elapsed().as_micros();
        let started = Instant::now();
        let composed = load_family_names_by_logical_name_ids(pool, chunk).await?;
        report.batch_us.1 += started.elapsed().as_micros();
        report.chunks += 1;
        for name in chunk {
            let started = Instant::now();
            load_name_current(pool, name).await?;
            report.single_us.0 += started.elapsed().as_micros();
            let started = Instant::now();
            load_family_name(pool, name).await?;
            report.single_us.1 += started.elapsed().as_micros();
            report.names += 1;
            let (served, composed) = match (served.get(name), composed.get(name)) {
                (None, None) => {
                    report.names -= 1;
                    continue;
                }
                (Some(served), Some(composed)) => (served, composed),
                (served, composed) => {
                    report.mismatched += 1;
                    *report
                        .mismatched_fields
                        .entry("presence".into())
                        .or_default() += 1;
                    report.lines.push(format!(
                        "MISMATCH {name} presence served={} composed={}",
                        served.is_some(),
                        composed.is_some()
                    ));
                    continue;
                }
            };
            let same_selection = AuthoritySelection::from_provenance(&served.provenance)
                == AuthoritySelection::from_provenance(&composed.provenance);
            let diffs = differing(&projection(served), &projection(composed));
            if diffs.is_empty() {
                report.equal += 1;
                continue;
            }
            // The resolver block is withheld for a released or reserved ENSv2 registration
            // (build.sql:141-169), so a resolver difference follows from a registration status
            // the control comparison decides: covered only when that status differs too and
            // the composed side withholds the resolver.
            let status_differs = diffs
                .iter()
                .any(|(path, _, _)| path == "declared_summary/registration/status");
            let withheld = projection(composed)
                .pointer("/declared_summary/resolver/address")
                .is_none_or(Value::is_null);
            // The direct topology requires that resolver too (families/name/topology.rs).
            // Its complete absence is the same consequence of the control-proven release;
            // a present but different topology must still fail this comparison.
            let topology_withheld = composed
                .declared_summary
                .get("topology")
                .is_none_or(Value::is_null);
            let mut mismatched = false;
            for (path, left, right) in diffs {
                let covered = same_selection
                    && (control_path(&path)
                        || (status_differs
                            && withheld
                            && (path.starts_with("declared_summary/resolver/")
                                || (topology_withheld
                                    && path.starts_with("declared_summary/topology/")))));
                let (kind, counter) = if covered {
                    report.covered_names.insert(name.clone());
                    ("COVERED_BY_CONTROL", &mut report.covered_by_control)
                } else {
                    mismatched = true;
                    ("MISMATCH", &mut report.mismatched_fields)
                };
                *counter.entry(path.clone()).or_default() += 1;
                report.lines.push(format!(
                    "{kind} {name} {path} served={left} composed={right}"
                ));
            }
            if mismatched {
                report.mismatched += 1;
            }
        }
    }
    Ok(report)
}
