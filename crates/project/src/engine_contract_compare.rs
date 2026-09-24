//! Separate contract oracle: exact old output unless independently proven untouched.
//! Original rows and all meaningful timestamps remain intact in private snapshots.
use anyhow::{Result, bail, ensure};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use std::collections::BTreeSet;

#[cfg(test)]
#[path = "engine_contract_compare_audit.rs"]
pub(crate) mod audit;
#[cfg(test)]
#[path = "engine_contract_compare_snapshot.rs"]
mod snapshot;
pub(super) use snapshot::{Expectations, Snapshot, Snapshots, assert_same_output, compare};

pub(super) const TABLES: &[(&str, &str)] = &[
    ("name_current", "logical_name_id"),
    (
        "children_current",
        "parent_logical_name_id,child_logical_name_id,surface_class",
    ),
    ("permissions_current", "resource_id,subject,scope"),
    (
        "account_permission_state_current",
        "chain_id,authority_kind,authority_contract,owner,subject,relation_kind",
    ),
    ("permissions_current_resource_summary", "resource_id"),
    (
        "record_inventory_current",
        "resource_id,record_version_boundary_key",
    ),
    ("resolver_current", "chain_id,resolver_address"),
    ("address_names_current", "address,logical_name_id,relation"),
    (
        "address_records_current",
        "address,coin_type,logical_name_id",
    ),
    ("primary_names_current", "address,coin_type,namespace"),
    (
        "child_registration_events",
        "parent_logical_name_id,event_identity",
    ),
];

#[derive(Default)]
pub(super) struct Scopes {
    names: BTreeSet<String>,
    resources: BTreeSet<String>,
    children: BTreeSet<String>,
    resolvers: BTreeSet<String>,
    accounts: BTreeSet<Vec<String>>,
    primary: BTreeSet<Vec<String>>,
    /// The batch's affected blocks: child registration rows are replaced by block, not by key.
    window: (i64, i64),
    audit_inputs: Value,
}
impl Scopes {
    fn evidence(&self) -> Value {
        json!({"names":self.names,"resources":self.resources,"children":self.children,"resolvers":self.resolvers,"accounts":self.accounts,"primary":self.primary,"window":self.window,"audit_inputs":self.audit_inputs})
    }

    pub(super) async fn capture(
        tx: &mut Transaction<'_, Postgres>,
        window: (i64, i64),
    ) -> Result<Self> {
        async fn single(
            tx: &mut Transaction<'_, Postgres>,
            table: &str,
            column: &str,
        ) -> Result<BTreeSet<String>> {
            Ok(
                sqlx::query_scalar::<_, String>(&format!("SELECT {column}::text FROM {table}"))
                    .fetch_all(&mut **tx)
                    .await?
                    .into_iter()
                    .collect(),
            )
        }
        async fn tuples(
            tx: &mut Transaction<'_, Postgres>,
            table: &str,
            columns: &str,
        ) -> Result<BTreeSet<Vec<String>>> {
            Ok(sqlx::query_scalar::<_, Vec<String>>(&format!(
                "SELECT ARRAY[{columns}]::text[] FROM {table}"
            ))
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .collect())
        }
        let has_audit: bool = sqlx::query_scalar(
            "SELECT to_regclass('pg_temp.project_contract_audit_changed_nodes') IS NOT NULL",
        )
        .fetch_one(&mut **tx)
        .await?;
        let audit_inputs = if has_audit {
            let changed:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(row) FROM project_contract_audit_changed_nodes row ORDER BY namespace,namehash,resource_id").fetch_all(&mut **tx).await?;
            json!({"changed_nodes":changed,"names":single(tx,"project_contract_audit_input_names","logical_name_id").await?,"resources":single(tx,"project_contract_audit_input_resources","resource_id").await?})
        } else {
            Value::Null
        };
        Ok(Self {
            window,
            audit_inputs,
            names: single(tx, "project_scope_names", "logical_name_id").await?,
            resources: single(tx, "project_scope_resources", "resource_id").await?,
            children: single(tx, "project_scope_children", "logical_name_id").await?,
            resolvers: single(tx, "project_scope_resolvers", "lower(resolver_address)").await?,
            accounts: tuples(
                tx,
                "project_scope_account_permissions",
                "chain_id,authority_kind,authority_contract,owner,subject,relation_kind",
            )
            .await?,
            primary: tuples(tx, "project_scope_primary", "address,coin_type,namespace").await?,
        })
    }
    fn owns(&self, table: &str, row: &Value) -> Result<bool> {
        let member = |set: &BTreeSet<String>, field: &str| {
            row.get(field)
                .and_then(Value::as_str)
                .is_some_and(|s| set.contains(s))
        };
        let tuple = |fields: &str| -> Result<Vec<String>> {
            fields
                .split(',')
                .map(|f| {
                    row[f]
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| anyhow::anyhow!("missing or nontext composite key {f}"))
                })
                .collect()
        };
        Ok(match table {
            "name_current" => member(&self.names, "logical_name_id"),
            "children_current" => {
                member(&self.children, "parent_logical_name_id")
                    || member(&self.children, "child_logical_name_id")
            }
            "permissions_current"
            | "permissions_current_resource_summary"
            | "record_inventory_current" => member(&self.resources, "resource_id"),
            "account_permission_state_current" => self.accounts.contains(&tuple(
                "chain_id,authority_kind,authority_contract,owner,subject,relation_kind",
            )?),
            "primary_names_current" => self
                .primary
                .contains(&tuple("address,coin_type,namespace")?),
            "address_names_current" => {
                member(&self.names, "logical_name_id") || member(&self.resources, "resource_id")
            }
            "address_records_current" => {
                member(&self.names, "logical_name_id")
                    || member(&self.resources, "resource_id")
                    || member(&self.resources, "record_resource_id")
            }
            // The publisher deletes the affected block range (and non-canonical rows above it)
            // and republishes it, whatever names the rows belong to.
            "child_registration_events" => {
                let block = row["block_number"]
                    .as_i64()
                    .ok_or_else(|| anyhow::anyhow!("missing child registration block"))?;
                (self.window.0..=self.window.1).contains(&block)
            }
            "resolver_current" => {
                let address = row["resolver_address"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("missing resolver address"))?;
                ensure!(
                    address.is_ascii(),
                    "non-ASCII resolver folding is unsupported by contract audit"
                );
                row["chain_id"] == "ethereum-sepolia"
                    && self.resolvers.contains(&address.to_ascii_lowercase())
            }
            _ => bail!("unknown projection family"),
        })
    }
}

fn meaningful(row: &Value) -> Value {
    let mut result = row.clone();
    if let Some(obj) = result.as_object_mut() {
        obj.remove("inserted_at");
        obj.remove("last_recomputed_at");
    }
    result
}

pub(super) struct Target {
    number: i64,
    hash: String,
    timestamp: Value,
}
impl Target {
    pub(super) async fn load(
        tx: &mut Transaction<'_, Postgres>,
        marker: &crate::Marker,
    ) -> Result<Self> {
        let timestamp:Value=sqlx::query_scalar("SELECT to_jsonb(block_timestamp) FROM chain_lineage WHERE chain_id='ethereum-sepolia' AND block_number=$1 AND block_hash=$2 AND canonicality_state IN ('canonical','safe','finalized')")
            .bind(marker.number).bind(&marker.hash).fetch_one(&mut **tx).await?;
        Ok(Self {
            number: marker.number,
            hash: marker.hash.clone(),
            timestamp,
        })
    }
    fn refresh(&self, table: &str, baseline: &Value) -> Result<Value> {
        fn replace(row: &mut Value, path: &str, value: Value) -> Result<()> {
            let slot = row
                .pointer_mut(path)
                .ok_or_else(|| anyhow::anyhow!("unsupported target metadata shape at {path}"))?;
            ensure!(!slot.is_null(), "null target metadata at {path}");
            *slot = value;
            Ok(())
        }
        let mut transformed = meaningful(baseline);
        if table == "child_registration_events" {
            replace(&mut transformed, "/target_block_number", json!(self.number))?;
            replace(&mut transformed, "/target_block_hash", json!(self.hash))?;
        } else if table == "primary_names_current" {
            replace(
                &mut transformed,
                "/claim_provenance/target_block_number",
                json!(self.number),
            )?;
            replace(
                &mut transformed,
                "/claim_provenance/target_block_hash",
                json!(self.hash),
            )?;
        } else {
            let prefix = if table == "name_current" {
                "/chain_positions/ethereum-sepolia"
            } else {
                "/chain_positions"
            };
            let number = if table == "name_current" {
                "block_number"
            } else {
                "target_block_number"
            };
            let hash = if table == "name_current" {
                "block_hash"
            } else {
                "target_block_hash"
            };
            replace(
                &mut transformed,
                &format!("{prefix}/{number}"),
                json!(self.number),
            )?;
            replace(
                &mut transformed,
                &format!("{prefix}/{hash}"),
                json!(self.hash),
            )?;
            if table == "name_current" {
                replace(
                    &mut transformed,
                    &format!("{prefix}/timestamp"),
                    self.timestamp.clone(),
                )?;
            }
            replace(
                &mut transformed,
                "/canonicality_summary/target_block_number",
                json!(self.number),
            )?;
            replace(
                &mut transformed,
                "/canonicality_summary/target_block_hash",
                json!(self.hash),
            )?;
        }
        Ok(transformed)
    }
}

#[derive(Debug, PartialEq)]
enum Verdict {
    Exact,
    Retained,
}
fn compare_row(
    table: &str,
    b: Option<&Value>,
    c: Option<&Value>,
    r: Option<&Value>,
    mandatory: &Scopes,
    old_scope: &Scopes,
    target: &Target,
) -> Result<Verdict> {
    let required = [b, c, r]
        .into_iter()
        .flatten()
        .map(|row| mandatory.owns(table, row))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .any(|owned| owned);
    let legacy_owned = [b, c, r]
        .into_iter()
        .flatten()
        .map(|row| old_scope.owns(table, row))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .any(|owned| owned);
    if !required && legacy_owned {
        let (Some(base), Some(candidate), Some(reference)) = (b, c, r) else {
            bail!(
                "absence/deletion outside mandatory scope requires an unsupported independent absence proof"
            );
        };
        ensure!(
            target.refresh(table, base)? == meaningful(reference),
            "legacy-only row differs beyond documented target refresh; independent content proof required"
        );
        ensure!(
            candidate == base,
            "independently read-only legacy-owned row was unnecessarily refreshed"
        );
        for row in [base, candidate, reference] {
            ensure!(
                old_scope.owns(table, row)?,
                "read-only row changed publication ownership"
            );
        }
        return Ok(Verdict::Retained);
    }

    if !required && !legacy_owned {
        ensure!(
            b == c && b == r,
            "row outside both publication scopes did not remain byte-equal to baseline"
        );
        return Ok(Verdict::Exact);
    }
    if c.map(meaningful) == r.map(meaningful) {
        return Ok(Verdict::Exact);
    }

    bail!("candidate differs from exact reference on required or unclassified output")
}

#[cfg(test)]
#[path = "engine_contract_compare_tests.rs"]
mod tests;
