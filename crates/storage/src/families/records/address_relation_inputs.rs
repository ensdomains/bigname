//! Shared inputs for the current address relations used by reads and Project publication.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, Row};

use super::{
    FamilyPosition,
    address_relations::{ControllerCandidate, NameRelationsInput},
    address_roles::{RoleHolder, RoleHolderLoad, role_holders},
};
use crate::{
    NameCurrentRow,
    families::control::{
        rows::{BindingCandidate, WrapperRow},
        wrapper::load_wrapper_rows,
    },
};

/// The per-chain family rows the relations of a batch of composed names read.
pub(super) struct ChainInputs {
    candidates: BTreeMap<String, Vec<ControllerCandidate>>,
    bindings: BTreeMap<String, BindingCandidate>,
    wrappers: BTreeMap<String, WrapperRow>,
    role_holders: BTreeMap<String, Vec<RoleHolder>>,
}

impl ChainInputs {
    pub(super) async fn load<'a>(
        conn: &mut PgConnection,
        chain_id: &str,
        composed: impl IntoIterator<Item = &'a NameCurrentRow>,
        clock_seconds: i64,
        roles: RoleHolderLoad<'_>,
    ) -> Result<Self> {
        let composed: Vec<&NameCurrentRow> = composed.into_iter().collect();
        let ids: Vec<String> = composed
            .iter()
            .map(|row| row.logical_name_id.clone())
            .collect();
        let rows = sqlx::query(
            "/* storage:families.records.address_controller_candidates */
             SELECT logical_name_id, block_number, transaction_index, log_index, event_identity,
                    resource_id::text AS resource_id, event_kind, source_family, action, subject
             FROM bigname_phase.project_address_controller_candidate
             WHERE chain_id = $1 AND logical_name_id = ANY($2)",
        )
        .bind(chain_id)
        .bind(&ids)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load the address controller candidates")?;
        let mut candidates: BTreeMap<String, Vec<ControllerCandidate>> = BTreeMap::new();
        for row in &rows {
            let candidate = ControllerCandidate {
                logical_name_id: row.try_get("logical_name_id")?,
                position: FamilyPosition::from_row(row)?,
                resource_id: row.try_get("resource_id")?,
                event_kind: row.try_get("event_kind")?,
                source_family: row.try_get("source_family")?,
                set: row.try_get::<String, _>("action")? == "set",
                subject: row.try_get("subject")?,
            };
            candidates
                .entry(candidate.logical_name_id.clone())
                .or_default()
                .push(candidate);
        }
        let selected: Vec<_> = composed
            .iter()
            .filter_map(|row| row.surface_binding_id)
            .collect();
        let bindings: Vec<Value> = sqlx::query_scalar(
            "/* storage:families.records.address_selected_bindings */
             SELECT to_jsonb(candidate) FROM bigname_phase.project_binding_candidate candidate
             WHERE candidate.chain_id = $1 AND candidate.surface_binding_id = ANY($2::uuid[])",
        )
        .bind(chain_id)
        .bind(&selected)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load the selected binding candidates")?;
        let bindings = bindings
            .iter()
            .filter_map(BindingCandidate::from_row)
            .map(|binding| (binding.surface_binding_id.clone(), binding))
            .collect();
        let resources: Vec<String> = composed
            .iter()
            .filter_map(|row| row.resource_id.map(|id| id.to_string()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let wrappers = load_wrapper_rows(&mut *conn, chain_id, &resources)
            .await?
            .into_iter()
            .map(|wrapper| (wrapper.resource_id.clone(), wrapper))
            .collect();
        let role_holders = role_holders(
            &mut *conn,
            chain_id,
            &resources,
            &wrappers,
            clock_seconds,
            roles,
        )
        .await?;
        Ok(Self {
            candidates,
            bindings,
            wrappers,
            role_holders,
        })
    }

    /// The relations input of one composed name of the batch.
    pub(super) fn input<'a>(
        &'a self,
        row: &'a NameCurrentRow,
        clock_seconds: i64,
    ) -> NameRelationsInput<'a> {
        let resource = row.resource_id.map(|resource| resource.to_string());
        NameRelationsInput {
            row,
            candidates: self
                .candidates
                .get(&row.logical_name_id)
                .map_or(&[][..], Vec::as_slice),
            binding: self.selected_binding(row),
            wrapper: resource
                .as_ref()
                .and_then(|resource| self.wrappers.get(resource)),
            clock_seconds,
            role_holders: resource
                .as_ref()
                .and_then(|resource| self.role_holders.get(resource))
                .map_or(&[][..], Vec::as_slice),
        }
    }

    fn selected_binding(&self, row: &NameCurrentRow) -> Option<&BindingCandidate> {
        self.bindings
            .get(&row.surface_binding_id?.to_string())
            .filter(|binding| binding.logical_name_id == row.logical_name_id)
    }
}
