//! Shared by the `families_shadow_*` fixture tests (TYR-36 step 3, docs/glossary.md "Shadow
//! read"): publish a block with the production batch, follow it with the owned key families, then
//! read the same name or resource through the production reader and through the family readers
//! of `bigname_storage::families::control`. The whole-publication comparison is the harness
//! module the Sepolia end-to-end run uses, so both levels compare the same fields the same way.
#![allow(dead_code)]

#[path = "../../../../apps/phase-runner/tests/project_end_to_end/shadow.rs"]
pub mod compare;
pub mod wrapper;

use anyhow::{Result, ensure};
use bigname_project::{BatchRequest, Engine, RunMode, families::FamilyMode};
use bigname_storage::{
    families::control::lifecycle::{
        AuthoritySelection, Clock, NameInput, ShadowName, load_shadow_names,
    },
    load_name_current_by_logical_name_ids,
};
use serde_json::Value;

use crate::support::{CHAIN, Fixture};

/// Publish `target` with the production batch, then apply the families to it.
pub async fn publish(fixture: &Fixture, target: i64) -> Result<()> {
    Engine::new(fixture.pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: 0,
            affected_to_block: target,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let outcome = fixture.apply(target, FamilyMode::Normal).await;
    ensure!(
        outcome.skipped.is_none(),
        "the families followed {target}: {:?}",
        outcome.skipped
    );
    Ok(())
}

/// Publish `target` and compare every served name, resource and account with its shadow read.
pub async fn publish_and_compare(fixture: &Fixture, target: i64) -> Result<compare::Report> {
    publish(fixture, target).await?;
    let report = compare::compare(&fixture.pool, CHAIN, target).await?;
    report.print(target);
    Ok(report)
}

/// One name as served: its declared summary and the provenance the selection reads.
pub struct Served {
    pub summary: Value,
    pub provenance: Value,
    pub resource_id: Option<String>,
}

impl Served {
    pub fn registration(&self, field: &str) -> Value {
        self.summary["registration"][field].clone()
    }

    pub fn control(&self, field: &str) -> Value {
        self.summary["control"][field].clone()
    }
}

/// The production row of `logical_name_id` and the family read of the same name at `target`.
pub async fn name(
    fixture: &Fixture,
    target: i64,
    logical_name_id: &str,
) -> Result<(Served, ShadowName)> {
    let rows =
        load_name_current_by_logical_name_ids(&fixture.pool, &[logical_name_id.to_owned()]).await?;
    let row = rows
        .get(logical_name_id)
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no name_current row"))?;
    let timestamp: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM block_timestamp)::bigint FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(target)
    .fetch_one(&fixture.pool)
    .await?;
    let input = NameInput {
        logical_name_id: row.logical_name_id.clone(),
        namehash: row.namehash.to_ascii_lowercase(),
        selection: AuthoritySelection::from_provenance(&row.provenance),
    };
    let clock = Clock {
        block_number: target,
        timestamp_seconds: timestamp,
    };
    let mut shadows = load_shadow_names(&fixture.pool, CHAIN, &clock, &[input]).await?;
    let shadow = shadows
        .remove(logical_name_id)
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no shadow read"))?;
    Ok((
        Served {
            summary: row.declared_summary.clone(),
            provenance: row.provenance.clone(),
            resource_id: row.resource_id.map(|id| id.to_string()),
        },
        shadow,
    ))
}
