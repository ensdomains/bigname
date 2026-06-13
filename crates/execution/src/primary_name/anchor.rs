use anyhow::{Context, Result, bail};
use bigname_storage::{PrimaryNameClaimStatus, load_primary_name_current_snapshot};
use sqlx::PgPool;

use super::context::verified_primary_context_label;
use super::{VerifiedPrimaryNameSection, VerifiedPrimaryNameStatus, VerifiedPrimaryNameTuple};

pub(crate) async fn ensure_primary_name_anchor_matches(
    pool: &PgPool,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    let Some(snapshot) = load_primary_name_current_snapshot(
        pool,
        &tuple.normalized_address,
        &tuple.namespace,
        &tuple.coin_type,
    )
    .await?
    else {
        bail!(
            "{context} persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type
        );
    };

    let expected_claim_status = match verified_primary_name.status {
        VerifiedPrimaryNameStatus::Success
        | VerifiedPrimaryNameStatus::Mismatch
        | VerifiedPrimaryNameStatus::ExecutionFailed => PrimaryNameClaimStatus::Success,
        VerifiedPrimaryNameStatus::NotFound => PrimaryNameClaimStatus::NotFound,
        VerifiedPrimaryNameStatus::InvalidName => PrimaryNameClaimStatus::InvalidName,
    };
    if snapshot.row.claim_status != expected_claim_status {
        bail!(
            "{context} persistence claim content changed for address {} namespace {} coin_type {}: current claim_status {} does not match verified status {:?}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type,
            snapshot.row.claim_status.as_str(),
            verified_primary_name.status
        );
    }

    if matches!(
        verified_primary_name.status,
        VerifiedPrimaryNameStatus::Success | VerifiedPrimaryNameStatus::Mismatch
    ) {
        let Some(current_claim_name) = snapshot.normalized_claim_name.as_deref() else {
            return Ok(());
        };
        let verified_claim_name = verified_primary_name
            .section
            .get("name")
            .and_then(|name| name.get("normalized_name"))
            .and_then(serde_json::Value::as_str)
            .context("verified-primary success/mismatch payload missing name.normalized_name")?;
        if current_claim_name != verified_claim_name {
            bail!(
                "{context} persistence claim content changed for address {} namespace {} coin_type {}: current claim name {} does not match verified claim name {}",
                tuple.normalized_address,
                tuple.namespace,
                tuple.coin_type,
                current_claim_name,
                verified_claim_name
            );
        }
    }

    Ok(())
}
