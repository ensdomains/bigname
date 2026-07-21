use anyhow::{Context, Result, bail};
use bigname_storage::{PrimaryNameClaimStatus, load_primary_name_current_snapshot};
use sqlx::{PgPool, Postgres, Transaction};

use super::context::verified_primary_context_label;
use super::{VerifiedPrimaryNameSection, VerifiedPrimaryNameStatus, VerifiedPrimaryNameTuple};
use crate::VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON;

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

    ensure_primary_name_anchor_content_matches(
        context,
        tuple,
        verified_primary_name,
        snapshot.row.claim_status.as_str(),
        snapshot.normalized_claim_name.as_deref(),
        snapshot.claim_name_is_normalized,
    )
}

pub(crate) async fn ensure_primary_name_anchor_absent(
    pool: &PgPool,
    tuple: &VerifiedPrimaryNameTuple,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if load_primary_name_current_snapshot(
        pool,
        &tuple.normalized_address,
        &tuple.namespace,
        &tuple.coin_type,
    )
    .await?
    .is_some()
    {
        bail!(
            "{context} route-local persistence requires no primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type
        );
    }
    Ok(())
}

pub(crate) async fn ensure_primary_name_anchor_matches_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    let Some(anchor) = sqlx::query_as::<_, LockedPrimaryNameAnchor>(
        r#"
        SELECT claim_status, normalized_claim_name, claim_name_is_normalized
        FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        FOR UPDATE
        "#,
    )
    .bind(&tuple.normalized_address)
    .bind(&tuple.namespace)
    .bind(&tuple.coin_type)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to lock primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address, tuple.namespace, tuple.coin_type
        )
    })?
    else {
        bail!(
            "{context} persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type
        );
    };

    ensure_primary_name_anchor_content_matches(
        context,
        tuple,
        verified_primary_name,
        &anchor.claim_status,
        anchor.normalized_claim_name.as_deref(),
        anchor.claim_name_is_normalized,
    )
}

pub(crate) async fn ensure_primary_name_anchor_absent_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    tuple: &VerifiedPrimaryNameTuple,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    // PostgreSQL cannot row-lock a tuple that does not exist. Hold a short SHARE
    // table lock so projection inserts/updates wait until the absence check and
    // execution trace commit as one serialized decision.
    sqlx::query("LOCK TABLE primary_names_current IN SHARE MODE")
        .execute(&mut **transaction)
        .await
        .context("failed to lock primary_names_current for route-local persistence")?;
    let anchor_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM primary_names_current
            WHERE address = $1
              AND namespace = $2
              AND coin_type = $3
            FOR UPDATE
        )
        "#,
    )
    .bind(&tuple.normalized_address)
    .bind(&tuple.namespace)
    .bind(&tuple.coin_type)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to check primary_names_current route-local anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address, tuple.namespace, tuple.coin_type
        )
    })?;
    if anchor_exists {
        bail!(
            "{context} route-local persistence requires no primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type
        );
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct LockedPrimaryNameAnchor {
    claim_status: String,
    normalized_claim_name: Option<String>,
    claim_name_is_normalized: bool,
}

pub(crate) fn ensure_primary_name_anchor_content_matches(
    context: &str,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
    current_claim_status: &str,
    current_normalized_claim_name: Option<&str>,
    current_claim_name_is_normalized: bool,
) -> Result<()> {
    let is_claim_not_normalized = verified_primary_name.status
        == VerifiedPrimaryNameStatus::InvalidName
        && verified_primary_name
            .section
            .get("failure_reason")
            .and_then(serde_json::Value::as_str)
            == Some(VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON);
    let expected_claim_status = match verified_primary_name.status {
        VerifiedPrimaryNameStatus::Success
        | VerifiedPrimaryNameStatus::Mismatch
        | VerifiedPrimaryNameStatus::NotFound
        | VerifiedPrimaryNameStatus::ExecutionFailed => PrimaryNameClaimStatus::Success.as_str(),
        VerifiedPrimaryNameStatus::InvalidName if is_claim_not_normalized => {
            PrimaryNameClaimStatus::Success.as_str()
        }
        VerifiedPrimaryNameStatus::InvalidName => PrimaryNameClaimStatus::InvalidName.as_str(),
    };
    if current_claim_status != expected_claim_status {
        bail!(
            "{context} persistence claim content changed for address {} namespace {} coin_type {}: current claim_status {} does not match verified status {:?}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type,
            current_claim_status,
            verified_primary_name.status
        );
    }

    if current_claim_status == PrimaryNameClaimStatus::Success.as_str()
        && current_claim_name_is_normalized == is_claim_not_normalized
    {
        bail!(
            "{context} persistence claim normalization changed for address {} namespace {} coin_type {}: claim_name_is_normalized is {} but verified failure_reason is {}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type,
            current_claim_name_is_normalized,
            if is_claim_not_normalized {
                VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON
            } else {
                "not claim_not_normalized"
            }
        );
    }

    // Payload validation keeps name absent for not_found, invalid_name, and execution_failed.
    // Those statuses can only be fenced by claim existence/status here; full name fencing for
    // stale error outcomes would require carrying the claimed name in the verified-primary
    // error payload contract.
    if matches!(
        verified_primary_name.status,
        VerifiedPrimaryNameStatus::Success | VerifiedPrimaryNameStatus::Mismatch
    ) {
        let Some(current_claim_name) = current_normalized_claim_name else {
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
