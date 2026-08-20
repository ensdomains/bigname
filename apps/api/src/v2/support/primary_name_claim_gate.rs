use super::*;
use crate::v2::shared_product_reason;
use crate::v2::vocab::MISSING_UNSUPPORTED_REASON;

/// The public reason for a claim whose selected authority has no declared entrypoint to verify
/// through. Distinct from an unsupported exact-name projection, which reports its own reason.
pub(super) const CLAIM_AUTHORITY_NOT_VERIFIABLE: &str = "exact_name_authority_not_verifiable";

pub(super) enum ForwardGateDecision {
    Admit,
    Refuse(String),
    /// The exact-name projection is not deployed. The indexed path answers this in band rather
    /// than failing, so the verified path degrades the same way instead of resolving a name whose
    /// authority it cannot check.
    ProjectionUnavailable,
}

/// Whether forward verification may run for this address's projected claim. A claim the exact-name
/// projection does not support, and a claim whose selected authority is an arm this deployment
/// declares no execution entrypoint for, are both answered in band rather than resolved through
/// the superseded ENSv1 authority.
pub(super) async fn unverifiable_claim_authority(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<ForwardGateDecision> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    let snapshot = match bigname_storage::load_primary_name_current_snapshot(
        pool, address, namespace, &coin_type,
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) if projection_unavailable(&error) => {
            return Ok(ForwardGateDecision::ProjectionUnavailable);
        }
        Err(error) => {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?error,
                "failed to load the projected primary-name claim"
            );
            return Err(ApiError::internal_error(
                "failed to load the projected primary-name claim",
            ));
        }
    };
    // Only an absent claim leaves the decision to the live reverse leg. A read that failed is not
    // an absent claim, so it is propagated above rather than skipping the gate.
    let Some(snapshot) = snapshot else {
        return Ok(ForwardGateDecision::Admit);
    };
    let Some(claim_name) = snapshot.normalized_claim_name.as_deref() else {
        return Ok(ForwardGateDecision::Admit);
    };
    unverifiable_name_authority(pool, namespace, claim_name).await
}

/// Whether forward verification may run for a specific name, whatever named it. The live reverse
/// leg reaches the same question as a projected claim does, so both gates share this decision.
pub(super) async fn unverifiable_name_authority(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> ApiResult<ForwardGateDecision> {
    let logical_name_id = bigname_storage::logical_name_id_for_name(namespace, name);
    let row = match bigname_storage::load_name_current(pool, &logical_name_id).await {
        Ok(row) => row,
        Err(error) if projection_unavailable(&error) => {
            return Ok(ForwardGateDecision::ProjectionUnavailable);
        }
        Err(error) => {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?error,
                "failed to load the claimed name's current authority"
            );
            return Err(ApiError::internal_error(
                "failed to load the claimed name's current authority",
            ));
        }
    };
    let Some(row) = row else {
        return Ok(ForwardGateDecision::Admit);
    };

    if crate::v2::name_record::string_field(row.coverage.get("status")).as_deref()
        == Some("unsupported")
    {
        let reason = crate::v2::name_record::string_field(row.coverage.get("unsupported_reason"))
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(|| MISSING_UNSUPPORTED_REASON.to_owned());
        return Ok(ForwardGateDecision::Refuse(
            shared_product_reason(
                &reason,
                "rejected exact-name reason containing pipeline vocabulary",
                "failed to map exact-name reason vocabulary",
            )
            .map_err(|error| {
                error!(
                    service = "api",
                    namespace = %namespace,
                    error = ?error,
                    "failed to map exact-name reason vocabulary"
                );
                ApiError::internal_error("failed to map exact-name reason vocabulary")
            })?,
        ));
    }

    // No manifest declares an execution entrypoint for any arm but ENSv1, so this deployment has
    // no forward-resolution path for a name whose selected authority is a later arm. We decline
    // rather than resolve such a name through the ENSv1 entrypoint, whose answer our own authority
    // selection has already ruled out as the current one.
    let verifiable = row
        .provenance
        .pointer("/authority_selection/authority_arm")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|arm| arm == "ens_v1");
    Ok(if verifiable {
        ForwardGateDecision::Admit
    } else {
        ForwardGateDecision::Refuse(CLAIM_AUTHORITY_NOT_VERIFIABLE.to_owned())
    })
}

fn projection_unavailable(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<sqlx::Error>()
        .is_some_and(super::primary_name_projection_sqlx_unavailable)
}
