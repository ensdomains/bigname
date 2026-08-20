use super::*;
use crate::v2::shared_product_reason;
use crate::v2::vocab::MISSING_UNSUPPORTED_REASON;

/// The public reason for a claim whose selected authority has no declared entrypoint to verify
/// through. Distinct from an unsupported exact-name projection, which reports its own reason.
const CLAIM_AUTHORITY_NOT_VERIFIABLE: &str = "exact_name_authority_not_verifiable";

/// The reason forward verification must not run for this address's projected claim, or `None`
/// when the claim may be verified. A claim the exact-name projection does not support, and a
/// claim whose selected authority is an arm this deployment declares no execution entrypoint
/// for, are both answered in band rather than resolved through the superseded ENSv1 authority.
pub(super) async fn unverifiable_claim_authority(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<Option<String>> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    let snapshot =
        bigname_storage::load_primary_name_current_snapshot(pool, address, namespace, &coin_type)
            .await
            .map_err(|error| {
                error!(
                    service = "api",
                    namespace = %namespace,
                    error = ?error,
                    "failed to load the projected primary-name claim"
                );
                ApiError::internal_error("failed to load the projected primary-name claim")
            })?;
    // Only an absent claim leaves the decision to the live reverse leg. A read that failed is not
    // an absent claim, so it is propagated above rather than skipping the gate.
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let Some(claim_name) = snapshot.normalized_claim_name.as_deref() else {
        return Ok(None);
    };
    let logical_name_id = bigname_storage::logical_name_id_for_name(namespace, claim_name);
    let Some(row) = bigname_storage::load_name_current(pool, &logical_name_id)
        .await
        .map_err(|error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?error,
                "failed to load the claimed name's current authority"
            );
            ApiError::internal_error("failed to load the claimed name's current authority")
        })?
    else {
        return Ok(None);
    };

    if crate::v2::name_record::string_field(row.coverage.get("status")).as_deref()
        == Some("unsupported")
    {
        let reason = crate::v2::name_record::string_field(row.coverage.get("unsupported_reason"))
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(|| MISSING_UNSUPPORTED_REASON.to_owned());
        return Ok(Some(
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
    Ok((!verifiable).then(|| CLAIM_AUTHORITY_NOT_VERIFIABLE.to_owned()))
}
