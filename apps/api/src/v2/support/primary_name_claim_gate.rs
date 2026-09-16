use super::*;
use crate::v2::vocab::{MISSING_UNSUPPORTED_REASON, projected_row_product_reason};

/// The public reason for a claim whose selected authority arm the deployment's `ens_execution`
/// declaration does not admit (`docs/manifests.md` § `verified_authority_arms`), or for a
/// supported row missing its arm. Distinct from an unsupported exact-name projection, which
/// reports its own reason.
pub(super) const CLAIM_AUTHORITY_NOT_VERIFIABLE: &str =
    crate::v2::name_records::EXACT_NAME_AUTHORITY_NOT_VERIFIABLE;

pub(super) enum ForwardGateDecision {
    Admit,
    Refuse(String),
    /// The exact-name projection is not deployed. The indexed path answers this in band rather
    /// than failing, so the verified path degrades the same way instead of resolving a name whose
    /// authority it cannot check.
    ProjectionUnavailable,
}

/// Whether forward verification may run for this address's projected claim. A claim the exact-name
/// projection does not support, and a claim whose selected authority is an arm the deployment's
/// execution declaration does not admit, are both answered in band rather than resolved through
/// an entrypoint the name's own authority selection has ruled out.
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
        return Ok(ForwardGateDecision::Refuse(projected_row_product_reason(
            &reason,
            "rejected exact-name reason containing pipeline vocabulary",
            "failed to map exact-name reason vocabulary",
        )));
    }

    // The selected `ens_execution` manifest declares which authority arms its Universal Resolver
    // can answer for. A name whose selected arm is outside that declaration has no forward path
    // here; we decline rather than resolve it through an entrypoint whose answer our own authority
    // selection has already ruled out as the current one.
    let Some(authority_arm) = row
        .provenance
        .pointer("/authority_selection/authority_arm")
        .and_then(serde_json::Value::as_str)
    else {
        // Unlike an absent row, a present supported row must carry the projected authority choice.
        // Missing selection provenance is a projection anomaly, so forward verification fails
        // closed instead of silently using the entrypoint.
        return Ok(ForwardGateDecision::Refuse(
            CLAIM_AUTHORITY_NOT_VERIFIABLE.to_owned(),
        ));
    };
    let lookup_chain_id = ens_primary_name_lookup_chain(pool, namespace).await?;
    let admitted_arms =
        match bigname_lookup::admitted_verified_authority_arms(pool, &lookup_chain_id).await {
            Ok(arms) => arms,
            // No declared entrypoint at all: nothing is verifiable, and the live lookup would
            // report the same in its own vocabulary, so decline in band before dispatching.
            Err(error) if error.kind() == bigname_lookup::ErrorKind::Unsupported => {
                return Ok(ForwardGateDecision::Refuse(
                    CLAIM_AUTHORITY_NOT_VERIFIABLE.to_owned(),
                ));
            }
            Err(error) => return Err(admitted_arms_error(namespace, error)),
        };
    Ok(if admitted_arms.iter().any(|arm| arm == authority_arm) {
        ForwardGateDecision::Admit
    } else {
        ForwardGateDecision::Refuse(CLAIM_AUTHORITY_NOT_VERIFIABLE.to_owned())
    })
}

fn admitted_arms_error(namespace: &str, error: bigname_lookup::LookupError) -> ApiError {
    warn!(
        service = "api",
        namespace = %namespace,
        error_kind = ?error.kind(),
        error = %error.message(),
        "failed to read the admitted verified authority arms"
    );
    match error.kind() {
        bigname_lookup::ErrorKind::Configuration
        | bigname_lookup::ErrorKind::Stale
        | bigname_lookup::ErrorKind::ConcurrentState => ApiError {
            status: StatusCode::CONFLICT,
            code: "stale",
            message: "verified primary-name lookup must be retried".to_owned(),
        },
        bigname_lookup::ErrorKind::Unsupported
        | bigname_lookup::ErrorKind::Transport
        | bigname_lookup::ErrorKind::Execution
        | bigname_lookup::ErrorKind::Database => {
            ApiError::internal_error("failed to read the admitted verified authority arms")
        }
    }
}

fn projection_unavailable(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<sqlx::Error>()
        .is_some_and(super::primary_name_projection_sqlx_unavailable)
}
