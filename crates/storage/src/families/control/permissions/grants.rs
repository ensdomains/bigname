//! The served permission rows of one resource from its raw F8 grants: the NameWrapper fuse
//! masks and `.eth` grace at the publication clock (permissions.rs:310-380), the empty-row drop
//! (:390), the ENSv2 path-expiry drop (:391-398) read from the F2a key state, and the wrapper
//! operator fan-out with its collision rule (wrapper_operators.rs:21-137).
use serde_json::{Map, Value, json};

use crate::families::control::{
    lifecycle::view::registration_lapsed,
    position::Position,
    rows::{Maxima, WrapperRow, json as column, text},
    wrapper::effective_wrapper,
};

/// One `project_grant` row.
#[derive(Clone, Debug)]
pub struct GrantRow {
    pub resource_id: String,
    pub subject: String,
    pub scope: String,
    pub position: Position,
    pub scope_kind: Option<String>,
    pub scope_detail: Value,
    pub effective_powers: Value,
    pub grant_source: Value,
    pub revocation_source: Value,
    pub inheritance_path: Value,
    pub transfer_behavior: Value,
}

impl GrantRow {
    pub(crate) fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            resource_id: text(row, "resource_id")?,
            subject: text(row, "subject")?,
            scope: text(row, "scope")?,
            position: Position::of_row(row)?,
            scope_kind: text(row, "scope_kind"),
            scope_detail: column(row, "scope_detail"),
            effective_powers: column(row, "effective_powers"),
            grant_source: column(row, "grant_source"),
            revocation_source: column(row, "revocation_source"),
            inheritance_path: column(row, "inheritance_path"),
            transfer_behavior: column(row, "transfer_behavior"),
        })
    }
}

/// One served permission row, in the columns the comparison reads.
#[derive(Clone, Debug, PartialEq)]
pub struct ServedGrant {
    pub resource_id: String,
    pub subject: String,
    pub scope: String,
    pub scope_kind: Option<String>,
    pub scope_detail: Value,
    pub effective_powers: Value,
    pub grant_source: Value,
    pub revocation_source: Value,
    pub inheritance_path: Value,
    pub transfer_behavior: Value,
}

/// Whether the fuses block a power (permissions.rs:348-377): CANNOT_UNWRAP 1, CANNOT_BURN_FUSES
/// 2, CANNOT_TRANSFER 4, CANNOT_SET_RESOLVER 8, CANNOT_SET_TTL 16, CANNOT_CREATE_SUBDOMAIN 32,
/// CANNOT_APPROVE 64, PARENT_CANNOT_CONTROL 65536, CAN_EXTEND_EXPIRY 262144.
/// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10-L20 @ ens_v1@91c966f)
fn blocked(power: &str, wrapper_state: &str, fuses: i64) -> bool {
    match power {
        "resource_control" => wrapper_state == "locked",
        "resolver_control" | "set_resolver" => fuses & 8 != 0,
        "set_ttl" => fuses & 16 != 0,
        "create_subnames" | "create_subdomain" => fuses & 32 != 0,
        "transfer" | "transfer_name" => fuses & 4 != 0,
        "unwrap" => fuses & 1 != 0,
        "burn_fuses" => fuses & 2 != 0 || fuses & 65536 == 0,
        "extend_expiry" => fuses & 262144 == 0,
        "approve" | "approve_wrapper" => fuses & 64 != 0,
        _ => false,
    }
}

/// The powers a grant serves under the resource's wrapper at `clock_seconds`.
pub fn masked_powers(powers: &Value, wrapper: Option<&WrapperRow>, clock_seconds: i64) -> Value {
    let Some(wrapper) = wrapper.filter(|wrapper| wrapper.has_modifier) else {
        return powers.clone();
    };
    let effective = effective_wrapper(wrapper, clock_seconds);
    let (Some(state), Some(fuses)) = (effective.wrapper_state.as_deref(), effective.fuses) else {
        return json!([]);
    };
    let kept: Vec<Value> = powers
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|power| {
            let text = power.as_str()?;
            let graced = !effective.in_grace || matches!(text, "approve" | "approve_wrapper");
            (graced && !blocked(text, state, fuses)).then(|| power.clone())
        })
        .collect();
    Value::Array(kept)
}

/// The served rows of one resource's grants, before the operator fan-out.
pub fn masked_grants(
    grants: &[GrantRow],
    wrapper: Option<&WrapperRow>,
    key_state: Option<&Maxima>,
    clock_seconds: i64,
) -> Vec<ServedGrant> {
    if key_state.is_some_and(registration_lapsed) {
        return Vec::new();
    }
    grants
        .iter()
        .filter_map(|grant| {
            let powers = masked_powers(&grant.effective_powers, wrapper, clock_seconds);
            powers
                .as_array()
                .is_some_and(|powers| !powers.is_empty())
                .then(|| ServedGrant {
                    resource_id: grant.resource_id.clone(),
                    subject: grant.subject.clone(),
                    scope: grant.scope.clone(),
                    scope_kind: grant.scope_kind.clone(),
                    scope_detail: grant.scope_detail.clone(),
                    effective_powers: powers,
                    grant_source: grant.grant_source.clone(),
                    revocation_source: grant.revocation_source.clone(),
                    inheritance_path: grant.inheritance_path.clone(),
                    transfer_behavior: grant.transfer_behavior.clone(),
                })
        })
        .collect()
}

/// A NameWrapper operator approval (`project_account_approval` with authority kind wrapper).
#[derive(Clone, Debug)]
pub struct WrapperApproval {
    pub authority_contract: String,
    pub owner: String,
    pub subject: String,
    pub approved: bool,
}

/// The wrapper operator fan-out (wrapper_operators.rs:21-137): every served NameWrapper holder
/// row is copied to each operator its holder approved on that wrapper contract; an operator who
/// already has a row for the resource and scope keeps the operator's powers, source and transfer
/// behaviour (the operator set is a superset), and every other operator row is added.
pub fn with_operators(
    mut rows: Vec<ServedGrant>,
    approvals: &[WrapperApproval],
) -> Vec<ServedGrant> {
    let holders: Vec<ServedGrant> = rows
        .iter()
        .filter(|row| {
            row.grant_source
                .get("authority_kind")
                .and_then(Value::as_str)
                == Some("wrapper")
                && row
                    .grant_source
                    .get("relation_kind")
                    .and_then(Value::as_str)
                    == Some("holder")
        })
        .cloned()
        .collect();
    let mut operators: Vec<ServedGrant> = Vec::new();
    for holder in &holders {
        let contract = holder
            .grant_source
            .get("authority_contract")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase);
        for approval in approvals.iter().filter(|approval| {
            approval.approved
                && approval.owner == holder.subject
                && Some(approval.authority_contract.as_str()) == contract.as_deref()
        }) {
            let mut source: Map<String, Value> =
                holder.grant_source.as_object().cloned().unwrap_or_default();
            source.remove("relation_kind");
            source.remove("source_event_kind");
            source.insert("relation_kind".into(), json!("operator"));
            source.insert("source_event_kind".into(), json!("ApprovalForAll"));
            source.insert("owner".into(), json!(holder.subject));
            operators.push(ServedGrant {
                subject: approval.subject.clone(),
                grant_source: Value::Object(source),
                revocation_source: Value::Null,
                transfer_behavior: json!({"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"}),
                ..holder.clone()
            });
        }
    }
    for operator in operators {
        match rows.iter_mut().find(|row| {
            row.resource_id == operator.resource_id
                && row.subject == operator.subject
                && row.scope == operator.scope
        }) {
            Some(existing) => {
                existing.effective_powers = operator.effective_powers;
                existing.grant_source = operator.grant_source;
                existing.transfer_behavior = operator.transfer_behavior;
            }
            None => rows.push(operator),
        }
    }
    rows.sort_by(|left, right| {
        (&left.resource_id, &left.subject, &left.scope).cmp(&(
            &right.resource_id,
            &right.subject,
            &right.scope,
        ))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapper(state: &str, fuses: i64, expiry: i64) -> WrapperRow {
        WrapperRow {
            resource_id: "w".into(),
            wrapper_state: Some(state.into()),
            fuses: Some(fuses),
            has_modifier: true,
            expiry_seconds: Some(expiry.to_string()),
            has_expiry: true,
            ..WrapperRow::default()
        }
    }

    #[test]
    fn fuses_mask_their_powers_and_grace_keeps_only_approval() {
        let powers = json!(["set_resolver", "transfer", "approve", "resource_control"]);
        let masked = masked_powers(
            &powers,
            Some(&wrapper("locked", 8 | 4, 2_000_000_000)),
            1_000,
        );
        assert_eq!(masked, json!(["approve"]));
        let grace = masked_powers(
            &powers,
            Some(&wrapper("wrapped", 131072, 1_000 + 100)),
            1_000,
        );
        assert_eq!(grace, json!(["approve"]));
        assert_eq!(masked_powers(&powers, None, 0), powers);
    }

    #[test]
    fn an_operator_who_is_also_a_delegate_keeps_the_operator_set() {
        let holder = ServedGrant {
            resource_id: "w".into(),
            subject: "0xholder".into(),
            scope: "resource".into(),
            scope_kind: Some("resource".into()),
            scope_detail: json!({"kind": "resource"}),
            effective_powers: json!(["resource_control", "transfer"]),
            grant_source: json!({"authority_kind": "wrapper", "relation_kind": "holder", "authority_contract": "0xABC"}),
            revocation_source: Value::Null,
            inheritance_path: json!([]),
            transfer_behavior: json!({"mode": "holder"}),
        };
        let delegate = ServedGrant {
            subject: "0xop".into(),
            effective_powers: json!(["set_resolver"]),
            grant_source: json!({"authority_kind": "wrapper", "relation_kind": "delegate"}),
            ..holder.clone()
        };
        let approvals = [WrapperApproval {
            authority_contract: "0xabc".into(),
            owner: "0xholder".into(),
            subject: "0xop".into(),
            approved: true,
        }];
        let rows = with_operators(vec![holder.clone(), delegate], &approvals);
        assert_eq!(rows.len(), 2);
        let operator = rows
            .iter()
            .find(|row| row.subject == "0xop")
            .expect("the operator row");
        assert_eq!(operator.effective_powers, holder.effective_powers);
        assert_eq!(operator.grant_source["relation_kind"], json!("operator"));
        assert_eq!(operator.grant_source["owner"], json!("0xholder"));
    }
}
