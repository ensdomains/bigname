//! Additional fields for explicit retained history actions.
use super::*;
use alloy_primitives::U256;

pub(super) fn append(
    data: &mut Map<String, Value>,
    row: &StorageHistoryEvent,
    context: &HistoryRowContext,
) {
    let after = &row.after_state;
    if let Some(handoff) = context.registry_handoff(row) {
        insert(data, "from_registry", pointer(row, &handoff.from_registry));
        insert(data, "to_registry", pointer(row, &handoff.to_registry));
        insert(
            data,
            "node",
            address_field(
                after,
                if row.event_kind == "SubregistryChanged" {
                    "child_node"
                } else {
                    "node"
                },
            ),
        );
        insert(data, "owner", address_field(after, "owner"));
    }
    match row.event_kind.as_str() {
        "AuthorityEpochChanged"
            if actions::is_wrapping(row) || after["source_event"] == "NameUnwrapped" =>
        {
            insert(data, "node", address_field(after, "node"));
            insert(data, "fuses", unsigned_field(after, "fuses"));
            insert_expiry(data, row);
        }
        "AccountPermissionChanged" => {
            let scope = &after["scope"];
            insert(data, "owner", address_field(scope, "owner"));
            insert(
                data,
                "approved",
                after.get("approved").filter(|v| v.is_boolean()).cloned(),
            );
            if let (Some(chain), Some(authority), Some(contract), Some(owner)) = (
                scope["chain_id"].as_str().and_then(slug_to_numeric),
                scope["authority_kind"].as_str(),
                scope["authority_contract"].as_str(),
                scope["owner"].as_str(),
            ) {
                data.insert("grant_scope".into(), json!({"kind":"account", "detail":{
                    "chain_id":chain, "authority_kind":authority,
                    "authority_contract":contract.to_ascii_lowercase(), "owner":owner.to_ascii_lowercase()
                }}));
            }
        }
        "RegistrationReserved" => {
            insert(data, "token_id", decimal(after.get("token_id")));
            insert(data, "node", address_field(after, "node"));
        }
        "ResolverRecordLinked" => {
            insert(data, "resolver", record_resolver(row));
            insert(data, "node", address_field(after, "node"));
            insert(data, "record_id", decimal(after.get("resolver_record_id")));
            insert(
                data,
                "dns_encoded_name",
                address_field(after, "dns_encoded_name"),
            );
        }
        "TokenRegenerated" => {
            insert(data, "old_token_id", decimal(after.get("old_token_id")));
            insert(data, "new_token_id", decimal(after.get("new_token_id")));
        }
        "ReverseChanged" => insert(data, "reverse_node", address_field(after, "reverse_node")),
        "RecordVersionChanged" => {
            insert(data, "record_version", decimal(after.get("record_version")))
        }
        "RegistryCreated" => insert(data, "registry", contract_ref(row, after, "registry")),
        "ParentChanged" => {
            insert(
                data,
                "registry",
                contract_ref(row, &row.raw_fact_ref, "emitting_address"),
            );
            insert(data, "parent", contract_ref(row, after, "parent"));
            // ParentUpdated retains an explicit JSON null for the emitted zero address.
            // A missing field does not prove a clear.
            if let Some(parent) = after.get("parent") {
                if parent.is_null() || parent.as_str() == Some(ZERO_ADDRESS) {
                    data.insert("parent_cleared".into(), json!(true));
                } else if parent.is_string() {
                    data.insert("parent_cleared".into(), json!(false));
                }
            }
            if let Some(raw) = after["raw_label_hex"].as_str() {
                if let Ok(bytes) = hex::decode(raw.strip_prefix("0x").unwrap_or(raw)) {
                    let value = match String::from_utf8(bytes) {
                        Ok(text) if !text.contains('\0') => json!(text),
                        _ => {
                            json!({"bytes": format!("0x{}", raw.trim_start_matches("0x").to_ascii_lowercase())})
                        }
                    };
                    data.insert("label".into(), value);
                }
            } else {
                insert(data, "label", present(after.get("label")).cloned());
            }
        }
        "Upgraded" => {
            insert(
                data,
                "proxy",
                contract_ref(row, &row.raw_fact_ref, "emitting_address"),
            );
            insert(
                data,
                "implementation",
                contract_ref(row, after, "implementation"),
            );
        }
        _ => {}
    }
}

fn pointer(row: &StorageHistoryEvent, address: &str) -> Option<Value> {
    Some(
        json!({"chain_id":slug_to_numeric(row.chain_id.as_deref()?)?, "address":address.to_ascii_lowercase()}),
    )
}

fn decimal(value: Option<&Value>) -> Option<Value> {
    let value = value?;
    let parsed = if let Some(number) = value.as_u64() {
        U256::from(number)
    } else {
        let text = value.as_str()?;
        let (digits, radix) = text
            .strip_prefix("0x")
            .map_or((text, 10), |digits| (digits, 16));
        if digits.is_empty()
            || !digits.bytes().all(|b| {
                if radix == 16 {
                    b.is_ascii_hexdigit()
                } else {
                    b.is_ascii_digit()
                }
            })
        {
            return None;
        }
        U256::from_str_radix(digits, radix).ok()?
    };
    Some(json!(parsed.to_string()))
}
