use alloy_primitives::U256;
use serde_json::{Value, json};

use crate::schema_v2::{catalog::Selected, model::RawLogInput};

#[derive(Clone, Copy)]
pub(super) enum V2Vocabulary {
    Registry,
    Resolver,
}

pub(super) struct V2PermissionState<'a> {
    pub upstream_resource: &'a str,
    pub account: String,
    pub old_bitmap: U256,
    pub new_bitmap: U256,
    pub root_resource: bool,
    pub selector: Value,
}

pub(super) fn v2_states(
    selected: &Selected,
    raw: &RawLogInput,
    vocabulary: V2Vocabulary,
    permission: V2PermissionState<'_>,
) -> (Value, Value) {
    let old_powers = powers(permission.old_bitmap, vocabulary);
    let effective_powers = powers(permission.new_bitmap, vocabulary);
    let changed_powers = changed_powers(permission.old_bitmap, permission.new_bitmap, vocabulary);
    let source_key = match vocabulary {
        V2Vocabulary::Registry => "registry_contract_instance_id",
        V2Vocabulary::Resolver => "resolver_contract_instance_id",
    };
    let scope = match vocabulary {
        V2Vocabulary::Registry => json!({
            "kind":if permission.root_resource {"registry_root"} else {"registry"},
            "chain_id":raw.chain_id,
            "registry_address":raw.emitting_address,
        }),
        V2Vocabulary::Resolver => json!({
            "kind":"resolver",
            "chain_id":raw.chain_id,
            "resolver_address":raw.emitting_address,
        }),
    };
    let inheritance_path = if permission.root_resource {
        match vocabulary {
            V2Vocabulary::Registry => json!([{
                "kind":"registry_root_fallback",
                "chain_id":raw.chain_id,
                "registry_address":raw.emitting_address,
                "upstream_resource":permission.upstream_resource,
            }]),
            V2Vocabulary::Resolver => json!([{
                "kind":"resolver_root_fallback",
                "chain_id":raw.chain_id,
                "resolver_address":raw.emitting_address,
                "upstream_resource":permission.upstream_resource,
            }]),
        }
    } else {
        json!([])
    };
    let source = |changed_powers: Vec<String>| {
        let mut value = json!({
            "kind":"raw_log",
            "source_event":"EACRolesChanged",
            "upstream_resource":permission.upstream_resource,
            "root_resource":permission.root_resource,
            "changed_powers":changed_powers,
        });
        value
            .as_object_mut()
            .expect("permission source is an object")
            .insert(
                source_key.to_owned(),
                Value::String(selected.contract_instance_id.to_string()),
            );
        value
    };
    let (has_grant, revoked_last_grant) = source_transition(&old_powers, &effective_powers);
    let grant_source = if has_grant {
        source(changed_powers.clone())
    } else {
        json!({})
    };
    let revocation_source = revoked_last_grant.then(|| source(changed_powers));
    let before = json!({
        "subject":permission.account,
        "role_bitmap":word(permission.old_bitmap),
        "effective_powers":old_powers,
    });
    let mut after = json!({
        "subject":permission.account,
        "scope":scope,
        "effective_powers":effective_powers,
        "grant_source":grant_source,
        "revocation_source":revocation_source,
        "inheritance_path":inheritance_path,
        "transfer_behavior":{},
        "source_event":"EACRolesChanged",
        "upstream_resource":permission.upstream_resource,
        "resource":permission.upstream_resource,
        "role_bitmap":word(permission.new_bitmap),
        "old_role_bitmap":word(permission.old_bitmap),
        "root_resource":permission.root_resource,
        "selector":permission.selector,
    });
    after
        .as_object_mut()
        .expect("permission state is an object")
        .insert(
            source_key.to_owned(),
            Value::String(selected.contract_instance_id.to_string()),
        );
    (before, after)
}

pub(in crate::schema_v2) fn v1_grant_states(
    subject: &str,
    scope: Value,
    power: &str,
    authority_kind: &str,
    authority_key: &str,
    source_event_kind: &str,
) -> (Value, Value) {
    let source = json!({
        "kind":"ens_v1_authority",
        "authority_kind":authority_kind,
        "authority_key":authority_key,
        "source_event_kind":source_event_kind,
    });
    let base = |effective_powers: Value, grant_source: Value, revocation_source: Value| {
        json!({
            "subject":subject,
            "scope":scope,
            "effective_powers":effective_powers,
            "grant_source":grant_source,
            "revocation_source":revocation_source,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change",
        })
    };
    (
        base(json!([]), Value::Null, Value::Null),
        base(json!([power]), source, Value::Null),
    )
}

pub(in crate::schema_v2) fn v1_revoke_states(
    subject: &str,
    scope: Value,
    power: &str,
    authority_kind: &str,
    authority_key: &str,
    source_event_kind: &str,
) -> (Value, Value) {
    let source = json!({
        "kind":"ens_v1_authority",
        "authority_kind":authority_kind,
        "authority_key":authority_key,
        "source_event_kind":source_event_kind,
    });
    let base = |effective_powers: Value, grant_source: Value, revocation_source: Value| {
        json!({
            "subject":subject,
            "scope":scope,
            "effective_powers":effective_powers,
            "grant_source":grant_source,
            "revocation_source":revocation_source,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change",
        })
    };
    (
        base(json!([power]), source.clone(), Value::Null),
        base(json!([]), Value::Null, source),
    )
}

fn powers(bitmap: U256, vocabulary: V2Vocabulary) -> Vec<String> {
    role_bits(vocabulary)
        .iter()
        .filter(|(bit, _)| bitmap.bit(*bit))
        .map(|(_, power)| (*power).to_owned())
        .collect()
}

fn changed_powers(old: U256, new: U256, vocabulary: V2Vocabulary) -> Vec<String> {
    role_bits(vocabulary)
        .iter()
        .filter(|(bit, _)| old.bit(*bit) != new.bit(*bit))
        .map(|(_, power)| (*power).to_owned())
        .collect()
}

fn source_transition(old_powers: &[String], new_powers: &[String]) -> (bool, bool) {
    let has_authorizing_power =
        |powers: &[String]| powers.iter().any(|power| power != "was_reserved");
    let old_grant = has_authorizing_power(old_powers);
    let new_grant = has_authorizing_power(new_powers);
    (new_grant, old_grant && !new_grant)
}

fn word(bitmap: U256) -> String {
    crate::evm_abi::u256_word_hex(bitmap)
}

fn role_bits(vocabulary: V2Vocabulary) -> &'static [(usize, &'static str)] {
    match vocabulary {
        V2Vocabulary::Registry => REGISTRY_ROLE_BITS,
        V2Vocabulary::Resolver => RESOLVER_ROLE_BITS,
    }
}

// Review this vocabulary against the pinned ENSv2 registry role constants.
// (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L7 @ ens_v2@a971bd64)
const REGISTRY_ROLE_BITS: &[(usize, &str)] = &[
    (0, "registrar"),
    (4, "register_reserved"),
    (8, "set_parent"),
    (12, "unregister"),
    (16, "renew"),
    (20, "set_subregistry"),
    (24, "set_resolver"),
    (32, "was_reserved"),
    (36, "set_uri"),
    (120, "can_name"),
    (124, "upgrade"),
    (128, "admin_registrar"),
    (132, "admin_register_reserved"),
    (136, "admin_set_parent"),
    (140, "admin_unregister"),
    (144, "admin_renew"),
    (148, "admin_set_subregistry"),
    (152, "admin_set_resolver"),
    (156, "can_transfer_admin"),
    (164, "admin_set_uri"),
    (248, "admin_can_name"),
    (252, "admin_upgrade"),
];

// Review this vocabulary against the admitted historical ENSv2 resolver role constants.
// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L7 @ ens_v2_sepolia_20260629@ccaeb58)
const RESOLVER_ROLE_BITS: &[(usize, &str)] = &[
    (0, "set_addr"),
    (4, "set_text"),
    (8, "set_contenthash"),
    (12, "set_pubkey"),
    (16, "set_abi"),
    (20, "set_interface"),
    (24, "set_name"),
    (28, "set_alias"),
    (32, "clear_records"),
    (36, "set_data"),
    (120, "can_name"),
    (124, "upgrade"),
    (128, "admin_set_addr"),
    (132, "admin_set_text"),
    (136, "admin_set_contenthash"),
    (140, "admin_set_pubkey"),
    (144, "admin_set_abi"),
    (148, "admin_set_interface"),
    (152, "admin_set_name"),
    (156, "admin_set_alias"),
    (160, "admin_clear_records"),
    (164, "admin_set_data"),
    (248, "admin_can_name"),
    (252, "admin_upgrade"),
];

#[cfg(test)]
mod tests {
    use super::*;

    fn pinned_source(relative_path: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative_path);
        std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!(
                "failed to read pinned upstream source {}: {error}; run scripts/sync-refs",
                path.display()
            )
        })
    }

    #[test]
    #[should_panic(expected = "run scripts/sync-refs")]
    fn missing_pinned_source_names_the_sync_command() {
        pinned_source(".refs/intentionally-missing-role-vocabulary.sol");
    }

    #[test]
    fn role_tables_cover_each_pinned_upstream_constant() {
        let registry =
            pinned_source(".refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol");
        let resolver = pinned_source(
            ".refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol",
        );
        let role_count = |source: &str| {
            source
                .lines()
                .filter(|line| line.contains("uint256 internal constant ROLE_"))
                .count()
        };
        assert_eq!(REGISTRY_ROLE_BITS.len(), role_count(&registry));
        assert_eq!(RESOLVER_ROLE_BITS.len(), role_count(&resolver));
        assert_eq!(
            powers(U256::from(1_u8) << 32, V2Vocabulary::Registry),
            ["was_reserved"]
        );
    }

    #[test]
    fn marker_only_state_preserves_real_permission_revocation() {
        let old = vec!["renew".to_owned(), "was_reserved".to_owned()];
        let new = vec!["was_reserved".to_owned()];
        assert_eq!(source_transition(&old, &new), (false, true));
    }
}
