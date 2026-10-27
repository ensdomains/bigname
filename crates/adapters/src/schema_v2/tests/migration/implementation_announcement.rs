//! Row 12: a registry implementation's own `RegistryCreated`, emitted when the implementation
//! contract is constructed, is an ordinary self-announced registry. Declaring the address under
//! an implementation role keeps no factory origin for it and correlates nothing.
use super::*;

const IMPLEMENTATION: &str = "0x4444444444444444444444444444444444444444";

/// The registry events and migration rows a lone `RegistryCreated` from the implementation
/// address derives, with the address declared under `role` or not declared.
fn announced(role: Option<&str>) -> anyhow::Result<Vec<(String, String, Value)>> {
    let fixture = fixture()?;
    let block = fixture["scenarios"]["L-01"]["migration_block"]
        .as_i64()
        .unwrap();
    let mut input = batch(
        vec![raw_at_transaction(
            super::super::RegistryCreated {}.encode_log_data(),
            block,
            0,
            0,
            IMPLEMENTATION,
        )],
        &fixture,
        true,
    );
    if let Some(role) = role {
        input.admissions.push(admission_at(
            MIGRATION_MANIFEST_ID,
            role,
            IMPLEMENTATION,
            950,
        ));
    }
    let output = interpret_test_batch(input)?;
    assert!(output.migration_discovery_associations.is_empty());
    assert!(output.migration_event_associations.is_empty());
    assert!(output.migration_candidate_identity_effects.is_empty());
    Ok(output
        .normalized_events
        .iter()
        .map(|event| {
            assert!(event.migration_correlation_ids.is_empty(), "{event:?}");
            // The instance id follows the admission row, which a declaration supplies.
            let mut after = event.after_state.clone();
            after
                .as_object_mut()
                .map(|after| after.remove("contract_instance_id"));
            (event.source_family.clone(), event.event_kind.clone(), after)
        })
        .collect())
}

#[test]
fn an_implementations_own_announcement_is_an_ordinary_registry() -> anyhow::Result<()> {
    let undeclared = announced(None)?;
    assert!(
        undeclared
            .iter()
            .any(|(family, kind, _)| family == "ens_v2_registry_l1" && kind == "RegistryCreated"),
        "{undeclared:?}"
    );
    for role in [
        "wrapper_registry_implementation",
        "user_registry_implementation",
    ] {
        let declared = announced(Some(role))?;
        assert!(
            declared
                .iter()
                .all(|(_, kind, _)| kind != "ContractDiscovered" && kind != "MigrationApplied"),
            "{role}: {declared:?}"
        );
        assert_eq!(declared, undeclared, "{role}");
    }
    Ok(())
}
