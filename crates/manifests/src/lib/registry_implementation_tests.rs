//! Row 15: a registry implementation role is repeatable, one declaration per address. Each
//! declaration is stored under its own `role@address` name, so a further pair is one more
//! `[[contracts]]` entry. One address twice under a role, or a singleton role twice, is refused.
use anyhow::Result;

use super::*;

fn implementation(role: &str, address: &str, start: u64) -> String {
    format!(
        "\n[[contracts]]\nrole = \"{role}\"\naddress = \"{address}\"\nproxy_kind = \"none\"\nstart_block = {start}\n"
    )
}

const FIRST: &str = "0x00000000000000000000000000000000000000e1";
const SECOND: &str = "0x00000000000000000000000000000000000000e2";

#[test]
fn an_implementation_role_takes_one_declaration_per_address() -> Result<()> {
    for role in [
        WRAPPER_REGISTRY_IMPLEMENTATION_ROLE,
        USER_REGISTRY_IMPLEMENTATION_ROLE,
    ] {
        let two = format!(
            "{}{}{}",
            manifest_contents(),
            implementation(role, FIRST, 10),
            implementation(role, SECOND, 20)
        );
        let repository = load_one(&two)?;
        let declared: Vec<_> = repository.manifests()[0]
            .manifest
            .contracts
            .iter()
            .filter(|contract| contract.role == role)
            .map(|contract| {
                (
                    schema_v2::contract_declaration_name(
                        &contract.role,
                        &normalize_address(&contract.address),
                    ),
                    contract.start_block,
                )
            })
            .collect();
        assert_eq!(
            declared,
            [
                (format!("{role}@{FIRST}"), Some(10)),
                (format!("{role}@{SECOND}"), Some(20)),
            ]
        );

        let same_address = format!(
            "{}{}{}",
            manifest_contents(),
            implementation(role, FIRST, 10),
            implementation(
                role,
                &FIRST.to_ascii_uppercase().replacen("0X", "0x", 1),
                20
            )
        );
        let error = load_one(&same_address).expect_err(role);
        assert!(
            format!("{error:#}").contains(&format!(
                "declares contract role {role} address {FIRST} more than once"
            )),
            "{role}: {error:#}"
        );
    }
    // The singleton roles stay singletons, the migration controllers among them.
    let two_controllers = format!(
        "{}{}{}",
        manifest_contents(),
        implementation("locked_migration_controller", FIRST, 10),
        implementation("locked_migration_controller", SECOND, 20)
    );
    let error = load_one(&two_controllers).expect_err("two locked controllers");
    assert!(
        format!("{error:#}").contains("duplicates contract role locked_migration_controller"),
        "{error:#}"
    );
    assert_eq!(
        schema_v2::contract_declaration_name("locked_migration_controller", FIRST),
        "locked_migration_controller"
    );
    Ok(())
}
