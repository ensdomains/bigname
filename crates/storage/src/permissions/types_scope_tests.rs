use super::*;

#[test]
fn root_scope_reads_its_registry_and_keeps_the_root_key() {
    let detail = json!({"kind": "registry_root", "chain_id": "ethereum-sepolia",
        "registry_address": "0x00000000000000000000000000000000000000AB"});
    let scope = PermissionScope::parse("root", &detail).expect("root scope must parse");
    let registry_address = "0x00000000000000000000000000000000000000ab".to_owned();
    let chain_id = "ethereum-sepolia".to_owned();
    assert_eq!(
        scope,
        PermissionScope::Root {
            chain_id,
            registry_address
        }
    );
    assert_eq!(
        (scope.kind(), scope.storage_key().as_str()),
        ("root", "root")
    );
    assert!(PermissionScope::parse("root", &json!({})).is_err());
}
