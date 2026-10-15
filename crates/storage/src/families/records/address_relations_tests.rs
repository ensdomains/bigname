use uuid::Uuid;

use super::*;

fn wrapper(fuses: Option<i64>, expiry: Option<&str>) -> WrapperRow {
    WrapperRow {
        resource_id: "r".into(),
        logical_name_id: None,
        wrapper_state: Some("wrapped".into()),
        fuses,
        has_modifier: true,
        expiry_seconds: expiry.map(str::to_owned),
        has_expiry: expiry.is_some(),
        lifecycle_unwrapped: None,
    }
}

// The served `in_grace` is null, not false, when the fuses or the expiry is unknown, so the
// wrapped-and-out-of-grace mask stays closed there.
#[test]
fn grace_is_unknown_without_fuses_or_expiry() {
    assert_eq!(in_grace(&wrapper(None, Some("100")), 50), None);
    assert_eq!(in_grace(&wrapper(Some(IS_DOT_ETH), None), 50), None);
    // Inside the last grace period before the wrapper expiry.
    let expiry = 10_000_000_i64;
    let clock = expiry - 10;
    assert_eq!(
        in_grace(&wrapper(Some(IS_DOT_ETH), Some(&expiry.to_string())), clock),
        Some(true)
    );
    // Past the expiry the served test is false (expiry >= clock fails).
    assert_eq!(
        in_grace(
            &wrapper(Some(IS_DOT_ETH), Some(&expiry.to_string())),
            expiry + 1
        ),
        Some(false)
    );
    // Not a .eth name: never in grace.
    assert_eq!(
        in_grace(&wrapper(Some(0), Some(&expiry.to_string())), clock),
        Some(false)
    );
}

#[test]
fn arms_follow_the_source_family_prefix() {
    assert_eq!(arm_of("ens_v1_registry_l1"), Some("ens_v1"));
    assert_eq!(arm_of("ens_v2_root_l1"), Some("ens_v2"));
    assert_eq!(arm_of("basenames_base_registry"), Some("basenames"));
    assert_eq!(arm_of("other"), None);
}

const OLD: &str = "0x00000000000000000000000000000000000000a1";
const NEW: &str = "0x00000000000000000000000000000000000000b1";

fn ens_v2_name(resource: Uuid) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: "ens:0x01".into(),
        namespace: "ens".into(),
        canonical_display_name: "alice.eth".into(),
        normalized_name: "alice.eth".into(),
        namehash: "0x01".into(),
        surface_binding_id: Some(Uuid::from_u128(1)),
        resource_id: Some(resource),
        serving_resource_id: None,
        token_lineage_id: Some(Uuid::from_u128(3)),
        binding_kind: Some(crate::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: serde_json::json!({}),
        provenance: serde_json::json!({"authority_selection": {
            "authority_arm": "ens_v2", "resource_id": resource.to_string()}}),
        coverage: serde_json::json!({}),
        chain_positions: serde_json::json!({}),
        canonicality_summary: serde_json::json!({}),
        manifest_version: 1,
        last_recomputed_at: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn candidate(
    (block, log): (i64, i64),
    kind: &str,
    resource: Option<Uuid>,
    set: bool,
    subject: &str,
) -> ControllerCandidate {
    ControllerCandidate {
        logical_name_id: "ens:0x01".into(),
        position: FamilyPosition {
            block_number: block,
            transaction_index: Some(0),
            log_index: Some(log),
            event_identity: format!("{kind}:{block}:{log}"),
        },
        resource_id: resource.map(|resource| resource.to_string()),
        event_kind: kind.into(),
        source_family: "ens_v2_registry_l1".into(),
        set,
        subject: Some(subject.into()),
    }
}

// An ENSv2 registry transfer's candidate sets the controller to its recipient when it follows
// the registration's AuthorityTransferred, in a later block or later in the same transaction,
// whatever order the rows are read in. The registry-scoped role changes of the transfer carry
// no resource, so they neither revoke the recipient nor restore the sender. A transfer before
// the AuthorityTransferred leaves the AuthorityTransferred owner.
#[test]
fn an_ens_v2_transfer_candidate_sets_the_controller_after_the_registration() {
    let resource = Uuid::from_u128(2);
    let row = ens_v2_name(resource);
    let fold = |candidates: &[ControllerCandidate]| {
        let input = NameRelationsInput {
            row: &row,
            candidates,
            binding: None,
            wrapper: None,
            clock_seconds: 0,
            role_holders: &[],
        };
        controller(&input, true).map(|(address, position)| (address, position.block_number))
    };
    let registered = candidate((121, 0), "AuthorityTransferred", Some(resource), true, OLD);
    let transfer = |at| candidate(at, "TokenControlTransferred", Some(resource), true, NEW);
    let sent = transfer((124, 0));
    let revoked = candidate((124, 1), "PermissionChanged", None, false, OLD);
    let granted = candidate((124, 2), "PermissionChanged", None, true, OLD);
    let revoked_new = candidate((124, 3), "PermissionChanged", None, false, NEW);
    let mut rows = vec![registered.clone(), sent, revoked, granted, revoked_new];
    assert_eq!(fold(&rows), Some((NEW.to_owned(), 124)));
    rows.reverse();
    assert_eq!(fold(&rows), Some((NEW.to_owned(), 124)));

    let same_transaction = [transfer((121, 3)), registered.clone()];
    assert_eq!(fold(&same_transaction), Some((NEW.to_owned(), 121)));

    let earlier = [registered, transfer((120, 0))];
    assert_eq!(fold(&earlier), Some((OLD.to_owned(), 121)));
}
