//! The mirror target a retained ENSv1 child keeps beyond the ENSv1 node-based model.
use serde_json::json;

use super::mirror_retains_permissioned_target;
use crate::families::records::{
    facts::ResolverClassification,
    mirror::{MirrorNearest, MirrorSelection},
};

const TARGET: &str = "0x0000000000000000000000000000000000025910";

fn class(
    source_family: &str,
    role: &str,
    supported: bool,
    namespace: &str,
) -> ResolverClassification {
    ResolverClassification {
        classification: json!({"source_family": source_family, "role": role}),
        support_status: Some(
            if supported {
                "supported"
            } else {
                "unsupported"
            }
            .into(),
        ),
        unsupported_reason: None,
        manifest_id: Some(1),
        declaration_namespace: Some(namespace.into()),
    }
}

fn permissioned() -> ResolverClassification {
    class("ens_v2_resolver_l1", "permissioned_resolver", true, "ens")
}

fn selection(depth: i32, reason: &str) -> MirrorSelection {
    MirrorSelection {
        mirror: class("ens_v2_resolver_l1", "ensv1_mirror_resolver", true, "ens"),
        mirror_namespace_matches: true,
        nearest: Some(MirrorNearest {
            ancestor_depth: depth,
            mirrored_node: "0x01".into(),
            mirrored_name: "child.envoy1084.eth".into(),
            mirrored_resource_id: None,
            mirrored_resolver_address: TARGET.into(),
            mirrored_pointer_event_id: Some(1),
            mirrored_pointer_source_family: "ens_v1_registry_l1".into(),
            mirrored_pointer_namespace: "ens".into(),
            mirrored_block_number: 1,
            mirrored_classification_supported: true,
            forwarding: "direct_call",
            mirrored_unsupported_reason: Some(reason.into()),
        }),
        consulted_nodes: Vec::new(),
    }
}

#[test]
fn mirror_target_admits_ensv2_resolver_only_at_requested_node() {
    let exact = selection(0, "mirrored_resolver_not_ensv1");
    assert!(mirror_retains_permissioned_target(
        &exact,
        Some(&permissioned()),
        Some(TARGET)
    ));
    for (case, inner) in [
        ("no classification", None),
        (
            "a mirror",
            Some(class(
                "ens_v2_resolver_l1",
                "ensv1_mirror_resolver",
                true,
                "ens",
            )),
        ),
        (
            "a PublicResolverV2",
            Some(class(
                "ens_v2_resolver_l1",
                "public_resolver_v2",
                true,
                "ens",
            )),
        ),
        (
            "another family and role, rejected by role",
            Some(class("ens_v1_resolver_l1", "public_resolver", true, "ens")),
        ),
        (
            "another namespace",
            Some(class(
                "ens_v2_resolver_l1",
                "permissioned_resolver",
                true,
                "basenames",
            )),
        ),
        (
            "unsupported",
            Some(class(
                "ens_v2_resolver_l1",
                "permissioned_resolver",
                false,
                "ens",
            )),
        ),
    ] {
        assert!(
            !mirror_retains_permissioned_target(&exact, inner.as_ref(), Some(TARGET)),
            "{case}"
        );
    }
    let permissioned = permissioned();
    let ancestor = selection(1, "mirrored_resolver_not_ensv1");
    let other_reason = selection(0, "mirrored_resolver_is_mirror");
    let mut unsupported_mirror = exact.clone();
    unsupported_mirror.mirror.support_status = Some("unsupported".into());
    let mut other_mirror_namespace = exact.clone();
    other_mirror_namespace.mirror_namespace_matches = false;
    let mut no_nearest = exact.clone();
    no_nearest.nearest = None;
    for (case, mirror, retained) in [
        ("depth 1", &ancestor, Some(TARGET)),
        ("another mirror reason", &other_reason, Some(TARGET)),
        ("an unsupported mirror", &unsupported_mirror, Some(TARGET)),
        (
            "a mirror in another namespace",
            &other_mirror_namespace,
            Some(TARGET),
        ),
        ("no selected node", &no_nearest, Some(TARGET)),
        (
            "another retained resolver",
            &exact,
            Some("0x0000000000000000000000000000000000000bad"),
        ),
        ("no retained resolver", &exact, None),
    ] {
        assert!(
            !mirror_retains_permissioned_target(mirror, Some(&permissioned), retained),
            "{case}"
        );
    }
}
