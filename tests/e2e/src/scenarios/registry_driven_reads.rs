use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

fn path_name(name: &str) -> String {
    name.replace('[', "%5B").replace(']', "%5D")
}

fn child_entries(body: &Value) -> Vec<Value> {
    body.pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

async fn children_for(run: &support::PipelineRun, parent: &str) -> Result<Vec<Value>> {
    let (status, body) = run
        .api
        .get_json(&format!("/v1/names/ens/{}/children", path_name(parent)))
        .await?;
    assert_eq!(status, 200, "children lookup for {parent} failed: {body}");
    Ok(child_entries(&body))
}

/// Registry-driven declared state under the shipped profile: resolver
/// bindings, registry-only subnames, and resolver-local records, ingested
/// through the active registry admission (registry manifest v3, which also
/// admits the old registry and the resolver/subregistry discovery rules).
#[tokio::test]
async fn registry_driven_reads() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "alice",
        alice,
        365 * 24 * 60 * 60,
        resolver,
    )
    .await?;
    ens_v1::set_addr_record(&rpc, resolver, alice, "alice.eth", alice).await?;
    ens_v1::set_text_record(&rpc, resolver, alice, "alice.eth", "com.twitter", "alice").await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "alice.eth", "sub", bob).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        // Both resolver-local record writes must have been derived before
        // intake stops; they are the last adapter outputs this scenario needs.
        Some(
            "SELECT count(DISTINCT after_state->>'record_key') >= 2 FROM normalized_events \
             WHERE logical_name_id = 'ens:alice.eth' AND event_kind = 'RecordChanged' \
             AND canonicality_state = 'canonical'",
        ),
    )
    .await?;

    // --- layer 2: registry- and resolver-family normalized events ---
    let event_kinds: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT event_kind, source_family FROM normalized_events \
         WHERE logical_name_id = 'ens:alice.eth' AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    for (kind, family) in [
        ("ResolverChanged", "ens_v1_registry_l1"),
        ("AuthorityTransferred", "ens_v1_registry_l1"),
        ("RecordChanged", "ens_v1_resolver_l1"),
    ] {
        assert!(
            event_kinds.iter().any(|(k, f)| k == kind && f == family),
            "expected canonical {kind} from {family} for ens:alice.eth; saw {event_kinds:?}"
        );
    }

    // --- layer 4: exact-name declared state now carries registry facts ---
    let (status, body) = run.api.get_json("/v1/names/ens/alice.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    let pointer = |path: &str| crate::harness::responses::pointer(&body, path);
    assert_eq!(
        pointer("/declared_state/resolver/address"),
        format!("{resolver:#x}"),
        "declared resolver should be the public resolver; body: {body}"
    );
    assert_eq!(
        pointer("/declared_state/resolver/chain_id"),
        "ethereum-mainnet"
    );
    assert_eq!(
        pointer("/declared_state/resolver/latest_event_kind"),
        "ResolverChanged"
    );
    assert_eq!(
        pointer("/declared_state/control/registry_owner"),
        format!("{alice:#x}"),
        "registry owner should be populated from registry facts"
    );
    let selectors = pointer("/declared_state/record_inventory/selectors");
    let record_keys: Vec<&str> = selectors
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.get("record_key").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    for expected in ["addr:60", "text:com.twitter"] {
        assert!(
            record_keys.contains(&expected),
            "expected record inventory selector {expected}; saw {record_keys:?}"
        );
    }

    // --- children: the subname appears as a labelhash placeholder ---
    // setSubnodeOwner carries only the labelhash, so the child stays a
    // bracketed placeholder and no exact-name surface is minted (see
    // docs/architecture.md § Name → children).
    let (status, children) = run.api.get_json("/v1/names/ens/alice.eth/children").await?;
    assert_eq!(status, 200, "children lookup failed: {children}");
    let sub_labelhash = ens_v1::labelhash("sub");
    let entries = children
        .pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        entries.len(),
        1,
        "expected exactly one child; body: {children}"
    );
    let child = &entries[0];
    assert_eq!(
        child.get("labelhash").and_then(Value::as_str),
        Some(format!("{sub_labelhash:#x}").as_str())
    );
    assert_eq!(
        child.get("owner").and_then(Value::as_str),
        Some(format!("{bob:#x}").as_str())
    );
    let child_name = child
        .get("normalized_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        child_name.starts_with('[') && child_name.ends_with(".alice.eth"),
        "expected bracketed placeholder child name; saw {child_name}"
    );

    let (status, _) = run.api.get_json("/v1/names/ens/sub.alice.eth").await?;
    assert_eq!(
        status, 404,
        "unrevealed-label subname must not mint an exact-name surface"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// The same registry labelhash under two different parents is two different
/// child nodes: the parent node participates in `setSubnodeOwner` child-node
/// derivation
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L80 @ ens_v1@91c966f).
#[tokio::test]
async fn same_label_under_two_parents_keeps_children_distinct() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob, carol, dave) = (accounts[1], accounts[2], accounts[3], accounts[4]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "alice",
        alice,
        365 * 24 * 60 * 60,
        resolver,
    )
    .await?;
    ens_v1::register_eth_name(&rpc, &deployment, "bob", bob, 365 * 24 * 60 * 60, resolver).await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "alice.eth", "sub", carol).await?;
    ens_v1::create_subname(&rpc, &deployment, bob, "bob.eth", "sub", dave).await?;

    let sub_labelhash = format!("{:#x}", ens_v1::labelhash("sub"));
    let alice_node = format!("{:#x}", ens_v1::namehash("alice.eth"));
    let bob_node = format!("{:#x}", ens_v1::namehash("bob.eth"));
    let ready_sql = format!(
        "SELECT count(*) >= 2 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' AND canonicality_state = 'canonical' \
         AND lower(after_state->>'labelhash') = '{sub_labelhash}' \
         AND lower(after_state->>'parent_node') IN ('{alice_node}', '{bob_node}') \
         AND lower(after_state->>'owner') IN ('{carol:#x}', '{dave:#x}')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let alice_children = children_for(&run, "alice.eth").await?;
    let bob_children = children_for(&run, "bob.eth").await?;
    assert_eq!(
        alice_children.len(),
        1,
        "alice.eth children: {alice_children:?}"
    );
    assert_eq!(bob_children.len(), 1, "bob.eth children: {bob_children:?}");

    let alice_child = &alice_children[0];
    let bob_child = &bob_children[0];
    let carol_owner = format!("{carol:#x}");
    let dave_owner = format!("{dave:#x}");
    let alice_sub_namehash = format!("{:#x}", ens_v1::namehash("sub.alice.eth"));
    let bob_sub_namehash = format!("{:#x}", ens_v1::namehash("sub.bob.eth"));
    assert_eq!(
        alice_child.get("labelhash").and_then(Value::as_str),
        Some(sub_labelhash.as_str())
    );
    assert_eq!(
        bob_child.get("labelhash").and_then(Value::as_str),
        Some(sub_labelhash.as_str())
    );
    assert_eq!(
        alice_child.get("owner").and_then(Value::as_str),
        Some(carol_owner.as_str())
    );
    assert_eq!(
        bob_child.get("owner").and_then(Value::as_str),
        Some(dave_owner.as_str())
    );
    assert_eq!(
        alice_child.get("namehash").and_then(Value::as_str),
        Some(alice_sub_namehash.as_str())
    );
    assert_eq!(
        bob_child.get("namehash").and_then(Value::as_str),
        Some(bob_sub_namehash.as_str())
    );
    assert_ne!(
        alice_child.get("namehash"),
        bob_child.get("namehash"),
        "same label under different parents must not collapse to one child"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// Registry-only descendants can be created below a placeholder parent by
/// sending the registry the parent node hash; the plaintext parent label is
/// not part of the registry call
/// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L40 @ ens_v1@91c966f).
#[tokio::test]
async fn deep_registry_hierarchy_lists_direct_children_only() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob, carol) = (accounts[1], accounts[2], accounts[3]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "parent",
        alice,
        365 * 24 * 60 * 60,
        resolver,
    )
    .await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "parent.eth", "a", bob).await?;
    ens_v1::create_subname(&rpc, &deployment, bob, "a.parent.eth", "b", carol).await?;

    let a_labelhash = format!("{:#x}", ens_v1::labelhash("a"));
    let b_labelhash = format!("{:#x}", ens_v1::labelhash("b"));
    let a_node = format!("{:#x}", ens_v1::namehash("a.parent.eth"));
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' AND canonicality_state = 'canonical' \
         AND lower(after_state->>'parent_node') = '{a_node}' \
         AND lower(after_state->>'labelhash') = '{b_labelhash}' \
         AND lower(after_state->>'owner') = '{carol:#x}')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let parent_children = children_for(&run, "parent.eth").await?;
    assert_eq!(
        parent_children.len(),
        1,
        "parent.eth should list only its direct child: {parent_children:?}"
    );
    let a_child = &parent_children[0];
    let bob_owner = format!("{bob:#x}");
    let carol_owner = format!("{carol:#x}");
    let b_node = format!("{:#x}", ens_v1::namehash("b.a.parent.eth"));
    assert_eq!(
        a_child.get("labelhash").and_then(Value::as_str),
        Some(a_labelhash.as_str())
    );
    assert_eq!(
        a_child.get("namehash").and_then(Value::as_str),
        Some(a_node.as_str())
    );
    assert_eq!(
        a_child.get("owner").and_then(Value::as_str),
        Some(bob_owner.as_str())
    );
    let a_placeholder = a_child
        .get("normalized_name")
        .and_then(Value::as_str)
        .context("a.parent.eth placeholder missing")?
        .to_owned();

    // The grandchild's registry facts derive fully at depth: a canonical
    // SubregistryChanged exists for b under the a-node (this is also the
    // readiness condition above), owned by carol.
    let b_events: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' AND canonicality_state = 'canonical' \
         AND lower(after_state->>'parent_node') = '{a_node}' \
         AND lower(after_state->>'owner') = '{carol_owner}'"
    ))
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        b_events >= 1,
        "expected canonical SubregistryChanged for b under the a-node"
    );

    // But enumeration stops at unknown surfaces, in two enforced layers:
    // (1) the by-name route rejects bracketed placeholders at the ENSIP-15
    // boundary before any lookup — placeholder names are not addressable;
    let (status, body) = run
        .api
        .get_json(&format!(
            "/v1/names/ens/{}/children",
            path_name(&a_placeholder)
        ))
        .await?;
    assert_eq!(
        status, 400,
        "placeholder names must be rejected by input normalization; body: {body}"
    );
    assert_eq!(
        body.pointer("/error/code").cloned().unwrap_or(Value::Null),
        "invalid_input"
    );
    // (2) children_current materializes rows only under known parent
    // surfaces (docs/architecture.md § Name → children), so the b-child has
    // no projected row at all.
    let b_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM children_current WHERE namehash = $1")
            .bind(&b_node)
            .fetch_one(&run.db.pool)
            .await?;
    assert_eq!(
        b_rows, 0,
        "children under an unrevealed-label parent must not project into children_current"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// A zero-owner subnode assignment is a tombstone in the registry-derived
/// child edge stream; the default children route lists only the current active
/// edge for a child node.
#[tokio::test]
async fn zero_owner_subname_leaves_default_children_listing() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "zero",
        alice,
        365 * 24 * 60 * 60,
        resolver,
    )
    .await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "zero.eth", "sub", bob).await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "zero.eth", "sub", Address::ZERO).await?;

    let parent_node = format!("{:#x}", ens_v1::namehash("zero.eth"));
    let sub_labelhash = format!("{:#x}", ens_v1::labelhash("sub"));
    let zero_owner = format!("{:#x}", Address::ZERO);
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' AND canonicality_state = 'canonical' \
         AND lower(after_state->>'parent_node') = '{parent_node}' \
         AND lower(after_state->>'labelhash') = '{sub_labelhash}' \
         AND lower(after_state->>'owner') = '{zero_owner}' \
         AND (after_state->>'tombstone')::boolean = TRUE)"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let children = children_for(&run, "zero.eth").await?;
    assert!(
        children.is_empty(),
        "zero-owner subname should leave default children listing; saw {children:?}"
    );

    run.db.cleanup().await?;
    Ok(())
}
