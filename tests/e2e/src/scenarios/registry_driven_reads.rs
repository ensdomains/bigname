use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

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
    let pointer = |path: &str| -> Value { body.pointer(path).cloned().unwrap_or(Value::Null) };
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
