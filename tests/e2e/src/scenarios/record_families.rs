use std::collections::BTreeSet;

use alloy_primitives::B256;
use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

fn selector_keys(body: &Value) -> BTreeSet<String> {
    body.pointer("/declared_state/record_inventory/selectors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("record_key").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

/// One DNS resource record in wire format: dns-encoded name, type, class IN,
/// ttl, rdlength, rdata. Empty rdata deletes the RRset
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L51 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L186 @ ens_v1@91c966f).
fn dns_rr(name: &str, rtype: u16, ttl: u32, rdata: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for label in name.trim_end_matches('.').split('.') {
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out.extend_from_slice(&rtype.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&ttl.to_be_bytes());
    out.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
    out.extend_from_slice(rdata);
    out
}

/// The remaining admitted record families (ABI, interface, DNS RRset +
/// deletion, zonehash, forward name()) derive at the normalized layer, while
/// the inventory keeps them family-only — no keyed selectors.
#[tokio::test]
async fn remaining_record_families_derive_normalized_but_stay_unenumerated() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, implementer) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "families", alice, YEAR, resolver).await?;
    let node = ens_v1::namehash("families.eth");
    ens_v1::set_abi_record(&rpc, resolver, alice, "families.eth", 1, b"[]").await?;
    ens_v1::set_interface_record(
        &rpc,
        resolver,
        alice,
        "families.eth",
        [0x90, 0x61, 0xb9, 0x23],
        implementer,
    )
    .await?;
    ens_v1::set_dns_records(
        &rpc,
        resolver,
        alice,
        "families.eth",
        &dns_rr("a.families.eth.", 1, 300, &[1, 2, 3, 4]),
    )
    .await?;
    ens_v1::set_zonehash(&rpc, resolver, alice, "families.eth", &[0xde, 0xad]).await?;
    ens_v1::set_name_record_for_node(&rpc, resolver, alice, node, "families.eth").await?;
    ens_v1::set_dns_records(
        &rpc,
        resolver,
        alice,
        "families.eth",
        &dns_rr("a.families.eth.", 1, 300, &[]),
    )
    .await?;
    ens_v1::set_text_record(&rpc, resolver, alice, "families.eth", "probe", "done").await?;

    let ready_sql = "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:families.eth' \
         AND event_kind = 'RecordChanged' \
         AND after_state->>'record_key' = 'text:probe' \
         AND canonicality_state = 'canonical')";
    let run = support::ingest_and_serve(&anvil, &deployment, Some(ready_sql)).await?;

    let derived: Vec<(String, Value)> = sqlx::query_as(
        "SELECT event_kind, after_state FROM normalized_events \
         WHERE logical_name_id = 'ens:families.eth' \
         AND source_family = 'ens_v1_resolver_l1' \
         AND canonicality_state = 'canonical' \
         ORDER BY block_number, log_index",
    )
    .fetch_all(&run.db.pool)
    .await?;
    let record_keys: Vec<String> = derived
        .iter()
        .filter(|(kind, _)| kind == "RecordChanged")
        .filter_map(|(_, state)| state.get("record_key").and_then(Value::as_str))
        .map(str::to_owned)
        .collect();
    let dns_key = "dns:1:0x01610866616d696c6965730365746800";
    assert_eq!(
        record_keys,
        vec![
            "abi:1",
            "interface:0x9061b923",
            dns_key,
            "dns:zonehash",
            "name",
            dns_key,
            "text:probe",
        ],
        "keyed derivation across the remaining families: {derived:?}"
    );
    let state_for = |key: &str, nth: usize| -> &Value {
        &derived
            .iter()
            .filter(|(kind, state)| kind == "RecordChanged" && state["record_key"] == key)
            .nth(nth)
            .unwrap_or_else(|| panic!("missing {key} #{nth}"))
            .1
    };
    assert_eq!(state_for("abi:1", 0)["value"], 1, "abi carries contentType");
    assert_eq!(
        state_for("interface:0x9061b923", 0)["value"],
        format!("{implementer:#x}"),
        "interface carries the implementer"
    );
    assert_eq!(
        state_for(dns_key, 0)["value"]["bytes"],
        "0x01610866616d696c6965730365746800000100010000012c000401020304",
        "dns change carries the wire RRset"
    );
    assert_eq!(
        state_for(dns_key, 1)["value"]["deleted"],
        true,
        "DNSRecordDeleted derives as supersession-by-delete on the same key"
    );
    assert_eq!(
        state_for("dns:zonehash", 0)["value"]["current"]["bytes"],
        "0xdead"
    );
    assert_eq!(
        state_for("dns:zonehash", 0)["value"]["previous"]["bytes"],
        "0x"
    );
    assert_eq!(
        state_for("name", 0)["raw_name"],
        "families.eth",
        "forward name() derives as a record, not a reverse claim"
    );
    assert_eq!(state_for("name", 0)["selector_key"], Value::Null);

    // The projection enumerates selectors only for addr/text/contenthash
    // families; the keyed families above stay out of the inventory.
    let (status, exact) = run.api.get_json("/v1/names/ens/families.eth").await?;
    assert_eq!(status, 200, "families.eth lookup failed: {exact}");
    assert_eq!(
        selector_keys(&exact),
        BTreeSet::from(["text:probe".to_owned()]),
        "keyed families must stay unenumerated: {exact}"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// setPubkey on the admitted PublicResolver: the only composed-profile event
/// outside the resolver ABI. The gate rejects it by design (a tested
/// exclusion), so nothing derives; whether the raw log even persists is
/// pinned here. Drift-vs-narrowing stays a doc-first question — no
/// divergence entry names pubkey.
#[tokio::test]
async fn pubkey_write_on_admitted_resolver_stays_invisible() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "pubkey", alice, YEAR, resolver).await?;
    ens_v1::set_pubkey_record(
        &rpc,
        resolver,
        alice,
        "pubkey.eth",
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x22),
    )
    .await?;
    ens_v1::set_text_record(&rpc, resolver, alice, "pubkey.eth", "probe", "done").await?;

    let ready_sql = "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:pubkey.eth' \
         AND event_kind = 'RecordChanged' \
         AND after_state->>'record_key' = 'text:probe' \
         AND canonicality_state = 'canonical')";
    let run = support::ingest_and_serve(&anvil, &deployment, Some(ready_sql)).await?;

    // PubkeyChanged(bytes32,bytes32,bytes32) topic0.
    let pubkey_topic = format!(
        "{:#x}",
        alloy_primitives::keccak256("PubkeyChanged(bytes32,bytes32,bytes32)".as_bytes())
    );
    let raw_pubkey_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE emitting_address = $1 AND topics[1] = $2",
    )
    .bind(format!("{resolver:#x}"))
    .bind(&pubkey_topic)
    .fetch_one(&run.db.pool)
    .await?;

    // The on-chain write succeeded, but the live scan is topic-filtered by
    // the manifest ABI: the pubkey log never even persists as a raw fact.
    assert_eq!(
        raw_pubkey_logs, 0,
        "PubkeyChanged is invisible at the raw layer by admission"
    );

    let derived_keys: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT after_state->>'record_key' FROM normalized_events \
         WHERE logical_name_id = 'ens:pubkey.eth' \
         AND event_kind = 'RecordChanged' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(derived_keys, vec!["text:probe".to_owned()]);

    let (status, exact) = run.api.get_json("/v1/names/ens/pubkey.eth").await?;
    assert_eq!(status, 200, "pubkey.eth lookup failed: {exact}");
    for section in ["selectors", "explicit_gaps", "unsupported_families"] {
        let families: Vec<Value> = exact
            .pointer(&format!("/declared_state/record_inventory/{section}"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|entry| entry["record_family"] == "pubkey" || *entry == "pubkey")
            .cloned()
            .collect();
        assert!(
            families.is_empty(),
            "pubkey family must not surface in inventory {section}: {exact}"
        );
    }
    assert_eq!(
        selector_keys(&exact),
        BTreeSet::from(["text:probe".to_owned()])
    );

    run.db.cleanup().await?;
    Ok(())
}
