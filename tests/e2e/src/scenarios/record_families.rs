use std::collections::BTreeSet;

use alloy_primitives::B256;
use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::responses::selector_keys;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

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

    let ready_sql = support::canonical_event_ready_sql(
        "ens:0x16111066005040f2b6b99f4a4da3e6e3fd7f54de8bd7b33a9a13ad77c51fd920",
        "RecordChanged",
        Some("text:probe"),
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let derived: Vec<(String, Value)> = sqlx::query_as(
        "SELECT event_kind, after_state FROM normalized_events \
         WHERE logical_name_id = 'ens:0x16111066005040f2b6b99f4a4da3e6e3fd7f54de8bd7b33a9a13ad77c51fd920' \
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
    assert_eq!(
        state_for("abi:1", 0)["value"],
        "1",
        "schema-v2 stores the ABI content type in its canonical string form"
    );
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
/// outside the resolver ABI. Live intake retains the watched emitter's raw
/// log, but the adapter's admitted resolver-event set excludes it, so nothing
/// derives.
/// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L29 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/PubkeyResolver.sol:L25 @ ens_v1@91c966f)
#[tokio::test]
async fn pubkey_write_on_admitted_resolver_stays_raw_only() -> Result<()> {
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

    let ready_sql = support::canonical_event_ready_sql(
        "ens:0x358ab11f9359d0ff796131478428e2a032776c290ff60b9b5e34ff00def18fde",
        "RecordChanged",
        Some("text:probe"),
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    // PubkeyChanged(bytes32,bytes32,bytes32) topic0.
    let pubkey_topic = format!(
        "{:#x}",
        alloy_primitives::keccak256("PubkeyChanged(bytes32,bytes32,bytes32)".as_bytes())
    );
    let raw_pubkey_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE emitting_address = $1 AND topics[1] = $2 \
         AND lineage.canonicality_state = 'canonical'",
    )
    .bind(format!("{resolver:#x}"))
    .bind(&pubkey_topic)
    .fetch_one(&run.db.pool)
    .await?;

    // Live intake retains logs from watched emitters even when their topic is
    // outside the active manifest ABI. Raw retention must not be mistaken for
    // normalized or projected admission.
    assert_eq!(
        raw_pubkey_logs, 1,
        "the single PubkeyChanged emission must remain available as a raw fact"
    );

    let derived_keys: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT after_state->>'record_key' FROM normalized_events \
         WHERE logical_name_id = 'ens:0x358ab11f9359d0ff796131478428e2a032776c290ff60b9b5e34ff00def18fde' \
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
