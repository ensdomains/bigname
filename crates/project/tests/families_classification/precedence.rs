//! Which family an address that both protocols name is filed under. A pointer from either
//! protocol names any address, so only an ENSv2 declaration or an `Upgraded` to a declared
//! implementation keeps an address with ENSv1 evidence in the ENSv2 family, and an ENSv2
//! declaration takes rank 0 from any candidate in its namespace, edge or not. Each case names
//! its rows of the input-class table and ends with the undo and rebuild checks.
use anyhow::Result;
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions},
};
use serde_json::{Value, json};

use super::{REGISTRY as V1_REGISTRY, columns, edge, manifest, namespaced_manifest, node, pointer};
use crate::families_support::{CHAIN, CONTENT_HASH, Fixture, hash, uuid};

const V2_REGISTRY: &str = "0x00000000000000000000000000000000000000e2";
const IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000c1";
const UNDECLARED_IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000c2";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const ETH_NODE: &str = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae";
const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];

/// The fixture address numbered `n`.
fn address(n: u32) -> String {
    format!("0x{n:040x}")
}

struct Manifests {
    v1_resolver: i64,
    v2_resolver: i64,
    v2_registry: i64,
}

/// The ENS-namespace resolver and registry manifests at block 1: the ENSv1 resolver manifest
/// declares `v1_declared`, the ENSv2 one declares each `(address, role)` of `v2_declared` and
/// admits `IMPLEMENTATION`.
async fn manifests(
    fixture: &Fixture,
    v1_declared: &[&str],
    v2_declared: &[(&str, &str)],
) -> Result<Manifests> {
    let v1: Vec<Value> = v1_declared
        .iter()
        .map(|address| json!({"address": address, "role": "public_resolver"}))
        .collect();
    let v1_resolver = manifest(
        fixture,
        None,
        "ens_v1_resolver_l1",
        1,
        json!({"contracts": v1}),
    )
    .await?;
    let v2_resolver = manifest(
        fixture,
        None,
        "ens_v2_resolver_l1",
        1,
        v2_payload(v2_declared),
    )
    .await?;
    manifest(
        fixture,
        None,
        "ens_v1_registry_l1",
        1,
        json!({"contracts": []}),
    )
    .await?;
    let v2_registry = manifest(
        fixture,
        None,
        "ens_v2_registry_l1",
        1,
        json!({"contracts": []}),
    )
    .await?;
    Ok(Manifests {
        v1_resolver,
        v2_resolver,
        v2_registry,
    })
}

fn v2_payload(declared: &[(&str, &str)]) -> Value {
    let contracts: Vec<Value> = declared
        .iter()
        .map(|(address, role)| json!({"address": address, "role": role, "proxy_kind": "none"}))
        .collect();
    json!({
        "correlation_addresses": {"ens_v1_registry": V1_REGISTRY},
        "contracts": contracts,
        "resolver_implementations": [
            {"address": IMPLEMENTATION, "role": "permissioned_resolver",
             "read_features": ["addr", "text"]}
        ]
    })
}

/// A contract at `address` from before the fixture's first block. Returns its instance id.
async fn target(fixture: &Fixture, address: &str) -> Result<String> {
    let instance = uuid(u32::from_str_radix(&address[address.len() - 6..], 16)? + 0x10_0000);
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1::uuid, $2, 'contract')",
    )
    .bind(&instance)
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address)
         VALUES ($1::uuid, $2, $3)",
    )
    .bind(&instance)
    .bind(CHAIN)
    .bind(address)
    .execute(&fixture.pool)
    .await?;
    Ok(instance)
}

/// An ENSv2 registry `ResolverChanged` of entry `entry` to `resolver`, on the entry's resource,
/// so it writes an F5 row of the ENSv2 family.
async fn v2_event(
    fixture: &Fixture,
    block: i64,
    log: i64,
    entry: u32,
    resolver: &str,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "ResolverChanged",
            "ens_v2_registry_l1",
            None,
            Some(&uuid(0x40_0000 + entry)),
            json!({"node": node(entry.into()), "resolver": resolver}),
            V2_REGISTRY,
        )
        .await?;
    Ok(())
}

/// [`v2_event`] and the `resolver` edge the ENSv2 registry manifest writes for it, active from
/// `block` until `until`.
#[allow(clippy::too_many_arguments)]
async fn v2_pointer(
    fixture: &Fixture,
    m: &Manifests,
    block: i64,
    log: i64,
    entry: u32,
    resolver: &str,
    instance: &str,
    until: Option<i64>,
) -> Result<()> {
    v2_event(fixture, block, log, entry, resolver).await?;
    edge(
        fixture,
        0x50_0000 + entry,
        instance,
        m.v2_registry,
        Some(block),
        until,
        false,
    )
    .await
}

async fn upgraded(
    fixture: &Fixture,
    block: i64,
    log: i64,
    family: &str,
    proxy: &str,
    implementation: &str,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "Upgraded",
            family,
            None,
            None,
            json!({"proxy_address": proxy, "implementation": implementation}),
            proxy,
        )
        .await?;
    Ok(())
}

/// The served fields of `address`'s classification row, null when it has none.
async fn served(fixture: &Fixture, address: &str) -> Result<Value> {
    Ok(fixture
        .rows("project_resolver_classification")
        .await?
        .into_iter()
        .find(|row| row["resolver_address"] == json!(address))
        .map_or(Value::Null, |row| {
            columns(
                &row,
                &[
                    "classification",
                    "support_status",
                    "unsupported_reason",
                    "manifest_id",
                    "admission_namespace",
                ],
            )
        }))
}

/// Family, role, support and reason of [`served`] fields.
fn brief(served: &Value) -> Value {
    json!({
        "family": served["classification"]["source_family"],
        "role": served["classification"]["role"],
        "support": served["support_status"],
        "reason": served["unsupported_reason"],
    })
}

fn filed(family: &str, role: Option<&str>, reason: Option<&str>) -> Value {
    json!({
        "family": family,
        "role": role,
        "support": if reason.is_none() { "supported" } else { "unsupported" },
        "reason": reason,
    })
}

fn v1_undeclared() -> Value {
    filed("ens_v1_resolver_l1", None, Some("resolver_not_declared"))
}

fn v2_unknown() -> Value {
    filed(
        "ens_v2_resolver_l1",
        None,
        Some("resolver_implementation_unknown"),
    )
}

async fn plan_mode(fixture: &Fixture, mode: &str) -> Result<()> {
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&fixture.pool)
        .await?;
    sqlx::raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET plan_cache_mode = {mode}; SET plan_cache_mode = {mode}",
        name.replace('"', r#""""#)
    ))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// A1, U1, A3, A4, A1x, A6: an address an ENSv1 name points at stays ENSv1 when ENSv2 entries
// point at it too. The ENSv2 events and edges arrive at block 8, after the ENSv1 pointers of
// block 5 (A1, three ENSv2 entries; U1, three ENSv1 pointers and one entry), in the same block
// (A3), beside a registry proxy's `Upgraded` (A4), beside an `Upgraded` to an undeclared
// implementation (A1x), or as a creation self-edge of the ENSv2 resolver manifest (A6).
#[tokio::test]
async fn an_ensv2_pointer_leaves_an_address_with_ensv1_evidence_in_ensv1() -> Result<()> {
    for mode in PLAN_MODES {
        let fixture = Fixture::new(&format!("classification_rb_{mode}"), 20).await?;
        plan_mode(&fixture, mode).await?;
        let m = manifests(&fixture, &[], &[]).await?;
        let [a1, u1, a3, a4, a1x, a6] = [0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6].map(address);
        let mut instance = std::collections::BTreeMap::new();
        for address in [&a1, &u1, &a3, &a4, &a1x, &a6] {
            instance.insert(address.clone(), target(&fixture, address).await?);
        }
        pointer(&fixture, 5, 1, 1, &a1).await?;
        for (log, n) in [(2, 2), (3, 3), (4, 4)] {
            pointer(&fixture, 5, log, n, &u1).await?;
        }
        pointer(&fixture, 5, 5, 5, &a4).await?;
        pointer(&fixture, 5, 6, 6, &a1x).await?;
        pointer(&fixture, 5, 7, 7, &a6).await?;
        for (log, entry) in [(1, 11), (2, 12), (3, 13)] {
            v2_pointer(&fixture, &m, 8, log, entry, &a1, &instance[&a1], None).await?;
        }
        v2_pointer(&fixture, &m, 8, 4, 14, &u1, &instance[&u1], None).await?;
        pointer(&fixture, 8, 5, 8, &a3).await?;
        v2_pointer(&fixture, &m, 8, 6, 15, &a3, &instance[&a3], None).await?;
        v2_pointer(&fixture, &m, 8, 7, 16, &a4, &instance[&a4], None).await?;
        upgraded(&fixture, 8, 8, "ens_v2_registry_l1", &a4, IMPLEMENTATION).await?;
        v2_pointer(&fixture, &m, 8, 9, 17, &a1x, &instance[&a1x], None).await?;
        upgraded(
            &fixture,
            8,
            10,
            "ens_v2_resolver_l1",
            &a1x,
            UNDECLARED_IMPLEMENTATION,
        )
        .await?;
        edge(
            &fixture,
            0x60_0006,
            &instance[&a6],
            m.v2_resolver,
            Some(8),
            None,
            false,
        )
        .await?;

        fixture.apply(7, FamilyMode::Normal).await?;
        let mut before = std::collections::BTreeMap::new();
        for (row, address) in [
            ("A1", &a1),
            ("U1", &u1),
            ("A4", &a4),
            ("A1x", &a1x),
            ("A6", &a6),
        ] {
            let fields = served(&fixture, address).await?;
            assert_eq!(brief(&fields), v1_undeclared(), "{row} at 7 ({mode})");
            assert_eq!(
                fields["manifest_id"],
                json!(m.v1_resolver),
                "{row} at 7 ({mode})"
            );
            before.insert(row, fields);
        }
        fixture.apply(8, FamilyMode::Normal).await?;
        for (row, address) in [
            ("A1", &a1),
            ("U1", &u1),
            ("A4", &a4),
            ("A1x", &a1x),
            ("A6", &a6),
        ] {
            assert_eq!(
                served(&fixture, address).await?,
                before[row],
                "{row}: the ENSv2 pointers at 8 re-file nothing ({mode})"
            );
        }
        assert_eq!(
            brief(&served(&fixture, &a3).await?),
            v1_undeclared(),
            "A3: both protocols in one block ({mode})"
        );
        fixture.assert_undo_restores(8).await?;
        fixture.assert_rebuild_equal(8).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

// A1u, A2u: an `Upgraded` to a declared implementation is ENSv2 evidence, whichever protocol
// pointed first.
#[tokio::test]
async fn an_upgraded_proxy_stays_ensv2_beside_ensv1_evidence() -> Result<()> {
    let fixture = Fixture::new("classification_rb_upgraded", 20).await?;
    let m = manifests(&fixture, &[], &[]).await?;
    let [a1u, a2u] = [0xc1_u32, 0xc2].map(|n| address(0x1000 + n));
    let (i1, i2) = (target(&fixture, &a1u).await?, target(&fixture, &a2u).await?);
    pointer(&fixture, 5, 1, 1, &a1u).await?;
    v2_pointer(&fixture, &m, 5, 2, 21, &a2u, &i2, None).await?;
    upgraded(&fixture, 5, 3, "ens_v2_resolver_l1", &a2u, IMPLEMENTATION).await?;
    v2_pointer(&fixture, &m, 8, 1, 22, &a1u, &i1, None).await?;
    upgraded(&fixture, 8, 2, "ens_v2_resolver_l1", &a1u, IMPLEMENTATION).await?;
    pointer(&fixture, 8, 3, 2, &a2u).await?;
    let supported = filed("ens_v2_resolver_l1", Some("permissioned_resolver"), None);

    fixture.apply(5, FamilyMode::Normal).await?;
    assert_eq!(
        brief(&served(&fixture, &a1u).await?),
        v1_undeclared(),
        "A1u at 5"
    );
    assert_eq!(brief(&served(&fixture, &a2u).await?), supported, "A2u at 5");
    fixture.apply(8, FamilyMode::Normal).await?;
    assert_eq!(
        brief(&served(&fixture, &a1u).await?),
        supported,
        "A1u: one flip, at 8"
    );
    assert_eq!(
        brief(&served(&fixture, &a2u).await?),
        supported,
        "A2u stays ENSv2"
    );
    fixture.assert_undo_restores(8).await?;
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

// A2, A5, R1: ENSv1 evidence re-files an ENSv2-only address once, and later ENSv1 pointer moves
// never move it back. A5's ENSv1 pointer is gone before the ENSv2 one arrives: the observed
// event alone is the evidence. The rebuild checks run the history in ranges and block by block.
#[tokio::test]
async fn ensv1_evidence_refiles_an_ensv2_address_once() -> Result<()> {
    let fixture = Fixture::new("classification_rb_once", 20).await?;
    let m = manifests(&fixture, &[], &[]).await?;
    let [a2, a5, other] = [0xd2_u32, 0xd5, 0xdf].map(|n| address(0x1000 + n));
    let (i2, i5) = (target(&fixture, &a2).await?, target(&fixture, &a5).await?);
    v2_pointer(&fixture, &m, 3, 1, 31, &a2, &i2, None).await?;
    pointer(&fixture, 3, 2, 5, &a5).await?;
    pointer(&fixture, 4, 1, 5, &other).await?;
    pointer(&fixture, 6, 1, 2, &a2).await?;
    v2_pointer(&fixture, &m, 6, 2, 32, &a5, &i5, None).await?;
    pointer(&fixture, 9, 1, 2, &other).await?;
    pointer(&fixture, 12, 1, 2, &a2).await?;

    fixture.apply(3, FamilyMode::Normal).await?;
    assert_eq!(
        brief(&served(&fixture, &a2).await?),
        v2_unknown(),
        "A2 at 3"
    );
    fixture.apply(6, FamilyMode::Normal).await?;
    let a2_at_6 = served(&fixture, &a2).await?;
    assert_eq!(
        brief(&a2_at_6),
        v1_undeclared(),
        "A2: the ENSv1 pointer re-files it"
    );
    assert_eq!(
        brief(&served(&fixture, &a5).await?),
        v1_undeclared(),
        "A5: an observed ENSv1 pointer is evidence"
    );
    fixture.apply(9, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &a2).await?,
        a2_at_6,
        "A2: the ENSv1 pointer leaving does not re-file it"
    );
    fixture.apply(12, FamilyMode::Normal).await?;
    assert_eq!(served(&fixture, &a2).await?, a2_at_6, "A2: nor its return");
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// V1, V1d, V2, V2u, D1, C2, N1: addresses one protocol names keep its family. D1's ENSv1
// declaration takes rank 0 from an ENSv2 registry edge. C2's ENSv2 event names no entry and has
// no edge. N1 is ENSv2-declared, but only a Basenames pointer names it, so no ENS candidate
// admits the declaration.
#[tokio::test]
async fn an_address_one_protocol_names_keeps_its_family() -> Result<()> {
    let fixture = Fixture::new("classification_rb_single", 20).await?;
    let [v1, v1d, v2, v2u, d1, c2, n1] =
        [0xe1_u32, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7].map(|n| address(0x1000 + n));
    let m = manifests(&fixture, &[&v1d, &d1], &[(&n1, "public_resolver_v2")]).await?;
    namespaced_manifest(
        &fixture,
        "basenames",
        "basenames_base_resolver",
        json!({"contracts": []}),
        "active",
    )
    .await?;
    let (i2, i2u, id1) = (
        target(&fixture, &v2).await?,
        target(&fixture, &v2u).await?,
        target(&fixture, &d1).await?,
    );
    pointer(&fixture, 5, 1, 1, &v1).await?;
    pointer(&fixture, 5, 2, 2, &v1d).await?;
    v2_pointer(&fixture, &m, 5, 3, 41, &v2, &i2, None).await?;
    v2_pointer(&fixture, &m, 5, 4, 42, &v2u, &i2u, None).await?;
    upgraded(&fixture, 5, 5, "ens_v2_resolver_l1", &v2u, IMPLEMENTATION).await?;
    edge(
        &fixture,
        0x60_00e5,
        &id1,
        m.v2_registry,
        Some(5),
        None,
        false,
    )
    .await?;
    fixture
        .write(
            5,
            6,
            "ResolverChanged",
            "ens_v2_registry_l1",
            None,
            None,
            json!({"node": node(43), "resolver": c2}),
            V2_REGISTRY,
        )
        .await?;
    fixture
        .write(
            5,
            7,
            "ResolverChanged",
            "basenames_base_registry",
            None,
            None,
            json!({"node": node(44), "resolver": n1}),
            V1_REGISTRY,
        )
        .await?;

    fixture.apply(5, FamilyMode::Normal).await?;
    let public = Some("public_resolver");
    for (row, address, expected) in [
        ("V1", &v1, v1_undeclared()),
        ("V1d", &v1d, filed("ens_v1_resolver_l1", public, None)),
        ("V2", &v2, v2_unknown()),
        (
            "V2u",
            &v2u,
            filed("ens_v2_resolver_l1", Some("permissioned_resolver"), None),
        ),
        ("D1", &d1, filed("ens_v1_resolver_l1", public, None)),
        ("C2", &c2, v2_unknown()),
        (
            "N1",
            &n1,
            filed(
                "basenames_base_resolver",
                None,
                Some("resolver_not_declared"),
            ),
        ),
    ] {
        assert_eq!(brief(&served(&fixture, address).await?), expected, "{row}");
    }
    assert_eq!(
        served(&fixture, &d1).await?["admission_namespace"],
        json!("ens"),
        "D1"
    );
    assert_eq!(
        served(&fixture, &n1).await?["admission_namespace"],
        Value::Null,
        "N1"
    );
    fixture.assert_undo_restores(5).await?;
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

// M1, M2, P1: a declared ENSv2 resolver is never demoted. With an edge (M1, the first P1
// address) it keeps rank 0 when an ENSv1 name points at it. With only ENSv2 F5 pointers (M2, the
// second P1 address) its ENSv2 pointer admits the declaration at rank 0, so the ENSv1 pointer
// cannot win on family name order.
#[tokio::test]
async fn a_declared_ensv2_resolver_is_never_demoted() -> Result<()> {
    for mode in PLAN_MODES {
        let fixture = Fixture::new(&format!("classification_rb_declared_{mode}"), 20).await?;
        plan_mode(&fixture, mode).await?;
        let [m1, m2, p1, p1b] = [0xf1_u32, 0xf2, 0xf3, 0xf4].map(|n| address(0x1000 + n));
        let mirror = "ensv1_mirror_resolver";
        let public_v2 = "public_resolver_v2";
        let m = manifests(
            &fixture,
            &[],
            &[
                (&m1, mirror),
                (&m2, mirror),
                (&p1, public_v2),
                (&p1b, public_v2),
            ],
        )
        .await?;
        let (i1, ip) = (target(&fixture, &m1).await?, target(&fixture, &p1).await?);
        v2_pointer(&fixture, &m, 3, 1, 51, &m1, &i1, None).await?;
        v2_event(&fixture, 3, 2, 52, &m2).await?;
        v2_pointer(&fixture, &m, 3, 3, 53, &p1, &ip, None).await?;
        v2_event(&fixture, 3, 4, 54, &p1b).await?;
        for (log, n, address) in [(1, 1, &m1), (2, 2, &m2), (3, 3, &p1), (4, 4, &p1b)] {
            pointer(&fixture, 6, log, n, address).await?;
        }
        let expected = [
            ("M1", &m1, mirror),
            ("M2", &m2, mirror),
            ("P1", &p1, public_v2),
            ("P1 without an edge", &p1b, public_v2),
        ];

        fixture.apply(3, FamilyMode::Normal).await?;
        let mut before = std::collections::BTreeMap::new();
        for (row, address, role) in expected {
            let fields = served(&fixture, address).await?;
            assert_eq!(
                brief(&fields),
                filed("ens_v2_resolver_l1", Some(role), None),
                "{row} at 3 ({mode})"
            );
            assert_eq!(
                columns(&fields, &["manifest_id", "admission_namespace"]),
                json!({"manifest_id": m.v2_resolver, "admission_namespace": "ens"}),
                "{row}: rank 0 at 3 ({mode})"
            );
            before.insert(row, fields);
        }
        fixture.apply(6, FamilyMode::Normal).await?;
        for (row, address, _) in expected {
            assert_eq!(
                served(&fixture, address).await?,
                before[row],
                "{row}: an ENSv1 pointer leaves it as it was ({mode})"
            );
        }
        fixture.assert_undo_restores(6).await?;
        fixture.assert_rebuild_equal(6).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

// E1, E2, C3, X2: the `eth` node's ENSv1 resolver, which three ENSv2 entries also name, stays
// ENSv1 while their pointers and edges come (block 5) and go (block 8). An ENSv2-declared
// address an ENSv1 name points at is supported throughout while an ENSv2 edge to it opens at 5
// and closes at 8 (C3). An ENSv2 manifest update at 11 declares the `eth` resolver: it alone
// changes, with no ENSv2 edge to it left (X2).
#[tokio::test]
async fn an_ensv2_declaration_holds_while_the_edges_to_it_change() -> Result<()> {
    for mode in PLAN_MODES {
        let fixture = Fixture::new(&format!("classification_rb_edges_{mode}"), 20).await?;
        plan_mode(&fixture, mode).await?;
        let [eth, c3] = [0xa1_u32, 0xa3].map(|n| address(0x2000 + n));
        let declared = [(c3.as_str(), "public_resolver_v2")];
        let m = manifests(&fixture, &[], &declared).await?;
        let (ie, ic) = (target(&fixture, &eth).await?, target(&fixture, &c3).await?);
        fixture
            .write(
                2,
                1,
                "ResolverChanged",
                "ens_v1_registry_l1",
                None,
                Some(&uuid(0x7e7)),
                json!({"node": ETH_NODE, "resolver": eth}),
                V1_REGISTRY,
            )
            .await?;
        pointer(&fixture, 2, 2, 1, &c3).await?;
        for (log, entry) in [(1, 61), (2, 62), (3, 63)] {
            v2_pointer(&fixture, &m, 5, log, entry, &eth, &ie, Some(8)).await?;
            v2_event(&fixture, 8, log, entry, ZERO).await?;
        }
        edge(
            &fixture,
            0x60_00a3,
            &ic,
            m.v2_registry,
            Some(5),
            Some(8),
            false,
        )
        .await?;
        let c3_supported = filed("ens_v2_resolver_l1", Some("public_resolver_v2"), None);

        fixture.apply(2, FamilyMode::Normal).await?;
        let eth_at_2 = served(&fixture, &eth).await?;
        assert_eq!(brief(&eth_at_2), v1_undeclared(), "E1 at 2 ({mode})");
        let c3_at_2 = served(&fixture, &c3).await?;
        assert_eq!(brief(&c3_at_2), c3_supported, "C3 at 2 ({mode})");
        for block in [5, 8, 10] {
            fixture.apply(block, FamilyMode::Normal).await?;
            assert_eq!(
                served(&fixture, &eth).await?,
                eth_at_2,
                "E1/E2 at {block}: the ENSv2 entries re-file nothing ({mode})"
            );
            assert_eq!(
                served(&fixture, &c3).await?,
                c3_at_2,
                "C3 at {block}: the edge changes nothing ({mode})"
            );
        }

        let rehearsal = [
            (c3.as_str(), "public_resolver_v2"),
            (eth.as_str(), "ensv1_mirror_resolver"),
        ];
        manifest(
            &fixture,
            Some(m.v2_resolver),
            "ens_v2_resolver_l1",
            11,
            v2_payload(&rehearsal),
        )
        .await?;
        fixture.apply(11, FamilyMode::Normal).await?;
        let eth_at_11 = served(&fixture, &eth).await?;
        assert_eq!(
            brief(&eth_at_11),
            filed("ens_v2_resolver_l1", Some("ensv1_mirror_resolver"), None),
            "X2: the declaration holds with no ENSv2 edge to it ({mode})"
        );
        assert_eq!(
            eth_at_11["admission_namespace"],
            json!("ens"),
            "X2 ({mode})"
        );
        assert_eq!(
            served(&fixture, &c3).await?,
            c3_at_2,
            "X2: no other row changes ({mode})"
        );
        fixture.assert_undo_restores(11).await?;
        fixture.assert_rebuild_equal(11).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

// X1: a manifest update that declares one undeclared ENSv1 address re-files that address alone.
// Every address that both protocols name, and the declared ENSv2 resolver an ENSv1 name points
// at, keeps its fields.
#[tokio::test]
async fn a_manifest_update_refiles_only_the_address_it_declares() -> Result<()> {
    for mode in PLAN_MODES {
        let fixture = Fixture::new(&format!("classification_rb_manifest_{mode}"), 20).await?;
        plan_mode(&fixture, mode).await?;
        let [a1, a2, m1, v2, v1] = [0xb1_u32, 0xb2, 0xb3, 0xb4, 0xb5].map(|n| address(0x3000 + n));
        let m = manifests(&fixture, &[], &[(&m1, "ensv1_mirror_resolver")]).await?;
        let mut instance = std::collections::BTreeMap::new();
        for address in [&a1, &a2, &m1, &v2] {
            instance.insert(address.clone(), target(&fixture, address).await?);
        }
        pointer(&fixture, 3, 1, 1, &a1).await?;
        v2_pointer(&fixture, &m, 3, 2, 71, &a2, &instance[&a2], None).await?;
        v2_pointer(&fixture, &m, 3, 3, 72, &m1, &instance[&m1], None).await?;
        v2_pointer(&fixture, &m, 3, 4, 73, &v2, &instance[&v2], None).await?;
        pointer(&fixture, 3, 5, 2, &v1).await?;
        v2_pointer(&fixture, &m, 5, 1, 74, &a1, &instance[&a1], None).await?;
        pointer(&fixture, 5, 2, 3, &a2).await?;
        pointer(&fixture, 5, 3, 4, &m1).await?;
        manifest(
            &fixture,
            Some(m.v1_resolver),
            "ens_v1_resolver_l1",
            8,
            json!({"contracts": [{"address": v1, "role": "public_resolver"}]}),
        )
        .await?;

        fixture.apply(7, FamilyMode::Normal).await?;
        let mut before = std::collections::BTreeMap::new();
        for address in [&a1, &a2, &m1, &v2, &v1] {
            before.insert(address.clone(), served(&fixture, address).await?);
        }
        fixture.apply(8, FamilyMode::Normal).await?;
        for (row, address) in [("A1", &a1), ("A2", &a2), ("M1", &m1), ("V2", &v2)] {
            assert_eq!(
                served(&fixture, address).await?,
                before[address],
                "{row} keeps its fields across the manifest update ({mode})"
            );
        }
        assert_eq!(
            brief(&served(&fixture, &v1).await?),
            filed("ens_v1_resolver_l1", Some("public_resolver"), None),
            "V1: its new declaration supports it ({mode})"
        );
        fixture.assert_undo_restores(8).await?;
        fixture.assert_rebuild_equal(8).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

// O1, O2, O3: a reorg that drops block 8 drops what it brought. O1's ENSv2 pointer and edge and
// O2's `Upgraded` go, and O3's ENSv1 pointer goes with its observation, so O3 is ENSv2 again.
#[tokio::test]
async fn a_reorg_across_the_evidence_restores_the_rows() -> Result<()> {
    let fixture = Fixture::new("classification_rb_reorg", 20).await?;
    let m = manifests(&fixture, &[], &[]).await?;
    let [o1, o2, o3] = [0xa1_u32, 0xa2, 0xa3].map(|n| address(0x4000 + n));
    let (i1, i2, i3) = (
        target(&fixture, &o1).await?,
        target(&fixture, &o2).await?,
        target(&fixture, &o3).await?,
    );
    pointer(&fixture, 5, 1, 1, &o1).await?;
    pointer(&fixture, 5, 2, 2, &o2).await?;
    v2_pointer(&fixture, &m, 5, 3, 81, &o3, &i3, None).await?;
    v2_pointer(&fixture, &m, 8, 1, 82, &o1, &i1, None).await?;
    v2_pointer(&fixture, &m, 8, 2, 83, &o2, &i2, None).await?;
    upgraded(&fixture, 8, 3, "ens_v2_resolver_l1", &o2, IMPLEMENTATION).await?;
    pointer(&fixture, 8, 4, 3, &o3).await?;

    fixture.apply(7, FamilyMode::Normal).await?;
    let mut before = Vec::new();
    for address in [&o1, &o2, &o3] {
        before.push(served(&fixture, address).await?);
    }
    fixture.apply(8, FamilyMode::Normal).await?;
    assert_eq!(
        brief(&served(&fixture, &o2).await?),
        filed("ens_v2_resolver_l1", Some("permissioned_resolver"), None),
        "O2 at 8"
    );
    assert_eq!(
        brief(&served(&fixture, &o3).await?),
        v1_undeclared(),
        "O3 at 8"
    );

    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 8",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         VALUES ($1, '0xreplacement8', $2, 8, to_timestamp(1800000096), 'canonical')",
    )
    .bind(CHAIN)
    .bind(hash(7))
    .execute(&fixture.pool)
    .await?;
    let token = families::input_token(&fixture.pool, CHAIN).await?;
    let outcome = families::apply(
        &fixture.pool,
        CHAIN,
        &Marker {
            number: 8,
            hash: "0xreplacement8".to_owned(),
        },
        FamilyMode::Normal,
        &token,
        &FamilyOptions::new(CONTENT_HASH),
    )
    .await?;
    assert_eq!(outcome.undone_blocks, 1);
    for (row, (address, was)) in ["O1", "O2", "O3"]
        .into_iter()
        .zip([&o1, &o2, &o3].into_iter().zip(&before))
    {
        assert_eq!(
            &served(&fixture, address).await?,
            was,
            "{row}: the replacement block leaves the fields of block 7"
        );
    }
    fixture.cleanup().await
}
