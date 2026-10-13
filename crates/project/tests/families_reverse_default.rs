//! F12's coin type 60 read follows the ENSv2 Universal Resolver's reverse order. A nonzero
//! resolver on the `addr.reverse` node answers alone: ENSv2 resolves a reverse name through the
//! ENSv1 registry, whose lookup takes the node's own resolver whenever it is nonzero, and an
//! empty name from that resolver ends the lookup with no primary name. The `default.reverse`
//! name (coin type 2147483648) is served only when the reverse node has no resolver.
//! (upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L33-L37 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/AbstractNormalizedUniversalResolver.sol:L247-L249 @ ens_v2_sepolia_20261001@07e55a05)
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use bigname_storage::{
    PrimaryNameClaimStatus, families::records::load_family_primary_name_snapshot,
};
use families_support::{Event, Fixture};
use serde_json::{Value, json};

const REVERSE: &str = "ens_v1_reverse_l1";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

fn reverse_node(address: &str, suffix: &str) -> String {
    let labels = [address.trim_start_matches("0x"), suffix, "reverse"];
    let hash = labels
        .iter()
        .rev()
        .fold(alloy_primitives::B256::ZERO, |parent, label| {
            let label = alloy_primitives::keccak256(label.as_bytes());
            alloy_primitives::keccak256([parent.as_slice(), label.as_slice()].concat())
        });
    format!("{hash:#x}")
}

fn address(n: u64) -> String {
    format!("0x{n:040x}")
}

struct Writer<'a> {
    fixture: &'a Fixture,
    block: i64,
    log: i64,
}

impl Writer<'_> {
    async fn event(&mut self, kind: &str, family: &str, after: Value) -> Result<()> {
        self.log += 1;
        let identity = format!("{kind}:{}", self.log);
        self.fixture
            .event(Event::new(&identity, self.block, self.log, kind, family).after(after))
            .await?;
        Ok(())
    }

    async fn claim(&mut self, address: &str) -> Result<()> {
        let node = reverse_node(address, "addr");
        self.event(
            "ReverseChanged",
            REVERSE,
            json!({"source_event": "ReverseClaimed", "address": address, "coin_type": "60",
                   "namespace": "ens", "reverse_node": node}),
        )
        .await
    }

    async fn resolver(&mut self, address: &str, resolver: &str) -> Result<()> {
        let node = reverse_node(address, "addr");
        self.event(
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"source_event": "NewResolver", "node": node, "resolver": resolver}),
        )
        .await
    }

    async fn name(&mut self, address: &str, name: &str) -> Result<()> {
        self.name_at(address, R1, name).await
    }

    async fn name_at(&mut self, address: &str, resolver: &str, name: &str) -> Result<()> {
        let node = reverse_node(address, "addr");
        self.event(
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({"source_event": "NameChanged", "node": node, "resolver": resolver,
                   "record_key": "name", "record_family": "name", "raw_name": name}),
        )
        .await
    }

    /// The resolver's record version of the reverse node moves on, which clears its name.
    async fn version_reset(&mut self, address: &str) -> Result<()> {
        let node = reverse_node(address, "addr");
        self.event(
            "RecordVersionChanged",
            "ens_v1_resolver_l1",
            json!({"node": node, "resolver": R1, "version": "1"}),
        )
        .await
    }

    async fn default_name(&mut self, address: &str, name: &str) -> Result<()> {
        let node = reverse_node(address, "default");
        let source = json!({"address": address, "namespace": "ens", "coin_type": "2147483648",
                            "reverse_node": node});
        self.event(
            "ReverseChanged",
            REVERSE,
            json!({"source_event": "NameForAddrChanged", "address": address,
                   "coin_type": "2147483648", "namespace": "ens", "reverse_node": node}),
        )
        .await?;
        self.event(
            "RecordChanged",
            REVERSE,
            json!({"source_event": "NameForAddrChanged", "address": address, "node": node,
                   "reverse_node": node, "record_key": "name", "record_family": "name",
                   "raw_name": name, "primary_claim_source": source}),
        )
        .await
    }
}

async fn served(fixture: &Fixture, address: &str, coin_type: &str) -> Result<Option<Value>> {
    Ok(
        load_family_primary_name_snapshot(&fixture.pool, address, "ens", coin_type)
            .await?
            .map(|claim| {
                json!({
                    "status": claim.row.claim_status.as_str(),
                    "name": claim.row.raw_claim_name,
                    "coin_type": claim.row.coin_type,
                    "reverse_node": claim.row.claim_provenance["reverse_node"],
                })
            }),
    )
}

/// The served claim of the `addr.reverse` tuple.
fn addr(status: &str, name: Option<&str>, address: &str) -> Value {
    json!({"status": status, "name": name, "coin_type": "60",
           "reverse_node": reverse_node(address, "addr")})
}

/// The `default.reverse` name served for coin type 60.
fn default_served(name: &str, address: &str) -> Value {
    json!({"status": "success", "name": name, "coin_type": "60",
           "reverse_node": reverse_node(address, "default")})
}

#[tokio::test]
async fn coin_60_falls_back_to_the_default_reverse_name() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    // Claimed with a zero resolver, as the ENSv2 app does before setting the default name.
    let zero = address(1);
    writer.claim(&zero).await?;
    writer.resolver(&zero, ZERO).await?;
    writer.default_name(&zero, "evers.eth").await?;
    // A nonzero resolver with a non-empty name wins over the default name.
    let set = address(2);
    writer.claim(&set).await?;
    writer.resolver(&set, R1).await?;
    writer.name(&set, "bob.eth").await?;
    writer.default_name(&set, "other.eth").await?;
    // An empty addr.reverse name on a nonzero resolver is the answer: no default name.
    let empty = address(3);
    writer.claim(&empty).await?;
    writer.resolver(&empty, R1).await?;
    writer.name(&empty, "").await?;
    writer.default_name(&empty, "carol.eth").await?;
    // No addr.reverse claim at all.
    let unclaimed = address(4);
    writer.default_name(&unclaimed, "dave.eth").await?;
    // An empty default name leaves no name either way.
    let cleared = address(5);
    writer.claim(&cleared).await?;
    writer.default_name(&cleared, "").await?;
    fixture.apply(1, FamilyMode::Normal).await?;

    let default = |name: Option<&str>, address: &str| {
        json!({"status": if name.is_some() { "success" } else { "not_found" }, "name": name,
               "coin_type": "60", "reverse_node": reverse_node(address, "default")})
    };
    assert_eq!(
        served(&fixture, &zero, "60").await?,
        Some(default(Some("evers.eth"), &zero))
    );
    assert_eq!(
        served(&fixture, &set, "60").await?,
        Some(
            json!({"status": "success", "name": "bob.eth", "coin_type": "60",
                    "reverse_node": reverse_node(&set, "addr")})
        )
    );
    assert_eq!(
        served(&fixture, &empty, "60").await?,
        Some(addr("not_found", None, &empty))
    );
    assert_eq!(
        served(&fixture, &unclaimed, "60").await?,
        Some(default(Some("dave.eth"), &unclaimed))
    );
    assert_eq!(
        served(&fixture, &cleared, "60").await?,
        Some(default(None, &cleared))
    );
    assert_eq!(
        served(&fixture, &set, "2147483648").await?,
        Some(
            json!({"status": "success", "name": "other.eth", "coin_type": "2147483648",
                    "reverse_node": reverse_node(&set, "default")})
        )
    );
    assert_eq!(
        load_family_primary_name_snapshot(&fixture.pool, &zero, "ens", "60")
            .await?
            .map(|claim| claim.row.claim_status),
        Some(PrimaryNameClaimStatus::Success)
    );
    fixture.cleanup().await
}

// A nonzero resolver's name answers even when the product cannot serve its bytes, and the
// default name is served once the resolver is cleared to zero.
#[tokio::test]
async fn nonempty_addr_reverse_bytes_stop_the_fallback() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_bytes", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let whitespace = address(1);
    writer.claim(&whitespace).await?;
    writer.resolver(&whitespace, R1).await?;
    writer.name(&whitespace, " ").await?;
    writer.default_name(&whitespace, "evers.eth").await?;
    let invalid = address(2);
    writer.claim(&invalid).await?;
    writer.resolver(&invalid, R1).await?;
    writer.name(&invalid, "bad name.eth").await?;
    writer.default_name(&invalid, "evers.eth").await?;
    let moved = address(3);
    writer.claim(&moved).await?;
    writer.resolver(&moved, R1).await?;
    writer.name(&moved, "bob.eth").await?;
    writer.default_name(&moved, "carol.eth").await?;
    let cleared = address(4);
    writer.default_name(&cleared, "dave.eth").await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &moved, "60")
            .await?
            .map(|claim| claim["name"].clone()),
        Some(json!("bob.eth"))
    );
    // Block 2 clears the resolver of one node and the default name of another address.
    writer.block = 2;
    writer.resolver(&moved, ZERO).await?;
    writer.default_name(&cleared, "").await?;
    fixture.apply(2, FamilyMode::Normal).await?;

    assert_eq!(
        served(&fixture, &whitespace, "60").await?,
        Some(addr("not_found", None, &whitespace))
    );
    assert_eq!(
        served(&fixture, &invalid, "60").await?,
        Some(addr("invalid_name", Some("bad name.eth"), &invalid))
    );
    assert_eq!(
        served(&fixture, &moved, "60").await?,
        Some(
            json!({"status": "success", "name": "carol.eth", "coin_type": "60",
                    "reverse_node": reverse_node(&moved, "default")})
        )
    );
    for coin_type in ["60", "2147483648"] {
        assert_eq!(
            served(&fixture, &cleared, coin_type).await?,
            Some(
                json!({"status": "not_found", "name": null, "coin_type": coin_type,
                        "reverse_node": reverse_node(&cleared, "default")})
            ),
            "coin type {coin_type}"
        );
    }
    fixture.cleanup().await
}

// A name cleared on the reverse node's resolver leaves no primary name while that resolver stays.
#[tokio::test]
async fn cleared_name_on_a_set_resolver_serves_no_default() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_cleared", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let alice = address(1);
    writer.default_name(&alice, "carol.eth").await?;
    writer.claim(&alice).await?;
    writer.resolver(&alice, R1).await?;
    writer.name(&alice, "alice.eth").await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("success", Some("alice.eth"), &alice))
    );
    writer.block = 2;
    writer.name(&alice, "").await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("not_found", None, &alice))
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn record_version_reset_serves_no_default() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_version", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let alice = address(1);
    writer.default_name(&alice, "carol.eth").await?;
    writer.claim(&alice).await?;
    writer.resolver(&alice, R1).await?;
    writer.name(&alice, "alice.eth").await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("success", Some("alice.eth"), &alice))
    );
    writer.block = 2;
    writer.version_reset(&alice).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("not_found", None, &alice))
    );
    fixture.cleanup().await
}

// The second resolver holds no name for the node, so nothing is served while it is current.
#[tokio::test]
async fn resolver_change_to_an_unnamed_resolver_serves_no_default() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_moved", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let alice = address(1);
    writer.default_name(&alice, "carol.eth").await?;
    writer.claim(&alice).await?;
    writer.resolver(&alice, R1).await?;
    writer.name(&alice, "alice.eth").await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    writer.block = 2;
    writer.resolver(&alice, R2).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("not_found", None, &alice))
    );
    fixture.cleanup().await
}

// The first resolver keeps its name for the node while another resolver is current, and that name
// is served again once the node points back at it.
#[tokio::test]
async fn resolver_change_back_serves_the_retained_name() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_back", 3).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let alice = address(1);
    writer.default_name(&alice, "carol.eth").await?;
    writer.claim(&alice).await?;
    writer.resolver(&alice, R1).await?;
    writer.name(&alice, "alice.eth").await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    writer.block = 2;
    writer.resolver(&alice, R2).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    writer.block = 3;
    writer.resolver(&alice, R1).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("success", Some("alice.eth"), &alice))
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn default_written_after_an_empty_claim_is_not_served() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_later", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let alice = address(1);
    writer.claim(&alice).await?;
    writer.resolver(&alice, R1).await?;
    writer.name(&alice, "").await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    // Resolver set, name empty, and no default tuple at all.
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("not_found", None, &alice))
    );
    assert_eq!(served(&fixture, &alice, "2147483648").await?, None);
    writer.block = 2;
    writer.default_name(&alice, "carol.eth").await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("not_found", None, &alice))
    );
    // The default name itself stays readable under its own coin type.
    assert_eq!(
        served(&fixture, &alice, "2147483648")
            .await?
            .map(|claim| claim["name"].clone()),
        Some(json!("carol.eth"))
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn empty_claim_hides_an_earlier_default() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_earlier", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let alice = address(1);
    writer.default_name(&alice, "carol.eth").await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(default_served("carol.eth", &alice))
    );
    writer.block = 2;
    writer.claim(&alice).await?;
    writer.resolver(&alice, R1).await?;
    writer.name(&alice, "").await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(
        served(&fixture, &alice, "60").await?,
        Some(addr("not_found", None, &alice))
    );
    fixture.cleanup().await
}

// The reverse node has a resolver the projection holds no name for: one that emitted no name
// event for the node, and one seen only through the registry because the node was claimed through
// a registrar no manifest admits. Neither serves the default name, and each answer equals the
// answer of the same address before any default name was written.
#[tokio::test]
async fn resolver_without_an_indexed_name_serves_no_default() -> Result<()> {
    let fixture = Fixture::new("families_reverse_default_unindexed", 2).await?;
    let mut writer = Writer {
        fixture: &fixture,
        block: 1,
        log: 0,
    };
    let silent = address(1);
    writer.claim(&silent).await?;
    writer.resolver(&silent, R1).await?;
    let unclaimed = address(2);
    writer.resolver(&unclaimed, R1).await?;
    fixture.apply(1, FamilyMode::Normal).await?;
    let silent_before = served(&fixture, &silent, "60").await?;
    let unclaimed_before = served(&fixture, &unclaimed, "60").await?;
    assert_eq!(silent_before, Some(addr("not_found", None, &silent)));
    assert_eq!(unclaimed_before, None);

    writer.block = 2;
    writer.default_name(&silent, "carol.eth").await?;
    writer.default_name(&unclaimed, "dave.eth").await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_eq!(
        (
            served(&fixture, &silent, "60").await?,
            served(&fixture, &unclaimed, "60").await?
        ),
        (silent_before, unclaimed_before)
    );
    fixture.cleanup().await
}
