//! F12's coin type 60 read follows ENS's ETH reverse resolver: the `addr.reverse` name wins only
//! when the reverse node has a nonzero resolver and that name is non-empty; otherwise the
//! `default.reverse` name (coin type 2147483648) is served.
//! (upstream: .refs/ens_v1/contracts/reverseResolver/ETHReverseResolver.sol:L42-L70 @ ens_v1@91c966f)
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
        let node = reverse_node(address, "addr");
        self.event(
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({"source_event": "NameChanged", "node": node, "resolver": R1,
                   "record_key": "name", "record_family": "name", "raw_name": name}),
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
    // An empty addr.reverse name falls back.
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
        Some(default(Some("carol.eth"), &empty))
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

// Upstream tests the name's byte length, so any stored bytes on a nonzero resolver stop the
// fallback, even when the product cannot serve them; and the fallback follows later changes.
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

    let addr = |status: &str, name: Option<&str>, address: &str| {
        json!({"status": status, "name": name, "coin_type": "60",
               "reverse_node": reverse_node(address, "addr")})
    };
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
