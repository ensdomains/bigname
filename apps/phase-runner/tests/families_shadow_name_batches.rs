//! TYR-36 step 7b: the composed name listings (search, the expiring listing and the names bound
//! to a resolver) walk their candidates in batches and stitch a page across them. The production
//! batch is at least 200 candidates, so a fixture of a few names never needs a second one; here
//! the test-only seam `families::name::seams::with_batch_size` shrinks it, and the shadow
//! comparison of `project_end_to_end/topology_shadow.rs` checks every page and every cursor
//! against the served readers at page sizes one and two. At batch size one every cursor lands on
//! a batch boundary and every walk ends on an empty, short batch; at two and three some pages
//! straddle a boundary. Most surfaces are ones the listings' filters reject (no registration, so
//! unsupported), and some names renew, so the expiring walk meets a name twice.
#[allow(dead_code)]
#[path = "project_end_to_end/name_shadow.rs"]
mod name_shadow;
#[allow(dead_code)]
#[path = "project_end_to_end/shadow_fixture.rs"]
mod shadow_fixture;
#[allow(dead_code)]
mod support;
#[allow(dead_code)]
#[path = "project_end_to_end/topology_shadow.rs"]
mod topology_shadow;

use anyhow::{Result, ensure};
use bigname_storage::families::name::seams::with_batch_size;
use serde_json::json;
use shadow_fixture::{Fixture, address, unexpected, uuid, word};

const REGISTRY: &str = "ens_v2_registry_l1";
const RESOLVER: &str = "ens_v2_resolver_l1";
const ETH: u64 = 0xe7;

/// A surface `{label}.eth` at block 1; registered under ENSv2 with `expiry` when given.
async fn name(fixture: &Fixture, n: u64, label: &str, expiry: Option<i64>) -> Result<String> {
    let node = word(0x1000 + n);
    let logical = fixture
        .surface(
            "ens",
            &node,
            &format!("{label}.eth"),
            &[word(0x2000 + n), word(ETH)],
            1,
        )
        .await?;
    let Some(expiry) = expiry else {
        return Ok(logical);
    };
    let resource = uuid(0xa000 + n);
    fixture
        .binding(
            &uuid(0xb000 + n),
            &logical,
            &resource,
            "declared_registry_path",
            "ens_v2",
            1,
        )
        .await?;
    fixture
        .event(
            &format!("granted-{label}"),
            Some(&logical),
            Some(&resource),
            REGISTRY,
            "RegistrationGranted",
            1,
            json!({"registry_contract_instance_id": uuid(0xf1), "status": "registered",
                   "registrant": address(0xa000 + n), "owner": address(0xa000 + n),
                   "expiry": expiry, "authority_kind": "registrar"}),
            &address(0xf1),
        )
        .await?;
    Ok(logical)
}

#[tokio::test]
async fn composed_listings_stitch_pages_across_candidate_batches() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_name_batches", 6).await?;
    let resolver = address(0xd1);
    fixture.declare_resolvers(RESOLVER, &[&resolver]).await?;
    // Twenty surfaces in name order; every third is registered, the rest are unsupported. Two
    // registrations share an expiry, so the expiring order falls back to the name.
    let mut registered = Vec::new();
    for n in 1..=20u64 {
        let expiry = (n % 3 == 1).then_some(4_000_000_000 + i64::try_from(n % 7)? * 1_000);
        let logical = name(&fixture, n, &format!("b{n:02}"), expiry).await?;
        if expiry.is_some() {
            registered.push((n, logical));
        }
    }
    for (n, logical) in &registered {
        let resource = uuid(0xa000 + n);
        // Every other registration points at the resolver, so the bound-name walk skips some.
        if n % 2 == 1 {
            fixture
                .event(
                    &format!("point-{n}"),
                    Some(logical),
                    Some(&resource),
                    REGISTRY,
                    "ResolverChanged",
                    2,
                    json!({"resolver": resolver}),
                    &address(0xf1),
                )
                .await?;
        }
        // Some renew, leaving an older expiry the walk also meets.
        if n % 4 == 0 || *n == 1 {
            fixture
                .event(
                    &format!("renewed-{n}"),
                    Some(logical),
                    Some(&resource),
                    REGISTRY,
                    "RegistrationRenewed",
                    3,
                    json!({"registry_contract_instance_id": uuid(0xf1),
                           "status": "registered",
                           "expiry": 4_100_000_000i64 + i64::try_from(*n)?}),
                    &address(0xf1),
                )
                .await?;
        }
    }
    fixture.publish(4).await?;
    for batch in [1, 2, 3, 200] {
        for page in [1, 2] {
            let report = with_batch_size(batch, fixture.compare(page)).await?;
            unexpected(&report, &[])?;
            ensure!(
                report.listing_excused == 0
                    && report.search_rows >= registered.len()
                    && report.expiring_rows >= registered.len()
                    && report.bound_names_composed >= 2,
                "batch {batch}, page {page}: {}",
                report.line()
            );
        }
    }
    fixture.cleanup().await
}
