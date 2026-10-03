use anyhow::Result;
use bigname_storage::{
    AddressNameRelation, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort, load_address_names_current_page_filtered,
};

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

/// The names the address holds as `owner`, grouped by `dedupe`, with `parent` when given, and
/// the page's exact count.
async fn owned(
    run: &support::PipelineRun,
    address: &str,
    dedupe: AddressNamesCurrentDedupe,
    parent: Option<&str>,
) -> Result<(Vec<String>, u64)> {
    let page = load_address_names_current_page_filtered(
        &run.db.pool,
        address,
        Some("ens"),
        Some(&[AddressNameRelation::TokenHolder]),
        dedupe,
        None,
        None,
        None,
        parent,
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        None,
        50,
    )
    .await?;
    let names = page
        .entries
        .into_iter()
        .map(|entry| entry.normalized_name)
        .collect();
    Ok((names, page.summary.grouped_entry_count))
}

/// `parent=eth` keeps an address's `.eth` registrations, a lease and a wrapped lease, and drops
/// its wrapped and unwrapped subnames, so the registration-grouped count is the registrations
/// it holds.
#[tokio::test]
async fn parent_eth_counts_the_eth_registrations_an_address_holds() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let alice = rpc.accounts().await?[1];
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "lease", alice, YEAR, resolver).await?;
    ens_v1::register_eth_name(&rpc, &deployment, "wrapped", alice, YEAR, resolver).await?;
    ens_v1::wrap_eth_2ld(&rpc, &deployment, alice, "wrapped", alice, 0, resolver).await?;
    ens_v1::set_wrapped_subnode_owner(
        &rpc,
        &deployment,
        alice,
        ens_v1::WrappedSubnodeOwner {
            parent: "wrapped.eth",
            label: "kid",
            owner: alice,
            fuses: 0,
            expiry: u64::MAX,
        },
    )
    .await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "lease.eth", "sub", alice).await?;

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' AND canonicality_state = 'canonical' \
         AND lower(after_state->>'labelhash') = '{:#x}' \
         AND lower(after_state->>'node') = '{:#x}' \
         AND lower(after_state->>'owner') = '{alice:#x}')",
        ens_v1::labelhash("sub"),
        ens_v1::namehash("lease.eth"),
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
    let alice = format!("{alice:#x}");

    let (all, all_count) = owned(&run, &alice, AddressNamesCurrentDedupe::Resource, None).await?;
    assert_eq!(all_count, 4, "{all:?}");
    assert_eq!(all.len(), 4, "{all:?}");
    for dedupe in [
        AddressNamesCurrentDedupe::Resource,
        AddressNamesCurrentDedupe::Surface,
    ] {
        assert_eq!(
            owned(&run, &alice, dedupe, Some("eth")).await?,
            (vec!["lease.eth".to_owned(), "wrapped.eth".to_owned()], 2),
            "{dedupe:?}"
        );
    }
    assert_eq!(
        owned(
            &run,
            &alice,
            AddressNamesCurrentDedupe::Resource,
            Some("wrapped.eth")
        )
        .await?,
        (vec!["kid.wrapped.eth".to_owned()], 1)
    );
    let (under_lease, under_lease_count) = owned(
        &run,
        &alice,
        AddressNamesCurrentDedupe::Resource,
        Some("lease.eth"),
    )
    .await?;
    assert_eq!(under_lease_count, 1, "{under_lease:?}");

    run.db.cleanup().await?;
    Ok(())
}
