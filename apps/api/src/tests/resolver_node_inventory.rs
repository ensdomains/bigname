//! The read by resolver and node (`FamilyRecordInventory::load_resolver_node_on`) selects through
//! the per-pointer selection of the resource-keyed read. For a name whose pointer is the resolver,
//! both build the same row. The read by resolver has no resource and no pointer event, so the
//! fields that name them differ. Each fixture publishes at its pointer's block, so the positions
//! that fall back to the pointer's block when no record or boundary is later agree too.
use super::*;
use bigname_storage::families::records::{
    FamilyAttribution, FamilyRecordInventory, load_family_record_inventory_detail_on,
};

/// The row with the fields that name the resource or the pointer event removed.
fn comparable(row: &bigname_storage::RecordInventoryCurrentRow) -> Value {
    let mut boundary = row.record_version_boundary.clone();
    boundary
        .as_object_mut()
        .expect("boundary")
        .remove("resource_id");
    let mut provenance = row.provenance.clone();
    provenance
        .as_object_mut()
        .expect("provenance")
        .remove("resolver_pointer_event_id");
    json!({
        "record_version_boundary": boundary,
        "enumeration_basis": row.enumeration_basis,
        "selectors": row.selectors,
        "explicit_gaps": row.explicit_gaps,
        "unsupported_families": row.unsupported_families,
        "last_change": row.last_change,
        "entries": row.entries,
        "provenance": provenance,
        "coverage": row.coverage,
        "chain_positions": row.chain_positions,
        "canonicality_summary": row.canonicality_summary,
        "manifest_version": row.manifest_version,
        "last_recomputed_at": row.last_recomputed_at.unix_timestamp(),
    })
}

async fn assert_same_row(
    database: &TestDatabase,
    chain_id: &str,
    resource: Uuid,
    resolver: &str,
    name: &str,
) -> Result<()> {
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", name);
    let node = logical_name_id
        .split_once(':')
        .context("node")?
        .1
        .to_owned();
    let mut snapshot = bigname_storage::begin_read_snapshot(&database.pool).await?;
    let by_resource = load_family_record_inventory_detail_on(
        &mut snapshot,
        chain_id,
        resource,
        FamilyAttribution::Omit,
    )
    .await?
    .context("the resource-keyed inventory")?;
    let by_resolver = FamilyRecordInventory::load_resolver_node_on(
        &mut snapshot,
        chain_id,
        resolver,
        "ens",
        &logical_name_id,
        &node,
    )
    .await?
    .context("the inventory by resolver and node")?;
    snapshot.commit().await?;
    assert!(
        by_resource
            .row
            .entries
            .as_array()
            .is_some_and(|entries| !entries.is_empty()),
        "{name}: the fixture serves records: {:#?}",
        by_resource.row
    );
    assert_eq!(
        comparable(&by_resolver.row),
        comparable(&by_resource.row),
        "{name}"
    );
    assert_eq!(by_resolver.mirrored, by_resource.mirrored, "{name}");
    assert_eq!(by_resolver.row.resource_id, Uuid::nil(), "{name}");
    Ok(())
}

#[tokio::test]
async fn matches_the_resource_keyed_reader_for_a_bound_name() -> Result<()> {
    const ALICE_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";
    let alice = Uuid::from_u128(0x2200);

    // An ENSv1 public resolver, before and after a record version boundary.
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    assert_same_row(
        &database,
        "ethereum-mainnet",
        alice,
        ALICE_RESOLVER,
        "alice.eth",
    )
    .await?;
    replace_alice_record_inputs(
        &database,
        &[family_fixture_record_write(
            "text:url",
            Some(json!("https://after.example")),
        )],
    )
    .await?;
    assert_same_row(
        &database,
        "ethereum-mainnet",
        alice,
        ALICE_RESOLVER,
        "alice.eth",
    )
    .await?;
    // An ENSv2 name behind the manifest-declared PublicResolverV2, in the guarded partition.
    seed_abi_public_resolver_v2_name(&database, "v2only.eth", &[]).await?;
    assert_same_row(
        &database,
        "ethereum-mainnet",
        Uuid::from_u128(0x5ab900),
        PUBLIC_RESOLVER_V2,
        "v2only.eth",
    )
    .await?;
    database.cleanup().await?;

    // A resolver whose implementation is not an admitted profile.
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    assert_same_row(
        &database,
        "ethereum-mainnet",
        alice,
        ALICE_RESOLVER,
        "alice.eth",
    )
    .await?;
    database.cleanup().await?;

    // A declared ENSv1 mirror resolver, read through its registry walk.
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    let database =
        v2_mirror_records_database("alice.eth", MIRROR, MirrorFixtureSource::Exact, "resolver")
            .await?;
    assert_same_row(
        &database,
        "ethereum-sepolia",
        Uuid::from_u128(0x6100),
        MIRROR,
        "alice.eth",
    )
    .await?;
    database.cleanup().await
}
