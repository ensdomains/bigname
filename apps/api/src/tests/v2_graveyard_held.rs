// A `.eth` name the admitted ENSv2 Graveyard holds is served `released` with no expiry, no owner
// and no manager, with its holder before the claim as `lapsed_registration.owner`, on name
// detail, in address lists and out of the expiry listing and its parent's subnames page (crates/storage/src/families/control/lifecycle/graveyard.rs).

const GH_HOLDER: &str = "0x00000000000000000000000000000000000000e1";
/// The admitted Graveyard. The fixture's registry writes carry the adapter's
/// `owner_getter_reason = graveyard`, which is what the composition reads.
const GH_GRAVEYARD: &str = "0x00000000000000000000000000000000000000e9";
const GH_CLAIM_BLOCK: i64 = 230;
/// A lease expiry past its 90-day grace at every fixture block.
const GH_LAPSED_EXPIRY: i64 = 1_600_000_000;
const GH_LIVE_EXPIRY: i64 = 1_900_000_000;

/// The block time of the fixture block `block`.
fn gh_time(block: i64) -> String {
    (1_700_000_000 + block).to_string()
}

/// A named current-registry owner write of `node` (a child of `eth`) to `owner` at `block`.
fn gh_registry_write(
    identity: &str,
    (name, resource): (&str, Uuid),
    (node, label): (&str, &str),
    (owner, reason): (&str, Option<&str>),
    leased: bool,
    block: i64,
) -> Result<NormalizedEvent> {
    let mut after = json!({"source_event": "NewOwner",
                           "node": bigname_lookup::ens_namehash_hex("eth")?,
                           "child_node": node, "labelhash": child_labelhash(label),
                           "owner": owner, "owner_getter": owner, "emitter_role": "registry"});
    if !leased {
        after["authority_kind"] = json!("registry_only");
    }
    if let Some(reason) = reason {
        after["owner_getter_reason"] = json!(reason);
    }
    Ok(family_event(
        identity,
        Some(name),
        Some(resource),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        block,
        1,
        after,
    ))
}

/// `eth` and four second-level names, each with its registry edge under `eth` and a name row:
/// - `held.eth`: a lease of GH_HOLDER that lapsed past its grace, which the Graveyard claims;
/// - `parked.eth`: a registry-only name of GH_HOLDER with no lease, served with its owner and no
///   expiry, which the Graveyard claims;
/// - `lapsed.eth`: a lease of GH_HOLDER past its grace, not claimed;
/// - `live.eth`: a live lease of GH_HOLDER.
///
/// Published at 220, before any claim. Returns the nodes of held and parked.
async fn seed_graveyard_fixture(database: &TestDatabase) -> Result<(String, String)> {
    seed_bounded_membership_blocks(database, 240).await?;
    seed_family_name(database, "eth", 0x7f6_0000, "ens_v1").await?;
    let mut nodes = std::collections::BTreeMap::new();
    let mut events = Vec::new();
    for (index, (label, expiry)) in [
        ("held", Some(GH_LAPSED_EXPIRY)),
        ("parked", None),
        ("lapsed", Some(GH_LAPSED_EXPIRY)),
        ("live", Some(GH_LIVE_EXPIRY)),
    ]
    .into_iter()
    .enumerate()
    {
        let index = u128::try_from(index)?;
        insert_family_label_preimage(&database.pool, label.as_bytes()).await?;
        let node = insert_registry_child(
            database,
            "eth",
            label,
            GH_HOLDER,
            202,
            Uuid::from_u128(0x7f6_1000 + index),
        )
        .await?;
        let (name, resource) = seed_family_name(
            database,
            &format!("{label}.eth"),
            0x7f6_2000 + 0x10 * index,
            "ens_v1",
        )
        .await?;
        events.push(gh_registry_write(
            &format!("gh-{label}-owner"),
            (&name, resource),
            (&node, label),
            (GH_HOLDER, None),
            expiry.is_some(),
            203,
        )?);
        if let Some(expiry) = expiry {
            events.push(family_event(
                &format!("gh-{label}-grant"),
                Some(&name),
                Some(resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                203,
                0,
                json!({"authority_kind": "registrar", "registrant": GH_HOLDER,
                       "expiry": expiry}),
            ));
        }
        nodes.insert(label, (name, resource, node));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(database, 220).await?;

    // The input classes before the claim.
    let (subnames, _) = eth_subnames(database, "").await?;
    let held = served_child(&subnames, &nodes["held"].2);
    assert_eq!(held["status"], json!("released"), "{held:#}");
    assert_eq!(
        held["expires_at"],
        json!(GH_LAPSED_EXPIRY.to_string()),
        "{held:#}"
    );
    let parked = served_child(&subnames, &nodes["parked"].2);
    assert_ne!(parked["status"], json!("released"), "{parked:#}");
    assert_eq!(parked["owner"], json!(GH_HOLDER), "{parked:#}");
    let listing = rows_of(
        &read_family_pages(
            database,
            "/v1/names?namespace=ens&parent=eth&expires_after=0&page_size=10",
        )
        .await?,
    );
    served_child(&listing, &nodes["held"].2);

    let claims = ["held", "parked"]
        .into_iter()
        .map(|label| {
            let (name, resource, node) = &nodes[label];
            gh_registry_write(
                &format!("gh-{label}-claim"),
                (name, *resource),
                (node, label),
                (GH_GRAVEYARD, Some("graveyard")),
                label == "held",
                GH_CLAIM_BLOCK,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &claims).await?;
    publish_test_families(database, GH_CLAIM_BLOCK + 1).await?;
    Ok((nodes["held"].2.clone(), nodes["parked"].2.clone()))
}

/// The held shape of one served row: `released`, no expiry with reason `released`, no grace
/// deadline, no owner or manager, `authority: ens_v1` with no lease expiry.
fn assert_gh_held_row(row: &Value) {
    assert_eq!(row["status"], json!("released"), "{row:#}");
    assert_eq!(row["authority"], json!("ens_v1"), "{row:#}");
    assert_eq!(row["expires_at"], Value::Null, "{row:#}");
    assert_eq!(row["expires_at_reason"], json!("released"), "{row:#}");
    assert_eq!(row["grace_ends_at"], Value::Null, "{row:#}");
    assert_eq!(row["ens_v1"]["expires_at"], Value::Null, "{row:#}");
    for absent in ["owner", "manager"] {
        assert_eq!(row.get(absent), None, "{absent}: {row:#}");
    }
}

/// Name detail of a held name: the held shape, with the holder before the claim as the lapsed
/// owner, released at the claim's block.
#[tokio::test]
async fn v2_graveyard_held_name_detail_is_released_with_no_expiry() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graveyard_fixture(&database).await?;
    for name in ["held.eth", "parked.eth"] {
        let (status, body) = read_family_response(&database, &format!("/v1/names/{name}")).await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        let record = &body["data"];
        assert_gh_held_row(record);
        assert_eq!(
            record["lapsed_registration"]["owner"],
            json!(GH_HOLDER),
            "{record:#}"
        );
        assert_eq!(
            record["lapsed_registration"]["released_at"],
            json!(gh_time(GH_CLAIM_BLOCK)),
            "{record:#}"
        );
        assert_eq!(
            record["lapsed_registration"]["release_kind"],
            json!("expired"),
            "{record:#}"
        );
    }
    database.cleanup().await
}

/// Rows 17 and 18: a held child is not on its parent's subnames page, whatever the fence, as for
/// any record the Graveyard holds. The unfiltered total and the `include_expired=false` total
/// both leave it out, and the fence drops the lapsed sibling only. The expiry listing of `eth`'s
/// children, which needs a finite expiry, leaves the held names out while it lists the live
/// sibling. Before the claim it listed `held.eth` with its old lease expiry.
#[tokio::test]
async fn v2_graveyard_held_child_leaves_the_lists_and_the_expiry_listing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (held, parked) = seed_graveyard_fixture(&database).await?;
    let lapsed = bigname_lookup::ens_namehash_hex("lapsed.eth")?;
    let live = bigname_lookup::ens_namehash_hex("live.eth")?;

    let (subnames, total) = eth_subnames(&database, "").await?;
    let (fenced, fenced_total) = eth_subnames(&database, "&include_expired=false").await?;
    for rows in [&subnames, &fenced] {
        for node in [&held, &parked] {
            assert!(
                rows.iter().all(|row| row["namehash"] != json!(node)),
                "{node}: {rows:#?}"
            );
        }
        served_child(rows, &live);
    }
    served_child(&subnames, &lapsed);
    assert!(
        fenced.iter().all(|row| row["namehash"] != json!(lapsed)),
        "{fenced:#?}"
    );
    assert_eq!(total, json!(2), "{subnames:#?}");
    assert_eq!(fenced_total, json!(1), "{fenced:#?}");

    let listing = rows_of(
        &read_family_pages(
            &database,
            "/v1/names?namespace=ens&parent=eth&expires_after=0&page_size=10",
        )
        .await?,
    );
    for node in [&held, &parked] {
        assert!(
            listing.iter().all(|row| row["namehash"] != json!(node)),
            "{node}: {listing:#?}"
        );
    }
    served_child(&listing, &live);
    database.cleanup().await
}

/// A held name is listed for nobody under the authority relations, neither its holder
/// before the claim nor the Graveyard, and its holder lists it under `former_owner`.
#[tokio::test]
async fn v2_graveyard_held_name_lists_its_holder_as_former_owner_only() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (held, parked) = seed_graveyard_fixture(&database).await?;
    for address in [GH_HOLDER, GH_GRAVEYARD] {
        for (relation, rows, _) in address_rows_by_relation(&database, address).await? {
            for node in [&held, &parked] {
                assert!(
                    rows.iter().all(|row| row["namehash"] != json!(node)),
                    "{address} {relation}: {rows:#?}"
                );
            }
        }
    }
    let former = rows_of(
        &read_family_pages(
            &database,
            &format!("/v1/addresses/{GH_HOLDER}/names?namespace=ens&relation=former_owner"),
        )
        .await?,
    );
    for node in [&held, &parked] {
        let row = served_child(&former, node);
        assert_eq!(row["status"], json!("released"), "{row:#}");
        assert_eq!(
            row["lapsed_registration"]["released_at"],
            json!(gh_time(GH_CLAIM_BLOCK)),
            "{row:#}"
        );
    }
    let graveyard_former = rows_of(
        &read_family_pages(
            &database,
            &format!("/v1/addresses/{GH_GRAVEYARD}/names?namespace=ens&relation=former_owner"),
        )
        .await?,
    );
    assert!(graveyard_former.is_empty(), "{graveyard_former:#?}");
    database.cleanup().await
}
