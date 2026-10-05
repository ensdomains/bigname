// `GET /v1/names` by expiry over leases whose surfaces store no raw label bytes: the listing
// selects its names by the served name, the text the composed rows carry.

#[tokio::test]
async fn v2_names_by_expiry_lists_leases_without_bytes_under_their_served_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let hash = |label: &[u8]| format!("{:#x}", alloy_primitives::keccak256(label));
    // `leased` and `eth` have usable preimages, so the name without bytes reads `leased.eth`;
    // nothing is known of the other label, so its name reads as a bracketed label hash, one
    // that sorts after the bracketed hash of `leased`: the order differs when a name is built
    // without its preimages.
    for preimage in [b"leased".as_slice(), b"eth"] {
        insert_family_label_preimage(&database.pool, preimage).await?;
    }
    let opaque = format!("0x{}", "ff".repeat(32));
    let (bytes, bytes_resource) =
        seed_family_name(&database, "zed.eth", 0x8e1_0000, "ens_v1").await?;
    let mut leases = vec![(bytes, bytes_resource)];
    for (index, label) in [hash(b"leased"), opaque.clone()].into_iter().enumerate() {
        leases.push(
            seed_textless_family_name(
                &database,
                &[label, hash(b"eth")],
                0x8e2_0000 + 0x10 * index as u128,
                "ens_v1",
                202 + index as i64,
            )
            .await?,
        );
    }
    // One expiry for all three, so the name alone orders them and places the cursor.
    let events: Vec<NormalizedEvent> = leases
        .iter()
        .enumerate()
        .map(|(index, (id, resource))| {
            family_event(
                &format!("tl-expiry-grant-{index}"),
                Some(id),
                Some(*resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                210 + index as i64,
                0,
                json!({"authority_kind": "registrar", "registrant": RC_OWNER,
                       "expiry": 1_850_000_000i64}),
            )
        })
        .collect();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(&database, 240).await?;

    let opaque_name = format!("{}.eth", tl_bracket(&opaque));
    let expected: BTreeSet<String> =
        ["leased.eth".to_owned(), "zed.eth".to_owned(), opaque_name.clone()].into();
    let window = "/v1/names?namespace=ens&expires_after=1800000000&expires_before=1900000000";
    for filters in ["", "&order=desc", "&parent=eth", "&authority=ens_v1"] {
        let whole = read_family_pages(&database, &format!("{window}{filters}&page_size=10")).await?;
        let names = tl_column(&whole, "name");
        assert_eq!(names.len(), 3, "{filters}: {whole:#?}");
        assert_eq!(names.iter().cloned().collect::<BTreeSet<_>>(), expected, "{filters}");
        // One name a page: every continuation is placed by a served name.
        let paged = read_family_pages(&database, &format!("{window}{filters}&page_size=1")).await?;
        assert_eq!(tl_column(&paged, "name"), names, "{filters}: {paged:#?}");
        for row in rows_of(&whole) {
            assert_eq!(row["expires_at"], json!("1850000000"), "{filters}: {row:#}");
        }
    }
    // A parent that is itself a bracketed label, and one no listed name sits below.
    for parent in [tl_path(&opaque_name), "leased.eth".to_owned()] {
        let pages =
            read_family_pages(&database, &format!("{window}&parent={parent}&page_size=10")).await?;
        assert!(rows_of(&pages).is_empty(), "{parent}: {pages:#?}");
    }

    database.cleanup().await
}
