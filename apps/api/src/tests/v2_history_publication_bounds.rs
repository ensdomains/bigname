// Interpret writes normalized events ahead of Project's publication, and each product read is
// bound to the published block of its snapshot. A registration witness above that block (the
// grant that turns an ENSv2 reservation resource into a registration) must not reclassify the
// rows at or below it: the read bound to the lower publication returns exactly what it returned
// before the witness was written, and only a read bound to a publication that includes the
// witness sees the registration.
#[tokio::test]
async fn registration_witness_above_the_published_block_does_not_reclassify_older_rows()
-> Result<()> {
    const NAME: &str = "reserved-then-granted.eth";
    const SEED: &str = "ens:reserved-then-granted.eth";
    const CHAIN: &str = "ethereum-mainnet";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let registration = Uuid::from_u128(0x71b0);
    let blocks = (120..=125)
        .map(|number| {
            raw_block(
                CHAIN,
                &format!("0xhistory{number}"),
                (number > 120)
                    .then(|| format!("0xhistory{}", number - 1))
                    .as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_identity_name(
        &database,
        SEED,
        NAME,
        NAME,
        "node:reserved-then-granted.eth",
        registration,
        Uuid::from_u128(0x81b0),
        Uuid::from_u128(0x91b1),
        "0x00000000000000000000000000000000000071b1",
        bigname_storage::AddressNameRelation::EffectiveController,
        120,
    )
    .await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            Uuid::from_u128(0x81b0),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            registration,
            Some(Uuid::from_u128(0x81b0)),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    // The name is bound to the resource from the reservation onwards; the binding sits on the
    // rows' own fork so noncanonical reads can connect it to them.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000120),
             block_hash = '0xhistory120',
             block_number = 120
         WHERE resource_id = $1",
    )
    .bind(registration)
    .execute(&database.pool)
    .await?;
    // Project has published block 122.
    seed_schema_v2_ens_lookup_head(&database.pool, 122, "0xhistory122", "2023-11-14T22:15:22Z")
        .await?;

    let registry_row = |identity: &str, kind: &str, source_event: &str, number: i64| {
        let mut event =
            v2_history_event(identity, Some(&logical_name_id), Some(registration), kind, number);
        event.source_family = "ens_v2_registry_l1".to_owned();
        event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        event.after_state["source_event"] = json!(source_event);
        if kind == "RegistrationGranted" {
            event.after_state["authority_kind"] = json!("ens_v2_registry");
        }
        event
    };
    // A record write on the name while it is only reserved: no resource, so it can belong to a
    // registration only through the name's binding and that binding's lifecycle witness.
    let mut record = v2_history_event(
        "reserved-then-granted-record-121",
        Some(&logical_name_id),
        None,
        "RecordChanged",
        121,
    );
    record.source_family = "ens_v1_resolver_l1".to_owned();
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            registry_row(
                "reserved-then-granted-120",
                "RegistrationReserved",
                "LabelReserved",
                120,
            ),
            record,
            registry_row(
                "reserved-then-granted-122",
                "ExpiryChanged",
                "ExpiryUpdated",
                122,
            ),
        ],
    )
    .await?;

    let route = format!("/v1/events?registration_id={registration}&page_size=20&include=total_count");
    for canonical_only in [true, false] {
        let rows =
            bounded_registration_history(&database.pool, registration, CHAIN, 122, canonical_only)
                .await?;
        assert!(rows.is_empty(), "canonical_only={canonical_only}: {rows:?}");
    }
    let before = v2_history_payload_for_database(&database, &route).await?;
    assert_eq!(before["data"], json!([]), "{before:?}");
    assert_eq!(before["page"]["total_count"], json!(0), "{before:?}");

    // Interpret writes the grant and a later expiry update above the published block.
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            registry_row(
                "reserved-then-granted-123",
                "RegistrationGranted",
                "LabelRegistered",
                123,
            ),
            registry_row(
                "reserved-then-granted-124",
                "ExpiryChanged",
                "ExpiryUpdated",
                124,
            ),
        ],
    )
    .await?;

    for canonical_only in [true, false] {
        let rows =
            bounded_registration_history(&database.pool, registration, CHAIN, 122, canonical_only)
                .await?;
        assert!(
            rows.is_empty(),
            "canonical_only={canonical_only}: a witness above the published block reclassified \
             older rows: {rows:?}"
        );
    }
    let after = v2_history_payload_for_database(&database, &route).await?;
    assert_eq!(
        after["data"],
        json!([]),
        "a witness above the published block reclassified older rows: {after:?}"
    );
    assert_eq!(after["page"]["total_count"], json!(0), "{after:?}");

    // A read bound to a publication that includes the grant sees the registration.
    for canonical_only in [true, false] {
        let rows =
            bounded_registration_history(&database.pool, registration, CHAIN, 125, canonical_only)
                .await?;
        let block_numbers = rows
            .iter()
            .map(|row| row.block_number.expect("block number"))
            .collect::<Vec<_>>();
        assert_eq!(block_numbers, vec![124, 123, 121], "canonical_only={canonical_only}: {rows:?}");
        assert_eq!(rows[0].registration_id, Some(registration));
        assert_eq!(rows[1].registration_id, Some(registration));
        assert_eq!(rows[2].registration_id, None);
    }
    seed_schema_v2_ens_lookup_head(&database.pool, 125, "0xhistory125", "2023-11-14T22:15:25Z")
        .await?;
    let published = v2_history_payload_for_database(&database, &route).await?;
    let block_numbers = published["data"]
        .as_array()
        .expect("registration history rows")
        .iter()
        .map(|row| row["block_number"].as_i64().expect("block number"))
        .collect::<Vec<_>>();
    assert_eq!(block_numbers, vec![124, 123, 121], "{published:?}");
    assert_eq!(published["page"]["total_count"], json!(3), "{published:?}");
    database.cleanup().await
}

/// The rows of one registration as a product read bound to `published_block` on `chain_id`
/// sees them: the anchors and the rows themselves stop at that block.
async fn bounded_registration_history(
    pool: &PgPool,
    registration_id: Uuid,
    chain_id: &str,
    published_block: i64,
    canonical_only: bool,
) -> Result<Vec<bigname_storage::HistoryEvent>> {
    bigname_storage::load_event_history(
        pool,
        bigname_storage::EventHistoryFilter {
            resource_id: Some(registration_id),
            to_block: Some(published_block),
            publication_block_bounds: Some(std::collections::BTreeMap::from([(
                chain_id.to_owned(),
                published_block,
            )])),
            ..Default::default()
        },
        canonical_only,
    )
    .await
}
