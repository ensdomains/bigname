// The names group under the publication switch (TYR-36 step 7b slice 2): every route whose name
// rows now come from the composed name reader (`bigname_storage::families::name`) answers the
// same body with the switch off and on, `meta.as_of` excepted, over a fixture that Project and
// the owned key families both build from the same normalized events.

const SWITCH_ALICE: &str = "0x00000000000000000000000000000000000a11ce";
const SWITCH_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";

/// alpha.eth granted at 201, pointed at a resolver at 202 and renewed at 203; beta.eth granted
/// at 204; both published at 240.
async fn seed_switch_names_fixture(database: &TestDatabase) -> Result<()> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) = seed_switch_name(database, "alpha.eth", 0x5a1_0000, "ens_v1").await?;
    let (beta, beta_resource) = seed_switch_name(database, "beta.eth", 0x5b1_0000, "ens_v1").await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id").to_owned();
    let grant = |expiry: i64| {
        json!({"authority_kind": "registrar", "registrant": SWITCH_ALICE, "expiry": expiry})
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            switch_event(
                "switch-alpha-grant",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                201,
                0,
                grant(1_900_000_000),
            ),
            switch_event(
                "switch-alpha-resolver",
                Some(&alpha),
                Some(alpha_resource),
                "ResolverChanged",
                "ens_v1_registry_l1",
                202,
                0,
                json!({"node": alpha_node, "resolver": SWITCH_RESOLVER}),
            ),
            switch_event(
                "switch-alpha-renewal",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationRenewed",
                "ens_v1_registrar_l1",
                203,
                0,
                json!({"expiry": 1_950_000_000i64}),
            ),
            switch_event(
                "switch-beta-grant",
                Some(&beta),
                Some(beta_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                204,
                0,
                grant(1_800_000_000),
            ),
        ],
    )
    .await?;
    publish_project_and_families(database, 240).await
}

#[tokio::test]
async fn v2_name_detail_is_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let (status, alpha) = assert_switch_differential(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(status, StatusCode::OK, "{alpha:#}");
    assert_eq!(alpha["data"]["registrant"], json!(SWITCH_ALICE), "{alpha:#}");
    assert_eq!(
        alpha["data"]["resolver"]["address"],
        json!(SWITCH_RESOLVER),
        "{alpha:#}"
    );
    for uri in [
        "/v1/names/beta.eth",
        "/v1/names/alpha.eth?include=counts",
        "/v1/names/missing.eth",
    ] {
        assert_switch_differential(&database, uri).await?;
    }
    assert_switch_on_ignores_served_tables(&database, "/v1/names/alpha.eth", &["name_current"])
        .await?;
    database.cleanup().await
}

// Ruling J5: a composed row describes the family marker's publication only, so an `at` below
// it answers 409 with the switch on (storage's `family_name_for_snapshot`), with the wording a
// served row that cannot prove the position gets today. Project restamps every served row with
// the publication it writes, so the switch-off side refuses the same `at` for the same reason
// (a row newer than the selected position) and the bodies are equal.
#[tokio::test]
async fn v2_name_detail_refuses_an_at_below_the_publication_both_ways() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    for block in [230, 239] {
        let at = crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(
            1_700_000_000 + block,
        )?);
        let (status, body) =
            assert_switch_differential(&database, &format!("/v1/names/alpha.eth?at={at}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
        assert_eq!(
            body["error"],
            json!({"code": "stale", "details": {},
                   "message": "requested snapshot is not available for name"}),
            "{body:#}"
        );
    }
    let at = crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(1_700_000_240)?);
    let (status, _) =
        assert_switch_differential(&database, &format!("/v1/names/alpha.eth?at={at}")).await?;
    assert_eq!(status, StatusCode::OK);
    database.cleanup().await
}
