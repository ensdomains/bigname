//! TYR-73: `owner=` and `exclude_owner=` on a registry's labels.
use super::*;

const LABEL_OWNER_A: &str = "0x00000000000000000000000000000000000000a1";
const LABEL_OWNER_B: &str = "0x00000000000000000000000000000000000000a2";
/// A label expiry long past the fixture's block times (1_700_000_200 onwards).
const LABEL_EXPIRED: i64 = 1_600_000_000;
const LABEL_LIVE: i64 = 1_900_000_000;

/// One label of the fixture: its label, its token owner (none for an ownerless label) and its
/// registration expiry.
struct FixtureLabel {
    label: String,
    owner: Option<&'static str>,
    expiry: i64,
}

fn fixture_label(label: &str, owner: Option<&'static str>, expiry: i64) -> FixtureLabel {
    FixtureLabel {
        label: label.to_owned(),
        owner,
        expiry,
    }
}

/// alpha.eth (ENSv1, bound at 200) points at the ENSv2 subregistry `b1`, which registers each
/// label at block 205; a label with an owner has its token transferred to it in the same
/// transaction, before the registration, as the registry emits them. Published at 240.
async fn seed_label_owner_fixture(database: &TestDatabase, labels: &[FixtureLabel]) -> Result<()> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(database, "alpha.eth", 0x7a1_0000, "ens_v1").await?;
    let alpha_registry = Uuid::from_u128(0x7b0_0001);
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instances (contract_instance_id, chain_id,
             contract_kind)
         VALUES ($1, $2, 'contract') ON CONFLICT DO NOTHING",
    )
    .bind(alpha_registry)
    .bind(FAMILY_CHAIN)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instance_addresses (contract_instance_id, chain_id,
             address, active_from_block_number)
         VALUES ($1, $2, $3, 200)",
    )
    .bind(alpha_registry)
    .bind(FAMILY_CHAIN)
    .bind(CHILD_ALPHA_REGISTRY)
    .execute(&database.pool)
    .await?;
    let mut events = vec![
        family_event(
            "owners-alpha-grant",
            Some(&alpha),
            Some(alpha_resource),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            201,
            0,
            json!({"authority_kind": "registrar", "registrant": CHILD_OWNER,
                   "expiry": LABEL_LIVE}),
        ),
        child_registry_event(
            "owners-root-created",
            None,
            "RegistryCreated",
            203,
            0,
            CHILD_ROOT_REGISTRY,
            json!({"source_event": "RegistryCreated", "registry": CHILD_ROOT_REGISTRY}),
        ),
        child_registry_event(
            "owners-alpha-subregistry",
            Some(&alpha),
            "SubregistryChanged",
            204,
            0,
            CHILD_ROOT_REGISTRY,
            json!({"source_event": "SubregistryUpdated", "subregistry": CHILD_ALPHA_REGISTRY}),
        ),
        child_registry_event(
            "owners-alpha-created",
            None,
            "RegistryCreated",
            204,
            1,
            CHILD_ALPHA_REGISTRY,
            json!({"source_event": "RegistryCreated", "registry": CHILD_ALPHA_REGISTRY}),
        ),
    ];
    for (index, label) in labels.iter().enumerate() {
        let seed = 0x7c0_0000 + 16 * u128::try_from(index)?;
        let (name, resource) = seed_family_name(
            database,
            &format!("{}.alpha.eth", label.label),
            seed,
            "ens_v2",
        )
        .await?;
        insert_family_label_preimage(&database.pool, label.label.as_bytes()).await?;
        let log = 2 * i64::try_from(index)?;
        if let Some(owner) = label.owner {
            let mut transfer = child_registry_event(
                &format!("owners-{}-transfer", label.label),
                Some(&name),
                "TokenControlTransferred",
                205,
                log,
                CHILD_ALPHA_REGISTRY,
                json!({"source_event": "Transfer", "to": owner}),
            );
            transfer.resource_id = Some(resource);
            events.push(transfer);
        }
        let mut registration = child_registry_event(
            &format!("owners-{}-registered", label.label),
            Some(&name),
            "RegistrationGranted",
            205,
            log + 1,
            CHILD_ALPHA_REGISTRY,
            json!({"source_event": "LabelRegistered", "authority_kind": "ens_v2_registry",
                   "registry_contract_instance_id": alpha_registry.to_string(),
                   "status": "registered", "registrant": CHILD_OWNER,
                   "expiry": label.expiry}),
        );
        registration.resource_id = Some(resource);
        events.push(registration);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(database, 240).await
}

fn labels_uri(query: &str) -> String {
    format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels?{query}")
}

/// Every served label of a filtered walk as (name, owner), following the route's cursors, with
/// the `total_count` every page reported.
async fn walk_labels(
    database: &TestDatabase,
    query: &str,
) -> Result<(Vec<(String, Option<String>)>, Vec<Value>)> {
    let pages = read_family_pages(database, &labels_uri(query)).await?;
    let rows = pages
        .iter()
        .flat_map(|page| page["data"].as_array().into_iter().flatten())
        .map(|row| {
            (
                row["name"].as_str().expect("label name").to_owned(),
                row["owner"].as_str().map(str::to_owned),
            )
        })
        .collect();
    let totals = pages
        .iter()
        .map(|page| page["total_count"].clone())
        .collect();
    Ok((rows, totals))
}

fn names(rows: &[(String, Option<String>)]) -> Vec<&str> {
    rows.iter().map(|(name, _)| name.as_str()).collect()
}

#[tokio::test]
async fn v2_registry_labels_filter_by_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_label_owner_fixture(
        &database,
        &[
            fixture_label("amber", Some(LABEL_OWNER_A), LABEL_LIVE),
            fixture_label("basil", Some(LABEL_OWNER_B), LABEL_LIVE),
            fixture_label("cedar", None, LABEL_LIVE),
            // Expired but not released: still held by its owner.
            fixture_label("delta", Some(LABEL_OWNER_A), LABEL_EXPIRED),
            fixture_label("ember", Some(LABEL_OWNER_A), LABEL_LIVE),
        ],
    )
    .await?;
    let (all, totals) = walk_labels(&database, "page_size=2").await?;
    assert_eq!(
        names(&all),
        [
            "amber.alpha.eth",
            "basil.alpha.eth",
            "cedar.alpha.eth",
            "delta.alpha.eth",
            "ember.alpha.eth"
        ]
    );
    assert!(totals.iter().all(|total| total == &json!(5)), "{totals:?}");
    assert_eq!(all[2].1, None, "cedar is served without an owner");

    // owner=A: A's labels only, the expired one included, counted before paging; the
    // address is compared lower-cased.
    let upper_a = format!("0x{}", LABEL_OWNER_A[2..].to_ascii_uppercase());
    for owner in [LABEL_OWNER_A, upper_a.as_str()] {
        let (owned, totals) = walk_labels(&database, &format!("owner={owner}&page_size=1")).await?;
        assert_eq!(
            names(&owned),
            ["amber.alpha.eth", "delta.alpha.eth", "ember.alpha.eth"]
        );
        assert!(
            owned
                .iter()
                .all(|(_, served)| served.as_deref() == Some(LABEL_OWNER_A))
        );
        assert!(totals.iter().all(|total| total == &json!(3)), "{totals:?}");
    }
    // exclude_owner=A: every other label, the ownerless one included and served without owner.
    let (excluded, totals) = walk_labels(
        &database,
        &format!("exclude_owner={LABEL_OWNER_A}&page_size=1"),
    )
    .await?;
    assert_eq!(
        excluded,
        [
            ("basil.alpha.eth".to_owned(), Some(LABEL_OWNER_B.to_owned())),
            ("cedar.alpha.eth".to_owned(), None),
        ]
    );
    assert!(totals.iter().all(|total| total == &json!(2)), "{totals:?}");
    // The two filters partition the labels, for each address.
    for owner in [LABEL_OWNER_A, LABEL_OWNER_B, CHILD_OWNER] {
        let (owned, _) = walk_labels(&database, &format!("owner={owner}&page_size=50")).await?;
        let (others, _) =
            walk_labels(&database, &format!("exclude_owner={owner}&page_size=50")).await?;
        let mut union = [owned.clone(), others.clone()].concat();
        union.sort();
        assert_eq!(union, all, "owner={owner}");
        assert!(
            owned
                .iter()
                .all(|(_, served)| served.as_deref() == Some(owner))
        );
        assert!(
            others
                .iter()
                .all(|(_, served)| served.as_deref() != Some(owner))
        );
    }
    // An address holding nothing is a proven empty answer.
    let (status, body) = read_family_response(
        &database,
        &labels_uri(&format!("owner={CHILD_OWNER}&page_size=1")),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"], json!([]), "{body:#}");
    assert_eq!(body["page"]["total_count"], json!(0), "{body:#}");

    for (query, message) in [
        (
            format!("owner={LABEL_OWNER_A}&exclude_owner={LABEL_OWNER_B}"),
            "owner and exclude_owner cannot be combined",
        ),
        ("owner=0x1234".to_owned(), "owner"),
        ("exclude_owner=alice.eth".to_owned(), "exclude_owner"),
    ] {
        let (status, body) = read_family_response(&database, &labels_uri(&query)).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body:#}");
        let text = body["error"]["message"].as_str().unwrap_or_default();
        assert!(text.contains(message), "{query}: {body:#}");
    }

    // A blank owner or exclude_owner counts as absent, as for every optional address parameter.
    for (query, expected) in [
        (
            format!("owner=&exclude_owner={LABEL_OWNER_A}&page_size=50"),
            &excluded,
        ),
        (
            format!("owner=%20&exclude_owner={LABEL_OWNER_A}&page_size=50"),
            &excluded,
        ),
        (
            format!("owner={LABEL_OWNER_B}&exclude_owner=&page_size=50"),
            &vec![("basil.alpha.eth".to_owned(), Some(LABEL_OWNER_B.to_owned()))],
        ),
        ("owner=%20&page_size=50".to_owned(), &all),
        ("owner=&exclude_owner=&page_size=50".to_owned(), &all),
    ] {
        let (rows, _) = walk_labels(&database, &query).await?;
        assert_eq!(&rows, expected, "{query}");
    }

    // A cursor continues only the filter it was issued for.
    let (_, first) = read_family_response(
        &database,
        &labels_uri(&format!("owner={LABEL_OWNER_A}&page_size=1")),
    )
    .await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("next cursor")
        .to_owned();
    for query in [
        String::new(),
        format!("owner={LABEL_OWNER_B}&"),
        format!("exclude_owner={LABEL_OWNER_A}&"),
    ] {
        let (status, body) =
            read_family_response(&database, &labels_uri(&format!("{query}cursor={cursor}")))
                .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body:#}");
    }
    let (status, body) = read_family_response(
        &database,
        &labels_uri(&format!("owner={upper_a}&page_size=1&cursor={cursor}")),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(
        body["data"][0]["name"],
        json!("delta.alpha.eth"),
        "{body:#}"
    );

    // Unavailable evidence stays a 409, never an empty 200.
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.pool)
        .await?;
    for query in [
        format!("exclude_owner={LABEL_OWNER_A}&page_size=1"),
        format!("owner={LABEL_OWNER_A}&page_size=1"),
    ] {
        let (status, body) = read_family_response(&database, &labels_uri(&query)).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{query}: {body:#}");
    }
    database.cleanup().await
}

/// A registry past the portal's old 2,000-row download: the filtered totals are exact and
/// a walk returns each admitted label once.
#[tokio::test]
async fn v2_registry_labels_owner_totals_past_two_thousand() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let labels: Vec<FixtureLabel> = (0..2003)
        .map(|index| {
            let owner = match index % 2 {
                _ if index == 2002 => None,
                0 => Some(LABEL_OWNER_A),
                _ => Some(LABEL_OWNER_B),
            };
            fixture_label(&format!("l{index:04}"), owner, LABEL_LIVE)
        })
        .collect();
    seed_label_owner_fixture(&database, &labels).await?;
    for (query, expected) in [
        String::from("page_size=1"),
        format!("owner={LABEL_OWNER_A}&page_size=1"),
        format!("owner={LABEL_OWNER_B}&page_size=1"),
        format!("exclude_owner={LABEL_OWNER_A}&page_size=1"),
    ]
    .into_iter()
    .zip([2003, 1001, 1001, 1002])
    {
        let (status, body) = read_family_response(&database, &labels_uri(&query)).await?;
        assert_eq!(status, StatusCode::OK, "{query}: {body:#}");
        assert_eq!(body["page"]["total_count"], json!(expected), "{query}");
        assert_eq!(body["page"]["has_more"], json!(true), "{query}");
    }
    let (others, totals) = walk_labels(
        &database,
        &format!("exclude_owner={LABEL_OWNER_A}&page_size=200"),
    )
    .await?;
    assert_eq!(others.len(), 1002);
    assert!(
        totals.iter().all(|total| total == &json!(1002)),
        "{totals:?}"
    );
    let distinct: std::collections::BTreeSet<&str> = names(&others).into_iter().collect();
    assert_eq!(distinct.len(), 1002);
    assert_eq!(
        others.iter().filter(|(_, owner)| owner.is_none()).count(),
        1
    );
    assert!(
        others
            .iter()
            .all(|(_, owner)| owner.as_deref() != Some(LABEL_OWNER_A))
    );
    database.cleanup().await
}

/// A token transfer in a later block moves a label between the filters on the ordinary
/// per-block follow, not only on a rebuild: the summary of the transferred label is composed again.
#[tokio::test]
async fn v2_registry_labels_owner_filter_follows_a_later_transfer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_label_owner_fixture(
        &database,
        &[
            fixture_label("amber", Some(LABEL_OWNER_A), LABEL_LIVE),
            fixture_label("basil", Some(LABEL_OWNER_B), LABEL_LIVE),
            fixture_label("cedar", None, LABEL_LIVE),
        ],
    )
    .await?;
    let owned_by_a = |rows: &[(String, Option<String>)]| names(rows).join(",");
    let (owned, _) = walk_labels(&database, &format!("owner={LABEL_OWNER_A}")).await?;
    assert_eq!(owned_by_a(&owned), "amber.alpha.eth");

    // basil (index 1) and cedar (index 2) change hands at block 241.
    let mut events = Vec::new();
    for (index, label, to) in [(1u128, "basil", LABEL_OWNER_A), (2, "cedar", LABEL_OWNER_B)] {
        let name = bigname_storage::logical_name_id_for_name("ens", &format!("{label}.alpha.eth"));
        let mut transfer = child_registry_event(
            &format!("owners-{label}-later-transfer"),
            Some(&name),
            "TokenControlTransferred",
            241,
            i64::try_from(index)?,
            CHILD_ALPHA_REGISTRY,
            json!({"source_event": "Transfer", "to": to}),
        );
        transfer.resource_id = Some(Uuid::from_u128(0x7c0_0000 + 16 * index));
        events.push(transfer);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(&database, 241).await?;

    let (owned, totals) =
        walk_labels(&database, &format!("owner={LABEL_OWNER_A}&page_size=1")).await?;
    assert_eq!(owned_by_a(&owned), "amber.alpha.eth,basil.alpha.eth");
    assert!(totals.iter().all(|total| total == &json!(2)), "{totals:?}");
    let (others, totals) = walk_labels(
        &database,
        &format!("exclude_owner={LABEL_OWNER_A}&page_size=1"),
    )
    .await?;
    assert_eq!(
        others,
        [("cedar.alpha.eth".to_owned(), Some(LABEL_OWNER_B.to_owned()))]
    );
    assert!(totals.iter().all(|total| total == &json!(1)), "{totals:?}");
    database.cleanup().await
}
