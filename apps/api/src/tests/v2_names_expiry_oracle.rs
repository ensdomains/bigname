// `GET /v1/names` storage pages against the unbounded oracle: every name of the `ens` namespace
// composed at once and run through the same page statement. The oracle covers `ens` only. Each page the listing serves, and
// each continuation cursor, must equal the oracle's at the same cursor.

const ORACLE_HOLDER: &str = "0x00000000000000000000000000000000000000d7";
const ORACLE_OLD_REGISTRY: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
const ORACLE_ZERO: &str = "0x0000000000000000000000000000000000000000";
/// Every listed name shares this second unless it says otherwise.
const ORACLE_TIE: i64 = 1_850_000_000;
/// Names in the shared second, cycling the ens_v2, ens_v0 and ens_v1 shapes.
const ORACLE_TIE_NAMES: u128 = 30;

/// Synthetic event shapes, named for the row bigname composes from each.
#[derive(Clone, Copy)]
enum OracleShape {
    /// A registrar-family grant on an `ens_v1` binding: served with `authority` `ens_v1`.
    EnsV1,
    /// The same with a registry Transfer whose emitter role is `registry_old`: served with
    /// `authority` `ens_v0`.
    EnsV0,
    /// A grant and a token transfer of the `ens_v2_registry_l1` family on an `ens_v2` binding.
    EnsV2,
    /// A registrar-family grant whose binding row is deleted: the composed row is unsupported.
    Unsupported,
    /// A `registry_old` Transfer to the zero owner, the binding ended and no token: the composed
    /// row serves no authority and no registration.
    Ownerless,
    /// A registrar-family grant followed by a `RegistrationReleased` event after its binding
    /// ended.
    Released,
}

struct OracleFixture {
    next_seed: u128,
    next_log: i64,
    events: Vec<NormalizedEvent>,
    /// Each seeded name's id and resource.
    names: std::collections::BTreeMap<String, (String, Uuid)>,
}

impl OracleFixture {
    fn new() -> Self {
        Self { next_seed: 0x7300_0000, next_log: 0, events: Vec::new(), names: Default::default() }
    }

    fn event(
        &mut self,
        logical: &str,
        resource: Uuid,
        kind: &str,
        family: &str,
        after: Value,
    ) -> NormalizedEvent {
        self.next_log += 1;
        family_event(
            &format!("oracle-{logical}-{}", self.next_log),
            Some(logical),
            Some(resource),
            kind,
            family,
            210,
            self.next_log,
            after,
        )
    }

    /// Seed `name` with registration expiry `expiry` (a JSON number, kept exact).
    async fn name(
        &mut self,
        database: &TestDatabase,
        name: &str,
        shape: OracleShape,
        expiry: Value,
    ) -> Result<(String, Uuid)> {
        let arm = if matches!(shape, OracleShape::EnsV2) { "ens_v2" } else { "ens_v1" };
        self.next_seed += 0x10;
        let (logical, resource) = seed_family_name(database, name, self.next_seed, arm).await?;
        let node = logical.strip_prefix("ens:").context("ens id")?.to_owned();
        let registrar = json!({"authority_kind": "registrar", "registrant": ORACLE_HOLDER,
            "expiry": expiry});
        let old_registry = |owner: &str| {
            json!({"source_event": "Transfer", "node": node, "owner": owner,
                "owner_getter": owner, "emitter_role": "registry_old",
                "registry_contract": ORACLE_OLD_REGISTRY})
        };
        let mut events = Vec::new();
        match shape {
            OracleShape::EnsV1 | OracleShape::Unsupported => {
                events.push(self.event(&logical, resource, "RegistrationGranted",
                    "ens_v1_registrar_l1", registrar));
            }
            OracleShape::EnsV0 => {
                events.push(self.event(&logical, resource, "RegistrationGranted",
                    "ens_v1_registrar_l1", registrar));
                events.push(self.event(&logical, resource, "AuthorityTransferred",
                    "ens_v1_registry_l1", old_registry(ORACLE_HOLDER)));
            }
            OracleShape::EnsV2 => {
                events.push(self.event(&logical, resource, "RegistrationGranted",
                    "ens_v2_registry_l1",
                    json!({"source_event": "NameRegistered", "authority_kind": "ens_v2_registry",
                        "owner": ORACLE_HOLDER, "registrant": ORACLE_HOLDER, "expiry": expiry})));
                events.push(self.event(&logical, resource, "TokenControlTransferred",
                    "ens_v2_registry_l1",
                    json!({"source_event": "Transfer", "from": ORACLE_ZERO, "to": ORACLE_HOLDER})));
            }
            OracleShape::Ownerless => {
                events.push(self.event(&logical, resource, "RegistrationGranted",
                    "ens_v1_registrar_l1", registrar));
                let mut ownerless = old_registry(ORACLE_ZERO);
                ownerless["owner_getter_reason"] = json!("literal_zero");
                events.push(self.event(&logical, resource, "AuthorityTransferred",
                    "ens_v1_registry_l1", ownerless));
            }
            OracleShape::Released => {
                events.push(self.event(&logical, resource, "RegistrationGranted",
                    "ens_v1_registrar_l1", registrar));
                let mut release = self.event(&logical, resource, "RegistrationReleased",
                    "ens_v1_registrar_l1",
                    json!({"expiry": expiry, "released_at": 1_700_000_230}));
                release.block_number = Some(230);
                release.block_hash = Some("0xhistory230".to_owned());
                release.transaction_hash = Some("0xtx230".to_owned());
                release.before_state = json!({"registrant": ORACLE_HOLDER,
                    "authority_kind": "registrar", "authority_key": format!("registrar:{name}")});
                events.push(release);
            }
        }
        self.events.extend(events);
        let binding = Uuid::from_u128(self.next_seed + 2);
        match shape {
            OracleShape::Unsupported => {
                sqlx::query("DELETE FROM surface_bindings WHERE surface_binding_id = $1")
                    .bind(binding)
                    .execute(&database.pool)
                    .await?;
            }
            OracleShape::Ownerless | OracleShape::Released => {
                sqlx::query(
                    "UPDATE surface_bindings SET active_to = to_timestamp(1700000230)
                     WHERE surface_binding_id = $1",
                )
                .bind(binding)
                .execute(&database.pool)
                .await?;
            }
            _ => {}
        }
        if matches!(shape, OracleShape::Ownerless) {
            sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
                .bind(resource)
                .execute(&database.pool)
                .await?;
        }
        self.names.insert(name.to_owned(), (logical.clone(), resource));
        Ok((logical, resource))
    }

    async fn insert(&mut self, database: &TestDatabase) -> Result<()> {
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &self.events).await?;
        self.events.clear();
        Ok(())
    }
}

/// The oracle fixture, published at 240:
/// - `tie00.eth`..`tie29.eth` share [`ORACLE_TIE`], cycling ens_v2, ens_v0 and ens_v1;
/// - `frac-a.eth` and `frac-b.eth` (the ens_v2 shape) expire a quarter and three quarters into
///   that second;
/// - `early.eth` and `late.eth` expire before and after it, `kid.late.eth` and `kid.tie00.eth`
///   are one label below listed names;
/// - `big.eth` expires at 2^63 and `max.eth` at the largest uint64, past every bigint;
/// - `orphan.eth` is unsupported yet keeps a finite expiry in the tie second, and
///   `ownerless.eth` serves no registration; neither is listed;
/// - `lapsed.eth` was released at 230 and keeps its 2020 expiry.
async fn seed_expiry_oracle_fixture(database: &TestDatabase) -> Result<OracleFixture> {
    seed_bounded_membership_blocks(database, 240).await?;
    let mut fixture = OracleFixture::new();
    for index in 0..ORACLE_TIE_NAMES {
        let shape = match index % 3 {
            0 => OracleShape::EnsV2,
            1 => OracleShape::EnsV0,
            _ => OracleShape::EnsV1,
        };
        fixture.name(database, &format!("tie{index:02}.eth"), shape, json!(ORACLE_TIE)).await?;
    }
    for (name, shape, expiry) in [
        ("frac-a.eth", OracleShape::EnsV2, json!(1_850_000_000.25)),
        ("frac-b.eth", OracleShape::EnsV2, json!(1_850_000_000.75)),
        ("early.eth", OracleShape::EnsV0, json!(1_840_000_000)),
        ("late.eth", OracleShape::EnsV2, json!(1_860_000_000)),
        ("kid.late.eth", OracleShape::EnsV2, json!(ORACLE_TIE)),
        ("kid.tie00.eth", OracleShape::EnsV2, json!(1_855_000_000)),
        ("big.eth", OracleShape::EnsV1, json!(9_223_372_036_854_775_808_u64)),
        ("max.eth", OracleShape::EnsV1, json!(u64::MAX)),
        ("orphan.eth", OracleShape::Unsupported, json!(ORACLE_TIE)),
        ("ownerless.eth", OracleShape::Ownerless, json!(ORACLE_TIE)),
        ("lapsed.eth", OracleShape::Released, json!(1_600_000_000)),
    ] {
        fixture.name(database, name, shape, expiry).await?;
    }
    fixture.insert(database).await?;
    publish_test_families(database, 240).await?;
    Ok(fixture)
}

fn oracle_filter(
    after: Option<&str>,
    before: Option<&str>,
    authorities: Option<&[&str]>,
    parent: Option<&str>,
) -> Result<bigname_storage::NameCurrentExpiringFilter> {
    Ok(bigname_storage::NameCurrentExpiringFilter {
        namespace: "ens".to_owned(),
        expires_after: after.map(str::parse).transpose()?,
        expires_before: before.map(str::parse).transpose()?,
        authorities: authorities
            .map(|values| values.iter().map(|value| (*value).to_owned()).collect()),
        parent: parent.map(str::to_owned),
    })
}

/// A page with the declared topology removed: the listing never serves it, so the walk and the
/// oracle may compose it or not. For an `ens` row that key is all topology enrichment changes.
fn oracle_comparable(
    page: &bigname_storage::NameCurrentListPage,
) -> (Vec<bigname_storage::NameCurrentListRow>, Option<bigname_storage::NameCurrentListCursor>) {
    let rows = page
        .rows
        .iter()
        .cloned()
        .map(|mut row| {
            if let Some(summary) = row.row.declared_summary.as_object_mut() {
                summary.remove("topology");
            }
            row
        })
        .collect();
    (rows, page.next_cursor.clone())
}

/// Every page of the listing at `page_size`, each checked against the oracle at the same cursor;
/// returns the listed names in order.
async fn oracle_walk(
    database: &TestDatabase,
    filter: &bigname_storage::NameCurrentExpiringFilter,
    order: bigname_storage::NameCurrentListOrder,
    page_size: u64,
) -> Result<Vec<String>> {
    let chains = [FAMILY_CHAIN.to_owned()];
    let mut cursor = None;
    let mut listed = Vec::new();
    for _ in 0..200 {
        let walked = bigname_storage::families::name::load_family_expiring_page(
            &database.pool, filter, order, cursor.as_ref(), page_size, &chains,
        )
        .await?;
        let oracle = bigname_storage::families::name::seams::load_family_expiring_page_unbounded(
            &database.pool, filter, order, cursor.as_ref(), page_size, &chains,
        )
        .await?;
        assert_eq!(
            oracle_comparable(&walked),
            oracle_comparable(&oracle),
            "{filter:?} {order:?} page_size {page_size} after {cursor:?}"
        );
        listed.extend(walked.rows.iter().map(|row| row.row.normalized_name.clone()));
        match walked.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(listed),
        }
    }
    anyhow::bail!("{filter:?}: the walk did not end")
}

fn oracle_orders() -> [bigname_storage::NameCurrentListOrder; 2] {
    [bigname_storage::NameCurrentListOrder::Asc, bigname_storage::NameCurrentListOrder::Desc]
}

#[tokio::test]
async fn v2_names_expiry_pages_equal_the_unbounded_oracle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expiry_oracle_fixture(&database).await?;
    type Case<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a [&'a str]>, Option<&'a str>);
    // (expires_after, expires_before, authority, parent); the first three also page one row
    // at a time, so every cursor of the shared second is resumed from.
    let cases: [Case; 11] = [
        (Some("1850000000"), Some("1850000001"), None, None),
        (Some("0"), None, None, None),
        (Some("0"), None, Some(&["ens_v0"]), None),
        (Some("1800000000"), Some("1900000000"), Some(&["ens_v1"]), None),
        (Some("1850000000.25"), Some("1850000000.75"), None, None),
        (None, Some("1850000000.25"), None, None),
        (Some("9223372036854775807"), None, None, None),
        (Some("0"), None, Some(&["ens_v0", "ens_v2"]), None),
        (Some("0"), None, None, Some("eth")),
        (Some("0"), None, Some(&["ens_v2"]), Some("late.eth")),
        (Some("1850000000"), Some("1850000001"), Some(&["ens_v1"]), Some("eth")),
    ];
    for (index, (after, before, authorities, parent)) in cases.into_iter().enumerate() {
        let filter = oracle_filter(after, before, authorities, parent)?;
        for order in oracle_orders() {
            let mut listed = Vec::new();
            for (page_size, batch) in [(1, 1_000), (7, 1_000), (7, 3)] {
                if page_size == 1 && index > 2 {
                    continue;
                }
                listed.push(
                    bigname_storage::families::name::seams::with_batch_size(
                        batch,
                        oracle_walk(&database, &filter, order, page_size),
                    )
                    .await?,
                );
            }
            assert!(listed.windows(2).all(|pair| pair[0] == pair[1]), "{filter:?} {order:?}");
            assert!(!listed[0].is_empty(), "{filter:?} lists nothing");
        }
    }
    database.cleanup().await
}

// What the oracle fixture lists, pinned so the comparison above cannot pass on an empty or
// degenerate listing.
#[tokio::test]
async fn v2_names_expiry_oracle_fixture_lists_each_shape() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expiry_oracle_fixture(&database).await?;
    let asc = bigname_storage::NameCurrentListOrder::Asc;
    let all = oracle_walk(&database, &oracle_filter(Some("0"), None, None, None)?, asc, 200).await?;
    let ties = (0..ORACLE_TIE_NAMES).map(|index| format!("tie{index:02}.eth"));
    let mut expected = vec!["lapsed.eth".to_owned(), "early.eth".to_owned()];
    let mut tie_second: Vec<String> = ties
        .chain(["kid.late.eth".to_owned()])
        .collect();
    tie_second.sort();
    expected.extend(tie_second);
    expected.extend(
        ["frac-a.eth", "frac-b.eth", "kid.tie00.eth", "late.eth", "big.eth", "max.eth"]
            .map(str::to_owned),
    );
    assert_eq!(all, expected, "orphan.eth and ownerless.eth are never listed");
    for (authority, names) in [
        ("ens_v0", vec!["early.eth", "tie01.eth", "tie04.eth"]),
        ("ens_v2", vec!["kid.late.eth", "tie00.eth", "tie03.eth"]),
    ] {
        let listed = oracle_walk(
            &database,
            &oracle_filter(Some("0"), Some("1850000000.5"), Some(&[authority]), None)?,
            asc,
            3,
        )
        .await?;
        assert_eq!(&listed[..3], names, "{authority}");
    }
    database.cleanup().await
}

/// One listing page with its work: (page, names composed, largest source, rows submitted).
async fn oracle_counted_page(
    database: &TestDatabase,
    filter: &bigname_storage::NameCurrentExpiringFilter,
    page_size: u64,
) -> Result<(bigname_storage::NameCurrentListPage, u64, u64, u64)> {
    use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
    use bigname_storage::families::name::seams;
    let chains = [FAMILY_CHAIN.to_owned()];
    let (composed, peak, submitted) =
        (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let page = seams::with_composed_names_counter(
        composed.clone(),
        seams::with_peak_source_counter(
            peak.clone(),
            seams::with_submitted_rows_counter(
                submitted.clone(),
                bigname_storage::families::name::load_family_expiring_page(
                    &database.pool, filter, bigname_storage::NameCurrentListOrder::Asc, None,
                    page_size, &chains,
                ),
            ),
        ),
    )
    .await?;
    Ok((
        page,
        composed.load(Ordering::Relaxed),
        peak.load(Ordering::Relaxed),
        submitted.load(Ordering::Relaxed),
    ))
}

// A page composes at most `page_size + 1` names and binds them to one page statement, whatever
// the filter leaves out and however many names share a second. Stale expiries (a name renewed
// out of the window many times) leave the page equal to the oracle and add no composition.
#[tokio::test]
async fn v2_names_expiry_listing_counts_its_composition_work() -> Result<()> {
    use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
    use bigname_storage::families::name::seams;
    let database = TestDatabase::new_migrated().await?;
    let mut fixture = seed_expiry_oracle_fixture(&database).await?;
    let filter = oracle_filter(Some("0"), None, Some(&["ens_v0"]), None)?;
    let (page, composed, peak, submitted) = oracle_counted_page(&database, &filter, 2).await?;
    eprintln!("expiring page_size=2 ens_v0: composed={composed} peak={peak} submitted={submitted}");
    assert_eq!(page.rows.len(), 2);
    assert_eq!((composed, peak, submitted), (3, 3, 3));
    // Sparse filters, a window inside the shared second and a window that holds one name: the
    // names the filter rejects are never composed.
    for (filter, page_size, listed) in [
        (oracle_filter(Some("0"), None, None, Some("late.eth"))?, 5, 1),
        (oracle_filter(Some("0"), None, Some(&["ens_v1"]), Some("eth"))?, 4, 4),
        (oracle_filter(Some("1850000000"), Some("1850000001"), None, None)?, 3, 3),
        (oracle_filter(Some("1850000000.5"), Some("1850000001"), None, None)?, 200, 1),
        (oracle_filter(Some("9223372036854775808"), None, None, None)?, 200, 2),
    ] {
        let (page, composed, peak, submitted) =
            oracle_counted_page(&database, &filter, page_size).await?;
        assert_eq!(page.rows.len() as u64, listed, "{filter:?}");
        let selected = listed + u64::from(page.next_cursor.is_some());
        assert_eq!((composed, peak, submitted), (selected, selected, selected), "{filter:?}");
        assert!(composed <= page_size + 1, "{filter:?} composed {composed}");
    }

    // The search listing binds its composed rows to its page statement: the peak is the largest
    // such source and the submitted count their sum.
    let (peak, submitted) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let search = bigname_storage::NameCurrentListFilter {
        namespace: Some("ens".to_owned()),
        supported_only: true,
        ..Default::default()
    };
    let searched = seams::with_peak_source_counter(
        peak.clone(),
        seams::with_submitted_rows_counter(
            submitted.clone(),
            seams::with_batch_size(
                3,
                bigname_storage::families::name::load_family_search_page(
                    &database.pool, &search, None, 5,
                ),
            ),
        ),
    )
    .await?;
    let (peak, submitted) = (peak.load(Ordering::Relaxed), submitted.load(Ordering::Relaxed));
    assert_eq!(searched.rows.len(), 5);
    assert!(peak >= 5 && submitted > peak, "search peak {peak} of {submitted} submitted");

    // The oracle refuses a namespace it does not cover.
    let mut basenames = filter.clone();
    basenames.namespace = "basenames".to_owned();
    assert!(
        seams::load_family_expiring_page_unbounded(
            &database.pool, &basenames, bigname_storage::NameCurrentListOrder::Asc, None, 2,
            &[FAMILY_CHAIN.to_owned()],
        )
        .await
        .is_err()
    );

    // late.eth renews twenty times through a window it then leaves.
    let (late, resource) = fixture.names["late.eth"].clone();
    for step in 0..20 {
        let mut renewal = fixture.event(&late, resource, "RegistrationRenewed",
            "ens_v2_registry_l1", json!({"expiry": 1_810_000_000 + step}));
        renewal.block_number = Some(241);
        renewal.block_hash = Some("0xhistory241".to_owned());
        renewal.transaction_hash = Some("0xtx241".to_owned());
        fixture.events.push(renewal);
    }
    let mut last = fixture.event(&late, resource, "RegistrationRenewed", "ens_v2_registry_l1",
        json!({"expiry": 1_860_000_000}));
    last.block_number = Some(241);
    last.block_hash = Some("0xhistory241".to_owned());
    last.transaction_hash = Some("0xtx241".to_owned());
    fixture.events.push(last);
    fixture.insert(&database).await?;
    publish_test_families(&database, 241).await?;
    let stale = oracle_filter(Some("1810000000"), Some("1810000100"), None, None)?;
    let (page, composed, peak, submitted) = oracle_counted_page(&database, &stale, 2).await?;
    assert!(page.rows.is_empty() && page.next_cursor.is_none(), "{page:?}");
    assert_eq!((composed, peak, submitted), (0, 0, 0), "a stale expiry composed a name");
    let asc = bigname_storage::NameCurrentListOrder::Asc;
    assert!(oracle_walk(&database, &stale, asc, 2).await?.is_empty());
    let all = oracle_walk(&database, &oracle_filter(Some("1800000000"), None, None, None)?, asc, 7)
        .await?;
    assert!(all.contains(&"late.eth".to_owned()), "{all:?}");
    database.cleanup().await
}

// A listing paused after it reads its publication holds one connection; another listing and a
// detail read still finish before it resumes.
#[tokio::test]
async fn v2_names_expiry_listing_does_not_serialize_other_reads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expiry_oracle_fixture(&database).await?;
    let chains = [FAMILY_CHAIN.to_owned()];
    let filter = oracle_filter(Some("0"), None, None, None)?;
    let asc = bigname_storage::NameCurrentListOrder::Asc;
    let late = bigname_storage::logical_name_id_for_name("ens", "late.eth");
    let pool = database.pool.clone();
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let paused = bigname_storage::families::name::seams::with_pause_after_publication(
        reached.clone(),
        resume.clone(),
        bigname_storage::families::name::load_family_expiring_page(
            &pool, &filter, asc, None, 5, &chains,
        ),
    );
    tokio::pin!(paused);
    let mut others = None;
    let page = loop {
        tokio::select! {
            page = &mut paused => break page?,
            () = reached.notified() => {
                if others.is_none() {
                    let other = bigname_storage::families::name::load_family_expiring_page(
                        &database.pool, &filter, asc, None, 5, &chains,
                    );
                    let detail = bigname_storage::families::name::load_family_name(
                        &database.pool, &late,
                    );
                    let (other, detail) = tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        async { tokio::join!(other, detail) },
                    )
                    .await
                    .context("reads waited on the paused listing")?;
                    others = Some((other?, detail?));
                }
                resume.notify_one();
            }
        }
    };
    let (other, detail) = others.context("the listing never paused")?;
    assert_eq!(oracle_comparable(&other), oracle_comparable(&page));
    assert!(detail.is_some());
    database.cleanup().await
}

// A listing paused after it reads its publication answers from that publication, though a
// newer one lands meanwhile; the next request sees the newer one.
#[tokio::test]
async fn v2_names_expiry_listing_answers_from_its_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_expiry_oracle_fixture(&database).await?;
    let chains = [FAMILY_CHAIN.to_owned()];
    let filter = oracle_filter(Some("1849000000"), Some("1851000000"), None, None)?;
    let asc = bigname_storage::NameCurrentListOrder::Asc;
    let before = bigname_storage::families::name::seams::load_family_expiring_page_unbounded(
        &database.pool, &filter, asc, None, 3, &chains,
    )
    .await?;
    let (early, early_resource) = fixture.names["early.eth"].clone();
    let pool = database.pool.clone();
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let paused = bigname_storage::families::name::seams::with_pause_after_publication(
        reached.clone(),
        resume.clone(),
        bigname_storage::families::name::load_family_expiring_page(
            &pool, &filter, asc, None, 3, &chains,
        ),
    );
    tokio::pin!(paused);
    let mut advanced = false;
    let page = loop {
        tokio::select! {
            page = &mut paused => break page?,
            () = reached.notified() => {
                if !advanced {
                    // early.eth renews into the window ahead of every tie name.
                    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[family_event(
                        "oracle-early-renewal", Some(&early), Some(early_resource),
                        "RegistrationRenewed", "ens_v1_registrar_l1", 241, 0,
                        json!({"expiry": 1_849_500_000}),
                    )]).await?;
                    publish_test_families(&database, 241).await?;
                    advanced = true;
                }
                resume.notify_one();
            }
        }
    };
    assert!(advanced, "the listing never paused");
    assert_eq!(oracle_comparable(&page), oracle_comparable(&before));
    let after = bigname_storage::families::name::load_family_expiring_page(
        &database.pool, &filter, asc, None, 3, &chains,
    )
    .await?;
    assert_eq!(after.rows[0].row.normalized_name, "early.eth");
    assert_ne!(oracle_comparable(&after), oracle_comparable(&before));
    database.cleanup().await
}
