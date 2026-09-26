//! F2b, F8 and F9 through the family loop: wrapper state and expiry, grants with revocations
//! and the admin aggregate, account approvals with an explicit `false`. Each case undoes its last
//! block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const WRAPPER: &str = "0x00000000000000000000000000000000000000d4";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const ALICE: &str = "0x00000000000000000000000000000000000000a1";
const BOB: &str = "0x00000000000000000000000000000000000000b2";

fn columns(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

#[tokio::test]
async fn wrapper_state_keeps_the_latest_fuses_and_the_latest_wrapper_expiry() -> Result<()> {
    let fixture = Fixture::new("families_wrapper_state", 20).await?;
    let resource = uuid(1);
    fixture
        .write(
            10,
            1,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            None,
            Some(&resource),
            json!({"fuses": 65537, "wrapper_state": "locked", "expiry": 2000}),
            WRAPPER,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            None,
            Some(&resource),
            json!({"expiry": 3000, "source_event": "ExpiryExtended"}),
            WRAPPER,
        )
        .await?;
    // A registrar renewal that is not the wrapper's does not move the wrapper expiry.
    fixture
        .write(
            12,
            1,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            None,
            Some(&resource),
            json!({"expiry": 9000, "source_event": "NameRenewed", "authority_kind": "registrar"}),
            WRAPPER,
        )
        .await?;
    fixture
        .write(
            12,
            2,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            None,
            Some(&resource),
            json!({"fuses": 0, "wrapper_state": "unknown"}),
            WRAPPER,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_wrapper_state").await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        columns(&rows[0], &["wrapper_state", "fuses", "expiry_seconds"]),
        json!({"wrapper_state": null, "fuses": 0, "expiry_seconds": 3000})
    );
    assert_eq!(rows[0]["expiry_position"]["block_number"], json!(11));
    assert_eq!(rows[0]["wrapper_state_position"]["block_number"], json!(12));
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn grants_keep_revocations_and_the_admin_aggregate_follows_each_holder() -> Result<()> {
    let fixture = Fixture::new("families_grants", 20).await?;
    let resource = uuid(2);
    let registry_scope =
        json!({"kind": "registry", "chain_id": "ethereum-sepolia", "registry_address": REGISTRY});
    let grant = |subject: &str, powers: Value, revoked: bool| {
        let mut after = json!({"subject": subject, "scope": registry_scope, "effective_powers": powers,
                               "grant_source": {"kind": "raw_log"}, "inheritance_path": [],
                               "transfer_behavior": "stays"});
        if revoked {
            after["revocation_source"] = json!({"kind": "raw_log"});
        }
        after
    };
    fixture
        .write(
            10,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(ALICE, json!(["admin_roles", "set_resolver"]), false),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(BOB, json!(["can_transfer_admin"]), false),
            REGISTRY,
        )
        .await?;
    // Alice's grant is revoked: the grant row stays, her admin entry goes.
    fixture
        .write(
            11,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(ALICE, json!([]), true),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;

    let mut grants = fixture.rows("project_grant").await?;
    grants.sort_by_key(|row| row["subject"].to_string());
    assert_eq!(
        grants
            .iter()
            .map(|row| columns(
                row,
                &[
                    "subject",
                    "scope",
                    "scope_kind",
                    "revoked",
                    "effective_powers"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"subject": ALICE, "scope": "registry", "scope_kind": "registry", "revoked": true, "effective_powers": []}),
            json!({"subject": BOB, "scope": "registry", "scope_kind": "registry", "revoked": false, "effective_powers": ["can_transfer_admin"]}),
        ]
    );
    assert_eq!(grants[1]["transfer_behavior"], json!({"mode": "stays"}));
    let aggregate = fixture.rows("project_resource_admin_aggregate").await?;
    assert_eq!(
        aggregate
            .iter()
            .map(|row| row["admin_powers"].clone())
            .collect::<Vec<_>>(),
        vec![json!({format!("{BOB}|registry"): ["can_transfer_admin"]})]
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;

    // Bob's revocation empties the aggregate, which then has no fact left and goes.
    fixture
        .write(
            12,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(BOB, json!([]), true),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    assert!(
        fixture
            .rows("project_resource_admin_aggregate")
            .await?
            .is_empty()
    );
    assert_eq!(fixture.rows("project_grant").await?.len(), 2);
    fixture.assert_undo_restores(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn approvals_keep_an_explicit_false() -> Result<()> {
    let fixture = Fixture::new("families_approvals", 20).await?;
    let approval = |approved: bool| {
        json!({"subject": BOB, "relation_kind": "operator", "approved": approved,
               "scope": {"kind": "account", "authority_kind": "registry",
                         "authority_contract": REGISTRY.to_uppercase().replace("0X", "0x"), "owner": ALICE},
               "effective_powers": if approved { json!(["registry_control"]) } else { json!([]) },
               "inheritance_path": [], "transfer_behavior": {"mode": "owner_scoped"}})
    };
    fixture
        .write(
            10,
            1,
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            None,
            None,
            approval(true),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            None,
            None,
            approval(false),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let rows = fixture.rows("project_account_approval").await?;
    assert_eq!(
        rows.iter()
            .map(|row| columns(
                row,
                &[
                    "authority_kind",
                    "authority_contract",
                    "owner",
                    "subject",
                    "approved"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"authority_kind": "registry", "authority_contract": REGISTRY, "owner": ALICE,
                    "subject": BOB, "approved": false})
        ]
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// A differential check against PostgreSQL itself for the approval flag: each value goes through
// the served builder's `(after_state ->> 'approved')::boolean` and through the family reducer.
// Where the cast gives a boolean, the family row carries it. Where the cast rejects the value, or
// gives null (the served column is NOT NULL), the served batch fails; the family keeps no row
// for that event.
#[tokio::test]
async fn an_approval_flag_matches_the_served_boolean_cast() -> Result<()> {
    let fixture = Fixture::new("families_approval_cast", 20).await?;
    let mut values: Vec<Value> = [
        "t",
        "tr",
        "tru",
        "true",
        "f",
        "fa",
        "fal",
        "fals",
        "false",
        "y",
        "ye",
        "yes",
        "n",
        "no",
        "on",
        "of",
        "off",
        "1",
        "0",
        "TrUe",
        "FALSE",
        "Off",
        "yEs",
        "o",
        "",
        "   ",
        "junk",
        "truex",
        "2",
        "01",
        "\u{a0}off\u{a0}",
        "\u{2003}on",
        "off\u{3000}",
        "\u{85}no",
    ]
    .into_iter()
    .map(|text| json!(text))
    .collect();
    for space in [' ', '\t', '\n', '\r', '\u{b}', '\u{c}'] {
        values.push(json!(format!("{space}off{space}")));
        values.push(json!(format!("{space}{space}yes")));
    }
    values.extend([
        json!(true),
        json!(false),
        Value::Null,
        json!(0),
        json!(1),
        json!(2),
        json!(1.0),
    ]);
    let mut expected = Vec::new();
    for (n, value) in (1..).zip(&values) {
        let cast: std::result::Result<Option<bool>, sqlx::Error> = sqlx::query_scalar(
            "SELECT (jsonb_build_object('approved', $1::jsonb) ->> 'approved')::boolean",
        )
        .bind(value)
        .fetch_one(&fixture.pool)
        .await;
        let flag = match cast {
            Ok(flag) => flag,
            Err(sqlx::Error::Database(error)) => {
                assert_eq!(error.code().as_deref(), Some("22P02"), "{value:?}: {error}");
                None
            }
            Err(error) => return Err(error.into()),
        };
        let subject = format!("0x{:040x}", 0x100 + n);
        fixture
            .write(
                10,
                n,
                "AccountPermissionChanged",
                "ens_v1_registry_l1",
                None,
                None,
                json!({"subject": subject, "relation_kind": "operator", "approved": value,
                       "scope": {"kind": "account", "authority_kind": "registry",
                                 "authority_contract": REGISTRY, "owner": ALICE},
                       "effective_powers": [], "inheritance_path": [],
                       "transfer_behavior": {"mode": "owner_scoped"}}),
                REGISTRY,
            )
            .await?;
        expected.push((value.clone(), subject, flag));
    }
    fixture.apply(10, FamilyMode::Normal).await;
    let rows = fixture.rows("project_account_approval").await?;
    let mismatches: Vec<String> = expected
        .iter()
        .filter_map(|(value, subject, flag)| {
            let stored = rows
                .iter()
                .find(|row| row["subject"] == json!(subject))
                .map(|row| row["approved"].clone());
            let want = flag.map(|flag| json!(flag));
            (stored != want).then(|| format!("{value:?}: family {stored:?}, served {want:?}"))
        })
        .collect();
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    fixture.cleanup().await
}

/// The served wrapper expiry: address_names.rs `wrapper_expiries` (:39-48), permissions.rs
/// `wrapper_expiries` (:158-166) and children.rs `latest_wrapper_expiries` (:164-167) keep the
/// numeric value from 0 to 2^64 - 1, and name_current/build.sql `expiry_seconds` (:557-563) is
/// the same expression.
const SERVED_WRAPPER_EXPIRY: &str = "CASE
    WHEN jsonb_typeof(after_state -> 'expiry') = 'number'
     AND (after_state ->> 'expiry')::numeric >= 0
     AND (after_state ->> 'expiry')::numeric <= 18446744073709551615
        THEN (after_state ->> 'expiry')::numeric END";
/// The served scope fuses: permissions.rs `modifiers` (:138-141) and address_names.rs
/// `scope_modifiers` (:70-77) cast a value from 0 to 2^63 - 1 to bigint, which fails with
/// 22P02 on a decimal spelling.
const SERVED_SCOPE_FUSES: &str = "CASE
    WHEN jsonb_typeof(after_state -> 'fuses') = 'number'
     AND (after_state ->> 'fuses')::numeric >= 0
     AND (after_state ->> 'fuses')::numeric <= 9223372036854775807
        THEN (after_state ->> 'fuses')::bigint END";
/// The served servable expiry: name_current/build.sql `servable_expiry_seconds` (:568-574)
/// casts an integral value from 1 to 253402300799 to bigint, which fails with 22P02 on an
/// integral decimal spelling such as 1.0 or 1000.0.
const SERVED_SERVABLE_EXPIRY: &str = "CASE
    WHEN jsonb_typeof(after_state -> 'expiry') = 'number'
     AND (after_state ->> 'expiry')::numeric = trunc((after_state ->> 'expiry')::numeric)
     AND (after_state ->> 'expiry')::numeric BETWEEN 1 AND 253402300799
        THEN (after_state ->> 'expiry')::bigint END";

/// One served expression over `{field: literal}`: its value as JSON, or `None` when it fails
/// with 22P02, the cast error that fails the served batch.
async fn served(
    fixture: &Fixture,
    expression: &str,
    field: &str,
    literal: &str,
) -> Result<Option<Value>> {
    let result: std::result::Result<Option<Value>, sqlx::Error> = sqlx::query_scalar(&format!(
        "SELECT to_jsonb({expression})
         FROM (SELECT jsonb_build_object($1::text, $2::text::jsonb) AS after_state) event"
    ))
    .bind(field)
    .bind(literal)
    .fetch_one(&fixture.pool)
    .await;
    match result {
        Ok(value) => Ok(Some(value.unwrap_or(Value::Null))),
        Err(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("22P02"), "{literal}: {error}");
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

// The wrapper expiry and fuses against the served expressions above, each case a jsonb literal
// written into the stored event as it is, so `1e3` is the jsonb numeric 1000 and not the Rust
// f64 1000.0. The fuses match SERVED_SCOPE_FUSES; where that cast fails the served batch, the
// family keeps no fuses. The expiry matches SERVED_WRAPPER_EXPIRY except for a decimal spelling,
// which the family keeps as null because it may arrive rounded (a_decimal_expiry_is_not_rounded);
// the adapter writes the expiry as a JSON integer. SERVED_SERVABLE_EXPIRY fails on exactly the
// integral decimal spellings 1.0 and 1000.0, where the family expiry is null.
#[tokio::test]
async fn wrapper_numbers_match_the_served_numeric_reads() -> Result<()> {
    let fixture = Fixture::new("families_wrapper_numbers", 20).await?;
    let literals = [
        "0",
        "1",
        "1.0",
        "1.5",
        "1000.0",
        "1e3",
        "-1",
        "-0.0",
        "-0.5",
        "2000",
        "4294967295",
        "9223372036854775807",
        "9223372036854775808",
        "18446744073709551615",
        "1.8e19",
        "1e20",
        "\"5\"",
        "true",
        "null",
    ];
    let mut expected = Vec::new();
    let mut servable_failures = Vec::new();
    for (n, literal) in (1..).zip(literals) {
        let spelled: String = sqlx::query_scalar("SELECT $1::text::jsonb::text")
            .bind(literal)
            .fetch_one(&fixture.pool)
            .await?;
        let expiry = served(&fixture, SERVED_WRAPPER_EXPIRY, "expiry", literal)
            .await?
            .expect("the served numeric expiry never fails");
        let expiry = if spelled.contains('.') {
            Value::Null
        } else {
            expiry
        };
        let fuses = served(&fixture, SERVED_SCOPE_FUSES, "fuses", literal)
            .await?
            .unwrap_or(Value::Null);
        if served(&fixture, SERVED_SERVABLE_EXPIRY, "expiry", literal)
            .await?
            .is_none()
        {
            servable_failures.push(literal);
        }
        let resource = uuid(100 + n);
        for (log, kind, after, field) in [
            (
                2 * i64::from(n),
                "PermissionScopeChanged",
                json!({"fuses": 0, "wrapper_state": "wrapped"}),
                "fuses",
            ),
            (
                2 * i64::from(n) + 1,
                "ExpiryChanged",
                json!({"expiry": 0, "source_event": "ExpiryExtended"}),
                "expiry",
            ),
        ] {
            let id = fixture
                .write(
                    10,
                    log,
                    kind,
                    "ens_v1_wrapper_l1",
                    None,
                    Some(&resource),
                    after,
                    WRAPPER,
                )
                .await?;
            sqlx::query(
                "UPDATE normalized_events
                 SET after_state = jsonb_set(after_state, ARRAY[$2::text], $3::text::jsonb)
                 WHERE normalized_event_id = $1",
            )
            .bind(id)
            .bind(field)
            .bind(literal)
            .execute(&fixture.pool)
            .await?;
        }
        expected.push((resource, literal, fuses, expiry));
    }
    assert_eq!(servable_failures, ["1.0", "1000.0"]);
    fixture.apply(10, FamilyMode::Normal).await;
    let rows = fixture.rows("project_wrapper_state").await?;
    let mut mismatches = Vec::new();
    for (resource, literal, fuses, expiry) in expected {
        let row = rows
            .iter()
            .find(|row| row["resource_id"] == json!(resource))
            .expect("each resource has a wrapper row");
        let got = (row["fuses"].clone(), row["expiry_seconds"].clone());
        if servable_failures.contains(&literal) {
            assert_eq!(got.1, Value::Null, "{literal}: the family keeps no expiry");
        }
        if got != (fuses.clone(), expiry.clone()) {
            mismatches.push(format!(
                "{literal}: family {got:?}, expected ({fuses}, {expiry})"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    fixture.assert_undo_restores(10).await?;
    fixture.assert_rebuild_equal(10).await?;
    fixture.cleanup().await
}

// A decimal expiry reaches the family as a best-effort f64, so its value may already have
// moved (9007199254740993.0 arrives as 9007199254740994, and 9007199254740991.0, below 2^53,
// as 9007199254740990). The family keeps no expiry for a decimal spelling rather than a
// different one; an integer, 2^53 + 1 included, is kept exactly.
#[tokio::test]
async fn a_decimal_expiry_is_not_rounded() -> Result<()> {
    let fixture = Fixture::new("families_wrapper_decimal_expiry", 20).await?;
    let cases = [
        ("9007199254740991.0", None),
        ("9007199254740992.0", None),
        ("9007199254740993.0", None),
        ("9007199254740993", Some("9007199254740993")),
    ];
    for (n, (spelling, _)) in (1..).zip(&cases) {
        let id = fixture
            .write(
                10,
                i64::from(n),
                "ExpiryChanged",
                "ens_v1_wrapper_l1",
                None,
                Some(&uuid(200 + n)),
                json!({"expiry": 0, "source_event": "ExpiryExtended"}),
                WRAPPER,
            )
            .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET after_state = jsonb_set(after_state, '{expiry}', $2::text::jsonb)
             WHERE normalized_event_id = $1",
        )
        .bind(id)
        .bind(spelling)
        .execute(&fixture.pool)
        .await?;
    }
    fixture.apply(10, FamilyMode::Normal).await;
    let mut mismatches = Vec::new();
    for (n, (spelling, expected)) in (1..).zip(&cases) {
        let got: Option<String> = sqlx::query_scalar(
            "SELECT expiry_seconds::text FROM project_wrapper_state WHERE resource_id = $1::uuid",
        )
        .bind(uuid(200 + n))
        .fetch_one(&fixture.pool)
        .await?;
        if got.as_deref() != *expected {
            mismatches.push(format!("{spelling}: family {got:?}, expected {expected:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    fixture.cleanup().await
}

// A grant is revoked when it was cleared, its effective powers empty; the revocation source is
// provenance only. The served wrapper fuse and grace masks are not applied here.
#[tokio::test]
async fn a_grant_is_revoked_exactly_when_its_powers_are_empty() -> Result<()> {
    let fixture = Fixture::new("families_grant_revoked", 20).await?;
    let resource = uuid(3);
    let scope =
        json!({"kind": "registry", "chain_id": "ethereum-sepolia", "registry_address": REGISTRY});
    let cases = [
        (
            ALICE,
            json!({"effective_powers": [], "grant_source": {"kind": "raw_log"}}),
            true,
        ),
        (
            BOB,
            json!({"effective_powers": ["set_resolver"], "revocation_source": {"kind": "raw_log"}}),
            false,
        ),
        // The adapter's reservation shape: a remaining power beside a revocation source.
        (
            WRAPPER,
            json!({"effective_powers": ["was_reserved"],
                   "revocation_source": {"kind": "raw_log", "relation_kind": "holder"}}),
            false,
        ),
        // A clear whose revocation source is JSON null.
        (
            REGISTRY,
            json!({"effective_powers": [], "grant_source": {"kind": "raw_log"},
                   "revocation_source": null}),
            true,
        ),
    ];
    for (n, (subject, extra, _)) in (1..).zip(&cases) {
        let mut after = json!({"subject": subject, "scope": scope, "inheritance_path": [],
                               "transfer_behavior": "stays"});
        if let (Value::Object(after), Value::Object(extra)) = (&mut after, extra.clone()) {
            after.extend(extra);
        }
        fixture
            .write(
                10,
                n,
                "PermissionChanged",
                "ens_v2_registry_l1",
                None,
                Some(&resource),
                after,
                REGISTRY,
            )
            .await?;
    }
    fixture.apply(10, FamilyMode::Normal).await;
    let rows = fixture.rows("project_grant").await?;
    for (subject, _, revoked) in cases {
        let row = rows
            .iter()
            .find(|row| row["subject"] == json!(subject))
            .expect("each subject has a grant row");
        assert_eq!(row["revoked"], json!(revoked), "{subject}");
    }
    fixture.assert_undo_restores(10).await?;
    fixture.assert_rebuild_equal(10).await?;
    fixture.cleanup().await
}

// A grant records the registration it belongs to: the resource's latest RegistrationGranted or
// RegistrationReserved before it, the F2a last_active rule, counting earlier events of its own
// block. A registration later in the block does not move an earlier grant's.
#[tokio::test]
async fn a_grant_records_the_registration_it_was_written_under() -> Result<()> {
    let fixture = Fixture::new("families_grant_registration", 20).await?;
    let (resource, bare) = (uuid(4), uuid(5));
    let scope =
        json!({"kind": "registry", "chain_id": "ethereum-sepolia", "registry_address": REGISTRY});
    let grant = |subject: &str| {
        json!({"subject": subject, "scope": scope, "effective_powers": ["set_resolver"],
               "grant_source": {"kind": "raw_log"}, "inheritance_path": [],
               "transfer_behavior": "stays"})
    };
    let registration = json!({"registry_contract_instance_id": "registry", "token_id": "1"});
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            registration.clone(),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(ALICE),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    fixture
        .write(
            12,
            1,
            "RegistrationReserved",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            registration.clone(),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            2,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(BOB),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            3,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&bare),
            grant(ALICE),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            4,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            registration,
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_grant").await?;
    let registration_of = |resource: &str, subject: &str| {
        rows.iter()
            .find(|row| row["resource_id"] == json!(resource) && row["subject"] == json!(subject))
            .map(|row| {
                json!([
                    row["registration_position"]["block_number"],
                    row["registration_position"]["log_index"]
                ])
            })
    };
    assert_eq!(registration_of(&resource, ALICE), Some(json!([10, 1])));
    assert_eq!(registration_of(&resource, BOB), Some(json!([12, 1])));
    assert_eq!(registration_of(&bare, ALICE), Some(json!([null, null])));
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// A RegistrationReleased between the registration and the grant does not move the grant's
// registration_position: the rule takes grants and reservations only.
#[tokio::test]
async fn a_release_before_the_grant_leaves_its_registration_position() -> Result<()> {
    let fixture = Fixture::new("families_grant_registration_release", 20).await?;
    let resource = uuid(6);
    let registration = json!({"registry_contract_instance_id": "registry", "token_id": "1"});
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            registration.clone(),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            registration,
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            json!({"subject": ALICE, "effective_powers": ["set_resolver"],
                   "scope": {"kind": "registry", "chain_id": "ethereum-sepolia",
                             "registry_address": REGISTRY},
                   "grant_source": {"kind": "raw_log"}, "inheritance_path": [],
                   "transfer_behavior": "stays"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_grant").await?;
    assert_eq!(
        rows.iter()
            .map(|row| json!([
                row["registration_position"]["block_number"],
                row["registration_position"]["log_index"]
            ]))
            .collect::<Vec<_>>(),
        vec![json!([10, 1])]
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
