//! Compare every named schema enum to its producer, including values a fixture
//! may not happen to serve. This file is a child of `v2` for module visibility.

use std::collections::{BTreeMap, BTreeSet};

use axum::http::StatusCode;
use bigname_domain::resolver_read::ResolverReadFeature;
use serde::Serialize;
use serde_json::Value;

use super::{
    error::ErrorCode,
    history::HistoryRowSubject,
    name_filter::NameMatch,
    params::SortOrder,
    permission_support::UnlistedPermissionSurface,
    permission_values::GrantRelation,
    support::status_freshness::NetworkHeadStatus,
    vocab::{
        AddressNamesDedupe, AddressNamesSort, Authority, AuthorityContext, Completeness, Finality,
        HistoryEventType, HistoryScope, OpsStatus, RegistrationStatus, Relation, Source, Status,
        WrapperState,
    },
};
use crate::tests::openapi_contract::document;

// One variant list supplies both the values and an exhaustive match. Adding a
// production variant therefore fails compilation instead of silently escaping
// an independently maintained test array. Serialization supplies wire spellings.
macro_rules! variants {
    ($enum:ident: $($variant:ident),+ $(,)?) => {
        [$($enum::$variant),+].map(|value| match value {
            $($enum::$variant => $enum::$variant),+
        })
    };
}

fn serialized(value: impl Serialize) -> String {
    serde_json::to_value(value)
        .expect("wire enum serializes")
        .as_str()
        .expect("wire enum is a string")
        .to_owned()
}

fn strings(value: &Value) -> BTreeSet<String> {
    let values = value.as_array().expect("enum is an array");
    let result: BTreeSet<_> = values
        .iter()
        .map(|value| value.as_str().expect("enum member is a string").to_owned())
        .collect();
    assert_eq!(values.len(), result.len(), "enum members must be unique");
    result
}

fn compare(covered: &mut BTreeSet<String>, name: &str, values: impl IntoIterator<Item = String>) {
    assert!(
        covered.insert(name.to_owned()),
        "duplicate enum check {name}"
    );
    let values: Vec<_> = values.into_iter().collect();
    let actual: BTreeSet<_> = values.iter().cloned().collect();
    assert_eq!(
        values.len(),
        actual.len(),
        "duplicate producer value in {name}"
    );
    let declared = strings(&document()["components"]["schemas"][name]["enum"]);
    assert_eq!(
        declared, actual,
        "{name} must match its complete producer vocabulary"
    );
}

/// A narrow, fail-closed reader for inaccessible unit-enum declarations. It
/// accepts only comments, unit variants and explicit serde rename attributes;
/// payloads, new attributes or expressions require choosing a new test adapter.
fn source_enum(source: &str, name: &str, serialized_enum: bool) -> Vec<(String, String)> {
    let declaration = format!("enum {name} {{");
    let (before, after) = source
        .split_once(&declaration)
        .expect("enum declaration exists");
    assert!(!after.contains(&declaration), "enum declaration is unique");
    if serialized_enum {
        let attributes = before
            .rsplit_once("#[derive(")
            .expect("enum derives serde")
            .1;
        assert!(attributes.contains("Serialize"), "{name} must serialize");
        assert!(
            attributes.contains("#[serde(rename_all = \"snake_case\")]"),
            "{name}: update this adapter if serde's naming rule changes"
        );
    }
    let body = after.split_once("\n}").expect("unit enum closes").0;
    let mut values = Vec::new();
    let mut rename = None;
    for line in body.lines().map(str::trim) {
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        if let Some(value) = line.strip_prefix("#[serde(rename = \"") {
            assert!(rename.is_none(), "duplicate serde rename");
            rename = Some(
                value
                    .strip_suffix("\")]")
                    .expect("serde rename closes")
                    .to_owned(),
            );
            continue;
        }
        let variant = line
            .strip_suffix(',')
            .expect("unit variant ends with comma");
        assert!(
            variant.starts_with(|c: char| c.is_ascii_uppercase())
                && variant.chars().all(|c| c.is_ascii_alphanumeric()),
            "{name}: unsupported declaration {line:?}"
        );
        let wire = rename.take().unwrap_or_else(|| {
            variant
                .chars()
                .enumerate()
                .fold(String::new(), |mut wire, (index, c)| {
                    if index > 0 && c.is_ascii_uppercase() {
                        wire.push('_');
                    }
                    wire.push(c.to_ascii_lowercase());
                    wire
                })
        });
        values.push((variant.to_owned(), wire));
    }
    assert!(
        rename.is_none() && !values.is_empty(),
        "complete nonempty enum"
    );
    values
}

fn lookup_profiles() -> Vec<String> {
    let source = include_str!("../v2/lookup/parse.rs");
    let variants = source_enum(source, "LookupProfile", false);
    let function = source.split_once("fn parse_lookup_profile(").unwrap().1;
    let body = function.split_once("\n}").unwrap().0;
    // Profile is parsed rather than serialized. Read the actual accepted arms;
    // their first spelling is canonical (`shadow` is an alias for `detail`).
    let mut parsed = BTreeMap::new();
    for line in body.lines().map(str::trim) {
        if let Some((patterns, result)) = line.split_once(" => Ok(LookupProfile::") {
            let variant = result.strip_suffix("),").expect("profile arm closes");
            let canonical = patterns.split(" | ").next().unwrap();
            let wire = canonical
                .strip_prefix('"')
                .unwrap()
                .strip_suffix('"')
                .unwrap();
            assert!(parsed.insert(variant.to_owned(), wire.to_owned()).is_none());
        }
    }
    let declared: BTreeMap<_, _> = variants.into_iter().collect();
    assert_eq!(
        parsed, declared,
        "every profile variant needs its canonical parser arm"
    );
    parsed.into_values().collect()
}

fn permission_powers() -> Vec<String> {
    // permission_values::tests::documented_powers_vocabulary_matches_code
    // independently compares this table to interpreter/role-table producers and
    // the product-name mapping. This comparison completes that chain to OpenAPI.
    let docs = include_str!("../../../../docs/api-v1.md");
    let table = docs
        .split_once("<!-- powers-vocabulary:start -->")
        .unwrap()
        .1;
    let table = table
        .split_once("<!-- powers-vocabulary:end -->")
        .unwrap()
        .0;
    table
        .lines()
        .filter_map(|line| line.strip_prefix("| `"))
        .map(|row| {
            row.split_once('`')
                .expect("power literal closes")
                .0
                .to_owned()
        })
        .collect()
}

fn error_codes_and_http() -> Vec<String> {
    let mut actual: BTreeMap<String, u16> = variants!(ErrorCode:
        InvalidInput, NotFound, Unsupported, Stale, Conflict, InternalError
    )
    .into_iter()
    .map(|code| (code.wire().to_owned(), code.http_status().as_u16()))
    .collect();
    // These three errors are produced by root middleware, outside V2Error. Read
    // only its bound_error calls, not arbitrary strings or its test expectations.
    let bounds = include_str!("../bounds.rs");
    for (index, _) in bounds.match_indices("bound_error(") {
        if bounds[..index]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            continue; // e.g. handle_global_bound_error is a different function.
        }
        let call = bounds[index + "bound_error(".len()..].trim_start();
        if call.starts_with("status: StatusCode") {
            continue; // The function declaration, not a producer call.
        }
        let (status, arguments) = call.split_once(',').expect("bound status argument");
        let status = match status.trim() {
            "StatusCode::REQUEST_TIMEOUT" => StatusCode::REQUEST_TIMEOUT,
            "StatusCode::TOO_MANY_REQUESTS" => StatusCode::TOO_MANY_REQUESTS,
            "StatusCode::SERVICE_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
            other => panic!("new bound-error mapping needs a source adapter: {other}"),
        };
        let code = arguments
            .trim_start()
            .strip_prefix('"')
            .expect("literal bound code")
            .split_once('"')
            .expect("bound code closes")
            .0;
        assert!(actual.insert(code.to_owned(), status.as_u16()).is_none());
    }
    let descriptions = document()["components"]["schemas"]["ErrorCode"]["x-enum-descriptions"]
        .as_object()
        .expect("ErrorCode retains its HTTP column");
    let declared: BTreeMap<String, u16> = descriptions
        .iter()
        .map(|(code, description)| {
            let status = description
                .as_str()
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .parse()
                .expect("error enum description begins with HTTP status");
            (code.clone(), status)
        })
        .collect();
    assert_eq!(
        declared, actual,
        "all error codes and HTTP statuses must match producers"
    );
    for methods in document()["paths"].as_object().unwrap().values() {
        for operation in methods.as_object().unwrap().values() {
            for (status, response) in operation["responses"].as_object().unwrap() {
                if !status.starts_with('4') && !status.starts_with('5') {
                    continue;
                }
                for description in response["description"].as_str().unwrap().split("\n\n") {
                    let code = description
                        .strip_prefix('`')
                        .expect("error description names code")
                        .split_once("`: ")
                        .expect("error code ends")
                        .0;
                    assert_eq!(
                        actual.get(code),
                        Some(&status.parse().unwrap()),
                        "response code {code}"
                    );
                }
            }
        }
    }
    actual.into_keys().collect()
}

fn expiry_reasons() -> Vec<String> {
    let contract = include_str!("../../../../crates/storage/src/expiry.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    let mut reasons = BTreeSet::new();
    for prefix in ["Some(\"", "then_some(\""] {
        for returned in contract.split(prefix).skip(1) {
            reasons.insert(returned.split_once('"').unwrap().0.to_owned());
        }
    }
    for seconds in [0, i128::from(u64::MAX)] {
        let value = bigname_storage::UnixSeconds::from_seconds(seconds).unwrap();
        let served = bigname_storage::contract_expiry_reason(value, "ens_v1_wrapper_l1", None)
            .expect("wrapper sentinel has a reason");
        assert!(
            reasons.contains(served),
            "expiry source adapter must include served reason"
        );
    }
    let lifecycle =
        include_str!("../../../../crates/storage/src/families/control/lifecycle/served.rs");
    let mut released = 0;
    for producer in lifecycle.split("\"expires_at_reason\"").skip(1) {
        let literal = producer
            .trim_start()
            .strip_prefix(".into(), json!(\"")
            .expect("expiry reason producer needs a source adapter if its shape changes");
        reasons.insert(literal.split_once('"').unwrap().0.to_owned());
        released += 1;
    }
    assert!(
        released > 0,
        "released expiry reason producer must be present"
    );
    reasons.into_iter().collect()
}

#[test]
fn every_named_openapi_enum_matches_its_complete_producer_vocabulary() {
    let mut covered = BTreeSet::new();
    macro_rules! serde_enum {
        ($enum:ident: $($variant:ident),+ $(,)?) => {
            compare(&mut covered, stringify!($enum), variants!($enum: $($variant),+).map(serialized));
        };
    }
    macro_rules! string_enum {
        ($enum:ident: $($variant:ident),+ $(,)?) => {
            compare(&mut covered, stringify!($enum), variants!($enum: $($variant),+)
                .map(|value| value.as_str().to_owned()));
        };
    }
    serde_enum!(Status: Ok, NotFound, InvalidName, Mismatch, Unsupported, Stale, Failed);
    serde_enum!(OpsStatus: Ready, Degraded, Stale);
    serde_enum!(Completeness: Full, Partial, Unsupported);
    serde_enum!(Source: Indexed, Verified);
    serde_enum!(Finality: Latest, Safe, Finalized);
    serde_enum!(HistoryScope: Name, Registration, Both);
    serde_enum!(RegistrationStatus: Active, Wrapped, Registered, Released, Unregistered);
    serde_enum!(AuthorityContext: CurrentForName, ResourceAudit);
    serde_enum!(AddressNamesDedupe: Name, Registration);
    serde_enum!(WrapperState: Wrapped, Emancipated, Locked);
    serde_enum!(Authority: EnsV0, EnsV1, EnsV2);
    serde_enum!(Relation: Owner, Manager, Registrant, RoleHolder, ResolvesTo, FormerRegistrant);
    serde_enum!(AddressNamesSort: Name, ExpiresAt, RegisteredAt, CreatedAt);
    serde_enum!(UnlistedPermissionSurface:
        EnsV2RegistryOperators, RegistrarApprovals, ResolverApprovals, WrapperParentControl);
    serde_enum!(GrantRelation: Operator);
    serde_enum!(ResolverReadFeature: Ensip19DefaultAddress, Ensip10ExtendedResolver);
    serde_enum!(HistoryRowSubject: Name, Child);
    string_enum!(NameMatch: Prefix, Contains);
    string_enum!(SortOrder: Asc, Desc);
    string_enum!(NetworkHeadStatus: Fresh, Stale, Unavailable, Pending, Unconfigured);

    let events = variants!(HistoryEventType:
        Registration, Renewal, Release, Expiry, Transfer, Authority, Resolver,
        Record, PrimaryName, Permission, Subregistry, Migration);
    compare(&mut covered, "HistoryEventType", events.map(serialized));
    // ALL drives the production reverse lookup. Its completeness is checked
    // against the same exhaustive list before using storage_event_kinds.
    assert_eq!(
        events.map(serialized).into_iter().collect::<BTreeSet<_>>(),
        HistoryEventType::ALL
            .map(serialized)
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    compare(
        &mut covered,
        "HistoryEventKind",
        events
            .into_iter()
            .flat_map(HistoryEventType::storage_event_kinds)
            .map(|value| (*value).to_owned()),
    );

    for (name, source) in [
        ("LookupKind", include_str!("../v2/lookup/dto.rs")),
        (
            "LapsedReleaseKind",
            include_str!("../v2/name_record/declared.rs"),
        ),
        (
            "LapsedHeldThrough",
            include_str!("../v2/name_record/declared.rs"),
        ),
    ] {
        compare(
            &mut covered,
            name,
            source_enum(source, name, true)
                .into_iter()
                .map(|(_, wire)| wire),
        );
    }
    compare(&mut covered, "LookupProfile", lookup_profiles());
    compare(&mut covered, "PermissionPower", permission_powers());
    compare(&mut covered, "ErrorCode", error_codes_and_http());
    compare(&mut covered, "ExpiryReason", expiry_reasons());

    let declared = document()["components"]["schemas"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, schema)| schema.get("enum").is_some())
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        covered, declared,
        "each named enum needs a producer adapter; remove stale adapters too"
    );
}
