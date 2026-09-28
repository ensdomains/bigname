//! Composed name reads (TYR-36 step 7b): the name row composed at read from the owned key
//! families (`bigname_storage::families::name`) must equal the served `name_current` row in every
//! field a route reads. Every case publishes with the production batch, follows it with the
//! families and runs the name comparison, which fails on any difference; each case also pins the
//! served value it was written for, so a fixture that stops reaching its shape fails here rather
//! than comparing two empty rows.
//!
//! The other shadow suites run the same comparison over their fixtures; the cases here reach
//! the shapes they do not: the resolver block from the resource pointer (F5) and the registry
//! node pointer (F4), a clear, the ownerless node's retained pointer and the ENSv2 root-registry
//! TLD pointer as serving pointers, a root release withdrawing it, `registration.created_at`, and an
//! ENSv2 reservation deferring to the ENSv1 registration.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use serde_json::{Value, json};
use shadow_support::publish_and_compare;
use support::{Fixture, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const ROOT: &str = "0x00000000000000000000000000000000000000e7";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const LATER: &str = "0x00000000000000000000000000000000000000d2";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_REGISTRY: &str = "ens_v1_registry_l1";
const V2_ROOT: &str = "ens_v2_root_l1";
const V2_REGISTRY: &str = "ens_v2_registry_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// Name 1 bound at block 9 to `lease` under arm ens_v1 and granted at 10.
async fn bound(fixture: &Fixture, lease: &str) -> Result<()> {
    fixture
        .binding(&uuid(100), &name(1), lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// A ResolverChanged of name `n`'s node from `family` at `block`, on `resource` when given.
async fn pointer(
    fixture: &Fixture,
    block: i64,
    n: u64,
    family: &str,
    resource: Option<&str>,
    resolver: &str,
) -> Result<()> {
    let emitter = if family == V2_ROOT { ROOT } else { REGISTRY };
    fixture
        .write(
            block,
            2,
            "ResolverChanged",
            family,
            Some(&name(n)),
            resource,
            json!({"node": node(n), "resolver": resolver}),
            emitter,
        )
        .await?;
    Ok(())
}

/// The served row's field at a JSON pointer.
async fn served(fixture: &Fixture, n: u64, path: &str) -> Result<Value> {
    let row: Option<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('declared_summary', declared_summary,
                    'provenance', provenance, 'serving_resource_id', serving_resource_id)
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(name(n))
    .fetch_optional(&fixture.pool)
    .await?;
    Ok(row
        .and_then(|row| row.pointer(path).cloned())
        .unwrap_or(Value::Null))
}

#[tokio::test]
async fn a_bound_name_serves_its_resource_pointer_then_its_clear() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_resource_pointer", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    pointer(&fixture, 11, 1, V1_REGISTRY, Some(&lease), RESOLVER).await?;
    publish_and_compare(&fixture, 12).await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver/address").await?,
        json!(RESOLVER)
    );
    pointer(&fixture, 13, 1, V1_REGISTRY, Some(&lease), ZERO).await?;
    publish_and_compare(&fixture, 14).await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver").await?,
        json!({"chain_id": null, "address": null, "latest_event_kind": "ResolverChanged"})
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_bound_name_serves_its_registry_node_pointer() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_node_pointer", 20).await?;
    bound(&fixture, &uuid(1)).await?;
    pointer(&fixture, 11, 1, V1_REGISTRY, None, RESOLVER).await?;
    publish_and_compare(&fixture, 12).await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver/address").await?,
        json!(RESOLVER)
    );
    assert_eq!(
        served(&fixture, 1, "/provenance/resolver_pointer_source_family").await?,
        json!(V1_REGISTRY)
    );
    pointer(&fixture, 13, 1, V1_REGISTRY, None, LATER).await?;
    publish_and_compare(&fixture, 14).await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver/address").await?,
        json!(LATER)
    );
    fixture.cleanup().await
}

/// An ownerless registry node (a bindingless AuthorityTransferred whose getter reads zero, on a
/// resource without token lineage) serves its retained registry pointer.
#[tokio::test]
async fn an_ownerless_node_serves_its_retained_registry_pointer() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_ownerless_pointer", 20).await?;
    let node_resource = uuid(7);
    fixture
        .write(
            11,
            1,
            "AuthorityTransferred",
            V1_REGISTRY,
            Some(&name(1)),
            Some(&node_resource),
            json!({"node": node(1), "owner": ZERO, "owner_getter": ZERO,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    pointer(&fixture, 12, 1, V1_REGISTRY, Some(&node_resource), RESOLVER).await?;
    publish_and_compare(&fixture, 13).await?;
    assert_eq!(
        served(&fixture, 1, "/provenance/read_reachability/basis").await?,
        json!("retained_registry_resolver_pointer")
    );
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver/address").await?,
        json!(RESOLVER)
    );
    fixture.cleanup().await
}

/// An ENSv2 root-registry TLD with a pointer and no observed registration serves the pointer,
/// then a root release at or after it withdraws it.
#[tokio::test]
async fn a_root_tld_serves_its_root_pointer_until_released() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_root_pointer", 20).await?;
    let token = uuid(9);
    pointer(&fixture, 11, 3, V2_ROOT, Some(&token), RESOLVER).await?;
    publish_and_compare(&fixture, 12).await?;
    assert_eq!(
        served(&fixture, 3, "/provenance/read_reachability/basis").await?,
        json!("root_registry_resolver_pointer")
    );
    assert_eq!(
        served(&fixture, 3, "/declared_summary/resolver/address").await?,
        json!(RESOLVER)
    );
    fixture
        .write(
            13,
            1,
            "RegistrationReleased",
            V2_ROOT,
            None,
            Some(&token),
            json!({"status": "released"}),
            ROOT,
        )
        .await?;
    publish_and_compare(&fixture, 14).await?;
    assert_eq!(
        served(&fixture, 3, "/provenance/read_reachability").await?,
        json!({})
    );
    // A later root pointer on the token serves again.
    pointer(&fixture, 15, 3, V2_ROOT, Some(&token), LATER).await?;
    publish_and_compare(&fixture, 16).await?;
    assert_eq!(
        served(&fixture, 3, "/declared_summary/resolver/address").await?,
        json!(LATER)
    );
    fixture.cleanup().await
}

/// A released ENSv1 lease that lapsed under the registry-only binding a transfer without
/// `reclaim` opened is a released tombstone (name_authority/build.sql:324-360): it keeps the
/// registry-only binding's admitted pointer but serves no resolver, and its lapsed registration
/// names the authority the lease had. Before the release, the registry-only binding serves the
/// pointer.
#[tokio::test]
async fn a_lease_lapsed_under_the_registry_only_binding_withholds_its_resolver() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_v1_tombstone", 20).await?;
    let (lease, registry) = (uuid(1), uuid(2));
    fixture
        .binding(&uuid(100), &name(1), &lease, "ens_v1", 8, 0, Some(10))
        .await?;
    fixture
        .binding(&uuid(101), &name(1), &registry, "ens_v1", 10, 0, None)
        .await?;
    let lease_facts = |extra: Value| {
        let mut after = json!({"authority_kind": "registrar", "namehash": node(1)});
        after
            .as_object_mut()
            .expect("object")
            .extend(extra.as_object().expect("object").clone());
        after
    };
    fixture
        .write(
            8,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            lease_facts(
                json!({"source_event": "NameRegistered", "registrant": OWNER,
                               "expiry": 1_800_000_100u64}),
            ),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            10,
            0,
            "TokenControlTransferred",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            lease_facts(json!({"source_event": "Transfer", "from": OWNER, "to": LATER})),
            REGISTRAR,
        )
        .await?;
    pointer(&fixture, 10, 1, V1_REGISTRY, Some(&registry), RESOLVER).await?;
    for (log, kind, after) in [
        (
            7,
            "AuthorityEpochChanged",
            json!({"authority_kind": "registry_only"}),
        ),
        (
            8,
            "AuthorityTransferred",
            json!({"node": node(1), "owner": OWNER, "owner_getter": OWNER,
                   "authority_kind": "registry_only"}),
        ),
    ] {
        fixture
            .write(
                10,
                log,
                kind,
                V1_REGISTRY,
                Some(&name(1)),
                Some(&registry),
                after,
                REGISTRY,
            )
            .await?;
    }
    publish_and_compare(&fixture, 10).await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver/address").await?,
        json!(RESOLVER)
    );
    fixture
        .write(
            11,
            1,
            "RegistrationReleased",
            V1_REGISTRAR,
            None,
            Some(&lease),
            json!({"source_event": "RegistrationReleased", "released_at": 1_807_776_101u64,
                   "expiry": 1_800_000_100u64, "namehash": node(1)}),
            REGISTRAR,
        )
        .await?;
    publish_and_compare(&fixture, 12).await?;
    assert_eq!(
        served(
            &fixture,
            1,
            "/provenance/authority_selection/resource_authority_context/released_tombstone"
        )
        .await?,
        json!("ens_v1")
    );
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver").await?,
        json!({"chain_id": null, "address": null, "latest_event_kind": "ResolverChanged"})
    );
    assert_eq!(
        served(
            &fixture,
            1,
            "/declared_summary/registration/lapsed_registration/authority_kind"
        )
        .await?,
        json!("registrar")
    );
    fixture.cleanup().await
}

/// A released ENSv2 registration keeps its admitted pointer but serves no resolver.
#[tokio::test]
async fn a_released_ensv2_registration_withholds_its_resolver() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_v2_released", 20).await?;
    let key = uuid(1);
    fixture
        .binding(&uuid(100), &name(1), &key, "ens_v2", 9, 0, None)
        .await?;
    let v2 = |kind: &'static str, block: i64, after: Value| {
        let (fixture, key) = (&fixture, key.clone());
        async move {
            let mut after = after;
            after["registry_contract_instance_id"] = json!("R");
            after["token_id"] = json!("7");
            after["authority_kind"] = json!("registrar");
            fixture
                .write(
                    block,
                    1,
                    kind,
                    V2_REGISTRY,
                    Some(&name(1)),
                    Some(&key),
                    after,
                    REGISTRY,
                )
                .await
        }
    };
    v2("SurfaceBound", 9, json!({"state_derived": false})).await?;
    v2(
        "RegistrationGranted",
        10,
        json!({"status": "registered", "registrant": OWNER, "expiry": 2_000_000_000u64}),
    )
    .await?;
    pointer(&fixture, 11, 1, V2_REGISTRY, Some(&key), RESOLVER).await?;
    publish_and_compare(&fixture, 12).await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver/address").await?,
        json!(RESOLVER)
    );
    v2("RegistrationReleased", 13, json!({"status": "released"})).await?;
    publish_and_compare(&fixture, 14).await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/resolver").await?,
        json!({"chain_id": null, "address": null, "latest_event_kind": "ResolverChanged"})
    );
    fixture.cleanup().await
}

/// `registration.created_at` is the block time of the name's first event, a resolver pointer
/// before the grant included.
#[tokio::test]
async fn created_at_is_the_first_event_time() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_created_at", 20).await?;
    let lease = uuid(1);
    pointer(&fixture, 5, 1, V1_REGISTRY, None, RESOLVER).await?;
    bound(&fixture, &lease).await?;
    publish_and_compare(&fixture, 12).await?;
    let first: Value = sqlx::query_scalar(
        "SELECT to_jsonb(block_timestamp) FROM chain_lineage WHERE block_number = 5",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        served(&fixture, 1, "/declared_summary/registration/created_at").await?,
        first
    );
    fixture.cleanup().await
}

/// A name live on ENSv1 whose ENSv2 label is only RESERVED (no ENSv2 binding, as a premigration
/// reservation is observed): the reservation defers to the ENSv1 registration, which wins the arm
/// (ADR 0007, "follow the chain"), so the composed row serves the ENSv1 lease, its registrant and
/// its resolver.
#[tokio::test]
async fn an_ensv2_reservation_defers_to_the_ensv1_registration() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_v2_reserved", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    pointer(&fixture, 11, 1, V1_REGISTRY, Some(&lease), RESOLVER).await?;
    fixture
        .write(
            12,
            1,
            "RegistrationReserved",
            V2_REGISTRY,
            Some(&name(1)),
            None,
            json!({"status": "reserved"}),
            REGISTRY,
        )
        .await?;
    publish_and_compare(&fixture, 13).await?;
    assert_eq!(
        (
            served(&fixture, 1, "/provenance/authority_selection/authority_arm").await?,
            served(
                &fixture,
                1,
                "/provenance/authority_selection/lifecycle_state"
            )
            .await?,
        ),
        (json!("ens_v1"), json!("registered"))
    );
    assert_eq!(
        (
            served(&fixture, 1, "/declared_summary/registration/status").await?,
            served(&fixture, 1, "/declared_summary/registration/registrant").await?,
            served(&fixture, 1, "/declared_summary/resolver/address").await?,
        ),
        (json!("active"), json!(OWNER), json!(RESOLVER))
    );
    fixture.cleanup().await
}

/// Two names bound to one resource, the resolver set on the first and later on the second: the
/// resource's latest pointer belongs to the second name, and the served row picks each name's
/// latest pointer among its own events, so the first name still serves its own earlier pointer
/// (name_current/build.sql, the `resolver` lateral filters by logical name first).
#[tokio::test]
async fn a_shared_resource_serves_each_name_its_own_latest_pointer() -> Result<()> {
    let fixture = Fixture::new("families_shadow_names_shared_resource", 20).await?;
    let lease = uuid(1);
    for (n, binding) in [(1u64, 100u32), (2, 101)] {
        fixture
            .binding(
                &uuid(binding),
                &name(n),
                &lease,
                "ens_v1",
                9,
                n as i64,
                None,
            )
            .await?;
        fixture
            .write(
                10,
                n as i64,
                "RegistrationGranted",
                V1_REGISTRAR,
                Some(&name(n)),
                Some(&lease),
                json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                       "expiry": 2_000_000_000u64}),
                REGISTRAR,
            )
            .await?;
    }
    pointer(&fixture, 11, 1, V1_REGISTRY, Some(&lease), RESOLVER).await?;
    pointer(&fixture, 12, 2, V1_REGISTRY, Some(&lease), LATER).await?;
    publish_and_compare(&fixture, 13).await?;
    assert_eq!(
        (
            served(&fixture, 1, "/declared_summary/resolver/address").await?,
            served(&fixture, 2, "/declared_summary/resolver/address").await?,
        ),
        (json!(RESOLVER), json!(LATER))
    );
    // Each resolver's bound names reach the name whose own pointer names it, although the
    // resource's latest pointer is the second name's.
    let mut bound = Vec::new();
    for resolver in [RESOLVER, LATER] {
        let rows = bigname_storage::families::name::load_family_bound_names(
            &fixture.pool,
            support::CHAIN,
            resolver,
            None,
            None,
            10,
        )
        .await?;
        bound.push(
            rows.into_iter()
                .map(|row| row.logical_name_id)
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(bound, [vec![name(1)], vec![name(2)]]);
    fixture.cleanup().await
}
