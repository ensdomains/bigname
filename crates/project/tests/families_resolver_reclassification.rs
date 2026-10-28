//! A resolver's classification changes block by block, one kind of change per block, and the
//! families followed incrementally equal a rebuild at every block. Each consumer of the change
//! (resolution paths, lookup inventories) recomposes only when a field it reads changed, so a
//! consumer that skipped a change it reads would leave a row the rebuild does not write.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use families_support::{
    CHAIN, Fixture,
    ens_v2_path::{self, Pointer},
    hash, uuid,
};
use serde_json::{Value, json};

/// The ENSv1 resolver whose classification the blocks change.
const RESOLVER: &str = "0x00000000000000000000000000000000000000c7";
/// The ENSv2 mirror resolver `foo`'s entry points at, declared with the ENSv1 mirror role.
const MIRROR: &str = "0x00000000000000000000000000000000000000d7";
/// Another ENSv1 resolver, the only one the undeclaring manifests declare.
const OTHER: &str = "0x00000000000000000000000000000000000000c8";
const V1_REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const V1_REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";

/// A manifest of `family` and its `SourceManifestUpdated` at `block` with `status` and
/// `payload`. A new manifest when `manifest_id` is none, else an update of it. Returns its id.
/// The classification reads the status from the update. The catalog row stays a draft, since
/// the catalog holds one active manifest per family.
async fn manifest(
    fixture: &Fixture,
    manifest_id: Option<i64>,
    family: &str,
    block: i64,
    payload: Value,
    status: &str,
) -> Result<i64> {
    let id = match manifest_id {
        Some(id) => id,
        None => {
            sqlx::query_scalar(
                "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload)
                 VALUES (1, 'ens', $1, $2, $3, 'draft', 'fixture', $3, $4)
                 RETURNING manifest_id",
            )
            .bind(family)
            .bind(CHAIN)
            .bind(format!("fixture/{family}-{block}.yaml"))
            .bind(&payload)
            .fetch_one(&fixture.pool)
            .await?
        }
    };
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, $5, $6, 'manifest_sync',
                 'canonical', '{}'::jsonb,
                 jsonb_build_object('rollout_status', $8::text, 'manifest_payload', $7::jsonb),
                 '{}'::jsonb)",
    )
    .bind(format!("manifest:{id}:{block}"))
    .bind(family)
    .bind(id)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(&payload)
    .bind(status)
    .execute(&fixture.pool)
    .await?;
    Ok(id)
}

/// An ENSv1 resolver manifest payload declaring `address` with `read_features`.
fn v1_resolvers(address: &str, read_features: &[&str]) -> Value {
    json!({"contracts": [{"address": address, "role": "public_resolver",
                          "read_features": read_features}]})
}

/// `RESOLVER`'s classification row: its support status, reason and manifest.
async fn classification(fixture: &Fixture) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('support_status', support_status,
             'unsupported_reason', unsupported_reason, 'manifest_id', manifest_id,
             'read_features', classification -> 'read_features')
         FROM project_resolver_classification WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(&fixture.pool)
    .await?)
}

/// The descendant's stored resolution reason, read through the ENSv2 path and the mirror.
async fn path_reason(fixture: &Fixture, descendant: &str) -> Result<Value> {
    let core: Option<Value> = sqlx::query_scalar(
        "SELECT core FROM project_lookup_name WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(descendant)
    .fetch_one(&fixture.pool)
    .await?;
    Ok(core.unwrap_or_default()["declared_summary"]["resolution_unsupported_reason"].clone())
}

/// Whether a stored lookup inventory names `reason`.
async fn inventory_names(fixture: &Fixture, reason: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM project_lookup_inventory inventory
             WHERE inventory.chain_id = $1 AND to_jsonb(inventory)::text LIKE '%' || $2 || '%')",
    )
    .bind(CHAIN)
    .bind(reason)
    .fetch_one(&fixture.pool)
    .await?)
}

/// The path world, published through block 3:
///
/// - 0: the root registry is admitted with its `eth` entry. An ENSv2 resolver manifest
///   declares `MIRROR`. No ENSv1 resolver manifest is active, so `RESOLVER` has no admission.
/// - 1: `foo` is registered in the `.eth` registry, and its entry's resolver is `MIRROR`.
/// - 2: `x.a.foo.eth` has an ENSv1 registry owner and no resolver of its own, and the node of
///   `a.foo.eth` has `RESOLVER`. So the descendant walks the ENSv2 path to `MIRROR`, and the
///   mirror walks the ENSv1 registry up to `RESOLVER`.
/// - 3: a block that names nothing.
///
/// Returns the descendant.
async fn path_world(fixture: &Fixture) -> Result<String> {
    let eth = ens_v2_path::admit(fixture).await?;
    manifest(
        fixture,
        None,
        "ens_v2_resolver_l1",
        0,
        json!({"correlation_addresses": {"ens_v1_registry": V1_REGISTRY},
               "contracts": [{"role": "ensv1_mirror_resolver", "address": MIRROR,
                              "proxy_kind": "none", "read_features": ["addr", "text"]}]}),
        "active",
    )
    .await?;
    ens_v2_path::register(fixture, 1, 1, &eth, "foo", 1_900_000_000).await?;
    ens_v2_path::point(fixture, 1, 3, &eth, "foo", Pointer::Resolver(MIRROR)).await?;

    ens_v2_path::surface(fixture, "a.foo.eth").await?;
    let descendant = ens_v2_path::surface(fixture, "x.a.foo.eth").await?;
    let resource = uuid(0x3001);
    fixture
        .binding(&uuid(301), &descendant, &resource, "ens_v1", 2, 1, None)
        .await?;
    fixture
        .write(
            2,
            1,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            Some(&descendant),
            Some(&resource),
            json!({"authority_kind": "registry", "owner": ens_v2_path::OWNER,
                   "registry_owner": ens_v2_path::OWNER,
                   "node": descendant.trim_start_matches("ens:")}),
            V1_REGISTRY,
        )
        .await?;
    let parent = ens_v2_path::logical_name_id("a.foo.eth");
    let node = parent.trim_start_matches("ens:");
    fixture
        .write(
            2,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"node": node, "resolver": RESOLVER}),
            V1_REGISTRY,
        )
        .await?;
    fixture
        .write(
            2,
            3,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"authority_kind": "registry", "owner": ens_v2_path::OWNER,
                   "registry_owner": ens_v2_path::OWNER, "node": node}),
            V1_REGISTRY,
        )
        .await?;
    ens_v2_path::filler(fixture, 3).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    Ok(descendant)
}

/// The inventory world, published through block 3, with no ENSv2 root registry: a name with an
/// ENSv1 registration, a registry pointer to `RESOLVER` and a text record at block 2, so its
/// lookup inventory reads `RESOLVER`'s classification.
async fn inventory_world(fixture: &Fixture) -> Result<()> {
    let named = format!("ens:0x{:064x}", 1);
    let served = uuid(1);
    fixture
        .binding(&uuid(100), &named, &served, "ens_v1", 2, 0, None)
        .await?;
    fixture
        .write(
            2,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&named),
            Some(&served),
            json!({"authority_kind": "registrar", "status": "registered",
                   "registrant": ens_v2_path::OWNER, "expiry": 2_000_000_000u64}),
            V1_REGISTRAR,
        )
        .await?;
    fixture
        .write(
            2,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&named),
            Some(&served),
            json!({"node": format!("0x{:064x}", 1), "resolver": RESOLVER}),
            V1_REGISTRY,
        )
        .await?;
    fixture
        .write(
            2,
            3,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(&named),
            Some(&served),
            json!({"node": format!("0x{:064x}", 1), "record_key": "text:arbitrary",
                   "record_family": "text", "selector_key": "arbitrary", "value": "one",
                   "source_event": "TextChanged"}),
            RESOLVER,
        )
        .await?;
    ens_v2_path::filler(fixture, 3).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    Ok(())
}

/// What the world shows of `RESOLVER` after a block: the descendant's path reason in the path
/// world, whether an inventory names `resolver_not_declared` in the inventory world.
async fn reads(fixture: &Fixture, descendant: Option<&str>) -> Result<Value> {
    Ok(match descendant {
        Some(descendant) => path_reason(fixture, descendant).await?,
        None => json!(inventory_names(fixture, "resolver_not_declared").await?),
    })
}

/// Apply `block`, require `RESOLVER`'s row to match `expected` and the world to read `seen`
/// (none: not checked), then require a rebuild at `block` to equal the families.
async fn step(
    fixture: &Fixture,
    descendant: Option<&str>,
    block: i64,
    expected: Value,
    seen: Option<Value>,
) -> Result<()> {
    fixture.apply(block, FamilyMode::Normal).await?;
    let row = classification(fixture).await?;
    for (field, value) in expected.as_object().expect("an object") {
        ensure!(
            &row[field] == value,
            "at {block} the resolver's row is {row}"
        );
    }
    if let Some(seen) = seen {
        let read = reads(fixture, descendant).await?;
        ensure!(
            read == seen,
            "at {block} the world reads {read}, not {seen}"
        );
    }
    fixture.assert_rebuild_equal(block).await
}

// One block per kind of classification change of `RESOLVER`, the rows of TYR-398's
// input-class table that a family run can reach, in a world where an ENSv2 path reads it
// through the mirror and in one where a lookup inventory reads it:
//
// - 4: an ENSv1 resolver manifest activates without declaring it. Its row is re-filed from no
//   admission to unsupported with a reason and a manifest. Paths read neither, the inventory
//   now names the reason.
// - 5: that manifest is withdrawn and another, also without it, activates. Only the row's
//   manifest changes, and no consumer reads it while the row is unsupported.
// - 6: the second manifest declares it. Support starts, and the descendant's path reads it.
// - 7: the second manifest is withdrawn and a third declares it the same way. Only the
//   manifest changes, while the row is supported.
// - 8: the third manifest changes its read features. The classification changes while
//   supported.
// - 9: the third manifest stops declaring it. Support ends.
//
// After every block the families equal a rebuild at that block.
#[tokio::test]
async fn each_kind_of_classification_change_rebuilds_equal() -> Result<()> {
    let family = "ens_v1_resolver_l1";
    let unprojected = json!("ens_v2_path_not_projected");
    for path in [true, false] {
        let fixture = Fixture::new("families_resolver_reclassification", 12).await?;
        let descendant = if path {
            Some(path_world(&fixture).await?)
        } else {
            inventory_world(&fixture).await?;
            None
        };
        let descendant = descendant.as_deref();
        let seen = |on_path: Value, in_inventory: bool| {
            Some(if path { on_path } else { json!(in_inventory) })
        };
        let row = classification(&fixture).await?;
        ensure!(
            row == json!({"support_status": "unsupported",
                          "unsupported_reason": "resolver_manifest_not_active",
                          "manifest_id": null, "read_features": []}),
            "at 3 the resolver has no admission: {row}"
        );
        ensure!(reads(&fixture, descendant).await? == seen(unprojected.clone(), false).unwrap());
        fixture.assert_rebuild_equal(3).await?;

        let first = manifest(
            &fixture,
            None,
            family,
            4,
            v1_resolvers(OTHER, &[]),
            "active",
        )
        .await?;
        step(
            &fixture,
            descendant,
            4,
            json!({"support_status": "unsupported", "unsupported_reason": "resolver_not_declared",
                   "manifest_id": first}),
            seen(unprojected.clone(), true),
        )
        .await?;

        manifest(
            &fixture,
            Some(first),
            family,
            5,
            v1_resolvers(OTHER, &[]),
            "deprecated",
        )
        .await?;
        let second = manifest(
            &fixture,
            None,
            family,
            5,
            v1_resolvers(OTHER, &[]),
            "active",
        )
        .await?;
        step(
            &fixture,
            descendant,
            5,
            json!({"support_status": "unsupported", "unsupported_reason": "resolver_not_declared",
                   "manifest_id": second, "read_features": []}),
            seen(unprojected.clone(), true),
        )
        .await?;

        let declared = v1_resolvers(RESOLVER, &["addr", "text"]);
        manifest(
            &fixture,
            Some(second),
            family,
            6,
            declared.clone(),
            "active",
        )
        .await?;
        step(
            &fixture,
            descendant,
            6,
            json!({"support_status": "supported", "unsupported_reason": null,
                   "manifest_id": second, "read_features": ["addr", "text"]}),
            None,
        )
        .await?;
        if let Some(descendant) = descendant {
            let reason = path_reason(&fixture, descendant).await?;
            ensure!(
                reason != unprojected,
                "at 6 the descendant's path reads the supported resolver: {reason}"
            );
        }

        manifest(
            &fixture,
            Some(second),
            family,
            7,
            declared.clone(),
            "deprecated",
        )
        .await?;
        let third = manifest(&fixture, None, family, 7, declared, "active").await?;
        step(
            &fixture,
            descendant,
            7,
            json!({"support_status": "supported", "manifest_id": third,
                   "read_features": ["addr", "text"]}),
            None,
        )
        .await?;

        manifest(
            &fixture,
            Some(third),
            family,
            8,
            v1_resolvers(RESOLVER, &["addr"]),
            "active",
        )
        .await?;
        step(
            &fixture,
            descendant,
            8,
            json!({"support_status": "supported", "read_features": ["addr"]}),
            None,
        )
        .await?;

        manifest(
            &fixture,
            Some(third),
            family,
            9,
            v1_resolvers(OTHER, &[]),
            "active",
        )
        .await?;
        step(
            &fixture,
            descendant,
            9,
            json!({"support_status": "unsupported", "unsupported_reason": "resolver_not_declared"}),
            seen(unprojected.clone(), true),
        )
        .await?;
        fixture.cleanup().await?;
    }
    Ok(())
}
