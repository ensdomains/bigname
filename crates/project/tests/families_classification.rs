//! F3 through the family loop: a resolver's classification pinned to the block that classified
//! it, from the candidates its events and pointers proposed, the manifests active at the block
//! and the discovery edges and declarations that start there. Each case undoes its last block
//! byte for byte and equals a rebuild, which visits the activation blocks as work blocks.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{CHAIN, Fixture, hash};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";
const R3: &str = "0x00000000000000000000000000000000000000a3";
const PROXY: &str = "0x00000000000000000000000000000000000000a4";
const IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000c1";
const OTHER_IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000c2";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn columns(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

/// A manifest of `family` and its SourceManifestUpdated at `block` carrying `payload`; the
/// same `manifest_id` again publishes an update. Returns the manifest id.
async fn manifest(
    fixture: &Fixture,
    manifest_id: Option<i64>,
    family: &str,
    block: i64,
    payload: Value,
) -> Result<i64> {
    let id = match manifest_id {
        Some(id) => id,
        None => {
            sqlx::query_scalar(
                "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload)
                 VALUES (1, 'ens', $1, $2, 'fixture', 'active', 'fixture', $3, $4)
                 RETURNING manifest_id",
            )
            .bind(family)
            .bind(CHAIN)
            .bind(format!("fixture/{family}.yaml"))
            .bind(&payload)
            .fetch_one(&fixture.pool)
            .await?
        }
    };
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, $5, $6,
                 'ens_v2_registry_resource_surface', 'canonical', '{}'::jsonb,
                 jsonb_build_object('rollout_status', 'active', 'manifest_payload', $7::jsonb),
                 '{}'::jsonb)",
    )
    .bind(format!("manifest:{id}:{block}"))
    .bind(family)
    .bind(id)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(&payload)
    .execute(&fixture.pool)
    .await?;
    Ok(id)
}

async fn classifications(fixture: &Fixture) -> Result<Vec<Value>> {
    Ok(fixture
        .rows("project_resolver_classification")
        .await?
        .iter()
        .map(|row| {
            let mut picked = columns(
                row,
                &[
                    "resolver_address",
                    "support_status",
                    "unsupported_reason",
                    "block_number",
                    "event_identity",
                ],
            );
            picked["source_family"] = row["classification"]["source_family"].clone();
            picked["role"] = row["classification"]["role"].clone();
            picked
        })
        .collect())
}

async fn pointer(fixture: &Fixture, block: i64, log: i64, n: u64, resolver: &str) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"node": node(n), "resolver": resolver}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

// A declared resolver is supported; an undeclared one of the same family is not; a family with
// no active manifest keeps a row with its reason, where the served build writes none. A
// declaration that starts at a later block reclassifies the resolver there with no event.
#[tokio::test]
async fn resolvers_are_classified_at_their_block_and_again_when_a_declaration_starts() -> Result<()>
{
    let fixture = Fixture::new("families_classification_declared", 20).await?;
    manifest(
        &fixture,
        None,
        "ens_v1_resolver_l1",
        1,
        json!({"contracts": [
            {"address": R1, "role": "public_resolver", "read_features": ["addr"]},
            {"address": R2, "role": "public_resolver", "start_block": 15}
        ]}),
    )
    .await?;
    pointer(&fixture, 10, 1, 1, R1).await?;
    pointer(&fixture, 10, 2, 2, R2).await?;
    fixture
        .write(
            10,
            3,
            "ResolverChanged",
            "basenames_base_registry",
            None,
            None,
            json!({"node": node(3), "resolver": R3}),
            REGISTRY,
        )
        .await?;
    fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(
        classifications(&fixture).await?,
        vec![
            json!({"resolver_address": R1, "support_status": "supported",
                   "unsupported_reason": null, "block_number": 10,
                   "event_identity": "ResolverChanged:10:1",
                   "source_family": "ens_v1_resolver_l1", "role": "public_resolver"}),
            json!({"resolver_address": R2, "support_status": "unsupported",
                   "unsupported_reason": "resolver_not_declared", "block_number": 10,
                   "event_identity": "ResolverChanged:10:2",
                   "source_family": "ens_v1_resolver_l1", "role": null}),
            json!({"resolver_address": R3, "support_status": "unsupported",
                   "unsupported_reason": "resolver_manifest_not_active", "block_number": 10,
                   "event_identity": "ResolverChanged:10:3",
                   "source_family": "basenames_base_resolver", "role": null}),
        ]
    );
    let rows = fixture.rows("project_resolver_classification").await?;
    assert_eq!(
        columns(&rows[0], &["observed_families", "pointer_families"]),
        json!({"observed_families": {"ens_v1_resolver_l1": 3},
               "pointer_families": {"ens_v1_resolver_l1": 1}})
    );
    assert_eq!(rows[0]["classification"]["read_features"], json!(["addr"]));

    fixture.apply(15, FamilyMode::Normal).await;
    let r2 = classifications(&fixture)
        .await?
        .into_iter()
        .find(|row| row["resolver_address"] == json!(R2))
        .expect("R2 keeps its row");
    assert_eq!(
        r2,
        json!({"resolver_address": R2, "support_status": "supported",
               "unsupported_reason": null, "block_number": 15,
               "event_identity": "activation:15",
               "source_family": "ens_v1_resolver_l1", "role": "public_resolver"}),
        "the declaration's start block reclassifies R2 with no event of its own"
    );
    fixture.assert_undo_restores(15).await?;
    fixture.assert_rebuild_equal(15).await?;
    fixture.cleanup().await
}

// A manifest update changes the admission epoch: every stored resolver is classified again under
// it, without moving the row's position.
#[tokio::test]
async fn a_manifest_update_reclassifies_every_resolver_in_place() -> Result<()> {
    let fixture = Fixture::new("families_classification_epoch", 20).await?;
    let id = manifest(
        &fixture,
        None,
        "ens_v1_resolver_l1",
        1,
        json!({"contracts": [{"address": R1, "role": "public_resolver"}]}),
    )
    .await?;
    pointer(&fixture, 10, 1, 2, R2).await?;
    fixture.apply(11, FamilyMode::Normal).await;
    assert_eq!(
        classifications(&fixture).await?[0]["unsupported_reason"],
        json!("resolver_not_declared")
    );
    let before =
        fixture.rows("project_resolver_classification").await?[0]["admission_epoch"].clone();
    manifest(
        &fixture,
        Some(id),
        "ens_v1_resolver_l1",
        12,
        json!({"contracts": [{"address": R1, "role": "public_resolver"},
                             {"address": R2, "role": "public_resolver"}]}),
    )
    .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_resolver_classification").await?;
    assert_eq!(
        columns(
            &rows[0],
            &["support_status", "block_number", "event_identity"]
        ),
        json!({"support_status": "supported", "block_number": 10,
               "event_identity": "ResolverChanged:10:1"}),
        "reclassified under the new epoch, still at the event that named it"
    );
    assert_ne!(rows[0]["admission_epoch"], before);
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// An ENSv2 proxy is classified by its latest Upgraded implementation: a declared one is
// supported with its read features, an undeclared one is not.
#[tokio::test]
async fn an_upgraded_proxy_follows_its_latest_implementation() -> Result<()> {
    let fixture = Fixture::new("families_classification_upgrade", 20).await?;
    manifest(
        &fixture,
        None,
        "ens_v2_resolver_l1",
        1,
        json!({"contracts": [], "resolver_implementations": [
            {"address": IMPLEMENTATION, "role": "permissioned_resolver",
             "read_features": ["addr", "text"]}
        ]}),
    )
    .await?;
    let upgraded =
        |implementation: &str| json!({"proxy_address": PROXY, "implementation": implementation});
    fixture
        .write(
            10,
            1,
            "Upgraded",
            "ens_v2_resolver_l1",
            None,
            None,
            upgraded(IMPLEMENTATION),
            PROXY,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    let rows = fixture.rows("project_resolver_classification").await?;
    assert_eq!(
        columns(
            &rows[0]["classification"],
            &["basis", "role", "implementation", "read_features"]
        ),
        json!({"basis": "erc1967_upgraded_history", "role": "permissioned_resolver",
               "implementation": IMPLEMENTATION, "read_features": ["addr", "text"]})
    );
    assert_eq!(rows[0]["support_status"], json!("supported"));
    fixture
        .write(
            11,
            1,
            "Upgraded",
            "ens_v2_resolver_l1",
            None,
            None,
            upgraded(OTHER_IMPLEMENTATION),
            PROXY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let rows = fixture.rows("project_resolver_classification").await?;
    assert_eq!(
        columns(&rows[0], &["support_status", "unsupported_reason"]),
        json!({"support_status": "unsupported",
               "unsupported_reason": "resolver_implementation_not_declared"})
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// A resolver edge that starts at a block with no event of the resolver creates its row there,
// classified by the same-namespace declaration it admits (declaration precedence).
#[tokio::test]
async fn a_resolver_edge_that_starts_creates_the_row_at_its_block() -> Result<()> {
    let fixture = Fixture::new("families_classification_edge", 20).await?;
    manifest(
        &fixture,
        None,
        "ens_v1_resolver_l1",
        1,
        json!({"contracts": [{"address": R3, "role": "public_resolver"}]}),
    )
    .await?;
    let registry_manifest = manifest(
        &fixture,
        None,
        "ens_v1_registry_l1",
        1,
        json!({"contracts": []}),
    )
    .await?;
    let (from, to) = (
        "00000000-0000-0000-0000-00000000f001",
        "00000000-0000-0000-0000-00000000f002",
    );
    for instance in [from, to] {
        sqlx::query(
            "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
             VALUES ($1::uuid, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(&fixture.pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address)
         VALUES ($1::uuid, $2, $3)",
    )
    .bind(to)
    .bind(CHAIN)
    .bind(R3)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, canonicality_state)
         VALUES ($1, 'resolver', $2::uuid, $3::uuid, 'NewResolver', 'fixture', $4, 13, $5,
                 'canonical')",
    )
    .bind(CHAIN)
    .bind(from)
    .bind(to)
    .bind(registry_manifest)
    .bind(hash(13))
    .execute(&fixture.pool)
    .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    assert!(classifications(&fixture).await?.is_empty());
    fixture.apply(13, FamilyMode::Normal).await;
    assert_eq!(
        classifications(&fixture).await?,
        vec![
            json!({"resolver_address": R3, "support_status": "supported",
                    "unsupported_reason": null, "block_number": 13,
                    "event_identity": "activation:13",
                    "source_family": "ens_v1_resolver_l1", "role": "public_resolver"})
        ]
    );
    assert_eq!(
        fixture.rows("project_resolver_classification").await?[0]["admission_namespace"],
        json!("ens")
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

// A declaration that starts below the retained lineage has no block to visit: a rebuild skips it
// and classifies under it at the first readable block.
#[tokio::test]
async fn a_rebuild_skips_activations_below_the_retained_lineage() -> Result<()> {
    let fixture = Fixture::new("families_classification_retained", 20).await?;
    sqlx::query("DELETE FROM chain_lineage WHERE chain_id = $1 AND block_number < 5")
        .bind(CHAIN)
        .execute(&fixture.pool)
        .await?;
    manifest(
        &fixture,
        None,
        "ens_v1_resolver_l1",
        5,
        json!({"contracts": [
            {"address": R1, "role": "public_resolver", "start_block": 2}
        ]}),
    )
    .await?;
    pointer(&fixture, 10, 1, 1, R1).await?;
    let rebuilt = fixture.apply(14, FamilyMode::Rebuild).await;
    assert_eq!(rebuilt.skipped, None);
    assert_eq!(
        classifications(&fixture).await?,
        vec![
            json!({"resolver_address": R1, "support_status": "supported",
                    "unsupported_reason": null, "block_number": 10,
                    "event_identity": "ResolverChanged:10:1",
                    "source_family": "ens_v1_resolver_l1", "role": "public_resolver"})
        ]
    );
    fixture.cleanup().await
}
