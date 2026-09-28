//! Family reader contracts over explicit family rows: resolver classification and manifest
//! admission bounded by its publication, the link
//! selection's exact-then-default rule, the alias source's latest-then-reject pointer and the
//! wildcard source's historical resolver.
#[path = "families_support/mod.rs"]
mod families_support;

use anyhow::{Context, Result};
use bigname_storage::families::records::{
    DEFAULT_RECORD_NODE, load_family_alias_source_pointer, load_family_link_selection,
    load_family_resolver_classification, load_family_wildcard_source,
};
use families_support::{CHAIN, Fixture, hash, uuid};
use serde_json::{Value, json};
use sqlx::PgPool;

const R1: &str = "0x00000000000000000000000000000000000000b1";
const R2: &str = "0x00000000000000000000000000000000000000b2";
const R3: &str = "0x00000000000000000000000000000000000000b3";
const R4: &str = "0x00000000000000000000000000000000000000b4";
const R5: &str = "0x00000000000000000000000000000000000000b5";
const NODE: &str = "0x1000000000000000000000000000000000000000000000000000000000000001";

fn position(block: i64, identity: &str) -> Value {
    json!({"block_number": block, "transaction_index": 0, "log_index": 0,
           "event_identity": identity})
}

/// A manifest version row in `namespace`; returns its id.
async fn manifest_version(pool: &PgPool, namespace: &str, label: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, $1, 'ens_v2_resolver_l1', $2, $3, 'shadow', 'fixture', $4, '{}')
         RETURNING manifest_id",
    )
    .bind(namespace)
    .bind(CHAIN)
    .bind(label)
    .bind(format!("fixture/{label}.toml"))
    .fetch_one(pool)
    .await?)
}

async fn manifest(
    pool: &PgPool,
    manifest_id: i64,
    block: i64,
    namespace: &str,
    rollout: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state)
         VALUES ($1, $2, 'SourceManifestUpdated', 'ens_v2_resolver_l1', 1, $3, $4, $5, $6,
                 'manifest_sync', 'canonical', $7)",
    )
    .bind(format!("manifest:{manifest_id}:{block}"))
    .bind(namespace)
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(json!({"rollout_status": rollout, "manifest_payload": {"contracts": []}}))
    .execute(pool)
    .await?;
    Ok(())
}

// The resolver's classification and declaration namespace have different sources: the latter
// comes from its manifest at the family marker, not the edge's admission namespace. R2's latest
// manifest there is inactive, so its classification has no active declaration. R3's later
// inactive declaration is beyond the marker and must not replace the active one. An absent F3
// row and a resolver_manifest_not_active row both remain unclassified.
#[tokio::test]
async fn resolver_classification_admission_is_bounded_by_the_family_marker() -> Result<()> {
    let fixture = Fixture::new("family_reads_classification", 10).await?;
    let pool = &fixture.pool;
    sqlx::query(
        "INSERT INTO project_family_marker (chain_id, current_block_number, current_block_hash,
             state)
         VALUES ($1, 6, $2, 'live')",
    )
    .bind(CHAIN)
    .bind(hash(6))
    .execute(pool)
    .await?;
    let (m0, m2, m3) = (
        manifest_version(pool, "ens", "r1-f3").await?,
        manifest_version(pool, "ens", "r2").await?,
        manifest_version(pool, "ens", "r3").await?,
    );
    manifest(pool, m0, 2, "ens", "active").await?;
    for (resolver, identity, status, reason, manifest_id) in [
        (R1, "f3:r1", "supported", None, Some(m0)),
        (R2, "f3:r2", "supported", None, Some(m2)),
        (R3, "f3:r3", "supported", None, Some(m3)),
        (
            R5,
            "f3:r5",
            "unsupported",
            Some("resolver_manifest_not_active"),
            None,
        ),
    ] {
        sqlx::query(
            "INSERT INTO project_resolver_classification (chain_id, resolver_address,
                 block_number, transaction_index, log_index, event_identity, classification,
                 support_status, unsupported_reason, manifest_id, admission_namespace)
             VALUES ($1, $2, 1, 0, 0, $3, $4, $5, $6, $7, 'basenames')",
        )
        .bind(CHAIN)
        .bind(resolver)
        .bind(identity)
        .bind(
            json!({"role": "public_resolver_v2", "source_family": "ens_v2_resolver_l1",
            "basis": "manifest_declared_address"}),
        )
        .bind(status)
        .bind(reason)
        .bind(manifest_id)
        .execute(pool)
        .await?;
    }
    manifest(pool, m2, 3, "ens", "active").await?;
    manifest(pool, m2, 5, "ens", "deprecated").await?;
    manifest(pool, m3, 3, "ens", "active").await?;
    manifest(pool, m3, 9, "ens", "deprecated").await?;

    let r1 = load_family_resolver_classification(pool, CHAIN, R1)
        .await?
        .context("R1 is classified")?;
    assert!(r1.supported());
    assert_eq!(r1.manifest_id, Some(m0));
    assert_eq!(r1.declaration_namespace.as_deref(), Some("ens"));
    let r2 = load_family_resolver_classification(pool, CHAIN, R2)
        .await?
        .context("R2 is classified")?;
    assert!(r2.supported());
    assert_eq!(r2.manifest_id, Some(m2));
    assert_eq!(r2.declaration_namespace, None);
    let r3 = load_family_resolver_classification(pool, CHAIN, R3)
        .await?
        .context("R3 is classified")?;
    assert_eq!(r3.manifest_id, Some(m3));
    assert_eq!(r3.declaration_namespace.as_deref(), Some("ens"));
    // A checksummed or otherwise mixed-case address reads the same classification from the family
    // publication.
    for (resolver, manifest_id) in [(R1, m0), (R2, m2)] {
        let mixed = format!("0x{}", resolver[2..].to_ascii_uppercase());
        let classification = load_family_resolver_classification(pool, CHAIN, &mixed)
            .await?
            .with_context(|| format!("{mixed} is classified"))?;
        assert_eq!(classification.manifest_id, Some(manifest_id), "{mixed}");
    }
    for unclassified in [R4, R5] {
        assert!(
            load_family_resolver_classification(pool, CHAIN, unclassified)
                .await?
                .is_none(),
            "{unclassified}"
        );
    }
    fixture.cleanup().await
}

async fn link(
    pool: &PgPool,
    resolver: &str,
    node: &str,
    record_id: &str,
    storage_model: &str,
    event: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_resolver_link (chain_id, resolver_address, node, block_number,
             transaction_index, log_index, event_identity, normalized_event_id, record_id,
             storage_model)
         VALUES ($1, $2, $3, $4, 0, 0, $5, $4, $6, $7)",
    )
    .bind(CHAIN)
    .bind(resolver)
    .bind(node)
    .bind(event)
    .bind(format!("link:{event}"))
    .bind(record_id)
    .bind(storage_model)
    .execute(pool)
    .await?;
    Ok(())
}

// The link selection serves the exact link at the node, and the default link only when the exact
// one is absent or 0. The records reader still returns the default candidate beside an exact link.
#[tokio::test]
async fn the_link_selection_takes_the_exact_link_then_the_default() -> Result<()> {
    let fixture = Fixture::new("family_reads_links", 2).await?;
    let pool = &fixture.pool;
    link(pool, R1, NODE, "5", "resolver_record_id", 1).await?;
    link(pool, R1, DEFAULT_RECORD_NODE, "6", "resolver_record_id", 2).await?;
    link(pool, R2, NODE, "7", "resolver_record_id", 3).await?;
    let selection = load_family_link_selection(pool, CHAIN, R1, NODE)
        .await?
        .context("the exact link selects")?;
    assert_eq!(
        selection.exact.as_ref().map(|link| link.record_id.as_str()),
        Some("5")
    );
    assert_eq!(selection.active_record_id(), Some("5"));
    assert_eq!(selection.exact_link_event_id, Some(1));
    assert_eq!(selection.default_link_event_id, None);
    // The records reader returns the default candidate eagerly; it does not contribute.
    assert_eq!(
        selection
            .default
            .as_ref()
            .map(|link| link.record_id.as_str()),
        Some("6")
    );
    assert_eq!(selection.contributing_links().count(), 1);
    let exact_only = load_family_link_selection(pool, CHAIN, R2, NODE)
        .await?
        .context("an exact link with no default selects")?;
    assert_eq!(exact_only.active_record_id(), Some("7"));
    assert_eq!(exact_only.exact_link_event_id, Some(3));
    fixture.cleanup().await
}

async fn resource_pointer(
    pool: &PgPool,
    resource: &str,
    current: (&str, i64),
    nonzero: (&str, i64),
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_resource_pointer (chain_id, resource_id, block_number,
             transaction_index, log_index, event_identity, resolver_address, pointer_position,
             namespace, source_family, namehash, nonzero_resolver_address, nonzero_position,
             boundary_kind, boundary_position)
         VALUES ($1, $2::uuid, $3, 0, 0, $4, $5, $6, 'ens', 'ens_v2_registry_l1', $7, $8, $9,
                 'ResolverChanged', $6)",
    )
    .bind(CHAIN)
    .bind(resource)
    .bind(current.1)
    .bind(format!("pointer:{}", current.1))
    .bind(current.0)
    .bind(position(current.1, &format!("pointer:{}", current.1)))
    .bind(NODE)
    .bind(nonzero.0)
    .bind(position(nonzero.1, &format!("pointer:{}", nonzero.1)))
    .execute(pool)
    .await?;
    Ok(())
}

// The alias source is the latest pointer, then a clear rejected: an empty or zero latest pointer
// after a non-zero one gives no alias source and never the older resolver. The wildcard source
// keeps the historical non-zero resolver beside the later clear as its boundary.
#[tokio::test]
async fn alias_rejects_a_later_clear_and_wildcard_keeps_the_historical_resolver() -> Result<()> {
    let fixture = Fixture::new("family_reads_pointer_views", 8).await?;
    let pool = &fixture.pool;
    let (empty, zero, live) = (uuid(1), uuid(2), uuid(3));
    resource_pointer(pool, &empty, ("", 5), (R1, 3)).await?;
    resource_pointer(
        pool,
        &zero,
        ("0x0000000000000000000000000000000000000000", 7),
        (R2, 4),
    )
    .await?;
    resource_pointer(pool, &live, (R3, 6), (R3, 6)).await?;
    for (resource, historical, clear_block) in [(&empty, R1, 5), (&zero, R2, 7)] {
        let id = resource.parse()?;
        assert!(
            load_family_alias_source_pointer(pool, CHAIN, id)
                .await?
                .is_none(),
            "{resource}"
        );
        let wildcard = load_family_wildcard_source(pool, CHAIN, id)
            .await?
            .context("a wildcard source")?;
        assert_eq!(
            wildcard.nonzero_resolver_address.as_deref(),
            Some(historical)
        );
        assert_eq!(wildcard.boundary_kind.as_deref(), Some("ResolverChanged"));
        assert_eq!(
            wildcard
                .boundary_position
                .map(|position| position.block_number),
            Some(clear_block)
        );
    }
    let alias = load_family_alias_source_pointer(pool, CHAIN, live.parse()?)
        .await?
        .context("a live alias source")?;
    assert_eq!(alias.resolver_address, R3);
    fixture.cleanup().await
}
