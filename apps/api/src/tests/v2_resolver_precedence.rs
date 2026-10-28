//! A declared ENSv2 resolver that an ENSv1 name also points at stays supported, with or without
//! an ENSv2 registry edge to it, so the names it serves keep their records. Rows M1, M2 and P1
//! of the resolver classification precedence table, asserted on the name records route.
use super::*;

/// An ENSv1 registry `ResolverChanged` that carries no name and points `other`'s node at
/// `resolver`, with the families rebuilt at the block.
async fn point_an_ensv1_node_at(
    database: &TestDatabase,
    chain: &str,
    (block, hash): (i64, &str),
    other: &str,
    resolver: &str,
) -> Result<()> {
    let mut pointer = history_event(
        "precedence-ensv1-pointer",
        None,
        None,
        Some(chain),
        Some(block),
        Some(hash),
        Some("0xprecedence"),
        Some(90),
        CanonicalityState::Canonical,
    );
    pointer.source_family = "ens_v1_registry_l1".into();
    pointer.event_kind = "ResolverChanged".into();
    pointer.before_state = json!({});
    pointer.after_state =
        json!({"node": bigname_lookup::ens_namehash_hex(other)?, "resolver": resolver});
    pointer.raw_fact_ref = json!({"emitting_address": "0x4444444444444444444444444444444444444401"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[pointer]).await?;
    rebuild_fixture_families(&database.pool, chain, block, hash).await
}

/// M1 and M2: `alice.eth`'s ENSv2 entry points at a declared ENSv1 mirror, which an ENSv1 name
/// also points at. The mirror keeps serving `alice.eth`'s ENSv1 records, whether an ENSv2
/// registry edge reaches it (M1) or only the entry's pointer names it (M2).
#[tokio::test]
async fn a_mirrored_eth_name_keeps_its_records_when_an_ensv1_name_points_at_the_mirror()
-> Result<()> {
    const CHAIN: &str = "ethereum-sepolia";
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    for (row, edge) in [("M1", true), ("M2", false)] {
        let database =
            v2_mirror_records_database("alice.eth", MIRROR, MirrorFixtureSource::Exact, "resolver")
                .await?;
        if !edge {
            sqlx::query("DELETE FROM discovery_edges WHERE chain_id = $1 AND edge_kind = 'resolver'")
                .bind(CHAIN)
                .execute(&database.pool)
                .await?;
        }
        point_an_ensv1_node_at(&database, CHAIN, (21000003, "0xmirror"), "other.eth", MIRROR)
            .await?;
        let payload = v2_name_record_payload_for_database(
            &database,
            "/v1/names/alice.eth/records?keys=addr:60,text:description",
        )
        .await?;
        let records = &payload["data"]["records"];
        assert_eq!(payload["data"]["resolver"]["address"], json!(MIRROR), "{row}: {payload:#}");
        assert_eq!(records["addr:60"]["status"], json!("ok"), "{row}: {payload:#}");
        assert_eq!(
            records["addr:60"]["value"],
            json!("0x0000000000000000000000000000000000000def"),
            "{row}: {payload:#}"
        );
        assert_eq!(
            records["text:description"],
            json!({"status": "ok", "value": "Alice profile"}),
            "{row}: {payload:#}"
        );
        database.cleanup().await?;
    }
    Ok(())
}

/// P1: an ENSv2 name whose registry points at the declared PublicResolverV2, which an ENSv1 name
/// also points at, keeps that resolver's records, with an ENSv2 registry edge to the resolver
/// and without one.
#[tokio::test]
async fn a_public_resolver_v2_name_keeps_its_records_when_an_ensv1_name_points_at_it()
-> Result<()> {
    let name = "precedence-v2.eth";
    for (row, edge) in [("P1 with an edge", true), ("P1 without an edge", false)] {
        let database = TestDatabase::new_migrated().await?;
        database.seed_default_ens_snapshot_selector_position().await?;
        seed_abi_public_resolver_v2_name(&database, name, &[]).await?;
        let (hash, number) = abi_head(&database).await?;
        if edge {
            let (registry, resolver) = (Uuid::from_u128(0x5ab9a0), Uuid::from_u128(0x5ab9a1));
            seed_schema_v2_ens_manifest(
                &database.pool,
                "ens_v2_registry_l1",
                "registry",
                "0x00000000000000000000000000000000005ab9a0",
                registry,
                false,
            )
            .await?;
            let origin: i64 = sqlx::query_scalar(
                "SELECT manifest_id FROM manifest_versions
                 WHERE source_family = 'ens_v2_registry_l1'",
            )
            .fetch_one(&database.pool)
            .await?;
            sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1, $2, 'contract')")
                .bind(resolver).bind(ABI_CHAIN).execute(&database.pool).await?;
            sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, source_manifest_id, active_from_block_number, active_from_block_hash) VALUES ($1, $2, $3, $4, $5, $6)")
                .bind(resolver).bind(ABI_CHAIN).bind(PUBLIC_RESOLVER_V2).bind(origin).bind(number).bind(&hash)
                .execute(&database.pool).await?;
            sqlx::query("INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source, admission_basis, source_manifest_id, active_from_block_number, active_from_block_hash, canonicality_state) VALUES ($1, 'resolver', $2, $3, 'ResolverChanged', 'registry_pointer', $4, $5, $6, 'canonical')")
                .bind(ABI_CHAIN).bind(registry).bind(resolver).bind(origin).bind(number).bind(&hash)
                .execute(&database.pool).await?;
        }
        point_an_ensv1_node_at(
            &database,
            ABI_CHAIN,
            (number, &hash),
            "other.eth",
            PUBLIC_RESOLVER_V2,
        )
        .await?;
        let payload = v2_name_record_payload_for_database(
            &database,
            &format!("/v1/names/{name}/records?keys=text:url"),
        )
        .await?;
        assert_eq!(
            payload["data"]["resolver"]["address"],
            json!(PUBLIC_RESOLVER_V2),
            "{row}: {payload:#}"
        );
        assert_eq!(
            payload["data"]["records"]["text:url"],
            json!({"status": "ok", "value": "https://example.test"}),
            "{row}: {payload:#}"
        );
        database.cleanup().await?;
    }
    Ok(())
}
