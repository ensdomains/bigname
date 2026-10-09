/// Direct lookup writes real disagreements while the chain's profile admits no ENSv2 root
/// registry. Admitting one makes an unreserved `.eth` name unresolvable, and the rebuild that
/// adopts the manifest change retires this chain's current evidence atomically. No Universal
/// Resolver proxy event is involved.
#[tokio::test]
async fn family_lookup_admission_retires_only_current_evidence_on_the_publishing_chain()
-> Result<()> {
    const PROXY: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
    for chain in ["ethereum-mainnet", "ethereum-sepolia"] {
        let database = TestDatabase::new_migrated().await?;
        seed_schema_v2_lookup_head(
            &database.pool,
            chain,
            200,
            "0xledger200",
            "2026-04-17T00:00:00Z",
        )
        .await?;
        seed_schema_v2_ens_manifest_on_chain(
            &database.pool,
            chain,
            "ens_execution",
            "universal_resolver",
            PROXY,
            Uuid::from_u128(0x988001),
            true,
        )
        .await?;
        let (manifest, mut payload): (i64, Value) = sqlx::query_as(
            "SELECT manifest_id, manifest_payload FROM manifest_versions WHERE source_family = 'ens_execution' AND chain_id = $1",
        ).bind(chain).fetch_one(&database.pool).await?;
        payload["capability_flags"]["verified_resolution"]["status"] = json!("shadow");
        sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
            .bind(manifest)
            .bind(&payload)
            .execute(&database.pool)
            .await?;
        seed_fixture_manifest_update(
            &database.pool,
            manifest,
            chain,
            "ens",
            "ens_execution",
            &payload,
        )
        .await?;
        let node = seed_record_lookup_inputs(
            &database.pool,
            chain,
            "ens",
            "ledger.eth",
            Uuid::from_u128(0x988010),
            Uuid::from_u128(0x988011),
            200,
            "0xledger200",
            "2026-04-17T00:00:00Z",
            FAMILY_ALICE,
        )
        .await?;
        let id = format!("ens:{node}");
        assert_eq!(
            cutover_lookup(&database, chain, &id, FAMILY_BOB).await?,
            bigname_lookup::LedgerAction::Written
        );
        assert_eq!(
            cutover_lookup(&database, chain, &id, FAMILY_ALICE).await?,
            bigname_lookup::LedgerAction::Cleared
        );
        seed_schema_v2_lookup_head(
            &database.pool,
            chain,
            201,
            "0xledger201",
            "2026-04-17T00:00:12Z",
        )
        .await?;
        publish_test_families_on(&database.pool, chain, 201).await?;
        assert_eq!(
            cutover_lookup(&database, chain, &id, FAMILY_BOB).await?,
            bigname_lookup::LedgerAction::Written
        );

        // A retained observation for the same name on the other ENS L1 must stay active.
        // This isolation control is seeded; both current and retired target-chain rows above
        // were produced by the guarded lookup writer at their actual canonical positions.
        let other = if chain == "ethereum-mainnet" {
            "ethereum-sepolia"
        } else {
            "ethereum-mainnet"
        };
        seed_schema_v2_lookup_head(
            &database.pool,
            other,
            201,
            "0xledger201",
            "2026-04-17T00:00:12Z",
        )
        .await?;
        sqlx::query("INSERT INTO resolution_divergences (logical_name_id, resolver_chain_id,
            resolver_address, request_kind, observed_positions, indexed_result, live_result,
            first_observed_at, last_observed_at, cleared_at)
            SELECT logical_name_id, $1, resolver_address, request_kind,
                jsonb_build_object(CASE WHEN $1 = 'ethereum-mainnet' THEN 'ethereum' ELSE 'ethereum-sepolia' END,
                    jsonb_build_object('chain_id', $1::text, 'block_number', 201,
                        'block_hash', '0xledger201', 'timestamp', '2026-04-17T00:00:12Z')),
                indexed_result, live_result, first_observed_at, last_observed_at, NULL
            FROM resolution_divergences WHERE resolver_chain_id = $2 AND cleared_at IS NULL")
            .bind(other).bind(chain).execute(&database.pool).await?;
        let before: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(d) FROM resolution_divergences d ORDER BY resolver_chain_id, observed_positions::text")
            .fetch_all(&database.pool).await?;
        admit_ens_v2_root_registry(&database, chain).await?;
        seed_schema_v2_lookup_head(
            &database.pool,
            chain,
            202,
            "0xledger202",
            "2026-04-17T00:00:24Z",
        )
        .await?;
        sqlx::raw_sql("CREATE FUNCTION refuse_cutover() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.current_block_number >= 200 THEN RAISE EXCEPTION 'publication refused'; END IF;
            RETURN NEW; END $$; CREATE TRIGGER refuse_cutover BEFORE INSERT OR UPDATE ON project_family_marker
            FOR EACH ROW EXECUTE FUNCTION refuse_cutover();").execute(&database.pool).await?;
        assert!(
            rebuild_fixture_families(&database.pool, chain, 202, "0xledger202")
                .await
                .is_err()
        );
        let refused: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(d) FROM resolution_divergences d ORDER BY resolver_chain_id, observed_positions::text")
            .fetch_all(&database.pool).await?;
        assert_eq!(
            refused, before,
            "failed publication changed evidence on {chain}"
        );
        sqlx::raw_sql("DROP TRIGGER refuse_cutover ON project_family_marker")
            .execute(&database.pool)
            .await?;
        rebuild_fixture_families(&database.pool, chain, 202, "0xledger202").await?;
        let row = bigname_storage::families::name::load_family_name(&database.pool, &id)
            .await?
            .context("admitted name")?;
        assert_eq!(
            row.declared_summary["unresolvable_reason"],
            json!("no_live_ens_v2_entry")
        );
        assert!(row.declared_summary["resolver"]["address"].is_null());
        let after: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(d) FROM resolution_divergences d ORDER BY resolver_chain_id, observed_positions::text")
            .fetch_all(&database.pool).await?;
        assert_eq!(after.len(), before.len(), "observations remain durable");
        for (previous, mut current) in before.into_iter().zip(after) {
            if previous["resolver_chain_id"] == chain && previous["cleared_at"].is_null() {
                assert!(
                    current["cleared_at"].is_string(),
                    "admission left current evidence active on {chain}: {current}"
                );
                current["cleared_at"] = Value::Null;
            }
            assert_eq!(
                current, previous,
                "publication changed an observation beyond retirement"
            );
        }
        database.cleanup().await?;
    }
    Ok(())
}

async fn cutover_lookup(
    database: &TestDatabase,
    chain: &str,
    id: &str,
    address: &str,
) -> Result<bigname_lookup::LedgerAction> {
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(address)])
            .await?;
    let engine = bigname_lookup::LookupEngine::new(
        database.pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{chain}={url}")])?,
    );
    let answer = engine
        .lookup(bigname_lookup::LookupRequest::new(id, ["addr:60"])?)
        .await?;
    assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 1);
    Ok(answer.records[0].ledger_action)
}
