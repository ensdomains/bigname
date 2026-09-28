// Live lookup after the family cutover: inputs originate in normalized events and actual
// Project reducers, and removing served rows must not change resolution or persistence.
async fn seed_family_lookup_fixture(database: &TestDatabase) -> Result<String> {
    seed_switch_records_fixture(database).await?;
    seed_schema_v2_ens_manifest_on_chain(
        &database.pool,
        SWITCH_CHAIN,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0x7b700001),
        true,
    )
    .await?;
    let id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = 'alpha.eth'",
    )
    .fetch_one(&database.pool)
    .await?;
    let served:Value = sqlx::query_scalar("SELECT declared_summary -> 'topology' FROM bigname_phase.name_current WHERE logical_name_id = $1")
        .bind(&id).fetch_one(&database.pool).await?;
    let family = bigname_storage::families::name::load_family_name(&database.pool, &id)
        .await?
        .context("family name")?;
    assert_eq!(family.declared_summary["topology"], served);
    assert_eq!(
        served["resolver_path"][0]["address"],
        json!(SWITCH_RESOLVER)
    );
    for table in ["record_inventory_current", "name_current"] {
        sqlx::query(&format!("DELETE FROM bigname_phase.{table}"))
            .execute(&database.pool)
            .await?;
    }
    // A stopped served batch leaves its old Project marker behind.
    sqlx::query("UPDATE bigname_phase.chain_phase_state SET current_block_number = 200, current_block_hash = '0xhistory200' WHERE phase_name = 'project'")
        .execute(&database.pool).await?;
    Ok(id)
}

#[tokio::test]
async fn family_lookup_compares_and_clears_divergence_without_served_inputs() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let id = seed_family_lookup_fixture(&database).await?;
    for (address, action, count) in [
        (SWITCH_ALICE, bigname_lookup::LedgerAction::None, 0_i64),
        (SWITCH_BOB, bigname_lookup::LedgerAction::Written, 1),
        (SWITCH_ALICE, bigname_lookup::LedgerAction::Cleared, 0),
    ] {
        let (url, handle) =
            spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
                address,
            )])
            .await?;
        let engine = bigname_lookup::LookupEngine::new(
            database.pool.clone(),
            bigname_lookup::ChainRpcUrls::from_entries(&[format!("{SWITCH_CHAIN}={url}")])?,
        );
        let answer = bigname_storage::publication_source::with_serve_from_families(
            true,
            engine.lookup(bigname_lookup::LookupRequest::new(&id, ["addr:60"])?),
        )
        .await?;
        assert_eq!(answer.records[0].value, Some(json!(address)));
        assert_eq!(answer.records[0].ledger_action, action);
        assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 1);
        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM bigname_phase.resolution_divergences WHERE cleared_at IS NULL",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(active, count);
    }
    database.cleanup().await
}

// The publication can advance or start a rebuild while the provider call is outstanding.
// Both are refused with the same result as a served Project republish. A stopped old Project
// publication is irrelevant when only its row version changes.
#[tokio::test]
async fn family_lookup_guards_its_publication_during_rpc() -> Result<()> {
    for (mutation, refused) in [
        (
            "UPDATE bigname_phase.project_family_marker SET sequence = sequence + 1",
            true,
        ),
        (
            "UPDATE bigname_phase.project_family_marker SET sequence = sequence + 1, current_block_number = NULL, current_block_hash = NULL, block_timestamp = NULL, state = 'bootstrap_pending'",
            true,
        ),
        (
            "UPDATE bigname_phase.chain_phase_state SET current_block_number = current_block_number WHERE phase_name = 'project'",
            false,
        ),
    ] {
        let database = TestDatabase::new_migrated().await?;
        let id = seed_family_lookup_fixture(&database).await?;
        let (url, reached, release, handle) =
            spawn_primary_name_mock_rpc_with_last_response_gate(vec![
                resolution_universal_resolver_addr60_response(SWITCH_BOB),
            ])
            .await?;
        let engine = bigname_lookup::LookupEngine::new(
            database.pool.clone(),
            bigname_lookup::ChainRpcUrls::from_entries(&[format!("{SWITCH_CHAIN}={url}")])?,
        );
        let request = bigname_lookup::LookupRequest::new(&id, ["addr:60"])?;
        let lookup = tokio::spawn(
            bigname_storage::publication_source::with_serve_from_families(true, async move {
                engine.lookup(request).await
            }),
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), reached)
            .await
            .context("lookup did not reach RPC")??;
        sqlx::query(mutation).execute(&database.pool).await?;
        release
            .send(())
            .map_err(|_| anyhow::anyhow!("RPC response receiver closed"))?;
        let result = lookup.await?;
        if refused {
            let error = result.expect_err("family publication changed after the snapshot");
            assert_eq!(error.kind(), bigname_lookup::ErrorKind::ConcurrentState);
        } else {
            assert_eq!(
                result?.records[0].ledger_action,
                bigname_lookup::LedgerAction::Written
            );
        }
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM bigname_phase.resolution_divergences")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(count, if refused { 0 } else { 1 });
        assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 1);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn family_lookup_batch_reads_names_inventory_and_relations_without_served_rows() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    for profile in ["feed", "detail"] {
        let request =
            json!({"profile":profile,"inputs":[{"name":"alpha.eth"},{"name":"beta.eth"}]});
        let served = bigname_storage::publication_source::with_serve_from_families(
            false,
            v2_lookup_json(&database, request.clone()),
        )
        .await?;
        let family = bigname_storage::publication_source::with_serve_from_families(
            true,
            v2_lookup_json(&database, request.clone()),
        )
        .await?;
        assert_eq!(family["data"], served["data"], "{profile}");
        assert_eq!(family["data"][0]["status"], "ok", "{family:#}");
        if profile == "detail" {
            assert_eq!(
                family["data"][0]["record"]["addresses"]["60"], SWITCH_ALICE,
                "{family:#}"
            );
        }
    }
    let request = json!({"profile":"detail","inputs":[{"name":"alpha.eth"}]});
    let before = bigname_storage::publication_source::with_serve_from_families(
        true,
        v2_lookup_json(&database, request.clone()),
    )
    .await?;
    for table in [
        "name_current",
        "record_inventory_current",
        "address_names_current",
    ] {
        sqlx::query(&format!("DELETE FROM bigname_phase.{table}"))
            .execute(&database.pool)
            .await?;
    }
    let after = bigname_storage::publication_source::with_serve_from_families(
        true,
        v2_lookup_json(&database, request),
    )
    .await?;
    assert_eq!(after["data"], before["data"]);
    database.cleanup().await
}
