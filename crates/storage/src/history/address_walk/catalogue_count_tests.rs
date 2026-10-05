//! Count eligibility over explicit catalogue states. Route tests separately publish through
//! real Project; these fixtures exercise disjoint SQL arms and conservative kind classification.

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::{
    AddressRead,
    catalogue_count::{self, CountOutcome},
    catalogue_tests::install_catalogue,
    plan_tests,
};
use crate::history::{EventHistoryReadFilter, HistoryScope, HistorySummaryMode};

const ADDRESS: &str = "0x0000000000000000000000000000000000000a11";

fn read() -> AddressRead<'static> {
    AddressRead {
        address: ADDRESS,
        namespace: Some("ens"),
        relations: None,
        scope: HistoryScope::Both,
        canonical_only: true,
        published: None,
        catalogue: true,
    }
}

#[tokio::test]
async fn catalogue_direct_counts_are_complete_and_disjoint() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("catalogue_direct_count")).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        plan_tests::install(&mut connection, 4, 20).await?;
        install_catalogue(&mut connection).await?;
        let filter = EventHistoryReadFilter { event_kinds: vec!["RegistrationGranted".into()], ..Default::default() };
        for scope in [HistoryScope::Both, HistoryScope::Surface, HistoryScope::Resource] {
            let read = AddressRead { scope, ..read() };
            assert_eq!(catalogue_count::count(&mut connection, &read, &filter, HistorySummaryMode::Count).await?, CountOutcome::Exact(4));
            assert_eq!(catalogue_count::count(&mut connection, &read, &filter, HistorySummaryMode::CappedCount(2)).await?, CountOutcome::OverCap(3));
            assert_eq!(catalogue_count::count(&mut connection, &read, &filter, HistorySummaryMode::CappedCount(4)).await?, CountOutcome::Exact(4));
        }
        // Name 1 has only a current NAME witness; name 2 has only a current RESOURCE
        // witness. Names 3/4 retain both historical arms. Each of the four grants still
        // belongs once to Both, and exactly three belong to either individual scope.
        sqlx::raw_sql("DELETE FROM project_address_history_anchor WHERE anchor_kind = 1 AND anchor_id = '00000000-0000-0000-0000-000000000001';
            DELETE FROM project_address_history_anchor WHERE anchor_kind = 0 AND anchor_id = 'ens:0x0000000000000000000000000000000000000000000000000000000000000002';
            UPDATE project_address_history_anchor SET current_mask=1,historical_mask=0 WHERE
              (anchor_kind=0 AND anchor_id='ens:0x0000000000000000000000000000000000000000000000000000000000000001') OR
              (anchor_kind=1 AND anchor_id='00000000-0000-0000-0000-000000000002');")
            .execute(&mut *connection).await?;
        for (scope, expected) in [(HistoryScope::Both,4), (HistoryScope::Surface,3), (HistoryScope::Resource,3)] {
            assert_eq!(catalogue_count::count(&mut connection, &AddressRead { scope, ..read() }, &filter, HistorySummaryMode::Count).await?, CountOutcome::Exact(expected));
        }
        // Independent retained events with only one key must not disappear from the
        // complete scalar count or be mistaken for overlapping name/resource witnesses.
        sqlx::raw_sql("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
            SELECT 'only-name','ens',logical_name_id,NULL,'RegistrationRenewed',source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,0,12,derivation_kind,canonicality_state,'{}'::jsonb FROM normalized_events WHERE event_identity='plan:grant:1'
            UNION ALL SELECT 'only-resource','ens',NULL,resource_id,'RegistrationRenewed',source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,0,12,derivation_kind,canonicality_state,'{}'::jsonb FROM normalized_events WHERE event_identity='plan:grant:2';")
            .execute(&mut *connection).await?;
        let renewals = EventHistoryReadFilter { event_kinds: vec!["RegistrationRenewed".into()], ..Default::default() };
        for (scope, expected) in [(HistoryScope::Both,2), (HistoryScope::Surface,1), (HistoryScope::Resource,1)] {
            assert_eq!(catalogue_count::count(&mut connection, &AddressRead { scope, ..read() }, &renewals, HistorySummaryMode::CappedCount(10)).await?, CountOutcome::Exact(expected));
        }
        let unknown = EventHistoryReadFilter { event_kinds: vec!["FutureUnknownKind".into()], ..Default::default() };
        assert_eq!(catalogue_count::count(&mut connection, &read(), &unknown, HistorySummaryMode::Count).await?, CountOutcome::Unknown);
        assert_eq!(catalogue_count::count(&mut connection, &read(), &filter, HistorySummaryMode::None).await?, CountOutcome::Unknown);
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}
