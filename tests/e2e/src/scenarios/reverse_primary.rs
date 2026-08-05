use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::responses::{pointer, primary_name};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

fn assert_declared_not_found(body: &Value) {
    assert_eq!(
        pointer(body, "/declared_state/claimed_primary_name/status"),
        "not_found",
        "a generic resolver NameChanged record is not an admitted primary-name claim; body: {body}"
    );
}

async fn assert_generic_name_record(run: &support::PipelineRun, raw_name: &str) -> Result<()> {
    let state: Value = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events \
         WHERE event_kind = 'RecordChanged' AND after_state->>'raw_name' = $1 \
           AND canonicality_state = 'canonical' ORDER BY normalized_event_id DESC LIMIT 1",
    )
    .bind(raw_name)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(state["raw_name"], raw_name);
    assert!(
        state.get("primary_claim_source").is_none(),
        "generic resolver record must remain separate from primary-name admission: {state}"
    );
    Ok(())
}

/// Generic resolver name records remain separate from admitted primary-name
/// claims across set, change, and clear operations.
#[tokio::test]
async fn generic_name_record_set_changed_then_cleared_stays_unadmitted() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let alice = rpc.accounts().await?[1];
    let alice_path = format!("{alice:#x}");

    ens_v1::set_reverse_name(&rpc, &deployment, alice, "alice.eth").await?;
    let first = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (
                 SELECT 1 FROM normalized_events
                 WHERE event_kind = 'RecordChanged'
                   AND canonicality_state = 'canonical'
                   AND after_state->>'raw_name' = 'alice.eth'
                   AND NOT (after_state ? 'primary_claim_source')
             )",
        ),
    )
    .await?;
    assert_generic_name_record(&first, "alice.eth").await?;
    let declared = primary_name(&first.api, "ens", 60, &alice_path, "declared").await?;
    assert_declared_not_found(&declared);
    first.db.cleanup().await?;

    ens_v1::set_reverse_name(&rpc, &deployment, alice, "bob.eth").await?;
    let changed = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (
                 SELECT 1 FROM normalized_events
                 WHERE event_kind = 'RecordChanged'
                   AND canonicality_state = 'canonical'
                   AND after_state->>'raw_name' = 'bob.eth'
                   AND NOT (after_state ? 'primary_claim_source')
             )",
        ),
    )
    .await?;
    assert_generic_name_record(&changed, "bob.eth").await?;
    let changed_body = primary_name(&changed.api, "ens", 60, &alice_path, "declared").await?;
    assert_declared_not_found(&changed_body);
    changed.db.cleanup().await?;

    ens_v1::set_reverse_name(&rpc, &deployment, alice, "").await?;
    let cleared = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (
                 SELECT 1 FROM normalized_events
                 WHERE event_kind = 'RecordChanged'
                   AND canonicality_state = 'canonical'
                   AND after_state->>'raw_name' = ''
                   AND NOT (after_state ? 'primary_claim_source')
             )",
        ),
    )
    .await?;
    assert_generic_name_record(&cleared, "").await?;
    let cleared_body = primary_name(&cleared.api, "ens", 60, &alice_path, "declared").await?;
    assert_declared_not_found(&cleared_body);

    cleared.db.cleanup().await?;
    Ok(())
}

/// Nonblank reverse claim strings that fail the ENSIP-15 boundary surface as
/// `invalid_name` with the raw claim preserved.
#[tokio::test]
async fn reverse_claim_invalid_name_surfaces_raw_claim() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let alice = rpc.accounts().await?[1];
    let alice_path = format!("{alice:#x}");
    let invalid_claim = "alice..eth";

    ens_v1::set_reverse_name(&rpc, &deployment, alice, invalid_claim).await?;
    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (
                 SELECT 1 FROM normalized_events
                 WHERE event_kind = 'RecordChanged'
                   AND canonicality_state = 'canonical'
                   AND after_state->>'raw_name' = 'alice..eth'
                   AND NOT (after_state ? 'primary_claim_source')
             )",
        ),
    )
    .await?;

    assert_generic_name_record(&run, invalid_claim).await?;
    let body = primary_name(&run.api, "ens", 60, &alice_path, "declared").await?;
    assert_declared_not_found(&body);

    run.db.cleanup().await?;
    Ok(())
}
