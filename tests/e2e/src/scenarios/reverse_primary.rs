use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::responses::{pointer, primary_name};
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

fn assert_declared_success(body: &Value, expected_name: &str) {
    assert_eq!(
        pointer(body, "/declared_state/claimed_primary_name/status"),
        "success",
        "claimed primary name should be a declared candidate only; body: {body}"
    );
    assert_eq!(
        pointer(body, "/declared_state/claimed_primary_name/name"),
        expected_name,
        "declared candidate should follow the latest reverse claim; body: {body}"
    );
    assert_eq!(
        pointer(
            body,
            "/declared_state/claimed_primary_name/provenance/source_family"
        ),
        "ens_v1_reverse_l1",
        "persisted declared claim should come from reverse intake; body: {body}"
    );
}

/// Reverse claims are declared candidates. With no execution RPC configured,
/// `mode=declared` omits verified state and `mode=both` keeps verification
/// separated as `not_found`.
#[tokio::test]
async fn reverse_claim_set_changed_then_cleared_tracks_declared_candidate() -> Result<()> {
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
                   AND after_state->'primary_claim_source'->>'address' IS NOT NULL
             )",
        ),
    )
    .await?;
    let declared = primary_name(&first.api, "ens", 60, &alice_path, "declared").await?;
    assert_declared_success(&declared, "alice.eth");
    assert_eq!(
        pointer(&declared, "/verified_state"),
        Value::Null,
        "declared mode should not fabricate verified primary state; body: {declared}"
    );
    let both = primary_name(&first.api, "ens", 60, &alice_path, "both").await?;
    assert_declared_success(&both, "alice.eth");
    assert_eq!(
        pointer(&both, "/verified_state/verified_primary_name/status"),
        "not_found",
        "without execution readback, verified primary state should remain absent/not_found; body: {both}"
    );
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
                   AND after_state->'primary_claim_source'->>'address' IS NOT NULL
             )",
        ),
    )
    .await?;
    let changed_body = primary_name(&changed.api, "ens", 60, &alice_path, "both").await?;
    assert_declared_success(&changed_body, "bob.eth");
    assert_eq!(
        pointer(
            &changed_body,
            "/verified_state/verified_primary_name/status"
        ),
        "not_found",
        "changed declared claim should not imply verified primary success; body: {changed_body}"
    );
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
                   AND after_state->'primary_claim_source'->>'address' IS NOT NULL
             )",
        ),
    )
    .await?;
    let cleared_body = primary_name(&cleared.api, "ens", 60, &alice_path, "both").await?;
    assert_eq!(
        pointer(&cleared_body, "/declared_state/claimed_primary_name/status"),
        "not_found",
        "blank reverse claim should clear the declared candidate; body: {cleared_body}"
    );
    assert_eq!(
        pointer(
            &cleared_body,
            "/verified_state/verified_primary_name/status"
        ),
        "not_found",
        "cleared declared claim should leave verified state not_found; body: {cleared_body}"
    );
    assert_eq!(
        pointer(&cleared_body, "/coverage/status"),
        "partial",
        "persisted ENS reverse tuple remains a supported partial primary-name class"
    );

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
                   AND after_state->'primary_claim_source'->>'address' IS NOT NULL
             )",
        ),
    )
    .await?;

    let body = primary_name(&run.api, "ens", 60, &alice_path, "both").await?;
    assert_eq!(
        pointer(&body, "/declared_state/claimed_primary_name/status"),
        "invalid_name",
        "invalid nonblank reverse claim should surface invalid_name; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/declared_state/claimed_primary_name/raw_claim_name"),
        invalid_claim,
        "invalid_name should preserve the raw claim string; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/declared_state/claimed_primary_name/name"),
        Value::Null,
        "invalid_name must not silently coerce a claimed_primary_name.name; body: {body}"
    );
    assert_eq!(
        pointer(&body, "/verified_state/verified_primary_name/status"),
        "not_found",
        "declared invalid_name does not create verified primary state; body: {body}"
    );

    let raw_claim: String = sqlx::query_scalar(
        "SELECT raw_claim_name
         FROM primary_names_current
         WHERE address = $1
           AND namespace = 'ens'
           AND coin_type = '60'
           AND claim_status = 'invalid_name'",
    )
    .bind(alice_path)
    .fetch_one(&run.db.pool)
    .await
    .context("primary_names_current invalid_name row should preserve raw_claim_name")?;
    assert_eq!(raw_claim, invalid_claim);

    run.db.cleanup().await?;
    Ok(())
}
