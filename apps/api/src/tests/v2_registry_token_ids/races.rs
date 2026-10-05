use super::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

#[tokio::test]
async fn registry_token_ids_keep_name_snapshots_and_reject_changed_lookup_publication() -> Result<()>
{
    for route in [
        format!("/v1/names/{NAME}"),
        "lookup".into(),
        format!("/v1/resolvers/1/{}", routes::RESOLVER),
    ] {
        let database = routes::database_at(120).await?;
        let (status, body) = {
            let reached = Arc::new(Notify::new());
            let resume = Arc::new(Notify::new());
            let request = crate::v2::registry_token_read_test_hooks::with_pause(
                reached.clone(),
                resume.clone(),
                routes::request(&database, &route),
            );
            tokio::pin!(request);
            tokio::select! {
                _=reached.notified()=>{},
                result=&mut request=>anyhow::bail!("request missed token read pause: {result:?}"),
                _=tokio::time::sleep(Duration::from_secs(10))=>anyhow::bail!("token read pause timed out"),
            }
            bigname_interpret::Engine::new(database.pool.clone())
                .run_batch(bigname_interpret::BatchRequest {
                    chain_id: CHAIN.into(),
                    from_block: 121,
                    to_block: 121,
                    resume_current: None,
                    mode: bigname_interpret::RunMode::Normal,
                })
                .await?;
            routes::publish(&database, 121).await?;
            resume.notify_one();
            request.await?
        };
        if route == "lookup" {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["code"], "stale", "{body}");
        } else if status == StatusCode::OK {
            let rows = routes::token_rows(&route, &body);
            assert!(!rows.is_empty(), "{body}");
            for row in rows {
                assert_eq!(row["token_id"], token(0).to_string(), "{body}");
            }
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["code"], "stale", "{body}");
        }
        let (status, body) = routes::request(&database, &route).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        for row in routes::token_rows(&route, &body) {
            assert_eq!(row["token_id"], token(1).to_string(), "{body}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn registry_token_ids_keep_name_snapshots_during_overlapping_interpret_redo() -> Result<()> {
    for route in [
        format!("/v1/names/{NAME}"),
        "lookup".into(),
        format!("/v1/resolvers/1/{}", routes::RESOLVER),
    ] {
        let database = routes::database_at(121).await?;
        let (status, body) = {
            let reached = Arc::new(Notify::new());
            let resume = Arc::new(Notify::new());
            let request = crate::v2::registry_token_read_test_hooks::with_pause(
                reached.clone(),
                resume.clone(),
                routes::request(&database, &route),
            );
            tokio::pin!(request);
            tokio::select! {
                _=reached.notified()=>{},
                result=&mut request=>anyhow::bail!("request missed token read pause: {result:?}"),
                _=tokio::time::sleep(Duration::from_secs(10))=>anyhow::bail!("token read pause timed out"),
            }
            // The test helper commits the runner's redo-start state. Actual Interpret then
            // replaces the normalized range beneath the unchanged family publication. The
            // removed raw fixture log models corrected intake; no token summary is edited.
            database
                .simulate_interpret_redo_begin(CHAIN, "redo")
                .await?;
            sqlx::query("DELETE FROM raw_logs WHERE chain_id=$1 AND block_number=121")
                .bind(CHAIN)
                .execute(&database.pool)
                .await?;
            bigname_interpret::Engine::new(database.pool.clone())
                .run_batch(bigname_interpret::BatchRequest {
                    chain_id: CHAIN.into(),
                    from_block: 121,
                    to_block: 121,
                    resume_current: None,
                    mode: bigname_interpret::RunMode::Redo,
                })
                .await?;
            let current: i64 = sqlx::query_scalar(
                "SELECT current_block_number FROM project_family_marker WHERE chain_id=$1",
            )
            .bind(CHAIN)
            .fetch_one(&database.pool)
            .await?;
            assert_eq!(current, 121);
            let remaining: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM normalized_events WHERE event_kind='TokenRegenerated'",
            )
            .fetch_one(&database.pool)
            .await?;
            assert_eq!(
                remaining, 0,
                "Interpret must replace the token evidence before enrichment"
            );
            resume.notify_one();
            request.await?
        };
        if route == "lookup" || status != StatusCode::OK {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["code"], "stale", "{body}");
        } else {
            let rows = routes::token_rows(&route, &body);
            assert!(!rows.is_empty(), "{body}");
            for row in rows {
                assert_eq!(
                    row["token_id"],
                    token(1).to_string(),
                    "old row needs old token: {body}"
                );
            }
        }
        database.cleanup().await?;
    }
    Ok(())
}
