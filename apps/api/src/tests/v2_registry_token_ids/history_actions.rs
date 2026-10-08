//! Product actions through admitted raw decoders, Interpret/Project and HTTP.
use super::compatibility::{admit_family_from, role_address};
use super::*;

#[path = "history_actions/account.rs"]
mod account;
#[path = "history_actions/handoff.rs"]
mod handoff;
#[path = "history_actions/pairs.rs"]
mod pairs;
#[path = "history_actions/registry.rs"]
mod registry;
#[path = "history_actions/resolver.rs"]
mod resolver;
#[path = "history_actions/wrapper.rs"]
mod wrapper;

async fn page(database: &TestDatabase, route: &str) -> Result<Value> {
    let (status, body) = read_family_response(
        database,
        &format!("{route}&include=data,raw,total_count&order=asc&page_size=200"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{route}: {body:#}");
    let actions = crate::tests::openapi_contract::document()["components"]["schemas"]["HistoryAction"]["enum"].as_array().unwrap().clone();
    for row in body["data"].as_array().unwrap() {
        assert!(actions.contains(&row["data"]["action"]), "{row:#}");
    }
    Ok(body)
}

fn emitted(
    data: alloy_primitives::LogData,
    emitter: Address,
    block: i64,
    index: i64,
) -> RawLogInput {
    let mut log = raw(data, block, index);
    log.emitting_address = format!("{emitter:#x}");
    log
}

fn action_rows<'a>(body: &'a Value, action: &str) -> Vec<&'a Value> {
    body["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["data"]["action"] == action)
        .collect()
}

#[tokio::test]
async fn history_actions_reject_previous_cursor_binding_with_restart() -> Result<()> {
    let database = routes::database_at(121).await?;
    for route in [
        format!("/v1/events?name={NAME}"),
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let (status, first) =
            read_family_response(&database, &format!("{route}&page_size=1")).await?;
        assert_eq!(status, StatusCode::OK, "{first}");
        let current = first["page"]["next_cursor"].as_str().context("cursor")?;
        // This is the actual previous payload format: v1 position/filter binding without
        // the history-only contract key, with identical sort, snapshot and last_item fields.
        let mut old: Value = serde_json::from_slice(&hex::decode(current)?)?;
        old["filters"]
            .as_object_mut()
            .unwrap()
            .remove("history_contract");
        let old = hex::encode(serde_json::to_vec(&old)?);
        let (status, error) =
            read_family_response(&database, &format!("{route}&page_size=1&cursor={old}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{error}");
        assert_eq!(error["error"]["code"], "stale");
        assert!(
            error["error"]["message"]
                .as_str()
                .unwrap()
                .contains("restart pagination without a cursor")
        );
        let (status, continued) =
            read_family_response(&database, &format!("{route}&page_size=1&cursor={current}"))
                .await?;
        assert_eq!(status, StatusCode::OK, "{continued}");
    }
    database.cleanup().await
}

pub(super) async fn assert_small_pages_match(
    database: &TestDatabase,
    route: &str,
    full: &Value,
) -> Result<()> {
    let rows = full["data"].as_array().context("history rows")?;
    assert_eq!(full["page"]["total_count"], rows.len(), "{full:#}");
    let mut cursor = None::<String>;
    let mut paged = Vec::new();
    loop {
        let url = format!(
            "{route}&include=data,raw,total_count&order=asc&page_size=1{}",
            cursor
                .as_ref()
                .map(|value| format!("&cursor={value}"))
                .unwrap_or_default()
        );
        let (status, body) = read_family_response(database, &url).await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        assert_eq!(body["page"]["total_count"], rows.len(), "{body:#}");
        paged.extend(
            body["data"]
                .as_array()
                .context("page rows")?
                .iter()
                .cloned(),
        );
        assert!(
            paged.len() <= rows.len(),
            "duplicate or unbounded history page: {body:#}"
        );
        cursor = body["page"]["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(paged, *rows, "{route}");
    Ok(())
}
