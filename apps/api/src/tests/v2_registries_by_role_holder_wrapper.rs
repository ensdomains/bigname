//! `GET /v1/registries?role_holder=` on the WrapperRegistry fixture. It lists stored root
//! grants only:
//!
//! | Holder | `/v1/permissions?registry=` | Expected here |
//! | --- | --- | --- |
//! | `PARENT`, the parent registry contract | its stored root grant on the WrapperRegistry | that row |
//! | `ADMIN` | its stored root grants on both registries | those rows |
//! | `ALICE`, the parent name's owner | a derived holder row with the parent's root powers, which replaces her stored grant | her stored grant, with its own powers |
//! | `OPERATOR`, approved by the owner | a derived operator row, which replaces its stored grant | its stored grant, with its own powers |
use super::*;

async fn registries_of(database: &TestDatabase, holder: &str) -> Result<Value> {
    let body = payload(database, &format!("/v1/registries?role_holder={holder}")).await?;
    assert_eq!(body["page"]["has_more"], false, "{body:#}");
    assert_eq!(body["page"]["total_count"], Value::Null, "{body:#}");
    Ok(body["data"].clone())
}

/// The rows the registry reads list for `holder` as its own stored root grants, one per
/// registry, in address order.
fn stored_rows(pages: &[(&str, Value)], holder: &str) -> Value {
    let mut rows = Vec::new();
    for (registry, page) in pages {
        assert_eq!(page["page"]["has_more"], false, "{page:#}");
        let held: Vec<&Value> = for_subject(page, holder)
            .into_iter()
            .filter(|row| {
                row["grant_scope"]["kind"] == "root" && row.get("grant_relation").is_none()
            })
            .collect();
        if held.is_empty() {
            continue;
        }
        let powers = held
            .iter()
            .flat_map(|row| row["powers"].as_array().unwrap().iter())
            .map(|power| power.as_str().unwrap().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        rows.push(
            json!({"chain_id":11155111, "address":registry, "grant_count":held.len(),
            "powers":powers}),
        );
    }
    Value::Array(rows)
}

#[tokio::test]
async fn registries_by_role_holder_list_stored_wrapper_registry_grants_only() -> Result<()> {
    let database = setup(&manual_logs(U256::ZERO, (TIME + 100) as u64)).await?;
    interpret(&database, false, false).await?;
    publish(&database, 4).await?;
    let pages = [
        (PARENT, registry_page(&database, PARENT).await?),
        (WRAPPER, registry_page(&database, WRAPPER).await?),
    ];
    assert_derived(&pages[1].1, ALICE, "holder", PARENT, ALICE);
    assert_derived(&pages[1].1, OPERATOR, "operator", PARENT, ALICE);

    assert_eq!(
        registries_of(&database, PARENT).await?,
        json!([{"chain_id":11155111, "address":WRAPPER, "grant_count":1,
                "powers":["can_transfer_admin", "registrar", "renew", "set_parent"]}])
    );
    for holder in [PARENT, ADMIN] {
        assert_eq!(
            registries_of(&database, holder).await?,
            stored_rows(&pages, holder),
            "{holder}"
        );
    }
    // The owner and its operator hold stored root grants on the WrapperRegistry, which its
    // registry read replaces with the parent's root powers. Here they are listed as stored.
    for (holder, power) in [(ALICE, "set_subregistry"), (OPERATOR, "set_resolver")] {
        assert_eq!(stored_rows(&pages, holder), json!([]), "{holder}");
        assert_eq!(
            registries_of(&database, holder).await?,
            json!([{"chain_id":11155111, "address":WRAPPER, "grant_count":1, "powers":[power]}]),
            "{holder}"
        );
    }
    database.cleanup().await
}
