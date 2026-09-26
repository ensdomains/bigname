use super::*;

// A wrapped ENSv1 name has an owner, so the control block of a grant whose `authority_kind` is
// `wrapper` is built like any other ENSv1 grant's: the registrant and the registry owner are
// served, and the block is not marked unsupported. On Sepolia every ENSv1 grant read on
// 2026-09-25 carried `authority_kind = registrar`, so the removed branch did not fire there; that
// is data read on that day, not a guarantee.
#[tokio::test]
async fn a_wrapper_grant_serves_its_control_owner() -> Result<()> {
    let (db, pool) = database("wrapper_grant_control").await?;
    let logical = surface(&pool, 95, "wrapped-owner.eth", &["ens_v1"]).await?;
    let resource = uuid(1, 95);
    let holder = "0x0000000000000000000000000000000000000095";
    let name_wrapper = "0x0000000000000000000000000000000000000a95";
    for (log, family, kind, after) in [
        (
            1,
            "ens_v1_registrar_l1",
            "RegistrationGranted",
            json!({"status":"registered","registrant":holder,"authority_kind":"wrapper","authority_key":"wrapper:wrapped-owner","expiry":4_000_000_000_i64}),
        ),
        (
            2,
            "ens_v1_registry_l1",
            "AuthorityTransferred",
            json!({"owner":name_wrapper,"owner_getter":name_wrapper}),
        ),
    ] {
        event(
            &pool,
            &format!("wrapper-control-{kind}"),
            &logical,
            Some(&resource),
            Event {
                family,
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    run(&pool).await?;
    let control: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'control' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    assert_ne!(control["status"], "unsupported", "{control:#}");
    assert!(control.get("unsupported_reason").is_none(), "{control:#}");
    assert_eq!(control["registrant"], holder, "{control:#}");
    assert_eq!(control["registry_owner"], name_wrapper, "{control:#}");
    db.cleanup().await?;
    Ok(())
}
