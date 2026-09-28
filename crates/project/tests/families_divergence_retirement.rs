//! A successful null-resolver family publication retires earlier direct observations in the
//! same transaction. The evidence remains durable and a failed publication retires nothing.
#[path = "families_support/mod.rs"]
mod support;
use anyhow::Result;
use serde_json::json;
use support::{Event, Fixture};
const CHAIN: &str = "ethereum-mainnet";
const RESOLVER: &str = "0x00000000000000000000000000000000000000a1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

#[tokio::test]
async fn null_resolver_publication_retires_evidence_atomically() -> Result<()> {
    let fixture = Fixture::new("families_divergence_retirement", 3).await?;
    fixture.lineage(CHAIN, 3).await?;
    let node = format!("0x{:064x}", 1);
    let name = format!("ens:{node}");
    fixture.surface_on(CHAIN, &name, &node).await?;
    for (identity, kind, after) in [
        (
            "owner:1",
            "AuthorityTransferred",
            json!({"owner":RESOLVER,"owner_getter":RESOLVER,"source_event":"Transfer", "node":node}),
        ),
        (
            "resolver:1",
            "ResolverChanged",
            json!({"resolver":RESOLVER,"node":node}),
        ),
    ] {
        fixture
            .event(
                Event::new(
                    identity,
                    1,
                    i64::from(kind == "ResolverChanged"),
                    kind,
                    "ens_v1_registry_l1",
                )
                .on(CHAIN)
                .name(&name)
                .after(after),
            )
            .await?;
    }
    fixture.apply_on(CHAIN, 1).await?;
    sqlx::query(
        "INSERT INTO resolution_divergences (logical_name_id, resolver_chain_id,
        resolver_address, request_kind, observed_positions, indexed_result, live_result)
        SELECT $1, $2, $3, 'text:avatar', jsonb_build_object('ethereum', jsonb_build_object(
          'chain_id', chain_id, 'block_number', block_number, 'block_hash', block_hash,
          'timestamp', to_jsonb(block_timestamp))), '\"before\"'::jsonb, '\"live\"'::jsonb
        FROM chain_lineage WHERE chain_id=$2 AND block_number=1",
    )
    .bind(&name)
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(&fixture.pool)
    .await?;
    fixture
        .event(
            Event::new("resolver:2", 2, 0, "ResolverChanged", "ens_v1_registry_l1")
                .on(CHAIN)
                .name(&name)
                .after(json!({"resolver":ZERO,"node":node})),
        )
        .await?;
    sqlx::raw_sql(
        "CREATE FUNCTION refuse_marker() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.current_block_number = 2 THEN RAISE EXCEPTION 'publication refused'; END IF;
        RETURN NEW; END $$;
      CREATE TRIGGER refuse_marker BEFORE UPDATE ON project_family_marker
        FOR EACH ROW EXECUTE FUNCTION refuse_marker();",
    )
    .execute(&fixture.pool)
    .await?;
    assert!(fixture.apply_on(CHAIN, 2).await.is_err());
    assert_eq!(active(&fixture).await?, (1, 1));
    sqlx::raw_sql("DROP TRIGGER refuse_marker ON project_family_marker")
        .execute(&fixture.pool)
        .await?;
    fixture.apply_on(CHAIN, 2).await?;
    let row = bigname_storage::families::name::load_family_name(&fixture.pool, &name)
        .await?
        .expect("name");
    assert!(row.declared_summary["resolver"]["address"].is_null());
    assert_eq!(active(&fixture).await?, (1, 0));
    fixture.cleanup().await
}
async fn active(fixture: &Fixture) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE cleared_at IS NULL) FROM resolution_divergences",
    )
    .fetch_one(&fixture.pool)
    .await?)
}
