//! Shared rollback safety checks for test-only retained-data entrypoints.
use super::TABLES;
use anyhow::{Context, Result, ensure};
use sqlx::{Postgres, Transaction};

pub(super) async fn projection_schema(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    // INSERT uses stage defaults. Refuse a schema that could advance persistent sequences,
    // because PostgreSQL sequence allocation is not undone by transaction rollback.
    let sequence_defaults: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_attrdef defaults JOIN pg_class relation
         ON relation.oid = defaults.adrelid JOIN pg_namespace namespace
         ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = 'bigname_phase' AND relation.relname = ANY($1)
           AND pg_get_expr(defaults.adbin, defaults.adrelid) LIKE '%nextval(%'",
    )
    .bind(TABLES)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        sequence_defaults == 0,
        "projection defaults use persistent sequences"
    );
    let identities: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_attribute attribute JOIN pg_class relation ON relation.oid=attribute.attrelid
         JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
         WHERE namespace.nspname='bigname_phase' AND relation.relname=ANY($1) AND attribute.attidentity <> ''")
        .bind(TABLES).fetch_one(&mut **tx).await?;
    ensure!(
        identities == 0,
        "projection tables contain sequence-backed identity columns"
    );
    let triggers: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT relation.relname, trigger.tgname, function.prosrc
         FROM pg_trigger trigger JOIN pg_class relation ON relation.oid=trigger.tgrelid
         JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
         JOIN pg_proc function ON function.oid=trigger.tgfoid
         WHERE namespace.nspname='bigname_phase' AND relation.relname=ANY($1)
           AND NOT trigger.tgisinternal",
    )
    .bind(TABLES)
    .fetch_all(&mut **tx)
    .await?;
    let expected_trigger = include_str!("../../../schema-v2/baseline/09_divergence.sql")
        .split("CREATE OR REPLACE FUNCTION retire_direct_divergences_for_null_resolver()")
        .nth(1)
        .context("missing pinned projection trigger")?
        .split("AS $$")
        .nth(1)
        .context("missing trigger body")?
        .split("$$;")
        .next()
        .context("missing trigger terminator")?
        .trim();
    for (table, name, body) in triggers {
        ensure!(
            table == "name_current"
                && name == "name_current_retire_null_resolver_divergences"
                && body.trim() == expected_trigger,
            "unreviewed projection trigger on {table}: {name}"
        );
    }
    Ok(())
}
