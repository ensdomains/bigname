//! Actual family publication and reads for the Interpret integration fixtures.
use super::*;
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use bigname_storage::{
    EffectivePermissionRow, NameCurrentRow, RecordInventoryCurrentRow, ResolverCurrentRow,
};

pub async fn publish_families(
    pool: &PgPool,
    chain: &str,
    height: i64,
    mode: FamilyMode,
) -> Result<()> {
    let hash: String = sqlx::query_scalar("SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2 AND canonicality_state IN ('canonical', 'safe', 'finalized')")
        .bind(chain).bind(height).fetch_one(pool).await?;
    let target = bigname_project::Marker {
        number: height,
        hash,
    };
    let token = families::input_token(pool, chain).await?;
    let outcome = families::apply(
        pool,
        chain,
        &target,
        mode,
        &token,
        &FamilyOptions::new(INTERPRETER_CONTENT_HASH).with_max_blocks_per_run(100_000),
    )
    .await?;
    assert!(!outcome.budget_exhausted);
    assert_eq!(outcome.marker.as_ref(), Some(&target));
    Ok(())
}

pub async fn project_name(pool: &PgPool, name: &str) -> Result<NameCurrentRow> {
    bigname_storage::families::name::load_family_name(pool, name)
        .await?
        .with_context(|| format!("published name {name}"))
}

pub async fn project_named(pool: &PgPool, display: &str) -> Result<NameCurrentRow> {
    let name: String =
        sqlx::query_scalar("SELECT logical_name_id FROM name_surfaces WHERE raw_name = $1")
            .bind(display)
            .fetch_one(pool)
            .await?;
    project_name(pool, &name).await
}

pub async fn project_resolver(
    pool: &PgPool,
    chain: &str,
    address: &str,
) -> Result<ResolverCurrentRow> {
    bigname_storage::families::topology::load_family_resolver_current(pool, chain, address)
        .await?
        .context("published resolver")
}

pub async fn project_inventory(
    pool: &PgPool,
    chain: &str,
    resource: Uuid,
) -> Result<RecordInventoryCurrentRow> {
    bigname_storage::families::records::load_family_record_inventory(pool, chain, resource)
        .await?
        .context("published record inventory")
}

pub async fn resource_permission(
    pool: &PgPool,
    resource: Uuid,
    subject: &str,
) -> Result<Option<EffectivePermissionRow>> {
    Ok(bigname_storage::load_serving_effective_permissions_page(
        pool,
        Some(subject),
        Some(resource),
        None,
        None,
        100,
    )
    .await?
    .rows
    .into_iter()
    .find(|row| row.scope.storage_key() == "resource"))
}

pub async fn served_fields(pool: &PgPool, name: &str) -> Result<Value> {
    let row = project_name(pool, name).await?;
    Ok(
        json!({"authority_arm": row.provenance["authority_selection"]["authority_arm"],
        "resource_id": row.resource_id, "surface_binding_id": row.surface_binding_id,
        "registration": row.declared_summary["registration"], "control": row.declared_summary["control"],
        "resolver": row.declared_summary["resolver"]}),
    )
}

/// The lifecycle selector's actual retained-fact trace, including the deciding event identity.
pub async fn cited_events(pool: &PgPool, name: &str) -> Result<Value> {
    use bigname_storage::families::control::lifecycle::{
        AuthoritySelection, Clock, NameInput, NamePlace, evaluate, load_name_facts,
    };
    let row = project_name(pool, name).await?;
    let chain = row
        .chain_positions
        .as_object()
        .and_then(|positions| positions.values().next())
        .and_then(|position| position["chain_id"].as_str())
        .context("name publication chain")?;
    let (block_number, timestamp_seconds): (i64, i64) = sqlx::query_as("SELECT marker.current_block_number, extract(epoch FROM lineage.block_timestamp)::bigint FROM project_family_marker marker JOIN chain_lineage lineage ON lineage.chain_id = marker.chain_id AND lineage.block_number = marker.current_block_number AND lineage.block_hash = marker.current_block_hash WHERE marker.chain_id = $1")
        .bind(chain).fetch_one(pool).await?;
    let facts = load_name_facts(
        pool,
        chain,
        &[NameInput {
            logical_name_id: name.into(),
            namehash: row.namehash.clone(),
            selection: AuthoritySelection::from_provenance(&row.provenance),
            place: NamePlace::of(
                &row.namespace,
                &row.normalized_name
                    .split('.')
                    .map(|label| {
                        format!(
                            "{:#x}",
                            alloy_primitives::B256::from(
                                bigname_storage::rendered_name::label_hash(label)
                            )
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
        }],
    )
    .await?;
    Ok(Value::Object(
        evaluate(
            facts.first().context("name lifecycle facts")?,
            &Clock {
                block_number,
                timestamp_seconds,
            },
        )?
        .trace,
    ))
}

/// Full family content.
pub async fn family_state(pool: &PgPool, chain: &str) -> Result<Value> {
    // Rebuild and restore may reach the same publication through different generations.
    // Verify each complete catalogue stamp before comparing content without that sequence.
    let invalid_catalogue: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM project_history_catalogue_marker catalogue
             FULL JOIN project_family_marker family USING (chain_id)
             WHERE COALESCE(catalogue.chain_id, family.chain_id) = $1
               AND (catalogue.chain_id IS NOT NULL OR family.current_block_number IS NOT NULL)
               AND ((catalogue.block_number, catalogue.block_hash,
                     catalogue.publication_sequence, catalogue.input_content_hash)
                    IS DISTINCT FROM (family.current_block_number, family.current_block_hash,
                                      family.sequence, family.input_content_hash)
                    OR catalogue.catalogue_version IS DISTINCT FROM 1))",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        !invalid_catalogue,
        "catalogue stamp does not match family publication"
    );
    let mut state = serde_json::Map::new();
    for table in families::family_tables() {
        let row = if table == "project_history_catalogue_marker" {
            "to_jsonb(row) - 'publication_sequence'"
        } else {
            "to_jsonb(row)"
        };
        let rows: Vec<Value> = sqlx::query_scalar(&format!(
            "SELECT {row} AS value FROM {table} row WHERE chain_id = $1 ORDER BY 1"
        ))
        .bind(chain)
        .fetch_all(pool)
        .await?;
        state.insert(table.into(), json!(rows));
    }
    Ok(Value::Object(state))
}
