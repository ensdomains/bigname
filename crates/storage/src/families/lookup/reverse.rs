//! Reverse pages select/count compact exact relation keys in SQL. Name cores and inventories
//! are loaded only for the requested page and sentinel; rendered identity remains live.
use crate::{
    IdentityPrimaryNameSnapshot, ReverseIdentityGroup, ReverseIdentityRecordRow,
    ReverseIdentityRoles, ReverseIdentityStorageInput,
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};
use std::collections::BTreeMap;

/// Address-first input and the current identity join are shared by page and exact-count reads.
/// No inventory payload or authority compositor participates in membership or count.
pub(crate) fn eligible_sql() -> String {
    format!("WITH related AS MATERIALIZED (
        SELECT relation.chain_id, relation.logical_name_id,
               array_agg(relation.relation ORDER BY CASE relation.relation WHEN 'token_holder' THEN 0 ELSE 1 END) AS relations,
               CASE WHEN bool_or(relation.relation='token_holder') THEN 0::smallint ELSE 1::smallint END AS role_rank
        FROM bigname_phase.project_lookup_relation relation
        WHERE relation.address=lower($1) AND relation.relation=ANY($3)
          AND ($4::text[] IS NULL OR relation.chain_id=ANY($4))
        GROUP BY relation.chain_id, relation.logical_name_id
    ), eligible AS (
        SELECT related.logical_name_id, related.relations, related.role_rank,
               {rendered} COLLATE \"C\" AS name, surface.namespace COLLATE \"C\" AS namespace,
               surface.namehash COLLATE \"C\" AS namehash
        FROM related
        JOIN bigname_phase.project_lookup_name stored
          ON stored.chain_id=related.chain_id AND stored.logical_name_id=related.logical_name_id
        JOIN bigname_phase.name_surfaces surface
          ON surface.chain_id=related.chain_id AND surface.logical_name_id=related.logical_name_id
        JOIN bigname_phase.project_family_marker marker ON marker.chain_id=related.chain_id
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id=surface.chain_id AND lineage.block_hash=surface.block_hash
        WHERE stored.supported
          AND surface.namespace=ANY($2) AND {composed}
          AND surface.block_number<=marker.current_block_number
          AND surface.canonicality_state IN ('canonical','safe','finalized')
          AND lineage.canonicality_state IN ('canonical','safe','finalized')
    )", rendered=super::super::name::rendered::rendered_name_sql("surface"),
        composed=super::super::name::rendered::composed_surface_sql("surface"))
}

pub(crate) async fn group_on(
    conn: &mut PgConnection,
    input: &ReverseIdentityStorageInput,
    namespaces: &[String],
    primary: &BTreeMap<String, IdentityPrimaryNameSnapshot>,
    chains: Option<&[String]>,
    include_count: bool,
    include_inventory: bool,
) -> Result<ReverseIdentityGroup> {
    let relations = match input.roles {
        ReverseIdentityRoles::Owned => vec!["token_holder"],
        ReverseIdentityRoles::Managed => vec!["effective_controller"],
        ReverseIdentityRoles::Both => vec!["token_holder", "effective_controller"],
    };
    let eligible = eligible_sql();
    let total_count = if include_count {
        let count: i64 = sqlx::query_scalar(&format!(
            "/* storage:families.lookup.reverse_count */ {eligible} SELECT count(*) FROM eligible"
        ))
        .bind(&input.address)
        .bind(namespaces)
        .bind(&relations)
        .bind(chains)
        .fetch_one(&mut *conn)
        .await
        .context("failed to count lookup relations")?;
        Some(u64::try_from(count).context("negative lookup relation count")?)
    } else {
        None
    };
    let primary_names: Value = primary
        .iter()
        .filter_map(|(namespace, claim)| {
            claim
                .normalized_claim_name
                .as_ref()
                .map(|name| (namespace.clone(), json!(name)))
        })
        .collect();
    let cursor = input.cursor.as_ref();
    let limit = input.page_size.max(0);
    let keys = sqlx::query(&format!("/* storage:families.lookup.reverse_page */
        {eligible}, ordered AS (
            SELECT eligible.*, NOT COALESCE(name=$5::jsonb ->> namespace, false) AS non_primary FROM eligible
        ) SELECT * FROM ordered
        WHERE ($6::boolean IS NULL OR (non_primary, role_rank, name, namespace, namehash) > ($6,$7,$8,$9,$10))
        ORDER BY non_primary, role_rank, name, namespace, namehash, logical_name_id COLLATE \"C\" LIMIT $11"))
        .bind(&input.address).bind(namespaces).bind(&relations).bind(chains).bind(primary_names)
        .bind(cursor.map(|c| !c.is_primary)).bind(cursor.map(|c| c.role_rank))
        .bind(cursor.map(|c| &c.normalized_name)).bind(cursor.map(|c| &c.namespace)).bind(cursor.map(|c| &c.namehash))
        .bind(limit.saturating_add(1)).fetch_all(&mut *conn).await.context("failed to page lookup relations")?;
    let ids: Vec<String> = keys
        .iter()
        .map(|row| row.try_get("logical_name_id"))
        .collect::<std::result::Result<_, _>>()?;
    let mut records: BTreeMap<_, _> = super::read::load_on(conn, &ids, include_inventory)
        .await?
        .into_iter()
        .map(|record| (record.row.logical_name_id.clone(), record))
        .collect();
    let has_more = i64::try_from(keys.len()).unwrap_or(i64::MAX) > limit;
    let mut entries = Vec::new();
    for key in keys
        .into_iter()
        .take(usize::try_from(limit).unwrap_or(usize::MAX))
    {
        let id: String = key.try_get("logical_name_id")?;
        let name_record = records
            .remove(&id)
            .with_context(|| format!("published lookup relation {id} has no name state"))?;
        let facets: Vec<String> = key.try_get("relations")?;
        let relation_facets = facets
            .iter()
            .map(|relation| super::read::relation_kind(relation))
            .collect::<Result<_>>()?;
        let claim = primary.get(&name_record.row.namespace).cloned();
        entries.push(ReverseIdentityRecordRow {
            name_record,
            relation_facets,
            primary_chain_positions: claim
                .as_ref()
                .and_then(|claim| claim.chain_positions.clone()),
            primary_name: claim,
            requested_coin_type: input.coin_type.clone(),
        });
    }
    Ok(ReverseIdentityGroup {
        input: input.clone(),
        entries,
        total_count,
        has_more,
    })
}
