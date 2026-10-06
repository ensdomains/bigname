//! Previous candidate selector kept only for its spelling/cursor reference tests.
use crate::{
    ReverseIdentityStorageInput,
    families::{
        name::rendered::composed_surface_sql,
        topology::{rendered_lateral_sql, textless_surface_sql, textless_surfaces_exist_sql},
    },
};
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;
type NameKey = (String, String, String);
type Candidate = (String, String, String, String);
/// The candidates with raw bytes, in page order; `candidates_sql` runs it as its first arm.
/// The page order is bytewise (`COLLATE "C"`) whatever the database collation: `group_on` and
/// the route's cursor compare the same keys as Rust strings.
const REVERSE_CANDIDATES_SQL: &str = "/* storage:families.records.reverse_candidates */
         SELECT DISTINCT surface.logical_name_id COLLATE \"C\" AS logical_name_id,
                surface.raw_name COLLATE \"C\" AS raw_name,
                surface.namespace COLLATE \"C\" AS namespace,
                surface.namehash COLLATE \"C\" AS namehash
         FROM bigname_phase.name_surfaces surface
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
         WHERE surface.visibility_state = 'active' AND surface.raw_name <> ''
           AND surface.block_number <= marker.current_block_number
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND surface.namespace = ANY($2)
           AND ($10::text[] IS NULL OR surface.chain_id = ANY($10))
           AND COALESCE(surface.raw_name = $3::jsonb ->> surface.namespace, false) = $4
           AND EXISTS (
               SELECT 1 FROM bigname_phase.project_address_name_index indexed
               WHERE indexed.address = lower($1)
                 AND indexed.logical_name_id = surface.logical_name_id
                 AND indexed.chain_id = surface.chain_id AND indexed.relation = ANY($5)
           )
           AND ($6::text IS NULL
                OR (surface.raw_name COLLATE \"C\", surface.namespace COLLATE \"C\",
                    surface.namehash COLLATE \"C\") > ($6, $7, $8))
         ORDER BY raw_name, namespace, namehash, logical_name_id
         LIMIT $9";

/// The statement `candidates_on` runs: [`REVERSE_CANDIDATES_SQL`], unchanged, then the same
/// candidates among the surfaces without raw bytes under their served name
/// (`name::rendered`), merged in page order. The second arm is not run while no surface lacks
/// its bytes (`textless_surfaces_exist_sql`).
fn candidates_sql() -> String {
    format!(
        "({REVERSE_CANDIDATES_SQL})
         UNION ALL
         (SELECT DISTINCT surface.logical_name_id COLLATE \"C\" AS logical_name_id,
                 rendered.name COLLATE \"C\" AS raw_name,
                 surface.namespace COLLATE \"C\" AS namespace,
                 surface.namehash COLLATE \"C\" AS namehash
          FROM bigname_phase.name_surfaces surface
          JOIN bigname_phase.chain_lineage lineage
            ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
          JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
          {rendered}
          WHERE {exist} AND {textless} AND {composed}
            AND surface.block_number <= marker.current_block_number
            AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            AND surface.namespace = ANY($2)
            AND ($10::text[] IS NULL OR surface.chain_id = ANY($10))
            AND COALESCE(rendered.name = $3::jsonb ->> surface.namespace, false) = $4
            AND EXISTS (
                SELECT 1 FROM bigname_phase.project_address_name_index indexed
                WHERE indexed.address = lower($1)
                  AND indexed.logical_name_id = surface.logical_name_id
                  AND indexed.chain_id = surface.chain_id AND indexed.relation = ANY($5)
            )
            AND ($6::text IS NULL
                 OR (rendered.name COLLATE \"C\", surface.namespace COLLATE \"C\",
                     surface.namehash COLLATE \"C\") > ($6, $7, $8))
          ORDER BY raw_name, namespace, namehash, logical_name_id
          LIMIT $9)
         ORDER BY raw_name, namespace, namehash, logical_name_id
         LIMIT $9",
        rendered = rendered_lateral_sql(),
        exist = textless_surfaces_exist_sql(),
        textless = textless_surface_sql("surface"),
        composed = composed_surface_sql("surface"),
    )
}

#[allow(clippy::too_many_arguments)]
async fn candidates_on(
    conn: &mut PgConnection,
    input: &ReverseIdentityStorageInput,
    namespaces: &[String],
    primary: &Value,
    is_primary: bool,
    rank: i16,
    after: Option<&NameKey>,
    chains: Option<&[String]>,
    limit: i64,
) -> Result<Vec<Candidate>> {
    let relations = if rank == 0 {
        vec!["token_holder"]
    } else {
        vec!["effective_controller"]
    };
    sqlx::query_as(&candidates_sql())
        .bind(&input.address)
        .bind(namespaces)
        .bind(primary)
        .bind(is_primary)
        .bind(relations)
        .bind(after.map(|key| &key.0))
        .bind(after.map(|key| &key.1))
        .bind(after.map(|key| &key.2))
        .bind(limit)
        .bind(chains)
        .persistent(false)
        .fetch_all(conn)
        .await
        .context("failed to seek family reverse lookup candidates")
}

#[path = "reverse_page_tests.rs"]
mod tests;
