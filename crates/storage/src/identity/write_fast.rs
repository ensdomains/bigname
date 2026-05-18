use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use sqlx::Postgres;
use uuid::Uuid;

use super::types::{NameSurface, Resource, SurfaceBinding, TokenLineage};

const IDENTITY_FAST_INSERT_BATCH_SIZE: usize = 10_000;

fn canonicality_merge_sql_from(existing_table: &str, incoming_state: &str) -> String {
    format!(
        r#"
        CASE
            WHEN {incoming_state} = 'orphaned'::canonicality_state THEN
                'orphaned'::canonicality_state
            WHEN {incoming_state} = 'observed'::canonicality_state THEN
                CASE
                    WHEN {existing_table}.canonicality_state = 'orphaned'::canonicality_state THEN
                        'observed'::canonicality_state
                    ELSE {existing_table}.canonicality_state
                END
            WHEN {existing_table}.canonicality_state = 'orphaned'::canonicality_state THEN
                {incoming_state}
            WHEN (
                CASE {existing_table}.canonicality_state
                    WHEN 'observed'::canonicality_state THEN 0
                    WHEN 'canonical'::canonicality_state THEN 1
                    WHEN 'safe'::canonicality_state THEN 2
                    WHEN 'finalized'::canonicality_state THEN 3
                    WHEN 'orphaned'::canonicality_state THEN 4
                END
            ) >= (
                CASE {incoming_state}
                    WHEN 'observed'::canonicality_state THEN 0
                    WHEN 'canonical'::canonicality_state THEN 1
                    WHEN 'safe'::canonicality_state THEN 2
                    WHEN 'finalized'::canonicality_state THEN 3
                    WHEN 'orphaned'::canonicality_state THEN 4
                END
            ) THEN {existing_table}.canonicality_state
            ELSE {incoming_state}
        END
        "#,
    )
}

fn canonicality_merge_sql(table_name: &str) -> String {
    canonicality_merge_sql_from(table_name, "EXCLUDED.canonicality_state")
}

fn surface_binding_active_to_merge_sql(existing_table: &str, incoming_table: &str) -> String {
    format!(
        r#"
        CASE
            WHEN {existing_table}.active_to IS NOT NULL
             AND {incoming_table}.active_to IS NOT NULL THEN
                LEAST({existing_table}.active_to, {incoming_table}.active_to)
            WHEN {existing_table}.active_to IS NOT NULL THEN {existing_table}.active_to
            ELSE {incoming_table}.active_to
        END
        "#
    )
}

fn stable_anchor_matches_sql(table_name: &str) -> String {
    format!(
        r#"
        (
            {table_name}.chain_id = EXCLUDED.chain_id
            AND {table_name}.block_hash = EXCLUDED.block_hash
            AND {table_name}.block_number = EXCLUDED.block_number
        )
        "#
    )
}

fn stable_provenance_merge_sql(table_name: &str) -> String {
    format!(
        r#"
        CASE
            WHEN {same_anchor}
             AND {table_name}.provenance = EXCLUDED.provenance THEN {table_name}.provenance
            ELSE EXCLUDED.provenance
        END
        "#,
        same_anchor = stable_anchor_matches_sql(table_name),
    )
}

fn stable_anchor_refresh_allowed_sql(table_name: &str) -> String {
    format!(
        r#"
        (
            {table_name}.canonicality_state = 'orphaned'::canonicality_state
            OR {same_anchor}
        )
        "#,
        same_anchor = stable_anchor_matches_sql(table_name),
    )
}

fn stable_later_anchor_canonicality_refresh_allowed_sql(table_name: &str) -> String {
    format!(
        r#"
        (
            EXCLUDED.canonicality_state <> 'orphaned'::canonicality_state
            AND {table_name}.canonicality_state IS DISTINCT FROM {canonicality_merge}
        )
        "#,
        canonicality_merge = canonicality_merge_sql(table_name),
    )
}

fn unique_uuid_count(ids: impl IntoIterator<Item = Uuid>) -> usize {
    ids.into_iter().collect::<HashSet<_>>().len()
}

fn unique_string_count<'a>(ids: impl IntoIterator<Item = &'a str>) -> usize {
    ids.into_iter().collect::<HashSet<_>>().len()
}

pub(super) async fn insert_token_lineages_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    token_lineages: &[TokenLineage],
) -> Result<HashSet<Uuid>> {
    let mut inserted_ids = HashSet::new();
    for chunk in token_lineages.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut token_lineage_ids = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for token_lineage in chunk {
            token_lineage_ids.push(token_lineage.token_lineage_id);
            chain_ids.push(token_lineage.chain_id.clone());
            block_hashes.push(token_lineage.block_hash.clone());
            block_numbers.push(token_lineage.block_number);
            provenances.push(
                serde_json::to_string(&token_lineage.provenance)
                    .context("failed to serialize token-lineage provenance")?,
            );
            canonicality_states.push(token_lineage.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO token_lineages (
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::UUID[],
                $2::TEXT[],
                $3::TEXT[],
                $4::BIGINT[],
                $5::TEXT[],
                $6::TEXT[]
            ) AS input(
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            ON CONFLICT (token_lineage_id) DO NOTHING
            RETURNING token_lineage_id
            "#,
        )
        .bind(&token_lineage_ids)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert token lineages")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}

pub(super) async fn bulk_upsert_token_lineages_without_snapshots(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    token_lineages: &[TokenLineage],
) -> Result<()> {
    for chunk in token_lineages.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut token_lineage_ids = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for token_lineage in chunk {
            token_lineage_ids.push(token_lineage.token_lineage_id);
            chain_ids.push(token_lineage.chain_id.clone());
            block_hashes.push(token_lineage.block_hash.clone());
            block_numbers.push(token_lineage.block_number);
            provenances.push(
                serde_json::to_string(&token_lineage.provenance)
                    .context("failed to serialize token-lineage provenance")?,
            );
            canonicality_states.push(token_lineage.canonicality_state.as_str().to_owned());
        }

        let expected_count = unique_uuid_count(token_lineage_ids.iter().copied());
        let accepted_canonicality_merge = canonicality_merge_sql_from(
            "token_lineages",
            "input_rows.canonicality_state::canonicality_state",
        );
        let sql = format!(
            r#"
            WITH input_rows AS (
                SELECT DISTINCT ON (token_lineage_id)
                    token_lineage_id,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state
                FROM unnest(
                    $1::UUID[],
                    $2::TEXT[],
                    $3::TEXT[],
                    $4::BIGINT[],
                    $5::TEXT[],
                    $6::TEXT[]
                ) WITH ORDINALITY AS input(
                    token_lineage_id,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state,
                    ordinality
                )
                ORDER BY token_lineage_id, ordinality DESC
            ),
            upserted AS (
            INSERT INTO token_lineages (
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM input_rows
            ON CONFLICT (token_lineage_id) DO UPDATE
            SET
                chain_id = CASE WHEN {anchor_refresh} THEN EXCLUDED.chain_id ELSE token_lineages.chain_id END,
                block_hash = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_hash ELSE token_lineages.block_hash END,
                block_number = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_number ELSE token_lineages.block_number END,
                provenance = CASE WHEN {anchor_refresh} THEN {provenance_merge} ELSE token_lineages.provenance END,
                canonicality_state = {canonicality_merge},
                observed_at = CASE WHEN {anchor_refresh} THEN now() ELSE token_lineages.observed_at END
            WHERE
                {anchor_refresh}
                OR {later_anchor_canonicality_refresh}
            RETURNING token_lineage_id
            ),
            accepted_existing AS (
                SELECT input_rows.token_lineage_id
                FROM input_rows
                JOIN token_lineages
                  ON token_lineages.token_lineage_id = input_rows.token_lineage_id
                WHERE
                    token_lineages.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM upserted
                        WHERE upserted.token_lineage_id = input_rows.token_lineage_id
                    )
            )
            SELECT token_lineage_id FROM upserted
            UNION ALL
            SELECT token_lineage_id FROM accepted_existing
            "#,
            provenance_merge = stable_provenance_merge_sql("token_lineages"),
            canonicality_merge = canonicality_merge_sql("token_lineages"),
            anchor_refresh = stable_anchor_refresh_allowed_sql("token_lineages"),
            later_anchor_canonicality_refresh =
                stable_later_anchor_canonicality_refresh_allowed_sql("token_lineages"),
            accepted_canonicality_merge = accepted_canonicality_merge,
        );

        let upserted_ids = sqlx::query_scalar::<_, Uuid>(&sql)
            .bind(&token_lineage_ids)
            .bind(&chain_ids)
            .bind(&block_hashes)
            .bind(&block_numbers)
            .bind(&provenances)
            .bind(&canonicality_states)
            .fetch_all(&mut **executor)
            .await
            .context("failed to bulk upsert token lineages without snapshots")?;

        if upserted_ids.len() != expected_count {
            bail!(
                "bulk token-lineage upsert skipped {} rows because existing observations were incompatible",
                expected_count.saturating_sub(upserted_ids.len())
            );
        }
    }

    Ok(())
}

pub(super) async fn insert_resources_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    resources: &[Resource],
) -> Result<HashSet<Uuid>> {
    let mut inserted_ids = HashSet::new();
    for chunk in resources.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut resource_ids = Vec::with_capacity(chunk.len());
        let mut token_lineage_ids = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for resource in chunk {
            resource_ids.push(resource.resource_id);
            token_lineage_ids.push(resource.token_lineage_id);
            chain_ids.push(resource.chain_id.clone());
            block_hashes.push(resource.block_hash.clone());
            block_numbers.push(resource.block_number);
            provenances.push(
                serde_json::to_string(&resource.provenance)
                    .context("failed to serialize resource provenance")?,
            );
            canonicality_states.push(resource.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO resources (
                resource_id,
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                resource_id,
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::UUID[],
                $2::UUID[],
                $3::TEXT[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::TEXT[],
                $7::TEXT[]
            ) AS input(
                resource_id,
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            ON CONFLICT (resource_id) DO NOTHING
            RETURNING resource_id
            "#,
        )
        .bind(&resource_ids)
        .bind(&token_lineage_ids)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert resources")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}

pub(super) async fn bulk_upsert_resources_without_snapshots(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    resources: &[Resource],
) -> Result<()> {
    for chunk in resources.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut resource_ids = Vec::with_capacity(chunk.len());
        let mut token_lineage_ids = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for resource in chunk {
            resource_ids.push(resource.resource_id);
            token_lineage_ids.push(resource.token_lineage_id);
            chain_ids.push(resource.chain_id.clone());
            block_hashes.push(resource.block_hash.clone());
            block_numbers.push(resource.block_number);
            provenances.push(
                serde_json::to_string(&resource.provenance)
                    .context("failed to serialize resource provenance")?,
            );
            canonicality_states.push(resource.canonicality_state.as_str().to_owned());
        }

        let expected_count = unique_uuid_count(resource_ids.iter().copied());
        let accepted_token_lineage_id_merge =
            "COALESCE(resources.token_lineage_id, input_rows.token_lineage_id)";
        let accepted_canonicality_merge = canonicality_merge_sql_from(
            "resources",
            "input_rows.canonicality_state::canonicality_state",
        );
        let sql = format!(
            r#"
            WITH input_rows AS (
                SELECT DISTINCT ON (resource_id)
                    resource_id,
                    token_lineage_id,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state
                FROM unnest(
                    $1::UUID[],
                    $2::UUID[],
                    $3::TEXT[],
                    $4::TEXT[],
                    $5::BIGINT[],
                    $6::TEXT[],
                    $7::TEXT[]
                ) WITH ORDINALITY AS input(
                    resource_id,
                    token_lineage_id,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state,
                    ordinality
                )
                ORDER BY resource_id, ordinality DESC
            ),
            upserted AS (
            INSERT INTO resources (
                resource_id,
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                resource_id,
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM input_rows
            ON CONFLICT (resource_id) DO UPDATE
            SET
                token_lineage_id = COALESCE(resources.token_lineage_id, EXCLUDED.token_lineage_id),
                chain_id = CASE WHEN {anchor_refresh} THEN EXCLUDED.chain_id ELSE resources.chain_id END,
                block_hash = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_hash ELSE resources.block_hash END,
                block_number = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_number ELSE resources.block_number END,
                provenance = CASE WHEN {anchor_refresh} THEN {provenance_merge} ELSE resources.provenance END,
                canonicality_state = {canonicality_merge},
                observed_at = CASE WHEN {anchor_refresh} THEN now() ELSE resources.observed_at END
            WHERE
                NOT (
                    resources.token_lineage_id IS NOT NULL
                    AND EXCLUDED.token_lineage_id IS NOT NULL
                    AND resources.token_lineage_id <> EXCLUDED.token_lineage_id
                )
                AND (
                    {anchor_refresh}
                    OR {later_anchor_canonicality_refresh}
                    OR (
                        resources.token_lineage_id IS NULL
                        AND EXCLUDED.token_lineage_id IS NOT NULL
                    )
                )
            RETURNING resource_id
            ),
            accepted_existing AS (
                SELECT input_rows.resource_id
                FROM input_rows
                JOIN resources
                  ON resources.resource_id = input_rows.resource_id
                WHERE
                    resources.token_lineage_id IS NOT DISTINCT FROM {accepted_token_lineage_id_merge}
                    AND resources.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM upserted
                        WHERE upserted.resource_id = input_rows.resource_id
                    )
            )
            SELECT resource_id FROM upserted
            UNION ALL
            SELECT resource_id FROM accepted_existing
            "#,
            provenance_merge = stable_provenance_merge_sql("resources"),
            canonicality_merge = canonicality_merge_sql("resources"),
            anchor_refresh = stable_anchor_refresh_allowed_sql("resources"),
            later_anchor_canonicality_refresh =
                stable_later_anchor_canonicality_refresh_allowed_sql("resources"),
            accepted_token_lineage_id_merge = accepted_token_lineage_id_merge,
            accepted_canonicality_merge = accepted_canonicality_merge,
        );

        let upserted_ids = sqlx::query_scalar::<_, Uuid>(&sql)
            .bind(&resource_ids)
            .bind(&token_lineage_ids)
            .bind(&chain_ids)
            .bind(&block_hashes)
            .bind(&block_numbers)
            .bind(&provenances)
            .bind(&canonicality_states)
            .fetch_all(&mut **executor)
            .await
            .context("failed to bulk upsert resources without snapshots")?;

        if upserted_ids.len() != expected_count {
            bail!(
                "bulk resource upsert skipped {} rows because existing identities or observations were incompatible",
                expected_count.saturating_sub(upserted_ids.len())
            );
        }
    }

    Ok(())
}

pub(super) async fn insert_name_surfaces_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    name_surfaces: &[NameSurface],
) -> Result<HashSet<String>> {
    let mut inserted_ids = HashSet::new();
    for chunk in name_surfaces.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut logical_name_ids = Vec::with_capacity(chunk.len());
        let mut namespaces = Vec::with_capacity(chunk.len());
        let mut input_names = Vec::with_capacity(chunk.len());
        let mut canonical_display_names = Vec::with_capacity(chunk.len());
        let mut normalized_names = Vec::with_capacity(chunk.len());
        let mut dns_encoded_names = Vec::with_capacity(chunk.len());
        let mut namehashes = Vec::with_capacity(chunk.len());
        let mut labelhashes = Vec::with_capacity(chunk.len());
        let mut normalizer_versions = Vec::with_capacity(chunk.len());
        let mut normalization_warnings = Vec::with_capacity(chunk.len());
        let mut normalization_errors = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for surface in chunk {
            logical_name_ids.push(surface.logical_name_id.clone());
            namespaces.push(surface.namespace.clone());
            input_names.push(surface.input_name.clone());
            canonical_display_names.push(surface.canonical_display_name.clone());
            normalized_names.push(surface.normalized_name.clone());
            dns_encoded_names.push(surface.dns_encoded_name.clone());
            namehashes.push(surface.namehash.clone());
            labelhashes.push(
                serde_json::to_string(&surface.labelhashes)
                    .context("failed to serialize name-surface labelhashes")?,
            );
            normalizer_versions.push(surface.normalizer_version.clone());
            normalization_warnings.push(
                serde_json::to_string(&surface.normalization_warnings)
                    .context("failed to serialize name-surface normalization_warnings")?,
            );
            normalization_errors.push(
                serde_json::to_string(&surface.normalization_errors)
                    .context("failed to serialize name-surface normalization_errors")?,
            );
            chain_ids.push(surface.chain_id.clone());
            block_hashes.push(surface.block_hash.clone());
            block_numbers.push(surface.block_number);
            provenances.push(
                serde_json::to_string(&surface.provenance)
                    .context("failed to serialize name-surface provenance")?,
            );
            canonicality_states.push(surface.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, String>(
            r#"
            INSERT INTO name_surfaces (
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                labelhashes,
                normalizer_version,
                normalization_warnings,
                normalization_errors,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                ARRAY(SELECT jsonb_array_elements_text(labelhashes::jsonb)),
                normalizer_version,
                normalization_warnings::jsonb,
                normalization_errors::jsonb,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::BYTEA[],
                $7::TEXT[],
                $8::TEXT[],
                $9::TEXT[],
                $10::TEXT[],
                $11::TEXT[],
                $12::TEXT[],
                $13::TEXT[],
                $14::BIGINT[],
                $15::TEXT[],
                $16::TEXT[]
            ) AS input(
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                labelhashes,
                normalizer_version,
                normalization_warnings,
                normalization_errors,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            ON CONFLICT (logical_name_id) DO NOTHING
            RETURNING logical_name_id
            "#,
        )
        .bind(&logical_name_ids)
        .bind(&namespaces)
        .bind(&input_names)
        .bind(&canonical_display_names)
        .bind(&normalized_names)
        .bind(&dns_encoded_names)
        .bind(&namehashes)
        .bind(&labelhashes)
        .bind(&normalizer_versions)
        .bind(&normalization_warnings)
        .bind(&normalization_errors)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert name surfaces")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}

pub(super) async fn bulk_upsert_name_surfaces_without_snapshots(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    name_surfaces: &[NameSurface],
) -> Result<()> {
    for chunk in name_surfaces.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut logical_name_ids = Vec::with_capacity(chunk.len());
        let mut namespaces = Vec::with_capacity(chunk.len());
        let mut input_names = Vec::with_capacity(chunk.len());
        let mut canonical_display_names = Vec::with_capacity(chunk.len());
        let mut normalized_names = Vec::with_capacity(chunk.len());
        let mut dns_encoded_names = Vec::with_capacity(chunk.len());
        let mut namehashes = Vec::with_capacity(chunk.len());
        let mut labelhashes = Vec::with_capacity(chunk.len());
        let mut normalizer_versions = Vec::with_capacity(chunk.len());
        let mut normalization_warnings = Vec::with_capacity(chunk.len());
        let mut normalization_errors = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for surface in chunk {
            logical_name_ids.push(surface.logical_name_id.clone());
            namespaces.push(surface.namespace.clone());
            input_names.push(surface.input_name.clone());
            canonical_display_names.push(surface.canonical_display_name.clone());
            normalized_names.push(surface.normalized_name.clone());
            dns_encoded_names.push(surface.dns_encoded_name.clone());
            namehashes.push(surface.namehash.clone());
            labelhashes.push(
                serde_json::to_string(&surface.labelhashes)
                    .context("failed to serialize name-surface labelhashes")?,
            );
            normalizer_versions.push(surface.normalizer_version.clone());
            normalization_warnings.push(
                serde_json::to_string(&surface.normalization_warnings)
                    .context("failed to serialize name-surface normalization_warnings")?,
            );
            normalization_errors.push(
                serde_json::to_string(&surface.normalization_errors)
                    .context("failed to serialize name-surface normalization_errors")?,
            );
            chain_ids.push(surface.chain_id.clone());
            block_hashes.push(surface.block_hash.clone());
            block_numbers.push(surface.block_number);
            provenances.push(
                serde_json::to_string(&surface.provenance)
                    .context("failed to serialize name-surface provenance")?,
            );
            canonicality_states.push(surface.canonicality_state.as_str().to_owned());
        }

        let expected_count = unique_string_count(logical_name_ids.iter().map(String::as_str));
        let accepted_canonicality_merge = canonicality_merge_sql_from(
            "name_surfaces",
            "input_rows.canonicality_state::canonicality_state",
        );
        let sql = format!(
            r#"
            WITH input_rows AS (
                SELECT DISTINCT ON (logical_name_id)
                    logical_name_id,
                    namespace,
                    input_name,
                    canonical_display_name,
                    normalized_name,
                    dns_encoded_name,
                    namehash,
                    labelhashes,
                    normalizer_version,
                    normalization_warnings,
                    normalization_errors,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state
                FROM unnest(
                    $1::TEXT[],
                    $2::TEXT[],
                    $3::TEXT[],
                    $4::TEXT[],
                    $5::TEXT[],
                    $6::BYTEA[],
                    $7::TEXT[],
                    $8::TEXT[],
                    $9::TEXT[],
                    $10::TEXT[],
                    $11::TEXT[],
                    $12::TEXT[],
                    $13::TEXT[],
                    $14::BIGINT[],
                    $15::TEXT[],
                    $16::TEXT[]
                ) WITH ORDINALITY AS input(
                    logical_name_id,
                    namespace,
                    input_name,
                    canonical_display_name,
                    normalized_name,
                    dns_encoded_name,
                    namehash,
                    labelhashes,
                    normalizer_version,
                    normalization_warnings,
                    normalization_errors,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state,
                    ordinality
                )
                ORDER BY logical_name_id, ordinality DESC
            ),
            upserted AS (
            INSERT INTO name_surfaces (
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                labelhashes,
                normalizer_version,
                normalization_warnings,
                normalization_errors,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                ARRAY(SELECT jsonb_array_elements_text(labelhashes::jsonb)),
                normalizer_version,
                normalization_warnings::jsonb,
                normalization_errors::jsonb,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM input_rows
            ON CONFLICT (logical_name_id) DO UPDATE
            SET
                chain_id = CASE WHEN {anchor_refresh} THEN EXCLUDED.chain_id ELSE name_surfaces.chain_id END,
                block_hash = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_hash ELSE name_surfaces.block_hash END,
                block_number = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_number ELSE name_surfaces.block_number END,
                provenance = CASE WHEN {anchor_refresh} THEN {provenance_merge} ELSE name_surfaces.provenance END,
                canonicality_state = {canonicality_merge},
                observed_at = CASE WHEN {anchor_refresh} THEN now() ELSE name_surfaces.observed_at END
            WHERE
                name_surfaces.namespace = EXCLUDED.namespace
                AND name_surfaces.normalized_name = EXCLUDED.normalized_name
                AND name_surfaces.dns_encoded_name = EXCLUDED.dns_encoded_name
                AND name_surfaces.namehash = EXCLUDED.namehash
                AND name_surfaces.labelhashes = EXCLUDED.labelhashes
                AND name_surfaces.normalization_errors = EXCLUDED.normalization_errors
                AND (
                    {anchor_refresh}
                    OR {later_anchor_canonicality_refresh}
                )
            RETURNING logical_name_id
            ),
            accepted_existing AS (
                SELECT input_rows.logical_name_id
                FROM input_rows
                JOIN name_surfaces
                  ON name_surfaces.logical_name_id = input_rows.logical_name_id
                WHERE
                    name_surfaces.namespace = input_rows.namespace
                    AND name_surfaces.normalized_name = input_rows.normalized_name
                    AND name_surfaces.dns_encoded_name = input_rows.dns_encoded_name
                    AND name_surfaces.namehash = input_rows.namehash
                    AND name_surfaces.labelhashes = ARRAY(SELECT jsonb_array_elements_text(input_rows.labelhashes::jsonb))
                    AND name_surfaces.normalization_errors = input_rows.normalization_errors::jsonb
                    AND name_surfaces.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM upserted
                        WHERE upserted.logical_name_id = input_rows.logical_name_id
                    )
            )
            SELECT logical_name_id FROM upserted
            UNION ALL
            SELECT logical_name_id FROM accepted_existing
            "#,
            provenance_merge = stable_provenance_merge_sql("name_surfaces"),
            canonicality_merge = canonicality_merge_sql("name_surfaces"),
            anchor_refresh = stable_anchor_refresh_allowed_sql("name_surfaces"),
            later_anchor_canonicality_refresh =
                stable_later_anchor_canonicality_refresh_allowed_sql("name_surfaces"),
            accepted_canonicality_merge = accepted_canonicality_merge,
        );

        let upserted_ids = sqlx::query_scalar::<_, String>(&sql)
            .bind(&logical_name_ids)
            .bind(&namespaces)
            .bind(&input_names)
            .bind(&canonical_display_names)
            .bind(&normalized_names)
            .bind(&dns_encoded_names)
            .bind(&namehashes)
            .bind(&labelhashes)
            .bind(&normalizer_versions)
            .bind(&normalization_warnings)
            .bind(&normalization_errors)
            .bind(&chain_ids)
            .bind(&block_hashes)
            .bind(&block_numbers)
            .bind(&provenances)
            .bind(&canonicality_states)
            .fetch_all(&mut **executor)
            .await
            .context("failed to bulk upsert name surfaces without snapshots")?;

        if upserted_ids.len() != expected_count {
            let diagnostics_sql = format!(
                r#"
                WITH input_rows AS (
                    SELECT DISTINCT ON (logical_name_id)
                        logical_name_id,
                        namespace,
                        input_name,
                        canonical_display_name,
                        normalized_name,
                        dns_encoded_name,
                        namehash,
                        labelhashes,
                        normalizer_version,
                        normalization_warnings,
                        normalization_errors,
                        chain_id,
                        block_hash,
                        block_number,
                        provenance,
                        canonicality_state
                    FROM unnest(
                        $1::TEXT[],
                        $2::TEXT[],
                        $3::TEXT[],
                        $4::TEXT[],
                        $5::TEXT[],
                        $6::BYTEA[],
                        $7::TEXT[],
                        $8::TEXT[],
                        $9::TEXT[],
                        $10::TEXT[],
                        $11::TEXT[],
                        $12::TEXT[],
                        $13::TEXT[],
                        $14::BIGINT[],
                        $15::TEXT[],
                        $16::TEXT[]
                    ) WITH ORDINALITY AS input(
                        logical_name_id,
                        namespace,
                        input_name,
                        canonical_display_name,
                        normalized_name,
                        dns_encoded_name,
                        namehash,
                        labelhashes,
                        normalizer_version,
                        normalization_warnings,
                        normalization_errors,
                        chain_id,
                        block_hash,
                        block_number,
                        provenance,
                        canonicality_state,
                        ordinality
                    )
                    ORDER BY logical_name_id, ordinality DESC
                ),
                rejected AS (
                    SELECT jsonb_build_object(
                        'logical_name_id', input_rows.logical_name_id,
                        'incoming', jsonb_build_object(
                            'namespace', input_rows.namespace,
                            'input_name', input_rows.input_name,
                            'canonical_display_name', input_rows.canonical_display_name,
                            'normalized_name', input_rows.normalized_name,
                            'dns_encoded_name_hex', encode(input_rows.dns_encoded_name, 'hex'),
                            'namehash', input_rows.namehash,
                            'labelhashes', input_rows.labelhashes::jsonb,
                            'normalization_errors', input_rows.normalization_errors::jsonb,
                            'chain_id', input_rows.chain_id,
                            'block_hash', input_rows.block_hash,
                            'block_number', input_rows.block_number,
                            'provenance', input_rows.provenance::jsonb,
                            'canonicality_state', input_rows.canonicality_state
                        ),
                        'existing',
                            CASE
                                WHEN name_surfaces.logical_name_id IS NULL THEN NULL
                                ELSE jsonb_build_object(
                                    'namespace', name_surfaces.namespace,
                                    'input_name', name_surfaces.input_name,
                                    'canonical_display_name', name_surfaces.canonical_display_name,
                                    'normalized_name', name_surfaces.normalized_name,
                                    'dns_encoded_name_hex', encode(name_surfaces.dns_encoded_name, 'hex'),
                                    'namehash', name_surfaces.namehash,
                                    'labelhashes', to_jsonb(name_surfaces.labelhashes),
                                    'normalization_errors', name_surfaces.normalization_errors,
                                    'chain_id', name_surfaces.chain_id,
                                    'block_hash', name_surfaces.block_hash,
                                    'block_number', name_surfaces.block_number,
                                    'provenance', name_surfaces.provenance,
                                    'canonicality_state', name_surfaces.canonicality_state
                                )
                            END,
                        'mismatch', jsonb_build_object(
                            'missing_existing', name_surfaces.logical_name_id IS NULL,
                            'namespace', name_surfaces.namespace IS DISTINCT FROM input_rows.namespace,
                            'normalized_name', name_surfaces.normalized_name IS DISTINCT FROM input_rows.normalized_name,
                            'dns_encoded_name', name_surfaces.dns_encoded_name IS DISTINCT FROM input_rows.dns_encoded_name,
                            'namehash', name_surfaces.namehash IS DISTINCT FROM input_rows.namehash,
                            'labelhashes', name_surfaces.labelhashes IS DISTINCT FROM ARRAY(SELECT jsonb_array_elements_text(input_rows.labelhashes::jsonb)),
                            'normalization_errors', name_surfaces.normalization_errors IS DISTINCT FROM input_rows.normalization_errors::jsonb,
                            'canonicality_state', name_surfaces.canonicality_state IS DISTINCT FROM {accepted_canonicality_merge}
                        )
                    ) AS sample
                    FROM input_rows
                    LEFT JOIN name_surfaces
                      ON name_surfaces.logical_name_id = input_rows.logical_name_id
                    WHERE
                        name_surfaces.logical_name_id IS NULL
                        OR NOT (
                            name_surfaces.namespace = input_rows.namespace
                            AND name_surfaces.normalized_name = input_rows.normalized_name
                            AND name_surfaces.dns_encoded_name = input_rows.dns_encoded_name
                            AND name_surfaces.namehash = input_rows.namehash
                            AND name_surfaces.labelhashes = ARRAY(SELECT jsonb_array_elements_text(input_rows.labelhashes::jsonb))
                            AND name_surfaces.normalization_errors = input_rows.normalization_errors::jsonb
                            AND name_surfaces.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
                        )
                    ORDER BY input_rows.logical_name_id
                    LIMIT 10
                )
                SELECT COALESCE(jsonb_agg(sample), '[]'::jsonb)
                FROM rejected
                "#,
                accepted_canonicality_merge = accepted_canonicality_merge,
            );
            let rejected_samples = sqlx::query_scalar::<_, serde_json::Value>(&diagnostics_sql)
                .bind(&logical_name_ids)
                .bind(&namespaces)
                .bind(&input_names)
                .bind(&canonical_display_names)
                .bind(&normalized_names)
                .bind(&dns_encoded_names)
                .bind(&namehashes)
                .bind(&labelhashes)
                .bind(&normalizer_versions)
                .bind(&normalization_warnings)
                .bind(&normalization_errors)
                .bind(&chain_ids)
                .bind(&block_hashes)
                .bind(&block_numbers)
                .bind(&provenances)
                .bind(&canonicality_states)
                .fetch_one(&mut **executor)
                .await
                .context("failed to diagnose skipped name-surface upsert rows")?;
            bail!(
                "bulk name-surface upsert skipped {} rows because existing identities or observations were incompatible: {}",
                expected_count.saturating_sub(upserted_ids.len()),
                rejected_samples
            );
        }
    }

    Ok(())
}

pub(super) async fn load_existing_surface_binding_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    bindings: &[SurfaceBinding],
) -> Result<HashSet<Uuid>> {
    let surface_binding_ids = bindings
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<Vec<_>>();
    if surface_binding_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let rows = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT surface_binding_id
        FROM surface_bindings
        WHERE surface_binding_id = ANY($1::UUID[])
        "#,
    )
    .bind(&surface_binding_ids)
    .fetch_all(&mut **executor)
    .await
    .context("failed to load existing surface binding ids for batch upsert")?;

    Ok(rows.into_iter().collect())
}

pub(super) async fn insert_surface_bindings_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    bindings: &[SurfaceBinding],
) -> Result<HashSet<Uuid>> {
    let mut inserted_ids = HashSet::new();
    for chunk in bindings.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut surface_binding_ids = Vec::with_capacity(chunk.len());
        let mut logical_name_ids = Vec::with_capacity(chunk.len());
        let mut resource_ids = Vec::with_capacity(chunk.len());
        let mut binding_kinds = Vec::with_capacity(chunk.len());
        let mut active_froms = Vec::with_capacity(chunk.len());
        let mut active_tos = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for binding in chunk {
            surface_binding_ids.push(binding.surface_binding_id);
            logical_name_ids.push(binding.logical_name_id.clone());
            resource_ids.push(binding.resource_id);
            binding_kinds.push(binding.binding_kind.as_str().to_owned());
            active_froms.push(binding.active_from);
            active_tos.push(binding.active_to);
            chain_ids.push(binding.chain_id.clone());
            block_hashes.push(binding.block_hash.clone());
            block_numbers.push(binding.block_number);
            provenances.push(
                serde_json::to_string(&binding.provenance)
                    .context("failed to serialize surface-binding provenance")?,
            );
            canonicality_states.push(binding.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO surface_bindings (
                surface_binding_id,
                logical_name_id,
                resource_id,
                binding_kind,
                active_from,
                active_to,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                surface_binding_id,
                logical_name_id,
                resource_id,
                binding_kind,
                active_from,
                active_to,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::UUID[],
                $2::TEXT[],
                $3::UUID[],
                $4::TEXT[],
                $5::TIMESTAMPTZ[],
                $6::TIMESTAMPTZ[],
                $7::TEXT[],
                $8::TEXT[],
                $9::BIGINT[],
                $10::TEXT[],
                $11::TEXT[]
            ) AS input(
                surface_binding_id,
                logical_name_id,
                resource_id,
                binding_kind,
                active_from,
                active_to,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            ON CONFLICT (surface_binding_id) DO NOTHING
            RETURNING surface_binding_id
            "#,
        )
        .bind(&surface_binding_ids)
        .bind(&logical_name_ids)
        .bind(&resource_ids)
        .bind(&binding_kinds)
        .bind(&active_froms)
        .bind(&active_tos)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert surface bindings")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}

pub(super) async fn bulk_upsert_surface_bindings_without_snapshots(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    bindings: &[SurfaceBinding],
) -> Result<()> {
    for chunk in bindings.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut surface_binding_ids = Vec::with_capacity(chunk.len());
        let mut logical_name_ids = Vec::with_capacity(chunk.len());
        let mut resource_ids = Vec::with_capacity(chunk.len());
        let mut binding_kinds = Vec::with_capacity(chunk.len());
        let mut active_froms = Vec::with_capacity(chunk.len());
        let mut active_tos = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for binding in chunk {
            surface_binding_ids.push(binding.surface_binding_id);
            logical_name_ids.push(binding.logical_name_id.clone());
            resource_ids.push(binding.resource_id);
            binding_kinds.push(binding.binding_kind.as_str().to_owned());
            active_froms.push(binding.active_from);
            active_tos.push(binding.active_to);
            chain_ids.push(binding.chain_id.clone());
            block_hashes.push(binding.block_hash.clone());
            block_numbers.push(binding.block_number);
            provenances.push(
                serde_json::to_string(&binding.provenance)
                    .context("failed to serialize surface-binding provenance")?,
            );
            canonicality_states.push(binding.canonicality_state.as_str().to_owned());
        }

        let expected_count = unique_uuid_count(surface_binding_ids.iter().copied());
        let active_to_merge = surface_binding_active_to_merge_sql("surface_bindings", "EXCLUDED");
        let accepted_active_to_merge =
            surface_binding_active_to_merge_sql("surface_bindings", "input_rows");
        let canonicality_merge = canonicality_merge_sql("surface_bindings");
        let accepted_canonicality_merge = canonicality_merge_sql_from(
            "surface_bindings",
            "input_rows.canonicality_state::canonicality_state",
        );
        let sql = format!(
            r#"
            WITH input_rows AS (
                SELECT DISTINCT ON (surface_binding_id)
                    surface_binding_id,
                    logical_name_id,
                    resource_id,
                    binding_kind,
                    active_from,
                    active_to,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state
                FROM unnest(
                    $1::UUID[],
                    $2::TEXT[],
                    $3::UUID[],
                    $4::TEXT[],
                    $5::TIMESTAMPTZ[],
                    $6::TIMESTAMPTZ[],
                    $7::TEXT[],
                    $8::TEXT[],
                    $9::BIGINT[],
                    $10::TEXT[],
                    $11::TEXT[]
                ) WITH ORDINALITY AS input(
                    surface_binding_id,
                    logical_name_id,
                    resource_id,
                    binding_kind,
                    active_from,
                    active_to,
                    chain_id,
                    block_hash,
                    block_number,
                    provenance,
                    canonicality_state,
                    ordinality
                )
                ORDER BY surface_binding_id, ordinality DESC
            ),
            upserted AS (
            INSERT INTO surface_bindings (
                surface_binding_id,
                logical_name_id,
                resource_id,
                binding_kind,
                active_from,
                active_to,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                surface_binding_id,
                logical_name_id,
                resource_id,
                binding_kind,
                active_from,
                active_to,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM input_rows
            ON CONFLICT (surface_binding_id) DO UPDATE
            SET
                active_to = {active_to_merge},
                canonicality_state = {canonicality_merge},
                observed_at = now()
            WHERE
                surface_bindings.logical_name_id = EXCLUDED.logical_name_id
                AND surface_bindings.resource_id = EXCLUDED.resource_id
                AND surface_bindings.binding_kind = EXCLUDED.binding_kind
                AND surface_bindings.active_from = EXCLUDED.active_from
                AND surface_bindings.chain_id = EXCLUDED.chain_id
                AND surface_bindings.block_hash = EXCLUDED.block_hash
                AND surface_bindings.block_number = EXCLUDED.block_number
                AND surface_bindings.provenance = EXCLUDED.provenance
                AND (
                    surface_bindings.active_to IS DISTINCT FROM {active_to_merge}
                    OR surface_bindings.canonicality_state IS DISTINCT FROM {canonicality_merge}
                )
            RETURNING surface_binding_id
            ),
            accepted_existing AS (
                SELECT input_rows.surface_binding_id
                FROM input_rows
                JOIN surface_bindings
                  ON surface_bindings.surface_binding_id = input_rows.surface_binding_id
                WHERE
                    surface_bindings.logical_name_id = input_rows.logical_name_id
                    AND surface_bindings.resource_id = input_rows.resource_id
                    AND surface_bindings.binding_kind = input_rows.binding_kind
                    AND surface_bindings.active_from = input_rows.active_from
                    AND surface_bindings.chain_id = input_rows.chain_id
                    AND surface_bindings.block_hash = input_rows.block_hash
                    AND surface_bindings.block_number = input_rows.block_number
                    AND surface_bindings.provenance = input_rows.provenance::jsonb
                    AND surface_bindings.active_to IS NOT DISTINCT FROM {accepted_active_to_merge}
                    AND surface_bindings.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
            )
            SELECT surface_binding_id FROM upserted
            UNION ALL
            SELECT surface_binding_id FROM accepted_existing
            "#,
            active_to_merge = active_to_merge,
            accepted_active_to_merge = accepted_active_to_merge,
            canonicality_merge = canonicality_merge,
            accepted_canonicality_merge = accepted_canonicality_merge,
        );

        let upserted_ids = sqlx::query_scalar::<_, Uuid>(&sql)
            .bind(&surface_binding_ids)
            .bind(&logical_name_ids)
            .bind(&resource_ids)
            .bind(&binding_kinds)
            .bind(&active_froms)
            .bind(&active_tos)
            .bind(&chain_ids)
            .bind(&block_hashes)
            .bind(&block_numbers)
            .bind(&provenances)
            .bind(&canonicality_states)
            .fetch_all(&mut **executor)
            .await
            .context("failed to bulk upsert surface bindings without snapshots")?;

        if upserted_ids.len() != expected_count {
            bail!(
                "bulk surface-binding upsert skipped {} rows because existing identities were incompatible",
                expected_count.saturating_sub(upserted_ids.len())
            );
        }
    }

    Ok(())
}
