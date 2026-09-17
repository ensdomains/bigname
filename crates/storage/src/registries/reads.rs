use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, split_keyset_page,
};

use super::types::{
    RegistryContractRow, RegistryCreation, RegistryCreationBasis, RegistryReferenceKeysetCursor,
    RegistryReferencePage, SubregistryPointer,
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

const READABLE_STATES: &str = r#"(
    'canonical'::bigname_phase.canonicality_state,
    'safe'::bigname_phase.canonicality_state,
    'finalized'::bigname_phase.canonicality_state
)"#;

/// Predicates shared by every ENSv2 subregistry-pointer read. They match the partial GIN
/// index over the pointer's before/after addresses, so the address-anchored reads below stay
/// indexed.
fn pointer_event_predicates(alias: &str) -> String {
    format!(
        r#"
        AND {alias}.event_kind = 'SubregistryChanged'
        AND {alias}.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
        AND {alias}.consumer_visibility = 'activated'
        AND {alias}.canonicality_state IN {READABLE_STATES}
        AND {alias}.logical_name_id IS NOT NULL
        "#
    )
}

fn lineage_readable(alias: &str) -> String {
    format!(" AND ({alias}.block_hash IS NULL OR rb.canonicality_state IN {READABLE_STATES}) ")
}

const POINTER_SELECT: &str = r#"
    SELECT pointer.logical_name_id, surface.namespace, surface.raw_name AS display_name,
           surface.namehash, pointer.chain_id, pointer.subregistry, pointer.registry,
           pointer.block_number, pointer.block_hash, pointer.transaction_hash,
           pointer.block_timestamp
"#;

/// Loads one known registry contract with its first-observation evidence. A registry is known
/// when it announced itself with `RegistryCreated`, was ever the target of an ENSv2
/// `SubregistryUpdated` pointer, or is a manifest-declared `root_registry`/`registry` contract.
/// `as_of_block` bounds events and inclusive declaration intervals to a served position.
pub async fn load_registry_contract(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    as_of_block: Option<i64>,
) -> Result<Option<RegistryContractRow>> {
    let address = address.to_ascii_lowercase();
    let rows = sqlx::query(&format!(
        r#"
        WITH announcement AS (
            SELECT ne.block_number, ne.block_hash, ne.transaction_hash, rb.block_timestamp
            FROM bigname_phase.normalized_events ne
            JOIN bigname_phase.chain_lineage rb
              ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash
            WHERE ne.chain_id = $1
              AND lower(ne.raw_fact_ref ->> 'emitting_address') = $2
              AND ne.event_kind = 'RegistryCreated'
              AND ne.consumer_visibility = 'activated'
              AND ne.canonicality_state IN {READABLE_STATES}
              AND rb.canonicality_state IN {READABLE_STATES}
              AND ($3::bigint IS NULL OR ne.block_number <= $3)
            ORDER BY ne.block_number, ne.transaction_index NULLS LAST, ne.log_index NULLS LAST,
                     ne.event_identity
            LIMIT 1
        ), pointer AS (
            SELECT ne.block_number, ne.block_hash, ne.transaction_hash, rb.block_timestamp
            FROM bigname_phase.normalized_events ne
            JOIN bigname_phase.chain_lineage rb
              ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash
            WHERE ne.chain_id = $1
              {pointer_predicates}
              AND ARRAY[lower(ne.after_state ->> 'subregistry'),
                        lower(ne.before_state ->> 'subregistry')] @> ARRAY[$2::text]
              AND lower(ne.after_state ->> 'subregistry') = $2
              AND rb.canonicality_state IN {READABLE_STATES}
              AND ($3::bigint IS NULL OR ne.block_number <= $3)
            ORDER BY ne.block_number, ne.transaction_index NULLS LAST, ne.log_index NULLS LAST,
                     ne.event_identity
            LIMIT 1
        ), declared AS (
            SELECT address.active_from_block_number AS block_number,
                   (
                       SELECT lineage.block_timestamp
                       FROM bigname_phase.chain_lineage lineage
                       WHERE lineage.chain_id = address.chain_id
                         AND lineage.block_number = address.active_from_block_number
                         AND lineage.canonicality_state IN {READABLE_STATES}
                       ORDER BY lineage.block_hash
                       LIMIT 1
                   ) AS block_timestamp
            FROM bigname_phase.contract_instance_addresses address
            WHERE address.chain_id = $1
              AND lower(address.address) = $2
              AND (address.deactivated_at IS NULL OR address.active_to_block_number IS NOT NULL)
              AND ($3::bigint IS NULL AND address.deactivated_at IS NULL OR $3 IS NOT NULL
                   AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $3)
                   AND (address.active_to_block_number IS NULL OR address.active_to_block_number >= $3))
              AND (
                  EXISTS (
                      SELECT 1
                      FROM bigname_phase.manifest_contract_instances declaration
                      JOIN bigname_phase.manifest_versions manifest
                        ON manifest.manifest_id = declaration.manifest_id
                       AND manifest.chain_id = declaration.chain_id
                      WHERE declaration.chain_id = address.chain_id
                        AND declaration.contract_instance_id = address.contract_instance_id
                        AND lower(declaration.declared_address) = lower(address.address)
                        AND (address.source_manifest_id IS NULL OR
                             address.source_manifest_id = declaration.manifest_id)
                        AND (address.deactivated_at IS NULL OR address.source_manifest_id IS NULL)
                        AND declaration.role IN ('root_registry', 'registry')
                        AND manifest.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                  ) OR EXISTS (
                      -- Sync replaces current declaration children. Its retained active payloads
                      -- classify finite retired intervals without letting a later re-admission
                      -- assign its role to an older interval of the same address.
                      SELECT 1
                      FROM bigname_phase.normalized_events event
                      CROSS JOIN LATERAL (VALUES (event.before_state), (event.after_state)) state(value)
                      CROSS JOIN LATERAL jsonb_array_elements(
                          state.value -> 'manifest_payload' -> 'contracts'
                      ) contract(value)
                      WHERE address.deactivated_at IS NOT NULL
                        AND address.provenance ->> 'source' = 'manifest_declaration'
                        AND lower(address.provenance ->> 'declared_address') = lower(address.address)
                        AND event.source_manifest_id = address.source_manifest_id
                        AND event.chain_id = address.chain_id
                        AND event.observed_at BETWEEN address.admitted_at AND address.deactivated_at
                        AND event.event_kind = 'SourceManifestUpdated'
                        AND event.derivation_kind = 'manifest_sync'
                        AND event.consumer_visibility = 'activated'
                        AND event.canonicality_state IN {READABLE_STATES}
                        AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                        AND state.value ->> 'rollout_status' = 'active'
                        AND state.value -> 'manifest_payload' ->> 'chain' = address.chain_id
                        AND state.value -> 'manifest_payload' ->> 'source_family' = event.source_family
                        AND lower(contract.value ->> 'address') = lower(address.address)
                        AND contract.value ->> 'role' IN ('root_registry', 'registry')
                        AND (
                            (address.provenance ->> 'declaration_kind' = 'contract'
                             AND address.provenance ->> 'declaration_name' = contract.value ->> 'role')
                            OR (address.provenance ->> 'declaration_kind' = 'root' AND EXISTS (
                                SELECT 1 FROM jsonb_array_elements(
                                    state.value -> 'manifest_payload' -> 'roots'
                                ) root(value)
                                WHERE root.value ->> 'name' = address.provenance ->> 'declaration_name'
                                  AND lower(root.value ->> 'address') = lower(address.address)
                            ))
                        )
                  )
              )
            ORDER BY address.active_from_block_number NULLS FIRST
            LIMIT 1
        )
        SELECT 'announcement' AS basis, block_number, block_hash, transaction_hash, block_timestamp
        FROM announcement
        UNION ALL
        SELECT 'subregistry_pointer', block_number, block_hash, transaction_hash, block_timestamp
        FROM pointer
        UNION ALL
        SELECT 'declared', block_number, NULL::text, NULL::text, block_timestamp
        FROM declared
        "#,
        pointer_predicates = pointer_event_predicates("ne"),
    ))
    .bind(chain_id)
    .bind(&address)
    .bind(as_of_block)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load registry contract {chain_id}:{address}"))?;

    let mut creations = rows
        .into_iter()
        .map(decode_registry_creation)
        .collect::<Result<Vec<_>>>()?;
    creations.sort_by_key(|creation| match creation.basis {
        RegistryCreationBasis::Announcement => 0,
        RegistryCreationBasis::SubregistryPointer => 1,
        RegistryCreationBasis::Declared => 2,
    });
    Ok(creations
        .into_iter()
        .next()
        .map(|created| RegistryContractRow {
            chain_id: chain_id.to_owned(),
            address,
            created,
        }))
}

/// Loads the current ENSv2 subregistry pointer for each requested name. Names without any
/// pointer event are omitted; a name whose latest pointer was cleared is returned with
/// `subregistry = None`. `as_of_block` bounds the pointer events to a served position.
pub async fn load_subregistry_pointers_for_names(
    pool: &PgPool,
    logical_name_ids: &[String],
    as_of_block: Option<i64>,
) -> Result<BTreeMap<String, SubregistryPointer>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(&format!(
        r#"
        WITH pointer AS (
            SELECT DISTINCT ON (ne.logical_name_id)
                   ne.logical_name_id, ne.chain_id,
                   lower(ne.after_state ->> 'subregistry') AS subregistry,
                   lower(ne.raw_fact_ref ->> 'emitting_address') AS registry,
                   ne.block_number, ne.block_hash, ne.transaction_hash, rb.block_timestamp
            FROM bigname_phase.normalized_events ne
            LEFT JOIN bigname_phase.chain_lineage rb
              ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash
            WHERE ne.logical_name_id = ANY($1::text[])
              {pointer_predicates}
              {lineage_readable}
              AND ($2::bigint IS NULL OR ne.block_number <= $2)
            ORDER BY ne.logical_name_id, ne.block_number DESC NULLS LAST,
                     ne.transaction_index DESC NULLS LAST, ne.log_index DESC NULLS LAST,
                     ne.event_identity DESC
        )
        {POINTER_SELECT}
        FROM pointer
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = pointer.logical_name_id
        "#,
        pointer_predicates = pointer_event_predicates("ne"),
        lineage_readable = lineage_readable("ne"),
    ))
    .bind(logical_name_ids)
    .bind(as_of_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load subregistry pointers for {} names",
            logical_name_ids.len()
        )
    })?;
    rows.into_iter()
        .map(|row| {
            let pointer = decode_subregistry_pointer(row)?;
            Ok((pointer.logical_name_id.clone(), pointer))
        })
        .collect()
}

/// Loads the name that a registry serves: the earliest-pointing name whose current pointer
/// targets `address`. `None` when no current pointer targets the registry (the root
/// registry, or a registry that was announced but never linked).
pub async fn load_registry_serving_pointer(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    as_of_block: Option<i64>,
) -> Result<Option<SubregistryPointer>> {
    let address = address.to_ascii_lowercase();
    let mut builder = current_pointers_to_registry(chain_id, &address, as_of_block);
    builder.push(
        " ORDER BY pointer.block_number NULLS LAST, pointer.transaction_index NULLS LAST, \
         pointer.log_index NULLS LAST, pointer.event_identity LIMIT 1",
    );
    let row = builder
        .build()
        .fetch_optional(pool)
        .await
        .with_context(|| {
            format!("failed to load serving name for registry {chain_id}:{address}")
        })?;
    row.map(decode_subregistry_pointer).transpose()
}

/// Loads one keyset page of names whose current subregistry pointer targets `address`,
/// ordered by display name then logical identity.
pub async fn load_registry_references_page(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    as_of_block: Option<i64>,
    cursor: Option<&RegistryReferenceKeysetCursor>,
    page_size: u64,
) -> Result<RegistryReferencePage> {
    let address = address.to_ascii_lowercase();
    let limit = checked_page_limit_i64(
        page_size,
        "registry references page_size must be positive",
        "registry references page_size is too large",
    )?;
    let page_size = checked_page_size_usize(
        page_size,
        "registry references page_size must be positive",
        "registry references page_size does not fit in usize",
    )?;
    let mut builder = current_pointers_to_registry(chain_id, &address, as_of_block);
    if let Some(cursor) = cursor {
        builder.push(" AND (surface.raw_name, pointer.logical_name_id) > (");
        builder.push_bind(cursor.display_name.clone());
        builder.push(", ");
        builder.push_bind(cursor.logical_name_id.clone());
        builder.push(")");
    }
    builder.push(" ORDER BY surface.raw_name, pointer.logical_name_id LIMIT ");
    builder.push_bind(limit);
    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load names referencing registry {chain_id}:{address}"))?
        .into_iter()
        .map(decode_subregistry_pointer)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, |row| {
        RegistryReferenceKeysetCursor::from(row)
    });
    Ok(RegistryReferencePage { rows, next_cursor })
}

/// Counts readable events of the given kinds emitted by one contract.
pub async fn count_contract_events(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    event_kinds: &[String],
    as_of_block: Option<i64>,
) -> Result<i64> {
    let address = address.to_ascii_lowercase();
    sqlx::query_scalar::<_, i64>(&format!(
        r#"
        SELECT count(*)::bigint
        FROM bigname_phase.normalized_events ne
        LEFT JOIN bigname_phase.chain_lineage rb
          ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash
        WHERE ne.chain_id = $1
          AND lower(ne.raw_fact_ref ->> 'emitting_address') = $2
          AND ne.event_kind = ANY($3::text[])
          AND ne.consumer_visibility = 'activated'
          AND ne.canonicality_state IN {READABLE_STATES}
          {lineage_readable}
          AND ($4::bigint IS NULL OR ne.block_number <= $4)
        "#,
        lineage_readable = lineage_readable("ne"),
    ))
    .bind(chain_id)
    .bind(&address)
    .bind(event_kinds)
    .bind(as_of_block)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to count events emitted by {chain_id}:{address}"))
}

/// Current pointers (latest per name) whose target is `address`, joined to the active name
/// surface. Callers append ordering and paging.
fn current_pointers_to_registry<'a>(
    chain_id: &'a str,
    address: &'a str,
    as_of_block: Option<i64>,
) -> QueryBuilder<'a, Postgres> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        WITH candidate AS (
            SELECT DISTINCT ne.logical_name_id
            FROM bigname_phase.normalized_events ne
            WHERE ne.chain_id = "#,
    );
    builder.push_bind(chain_id);
    builder.push(pointer_event_predicates("ne"));
    builder.push(
        " AND ARRAY[lower(ne.after_state ->> 'subregistry'), \
         lower(ne.before_state ->> 'subregistry')] @> ARRAY[",
    );
    builder.push_bind(address);
    builder.push(
        r#"::text]
        ), pointer AS (
            SELECT DISTINCT ON (ne.logical_name_id)
                   ne.logical_name_id, ne.chain_id,
                   lower(ne.after_state ->> 'subregistry') AS subregistry,
                   lower(ne.raw_fact_ref ->> 'emitting_address') AS registry,
                   ne.block_number, ne.block_hash, ne.transaction_hash, ne.transaction_index,
                   ne.log_index, ne.event_identity, rb.block_timestamp
            FROM candidate
            JOIN bigname_phase.normalized_events ne
              ON ne.logical_name_id = candidate.logical_name_id
            LEFT JOIN bigname_phase.chain_lineage rb
              ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash
            WHERE ne.chain_id = "#,
    );
    builder.push_bind(chain_id);
    builder.push(pointer_event_predicates("ne"));
    builder.push(lineage_readable("ne"));
    builder.push(" AND (");
    builder.push_bind(as_of_block);
    builder.push("::bigint IS NULL OR ne.block_number <= ");
    builder.push_bind(as_of_block);
    builder.push(
        r#")
            ORDER BY ne.logical_name_id, ne.block_number DESC NULLS LAST,
                     ne.transaction_index DESC NULLS LAST, ne.log_index DESC NULLS LAST,
                     ne.event_identity DESC
        )
        "#,
    );
    builder.push(POINTER_SELECT);
    builder.push(
        r#"
        FROM pointer
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = pointer.logical_name_id
         AND surface.visibility_state = 'active'
        WHERE pointer.subregistry = "#,
    );
    builder.push_bind(address);
    builder
}

fn decode_registry_creation(row: PgRow) -> Result<RegistryCreation> {
    let basis: String = crate::sql_row::get(&row, "basis")?;
    let basis = match basis.as_str() {
        "announcement" => RegistryCreationBasis::Announcement,
        "subregistry_pointer" => RegistryCreationBasis::SubregistryPointer,
        "declared" => RegistryCreationBasis::Declared,
        other => anyhow::bail!("unknown registry creation basis {other}"),
    };
    Ok(RegistryCreation {
        basis,
        block_number: crate::sql_row::get(&row, "block_number")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        block_timestamp: crate::sql_row::get(&row, "block_timestamp")?,
    })
}

fn decode_subregistry_pointer(row: PgRow) -> Result<SubregistryPointer> {
    let subregistry: Option<String> = row.try_get("subregistry")?;
    Ok(SubregistryPointer {
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        display_name: crate::sql_row::get(&row, "display_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        subregistry: subregistry.filter(|value| !value.is_empty() && value != ZERO_ADDRESS),
        registry: crate::sql_row::get(&row, "registry")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        block_timestamp: crate::sql_row::get(&row, "block_timestamp")?,
    })
}
