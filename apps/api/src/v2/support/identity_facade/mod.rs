use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    IdentityNameRecordRow, IdentityPrimaryNameSnapshot, PrimaryNameClaimStatus,
    ReverseIdentityGroup, ReverseIdentityRecordRow, ReverseIdentityRoles,
    ReverseIdentityStorageInput,
};
use sqlx::{PgPool, Row};

mod page;

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;

    static COUNT_CALLS: ScopedTestHookRegistry<String, Arc<AtomicUsize>> =
        ScopedTestHookRegistry::new();

    pub(crate) struct CountCallControl(Arc<AtomicUsize>);

    impl CountCallControl {
        pub(crate) fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, Arc<AtomicUsize>>,
        CountCallControl,
    )> {
        let database = current_test_database(pool).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = COUNT_CALLS.install(database, Arc::clone(&calls));
        Ok((guard, CountCallControl(calls)))
    }

    pub(super) async fn record(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(calls) = COUNT_CALLS.get_cloned(&database) {
            calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND surface_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND resource_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.active_to IS NULL
  AND (
      anc.token_lineage_id IS NULL
      OR (
          token_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND token_lineage_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
      )
  )
"#;

const DEFAULT_ADDRESS_NAMES_CURRENT_LINEAGE_JOINS: &str = r#"
  JOIN chain_lineage surface_lineage
    ON surface_lineage.chain_id = surface.chain_id
   AND surface_lineage.block_hash = surface.block_hash
  JOIN chain_lineage resource_lineage
    ON resource_lineage.chain_id = resource.chain_id
   AND resource_lineage.block_hash = resource.block_hash
  JOIN chain_lineage binding_lineage
    ON binding_lineage.chain_id = binding.chain_id
   AND binding_lineage.block_hash = binding.block_hash
  LEFT JOIN chain_lineage token_lineage_lineage
    ON token_lineage_lineage.chain_id = token_lineage.chain_id
   AND token_lineage_lineage.block_hash = token_lineage.block_hash
"#;

const DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER: &str = r#"
  AND identity_nc_surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND identity_nc_surface_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND (
      identity_nc.surface_binding_id IS NULL
      OR (
          identity_nc_resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_resource_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_binding.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_binding_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_binding.active_to IS NULL
          AND (
              identity_nc.token_lineage_id IS NULL
              OR (
                  identity_nc_token_lineage.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND identity_nc_token_lineage_lineage.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
              )
          )
      )
  )
"#;

const DEFAULT_IDENTITY_NAME_CURRENT_LINEAGE_JOINS: &str = r#"
  JOIN chain_lineage identity_nc_surface_lineage
    ON identity_nc_surface_lineage.chain_id = identity_nc_surface.chain_id
   AND identity_nc_surface_lineage.block_hash = identity_nc_surface.block_hash
  LEFT JOIN chain_lineage identity_nc_resource_lineage
    ON identity_nc_resource_lineage.chain_id = identity_nc_resource.chain_id
   AND identity_nc_resource_lineage.block_hash = identity_nc_resource.block_hash
  LEFT JOIN chain_lineage identity_nc_binding_lineage
    ON identity_nc_binding_lineage.chain_id = identity_nc_binding.chain_id
   AND identity_nc_binding_lineage.block_hash = identity_nc_binding.block_hash
  LEFT JOIN chain_lineage identity_nc_token_lineage_lineage
    ON identity_nc_token_lineage_lineage.chain_id = identity_nc_token_lineage.chain_id
   AND identity_nc_token_lineage_lineage.block_hash = identity_nc_token_lineage.block_hash
"#;

#[derive(Clone)]
struct ReverseIdentityPageRow {
    input_index: usize,
    logical_name_id: String,
}

pub(crate) async fn load_reverse_identity_records_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<ReverseIdentityGroup>> {
    load_reverse_identity_records_live_with_count_mode(pool, inputs, ReverseCountMode::Include)
        .await
}

pub(crate) async fn load_reverse_identity_records_page_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<ReverseIdentityGroup>> {
    load_reverse_identity_records_live_with_count_mode(pool, inputs, ReverseCountMode::Omit).await
}

#[derive(Clone, Copy)]
enum ReverseCountMode {
    Include,
    Omit,
}

async fn load_reverse_identity_records_live_with_count_mode(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    count_mode: ReverseCountMode,
) -> Result<Vec<ReverseIdentityGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    let first_page_feed = inputs
        .iter()
        .all(|input| input.page_size == 1 && input.cursor.is_none());
    let page_records_future = async {
        let page_rows = page::load_reverse_identity_page_rows(pool, inputs).await?;
        let logical_name_ids =
            dedupe_in_order(page_rows.iter().map(|row| row.logical_name_id.clone()));
        let name_records = bigname_storage::load_identity_records_by_names(pool, &logical_name_ids)
            .await?
            .into_iter()
            .map(|record| (record.row.logical_name_id.clone(), record))
            .collect::<BTreeMap<_, _>>();
        Result::<_>::Ok((page_rows, name_records))
    };

    let total_counts_future = async {
        match count_mode {
            ReverseCountMode::Include => load_reverse_identity_total_counts_live(pool, inputs)
                .await
                .map(Some),
            ReverseCountMode::Omit => Ok(None),
        }
    };
    let ((page_rows, name_records), primary_names, total_counts) = tokio::try_join!(
        page_records_future,
        load_identity_primary_name_snapshots(pool, inputs),
        total_counts_future,
    )?;

    let rows_by_input = page_rows.into_iter().fold(
        BTreeMap::<usize, Vec<ReverseIdentityPageRow>>::new(),
        |mut grouped, row| {
            grouped.entry(row.input_index).or_default().push(row);
            grouped
        },
    );

    Ok(inputs
        .iter()
        .enumerate()
        .map(|(input_index, input)| {
            let mut entries = rows_by_input
                .get(&input_index)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|row| {
                    reverse_identity_record(&name_records, &primary_names, input, row)
                })
                .collect::<Vec<_>>();
            let total_count = total_counts.as_ref().map(|counts| {
                *counts
                    .get(&(input.address.clone(), input.roles))
                    .unwrap_or(&0)
            });
            let has_more = match (first_page_feed, total_count) {
                (true, Some(total_count)) => {
                    total_count > input.page_size.max(0) as u64 && !entries.is_empty()
                }
                _ => entries.len() as i64 > input.page_size,
            };
            entries.truncate(input.page_size.max(0) as usize);

            ReverseIdentityGroup {
                input: input.clone(),
                entries,
                total_count,
                has_more,
            }
        })
        .collect())
}

fn reverse_identity_record(
    name_records: &BTreeMap<String, IdentityNameRecordRow>,
    primary_names: &BTreeMap<(String, String, String), IdentityPrimaryNameSnapshot>,
    input: &ReverseIdentityStorageInput,
    row: ReverseIdentityPageRow,
) -> Option<ReverseIdentityRecordRow> {
    let name_record = name_records.get(&row.logical_name_id)?.clone();
    let primary_name = primary_names
        .get(&(
            input.address.clone(),
            name_record.row.namespace.clone(),
            input.coin_type.clone(),
        ))
        .cloned();
    let mut relation_facets = name_record
        .relations
        .iter()
        .filter(|relation| {
            relation.address == input.address && input.roles.includes(relation.relation)
        })
        .map(|relation| relation.relation)
        .collect::<Vec<_>>();
    relation_facets.sort();
    relation_facets.dedup();

    Some(ReverseIdentityRecordRow {
        name_record,
        relation_facets,
        primary_name,
        requested_coin_type: input.coin_type.clone(),
    })
}

async fn load_identity_primary_name_snapshots(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<BTreeMap<(String, String, String), IdentityPrimaryNameSnapshot>> {
    let addresses = dedupe_in_order(inputs.iter().map(|input| input.address.clone()));
    let coin_types = dedupe_in_order(inputs.iter().map(|input| input.coin_type.clone()));
    if addresses.is_empty() || coin_types.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT address, namespace, coin_type, claim_status, normalized_claim_name
        FROM primary_names_current
        WHERE address = ANY($1::TEXT[])
          AND coin_type = ANY($2::TEXT[])
        ORDER BY address, namespace, coin_type
        "#,
    )
    .bind(&addresses)
    .bind(&coin_types)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to batch load primary_names_current snapshots for {} addresses and {} coin types",
            addresses.len(),
            coin_types.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let address = row.try_get::<String, _>("address")?.to_ascii_lowercase();
            let namespace = row.try_get::<String, _>("namespace")?;
            let coin_type = row.try_get::<String, _>("coin_type")?;
            let claim_status =
                parse_primary_name_claim_status(&row.try_get::<String, _>("claim_status")?)?;
            let snapshot = IdentityPrimaryNameSnapshot {
                address: address.clone(),
                namespace: namespace.clone(),
                coin_type: coin_type.clone(),
                claim_status,
                normalized_claim_name: row.try_get("normalized_claim_name")?,
            };
            Ok(((address, namespace, coin_type), snapshot))
        })
        .collect()
}

async fn load_reverse_identity_total_counts_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<BTreeMap<(String, ReverseIdentityRoles), u64>> {
    #[cfg(test)]
    test_hooks::record(pool).await?;

    let requests = inputs
        .iter()
        .map(|input| (input.address.clone(), input.roles))
        .collect::<BTreeSet<_>>();
    let addresses = requests
        .iter()
        .map(|(address, _)| address.clone())
        .collect::<Vec<_>>();
    let roles = requests
        .iter()
        .map(|(_, roles)| roles_storage_value(*roles).to_owned())
        .collect::<Vec<_>>();

    let rows = sqlx::query(&format!(
        r#"
        WITH requested AS (
            SELECT *
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS requested(address, roles)
        )
        SELECT
            requested.address,
            requested.roles,
            COUNT(DISTINCT anc.logical_name_id)::BIGINT AS total_count
        FROM requested
        LEFT JOIN address_names_current anc
          ON anc.address = requested.address
         AND (
             requested.roles = 'both'
             OR (requested.roles = 'owned' AND anc.relation IN ('registrant', 'token_holder'))
             OR (requested.roles = 'managed' AND anc.relation = 'effective_controller')
         )
        LEFT JOIN name_surfaces surface
          ON surface.logical_name_id = anc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = anc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = anc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = anc.token_lineage_id
        LEFT JOIN chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
        LEFT JOIN chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
        LEFT JOIN chain_lineage binding_lineage
          ON binding_lineage.chain_id = binding.chain_id
         AND binding_lineage.block_hash = binding.block_hash
        LEFT JOIN chain_lineage token_lineage_lineage
          ON token_lineage_lineage.chain_id = token_lineage.chain_id
         AND token_lineage_lineage.block_hash = token_lineage.block_hash
        LEFT JOIN name_current identity_nc
          ON identity_nc.logical_name_id = anc.logical_name_id
        LEFT JOIN name_surfaces identity_nc_surface
          ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
        LEFT JOIN resources identity_nc_resource
          ON identity_nc_resource.resource_id = identity_nc.resource_id
        LEFT JOIN surface_bindings identity_nc_binding
          ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
        LEFT JOIN token_lineages identity_nc_token_lineage
          ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
        LEFT JOIN chain_lineage identity_nc_surface_lineage
          ON identity_nc_surface_lineage.chain_id = identity_nc_surface.chain_id
         AND identity_nc_surface_lineage.block_hash = identity_nc_surface.block_hash
        LEFT JOIN chain_lineage identity_nc_resource_lineage
          ON identity_nc_resource_lineage.chain_id = identity_nc_resource.chain_id
         AND identity_nc_resource_lineage.block_hash = identity_nc_resource.block_hash
        LEFT JOIN chain_lineage identity_nc_binding_lineage
          ON identity_nc_binding_lineage.chain_id = identity_nc_binding.chain_id
         AND identity_nc_binding_lineage.block_hash = identity_nc_binding.block_hash
        LEFT JOIN chain_lineage identity_nc_token_lineage_lineage
          ON identity_nc_token_lineage_lineage.chain_id = identity_nc_token_lineage.chain_id
         AND identity_nc_token_lineage_lineage.block_hash = identity_nc_token_lineage.block_hash
        WHERE anc.logical_name_id IS NULL
           OR (TRUE {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER} {DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER})
        GROUP BY requested.address, requested.roles
        ORDER BY requested.address, requested.roles
        "#,
    ))
    .bind(&addresses)
    .bind(&roles)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to live-count reverse identity rows for {} inputs",
            inputs.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let address = row.try_get::<String, _>("address")?;
            let roles = parse_roles(&row.try_get::<String, _>("roles")?)?;
            let total_count = row.try_get::<i64, _>("total_count")?;
            Ok(((address, roles), u64::try_from(total_count).unwrap_or(0)))
        })
        .collect()
}

fn parse_primary_name_claim_status(value: &str) -> Result<PrimaryNameClaimStatus> {
    match value {
        "success" => Ok(PrimaryNameClaimStatus::Success),
        "not_found" => Ok(PrimaryNameClaimStatus::NotFound),
        "unsupported" => Ok(PrimaryNameClaimStatus::Unsupported),
        "invalid_name" => Ok(PrimaryNameClaimStatus::InvalidName),
        _ => bail!("unknown identity primary-name status {value}"),
    }
}

fn roles_storage_value(roles: ReverseIdentityRoles) -> &'static str {
    match roles {
        ReverseIdentityRoles::Owned => "owned",
        ReverseIdentityRoles::Managed => "managed",
        ReverseIdentityRoles::Both => "both",
    }
}

fn parse_roles(value: &str) -> Result<ReverseIdentityRoles> {
    match value {
        "owned" => Ok(ReverseIdentityRoles::Owned),
        "managed" => Ok(ReverseIdentityRoles::Managed),
        "both" => Ok(ReverseIdentityRoles::Both),
        _ => bail!("unknown reverse identity roles {value}"),
    }
}

fn dedupe_in_order(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}
