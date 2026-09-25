//! F3, resolver classification (builders/resolver/build.sql, the `candidates` to `summarized`
//! CTEs, without the sampled sections). A resolver's row keeps what the served candidate set is
//! built from, so the row is classified again from it alone, block-pinned:
//!
//! - `observed_families`: every family an event proposed the resolver under, with its best
//!   priority: 3 for an ENSv2 `Upgraded` proxy, an `AliasChanged` and either side of a
//!   `ResolverChanged`, 4 for either side of a `PermissionChanged` scope (build.sql:5-86). Events
//!   only add, so the map is a fold over the resolver's events.
//! - `pointer_families`: per family, how many F4 and F5 pointer rows point at the resolver now,
//!   standing for priority 2, the name pointers of `project_resolver_binding_summary`. A name's
//!   current pointer is one of those rows, so a family with a count proposes the resolver.
//! - `upgrades`: per family, the latest `Upgraded` of the proxy (build.sql:282-298).
//!
//! The discovered candidates (priority 0 declarations admitted by a same-namespace resolver edge,
//! priority 1 the edges themselves, declaration_precedence.rs) and the manifests are read at the
//! block. A resolver is classified again when an event of the block names it, when a pointer row
//! moves to or from it, when a resolver edge, its contract address or a manifest declaration of
//! it starts or stops at the block, and, every stored resolver, when the block sees another
//! admission epoch. A resolver with no candidate has no row. One with candidates but no active
//! manifest of its family, which the served build leaves out, keeps a row marked unsupported with
//! `resolver_manifest_not_active`. The row's position is the latest event that named the
//! resolver, or `activation:<block>` for an activation; an epoch change reclassifies without
//! moving it, and `admission_epoch` records the epoch the classification was made under.
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::{BlockEvent, Position},
    keys::{self, ZERO_ADDRESS},
    reduce::{Context, in_family, key_of, load_rows, raw_lower, set},
    store::{Row, RowSet},
    tables,
};
use crate::{ProjectError, Result};

/// The served classification summary version (resolver/section_summaries.rs).
const SUMMARY_VERSION: i32 = 1;

/// The resolver family a proposing event's family maps to (build.sql:42-48).
fn resolver_family(source_family: &str) -> &'static str {
    if source_family.starts_with("ens_v2_") {
        "ens_v2_resolver_l1"
    } else if source_family.starts_with("basenames_") {
        "basenames_base_resolver"
    } else {
        "ens_v1_resolver_l1"
    }
}

const ALIAS_FAMILIES: [&str; 3] = [
    "ens_v1_resolver_l1",
    "ens_v2_resolver_l1",
    "basenames_base_resolver",
];

fn resolver(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && value != ZERO_ADDRESS)
}

/// The (resolver, family, priority) proposals of one event.
fn proposals(event: &BlockEvent) -> Vec<(String, &'static str, i64)> {
    let family = event.source_family.as_str();
    let mut found = Vec::new();
    match event.event_kind.as_str() {
        "Upgraded" if family == "ens_v2_resolver_l1" => {
            found.extend(
                resolver(raw_lower(&event.after, "proxy_address"))
                    .map(|address| (address, "ens_v2_resolver_l1", 3)),
            );
        }
        "AliasChanged" if ALIAS_FAMILIES.contains(&family) => {
            found.extend(
                resolver(keys::alias_resolver(event))
                    .map(|address| (address, resolver_family(family), 3)),
            );
        }
        "ResolverChanged" => {
            for state in [&event.after, &event.before] {
                found.extend(
                    resolver(raw_lower(state, "resolver"))
                        .map(|address| (address, resolver_family(family), 3)),
                );
            }
        }
        "PermissionChanged" => {
            for state in [&event.after, &event.before] {
                let address = state
                    .get("scope")
                    .and_then(|scope| raw_lower(scope, "resolver_address"));
                found
                    .extend(resolver(address).map(|address| (address, resolver_family(family), 4)));
            }
        }
        _ => {}
    }
    found
}

/// The proxy an `Upgraded` of any family upgrades, with its family.
fn upgrade(event: &BlockEvent) -> Option<(String, String)> {
    (event.event_kind == "Upgraded").then_some(())?;
    Some((
        resolver(raw_lower(&event.after, "proxy_address"))?,
        event.source_family.clone(),
    ))
}

fn object(row: &Row, column: &str) -> Map<String, Value> {
    row.get(column)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::RESOLVER_CLASSIFICATION;
    let chain = json!(context.chain_id);
    let pointer_deltas = pointer_deltas(rows);
    let activated = activated(transaction, context).await?;
    let mut touched: BTreeSet<String> = events
        .iter()
        .flat_map(proposals)
        .map(|(address, _, _)| address)
        .chain(
            events
                .iter()
                .filter_map(upgrade)
                .map(|(address, _)| address),
        )
        .chain(pointer_deltas.keys().map(|(address, _)| address.clone()))
        .chain(activated.iter().cloned())
        .collect();
    if context.epoch_changed {
        let stored: Vec<String> = sqlx::query_scalar(
            "/* project:families.classification.stored */ SELECT resolver_address
             FROM project_resolver_classification WHERE chain_id = $1",
        )
        .bind(context.chain_id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to read classified resolvers", error))
        .map_err(in_family(table.name))?;
        touched.extend(stored);
    }
    if touched.is_empty() {
        return Ok(());
    }
    let key = |address: &str| key_of(table, [chain.clone(), json!(address)]);
    load_rows(
        transaction,
        rows,
        table,
        touched.iter().map(|a| key(a)).collect(),
    )
    .await?;

    // Fold the block's events and pointer moves into each resolver's accumulators.
    let mut state: BTreeMap<String, Row> = touched
        .iter()
        .map(|address| {
            let row = rows.get(table, &key(address)).cloned().unwrap_or_else(|| {
                let mut row = key(address);
                for column in ["observed_families", "pointer_families", "upgrades"] {
                    set(&mut row, column, json!({}));
                }
                row
            });
            (address.clone(), row)
        })
        .collect();
    let mut last_event: BTreeMap<String, &BlockEvent> = BTreeMap::new();
    for event in events {
        for (address, family, priority) in proposals(event) {
            let row = state.get_mut(&address).expect("touched");
            let mut observed = object(row, "observed_families");
            let best = observed
                .get(family)
                .and_then(Value::as_i64)
                .map_or(priority, |known| known.min(priority));
            observed.insert(family.to_owned(), json!(best));
            set(row, "observed_families", Value::Object(observed));
            last_event.insert(address, event);
        }
        if let Some((address, family)) = upgrade(event) {
            let row = state.get_mut(&address).expect("touched");
            let mut upgrades = object(row, "upgrades");
            let mut latest = event.position.to_json();
            latest["implementation"] = json!(raw_lower(&event.after, "implementation"));
            latest["normalized_event_id"] = json!(event.normalized_event_id);
            upgrades.insert(family, latest);
            set(row, "upgrades", Value::Object(upgrades));
            last_event.insert(address, event);
        }
    }
    for ((address, family), delta) in &pointer_deltas {
        let row = state.get_mut(address).expect("touched");
        let mut counts = object(row, "pointer_families");
        let count = counts.get(*family).and_then(Value::as_i64).unwrap_or(0) + delta;
        if count > 0 {
            counts.insert((*family).to_owned(), json!(count));
        } else {
            counts.remove(*family);
        }
        set(row, "pointer_families", Value::Object(counts));
    }

    let classified = classify(transaction, context, &state).await?;
    for (address, mut row) in state {
        let existed = rows.get(table, &key(&address)).is_some();
        let Some(result) = classified.get(&address) else {
            if existed {
                rows.delete(table, &key(&address))
                    .map_err(in_family(table.name))?;
            }
            continue;
        };
        for (column, value) in result {
            set(&mut row, column, value.clone());
        }
        set(&mut row, "admission_epoch", context.epoch);
        set(&mut row, "summary_version", SUMMARY_VERSION.to_string());
        if let Some(event) = last_event.get(&address) {
            event.write_position(&mut row);
        } else if activated.contains(&address) || !existed {
            Position {
                block_number: context.block.number,
                transaction_index: None,
                log_index: None,
                event_identity: format!("activation:{}", context.block.number),
            }
            .write_columns(&mut row);
            set(&mut row, "normalized_event_id", Value::Null);
        }
        rows.put(table, row).map_err(in_family(table.name))?;
    }
    Ok(())
}

/// How many pointer rows each (resolver, family) gained or lost in the block, from the F4 and
/// F5 rows the block changed (their pre-block images against their current state).
fn pointer_deltas(rows: &RowSet) -> BTreeMap<(String, &'static str), i64> {
    let mut deltas = BTreeMap::new();
    let pointer = |row: Option<&Row>| {
        let row = row?;
        let address = resolver(
            row.get("resolver_address")
                .and_then(Value::as_str)
                .map(str::to_owned),
        )?;
        let family = resolver_family(row.get("source_family").and_then(Value::as_str)?);
        Some((address, family))
    };
    for change in rows.changes() {
        if change.table.name != tables::REGISTRY_POINTER.name
            && change.table.name != tables::RESOURCE_POINTER.name
        {
            continue;
        }
        let (before, after) = (pointer(change.before), pointer(change.after));
        if before == after {
            continue;
        }
        if let Some(key) = before {
            *deltas.entry(key).or_insert(0) -= 1;
        }
        if let Some(key) = after {
            *deltas.entry(key).or_insert(0) += 1;
        }
    }
    deltas.retain(|_, delta| *delta != 0);
    deltas
}

/// The resolvers whose discovered candidates can change at this block: a resolver edge or its
/// target's contract address that starts or stops here, or an active manifest declaration of the
/// address whose start block is this block.
async fn activated(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
) -> Result<BTreeSet<String>> {
    let addresses: Vec<String> = sqlx::query_scalar(&format!(
        "/* project:families.classification.activated */ WITH {MANIFESTS},
         edges AS (
             SELECT edge.chain_id, edge.to_contract_instance_id FROM discovery_edges edge
             WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
               AND edge.active_from_block_number = $2
             UNION
             SELECT edge.chain_id, edge.to_contract_instance_id FROM discovery_edges edge
             WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
               AND edge.active_to_block_number = $2
         )
         SELECT lower(address.address) FROM edges edge
         JOIN contract_instance_addresses address
           ON address.contract_instance_id = edge.to_contract_instance_id
          AND address.chain_id = edge.chain_id
         UNION
         SELECT lower(address.address) FROM contract_instance_addresses address
         WHERE address.chain_id = $1
           AND (address.active_from_block_number = $2 OR address.active_to_block_number = $2)
           AND EXISTS (
               SELECT 1 FROM discovery_edges edge
               WHERE edge.chain_id = address.chain_id AND edge.edge_kind = 'resolver'
                 AND edge.to_contract_instance_id = address.contract_instance_id
           )
         UNION
         SELECT lower(declaration ->> 'address') FROM manifests manifest
         CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
             manifest.manifest_payload -> 'contracts', '[]'::jsonb)) declaration
         WHERE declaration ->> 'start_block' ~ '^[0-9]+$'
           AND (declaration ->> 'start_block')::bigint = $2"
    ))
    .bind(context.chain_id)
    .bind(context.block.number)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read resolver activations", error))
    .map_err(in_family(tables::RESOLVER_CLASSIFICATION.name))?;
    Ok(addresses
        .into_iter()
        .filter_map(|address| resolver(Some(address)))
        .collect())
}

/// The active manifests at block $2 (stage.rs `create_manifests`).
pub(crate) const MANIFESTS: &str = "manifests AS (
    SELECT * FROM (
        SELECT DISTINCT ON (event.source_manifest_id)
               event.source_manifest_id AS manifest_id, event.namespace, event.source_family,
               event.after_state ->> 'rollout_status' AS rollout_status,
               event.after_state -> 'manifest_payload' AS manifest_payload,
               event.normalized_event_id AS manifest_event_id
        FROM normalized_events event
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
         AND lineage.block_number = event.block_number
        WHERE (event.chain_id = $1
               OR ($1 = 'base-mainnet' AND event.namespace = 'basenames'
                   AND event.source_family = 'basenames_execution'
                   AND event.chain_id = 'ethereum-mainnet'))
          AND event.event_kind = 'SourceManifestUpdated'
          AND event.source_manifest_id IS NOT NULL
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND (event.block_hash IS NULL
               OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
          AND (event.block_number IS NULL OR event.block_number <= $2)
        ORDER BY event.source_manifest_id, event.normalized_event_id DESC
    ) latest
    WHERE rollout_status = 'active' AND manifest_payload IS NOT NULL
)";

/// Classify every resolver of `state` at the block from its stored accumulators and the
/// discovered candidates. Returns the columns to set per resolver that has a candidate.
async fn classify(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    state: &BTreeMap<String, Row>,
) -> Result<BTreeMap<String, Vec<(&'static str, Value)>>> {
    let input: Vec<Value> = state
        .iter()
        .map(|(address, row)| {
            let mut candidates: Vec<Value> = object(row, "observed_families")
                .into_iter()
                .map(|(family, priority)| json!({"family": family, "priority": priority}))
                .collect();
            candidates.extend(
                object(row, "pointer_families")
                    .into_iter()
                    .map(|(family, _)| json!({"family": family, "priority": 2})),
            );
            json!({"resolver_address": address, "candidates": candidates,
                   "upgrades": object(row, "upgrades")})
        })
        .collect();
    let rows: Vec<Value> = sqlx::query_scalar(&format!(
        "/* project:families.classification.classify */ WITH {MANIFESTS},
         input AS (
             SELECT item ->> 'resolver_address' AS resolver_address, item
             FROM jsonb_array_elements($3::jsonb) item
         ),
         {CLASSIFY}"
    ))
    .bind(context.chain_id)
    .bind(context.block.number)
    .bind(Value::Array(input))
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to classify resolvers", error))
    .map_err(in_family(tables::RESOLVER_CLASSIFICATION.name))?;
    let mut classified = BTreeMap::new();
    for row in rows {
        let Some(address) = row.get("resolver_address").and_then(Value::as_str) else {
            continue;
        };
        let columns = [
            "classification",
            "support_status",
            "unsupported_reason",
            "manifest_id",
            "manifest_event_id",
            "admission_namespace",
        ]
        .into_iter()
        .map(|column| (column, row.get(column).cloned().unwrap_or(Value::Null)))
        .collect();
        classified.insert(address.to_owned(), columns);
    }
    Ok(classified)
}

/// The served classification over the input resolvers: the candidate precedence of build.sql
/// `candidates`, the manifest join, role, support and read features of `classified` to
/// `summarized`, one row per resolver with a candidate (the manifest with the lowest id when
/// several of its family are active).
const CLASSIFY: &str = r#"
declared AS (
    SELECT manifest.namespace, manifest.source_family,
           lower(declaration ->> 'address') AS resolver_address,
           declaration ->> 'role' AS classification_role,
           (declaration ->> 'start_block')::bigint AS declaration_start_block,
           declaration_ordinality AS classification_declaration_ordinality,
           manifest.manifest_id
    FROM manifests manifest
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
        manifest.manifest_payload -> 'contracts', '[]'::jsonb
    )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
    WHERE (manifest.source_family = 'ens_v1_resolver_l1'
           OR (manifest.source_family = 'ens_v2_resolver_l1'
               AND declaration ->> 'role' IN ('public_resolver_v2', 'ensv1_mirror_resolver')
               AND declaration ->> 'proxy_kind' = 'none'))
      AND lower(declaration ->> 'address') IN (SELECT resolver_address FROM input)
      AND (declaration ->> 'start_block' IS NULL
           OR (declaration ->> 'start_block')::bigint <= $2)
      AND (manifest.source_family <> 'ens_v2_resolver_l1' OR NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(manifest.manifest_payload -> 'contracts')
              WITH ORDINALITY later(item, ordinal)
          WHERE lower(item ->> 'address') = lower(declaration ->> 'address')
            AND COALESCE((item ->> 'start_block')::bigint, 0) <= $2
            AND (COALESCE((item ->> 'start_block')::bigint, 0), ordinal) >
                (COALESCE((declaration ->> 'start_block')::bigint, 0), declaration_ordinality)
      ))
),
admissions AS (
    SELECT lower(address.address) AS resolver_address, origin.namespace,
           CASE origin.source_family
               WHEN 'ens_v1_registry_l1' THEN 'ens_v1_resolver_l1'
               WHEN 'ens_v1_resolver_l1' THEN 'ens_v1_resolver_l1'
               WHEN 'ens_v2_registry_l1' THEN 'ens_v2_resolver_l1'
               WHEN 'ens_v2_resolver_l1' THEN 'ens_v2_resolver_l1'
               WHEN 'basenames_base_registry' THEN 'basenames_base_resolver'
               WHEN 'basenames_base_resolver' THEN 'basenames_base_resolver'
           END AS source_family
    FROM contract_instance_addresses address
    JOIN discovery_edges edge
      ON edge.to_contract_instance_id = address.contract_instance_id
     AND edge.chain_id = address.chain_id
    LEFT JOIN manifests origin ON origin.manifest_id = edge.source_manifest_id
    WHERE address.chain_id = $1
      AND lower(address.address) IN (SELECT resolver_address FROM input)
      AND edge.edge_kind = 'resolver'
      AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (edge.active_from_block_number IS NULL OR edge.active_from_block_number <= $2)
      AND (edge.active_to_block_number IS NULL OR edge.active_to_block_number > $2)
      AND edge.deactivated_at IS NULL
      AND (edge.active_from_block_hash IS NULL OR EXISTS (
          SELECT 1 FROM chain_lineage lineage
          WHERE lineage.chain_id = edge.chain_id
            AND lineage.block_number = edge.active_from_block_number
            AND lineage.block_hash = edge.active_from_block_hash
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
      AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $2)
      AND (address.active_to_block_number IS NULL OR address.active_to_block_number > $2)
      AND address.deactivated_at IS NULL
      AND (address.active_from_block_hash IS NULL OR EXISTS (
          SELECT 1 FROM chain_lineage lineage
          WHERE lineage.chain_id = address.chain_id
            AND lineage.block_number = address.active_from_block_number
            AND lineage.block_hash = address.active_from_block_hash
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
),
combined AS (
    SELECT admission.resolver_address, admission.source_family,
           NULL::text AS classification_role, 1 AS priority,
           NULL::bigint AS classification_manifest_id,
           NULL::text AS classification_admission_namespace,
           NULL::bigint AS classification_declaration_start_block,
           NULL::bigint AS classification_declaration_ordinality
    FROM admissions admission
    UNION ALL
    SELECT admission.resolver_address, declaration.source_family,
           declaration.classification_role, 0, declaration.manifest_id, admission.namespace,
           declaration.declaration_start_block,
           declaration.classification_declaration_ordinality
    FROM admissions admission
    JOIN declared declaration
      ON declaration.namespace = admission.namespace
     AND declaration.resolver_address = admission.resolver_address
    UNION ALL
    SELECT input.resolver_address, candidate ->> 'family', NULL, (candidate ->> 'priority')::int,
           NULL, NULL, NULL, NULL
    FROM input
    CROSS JOIN LATERAL jsonb_array_elements(input.item -> 'candidates') candidate
),
candidates AS (
    SELECT DISTINCT ON (combined.resolver_address) combined.*
    FROM combined
    WHERE combined.source_family IS NOT NULL
    ORDER BY combined.resolver_address, combined.priority, combined.source_family,
             combined.classification_manifest_id NULLS LAST,
             COALESCE(combined.classification_declaration_start_block, 0) DESC,
             combined.classification_declaration_ordinality DESC NULLS LAST,
             combined.classification_role
),
classified AS (
    SELECT DISTINCT ON (candidate.resolver_address)
           candidate.resolver_address, candidate.source_family,
           candidate.classification_admission_namespace,
           manifest.manifest_id, manifest.manifest_payload, manifest.manifest_event_id,
           upgrade.value AS upgrade,
           upgrade.value ->> 'implementation' AS implementation,
           COALESCE((
               SELECT declaration -> 'read_features'
               FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'contracts', '[]'::jsonb
               )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
               WHERE lower(declaration ->> 'address') = candidate.resolver_address
                 AND (declaration ->> 'start_block' IS NULL
                      OR (declaration ->> 'start_block')::bigint <= $2)
               ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC,
                        declaration_ordinality DESC
               LIMIT 1
           ), '[]'::jsonb) AS declared_read_features,
           COALESCE((
               SELECT admitted -> 'read_features'
               FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
               )) WITH ORDINALITY implementations(admitted, admitted_ordinality)
               WHERE lower(admitted ->> 'address') = lower(upgrade.value ->> 'implementation')
               ORDER BY admitted_ordinality DESC
               LIMIT 1
           ), '[]'::jsonb) AS implementation_read_features,
           COALESCE(
               candidate.classification_role,
               (
                   SELECT declaration ->> 'role'
                   FROM jsonb_array_elements(COALESCE(
                       manifest.manifest_payload -> 'contracts', '[]'::jsonb
                   )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
                   WHERE lower(declaration ->> 'address') = candidate.resolver_address
                     AND (declaration ->> 'start_block' IS NULL
                          OR (declaration ->> 'start_block')::bigint <= $2)
                   ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC,
                            declaration_ordinality DESC
                   LIMIT 1
               ),
               (
                   SELECT implementation ->> 'role'
                   FROM jsonb_array_elements(COALESCE(
                       manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
                   )) WITH ORDINALITY implementations(implementation, implementation_ordinality)
                   WHERE lower(implementation ->> 'address') =
                         lower(upgrade.value ->> 'implementation')
                   ORDER BY implementation_ordinality DESC
                   LIMIT 1
               )
           ) AS classification_role,
           EXISTS (
               SELECT 1 FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'contracts', '[]'::jsonb)) declaration
               WHERE lower(declaration ->> 'address') = candidate.resolver_address
                 AND (declaration ->> 'role' <> 'public_resolver_v2'
                      OR declaration ->> 'proxy_kind' = 'none')
                 AND (declaration ->> 'start_block' IS NULL
                      OR (declaration ->> 'start_block')::bigint <= $2)
           ) AS exact_declared,
           EXISTS (SELECT 1 FROM declared direct
                   WHERE direct.manifest_id = manifest.manifest_id
                     AND direct.resolver_address = candidate.resolver_address
                     AND direct.classification_role = 'public_resolver_v2'
                     AND direct.source_family = 'ens_v2_resolver_l1') AS direct_public_v2,
           EXISTS (SELECT 1 FROM declared direct
                   WHERE direct.manifest_id = manifest.manifest_id
                     AND direct.resolver_address = candidate.resolver_address
                     AND direct.classification_role = 'ensv1_mirror_resolver'
                     AND direct.source_family = 'ens_v2_resolver_l1') AS direct_mirror,
           EXISTS (
               SELECT 1 FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
               )) implementation
               WHERE lower(implementation ->> 'address') =
                     lower(upgrade.value ->> 'implementation')
           ) AS upgraded_to_declared
    FROM candidates candidate
    JOIN input ON input.resolver_address = candidate.resolver_address
    LEFT JOIN manifests manifest
      ON (candidate.classification_manifest_id IS NOT NULL
          AND manifest.manifest_id = candidate.classification_manifest_id)
      OR (candidate.classification_manifest_id IS NULL
          AND manifest.source_family = candidate.source_family)
    LEFT JOIN LATERAL (
        SELECT input.item -> 'upgrades' -> candidate.source_family AS value
    ) upgrade ON TRUE
    ORDER BY candidate.resolver_address, manifest.manifest_id NULLS LAST
),
supported AS (
    SELECT classified.*,
           CASE WHEN manifest_id IS NULL THEN false
                WHEN source_family = 'ens_v2_resolver_l1'
                     AND classification_role = 'public_resolver_v2' THEN direct_public_v2
                WHEN source_family = 'ens_v2_resolver_l1'
                     AND classification_role = 'ensv1_mirror_resolver' THEN direct_mirror
                WHEN source_family = 'ens_v2_resolver_l1'
                    THEN upgraded_to_declared AND upgrade IS NOT NULL
                ELSE exact_declared END AS supported,
           CASE
               WHEN manifest_id IS NULL THEN 'resolver_manifest_not_active'
               WHEN source_family = 'ens_v2_resolver_l1'
                AND classification_role = 'public_resolver_v2'
                   THEN CASE WHEN NOT direct_public_v2 THEN 'resolver_not_declared' END
               WHEN source_family = 'ens_v2_resolver_l1'
                AND classification_role = 'ensv1_mirror_resolver'
                   THEN CASE WHEN NOT direct_mirror THEN 'resolver_not_declared' END
               WHEN source_family = 'ens_v2_resolver_l1' AND upgrade IS NULL
                   THEN 'resolver_implementation_unknown'
               WHEN source_family = 'ens_v2_resolver_l1' AND NOT upgraded_to_declared
                   THEN 'resolver_implementation_not_declared'
               WHEN source_family <> 'ens_v2_resolver_l1' AND NOT exact_declared
                   THEN 'resolver_not_declared'
           END AS support_reason
    FROM classified
)
SELECT jsonb_build_object(
           'resolver_address', resolver_address,
           'classification', jsonb_strip_nulls(jsonb_build_object(
               'source_family', source_family,
               'role', classification_role,
               'basis', CASE WHEN source_family = 'ens_v2_resolver_l1'
                    AND COALESCE(classification_role, '')
                        NOT IN ('public_resolver_v2', 'ensv1_mirror_resolver')
                   THEN 'erc1967_upgraded_history'
                   ELSE 'manifest_declared_address' END,
               'implementation', implementation,
               'read_features', CASE
                   WHEN NOT supported THEN '[]'::jsonb
                   WHEN source_family = 'ens_v2_resolver_l1'
                    AND COALESCE(classification_role, '')
                        NOT IN ('public_resolver_v2', 'ensv1_mirror_resolver')
                       THEN implementation_read_features
                   ELSE declared_read_features
               END,
               'mirror', CASE WHEN classification_role = 'ensv1_mirror_resolver'
                   THEN jsonb_strip_nulls(jsonb_build_object(
                       'mirrored_source_family', 'ens_v1_resolver_l1',
                       'mirrored_registry_source_family', 'ens_v1_registry_l1',
                       'mirrored_registry_address',
                           lower(manifest_payload #>> '{correlation_addresses,ens_v1_registry}')
                   )) END,
               'upgrade', upgrade
           )),
           'support_status', CASE WHEN supported THEN 'supported' ELSE 'unsupported' END,
           'unsupported_reason', support_reason,
           'manifest_id', manifest_id,
           'manifest_event_id', manifest_event_id,
           'admission_namespace', classification_admission_namespace
       )
FROM supported
"#;
