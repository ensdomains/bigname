use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{
    DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS, DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
    decode::decode_address_name_current_row,
    types::{AddressNameCurrentRow, AddressNameRelation},
};

/// Load current address-name relation rows from the default canonical read set.
pub async fn load_address_names_current(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    load_address_names_current_for_relations(pool, address, namespace, relations).await
}

/// Load current address-name relation rows, including noncanonical supporting identity rows.
pub async fn load_address_names_current_including_noncanonical(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    load_address_names_current_including_noncanonical_for_relations(
        pool, address, namespace, relations,
    )
    .await
}

/// Load current address-name relation rows from the default canonical read set.
pub async fn load_address_names_current_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
) -> Result<Vec<AddressNameCurrentRow>> {
    load_address_names_current_internal(pool, address, namespace, relations, false, None).await
}

/// Load current address-name relation rows, including noncanonical supporting identity rows.
pub async fn load_address_names_current_including_noncanonical_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
) -> Result<Vec<AddressNameCurrentRow>> {
    load_address_names_current_internal(pool, address, namespace, relations, true, None).await
}

/// Current address-name relation rows the address held at `published`: the relation's
/// `provenance.chain_id` is a bound chain and either its `chain_positions.block_number` is at or
/// below that chain's bound, or the event it cites is a token transfer from the address to itself
/// and every registration event between the bound and it is one too. Any other relation Project
/// cites at a later block, or without a block, is not returned. History reads use this so a
/// relation acquired after the block a read is bound to cannot admit older events.
pub(crate) async fn load_address_names_current_at_bound(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    include_noncanonical: bool,
    published: &BTreeMap<String, i64>,
) -> Result<Vec<AddressNameCurrentRow>> {
    load_address_names_current_internal(
        pool,
        address,
        namespace,
        relations,
        include_noncanonical,
        Some(published),
    )
    .await
}

async fn load_address_names_current_internal(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    include_noncanonical: bool,
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_query(
        &mut builder,
        address,
        namespace,
        relations,
        include_noncanonical,
        published,
    );
    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
            parts.push(format!(
                "relations {}",
                relations
                    .iter()
                    .map(|relation| relation.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        format!(
            "failed to load address_names_current rows for {}",
            parts.join(" ")
        )
    })?;

    rows.into_iter()
        .map(decode_address_name_current_row)
        .collect()
}

/// The current-row read: the address's rows, optionally narrowed by namespace and relations,
/// through the default canonical read set unless `include_noncanonical`, and bounded by
/// `published` when given.
pub(crate) fn push_address_names_current_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    namespace: Option<&'a str>,
    relations: Option<&[AddressNameRelation]>,
    include_noncanonical: bool,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(
        r#"
        SELECT
            anc.address,
            anc.logical_name_id,
            anc.relation,
            anc.namespace,
            anc.raw_name AS canonical_display_name,
            anc.namehash,
            anc.surface_binding_id,
            anc.resource_id,
            anc.token_lineage_id,
            anc.binding_kind,
            anc.provenance,
            CASE WHEN anc.support_status = 'supported'
                 THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                 ELSE jsonb_build_object(
                     'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                     'unsupported_reason', anc.unsupported_reason
                 ) END AS coverage,
            anc.chain_positions,
            anc.canonicality_summary,
            anc.manifest_version,
            anc.last_recomputed_at
        FROM bigname_phase.address_names_current anc
        "#,
    );
    if !include_noncanonical {
        builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS);
    }
    builder.push(" WHERE anc.address = ");
    builder.push_bind(address);

    if let Some(namespace) = namespace {
        builder.push(" AND anc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
        let relation_values = relations
            .iter()
            .map(|relation| relation.as_str().to_owned())
            .collect::<Vec<_>>();
        builder.push(" AND anc.relation::TEXT = ANY(");
        builder.push_bind(relation_values);
        builder.push(")");
    }
    if !include_noncanonical {
        builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER);
    }
    if let Some(published) = published {
        push_cited_event_bound(builder, published);
    }

    builder.push(
        r#"
        ORDER BY
            anc.raw_name ASC,
            anc.logical_name_id ASC,
            CASE anc.relation
                WHEN 'registrant' THEN 0
                WHEN 'token_holder' THEN 1
                WHEN 'effective_controller' THEN 2
                ELSE 99
            END ASC
        "#,
    );
}

fn push_cited_event_bound(
    builder: &mut QueryBuilder<'_, Postgres>,
    published: &BTreeMap<String, i64>,
) {
    if published.is_empty() {
        builder.push(" AND FALSE");
        return;
    }
    builder.push(" AND (");
    for (index, (chain_id, block_number)) in published.iter().enumerate() {
        if index > 0 {
            builder.push(" OR ");
        }
        builder.push("(anc.provenance ->> 'chain_id' = ");
        builder.push_bind(chain_id.clone());
        builder.push(
            " AND (CASE WHEN jsonb_typeof(anc.chain_positions -> 'block_number') = 'number'
                        THEN (anc.chain_positions ->> 'block_number')::bigint END <= ",
        );
        builder.push_bind(*block_number);
        builder.push(" OR ");
        push_same_holder_since_bound(builder, chain_id, *block_number);
        builder.push("))");
    }
    builder.push(")");
}

/// A row cited above the bound still held at the bound: Project cites the latest registration
/// event for the registrant, token holder and fallback controller rows, and a token transfer from
/// the holder to itself moves that citation without changing the holder. The cited event must be
/// such a transfer, and every registration event on its resource between the bound and it must be
/// one too. The earliest of them names the address as its sender, so the address held the token
/// just before it, and no event in the range changed the holder, so it held the token at the
/// bound. A relation that began after the bound has a grant, an ENSv2 reservation or a transfer to
/// it in the range and stays excluded. A controller row also refuses any controller event in the
/// range.
fn push_same_holder_since_bound(
    builder: &mut QueryBuilder<'_, Postgres>,
    chain_id: &str,
    block_number: i64,
) {
    builder.push(
        r#"EXISTS (
            SELECT 1
            FROM normalized_events cited
            WHERE cited.normalized_event_id = CASE
                      WHEN jsonb_typeof(anc.provenance -> 'normalized_event_id') = 'number'
                      THEN (anc.provenance ->> 'normalized_event_id')::bigint END
              AND cited.chain_id = "#,
    );
    builder.push_bind(chain_id.to_owned());
    builder.push(" AND cited.block_number > ");
    builder.push_bind(block_number);
    builder.push(
        r#"
              AND cited.resource_id IS NOT NULL
              AND cited.consumer_visibility = 'activated'
              AND cited.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND cited.event_kind = 'TokenControlTransferred'
              AND lower(cited.after_state ->> 'to') = anc.address
              AND lower(cited.before_state ->> 'from') = anc.address
              AND NOT EXISTS (
                  SELECT 1
                  FROM normalized_events moved
                  WHERE moved.resource_id = cited.resource_id
                    AND moved.canonicality_state IN (
                        'canonical'::bigname_phase.canonicality_state,
                        'safe'::bigname_phase.canonicality_state,
                        'finalized'::bigname_phase.canonicality_state
                    )
                    AND moved.block_number > "#,
    );
    builder.push_bind(block_number);
    builder.push(
        r#"
                    AND moved.block_number <= cited.block_number
                    AND moved.chain_id = cited.chain_id
                    AND moved.consumer_visibility = 'activated'
                    AND (
                        moved.event_kind IN (
                            'RegistrationGranted', 'RegistrationReserved',
                            'RegistrationReleased', 'TokenControlTransferred'
                        )
                        OR (
                            anc.relation = 'effective_controller'
                            AND moved.event_kind IN (
                                'AuthorityTransferred', 'SurfaceBound', 'PermissionChanged'
                            )
                        )
                    )
                    AND NOT (
                        moved.event_kind = 'TokenControlTransferred'
                        AND lower(moved.after_state ->> 'to') IS NOT DISTINCT FROM anc.address
                        AND lower(moved.before_state ->> 'from') IS NOT DISTINCT FROM anc.address
                    )
              )
        )"#,
    );
}
