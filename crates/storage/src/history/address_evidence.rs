//! Historical address membership shared by authoritative reads and Project publication.
//! The filter is one definition: retaining compact membership must not change custody,
//! lineage or registry-only lease semantics.
use super::source::{
    push_history_canonicality_filter, push_history_lineage_join, push_readable_anchored_row_filter,
};
use crate::AddressNameRelation;
use sqlx::{Postgres, QueryBuilder};

pub(super) const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
pub(super) const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ADDRESS_HISTORY_MATCH_DERIVATION_KINDS: &[&str] = &[
    ENS_V1_AUTHORITY_DERIVATION_KIND,
    ENS_V2_REGISTRY_DERIVATION_KIND,
];
const ADDRESS_HISTORY_MATCH_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "TokenControlTransferred",
    "AuthorityTransferred",
];

#[derive(Clone, Copy)]
enum Address<'a> {
    Bound(&'a str),
    Candidate,
}
impl<'a> Address<'a> {
    fn push(self, builder: &mut QueryBuilder<'a, Postgres>) {
        match self {
            Self::Bound(value) => {
                builder.push_bind(value);
            }
            Self::Candidate => {
                builder.push("matched_address.address");
            }
        }
    }
}

fn push_evidence_eligibility(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
    include_candidates: bool,
) {
    push_history_lineage_join(builder);
    if include_candidates {
        builder.push(" WHERE ne.derivation_kind IN (");
    } else {
        builder.push(" WHERE ne.consumer_visibility = 'activated' AND ne.derivation_kind IN (");
    }
    let mut separated = builder.separated(", ");
    for derivation_kind in ADDRESS_HISTORY_MATCH_DERIVATION_KINDS {
        separated.push_bind(*derivation_kind);
    }
    separated.push_unseparated(") AND ne.event_kind IN (");
    let mut separated = builder.separated(", ");
    for event_kind in ADDRESS_HISTORY_MATCH_EVENT_KINDS {
        separated.push_bind(*event_kind);
    }
    separated.push_unseparated(")");

    push_history_canonicality_filter(builder, canonical_only);
    if canonical_only {
        builder.push(" AND (ne.resource_id IS NULL OR (TRUE ");
        push_readable_anchored_row_filter(builder, "r", "resource_lineage");
        builder.push("))");
    }
}

/// The names and resources an address held in history: registration grants, token transfers
/// and registry ownership transfers whose new holder is `address`. Each arm matches one partial
/// expression index on `normalized_events` (`normalized_events_address_*_match_idx`).
pub(super) fn push_historical_address_matches_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    namespace: Option<&'a str>,
    relations: Option<&'a [AddressNameRelation]>,
    canonical_only: bool,
    include_candidates: bool,
    published: Option<&'a std::collections::BTreeMap<String, i64>>,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            ne.logical_name_id,
            ne.resource_id
        FROM normalized_events ne
        LEFT JOIN resources r
          ON r.resource_id = ne.resource_id
        LEFT JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = r.chain_id
         AND resource_lineage.block_hash = r.block_hash
        "#,
    );
    push_evidence_eligibility(builder, canonical_only, include_candidates);

    if let Some(namespace) = namespace {
        builder.push(" AND ne.namespace = ");
        builder.push_bind(namespace);
    }

    if let Some(bounds) = published {
        if bounds.is_empty() {
            builder.push(" AND FALSE");
        } else {
            builder.push(" AND (");
            for (index, (chain, block)) in bounds.iter().enumerate() {
                if index > 0 {
                    builder.push(" OR ");
                }
                builder.push("(ne.chain_id = ");
                builder.push_bind(chain);
                builder.push(" AND ne.block_number <= ");
                builder.push_bind(*block);
                builder.push(")");
            }
            builder.push(")");
        }
    }
    builder.push(" AND ");
    push_address_match_filter(builder, Address::Bound(address), relations);
}

fn push_address_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: Address<'a>,
    relations: Option<&'a [AddressNameRelation]>,
) {
    let include_token_holder =
        relations.is_none_or(|relations| relations.contains(&AddressNameRelation::TokenHolder));
    let include_controller = relations
        .is_none_or(|relations| relations.contains(&AddressNameRelation::EffectiveController));

    builder.push("(");
    let mut needs_or = false;
    if include_token_holder {
        push_registrant_match_filter(builder, address);
        builder.push(" OR ");
        push_token_holder_match_filter(builder, address);
        needs_or = true;
    }
    // A name with no token is owned by its registry owner, so `owner` also matches the registry
    // ownership transfers `manager` does, except from the position a registry-only binding that
    // stands for a BaseRegistrar lease opens: the lease's holder owns the name after a transfer
    // without `reclaim`, and a released lease has no owner. The registry-only resource is one
    // per node, so an earlier tokenless owner of it keeps its history.
    if include_token_holder || include_controller {
        if needs_or {
            builder.push(" OR ");
        }
        push_registry_owner_match_filter(builder, address, !include_controller);
        needs_or = true;
    }
    if !needs_or {
        builder.push("FALSE");
    }
    builder.push(")");
}

fn push_registrant_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: Address<'a>,
) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NOT NULL
                OR ne.namespace =
        "#,
    );
    builder.push_bind("basenames");
    builder.push(" OR ne.derivation_kind = ");
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND (
                (
                    ne.event_kind = 'RegistrationGranted'
                    AND LOWER(COALESCE(ne.after_state ->> 'registrant', '')) =
        "#,
    );
    address.push(builder);
    // A BaseRegistrar lease registered straight into the NameWrapper names it as registrant, but
    // the wrapped token's holder owns the name; `registerAndWrapETH2LD` wraps it in the same
    // transaction. A receiver that unwraps in its mint callback leaves that wrap with no registrar
    // link, so the wrap's node also identifies it. Grants of other families are not this custody.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L902 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L257-L265 @ ens_v1@91c966f)
    builder.push(
        r#"
                    AND NOT EXISTS (
                        SELECT 1 FROM normalized_events wrap
                        WHERE ne.source_family = 'ens_v1_registrar_l1'
                          AND wrap.chain_id = ne.chain_id
                          AND wrap.block_number = ne.block_number
                          AND wrap.transaction_index IS NOT DISTINCT FROM ne.transaction_index
                          AND wrap.source_family = 'ens_v1_wrapper_l1'
                          AND (
                              wrap.after_state ->> 'wrapped_registrar_resource_id'
                                  = ne.resource_id::text
                              OR (
                                  wrap.after_state ->> 'source_event' = 'NameWrapped'
                                  AND LOWER(wrap.after_state ->> 'node')
                                      = LOWER(ne.after_state ->> 'namehash')
                              )
                          )
                          AND LOWER(wrap.raw_fact_ref ->> 'emitting_address')
                              = LOWER(ne.after_state ->> 'registrant')
                          AND wrap.canonicality_state
                              <> 'orphaned'::bigname_phase.canonicality_state
                    )
                )
            )
        )
        "#,
    );
}

fn push_token_holder_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: Address<'a>,
) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NOT NULL
                OR ne.namespace =
        "#,
    );
    builder.push_bind("basenames");
    builder.push(" OR ne.derivation_kind = ");
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND (
                (
                    ne.event_kind = 'TokenControlTransferred'
                    AND LOWER(COALESCE(ne.after_state ->> 'to', '')) =
        "#,
    );
    address.push(builder);
    builder.push(
        r#"
                )
            )
        )
        "#,
    );
}

fn push_registry_owner_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: Address<'a>,
    owner_only: bool,
) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NULL
                OR ne.derivation_kind =
        "#,
    );
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND ne.event_kind = 'AuthorityTransferred'
            AND (ne.after_state ->> 'owner_word_unmasked' = 'true') IS NOT TRUE
            AND LOWER(COALESCE(ne.after_state ->> 'owner', '')) =
        "#,
    );
    address.push(builder);
    if owner_only {
        // As the served owner: a write the admitted Graveyard holds, or one the registry getter
        // reports as zero, names no owner.
        builder.push(
            r#"
            AND (ne.after_state ->> 'owner_getter_reason') IS DISTINCT FROM 'graveyard'
            AND LOWER(COALESCE(ne.after_state ->> 'owner_getter', ''))
                <> '0x0000000000000000000000000000000000000000'
            AND NOT EXISTS (
                SELECT 1
                FROM bigname_phase.project_binding_candidate handoff
                WHERE handoff.chain_id = ne.chain_id
                  AND handoff.resource_id = ne.resource_id
                  AND handoff.registry_only
                  AND ROW(handoff.block_number, COALESCE(handoff.transaction_index, -1),
                          COALESCE(handoff.log_index, -1))
                      <= ROW(ne.block_number, COALESCE(ne.transaction_index, -1),
                             COALESCE(ne.log_index, -1))
                  AND EXISTS (
                      SELECT 1 FROM normalized_events lease
                      WHERE lease.resource_id = handoff.lease_resource_id
                        AND lease.source_family = 'ens_v1_registrar_l1'
                        AND lease.canonicality_state <> 'orphaned'::bigname_phase.canonicality_state
                  )
            )
            "#,
        );
    }
    builder.push(")");
}

/// One qualified historical holding. Project reduces these into independent name/resource
/// masks, so several events or relation reasons add no event-address rows.
#[derive(Debug, sqlx::FromRow)]
pub struct HistoricalHistoryRelation {
    pub address: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<uuid::Uuid>,
    pub relation_mask: i16,
}

/// Re-evaluate the complete historical evidence of affected names/resources at publication.
/// Each source is probed by its existing leading key before the shared matcher runs; the
/// isolated changed event alone cannot decide custody or later lease-handoff exclusions.
pub async fn historical_history_relations(
    connection: &mut sqlx::PgConnection,
    chain_id: &str,
    block_number: i64,
    names: &[String],
    resources: &[uuid::Uuid],
) -> anyhow::Result<Vec<HistoricalHistoryRelation>> {
    use anyhow::Context;
    let mut query = QueryBuilder::<Postgres>::new(
        "/* storage:history.publication_memberships */ WITH evidence AS (
        SELECT ne.* FROM unnest(",
    );
    query.push_bind(names).push(
        "::text[]) wanted(name)
        CROSS JOIN LATERAL (SELECT ne.* FROM normalized_events ne
            WHERE ne.logical_name_id = wanted.name AND ne.chain_id = ",
    );
    query
        .push_bind(chain_id)
        .push(" AND ne.block_number <= ")
        .push_bind(block_number);
    query
        .push(
            " AND ne.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
              ) OFFSET 0) ne UNION
        SELECT ne.* FROM unnest(",
        )
        .push_bind(resources)
        .push(
            "::uuid[]) wanted(resource)
        CROSS JOIN LATERAL (SELECT ne.* FROM normalized_events ne
            WHERE ne.resource_id = wanted.resource AND ne.chain_id = ",
        );
    query
        .push_bind(chain_id)
        .push(" AND ne.block_number <= ")
        .push_bind(block_number);
    query.push(
        " AND ne.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
          ) OFFSET 0) ne)
        SELECT DISTINCT matched_address.address, ne.namespace, ne.logical_name_id,
               ne.resource_id, relation.mask AS relation_mask
        FROM evidence ne
        CROSS JOIN LATERAL (
            SELECT DISTINCT lower(address) AS address FROM (VALUES
                (ne.after_state ->> 'registrant'), (ne.after_state ->> 'to'),
                (ne.after_state ->> 'owner')) candidate(address)
            WHERE address IS NOT NULL
        ) matched_address
        CROSS JOIN (VALUES (1::smallint), (2::smallint)) relation(mask)
        LEFT JOIN resources r ON r.resource_id = ne.resource_id
        LEFT JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = r.chain_id
         AND resource_lineage.block_hash = r.block_hash ",
    );
    push_evidence_eligibility(&mut query, true, false);
    query.push(" AND ((relation.mask = 1 AND ");
    push_address_match_filter(
        &mut query,
        Address::Candidate,
        Some(&[AddressNameRelation::TokenHolder]),
    );
    query.push(") OR (relation.mask = 2 AND ");
    push_address_match_filter(
        &mut query,
        Address::Candidate,
        Some(&[AddressNameRelation::EffectiveController]),
    );
    query.push("))");
    query
        .build_query_as()
        .fetch_all(connection)
        .await
        .context("failed to derive historical history membership")
}
