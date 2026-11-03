//! The event predicate Project's stored event counts share with the event reads they count
//! (`docs/projections.md`, "Event counts"): which rows an emitter or a resolver reads, the fixed
//! product duplicate filters, the registry-fallback handoff minimum and the proven address pair.
//! The reads in `duplicates.rs`, `address_pairs.rs`, `paging.rs` and `resolver_source.rs` push
//! these pieces, and [`block_event_counts_sql`] builds Project's count statement from them, so a
//! stored count is a count of the rows the read admits.

use sqlx::{Postgres, QueryBuilder};

use super::source::{push_history_canonicality_filter, push_history_lineage_join};

/// The row emitted by the anchor: `GET /v1/events?contract_address=`, and a resolver's own rows.
pub const EMITTER: i16 = 0;
/// A `ResolverChanged` row that sets the pointer to the anchor, emitted by another contract.
pub const POINTER_SET: i16 = 1;
/// A `ResolverChanged` row that clears the pointer from the anchor, emitted by another contract
/// and setting it to another resolver.
pub const POINTER_CLEARED: i16 = 2;

/// The lower-cased emitter of `alias`'s row.
pub(super) fn emitter(alias: &str) -> String {
    format!("lower({alias}.raw_fact_ref ->> 'emitting_address')")
}

/// The lower-cased resolver a `ResolverChanged` row of `alias` sets.
pub(super) fn set_resolver(alias: &str) -> String {
    format!("lower({alias}.after_state ->> 'resolver')")
}

/// The lower-cased resolver a `ResolverChanged` row of `alias` replaces.
pub(super) fn cleared_resolver(alias: &str) -> String {
    format!("lower({alias}.before_state ->> 'resolver')")
}

/// Duplicate rules independent of the selected anchor set, for a row aliased `row_alias`.
pub(super) fn push_fixed_duplicate_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
) {
    // A named registrar snapshot describes retained state, not another on-chain action.
    // Filter before keyset pagination; diagnostics bypasses this product-only filter.
    builder.push(aliased(" AND NOT (@ROW@.after_state @> '{\"state_derived\":true,\"registrar_surface_snapshot\":true}'::jsonb)", row_alias));
    // An ENSv2 registry `ExpiryUpdated` whose token the adapter holds no state for is kept for
    // the registry entry row only: it has no name and no resource to describe. Product reads
    // neither list nor count it; diagnostics bypasses this filter.
    builder.push(aliased(
        " AND NOT (@ROW@.event_kind = 'ExpiryChanged' AND @ROW@.logical_name_id IS NULL \
           AND @ROW@.resource_id IS NULL \
           AND @ROW@.after_state @> '{\"token_state_absent\":true}'::jsonb)",
        row_alias,
    ));
    // Wrapping has a canonical authority row, including a positively matched mint. Its
    // derived transfer is an ownership effect only when both owners are retained, nonzero
    // and different; independent ERC-1155 transfers keep their existing behavior.
    builder.push(aliased(
        r#" AND NOT (@ROW@.event_kind = 'TokenControlTransferred'
        AND @ROW@.source_family = 'ens_v1_wrapper_l1'
        AND (@ROW@.after_state @> '{"source_event":"NameWrapped"}'::jsonb
             OR @ROW@.after_state @> '{"source_event":"TransferSingle","wrapper_mint":true,
                "matched_wrapper_completion":{"source_event":"NameWrapped"}}'::jsonb)
        AND NOT COALESCE(
          @ROW@.before_state ->> 'from' ~ '^0x[0-9a-fA-F]{40}$'
          AND @ROW@.after_state ->> 'to' ~ '^0x[0-9a-fA-F]{40}$'
          AND lower(@ROW@.before_state ->> 'from') <> '0x0000000000000000000000000000000000000000'
          AND lower(@ROW@.after_state ->> 'to') <> '0x0000000000000000000000000000000000000000'
          AND lower(@ROW@.before_state ->> 'from') <> lower(@ROW@.after_state ->> 'to'), false))"#,
        row_alias,
    ));
    push_address_pair_filter(builder, row_alias);
    // Registry read copies retain the original control-resource representation.
    // NOT LIKE rather than strpos(...) = 0: PostgreSQL has no statistics for the strpos
    // result and estimates the equality at 0.5% of rows, so a page read looked 200 times
    // smaller than it is and the planner sorted every matching row instead of reading the
    // order index up to the page limit. The marker has no LIKE wildcard or escape character.
    builder.push(aliased(
        " AND @ROW@.event_identity NOT LIKE '%:ResolverChanged:registry-read:%'",
        row_alias,
    ));
}

/// The opening of the registry-fallback handoff rule for a row aliased `row_alias`, up to the
/// `SELECT min(ne.event_identity)` of the subquery that picks one copy of the row's group. The
/// caller pushes the subquery's source of `ne` rows, [`push_handoff_group`], its own membership
/// conditions and `))`. The `handoff` row holds the outer row's group key, and `extra` adds outer
/// columns to it. Handoffs have no separate original: the rule keeps the matching copy with the
/// least identity. NOT LIKE also exposes marker selectivity to the global lineage-join planner.
pub(super) fn push_handoff_head(
    builder: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
    extra: &str,
) {
    builder.push(aliased(
        &format!(
            r#"
        AND (@ROW@.event_identity NOT LIKE '%:ResolverChanged:registry-fallback-handoff:%'
             OR @ROW@.event_identity = (
                 WITH handoff AS (
                     SELECT @ROW@.chain_id, @ROW@.block_number, @ROW@.block_hash,
                            @ROW@.after_state ->> 'node' AS node,
                            split_part(@ROW@.event_identity,
                                ':ResolverChanged:registry-fallback-handoff:', 1) AS origin{extra}
                 )
                 SELECT min(ne.event_identity)
        "#
        ),
        row_alias,
    ));
}

/// The subquery's group conditions on `ne`: the copies of the outer row's group. Handoffs come
/// from RawLogInput, whose chain and block number are required. Equality bounds use the existing
/// chain/block index before matching origin.
pub(super) fn push_handoff_group(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
        AND ne.event_kind = 'ResolverChanged'
        AND strpos(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:') > 0
        AND ne.chain_id = (SELECT chain_id FROM handoff)
        AND ne.block_number = (SELECT block_number FROM handoff)
        AND ne.block_hash IS NOT DISTINCT FROM (SELECT block_hash FROM handoff)
        AND ne.after_state ->> 'node' IS NOT DISTINCT FROM (SELECT node FROM handoff)
        AND split_part(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:', 1)
            = (SELECT origin FROM handoff)
        "#,
    );
}

/// The per-key count increments of one chain's rows in the block range `($2, $3]`, `$1` the
/// chain, as `(anchor_kind, anchor, namespace, event_kind, delta)`. A row counts for each anchor
/// whose event read admits it, under the read's own visibility, canonicality and duplicate rules.
/// The emitter anchor's handoff copy is chosen among the copies that anchor's
/// `contract_address` read admits, and a resolver anchor's among the copies its `resolver` read
/// admits, each within the row's namespace. A row without a block is never counted: every read a
/// stored count serves is bounded by a published block.
pub fn block_event_counts_sql() -> String {
    let (emitter, set, cleared) = (emitter("ne"), set_resolver("ne"), cleared_resolver("ne"));
    let mut builder = QueryBuilder::<Postgres>::new(
        "/* storage:history.block_event_counts */ SELECT membership.anchor_kind, \
         membership.anchor, ne.namespace, ne.event_kind, count(*)::bigint AS delta \
         FROM normalized_events ne ",
    );
    push_history_lineage_join(&mut builder);
    // The arms are those of `resolver_source.rs`, mutually exclusive by priority, so a resolver's
    // rows are the sum of its three anchors.
    builder.push(format!(
        "
        CROSS JOIN LATERAL (
            SELECT {EMITTER}::smallint AS anchor_kind, {emitter} AS anchor
            WHERE {emitter} IS NOT NULL
            UNION ALL
            SELECT {POINTER_SET}::smallint, {set}
            WHERE ne.event_kind = 'ResolverChanged' AND {set} IS NOT NULL
              AND {emitter} IS DISTINCT FROM {set}
            UNION ALL
            SELECT {POINTER_CLEARED}::smallint, {cleared}
            WHERE ne.event_kind = 'ResolverChanged' AND {cleared} IS NOT NULL
              AND {emitter} IS DISTINCT FROM {cleared} AND {set} IS DISTINCT FROM {cleared}
        ) membership
        WHERE ne.consumer_visibility = 'activated'
          AND ne.chain_id = $1 AND ne.block_number > $2 AND ne.block_number <= $3
        "
    ));
    push_history_canonicality_filter(&mut builder, true);
    push_fixed_duplicate_filter(&mut builder, "ne");
    push_handoff_head(
        &mut builder,
        "ne",
        ", membership.anchor_kind, membership.anchor, ne.namespace",
    );
    builder.push(" FROM normalized_events ne ");
    push_history_lineage_join(&mut builder);
    builder.push(" WHERE ne.consumer_visibility = 'activated' ");
    push_handoff_group(&mut builder);
    builder.push(format!(
        "
        AND ne.namespace = (SELECT namespace FROM handoff)
        AND CASE WHEN (SELECT anchor_kind FROM handoff) = {EMITTER}
            THEN {emitter} = (SELECT anchor FROM handoff)
            ELSE {emitter} = (SELECT anchor FROM handoff)
                 OR (ne.event_kind = 'ResolverChanged'
                     AND ({set} = (SELECT anchor FROM handoff)
                          OR {cleared} = (SELECT anchor FROM handoff)))
        END
        "
    ));
    push_history_canonicality_filter(&mut builder, true);
    builder.push(")) GROUP BY 1, 2, 3, 4");
    builder.sql().to_owned()
}

/// One product row for a source-proven adjacent ETH-address event pair. Physical logs and
/// normalized rows stay unchanged; the AddressChanged row keeps its identity and position.
pub(super) fn push_address_pair_filter(query: &mut QueryBuilder<'_, Postgres>, row_alias: &str) {
    // Match a physical predecessor independently of cursors and page batches. The equality
    // keys use normalized_events_block_idx; a block's publication and lineage apply to both.
    query.push(aliased(" AND NOT (@ROW@.event_kind = 'RecordChanged' AND @ROW@.after_state @> '{\"source_event\":\"AddrChanged\",\"record_key\":\"addr:60\",\"selector_key\":\"60\"}'::jsonb AND @ROW@.canonicality_state IN ('canonical','safe','finalized') AND EXISTS (SELECT 1 FROM bigname_phase.normalized_events paired JOIN ", row_alias));
    query.push(PROVEN_GENERATIONS);
    query.push(aliased(r#" AS generation(chain, family, role, address, label, empty_clear)
      ON generation.chain = @ROW@.chain_id AND generation.family = @ROW@.source_family
         AND generation.address = lower(@ROW@.raw_fact_ref ->> 'emitting_address')
      JOIN bigname_phase.manifest_contract_instances declared
        ON declared.manifest_id = @ROW@.source_manifest_id AND declared.chain_id = @ROW@.chain_id
       AND declared.contract_instance_id::text = @ROW@.after_state ->> 'resolver_contract_instance_id'
       AND declared.declaration_kind = 'contract' AND declared.role = generation.role
       AND declared.proxy_kind = 'none'
       AND lower(declared.declared_address) = lower(@ROW@.raw_fact_ref ->> 'emitting_address')
       AND (generation.label IS NULL OR EXISTS (
            SELECT 1 FROM bigname_phase.manifest_versions manifest
            WHERE manifest.manifest_id = declared.manifest_id
              AND manifest.chain_id = declared.chain_id
              AND manifest.deployment_label = generation.label))
       AND (declared.start_block_number IS NULL OR declared.start_block_number <= @ROW@.block_number)
      WHERE paired.chain_id = @ROW@.chain_id AND paired.block_hash = @ROW@.block_hash
        AND paired.transaction_index = @ROW@.transaction_index AND paired.log_index = @ROW@.log_index - 1
        AND paired.block_number = @ROW@.block_number AND paired.transaction_hash = @ROW@.transaction_hash
        AND paired.namespace = @ROW@.namespace AND paired.source_family = @ROW@.source_family
        AND paired.source_manifest_id = @ROW@.source_manifest_id
        AND paired.resource_id IS NOT DISTINCT FROM @ROW@.resource_id
        AND paired.logical_name_id IS NOT DISTINCT FROM @ROW@.logical_name_id
        AND paired.event_kind = 'RecordChanged'
        AND paired.after_state @> '{"source_event":"AddressChanged","record_key":"addr:60","selector_key":"60"}'::jsonb
        AND lower(paired.raw_fact_ref ->> 'emitting_address') = lower(declared.declared_address)
        AND lower(paired.after_state ->> 'resolver') = lower(declared.declared_address)
        AND lower(@ROW@.after_state ->> 'resolver') = lower(declared.declared_address)
        AND paired.after_state ->> 'resolver_contract_instance_id' = @ROW@.after_state ->> 'resolver_contract_instance_id'
        AND lower(paired.after_state ->> 'node') = lower(@ROW@.after_state ->> 'node')
        AND lower(@ROW@.after_state ->> 'node') ~ '^0x[0-9a-f]{64}$'
        AND @ROW@.after_state ->> 'value' ~ '^0x[0-9a-fA-F]{40}$'
        AND (lower(paired.after_state ->> 'value') = lower(@ROW@.after_state ->> 'value')
             OR (generation.empty_clear AND paired.after_state ->> 'address_bytes_hex' = '0x'
                 AND paired.after_state ->> 'coin_type' = '60'
                 AND lower(@ROW@.after_state ->> 'value') = '0x0000000000000000000000000000000000000000'))
        AND paired.consumer_visibility = 'activated'
        AND paired.canonicality_state IN ('canonical','safe','finalized')
        AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage pair_lineage
                    WHERE pair_lineage.chain_id = paired.chain_id
                      AND pair_lineage.block_hash = paired.block_hash
                      AND pair_lineage.canonicality_state IN ('canonical','safe','finalized'))
      ))"#, row_alias));
}

// Each row binds a deployed, non-proxy generation to its declared manifest role. A generic
// match-all ABI admission is insufficient. Empty clears are supported only by modern source.
// A row without a deployment label applies under any of its chain's manifests. The ENSv2
// PublicResolverV2 rows, one per (chain, deployment label) whose compiler input was reviewed,
// need both the exact reviewed address and their deployment label. A stored event count holds
// the pair decision this list made when its block was counted, so an edit here rotates the
// projection content hash.
// (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/deployments/sepolia/PublicResolver.json:L2 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1_mainnet_1a2ac5c/deployments/mainnet/PublicResolver.json:L2 @ ens_v1_mainnet_1a2ac5c@1a2ac5c)
// (upstream: .refs/ens_v1_mainnet_1a2ac5c/contracts/resolvers/profiles/AddrResolver.sol:L23-L54 @ ens_v1_mainnet_1a2ac5c@1a2ac5c)
// (upstream: .refs/ens_v1_sepolia_8209157/deployments/sepolia/PublicResolver.json:L2 @ ens_v1_sepolia_8209157@8209157)
// (upstream: .refs/ens_v1_sepolia_8209157/contracts/resolvers/profiles/AddrResolver.sol:L23-L54 @ ens_v1_sepolia_8209157@8209157)
// (upstream: .refs/ens_v1_sepolia_ac32490/deployments/sepolia/PublicResolver.json:L2 @ ens_v1_sepolia_ac32490@ac32490)
// (upstream: .refs/ens_v1_sepolia_ac32490/contracts/resolvers/profiles/AddrResolver.sol:L23-L54 @ ens_v1_sepolia_ac32490@ac32490)
// (upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/L2Resolver.sol:L5-L32 @ basenames@1809bbc)
// (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L45-L76 @ basenames@1809bbc)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PublicResolverV2.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PublicResolverV2.json:L1271-L1272 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L852-L854 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/PublicResolverV2.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/PublicResolverV2.json:L1287-L1288 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/build-info/solc-0_8_25-8550fee9a7d7605147b869f3868c3d2bc5861ae7.json:L389-L391 @ ens_v2_mainnet_20261002@b1f97b91)
const PROVEN_GENERATIONS: &str = "(VALUES
 ('ethereum-mainnet','ens_v1_resolver_l1','public_resolver','0xf29100983e058b709f3d539b0c765937b804ac15',NULL,true),
 ('ethereum-sepolia','ens_v1_resolver_l1','public_resolver','0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5',NULL,true),
 ('ethereum-mainnet','ens_v1_resolver_l1','public_resolver_231b0ee','0x231b0ee14048e9dccd1d247744d114a4eb5e8e63',NULL,false),
 ('ethereum-sepolia','ens_v1_resolver_l1','public_resolver_8948458','0x8948458626811dd0c23eb25cc74291247077cc51',NULL,false),
 ('ethereum-sepolia','ens_v1_resolver_l1','public_resolver_8fade66','0x8fade66b79cc9f707ab26799354482eb93a5b7dd',NULL,false),
 ('ethereum-sepolia','ens_v2_resolver_l1','public_resolver_v2','0xdc4a563d00f5c3012b699794eb9e13a561be386f','ens_v2_sepolia_20261001',true),
 ('ethereum-mainnet','ens_v2_resolver_l1','public_resolver_v2','0x66f9f566ff2f26309c1bd9134388478a801da3e1','ens_v2_mainnet_20261002',true),
 ('base-mainnet','basenames_base_resolver','resolver','0xc6d566a56a1aff6508b41f6c90ff131615583bcd',NULL,false)
)";

fn aliased(sql: &str, row_alias: &str) -> String {
    sql.replace("@ROW@.", &format!("{row_alias}."))
}
