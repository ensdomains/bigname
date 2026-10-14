//! One product row for a source-proven adjacent ETH-address event pair. Physical logs and
//! normalized rows stay unchanged; the AddressChanged row keeps its identity and position.

use sqlx::{Postgres, QueryBuilder};

// Each row binds a deployed, non-proxy generation to its declared manifest role. A generic
// match-all ABI admission is insufficient. Empty clears are supported only by modern source.
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
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PublicResolverV2.json:L1271-L1272 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L852-L854 @ ens_v2_sepolia_20261001@07e55a05)
const PROVEN_GENERATIONS: &str = "(VALUES
 ('ethereum-mainnet','ens_v1_resolver_l1','public_resolver','0xf29100983e058b709f3d539b0c765937b804ac15',true),
 ('ethereum-sepolia','ens_v1_resolver_l1','public_resolver','0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5',true),
 ('ethereum-mainnet','ens_v1_resolver_l1','public_resolver_231b0ee','0x231b0ee14048e9dccd1d247744d114a4eb5e8e63',false),
 ('ethereum-sepolia','ens_v1_resolver_l1','public_resolver_8948458','0x8948458626811dd0c23eb25cc74291247077cc51',false),
 ('ethereum-sepolia','ens_v1_resolver_l1','public_resolver_8fade66','0x8fade66b79cc9f707ab26799354482eb93a5b7dd',false),
 ('ethereum-sepolia','ens_v2_resolver_l1','public_resolver_v2','0xdc4a563d00f5c3012b699794eb9e13a561be386f',true),
 ('base-mainnet','basenames_base_resolver','resolver','0xc6d566a56a1aff6508b41f6c90ff131615583bcd',false)
)";

pub(super) fn push_address_pair_filter_for_alias(
    query: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
) {
    // Match a physical predecessor independently of cursors and page batches. The equality
    // keys use normalized_events_block_idx; a block's publication and lineage apply to both.
    query.push(aliased(" AND NOT (@ROW@.event_kind = 'RecordChanged' AND @ROW@.after_state @> '{\"source_event\":\"AddrChanged\",\"record_key\":\"addr:60\",\"selector_key\":\"60\"}'::jsonb AND @ROW@.canonicality_state IN ('canonical','safe','finalized') AND EXISTS (SELECT 1 FROM bigname_phase.normalized_events paired JOIN ", row_alias));
    query.push(PROVEN_GENERATIONS);
    query.push(aliased(r#" AS generation(chain, family, role, address, empty_clear)
      ON generation.chain = @ROW@.chain_id AND generation.family = @ROW@.source_family
         AND generation.address = lower(@ROW@.raw_fact_ref ->> 'emitting_address')
      JOIN bigname_phase.manifest_contract_instances declared
        ON declared.manifest_id = @ROW@.source_manifest_id AND declared.chain_id = @ROW@.chain_id
       AND declared.contract_instance_id::text = @ROW@.after_state ->> 'resolver_contract_instance_id'
       AND declared.declaration_kind = 'contract' AND declared.role = generation.role
       AND declared.proxy_kind = 'none' AND lower(declared.declared_address) = generation.address
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
        AND lower(paired.raw_fact_ref ->> 'emitting_address') = generation.address
        AND lower(paired.after_state ->> 'resolver') = generation.address
        AND lower(@ROW@.after_state ->> 'resolver') = generation.address
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

fn aliased(sql: &str, row_alias: &str) -> String {
    sql.replace("@ROW@.", &format!("{row_alias}."))
}

#[cfg(test)]
#[path = "address_pairs_tests.rs"]
mod tests;
