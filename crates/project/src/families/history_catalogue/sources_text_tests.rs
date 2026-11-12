//! The live catalogue's source statements, frozen as they were before their filters were shared
//! with the composition pass's catalogue build: the live path must emit them byte for byte.
use bigname_storage::history_catalogue_contract::{event_key_bloom_sql, event_kind_mask_sql};

use super::{INCOMING, envelope_sql};

const FROZEN_INCOMING: &str = "/* project:history.incoming_sources */
        SELECT DISTINCT source.* FROM normalized_events ne
        CROSS JOIN LATERAL (
            SELECT 0::smallint AS source_kind, ne.logical_name_id AS source_key,
                   ''::text AS resolver_address WHERE ne.logical_name_id IS NOT NULL
            UNION ALL SELECT 1::smallint, ne.resource_id::text, '' WHERE ne.resource_id IS NOT NULL
            UNION ALL SELECT 2::smallint, lower(ne.after_state->>'node'), ''
            WHERE ne.logical_name_id IS NULL AND ne.after_state->>'node' IS NOT NULL
              AND ne.event_kind IN ('RecordChanged','RecordVersionChanged')
              AND ne.source_family IN ('ens_v1_resolver_l1','ens_v2_resolver_l1','basenames_base_resolver')
            UNION ALL SELECT 3::smallint, ne.after_state->>'resolver_record_id', lower(ne.after_state->>'resolver')
            WHERE ne.event_kind='RecordChanged' AND ne.after_state->>'storage_model'='resolver_record_id'
              AND ne.after_state->>'resolver_record_id' IS NOT NULL
              AND ne.after_state->>'resolver' IS NOT NULL
        ) source
        WHERE ne.chain_id=$1 AND ((ne.block_number>$2 AND ne.block_number<=$3)
            OR ($2=-1 AND ne.block_number IS NULL))
          AND ne.consumer_visibility='activated'
          AND ne.canonicality_state IN ('canonical','safe','finalized')";

fn frozen_envelope(predicate: &str) -> String {
    format!("/* project:history.source_envelopes */
                SELECT to_jsonb(fresh) FROM jsonb_to_recordset($2)
                    wanted(source_kind smallint,source_key text,resolver_address text)
                LEFT JOIN project_history_source prior ON prior.chain_id=$1
                  AND prior.source_kind=wanted.source_kind AND prior.source_key=wanted.source_key
                  AND prior.resolver_address=wanted.resolver_address
                CROSS JOIN LATERAL (
                    SELECT min(COALESCE(ne.block_number/256,-1)) AS first_bucket,
                           max(COALESCE(ne.block_number/256,-1)) AS last_bucket,
                           bit_or({kind_mask}) AS event_mask, bit_or({bloom}) AS key_bloom
                    FROM normalized_events ne
                    WHERE ne.chain_id=$1 AND {predicate}
                      AND (ne.block_number<=$3 OR ne.block_number IS NULL)
                      AND (prior.chain_id IS NULL OR ne.block_number>$4)
                      AND ne.consumer_visibility='activated'
                      AND ne.canonicality_state IN ('canonical','safe','finalized')
                      AND (ne.block_hash IS NULL OR EXISTS (SELECT 1 FROM chain_lineage lineage
                        WHERE lineage.chain_id=ne.chain_id AND lineage.block_hash=ne.block_hash
                          AND lineage.canonicality_state IN ('canonical','safe','finalized')))
                ) observed
                CROSS JOIN LATERAL (SELECT least(prior.first_bucket,observed.first_bucket) AS first_bucket,
                    greatest(prior.last_bucket,observed.last_bucket) AS last_bucket,
                    COALESCE(prior.event_mask,0)|COALESCE(observed.event_mask,0) AS event_mask,
                    COALESCE(prior.key_bloom,B'0'::bit(256))|COALESCE(observed.key_bloom,B'0'::bit(256)) AS key_bloom) merged
                CROSS JOIN LATERAL (SELECT $1::text AS chain_id,wanted.source_kind,wanted.source_key,
                    wanted.resolver_address,merged.first_bucket,merged.last_bucket,
                    CASE WHEN merged.first_bucket IS NULL THEN 'empty'::int8range
                         ELSE int8range(merged.first_bucket,merged.last_bucket+1,'[)') END AS bucket_range,
                    merged.event_mask,merged.key_bloom) fresh",
                kind_mask=event_kind_mask_sql("ne.event_kind"),bloom=event_key_bloom_sql("ne"))
}

#[test]
fn the_live_source_statements_are_unchanged() {
    assert_eq!(INCOMING, FROZEN_INCOMING);
    for predicate in [
        "ne.logical_name_id=wanted.source_key",
        "lower(x)=wanted.source_key",
    ] {
        assert_eq!(envelope_sql(predicate), frozen_envelope(predicate));
    }
}
