use bigname_domain::normalization::normalize_name;
use sqlx::{Postgres, Row, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    // Primary-name rows are keyed by the current reverse tuple and its current name() claim.
    // Resolver history matters only for that tuple's current reverse node, so no historical
    // resolver-pointer dependent can change another address's primary-name row.
    stage_claim_normalization(transaction).await?;
    sqlx::query(BUILD_PRIMARY_NAMES)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build primary_names_current", error))?;
    Ok(())
}

pub(in crate::builders) const BUILD_PRIMARY_NAMES: &str = r#"/* project:builders.primary_names.build_primary_names */
        WITH reverse_candidates AS (
            SELECT event.*,
                   event.after_state ->> 'address' AS address,
                   event.after_state ->> 'coin_type' AS coin_type,
                   event.after_state ->> 'namespace' AS claim_namespace
            FROM project_events event
            WHERE event.event_kind = 'ReverseChanged'
              AND event.after_state ->> 'address' IS NOT NULL
              AND event.after_state ->> 'coin_type' IS NOT NULL
              AND event.after_state ->> 'namespace' IS NOT NULL
        ),
        latest_reverse AS (
            SELECT DISTINCT ON (lower(address), coin_type, claim_namespace) *
            FROM reverse_candidates
            ORDER BY lower(address), coin_type, claim_namespace,
                     block_number DESC NULLS LAST,
                     transaction_index DESC NULLS LAST,
                     log_index DESC NULLS LAST,
                     normalized_event_id DESC
        ),
        claim_candidates AS (
            SELECT event.*,
                   lower(event.after_state -> 'primary_claim_source' ->> 'address')
                       AS claim_address,
                   event.after_state -> 'primary_claim_source' ->> 'coin_type'
                       AS claim_coin_type,
                   event.after_state -> 'primary_claim_source' ->> 'namespace'
                       AS claim_namespace
            FROM project_events event
            WHERE event.event_kind = 'RecordChanged'
              AND event.after_state ? 'primary_claim_source'
        ),
        latest_claim AS (
            SELECT DISTINCT ON (claim_address, claim_coin_type, claim_namespace) *
            FROM claim_candidates
            ORDER BY claim_address, claim_coin_type, claim_namespace,
                     block_number DESC NULLS LAST,
                     transaction_index DESC NULLS LAST,
                     log_index DESC NULLS LAST,
                     normalized_event_id DESC
        )
        INSERT INTO project_stage_primary_names_current (
            address, coin_type, namespace, claim_status, raw_claim_name,
            claim_name_is_normalized, unsupported_reason, claim_provenance
        )
        SELECT lower(reverse.address),
               reverse.coin_type,
               reverse.claim_namespace,
               COALESCE(normalized.claim_status, 'not_found'),
               normalized.raw_claim_name,
               COALESCE(normalized.claim_name_is_normalized, false),
               normalized.unsupported_reason,
               COALESCE(
                   claim.after_state -> 'primary_claim_source' -> 'claim_provenance',
                   reverse.after_state -> 'claim_provenance',
                   '{}'::jsonb
               ) || jsonb_strip_nulls(jsonb_build_object(
                   'chain_id', $1,
                   'reverse_event_id', reverse.normalized_event_id,
                   'claim_event_id', claim.normalized_event_id,
                   'resolver_event_id', resolver.normalized_event_id,
                   'reverse_node', lower(reverse.after_state ->> 'reverse_node'),
                   'resolver_address', resolver.resolver_address,
                   'target_block_number', $2,
                   'target_block_hash', $3,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ))
        FROM latest_reverse reverse
        LEFT JOIN latest_claim direct_claim
          ON direct_claim.claim_address = lower(reverse.address)
         AND direct_claim.claim_coin_type = reverse.coin_type
         AND direct_claim.claim_namespace = reverse.claim_namespace
        LEFT JOIN LATERAL (
            SELECT event.normalized_event_id,
                   lower(event.after_state ->> 'resolver') AS resolver_address
            FROM project_events event
            WHERE event.event_kind = 'ResolverChanged'
              AND event.namespace = reverse.namespace
              AND lower(event.after_state ->> 'node') =
                  lower(reverse.after_state ->> 'reverse_node')
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) resolver ON TRUE
        LEFT JOIN LATERAL (
            SELECT event.normalized_event_id, event.after_state
            FROM project_events event
            WHERE reverse.after_state ->> 'source_event' = 'ReverseClaimed'
              AND event.namespace = reverse.namespace
              AND lower(event.after_state ->> 'node') =
                  lower(reverse.after_state ->> 'reverse_node')
              AND lower(event.after_state ->> 'resolver') = resolver.resolver_address
              AND (event.event_kind = 'RecordVersionChanged'
                   OR (event.event_kind = 'RecordChanged'
                       AND event.after_state ->> 'source_event' = 'NameChanged'
                       AND event.after_state ->> 'record_key' = 'name'))
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) node_claim ON TRUE
        LEFT JOIN LATERAL (
            SELECT node_claim.normalized_event_id, node_claim.after_state
            WHERE reverse.after_state ->> 'source_event' = 'ReverseClaimed'
            UNION ALL
            SELECT direct_claim.normalized_event_id, direct_claim.after_state
            WHERE reverse.after_state ->> 'source_event' IS DISTINCT FROM 'ReverseClaimed'
        ) claim ON TRUE
        LEFT JOIN project_primary_claim_normalization normalized
          ON normalized.normalized_event_id = claim.normalized_event_id
        ORDER BY lower(reverse.address), reverse.coin_type, reverse.claim_namespace
        "#;

async fn stage_claim_normalization(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "/* project:builders.primary_names.stage_claim_normalization.create_primary_claim_normalization */ CREATE TEMP TABLE project_primary_claim_normalization (
             normalized_event_id bigint PRIMARY KEY,
             claim_status text NOT NULL,
             raw_claim_name text,
             claim_name_is_normalized boolean NOT NULL,
             unsupported_reason text
         ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to create primary-name normalization stage", error)
    })?;

    let claims = sqlx::query(
        "/* project:builders.primary_names.stage_claim_normalization.select_claims */ SELECT normalized_event_id,
                CASE WHEN jsonb_typeof(after_state -> 'raw_name') = 'string'
                     THEN after_state ->> 'raw_name' END AS raw_name,
                (after_state ? 'raw_name_bytes'
                 OR COALESCE(jsonb_typeof(after_state -> 'raw_name') = 'object', false))
                    AS has_raw_name_bytes
         FROM project_events event
         WHERE event.event_kind = 'RecordChanged'
           AND (event.after_state ? 'primary_claim_source'
                OR (event.after_state ->> 'source_event' = 'NameChanged'
                    AND event.after_state ->> 'record_key' = 'name'
                    AND EXISTS (
                        SELECT 1 FROM project_events reverse
                        WHERE reverse.event_kind = 'ReverseChanged'
                          AND reverse.after_state ->> 'source_event' = 'ReverseClaimed'
                          AND reverse.namespace = event.namespace
                          AND lower(reverse.after_state ->> 'reverse_node') =
                              lower(event.after_state ->> 'node')
                    )))",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to load primary-name normalization inputs", error)
    })?;
    for row in claims {
        let normalized_event_id =
            row.try_get::<i64, _>("normalized_event_id")
                .map_err(|error| {
                    ProjectError::database("failed to decode primary-name event ID", error)
                })?;
        let raw_name = row
            .try_get::<Option<String>, _>("raw_name")
            .map_err(|error| {
                ProjectError::database("failed to decode primary-name raw claim", error)
            })?;
        let has_raw_name_bytes = row
            .try_get::<bool, _>("has_raw_name_bytes")
            .map_err(|error| {
                ProjectError::database("failed to decode primary-name byte marker", error)
            })?;
        let claim = classify_claim(raw_name.as_deref(), has_raw_name_bytes);
        sqlx::query(
            "/* project:builders.primary_names.stage_claim_normalization.insert_primary_claim_normalization */ INSERT INTO project_primary_claim_normalization (
                 normalized_event_id, claim_status, raw_claim_name,
                 claim_name_is_normalized, unsupported_reason
             ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(normalized_event_id)
        .bind(claim.status)
        .bind(claim.raw_name)
        .bind(claim.is_normalized)
        .bind(claim.unsupported_reason)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to stage primary-name normalization", error)
        })?;
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct ClaimClassification<'a> {
    status: &'static str,
    raw_name: Option<&'a str>,
    is_normalized: bool,
    unsupported_reason: Option<&'static str>,
}

fn classify_claim(raw_name: Option<&str>, has_raw_name_bytes: bool) -> ClaimClassification<'_> {
    let Some(raw_name) = raw_name else {
        return ClaimClassification {
            status: if has_raw_name_bytes {
                "unsupported"
            } else {
                "not_found"
            },
            raw_name: None,
            is_normalized: false,
            unsupported_reason: has_raw_name_bytes.then_some("claim_name_not_decodable"),
        };
    };
    if raw_name.is_empty() || raw_name.chars().all(char::is_whitespace) {
        return ClaimClassification {
            status: "not_found",
            raw_name: None,
            is_normalized: false,
            unsupported_reason: None,
        };
    }
    match normalize_name(raw_name) {
        Ok(normalized) => ClaimClassification {
            status: "success",
            raw_name: Some(raw_name),
            is_normalized: normalized.normalized_name.as_bytes() == raw_name.as_bytes(),
            unsupported_reason: None,
        },
        Err(_) => ClaimClassification {
            status: "invalid_name",
            raw_name: Some(raw_name),
            is_normalized: false,
            unsupported_reason: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_primary_name_claims_with_the_retained_normalization_boundary() {
        assert_eq!(
            classify_claim(Some("alice.eth"), false),
            ClaimClassification {
                status: "success",
                raw_name: Some("alice.eth"),
                is_normalized: true,
                unsupported_reason: None,
            }
        );
        assert_eq!(classify_claim(Some("Alice.eth"), false).status, "success");
        assert!(!classify_claim(Some("Alice.eth"), false).is_normalized);
        assert_eq!(
            classify_claim(Some("bad name.eth"), false).status,
            "invalid_name"
        );
        assert_eq!(classify_claim(Some(" \t"), false).status, "not_found");
        assert_eq!(
            classify_claim(None, true).unsupported_reason,
            Some("claim_name_not_decodable")
        );
    }
}
