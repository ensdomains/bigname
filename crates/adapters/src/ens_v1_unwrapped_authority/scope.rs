use std::collections::HashSet;

use super::*;
use super::{ids::new_owner_topic0, migration_guard::registry_new_owner_child_node_from_topics};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AuthorityRawLogSourceScopeTarget {
    pub(super) source_family: String,
    pub(super) address: String,
    pub(super) effective_from_block: i64,
    pub(super) effective_to_block: i64,
}

pub(super) fn normalized_authority_source_scope_targets(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<AuthorityRawLogSourceScopeTarget> {
    source_scope
        .iter()
        .map(
            |(source_family, address, effective_from_block, effective_to_block)| {
                AuthorityRawLogSourceScopeTarget {
                    source_family: source_family.clone(),
                    address: address.to_ascii_lowercase(),
                    effective_from_block: *effective_from_block,
                    effective_to_block: *effective_to_block,
                }
            },
        )
        .collect()
}

pub(super) async fn load_migrated_registry_nodes_before_block(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    before_block: i64,
) -> Result<HashSet<String>> {
    let current_registry_emitters = active_emitters
        .iter()
        .filter(|emitter| {
            emitter.source_family == SOURCE_FAMILY_ENS_V1_REGISTRY_L1
                && emitter.contract_role.as_deref() == Some("registry")
        })
        .collect::<Vec<_>>();
    if current_registry_emitters.is_empty() {
        return Ok(HashSet::new());
    }

    let addresses = current_registry_emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let from_blocks = current_registry_emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let to_blocks = current_registry_emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT rl.topics AS topics
        FROM raw_logs rl
        WHERE rl.chain_id = $1
          AND lower(rl.emitting_address) = ANY($2::TEXT[])
          AND rl.block_number < $3
          AND rl.topics[1] = $4
          AND EXISTS (
              SELECT 1
              FROM unnest($2::TEXT[], $5::BIGINT[], $6::BIGINT[]) AS watched(
                  address,
                  effective_from_block,
                  effective_to_block
              )
              WHERE watched.address = lower(rl.emitting_address)
                AND rl.block_number BETWEEN watched.effective_from_block
                    AND watched.effective_to_block
          )
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index
        "#,
    )
    .bind(chain)
    .bind(&addresses)
    .bind(before_block)
    .bind(new_owner_topic0())
    .bind(&from_blocks)
    .bind(&to_blocks)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load ENSv1 registry migration markers before block {before_block}")
    })?;

    rows.into_iter()
        .map(|row| {
            let topics = row
                .try_get::<Vec<String>, _>("topics")
                .context("missing topics")?;
            registry_new_owner_child_node_from_topics(&topics)
        })
        .collect()
}

pub(super) fn scoped_ranges_for_active_emitters(
    source_scope: &[AuthorityRawLogSourceScopeTarget],
    active_emitters: &[ActiveEmitter],
) -> Result<Vec<AuthorityRawLogSourceScopeTarget>> {
    let mut ranges = Vec::new();
    for target in source_scope {
        if target.effective_to_block < target.effective_from_block {
            bail!(
                "ENSv1 unwrapped authority source scope range {}..={} is invalid for {} {}",
                target.effective_from_block,
                target.effective_to_block,
                target.source_family,
                target.address
            );
        }
        if active_emitters
            .iter()
            .any(|emitter| source_scope_target_intersects_active_emitter(target, emitter))
        {
            ranges.push(target.clone());
        }
    }
    Ok(ranges)
}

fn source_scope_target_intersects_active_emitter(
    target: &AuthorityRawLogSourceScopeTarget,
    emitter: &ActiveEmitter,
) -> bool {
    if target.source_family != emitter.source_family || target.address != emitter.address {
        return false;
    }

    let emitter_from = emitter.active_from_block_number.unwrap_or(0);
    let emitter_to = emitter.active_to_block_number.unwrap_or(i64::MAX);
    target.effective_from_block <= emitter_to && emitter_from <= target.effective_to_block
}

pub(super) fn emitter_for_block_and_scope<'a>(
    emitters: &'a [ActiveEmitter],
    block_number: i64,
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
) -> Option<&'a ActiveEmitter> {
    let Some(source_scope) = source_scope else {
        return emitters
            .iter()
            .filter(|emitter| emitter_active_at_block(emitter, block_number))
            .min_by(|left, right| {
                (left.source_rank, left.source_manifest_id)
                    .cmp(&(right.source_rank, right.source_manifest_id))
            });
    };

    emitters
        .iter()
        .filter(|emitter| emitter_active_at_block(emitter, block_number))
        .filter(|emitter| {
            source_scope.iter().any(|target| {
                target.source_family == emitter.source_family
                    && target.address == emitter.address
                    && block_number >= target.effective_from_block
                    && block_number <= target.effective_to_block
            })
        })
        .min_by(|left, right| {
            (left.source_rank, left.source_manifest_id)
                .cmp(&(right.source_rank, right.source_manifest_id))
        })
}

fn emitter_active_at_block(emitter: &ActiveEmitter, block_number: i64) -> bool {
    emitter
        .active_from_block_number
        .is_none_or(|active_from| block_number >= active_from)
        && emitter
            .active_to_block_number
            .is_none_or(|active_to| block_number <= active_to)
}
