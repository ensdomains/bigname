use super::*;

impl AuthorityRawLogRow {
    pub(super) fn reference(&self) -> ObservationRef {
        ObservationRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            transaction_hash: Some(self.transaction_hash.clone()),
            transaction_index: Some(self.transaction_index),
            log_index: Some(self.log_index),
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}

impl CanonicalBlockIndex {
    pub(super) fn first_block_at_or_after(
        &self,
        timestamp: OffsetDateTime,
        namespace: &str,
    ) -> Option<BoundaryRef> {
        self.blocks
            .iter()
            .find(|block| block.block_timestamp >= timestamp)
            .map(|block| BoundaryRef {
                chain_id: block.chain_id.clone(),
                block_hash: block.block_hash.clone(),
                block_number: block.block_number,
                block_timestamp: block.block_timestamp,
                canonicality_state: block.canonicality_state,
                namespace: namespace.to_owned(),
            })
    }
}

pub(super) async fn load_canonical_blocks(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<RawBlockSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_blocks
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load canonical raw blocks for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(RawBlockSnapshot {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                block_timestamp: row
                    .try_get("block_timestamp")
                    .context("missing block_timestamp")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
            })
        })
        .collect()
}

pub(super) async fn load_authority_raw_logs(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<Vec<AuthorityRawLogRow>> {
    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for emitter in active_emitters {
        match emitters_by_address.get(&emitter.address) {
            Some(current) if !candidate_precedes(emitter, current) => {}
            _ => {
                emitters_by_address.insert(emitter.address.clone(), emitter.clone());
            }
        }
    }
    let watched_addresses = active_emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let watched_effective_from_blocks = active_emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let watched_effective_to_blocks = active_emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id AS chain_id,
            rl.block_hash AS block_hash,
            rl.block_number AS block_number,
            rb.block_timestamp AS block_timestamp,
            rl.transaction_hash AS transaction_hash,
            rl.transaction_index AS transaction_index,
            rl.log_index AS log_index,
            rl.emitting_address AS emitting_address,
            rl.topics AS topics,
            rl.data AS data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        JOIN raw_blocks rb
          ON rb.chain_id = rl.chain_id
         AND rb.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND lower(rl.emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
          AND EXISTS (
              SELECT 1
              FROM unnest($5::TEXT[], $6::BIGINT[], $7::BIGINT[]) AS watched(
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
    .bind(&watched_addresses)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .bind(&watched_addresses)
    .bind(&watched_effective_from_blocks)
    .bind(&watched_effective_to_blocks)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load ENSv1 unwrapped authority raw logs for chain {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?
                .to_ascii_lowercase();
            let emitter = emitters_by_address.get(&address).with_context(|| {
                format!("missing active emitter metadata for chain {chain} address {address}")
            })?;
            Ok(AuthorityRawLogRow {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                block_timestamp: row
                    .try_get("block_timestamp")
                    .context("missing block_timestamp")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("missing transaction_hash")?,
                transaction_index: row
                    .try_get("transaction_index")
                    .context("missing transaction_index")?,
                log_index: row.try_get("log_index").context("missing log_index")?,
                emitting_address: address,
                topics: row.try_get("topics").context("missing topics")?,
                data: row.try_get("data").context("missing data")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
                source_manifest_id: emitter.source_manifest_id,
                namespace: emitter.namespace.clone(),
                source_family: emitter.source_family.clone(),
                manifest_version: emitter.manifest_version,
                normalizer_version: emitter.normalizer_version.clone(),
            })
        })
        .collect()
}

pub(super) async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv1 unwrapped authority attribution")?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contracts
        .iter()
        .map(|contract| {
            contract.source_manifest_id.with_context(|| {
                format!(
                    "watched contract {} on {} is missing source_manifest_id",
                    contract.address, contract.chain
                )
            })
        })
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();

    let active_manifests = load_active_manifest_metadata(pool, &manifest_ids).await?;
    let mut emitters = Vec::new();
    for watched_contract in watched_contracts {
        let Some(source_manifest_id) = watched_contract.source_manifest_id else {
            continue;
        };
        let Some(manifest) = active_manifests.get(&source_manifest_id) else {
            continue;
        };
        if manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRY_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_WRAPPER_L1
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
        {
            continue;
        }

        let candidate = ActiveEmitter {
            address: watched_contract.address,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            normalizer_version: manifest.normalizer_version.clone(),
            active_from_block_number: watched_contract.active_from_block_number,
            active_to_block_number: watched_contract.active_to_block_number,
            source_rank: source_rank(watched_contract.source),
        };

        emitters.push(candidate);
    }

    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
    });
    Ok(emitters)
}

pub(super) async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv1 unwrapped authority")?;

    rows.into_iter()
        .map(|row| {
            let manifest = ActiveManifestMetadata {
                manifest_id: row.try_get("manifest_id").context("missing manifest_id")?,
                chain: row.try_get("chain").context("missing chain")?,
                namespace: row.try_get("namespace").context("missing namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("missing source_family")?,
                manifest_version: row
                    .try_get("manifest_version")
                    .context("missing manifest_version")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("missing normalizer_version")?,
            };
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

fn source_rank(source: WatchedContractSource) -> i32 {
    match source {
        WatchedContractSource::ManifestRoot => 0,
        WatchedContractSource::ManifestContract => 1,
        WatchedContractSource::DiscoveryEdge => 2,
    }
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (candidate.source_rank, candidate.source_manifest_id)
        < (current.source_rank, current.source_manifest_id)
}
