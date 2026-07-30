use anyhow::{Context, Result, bail};
use sqlx::{
    Decode, Postgres, Type,
    error::BoxDynError,
    postgres::{PgTypeInfo, PgValueRef},
};

use super::reads::{
    ensure_chain_lineage_path_reaches_stop, load_chain_lineage_path,
    load_lineage_snapshots_for_hashes,
};
use super::types::{CanonicalityState, ChainLineageBlock};

impl CanonicalityState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Canonical => "canonical",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
            Self::Orphaned => "orphaned",
        }
    }

    pub(crate) fn promote_to(self, target: Self) -> Self {
        match target {
            Self::Observed => {
                if self == Self::Orphaned {
                    Self::Observed
                } else {
                    self
                }
            }
            Self::Canonical | Self::Safe | Self::Finalized => {
                if self == Self::Orphaned {
                    return target;
                }

                if self.rank() >= target.rank() {
                    self
                } else {
                    target
                }
            }
            Self::Orphaned => Self::Orphaned,
        }
    }

    pub fn merge_observation(self, incoming: Self) -> Self {
        match incoming {
            Self::Orphaned => Self::Orphaned,
            Self::Observed => {
                if self == Self::Orphaned {
                    Self::Observed
                } else {
                    self
                }
            }
            Self::Canonical | Self::Safe | Self::Finalized => {
                if self == Self::Orphaned {
                    incoming
                } else {
                    self.promote_to(incoming)
                }
            }
        }
    }

    pub const fn rank(self) -> u8 {
        match self {
            Self::Observed => 0,
            Self::Canonical => 1,
            Self::Safe => 2,
            Self::Finalized => 3,
            Self::Orphaned => 4,
        }
    }

    pub fn weakest(states: impl IntoIterator<Item = Self>) -> Option<Self> {
        states.into_iter().min_by_key(|state| state.rank())
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "observed" => Ok(Self::Observed),
            "canonical" => Ok(Self::Canonical),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            "orphaned" => Ok(Self::Orphaned),
            _ => bail!("unknown canonicality_state value {value}"),
        }
    }
}

impl Type<Postgres> for CanonicalityState {
    fn type_info() -> PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <String as Type<Postgres>>::compatible(ty)
    }
}

impl<'r> Decode<'r, Postgres> for CanonicalityState {
    fn decode(value: PgValueRef<'r>) -> std::result::Result<Self, BoxDynError> {
        let value = <String as Decode<Postgres>>::decode(value)?;
        Self::parse(&value).map_err(Into::into)
    }
}

pub(crate) async fn promote_chain_lineage_path(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
    target_state: CanonicalityState,
    require_stop: bool,
) -> Result<Vec<ChainLineageBlock>> {
    let path = load_chain_lineage_path(&mut **executor, chain_id, from_hash, stop_before_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if path.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }
    if require_stop {
        ensure_chain_lineage_path_reaches_stop(chain_id, from_hash, stop_before_hash, &path)?;
    }

    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();

    match target_state {
        CanonicalityState::Observed => {}
        CanonicalityState::Canonical => {
            advance_lineage_path_state(
                executor,
                chain_id,
                &block_hashes,
                &[CanonicalityState::Observed, CanonicalityState::Orphaned],
                CanonicalityState::Canonical,
            )
            .await?;
        }
        CanonicalityState::Safe => {
            advance_lineage_path_to_canonical(executor, chain_id, &block_hashes).await?;
            advance_lineage_path_state(
                executor,
                chain_id,
                &block_hashes,
                &[CanonicalityState::Canonical],
                CanonicalityState::Safe,
            )
            .await?;
        }
        CanonicalityState::Finalized => {
            advance_lineage_path_to_canonical(executor, chain_id, &block_hashes).await?;
            advance_lineage_path_state(
                executor,
                chain_id,
                &block_hashes,
                &[CanonicalityState::Canonical],
                CanonicalityState::Safe,
            )
            .await?;
            advance_lineage_path_state(
                executor,
                chain_id,
                &block_hashes,
                &[CanonicalityState::Safe],
                CanonicalityState::Finalized,
            )
            .await?;
        }
        CanonicalityState::Orphaned => {
            bail!("lineage path promotion cannot target orphaned state");
        }
    }

    load_lineage_snapshots_for_hashes(&mut **executor, chain_id, &block_hashes)
        .await
        .with_context(|| {
            format!(
                "failed to reload promoted lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })
}

async fn advance_lineage_path_to_canonical(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<()> {
    advance_lineage_path_state(
        executor,
        chain_id,
        block_hashes,
        &[CanonicalityState::Observed, CanonicalityState::Orphaned],
        CanonicalityState::Canonical,
    )
    .await
}

async fn advance_lineage_path_state(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    block_hashes: &[String],
    from_states: &[CanonicalityState],
    target_state: CanonicalityState,
) -> Result<()> {
    let from_states = from_states
        .iter()
        .map(|state| state.as_str())
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = $3::canonicality_state
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state::TEXT = ANY($4::TEXT[])
        "#,
    )
    .bind(chain_id)
    .bind(block_hashes)
    .bind(target_state.as_str())
    .bind(from_states)
    .execute(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to advance lineage path for chain {chain_id} to {}",
            target_state.as_str()
        )
    })?;
    Ok(())
}
