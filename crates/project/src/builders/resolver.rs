mod alias_summary;
mod binding_summary;
mod declaration_precedence;
mod link_summary;
mod mirror;
mod permission_summary;
mod read_features;
mod section_summaries;

use sqlx::{Postgres, Transaction};

use crate::{
    Marker, ProjectError, Result, resolver_address::PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES,
};
use declaration_precedence::discovery_ctes;
pub(super) use link_summary::DEFAULT_RECORD_NODE;
pub(crate) use link_summary::LINK_DIGEST_SQL;
use mirror::{DIRECT_MIRROR_DECLARED, MIRROR_CLASSIFICATION, MIRROR_ROLE};
use read_features::{DECLARED_READ_FEATURES, IMPLEMENTATION_READ_FEATURES};
use section_summaries::SECTION_SUMMARIES;
pub(crate) use section_summaries::SUMMARY_VERSION;

const SUMMARY_SAMPLE_LIMIT: i32 = 100;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    binding_summary::stage(transaction, chain_id, SUMMARY_SAMPLE_LIMIT, full_rebuild).await?;
    alias_summary::stage(transaction, chain_id, SUMMARY_SAMPLE_LIMIT).await?;
    link_summary::stage(transaction, chain_id, SUMMARY_SAMPLE_LIMIT, target.number).await?;
    permission_summary::stage(transaction, chain_id, SUMMARY_SAMPLE_LIMIT, full_rebuild).await?;

    let discovery = discovery_ctes(full_rebuild);
    #[cfg(test)]
    let discovery = if crate::reference::enabled(transaction).await? {
        std::borrow::Cow::Borrowed(include_str!(
            "../../testdata/sql/builders/resolver/declaration_precedence_previous.sql"
        ))
    } else {
        discovery
    };
    let resolver_build = format!(
        include_str!("resolver/build.sql"),
        discovery = discovery,
        PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES = PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES,
        DIRECT_MIRROR_DECLARED = DIRECT_MIRROR_DECLARED,
        MIRROR_CLASSIFICATION = MIRROR_CLASSIFICATION,
        MIRROR_ROLE = MIRROR_ROLE,
        DECLARED_READ_FEATURES = DECLARED_READ_FEATURES,
        IMPLEMENTATION_READ_FEATURES = IMPLEMENTATION_READ_FEATURES,
        SECTION_SUMMARIES = SECTION_SUMMARIES,
        SUMMARY_VERSION = SUMMARY_VERSION,
    );
    #[cfg(test)]
    if crate::profile::execute_bound(
        transaction,
        &resolver_build,
        crate::profile::Stage::Resolver,
        &[
            crate::profile::Parameter::Text(chain_id),
            crate::profile::Parameter::I64(target.number),
            crate::profile::Parameter::Text(&target.hash),
            crate::profile::Parameter::Bool(full_rebuild),
            crate::profile::Parameter::I32(SUMMARY_SAMPLE_LIMIT),
        ],
    )
    .await?
    {
        return Ok(());
    }
    sqlx::query(&resolver_build)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .bind(full_rebuild)
        .bind(SUMMARY_SAMPLE_LIMIT)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build resolver_current", error))?;
    Ok(())
}

#[cfg(test)]
#[path = "resolver/discovery_tests.rs"]
mod discovery_tests;
