use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};

pub(in crate::builders) mod query;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    for statement in query::STAGE_V2_LIFECYCLE_EVENTS {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to stage ENSv2 lifecycle events", error)
            })?;
    }
    #[cfg(test)]
    if crate::profile::execute_bound(
        transaction,
        query::BUILD_NAME_CURRENT,
        crate::profile::Stage::NameCurrent,
        &[
            crate::profile::Parameter::Text(chain_id),
            crate::profile::Parameter::I64(target.number),
            crate::profile::Parameter::Text(&target.hash),
        ],
    )
    .await?
    {
        return Ok(());
    }
    sqlx::query(query::BUILD_NAME_CURRENT)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build name_current", error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn product_registration_lookup_is_resource_keyed() {
        let query = super::query::BUILD_NAME_CURRENT;
        assert!(
            !query.contains("registrar_grant.resource_id::text"),
            "the registration-identity lookup defeats the project_events UUID index"
        );
        assert!(
            query.contains("current_wrapper.resource_id = selected_registration.resource_id"),
            "the registration-identity lookup must anchor the selected wrapper by resource_id"
        );
    }
}
