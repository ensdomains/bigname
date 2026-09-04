use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};

mod query;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
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
        assert!(
            query.contains("registrar_grant.resource_id = lifecycle.registrar_resource_id"),
            "the registration-identity lookup must anchor the registrar grant by resource_id"
        );
    }
}
