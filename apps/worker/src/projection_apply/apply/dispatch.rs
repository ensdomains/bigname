use anyhow::{Result, bail};
use sqlx::PgPool;

use crate::{
    address_names, children, name_current, permissions, primary_name,
    primary_name::rebuild_heartbeat::LoopHeartbeat, record_inventory, resolver,
};

use super::{ClaimedInvalidation, optional_payload_str, payload_str};

pub(super) async fn apply_one(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<()> {
    match invalidation.projection.as_str() {
        "name_current" => match loop_heartbeat.as_deref_mut() {
            Some(loop_heartbeat) => {
                name_current::rebuild_name_current_with_heartbeat(
                    pool,
                    Some(&invalidation.projection_key),
                    loop_heartbeat,
                )
                .await?;
            }
            None => {
                name_current::rebuild_name_current(pool, Some(&invalidation.projection_key))
                    .await?;
            }
        },
        "children_current" => match loop_heartbeat.as_deref_mut() {
            Some(loop_heartbeat) => {
                children::rebuild_children_current_with_heartbeat(
                    pool,
                    Some(&invalidation.projection_key),
                    loop_heartbeat,
                )
                .await?;
            }
            None => {
                children::rebuild_children_current(pool, Some(&invalidation.projection_key))
                    .await?;
            }
        },
        "permissions_current" => match loop_heartbeat.as_deref_mut() {
            Some(loop_heartbeat) => {
                permissions::rebuild_permissions_current_with_heartbeat(
                    pool,
                    Some(&invalidation.projection_key),
                    loop_heartbeat,
                )
                .await?;
            }
            None => {
                permissions::rebuild_permissions_current(pool, Some(&invalidation.projection_key))
                    .await?;
            }
        },
        "record_inventory_current" => {
            match loop_heartbeat.as_deref_mut() {
                Some(loop_heartbeat) => {
                    record_inventory::rebuild_record_inventory_current_with_heartbeat(
                        pool,
                        Some(&invalidation.projection_key),
                        loop_heartbeat,
                    )
                    .await?;
                }
                None => {
                    record_inventory::rebuild_record_inventory_current(
                        pool,
                        Some(&invalidation.projection_key),
                    )
                    .await?;
                }
            }
            if let Some(config) = text_hydration_config {
                let hydration_summary = match loop_heartbeat.as_deref_mut() {
                    Some(loop_heartbeat) => {
                        record_inventory::hydrate_record_inventory_text_values_with_heartbeat(
                            pool,
                            Some(&invalidation.projection_key),
                            config.clone(),
                            loop_heartbeat,
                        )
                        .await?
                    }
                    None => {
                        record_inventory::hydrate_record_inventory_text_values(
                            pool,
                            Some(&invalidation.projection_key),
                            config.clone(),
                        )
                        .await?
                    }
                };
                record_inventory::log_text_hydration_summary(
                    Some(&invalidation.projection_key),
                    &hydration_summary,
                );
            }
        }
        "resolver_current" => {
            let chain_id = payload_str(&invalidation.key_payload, "chain_id")?;
            let resolver_address = payload_str(&invalidation.key_payload, "resolver_address")?;
            match loop_heartbeat.as_deref_mut() {
                Some(loop_heartbeat) => {
                    resolver::rebuild_resolver_current_with_heartbeat(
                        pool,
                        Some(chain_id),
                        Some(resolver_address),
                        loop_heartbeat,
                    )
                    .await?;
                }
                None => {
                    resolver::rebuild_resolver_current(
                        pool,
                        Some(chain_id),
                        Some(resolver_address),
                    )
                    .await?;
                }
            }
        }
        "address_names_current" => {
            if let Some(logical_name_id) =
                optional_payload_str(&invalidation.key_payload, "logical_name_id")
            {
                let address = payload_str(&invalidation.key_payload, "address")?;
                match loop_heartbeat.as_deref_mut() {
                    Some(loop_heartbeat) => {
                        address_names::rebuild_address_names_current_logical_names_with_heartbeat(
                            pool,
                            address,
                            &[logical_name_id.to_owned()],
                            loop_heartbeat,
                        )
                        .await?;
                    }
                    None => {
                        address_names::rebuild_address_names_current_logical_name(
                            pool,
                            address,
                            logical_name_id,
                        )
                        .await?;
                    }
                }
            } else {
                match loop_heartbeat.as_deref_mut() {
                    Some(loop_heartbeat) => {
                        address_names::rebuild_address_names_current_with_heartbeat(
                            pool,
                            Some(&invalidation.projection_key),
                            loop_heartbeat,
                        )
                        .await?;
                    }
                    None => {
                        address_names::rebuild_address_names_current(
                            pool,
                            Some(&invalidation.projection_key),
                        )
                        .await?;
                    }
                }
            }
        }
        "primary_names_current" => {
            let address = payload_str(&invalidation.key_payload, "address")?;
            let namespace = payload_str(&invalidation.key_payload, "namespace")?;
            let coin_type = payload_str(&invalidation.key_payload, "coin_type")?;
            match loop_heartbeat.as_deref_mut() {
                Some(loop_heartbeat) => {
                    primary_name::rebuild_primary_names_current_with_heartbeat(
                        pool,
                        Some(address),
                        Some(namespace),
                        Some(coin_type),
                        loop_heartbeat,
                    )
                    .await?;
                }
                None => {
                    primary_name::rebuild_primary_names_current(
                        pool,
                        Some(address),
                        Some(namespace),
                        Some(coin_type),
                    )
                    .await?;
                }
            }
        }
        projection => bail!("unsupported projection invalidation family {projection}"),
    }

    Ok(())
}
