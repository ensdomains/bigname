use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;

use super::{constants::*, types::RelevantEvent};

#[derive(Clone, Debug)]
pub(super) struct ResolverProfileGate {
    admissions: BTreeMap<(String, String, String, String), String>,
}

impl ResolverProfileGate {
    pub(super) async fn load(pool: &PgPool) -> Result<Self> {
        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions(pool)
                .await
                .context("failed to load ENSv1 PublicResolver profile admissions")?
                .into_iter()
                .collect::<Vec<_>>();
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions(pool)
                .await
                .context("failed to load Basenames L2Resolver profile admissions")?,
        );

        let admissions = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_for_source_family(&admission.source_family)
                    .is_some_and(|profile| admission.profile == profile)
            })
            .map(|admission| {
                (
                    (
                        admission.chain,
                        admission.source_family,
                        normalize_address(&admission.address),
                        admission.fact_family,
                    ),
                    admission.status,
                )
            })
            .collect();

        Ok(Self { admissions })
    }

    fn status_for(
        &self,
        chain_id: &str,
        source_family: &str,
        resolver_address: &str,
        fact_family: &str,
    ) -> Option<&str> {
        self.admissions
            .get(&(
                chain_id.to_owned(),
                source_family.to_owned(),
                normalize_address(resolver_address),
                fact_family.to_owned(),
            ))
            .map(String::as_str)
    }

    pub(super) fn allows_event(&self, event: &RelevantEvent) -> bool {
        let Some(source_family) = resolver_local_source_family(&event.source_family) else {
            return true;
        };

        let Some(fact_family) = resolver_fact_family_for_event(source_family, &event.event_kind)
        else {
            return true;
        };
        let Some(emitting_address) = event.emitting_address.as_deref() else {
            return false;
        };

        self.status_for(
            &event.chain_id,
            source_family,
            emitting_address,
            fact_family,
        ) == Some(RESOLVER_PROFILE_STATUS_SUPPORTED)
    }

    pub(super) fn current_record_status(&self, event: &RelevantEvent) -> Option<&str> {
        if event.event_kind != EVENT_KIND_RESOLVER_CHANGED {
            return None;
        }

        let source_family = resolver_source_family_for_resolver_event(&event.source_family)?;
        let resolver_address = resolver_address_from_event(event)?;
        Some(
            self.status_for(
                &event.chain_id,
                source_family,
                &resolver_address,
                RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            )
            .unwrap_or(RESOLVER_PROFILE_STATUS_PENDING),
        )
    }
}

fn resolver_profile_for_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE),
        _ => None,
    }
}

fn resolver_source_family_for_resolver_event(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

pub(super) fn resolver_local_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

fn resolver_fact_family_for_event(source_family: &str, event_kind: &str) -> Option<&'static str> {
    match (source_family, event_kind) {
        (_, EVENT_KIND_RECORD_CHANGED) => Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD),
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION)
        }
        (SOURCE_FAMILY_BASENAMES_BASE_RESOLVER, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD)
        }
        _ => None,
    }
}

fn resolver_address_from_event(event: &RelevantEvent) -> Option<String> {
    event
        .after_state
        .get("resolver")
        .and_then(Value::as_str)
        .map(normalize_address)
}

fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}
