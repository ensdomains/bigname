use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct ResolverProfileGate {
    supported_fact_families: HashSet<(String, String, String, &'static str)>,
}

impl ResolverProfileGate {
    pub(super) async fn load(pool: &PgPool) -> Result<Self> {
        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions(pool)
                .await
                .context("failed to load ENSv1 PublicResolver profile admissions")?;
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions(pool)
                .await
                .context("failed to load Basenames L2Resolver profile admissions")?,
        );
        let supported_fact_families = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_for_source_family(&admission.source_family)
                    .is_some_and(|profile| admission.profile == profile)
                    && admission.status == "supported"
            })
            .filter_map(|admission| {
                resolver_fact_family_key(&admission.fact_family).map(|fact_family| {
                    (
                        admission.chain,
                        admission.source_family,
                        admission.address.to_ascii_lowercase(),
                        fact_family,
                    )
                })
            })
            .collect();

        Ok(Self {
            supported_fact_families,
        })
    }

    pub(super) fn rejects_resolver_local_fact(&self, raw_log: &AuthorityRawLogRow) -> bool {
        if resolver_profile_for_source_family(&raw_log.source_family).is_none() {
            return false;
        }

        let Some(topic0) = raw_log.topics.first() else {
            return false;
        };
        let Some(fact_family) = resolver_fact_family_for_topic0(&raw_log.source_family, topic0)
        else {
            return false;
        };

        !self.supported_fact_families.contains(&(
            raw_log.chain_id.clone(),
            raw_log.source_family.clone(),
            raw_log.emitting_address.to_ascii_lowercase(),
            fact_family,
        ))
    }
}

fn resolver_fact_family_key(fact_family: &str) -> Option<&'static str> {
    match fact_family {
        "resolver_record" => Some("resolver_record"),
        "resolver_record_version" => Some("resolver_record_version"),
        _ => None,
    }
}

fn resolver_profile_for_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some("public_resolver_compatible"),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some("l2_resolver_compatible"),
        _ => None,
    }
}

fn resolver_fact_family_for_topic0(source_family: &str, topic0: &str) -> Option<&'static str> {
    if topic0.eq_ignore_ascii_case(&text_changed_topic0())
        || topic0.eq_ignore_ascii_case(&name_changed_topic0())
        || topic0.eq_ignore_ascii_case(&addr_changed_topic0())
        || topic0.eq_ignore_ascii_case(&address_changed_topic0())
    {
        return Some("resolver_record");
    }

    if topic0.eq_ignore_ascii_case(&version_changed_topic0()) {
        return match source_family {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some("resolver_record_version"),
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some("resolver_record"),
            _ => None,
        };
    }

    None
}
