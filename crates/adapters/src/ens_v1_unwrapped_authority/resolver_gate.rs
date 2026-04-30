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
                resolver_profile_admitted(&admission.source_family, &admission.profile)
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

    pub(super) async fn load_for_raw_logs(
        pool: &PgPool,
        raw_logs: &[AuthorityRawLogRow],
    ) -> Result<Self> {
        let mut ens_v1_targets = Vec::<(String, String)>::new();
        let mut basenames_targets = Vec::<(String, String)>::new();
        let mut seen_targets = HashSet::<(String, String, String)>::new();

        for raw_log in raw_logs {
            if !resolver_source_family_has_profiles(&raw_log.source_family) {
                continue;
            }
            let Some(topic0) = raw_log.topics.first() else {
                continue;
            };
            if resolver_fact_families_for_topic0(&raw_log.source_family, topic0).is_empty() {
                continue;
            }

            let address = raw_log.emitting_address.to_ascii_lowercase();
            if !seen_targets.insert((
                raw_log.chain_id.clone(),
                raw_log.source_family.clone(),
                address.clone(),
            )) {
                continue;
            }

            match raw_log.source_family.as_str() {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => {
                    ens_v1_targets.push((raw_log.chain_id.clone(), address));
                }
                SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => {
                    basenames_targets.push((raw_log.chain_id.clone(), address));
                }
                _ => {}
            }
        }

        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions_for_targets(
                pool,
                &ens_v1_targets,
            )
            .await
            .context("failed to load scoped ENSv1 PublicResolver profile admissions")?;
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions_for_targets(
                pool,
                &basenames_targets,
            )
            .await
            .context("failed to load scoped Basenames L2Resolver profile admissions")?,
        );

        Ok(Self::from_admissions(admissions))
    }

    fn from_admissions(admissions: Vec<bigname_manifests::ResolverProfileAdmission>) -> Self {
        let supported_fact_families = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_admitted(&admission.source_family, &admission.profile)
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

        Self {
            supported_fact_families,
        }
    }

    pub(super) fn rejects_resolver_local_fact(&self, raw_log: &AuthorityRawLogRow) -> bool {
        if !resolver_source_family_has_profiles(&raw_log.source_family) {
            return false;
        }
        if raw_log.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
            return false;
        }

        let Some(topic0) = raw_log.topics.first() else {
            return false;
        };
        let fact_families = resolver_fact_families_for_topic0(&raw_log.source_family, topic0);
        if fact_families.is_empty() {
            return false;
        }

        let chain_id = raw_log.chain_id.clone();
        let source_family = raw_log.source_family.clone();
        let emitting_address = raw_log.emitting_address.to_ascii_lowercase();
        !fact_families.iter().any(|fact_family| {
            self.supported_fact_families.contains(&(
                chain_id.clone(),
                source_family.clone(),
                emitting_address.clone(),
                fact_family,
            ))
        })
    }
}

fn resolver_fact_family_key(fact_family: &str) -> Option<&'static str> {
    match fact_family {
        "resolver_record" => Some("resolver_record"),
        "resolver_record:addr" => Some("resolver_record:addr"),
        "resolver_record:multicoin_addr" => Some("resolver_record:multicoin_addr"),
        "resolver_record:name" => Some("resolver_record:name"),
        "resolver_record:text" => Some("resolver_record:text"),
        "resolver_record:abi" => Some("resolver_record:abi"),
        "resolver_record:contenthash" => Some("resolver_record:contenthash"),
        "resolver_record:dns" => Some("resolver_record:dns"),
        "resolver_record:interface" => Some("resolver_record:interface"),
        "resolver_record:data" => Some("resolver_record:data"),
        "resolver_record_version" => Some("resolver_record_version"),
        _ => None,
    }
}

fn resolver_source_family_has_profiles(source_family: &str) -> bool {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => true,
        _ => false,
    }
}

fn resolver_profile_admitted(source_family: &str, profile: &str) -> bool {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => matches!(
            profile,
            "public_resolver_compatible"
                | "public_resolver_wrapper_aware"
                | "public_resolver_legacy_multicoin_dns"
                | "public_resolver_legacy_multicoin"
                | "public_resolver_legacy_eth_addr_text"
                | "public_resolver_legacy_eth_addr"
        ),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => profile == "l2_resolver_compatible",
        _ => false,
    }
}

pub(super) fn resolver_fact_families_for_topic0(
    source_family: &str,
    topic0: &str,
) -> Vec<&'static str> {
    if is_text_changed_topic0(topic0) {
        return match source_family {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => vec!["resolver_record:text", "resolver_record"],
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => vec!["resolver_record"],
            _ => Vec::new(),
        };
    }

    if topic0.eq_ignore_ascii_case(&name_changed_topic0()) {
        return match source_family {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => vec!["resolver_record:name", "resolver_record"],
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => vec!["resolver_record"],
            _ => Vec::new(),
        };
    }

    if topic0.eq_ignore_ascii_case(&addr_changed_topic0()) {
        return match source_family {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => vec!["resolver_record:addr", "resolver_record"],
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => vec!["resolver_record"],
            _ => Vec::new(),
        };
    }

    if topic0.eq_ignore_ascii_case(&address_changed_topic0()) {
        return match source_family {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => {
                vec!["resolver_record:multicoin_addr", "resolver_record"]
            }
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => vec!["resolver_record"],
            _ => Vec::new(),
        };
    }

    if topic0.eq_ignore_ascii_case(&version_changed_topic0()) {
        return match source_family {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => vec!["resolver_record_version"],
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => vec!["resolver_record"],
            _ => Vec::new(),
        };
    }

    if source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
        if topic0.eq_ignore_ascii_case(&abi_changed_topic0()) {
            return vec!["resolver_record:abi", "resolver_record"];
        }
        if topic0.eq_ignore_ascii_case(&content_changed_topic0())
            || topic0.eq_ignore_ascii_case(&contenthash_changed_topic0())
        {
            return vec!["resolver_record:contenthash", "resolver_record"];
        }
        if topic0.eq_ignore_ascii_case(&dns_record_changed_topic0())
            || topic0.eq_ignore_ascii_case(&dns_record_deleted_topic0())
            || topic0.eq_ignore_ascii_case(&dns_zonehash_changed_topic0())
        {
            return vec!["resolver_record:dns", "resolver_record"];
        }
        if topic0.eq_ignore_ascii_case(&interface_changed_topic0()) {
            return vec!["resolver_record:interface", "resolver_record"];
        }
        if topic0.eq_ignore_ascii_case(&data_changed_topic0()) {
            return vec!["resolver_record:data"];
        }
    }

    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolver_profile_gate_keeps_observed_record_facts_without_supported_profile() {
        let supported_resolver = "0x00000000000000000000000000000000000000c1";
        let unsupported_resolver = "0x00000000000000000000000000000000000000c2";
        let eth_only_resolver = "0x00000000000000000000000000000000000000c3";
        let multicoin_resolver = "0x00000000000000000000000000000000000000c4";
        let gate = ResolverProfileGate {
            supported_fact_families: HashSet::from([
                (
                    "ethereum-mainnet".to_owned(),
                    SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
                    supported_resolver.to_owned(),
                    "resolver_record",
                ),
                (
                    "ethereum-mainnet".to_owned(),
                    SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
                    eth_only_resolver.to_owned(),
                    "resolver_record:addr",
                ),
                (
                    "ethereum-mainnet".to_owned(),
                    SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
                    multicoin_resolver.to_owned(),
                    "resolver_record:multicoin_addr",
                ),
            ]),
        };

        assert!(
            !gate.rejects_resolver_local_fact(&resolver_log(
                supported_resolver,
                name_changed_topic0(),
            ))
        );
        assert!(!gate.rejects_resolver_local_fact(&resolver_log(
            unsupported_resolver,
            name_changed_topic0(),
        )));
        assert!(!gate.rejects_resolver_local_fact(&resolver_log(
            supported_resolver,
            version_changed_topic0(),
        )));
        assert!(
            !gate.rejects_resolver_local_fact(&resolver_log(
                eth_only_resolver,
                addr_changed_topic0(),
            ))
        );
        assert!(!gate.rejects_resolver_local_fact(&resolver_log(
            eth_only_resolver,
            address_changed_topic0(),
        )));
        assert!(!gate.rejects_resolver_local_fact(&resolver_log(
            multicoin_resolver,
            address_changed_topic0(),
        )));
        assert_eq!(
            resolver_fact_families_for_topic0(
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
                &data_changed_topic0(),
            ),
            vec!["resolver_record:data"]
        );
        assert_eq!(
            resolver_fact_families_for_topic0(
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
                &pubkey_changed_topic0(),
            ),
            Vec::<&str>::new()
        );
    }

    fn resolver_log(emitting_address: &str, topic0: String) -> AuthorityRawLogRow {
        AuthorityRawLogRow {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: emitting_address.to_owned(),
            topics: vec![
                topic0,
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ],
            data: Vec::new(),
            canonicality_state: CanonicalityState::Canonical,
            source_manifest_id: 1,
            namespace: "ens".to_owned(),
            source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
            manifest_version: 1,
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            contract_role: Some("public_resolver".to_owned()),
        }
    }
}
