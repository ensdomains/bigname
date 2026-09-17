//! Bind a fresh hackathon intake start to the already fingerprint-verified corpus.
use super::*;

/// Call only after the runtime corpus has matched its binary-approved fingerprint.
/// Existing cursor identity checks still reject changing an initialized source.
pub fn bind_profile_start(
    chains: &mut [ChainConfig],
    repository: &bigname_manifests::ManifestRepository,
    profile: &str,
) -> RunnerResult<()> {
    let has_sepolia_start = chains.iter().any(|chain| {
        chain.chain_id == "ethereum-sepolia"
            && chain.sources.iter().any(|s| s.start_block_number > 0)
    });
    if !has_sepolia_start {
        return Ok(());
    }
    let fail = || {
        RunnerError::new(
            ErrorKind::Configuration,
            "nonzero Sepolia intake requires the binary-approved sepolia-hackathon corpus and its exact earliest declared start",
        )
    };
    if profile != "sepolia-hackathon" || chains.len() != 1 {
        return Err(fail());
    }
    let mut starts = Vec::new();
    for loaded in repository.manifests() {
        let manifest = &loaded.manifest;
        if manifest.chain != "ethereum-sepolia" || manifest.namespace != "ens" {
            return Err(fail());
        }
        for start in manifest
            .roots
            .iter()
            .map(|r| r.start_block)
            .chain(manifest.contracts.iter().map(|c| c.start_block))
        {
            starts.push(i64::try_from(start.ok_or_else(fail)?).map_err(|_| fail())?);
        }
    }
    let floor = starts
        .into_iter()
        .min()
        .filter(|v| *v > 0)
        .ok_or_else(fail)?;
    for chain in chains {
        let mut sources = chain.sources.to_vec();
        for source in &mut sources {
            if source.start_block_number != floor
                || normalized_source_kind(&source.source_kind) != "drpc"
                || source.seed_basis != SeedBasis::EthereumHead
            {
                return Err(fail());
            }
            source.admitted_hackathon_start = Some(floor);
        }
        *chain = ChainConfig::new(chain.chain_id.clone(), sources, chain.verify_before_live)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn source(start: i64) -> SourceConfig {
        SourceConfig::new(
            "ethereum-sepolia",
            "hackathon-intake",
            "drpc",
            SeedBasis::EthereumHead,
            start,
            "https://rpc.invalid",
        )
        .unwrap()
    }
    fn repository() -> bigname_manifests::ManifestRepository {
        bigname_manifests::load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../manifests/sepolia-hackathon"),
        )
        .unwrap()
    }
    #[test]
    fn hackathon_start_binds_exact_corpus_floor_and_intake_clone() {
        let mut chains =
            vec![ChainConfig::new("ethereum-sepolia", vec![source(11_626_442)], false).unwrap()];
        assert!(!chains[0].sources[0].sepolia_start_is_admitted());
        assert!(
            crate::verify_phase::validate_reported_level(
                "ethereum-sepolia",
                &chains[0].sources,
                Some(crate::phase::VerificationLevel::QuickSynced),
            )
            .is_err()
        );
        bind_profile_start(&mut chains, &repository(), "sepolia-hackathon").unwrap();
        assert!(chains[0].sources[0].sepolia_start_is_admitted());
        assert!(chains[0].intake_sources()[0].sepolia_start_is_admitted());
        assert_eq!(chains[0].sources[0].start_block_number, 11_626_442);
        crate::verify_phase::validate_reported_level(
            "ethereum-sepolia",
            &chains[0].sources,
            Some(crate::phase::VerificationLevel::QuickSynced),
        )
        .unwrap();
    }
    #[test]
    fn hackathon_start_rejects_other_profile_and_later_or_earlier_start() {
        for (profile, start) in [
            ("sepolia", 11_626_442),
            ("mainnet", 11_626_442),
            ("sepolia-hackathon", 11_626_441),
            ("sepolia-hackathon", 11_626_443),
        ] {
            let mut chains =
                vec![ChainConfig::new("ethereum-sepolia", vec![source(start)], false).unwrap()];
            assert!(bind_profile_start(&mut chains, &repository(), profile).is_err());
        }
    }
    #[test]
    fn hackathon_start_does_not_change_zero_start() {
        let mut chains =
            vec![ChainConfig::new("ethereum-sepolia", vec![source(0)], false).unwrap()];
        bind_profile_start(&mut chains, &repository(), "sepolia").unwrap();
        assert!(chains[0].sources[0].sepolia_start_is_admitted());
        assert_eq!(chains[0].sources[0].start_block_number, 0);
    }
}
