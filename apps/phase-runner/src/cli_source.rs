use crate::{
    config::{SeedBasis, SourceConfig, SourceRole},
    error::{ErrorKind, RunnerError, RunnerResult},
};

pub(super) fn parse_source(specification: &str) -> RunnerResult<SourceConfig> {
    let (descriptor, environment_name) = specification
        .split_once('=')
        .ok_or_else(|| invalid_source("missing =URL_ENV", specification))?;
    if environment_name.trim().is_empty() {
        return Err(invalid_source(
            "URL environment name is empty",
            specification,
        ));
    }
    let fields = descriptor.split(':').collect::<Vec<_>>();
    if !matches!(fields.len(), 5 | 6) {
        return Err(invalid_source(
            "expected CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV",
            specification,
        ));
    }
    let endpoint = std::env::var(environment_name).map_err(|_| {
        RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "source {} for chain {} requires environment variable {environment_name}",
                fields[1], fields[0]
            ),
        )
    })?;
    let start_block_number = fields[4]
        .parse::<i64>()
        .map_err(|_| invalid_source("START_BLOCK is not an integer", specification))?;
    let role = fields
        .get(5)
        .map(|role| {
            SourceRole::parse(role)
                .map_err(|error| invalid_source(&error.to_string(), specification))
        })
        .transpose()?
        .unwrap_or(SourceRole::Both);
    SourceConfig::new_with_role(
        fields[0],
        fields[1],
        fields[2],
        SeedBasis::parse(fields[3])?,
        start_block_number,
        role,
        endpoint,
    )
}

fn invalid_source(reason: &str, specification: &str) -> RunnerError {
    let descriptor = specification
        .split_once('=')
        .map(|(descriptor, _)| descriptor)
        .unwrap_or(specification);
    RunnerError::new(
        ErrorKind::Configuration,
        format!("invalid source descriptor {descriptor:?}: {reason}"),
    )
}
