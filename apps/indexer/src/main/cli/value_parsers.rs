pub(super) fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    let maximum = usize::try_from(i64::MAX - 1).unwrap_or(usize::MAX);
    match value {
        0 => Err("value must be positive".to_owned()),
        value if value > maximum => Err(format!("value must be between 1 and {maximum}")),
        value => Ok(value),
    }
}
