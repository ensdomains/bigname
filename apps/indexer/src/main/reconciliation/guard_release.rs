use anyhow::Result;

pub(crate) fn prioritize_operation_error<T>(
    operation_result: Result<T>,
    release_result: Result<()>,
) -> Result<T> {
    match (operation_result, release_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::prioritize_operation_error;

    #[test]
    fn operation_error_has_priority_over_guard_release_error() {
        let error = prioritize_operation_error::<()>(
            Err(anyhow!("actionable operation failure")),
            Err(anyhow!("secondary guard release failure")),
        )
        .expect_err("the operation failure must remain actionable");

        assert_eq!(error.to_string(), "actionable operation failure");
    }

    #[test]
    fn guard_release_error_is_returned_after_successful_operation() {
        let error = prioritize_operation_error(Ok(()), Err(anyhow!("guard release failure")))
            .expect_err("a failed guard release must fail the operation");

        assert_eq!(error.to_string(), "guard release failure");
    }

    #[test]
    fn successful_operation_returns_its_value_after_successful_release() {
        let value = prioritize_operation_error(Ok(42), Ok(()))
            .expect("a successful operation and release must return the operation value");

        assert_eq!(value, 42);
    }
}
