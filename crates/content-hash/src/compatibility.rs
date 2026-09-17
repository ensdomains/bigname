//! Tate-approved compatibility exception for the isolated blue-brain loader experiment.

pub(crate) const EXCEPTION: &str = "blue-brain-v1-lookahead-20260917";
pub(crate) const SOURCE_HASH: &str =
    "keccak256:5b4e1689c140c935ce4f3fccd28dd766f761077127b24405df41d93a7e7ea7fd";
pub(crate) const RETAINED_HASH: &str =
    "keccak256:e292847c25244de4a800c580f291fca2d2259b9ac6915fb828a390032c6a7f43";

pub(crate) fn effective_hash<'a>(
    source_hash: &'a str,
    requested: Option<&str>,
) -> Result<&'a str, &'static str> {
    match requested {
        None => Ok(source_hash),
        Some(EXCEPTION) if source_hash == SOURCE_HASH => Ok(RETAINED_HASH),
        Some(EXCEPTION) => Err("blue-brain compatibility requires the exact reviewed source hash"),
        Some(_) => Err("unrecognized blue-brain interpretation compatibility exception"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_builds_keep_their_source_hash() {
        for hash in [SOURCE_HASH, RETAINED_HASH, "future-source-hash"] {
            assert_eq!(effective_hash(hash, None), Ok(hash));
        }
    }

    #[test]
    fn only_the_reviewed_source_can_retain_the_existing_version() {
        assert_eq!(
            effective_hash(SOURCE_HASH, Some(EXCEPTION)),
            Ok(RETAINED_HASH)
        );
        for hash in [RETAINED_HASH, "future-source-hash", ""] {
            assert!(effective_hash(hash, Some(EXCEPTION)).is_err());
        }
        for request in ["", "true", RETAINED_HASH, "another-exception"] {
            assert!(effective_hash(SOURCE_HASH, Some(request)).is_err());
        }
    }
}
