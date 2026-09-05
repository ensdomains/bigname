use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};

/// Arbitrary-width integer encoded as a decimal string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BigInt(String);

impl BigInt {
    pub(crate) fn from_i64(value: i64) -> Self {
        Self(value.to_string())
    }
}

#[Scalar(name = "BigInt")]
impl ScalarType for BigInt {
    fn parse(value: Value) -> InputValueResult<Self> {
        let Value::String(value) = value else {
            return Err(InputValueError::expected_type(value));
        };
        if is_decimal_integer(&value) {
            Ok(Self(value))
        } else {
            Err(InputValueError::custom(
                "BigInt must be an arbitrary-width decimal string",
            ))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

fn is_decimal_integer(value: &str) -> bool {
    let digits = value.strip_prefix('-').unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// Byte sequence encoded as an even-length, `0x`-prefixed hexadecimal string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Bytes(String);

impl Bytes {
    pub(crate) fn parse_string(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into();
        if is_prefixed_bytes(&value) {
            Ok(Self(value.to_ascii_lowercase()))
        } else {
            Err("Bytes must be an even-length, 0x-prefixed hexadecimal string")
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[Scalar(name = "Bytes")]
impl ScalarType for Bytes {
    fn parse(value: Value) -> InputValueResult<Self> {
        let Value::String(value) = value else {
            return Err(InputValueError::expected_type(value));
        };
        Self::parse_string(value).map_err(InputValueError::custom)
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

fn is_prefixed_bytes(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("0x") else {
        return false;
    };
    hex.len() % 2 == 0 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigint_accepts_decimal_strings_of_unbounded_width() {
        assert!(is_decimal_integer(
            "340282366920938463463374607431768211456"
        ));
        assert!(is_decimal_integer(
            "-340282366920938463463374607431768211456"
        ));
        assert!(!is_decimal_integer(""));
        assert!(!is_decimal_integer("12.5"));
        assert!(!is_decimal_integer("0x12"));
    }

    #[test]
    fn bytes_requires_prefixed_complete_hex_bytes() {
        assert!(is_prefixed_bytes("0x"));
        assert!(is_prefixed_bytes("0x00aB"));
        assert!(is_prefixed_bytes("0xAB"));
        assert!(!is_prefixed_bytes("0X00ab"));
        assert!(!is_prefixed_bytes("00ab"));
        assert!(!is_prefixed_bytes("0x0"));
        assert!(!is_prefixed_bytes("0xzz"));
    }
}
