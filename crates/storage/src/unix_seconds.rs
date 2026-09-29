//! Exact Unix seconds for expiry values and collection keys, including the full uint64 range.
use std::{fmt, str::FromStr};

use bigdecimal::BigDecimal;
use serde::{Serialize, Serializer};
use serde_json::Value;
use sqlx::{
    Decode, Encode, Postgres, Type,
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
    types::time::OffsetDateTime,
};

const NANOS: i128 = 1_000_000_000;

/// An exact instant or expiry in seconds. Nanosecond precision preserves accepted RFC3339
/// filter boundaries, while the integer range includes every uint64 expiry plus registrar grace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UnixSeconds(i128);

impl UnixSeconds {
    pub fn from_seconds(seconds: i128) -> Option<Self> {
        seconds.checked_mul(NANOS).map(Self)
    }

    pub fn unix_timestamp(self) -> i128 {
        self.0.div_euclid(NANOS)
    }
    pub fn nanosecond(self) -> u32 {
        self.0.rem_euclid(NANOS) as u32
    }
    pub fn checked_add_seconds(self, seconds: i64) -> Option<Self> {
        self.0.checked_add(i128::from(seconds) * NANOS).map(Self)
    }
    pub fn from_json(value: &Value) -> Option<Self> {
        match value {
            Value::String(value) => value.parse().ok(),
            Value::Number(value) => value.to_string().parse().ok(),
            _ => None,
        }
    }
    /// Snapshot and cursor values keep their existing precise calendar representation where
    /// possible. Expiries beyond the calendar range use an exact decimal key instead.
    pub fn to_datetime(self) -> Option<OffsetDateTime> {
        OffsetDateTime::from_unix_timestamp_nanos(self.0).ok()
    }

    pub fn internal_string(self) -> String {
        OffsetDateTime::from_unix_timestamp_nanos(self.0)
            .map(crate::time::format_timestamp)
            .unwrap_or_else(|_| self.to_string())
    }
}

impl From<OffsetDateTime> for UnixSeconds {
    fn from(value: OffsetDateTime) -> Self {
        Self(value.unix_timestamp_nanos())
    }
}

impl FromStr for UnixSeconds {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Ok(time) = crate::parse_rfc3339_utc_timestamp(value) {
            return Ok(time.into());
        }
        let (negative, digits) = value
            .strip_prefix('-')
            .map_or((false, value), |v| (true, v));
        let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
        anyhow::ensure!(
            !whole.is_empty()
                && whole.bytes().all(|c| c.is_ascii_digit())
                && fraction.len() <= 9
                && fraction.bytes().all(|c| c.is_ascii_digit())
                && !value.ends_with('.'),
            "invalid Unix seconds"
        );
        let whole: i128 = whole.parse()?;
        let nanos = if fraction.is_empty() {
            0
        } else {
            fraction.parse::<i128>()? * 10_i128.pow(9 - fraction.len() as u32)
        };
        let nanos = whole
            .checked_mul(NANOS)
            .and_then(|v| v.checked_add(nanos))
            .ok_or_else(|| anyhow::anyhow!("Unix seconds exceed the supported range"))?;
        Ok(Self(if negative { -nanos } else { nanos }))
    }
}

impl fmt::Display for UnixSeconds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let magnitude = self.0.unsigned_abs();
        if self.0 < 0 {
            f.write_str("-")?;
        }
        write!(f, "{}", magnitude / NANOS as u128)?;
        let fraction = magnitude % NANOS as u128;
        if fraction != 0 {
            write!(f, ".{}", format!("{fraction:09}").trim_end_matches('0'))?;
        }
        Ok(())
    }
}

impl Serialize for UnixSeconds {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl Type<Postgres> for UnixSeconds {
    fn type_info() -> PgTypeInfo {
        <BigDecimal as Type<Postgres>>::type_info()
    }
    fn compatible(ty: &PgTypeInfo) -> bool {
        <BigDecimal as Type<Postgres>>::compatible(ty)
    }
}

impl Encode<'_, Postgres> for UnixSeconds {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <BigDecimal as Encode<Postgres>>::encode_by_ref(&self.to_string().parse()?, buf)
    }
}

impl Decode<'_, Postgres> for UnixSeconds {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        let value = <BigDecimal as Decode<Postgres>>::decode(value)?;
        Ok(value.normalized().to_plain_string().parse()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn postgres_padded_numeric_scale_decodes_without_rounding() -> anyhow::Result<()> {
        let db = bigname_test_support::TestDatabase::create(
            bigname_test_support::TestDatabaseConfig::new("unix_seconds_scale"),
        )
        .await?;
        for decimal in [
            "9007199254740993.000000000000",
            "18446744073709551614.000000001000",
        ] {
            let value: UnixSeconds = sqlx::query_scalar("SELECT $1::text::numeric")
                .bind(decimal)
                .fetch_one(db.pool())
                .await?;
            assert_eq!(
                value.to_string(),
                decimal.trim_end_matches('0').trim_end_matches('.')
            );
        }
        db.cleanup().await?;
        Ok(())
    }

    #[test]
    fn finite_uint64_and_fractional_boundaries_round_trip_exactly() {
        for value in [
            "253402300800",
            "9007199254740993",
            "9223372036854775808",
            "18446744073709551614",
            "18446744073709551615.000000001",
            "-0.000000001",
        ] {
            let time: UnixSeconds = value.parse().unwrap();
            assert_eq!(time.to_string(), value);
            assert_eq!(time.internal_string().parse::<UnixSeconds>().unwrap(), time);
        }
        let rfc: UnixSeconds = "2026-01-02T03:04:05.123456789Z".parse().unwrap();
        assert_eq!(rfc, "1767323045.123456789".parse().unwrap());
        assert!("999".parse::<UnixSeconds>().unwrap() < "1000".parse().unwrap());
    }
}
