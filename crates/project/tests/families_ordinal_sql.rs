//! The SQL parse of the emission ordinal (docs/glossary.md#emission-ordinal) that a served
//! reader ported to the canonical event order must use, checked against the Rust parse the
//! families order by (`families::emission_ordinal`). The SQL strips leading zeros and checks
//! the significant length and the ten-digit bound before it casts, so no suffix can make it
//! error where Rust yields no ordinal.
mod families_support;

use anyhow::Result;
use bigname_project::families::emission_ordinal;
use families_support::Fixture;

/// The glossary's SQL form, with the identity and both indexes as parameters.
const ORDINAL_SQL: &str = r#"
SELECT CASE WHEN $2::bigint IS NOT NULL AND $3::bigint IS NOT NULL THEN (
    SELECT CASE
        WHEN digits.d = '' THEN 0::bigint
        WHEN length(digits.d) < 10
          OR (length(digits.d) = 10 AND digits.d COLLATE "C" <= '4294967295' COLLATE "C")
            THEN digits.d::bigint
    END
    FROM (SELECT ltrim(m[1], '0') AS d
          FROM regexp_match($1::text COLLATE "C", ':([0-9]+)$') m) digits
) END"#;

/// A case's transaction and log index.
type Indexes = (Option<i64>, Option<i64>);

#[tokio::test]
async fn the_sql_parse_matches_the_rust_parse() -> Result<()> {
    let fixture = Fixture::new("families_ordinal_sql", 1).await?;
    let nines = format!("p:{}", "9".repeat(131_073));
    let zero_prefixed = format!("p:{}7", "0".repeat(200_000));
    let both = (Some(0), Some(5));
    let cases: Vec<(&str, Indexes, String)> = vec![
        ("no transaction index", (None, Some(5)), "p:3".into()),
        ("no log index", (Some(0), None), "p:3".into()),
        ("no indexes", (None, None), "p:3".into()),
        ("no colon", both, "p3".into()),
        ("empty tail", both, "p:".into()),
        ("letters", both, "p:12a".into()),
        ("sign", both, "p:-1".into()),
        ("plus", both, "p:+1".into()),
        ("inner space", both, "p: 1".into()),
        ("trailing newline", both, "p:7\n".into()),
        ("arabic-indic digit", both, "p:\u{0663}".into()),
        ("fullwidth digit", both, "p:\u{FF17}".into()),
        ("mixed digits", both, "p:1\u{0663}".into()),
        ("earlier segment", both, "p:12:x".into()),
        ("last segment", both, "p:12:34".into()),
        ("zero", both, "p:0".into()),
        ("zeros", both, "p:000".into()),
        ("leading zeros", both, "p:007".into()),
        ("nine digits", both, "p:999999999".into()),
        ("ten digits", both, "p:1000000000".into()),
        ("u32 max", both, "p:4294967295".into()),
        ("u32 max + 1", both, "p:4294967296".into()),
        ("ten nines", both, "p:9999999999".into()),
        ("eleven digits", both, "p:10000000000".into()),
        ("zero-padded u32 max", both, "p:0004294967295".into()),
        ("131073 nines", both, nines.clone()),
        ("long zero-prefixed 7", both, zero_prefixed),
    ];
    for (label, (transaction, log), identity) in &cases {
        let sql: Option<i64> = sqlx::query_scalar(ORDINAL_SQL)
            .bind(identity)
            .bind(transaction)
            .bind(log)
            .fetch_one(&fixture.pool)
            .await?;
        let rust = emission_ordinal(*transaction, *log, identity).map(i64::from);
        assert_eq!(sql, rust, "{label}");
    }
    // The earlier form casts to numeric before its range check, so 131073 digits error in
    // PostgreSQL where Rust yields no ordinal.
    let earlier = sqlx::query_scalar::<_, Option<i64>>(
        r#"SELECT CASE WHEN m[1]::numeric <= 4294967295 THEN m[1]::bigint END
           FROM regexp_match($1::text, ':([0-9]+)$') m"#,
    )
    .bind(&nines)
    .fetch_one(&fixture.pool)
    .await;
    assert!(earlier.is_err(), "the numeric cast rejects 131073 digits");
    fixture.cleanup().await
}
