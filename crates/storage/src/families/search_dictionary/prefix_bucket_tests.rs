//! The dense-class prefix walk of `storage:families.name.search_candidates`. When a class has
//! more prefix matches than the posting probe collects, `load` reads that class through the
//! bucket index of the first `k` characters of the prefix, `name_search_documents_prefix{k}_idx`.
//! The walk starts at the cursor inside the bucket, runs in page order and stops at the batch
//! limit, so it reads only the bucket's entries up to its last returned candidate. A prefix
//! longer than the last bucket walks that bucket and filters by its `LIKE`.
//!
//! The tests call `load` itself. In one transaction per call they count the entries it read
//! from the bucket indexes and the rows it read from `name_surfaces`, and they find the
//! statements it executed in `pg_prepared_statements`. Each bucket statement it prepared is
//! then explained under both plan modes. Sorts are off to stand in for a large table, as in
//! `name_order_plan_tests.rs`. So the plan assertions prove that the ordered path exists, not
//! that the planner picks it at real costs. The staging measurement covers that choice. The
//! number of buckets is read from the bucket indexes the identity baseline installs.
//!
//! Expected rows, orders and walk lengths come from SQL over the fixture, never from the
//! statement under test or a Rust ordering, because the database collation decides the order.

use anyhow::{Context, Result, ensure};
use sqlx::{Connection, PgConnection, raw_sql};

use super::{DENSE_SHORT, MERGE, RAW_SHORT, load};
use crate::{
    NameCurrentListFilter,
    families::{
        id_index_plan_tests::{PLAN_MODES, explain_execute, with_database},
        search_dictionary::escape_like,
    },
};

type Candidate = (String, String, String, String);
type Key = (String, String, String);

const BUCKET_STATEMENTS: &str = "/* storage:families.name.search_candidates.prefix%";
/// The release line's walks of the raw-backed and short structural classes.
const WALK_STATEMENTS: [&str; 2] = [
    "/* storage:families.name.search_candidates */%",
    "SELECT rendered.logical_name_id, rendered.name, rendered.namespace%",
];

/// One `load` call. `after` is the key of that row of the same search read without a limit.
struct Case {
    label: &'static str,
    query: &'static str,
    contains: bool,
    /// A term of the prefix has few enough postings that the dense path is not taken.
    sparse: bool,
    namespaces: &'static [&'static str],
    after: Option<usize>,
    limit: usize,
    supported_before_limit: bool,
}

const fn prefix_case(label: &'static str, query: &'static str) -> Case {
    Case {
        label,
        query,
        contains: false,
        sparse: false,
        namespaces: &["ens"],
        after: None,
        limit: 51,
        supported_before_limit: true,
    }
}

#[tokio::test]
async fn dense_prefix_walks_read_only_their_bucket() -> Result<()> {
    with_database("family_search_prefix_bucket", async |connection| {
        install_fixture(connection).await?;
        let buckets: i64 = sqlx::query_scalar(
            r"SELECT count(*) FROM pg_indexes
              WHERE schemaname = 'bigname_phase' AND tablename = 'name_search_documents'
                AND indexname LIKE 'name\_search\_documents\_prefix%\_idx'",
        )
        .fetch_one(&mut *connection)
        .await?;
        ensure!(buckets >= 1, "the baseline installs no bucket index");
        let buckets = usize::try_from(buckets)?;
        check_cases(connection, buckets).await?;
        check_cursor_walk(connection, buckets).await?;
        check_plans(connection, buckets).await
    })
    .await
}

fn cases() -> Vec<Case> {
    vec![
        // The 600 digit-led names of each class sort before the `a` bucket, and in byte order
        // the 1,100 `_xy` names do too. The 100 `ab` names lead it.
        prefix_case("prefix a", "a"),
        prefix_case("prefix al", "al"),
        prefix_case("prefix ali", "ali"),
        // Every term of `alic` is dense in both classes. The 200 `alia` names come first in
        // the `ali` bucket, and some `alic` names are shadowed, orphaned or past the marker.
        prefix_case("prefix alic", "alic"),
        Case {
            after: Some(49),
            ..prefix_case("prefix alic, second page", "alic")
        },
        // Two documents share a name and namespace. The first page ends between them.
        Case {
            limit: 1,
            ..prefix_case("prefix alic, first of a tie", "alic")
        },
        Case {
            after: Some(0),
            limit: 1,
            ..prefix_case("prefix alic, second of a tie", "alic")
        },
        // A bare search interleaves the namespaces by name inside one bucket.
        Case {
            namespaces: &["basenames", "ens"],
            ..prefix_case("prefix alic, both namespaces", "alic")
        },
        // 300 basenames names lead the `z` bucket of an ens-only search.
        prefix_case("prefix z, ens only", "z"),
        // Characters of two UTF-8 bytes: the bucket counts characters. The 100 `é` names of
        // each class share the first byte of `ñ` (C3) and sort before it, in byte order and
        // under glibc, so a bucket of bytes would read them on the first `ñ` page. The
        // `LIKE` drops them from the rows, and the fetch bound catches the read.
        prefix_case("prefix ñ, two-byte character", "ñ"),
        prefix_case("prefix ñüé, two-byte characters", "ñüé"),
        prefix_case("prefix ñüé0, two-byte characters past the bucket", "ñüé0"),
        // A leading `_`, which `LIKE` reads as a wildcard unless escaped.
        prefix_case("prefix _, a LIKE wildcard", "_"),
        prefix_case("prefix _xy, a LIKE wildcard first", "_xy"),
        prefix_case("prefix _xy0, a LIKE wildcard past the bucket", "_xy0"),
        // 300 unsupported names lead the `q` bucket.
        prefix_case("prefix q, supported first", "q"),
        Case {
            supported_before_limit: false,
            ..prefix_case("prefix q, unsupported kept", "q")
        },
        // The `q` buckets of the two classes hold 1,600 supported names: a page of exactly
        // 1,600, a page of 1,599 with one more, that one by cursor, and nothing after it.
        Case {
            limit: 1601,
            ..prefix_case("prefix q, exactly the page", "q")
        },
        Case {
            limit: 1600,
            ..prefix_case("prefix q, one past the page", "q")
        },
        Case {
            after: Some(1598),
            limit: 1600,
            ..prefix_case("prefix q, the page after", "q")
        },
        Case {
            after: Some(1599),
            ..prefix_case("prefix q, past the bucket", "q")
        },
        // `lig` has no posting, so the sparse path reads the empty id list.
        Case {
            sparse: true,
            ..prefix_case("prefix alig, one sparse term", "alig")
        },
        Case {
            contains: true,
            ..prefix_case("contains ali", "ali")
        },
    ]
}

async fn check_cases(connection: &mut PgConnection, buckets: usize) -> Result<()> {
    let mut failures = Vec::new();
    for mode in PLAN_MODES {
        raw_sql(&format!(
            "SET plan_cache_mode = {mode}; SET enable_sort = off"
        ))
        .execute(&mut *connection)
        .await?;
        for case in cases() {
            let label = format!("{} ({mode})", case.label);
            let after = match case.after {
                Some(row) => Some(key(
                    &reference(connection, &case, None, None, None).await?[row]
                )),
                None => None,
            };
            let k = case.query.chars().count().min(buckets);
            let bucketed = !case.contains && !case.sparse;
            let statement = format!("/* storage:families.name.search_candidates.prefix{k} */%");

            let mut transaction = connection.begin().await?;
            let read_before = walk_reads(&mut transaction).await?;
            let before = (
                executions(&mut transaction, &statement).await?,
                executions(&mut transaction, BUCKET_STATEMENTS).await?,
                walks(&mut transaction).await?,
            );
            let found = load(
                &mut transaction,
                &filter(&case),
                Some(
                    case.namespaces
                        .iter()
                        .map(|value| (*value).to_owned())
                        .collect(),
                ),
                Some(pattern(&case)),
                after.as_ref(),
                case.limit,
                case.supported_before_limit,
            )
            .await
            .with_context(|| label.clone())?;
            let fetched = walk_reads(&mut transaction).await? - read_before;
            let executed = (
                executions(&mut transaction, &statement).await? - before.0,
                executions(&mut transaction, BUCKET_STATEMENTS).await? - before.1,
                walks(&mut transaction).await? - before.2,
            );
            transaction.rollback().await?;

            let limit = i64::try_from(case.limit)?;
            let expected = reference(connection, &case, after.as_ref(), Some(limit), None).await?;
            if found != expected {
                failures.push(format!(
                    "{label}: {} candidates differ from the {} expected",
                    found.len(),
                    expected.len()
                ));
            }
            if bucketed && found != old_path(connection, &case, after.as_ref(), limit).await? {
                failures.push(format!(
                    "{label}: differs from the release line's statements"
                ));
            }
            if !bucketed {
                if executed.1 != 0 {
                    failures.push(format!("{label}: ran a bucket statement"));
                }
                continue;
            }
            // Both dense classes run the bucket statement of length k, and neither walk runs.
            if executed != (2, 2, 0) {
                failures.push(format!(
                    "{label}: bucket statement prefix{k} ran {} times, any bucket statement {}, \
                     the walks {}",
                    executed.0, executed.1, executed.2
                ));
            }
            // Each walked entry reads its bucket entry and at most its surface. The long
            // structural class reads by id and only its surface counts.
            let mut walked = 0;
            for class in [0, 1] {
                walked += walk_length(connection, &case, after.as_ref(), k, class).await?;
            }
            if fetched > 2 * walked + 2 {
                failures.push(format!(
                    "{label}: fetched {fetched} rows for a walk of {walked} bucket entries"
                ));
            }
        }
    }
    raw_sql("RESET plan_cache_mode; RESET enable_sort")
        .execute(&mut *connection)
        .await?;
    ensure!(
        failures.is_empty(),
        "bucket walks:\n{}",
        failures.join("\n")
    );
    Ok(())
}

/// A walk in batches of 200 with unsupported candidates kept, as a search over a scope without
/// a ready publication reads, returns every match once and in order, the long structural one
/// included.
async fn check_cursor_walk(connection: &mut PgConnection, buckets: usize) -> Result<()> {
    let case = Case {
        limit: 200,
        supported_before_limit: false,
        ..prefix_case("prefix alic in batches", "alic")
    };
    let k = case.query.chars().count().min(buckets);
    let expected = reference(connection, &case, None, None, None).await?;
    ensure!(
        expected.iter().any(|row| row.1.len() > 2000),
        "the walk misses the long structural name"
    );
    for mode in PLAN_MODES {
        raw_sql(&format!(
            "SET plan_cache_mode = {mode}; SET enable_sort = off"
        ))
        .execute(&mut *connection)
        .await?;
        let mut walked = Vec::new();
        loop {
            let after = walked.last().map(key);
            let mut transaction = connection.begin().await?;
            let read_before = walk_reads(&mut transaction).await?;
            let walks_before = walks(&mut transaction).await?;
            let batch = load(
                &mut transaction,
                &filter(&case),
                Some(vec!["ens".to_owned()]),
                Some(pattern(&case)),
                after.as_ref(),
                case.limit,
                false,
            )
            .await?;
            let fetched = walk_reads(&mut transaction).await? - read_before;
            let walked_old = walks(&mut transaction).await? - walks_before;
            transaction.rollback().await?;
            ensure!(
                walked_old == 0,
                "{mode}: a batch after {after:?} ran a walk"
            );
            let mut bound = 2;
            for class in [0, 1] {
                bound += 2 * walk_length(connection, &case, after.as_ref(), k, class).await?;
            }
            ensure!(
                fetched <= bound,
                "{mode}: a batch after {after:?} fetched {fetched} rows, more than {bound}"
            );
            let done = batch.len() < case.limit;
            walked.extend(batch);
            if done {
                break;
            }
        }
        ensure!(
            walked == expected,
            "{mode}: the batched walk differs from the matches in order"
        );
    }
    raw_sql("RESET plan_cache_mode; RESET enable_sort")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// Each bucket statement `load` prepared can read its bucket index in page order, with the
/// class, the bucket and the cursor as the index condition, under a `Limit` and without a sort,
/// for both dense classes, the first page and a later one, and for two seeds whose buckets
/// differ from the first character. Sorts are off, so this shows the path exists, not that the
/// planner prefers it at real costs.
async fn check_plans(connection: &mut PgConnection, buckets: usize) -> Result<()> {
    raw_sql("SET enable_sort = off")
        .execute(&mut *connection)
        .await?;
    let mut failures = Vec::new();
    for k in 1..=buckets {
        let prepared: Option<(String, Vec<String>)> = sqlx::query_as(
            "SELECT name, parameter_types::text[] FROM pg_prepared_statements
             WHERE statement LIKE $1",
        )
        .bind(format!(
            "/* storage:families.name.search_candidates.prefix{k} */%"
        ))
        .fetch_optional(&mut *connection)
        .await?;
        let Some((statement, types)) = prepared else {
            failures.push(format!("prefix{k}: load never prepared it"));
            continue;
        };
        // The values below are bound in this order and with these types.
        let bound = [
            "text[]", "text", "text", "text", "text", "text", "bigint", "boolean", "smallint",
        ];
        if types != bound {
            failures.push(format!("prefix{k}: bound {types:?}"));
            continue;
        }
        // Two seeds in different buckets, so a plan that flips on the bucket's value is seen and
        // named. Each cursor sits inside its seed's ens names of both classes: group 1 for
        // `alic`, group 10 for `zb01`.
        for (seed, cursor) in [("alic", "alic00500.eth"), ("zb01", "zb01500.eth")] {
            let bucket: String = seed.chars().take(k).collect();
            let mut prefixes = vec![bucket.clone()];
            if k == buckets {
                prefixes.push(seed.to_owned());
            }
            for prefix in prefixes {
                for class in [0, 1] {
                    for after in [
                        "NULL, NULL, NULL".to_owned(),
                        format!("'{cursor}', 'ens', '0x'"),
                    ] {
                        let values = format!(
                            "'{{ens}}', '{bucket}', '{}%', {after}, 51, true, {class}",
                            escape_like(&prefix)
                        );
                        for mode in PLAN_MODES {
                            let plan =
                                explain_execute(connection, mode, &statement, &values).await?;
                            let label = format!(
                                "prefix{k} seed {seed} {prefix}% class {class} {after} ({mode})"
                            );
                            if let Some(failure) = plan_failure(&plan, k, prefix.len() > k) {
                                failures.push(format!("{label}: {failure}\n{}", plan.join("\n")));
                            }
                        }
                    }
                }
            }
        }
    }
    raw_sql("RESET enable_sort; RESET plan_cache_mode")
        .execute(&mut *connection)
        .await?;
    ensure!(
        failures.is_empty(),
        "bucket plans:\n{}",
        failures.join("\n\n")
    );
    Ok(())
}

fn plan_failure(plan: &[String], k: usize, filtered: bool) -> Option<String> {
    let index = format!("using name_search_documents_prefix{k}_idx on");
    let bucket = format!("(\"left\"(name, {k}) = ");
    let conditions = [
        "(spelling_class = ",
        bucket.as_str(),
        "(ROW(name, namespace, namehash) > ROW(",
    ];
    if !plan.first().is_some_and(|line| line.starts_with("Limit")) {
        return Some("no Limit at the top of the plan".to_owned());
    }
    // The scan is the Limit's input, and its index condition is the next line.
    let scan = plan.get(1).is_some_and(|line| line.contains(&index))
        && plan.get(2).is_some_and(|line| {
            line.contains("Index Cond:")
                && conditions.iter().all(|condition| line.contains(condition))
        });
    if !scan {
        return Some(format!("no ordered scan of the bucket index {k}"));
    }
    if plan.iter().any(|line| line.contains("Sort")) {
        return Some("sorts".to_owned());
    }
    if filtered
        && !plan
            .iter()
            .any(|line| line.contains("Filter:") && line.contains("(name ~~ "))
    {
        return Some("the longer prefix is not a filter inside the bucket".to_owned());
    }
    None
}

fn filter(case: &Case) -> NameCurrentListFilter {
    let query = Some(case.query.to_owned());
    NameCurrentListFilter {
        prefix: if case.contains { None } else { query.clone() },
        contains: if case.contains { query } else { None },
        supported_only: true,
        ..Default::default()
    }
}

fn pattern(case: &Case) -> String {
    let escaped = escape_like(case.query);
    if case.contains {
        format!("%{escaped}%")
    } else {
        format!("{escaped}%")
    }
}

fn key(row: &Candidate) -> Key {
    (row.1.clone(), row.2.clone(), row.3.clone())
}

/// The candidates of a dense prefix from the release line's statements, which walk the whole
/// name order: the raw-backed walk and the structural walk, merged in page order. The long
/// structural name is not among them, and no case compared with them reaches it.
async fn old_path(
    connection: &mut PgConnection,
    case: &Case,
    after: Option<&Key>,
    limit: i64,
) -> Result<Vec<Candidate>> {
    let namespaces: Vec<String> = case
        .namespaces
        .iter()
        .map(|value| (*value).to_owned())
        .collect();
    let mut rows: Vec<Candidate> = Vec::new();
    for statement in [RAW_SHORT, DENSE_SHORT] {
        rows.extend(
            sqlx::query_as::<_, Candidate>(statement)
                .bind(namespaces.as_slice())
                .bind(None::<&str>)
                .bind(pattern(case))
                .bind(after.map(|key| key.0.as_str()))
                .bind(after.map(|key| key.1.as_str()))
                .bind(after.map(|key| key.2.as_str()))
                .bind(limit)
                .bind(case.supported_before_limit)
                .persistent(false)
                .fetch_all(&mut *connection)
                .await?,
        );
    }
    let column = |pick: fn(&Candidate) -> &String| -> Vec<String> {
        rows.iter().map(|row| pick(row).clone()).collect()
    };
    Ok(sqlx::query_as(MERGE)
        .bind(column(|row| &row.0))
        .bind(column(|row| &row.1))
        .bind(column(|row| &row.2))
        .bind(column(|row| &row.3))
        .bind(limit)
        .persistent(false)
        .fetch_all(connection)
        .await?)
}

/// What the dense walk has read in this session: entries of the bucket indexes, rows of
/// `name_search_documents` read without an index, and rows of `name_surfaces`. Documents read
/// through the table's other indexes are left out. In a bucketed call only the long structural
/// class's by-id statement reads them, as the walks are checked not to run. Under sorts off
/// that statement, given an empty constant id list in a custom plan, walks a name-ordered
/// partial index from the cursor to its end: the empty `= ANY` is false, so it implies every
/// partial predicate, and the ordered walk is then the only plan without a sort. At default
/// settings its custom plan probes the primary key and reads nothing. Bucket entries are
/// counted as the index returns them, so a bitmap scan counts too.
///
/// From PostgreSQL 15 these per-transaction counts are the session's pending counts, which
/// still hold earlier transactions' reads until the session reports them, at most once a
/// second and only between transactions. A difference taken inside one transaction is exact.
async fn walk_reads(connection: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT ((SELECT coalesce(sum(pg_stat_get_xact_tuples_returned(entry.indexrelid)), 0)
                 FROM pg_index entry JOIN pg_class relation ON relation.oid = entry.indexrelid
                 WHERE entry.indrelid = 'bigname_phase.name_search_documents'::regclass
                   AND relation.relname LIKE 'name\\_search\\_documents\\_prefix%')
              + (SELECT coalesce(sum(coalesce(seq_tup_read, 0)), 0)
                 FROM pg_stat_xact_user_tables
                 WHERE schemaname = 'bigname_phase' AND relname = 'name_search_documents')
              + (SELECT coalesce(sum(coalesce(seq_tup_read, 0) + coalesce(idx_tup_fetch, 0)), 0)
                 FROM pg_stat_xact_user_tables
                 WHERE schemaname = 'bigname_phase' AND relname = 'name_surfaces'))::bigint",
    )
    .fetch_one(connection)
    .await?)
}

/// Executions of the release line's walks.
async fn walks(connection: &mut PgConnection) -> Result<i64> {
    let mut total = 0;
    for statement in WALK_STATEMENTS {
        total += executions(&mut *connection, statement).await?;
    }
    Ok(total)
}

/// Executions of the prepared statements whose text matches.
async fn executions(connection: &mut PgConnection, statement: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT coalesce(sum(generic_plans + custom_plans), 0)::bigint
         FROM pg_prepared_statements WHERE statement LIKE $1",
    )
    .bind(statement)
    .fetch_one(connection)
    .await?)
}

/// The readable matches of a case in page order, from the fixture's own meaning of readable:
/// an active surface on a canonical block at or before the marker, and a supported summary
/// when the case asks for one. `class` narrows them to one spelling class.
async fn reference(
    connection: &mut PgConnection,
    case: &Case,
    after: Option<&Key>,
    limit: Option<i64>,
    class: Option<i16>,
) -> Result<Vec<Candidate>> {
    sqlx::query_as(
        "SELECT document.logical_name_id, document.name, document.namespace, document.namehash
         FROM name_search_documents document
         JOIN name_surfaces surface ON surface.logical_name_id = document.logical_name_id
         JOIN chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         JOIN project_family_marker marker ON marker.chain_id = surface.chain_id
         JOIN project_name_summary summary
           ON summary.chain_id = document.chain_id
          AND summary.logical_name_id = document.logical_name_id
         WHERE document.namespace = ANY($1)
           AND CASE WHEN $2 THEN strpos(document.name, $3) > 0
                    ELSE left(document.name, char_length($3)) = $3 END
           AND surface.visibility_state = 'active'
           AND lineage.canonicality_state = 'canonical'
           AND surface.block_number <= marker.current_block_number
           AND (NOT $4 OR summary.search_supported)
           AND ($5::text IS NULL
                OR (document.name, document.namespace, document.namehash) > ($5, $6, $7))
           AND ($8::smallint IS NULL OR document.spelling_class = $8)
         ORDER BY document.name, document.namespace, document.namehash
         LIMIT $9",
    )
    .bind(case.namespaces)
    .bind(case.contains)
    .bind(case.query)
    .bind(case.supported_before_limit)
    .bind(after.map(|key| key.0.as_str()))
    .bind(after.map(|key| key.1.as_str()))
    .bind(after.map(|key| key.2.as_str()))
    .bind(class)
    .bind(limit)
    .fetch_all(connection)
    .await
    .with_context(|| format!("reference for {}", case.label))
}

/// The bucket entries of one class that a walk passes: every entry after the cursor, up to the
/// class's last candidate when the class fills the limit.
async fn walk_length(
    connection: &mut PgConnection,
    case: &Case,
    after: Option<&Key>,
    k: usize,
    class: i16,
) -> Result<i64> {
    let limit = i64::try_from(case.limit)?;
    let candidates = reference(connection, case, after, Some(limit), Some(class)).await?;
    let last = if candidates.len() == case.limit {
        candidates.last().map(key)
    } else {
        None
    };
    let bucket: String = case.query.chars().take(k).collect();
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM name_search_documents document
         WHERE document.spelling_class = $1 AND left(document.name, $2) = $3
           AND ($4::text IS NULL
                OR (document.name, document.namespace, document.namehash) > ($4, $5, $6))
           AND ($7::text IS NULL
                OR (document.name, document.namespace, document.namehash) <= ($7, $8, $9))",
    )
    .bind(class)
    .bind(i32::try_from(k)?)
    .bind(bucket)
    .bind(after.map(|key| key.0.as_str()))
    .bind(after.map(|key| key.1.as_str()))
    .bind(after.map(|key| key.2.as_str()))
    .bind(last.as_ref().map(|key| key.0.as_str()))
    .bind(last.as_ref().map(|key| key.1.as_str()))
    .bind(last.as_ref().map(|key| key.2.as_str()))
    .fetch_one(connection)
    .await?)
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    // Every group has a raw-backed (class 0) and a structural (class 1) copy, with
    // interleaved names, unless it says otherwise. The group's names are its stem and
    // 2 * i * stride + class in five digits. Group 1 rejects some names through an orphaned
    // block, a block past the marker or, raw-backed only, a shadowed surface. Group 2 repeats
    // group 1's first raw-backed name under another namehash. Group 11 repeats every 25th
    // name of group 1 in basenames. Groups 12 and 13 spell two-byte characters, and
    // group 14 starts with a `LIKE` wildcard.
    raw_sql(
        r#"CREATE TEMP TABLE fixture AS
         SELECT n, name, namespace, class, supported, rejection,
                '0x' || lpad(to_hex(n), 64, '0') AS namehash,
                CASE WHEN rejection = 'above' THEN n + 100000 ELSE n END AS block
         FROM (
             SELECT row_number() OVER (ORDER BY ordinal, i, class) AS n,
                    stem || lpad((2 * i * stride + class)::text, 5, '0') || '.eth' AS name,
                    namespace, class::smallint AS class, supported,
                    CASE WHEN ordinal = 1 AND i % 37 = 3 THEN 'orphaned'
                         WHEN ordinal = 1 AND i % 41 = 4 THEN 'above'
                         WHEN ordinal = 1 AND class = 0 AND i % 43 = 6 THEN 'shadow'
                    END AS rejection
             FROM (VALUES
                     (1, 'alic', 0, 1100, 1, 'ens', ARRAY[0, 1], true),
                     (2, 'alic', 0, 0, 1, 'ens', ARRAY[0], true),
                     (3, 'alia', 1, 200, 1, 'ens', ARRAY[0, 1], true),
                     (4, 'alix', 1, 50, 1, 'ens', ARRAY[0, 1], true),
                     (5, 'ab', 1, 100, 1, 'ens', ARRAY[0, 1], true),
                     (6, '0', 1, 600, 1, 'ens', ARRAY[0, 1], true),
                     (7, 'qa', 1, 300, 1, 'ens', ARRAY[0, 1], false),
                     (8, 'qb', 1, 800, 1, 'ens', ARRAY[0, 1], true),
                     (9, 'za', 1, 300, 1, 'basenames', ARRAY[0, 1], true),
                     (10, 'zb', 1, 1100, 1, 'ens', ARRAY[0, 1], true),
                     (11, 'alic', 1, 40, 25, 'basenames', ARRAY[0, 1], true),
                     (12, 'ñüé', 1, 1100, 1, 'ens', ARRAY[0, 1], true),
                     (13, 'éa', 1, 100, 1, 'ens', ARRAY[0, 1], true),
                     (14, '_xy', 1, 1100, 1, 'ens', ARRAY[0, 1], true)
                 ) spec (ordinal, stem, low, high, stride, namespace, classes, supported),
                 generate_series(low, high) i,
                 unnest(classes) class
         ) named;

         -- One structural name over 2000 bytes, which only the sparse path reads.
         INSERT INTO fixture
         SELECT n, 'alic' || repeat('x', 2100) || '.eth', 'ens', 2, true, NULL,
                '0x' || lpad(to_hex(n), 64, '0'), n
         FROM (SELECT max(n) + 1 AS n FROM fixture) following;

         INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT 'ethereum-sepolia', 'block-' || n, block, to_timestamp(block * 12),
                (CASE WHEN rejection = 'orphaned' THEN 'orphaned' ELSE 'canonical' END)
                    ::canonicality_state
         FROM fixture;

         INSERT INTO project_family_marker
             (chain_id, current_block_number, current_block_hash, state)
         VALUES ('ethereum-sepolia', 100000, 'block-100000', 'live');

         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT namespace || ':' || namehash, namespace,
                CASE WHEN class = 0 THEN name END,
                CASE WHEN class = 0 THEN string_to_array(name, '.') END,
                CASE WHEN class = 0 THEN '\x00'::bytea END,
                namehash,
                CASE WHEN class = 0
                     THEN ARRAY(SELECT md5(label) FROM unnest(string_to_array(name, '.')) label)
                     ELSE ARRAY[md5(name)] END,
                'v1',
                CASE WHEN rejection = 'shadow' THEN 'shadow' ELSE 'active' END,
                CASE WHEN rejection = 'shadow' THEN 'invalid' END,
                CASE WHEN rejection = 'shadow' THEN now() END,
                'ethereum-sepolia', 'block-' || n, block, 'canonical'
         FROM fixture;

         INSERT INTO name_search_documents
             (logical_name_id, chain_id, namespace, namehash, name, spelling_class)
         SELECT namespace || ':' || namehash, 'ethereum-sepolia', namespace, namehash, name, class
         FROM fixture ORDER BY n;

         INSERT INTO project_name_summary (chain_id, logical_name_id, namespace, namehash,
             serving, zero_owner, expiry_listable, search_supported, search_fields,
             resolver_listing_eligible, resolver_support_exempt)
         SELECT 'ethereum-sepolia', namespace || ':' || namehash, namespace, namehash,
                true, false, false, supported,
                CASE WHEN supported THEN '{"status": "active"}'::jsonb END, false, false
         FROM fixture;

         -- The tokens the probes read: prefixes of one to three characters and the
         -- three-character contains tokens.
         INSERT INTO name_search_postings
             (namespace, spelling_class, token_kind, token_length, token_bytes, search_id)
         SELECT DISTINCT document.namespace, document.spelling_class, token.kind, token.size,
                convert_to(token.token_text, 'UTF8'), document.search_id
         FROM name_search_documents document,
         LATERAL (
             SELECT 2::smallint, size::smallint, left(document.name, size)
             FROM generate_series(1, 3) size
             UNION ALL
             SELECT 1::smallint, 3::smallint, substr(document.name, pos, 3)
             FROM generate_series(1, char_length(document.name) - 2) pos
         ) token (kind, size, token_text);

         ANALYZE"#,
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}
