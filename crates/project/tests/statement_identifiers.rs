//! Every statement Project sends to PostgreSQL should start with a `/* project:<name> */`
//! comment, so the slow log, `pg_stat_activity` and `pg_stat_statements` name the statement that
//! did the work.
//!
//! The guard reads the production sources of `crates/project/src` as text, and checks, without
//! relying on the identifiers it checks for:
//!
//! - every `.sql` file: with `--` comments removed, each `;`-separated statement starts with an
//!   identifier, and the file's first line is one;
//! - every Rust string literal outside test code that begins, after any leading comments, with one
//!   of the SQL command keywords in `KEYWORDS` in any letter case or with an identifier: it must
//!   carry an identifier
//!   among those leading comments. Fragments spliced into a larger statement carry one too;
//!   PostgreSQL treats the nested comment as whitespace;
//! - every literal, `format!` of a literal, or `include_str!` written as the first argument of a
//!   sqlx statement constructor spelled `sqlx::query`, `query_as`, `query_scalar`, their `_with`
//!   forms or `raw_sql`, of `QueryBuilder::new`, or of an executor method (`execute`, `fetch` and
//!   its `fetch_*` forms): a literal must be named whatever its first word, and an `include_str!`
//!   must include a `.sql` file under `src`, which the first rule checks. `QueryBuilder::new` must
//!   be given one of these, since the fragments pushed after it need not start with a keyword.
//!   The argument is recognised as text: a Rust comment between the parenthesis and the literal
//!   stops the recognition, and the site is then counted as given a value built elsewhere.
//!
//! A value built elsewhere is counted, not checked: a constant, a loop variable or a bound
//! `format!` passed to a constructor, or `<name>.as_str()` or `&<name>` passed to an executor. Its
//! text is checked only when it also comes from a literal the second rule matches or from a `.sql`
//! file. The known ways past the guard are pinned by tests below, so a tightening shows: a bound
//! value whose text starts with neither a keyword nor an identifier, `concat!`, a constant bound
//! to `include_str!` of a file outside `src`, and a call a macro assembles from its arguments.
//! Explicit named imports of the constructors matching `use sqlx::` are rejected, so a direct
//! constructor call cannot hide behind them; glob imports and aliases are not resolved, and a
//! constructor reached through `use sqlx::*` is neither checked nor counted.
//!
//! Test code is a file reached through a `#[cfg(test)]` module declaration, or an item or statement
//! under `#[cfg(test)]` inside a production file. Names are unique across the crate; a name built
//! with `format!` counts once per call site, with its placeholder spelled as written.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

const MARKER_OPEN: &str = "/* project:";
const MARKER_CLOSE: &str = " */";
const KEYWORDS: &[&str] = &[
    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "VALUES", "CREATE", "ALTER", "DROP",
    "TRUNCATE", "ANALYZE", "VACUUM", "SET", "RESET", "SHOW", "LOCK", "DECLARE", "FETCH", "CLOSE",
    "EXPLAIN", "COPY", "CALL", "TABLE",
];
const CONSTRUCTORS: &[&str] = &[
    "query",
    "query_as",
    "query_scalar",
    "query_with",
    "query_as_with",
    "query_scalar_with",
    "raw_sql",
];
const EXECUTORS: &[&str] = &[
    "execute",
    "fetch",
    "fetch_all",
    "fetch_one",
    "fetch_optional",
    "fetch_many",
];

#[test]
fn every_production_statement_starts_with_an_identifier() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut rust_files = Vec::new();
    let mut sql_files = Vec::new();
    collect(&source_root, &mut rust_files, &mut sql_files);
    let gated = gated_files(&rust_files);

    let mut report = Report::default();
    for path in &sql_files {
        report.check_sql(
            &relative(&source_root, path),
            &fs::read_to_string(path).unwrap(),
        );
    }
    for path in rust_files.iter().filter(|path| !gated.contains(*path)) {
        let place = relative(&source_root, path);
        let text = fs::read_to_string(path).unwrap();
        report.check_rust(&place, &text, |included| {
            let file = normalize(&path.parent().unwrap().join(included));
            file.starts_with(&source_root) && sql_files.iter().any(|sql| normalize(sql) == file)
        });
    }
    report.check_names();
    println!(
        "statement identifiers: {} Rust statement sites, {} statements in {} .sql files, {} \
         identified, {} identifiers in the text; {} execution sites, {} of them given a value \
         built elsewhere (counted, not checked)",
        report.rust_sites,
        report.sql_statements,
        sql_files.len(),
        report.identified(),
        report.markers_in_text,
        report.execution_sites,
        report.indirect_sites,
    );
    assert!(
        report.failures.is_empty(),
        "{} statement identifier failures:\n{}",
        report.failures.len(),
        report.failures.join("\n")
    );
    assert_eq!(
        report.markers_in_text,
        report.rust_sites + report.sql_statements,
        "an identifier appears outside a statement start"
    );
}

#[test]
fn an_unnamed_lowercase_statement_fails() {
    let report = check_one(r#"async fn f() { sqlx::query("select 1").execute(&pool).await; }"#);
    assert_eq!(report.failures.len(), 2, "{:?}", report.failures);
    assert!(
        report
            .failures
            .iter()
            .all(|failure| failure.contains("select 1"))
    );
}

#[test]
fn an_ordinary_leading_comment_does_not_name_a_statement() {
    for text in [
        "fn f() { sqlx::query(\"-- counts names\\nSELECT count(*) FROM name_current\"); }",
        "fn f() { sqlx::query(\"/* counts names */ SELECT count(*) FROM name_current\"); }",
    ] {
        let report = check_one(text);
        assert_eq!(report.failures.len(), 2, "{text}: {:?}", report.failures);
    }
}

#[test]
fn a_site_passes_only_a_named_literal_or_an_included_sql_file() {
    let report = check_one(
        "fn f() {
             sqlx::query(\"/* project:a.one */ select 1\");
             sqlx::query_scalar::<_, i64>(&format!(\"/* project:a.two */ SELECT {n}\"));
             sqlx::query(include_str!(\"a/three.sql\"));
             sqlx::query(statement);
         }",
    );
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!((report.execution_sites, report.indirect_sites), (4, 1));

    // A literal that does not start with a keyword is still a statement when it is executed.
    let report = check_one("fn f() { sqlx::query(\"(select 1) union (select 2)\"); }");
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    let report = check_one("fn f() { sqlx::query(include_str!(\"../../elsewhere.sql\")); }");
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    let report = check_one("fn f() { tx.execute(\"SET LOCAL jit = off\"); }");
    assert_eq!(report.failures.len(), 2, "{:?}", report.failures);
    let report = check_one("use sqlx::{Postgres, raw_sql};");
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    // Test code is out of scope.
    let report = check_one("#[cfg(test)]\nmod tests { fn f() { sqlx::query(\"select 1\"); } }");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
}

#[test]
fn a_query_builder_starts_with_a_named_fragment() {
    // Started with a fragment that is not a keyword, the builder would hide the statement it
    // builds from the literal rule.
    let report = check_one(
        "fn f() {
             let mut builder = QueryBuilder::<Postgres>::new(\"sel\");
             builder.push(\"ect 1\");
             builder.build().execute(pool);
         }",
    );
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    let report = check_one("fn f() { let builder = sqlx::QueryBuilder::new(fragment); }");
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    let report = check_one(
        "fn f() {
             QueryBuilder::<Postgres>::new(\"/* project:a.one */ select 1\");
             QueryBuilder::new(format!(\"/* project:a.two */ {prefix}UPDATE t\"));
             QueryBuilder::new(include_str!(\"a/three.sql\"));
         }",
    );
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!((report.execution_sites, report.indirect_sites), (3, 0));
}

/// Known gaps: each example executes an unnamed statement and passes. The guard does not resolve
/// constants, evaluate `concat!` or follow a bound `include_str!`; it counts the site as given a
/// value built elsewhere. A tightening that closes one of these should change its assertion.
#[test]
fn a_value_built_elsewhere_is_counted_not_checked() {
    for text in [
        // A bound `format!` whose text starts with a parenthesis, given to an executor.
        "fn f() { let sql = format!(\"(select {})\", 1); connection.execute(sql.as_str()); }",
        "fn f() { let sql = format!(\"(select {})\", 1); connection.execute(&sql); }",
        // A constant that starts with a parenthesis.
        "const SQL: &str = \"(select 1)\"; fn f() { sqlx::query(SQL); }",
        "fn f() { sqlx::query(concat!(\"sel\", \"ect 1\")); }",
        // A constant bound to a file outside `src`, which the `.sql` rule never reads.
        "const SQL: &str = include_str!(\"../../outside.sql\"); fn f() { sqlx::query(SQL); }",
    ] {
        let report = check_one(text);
        assert!(report.failures.is_empty(), "{text}: {:?}", report.failures);
        assert_eq!(
            (report.execution_sites, report.indirect_sites),
            (1, 1),
            "{text}"
        );
    }
}

/// Known gaps in the text recognition: a Rust comment before a direct argument stops the guard
/// from seeing the literal, so the site is counted as a value built elsewhere; a named
/// `QueryBuilder::new` literal behind such a comment is wrongly rejected; a constructor reached
/// through a glob import is neither checked nor counted. Each is pinned so a tightening shows.
#[test]
fn a_comment_before_a_direct_argument_or_a_glob_import_is_not_seen_through() {
    let report = check_one("fn f() { sqlx::query(/* note */ \"(select 1) union (select 2)\"); }");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!((report.execution_sites, report.indirect_sites), (1, 1));

    let report = check_one(
        "fn f() { QueryBuilder::<Postgres>::new(/* note */ \"/* project:a.one */ select 1\"); }",
    );
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert_eq!((report.execution_sites, report.indirect_sites), (1, 0));

    let report = check_one("use sqlx::*;\nfn f() { query(\"(select 1)\"); }");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!((report.execution_sites, report.indirect_sites), (0, 0));
}

/// Known gap: a macro that assembles the constructor from its arguments is not expanded, so the
/// call it generates is neither checked nor counted.
#[test]
fn a_call_a_macro_assembles_is_missed() {
    let report = check_one(
        "macro_rules! run { ($c:ident) => { sqlx::$c(concat!(\"sel\", \"ect 1\")) }; }
         fn f() { run!(query).execute(pool); }",
    );
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!((report.execution_sites, report.indirect_sites), (0, 0));
}

fn check_one(text: &str) -> Report {
    let mut report = Report::default();
    report.check_rust("example.rs", text, |included| {
        included.starts_with("a/") && included.ends_with(".sql")
    });
    report
}

#[derive(Default)]
struct Report {
    failures: Vec<String>,
    names: BTreeMap<String, Vec<String>>,
    rust_sites: usize,
    sql_statements: usize,
    markers_in_text: usize,
    execution_sites: usize,
    indirect_sites: usize,
}

impl Report {
    fn identified(&self) -> usize {
        self.names.values().map(Vec::len).sum()
    }

    fn check_sql(&mut self, place: &str, text: &str) {
        self.markers_in_text += text.matches(MARKER_OPEN).count();
        if marker_name(text.lines().next().unwrap_or_default())
            .is_none_or(|(_, rest)| !rest.trim().is_empty())
        {
            self.failures
                .push(format!("{place}: first line is not a statement identifier"));
        }
        let without_comments = text
            .lines()
            .map(|line| line.find("--").map_or(line, |start| &line[..start]))
            .collect::<Vec<_>>()
            .join("\n");
        for (index, statement) in without_comments
            .split(';')
            .map(str::trim_start)
            .filter(|statement| !statement.trim().is_empty())
            .enumerate()
        {
            self.sql_statements += 1;
            match marker_name(statement) {
                Some((name, _)) => self.names.entry(name).or_default().push(place.to_owned()),
                None => self.failures.push(format!(
                    "{place}: statement {} does not start with an identifier",
                    index + 1
                )),
            }
        }
    }

    /// `included_sql` says whether an `include_str!` path, relative to the file, is a checked
    /// `.sql` file.
    fn check_rust(&mut self, place: &str, text: &str, included_sql: impl Fn(&str) -> bool) {
        let scanned = Scanned::new(text);
        let test_ranges = scanned.test_ranges();
        let in_test = |offset: usize| {
            test_ranges
                .iter()
                .any(|range| range.0 <= offset && offset < range.1)
        };
        let line = |offset: usize| text[..offset].matches('\n').count() + 1;
        let excerpt = |literal: &Literal| {
            literal
                .text
                .trim_start()
                .chars()
                .take(60)
                .collect::<String>()
                .replace('\n', " ")
        };
        for literal in scanned
            .literals
            .iter()
            .filter(|literal| !in_test(literal.start))
        {
            self.markers_in_text += literal.text.matches(MARKER_OPEN).count();
            let leading = Leading::of(&literal.text);
            if leading.marker.is_none() && !leading.keyword {
                continue;
            }
            self.rust_sites += 1;
            match leading.marker {
                Some(name) => self
                    .names
                    .entry(name)
                    .or_default()
                    .push(format!("{place}:{}", line(literal.start))),
                None => self.failures.push(format!(
                    "{place}:{}: statement does not start with an identifier: {}",
                    line(literal.start),
                    excerpt(literal)
                )),
            }
        }
        let code = String::from_utf8_lossy(&scanned.code).into_owned();
        for import in code.match_indices("use sqlx::").map(|(offset, _)| offset) {
            let statement = &code[import
                ..code[import..]
                    .find(';')
                    .map_or(code.len(), |end| import + end)];
            if !in_test(import)
                && CONSTRUCTORS.iter().any(|name| {
                    statement
                        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .any(|word| word == *name)
                })
            {
                self.failures.push(format!(
                    "{place}:{}: import sqlx statement constructors qualified, as sqlx::query",
                    line(import)
                ));
            }
        }
        for site in execution_sites(&code) {
            if in_test(site.offset) {
                continue;
            }
            // Literals are blanked in `code`, so the argument is read from the source text.
            let literal_at = |offset: usize| {
                let offset = offset + (text[offset..].len() - text[offset..].trim_start().len());
                scanned
                    .literals
                    .iter()
                    .find(|literal| literal.start == offset)
            };
            let argument = text[site.argument..].trim_start();
            let argument_offset = text.len() - argument.len();
            let direct = if let Some(rest) = argument
                .strip_prefix('&')
                .unwrap_or(argument)
                .strip_prefix("format!(")
            {
                literal_at(text.len() - rest.len())
            } else if let Some(rest) = argument.strip_prefix("include_str!(") {
                let path = literal_at(text.len() - rest.len());
                self.execution_sites += 1;
                if !path.is_some_and(|path| path.text.ends_with(".sql") && included_sql(&path.text))
                {
                    self.failures.push(format!(
                        "{place}:{}: include_str! of something other than a .sql file under src",
                        line(site.offset)
                    ));
                }
                continue;
            } else {
                literal_at(argument_offset + usize::from(argument.starts_with('&')))
            };
            match (direct, site.kind) {
                (Some(literal), _) => {
                    self.execution_sites += 1;
                    if Leading::of(&literal.text).marker.is_none() {
                        self.failures.push(format!(
                            "{place}:{}: executes an unnamed statement: {}",
                            line(site.offset),
                            excerpt(literal)
                        ));
                    }
                }
                (None, SiteKind::Builder) => {
                    self.execution_sites += 1;
                    self.failures.push(format!(
                        "{place}:{}: QueryBuilder::new must start with a named literal or an \
                         included .sql file",
                        line(site.offset)
                    ));
                }
                // An executor usually takes a connection; a string it is given by name counts.
                (None, SiteKind::Executor) if !names_a_string(&code[site.argument..]) => {}
                (None, _) => {
                    self.execution_sites += 1;
                    self.indirect_sites += 1;
                }
            }
        }
    }

    fn check_names(&mut self) {
        for (name, places) in &self.names {
            if places.len() > 1 {
                self.failures.push(format!(
                    "identifier {name} is used by {}",
                    places.join(", ")
                ));
            }
        }
    }
}

/// The comments before a statement's first word, and whether that word is a command keyword.
struct Leading {
    marker: Option<String>,
    keyword: bool,
}

impl Leading {
    fn of(text: &str) -> Self {
        let mut rest = text.trim_start();
        let mut marker = None;
        loop {
            if let Some((name, after)) = marker_name(rest) {
                marker.get_or_insert(name);
                rest = after.trim_start();
            } else if let Some(comment) = rest.strip_prefix("/*") {
                rest = comment
                    .find("*/")
                    .map_or("", |end| &comment[end + 2..])
                    .trim_start();
            } else if let Some(comment) = rest.strip_prefix("--") {
                rest = comment
                    .find('\n')
                    .map_or("", |end| &comment[end..])
                    .trim_start();
            } else {
                break;
            }
        }
        let word = rest
            .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .next()
            .unwrap_or_default();
        Self {
            marker,
            keyword: KEYWORDS
                .iter()
                .any(|keyword| keyword.eq_ignore_ascii_case(word)),
        }
    }
}

#[derive(Clone, Copy)]
enum SiteKind {
    /// A sqlx statement constructor.
    Constructor,
    /// `QueryBuilder::new`.
    Builder,
    /// An executor method.
    Executor,
}

struct Site {
    /// Where the call's name starts.
    offset: usize,
    /// Just after the call's opening parenthesis.
    argument: usize,
    kind: SiteKind,
}

/// Whether an executor's argument, in blanked code, is `<name>.as_str()` or `&<name>`: a string
/// bound elsewhere rather than a connection expression such as `&mut *transaction` or `pool`.
fn names_a_string(argument: &str) -> bool {
    let argument = argument.trim_start();
    let (reference, rest) = match argument.strip_prefix('&') {
        Some(rest) => (true, rest.trim_start()),
        None => (false, argument),
    };
    let length = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        .count();
    if length == 0 || rest.as_bytes()[0].is_ascii_digit() || &rest[..length] == "mut" {
        return false;
    }
    let after = rest[length..].trim_start();
    let after = if reference {
        after
    } else {
        match after.strip_prefix(".as_str()") {
            Some(after) => after.trim_start(),
            None => return false,
        }
    };
    after.starts_with(')')
}

/// Whether `before`, the code ending just before `new`, ends with `QueryBuilder::` or
/// `QueryBuilder::<..>::`.
fn follows_query_builder(before: &str) -> bool {
    let Some(mut path) = before.strip_suffix("::").map(str::trim_end) else {
        return false;
    };
    if path.ends_with('>') {
        let mut depth = 0_i32;
        let mut open = None;
        for (offset, character) in path.char_indices().rev() {
            match character {
                '>' => depth += 1,
                '<' => {
                    depth -= 1;
                    if depth == 0 {
                        open = Some(offset);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(turbofish) = open.and_then(|open| path[..open].trim_end().strip_suffix("::"))
        else {
            return false;
        };
        path = turbofish.trim_end();
    }
    path.strip_suffix("QueryBuilder").is_some_and(|prefix| {
        !prefix
            .bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    })
}

/// Calls of the sqlx statement constructors, `QueryBuilder::new` and executor methods in code
/// with literals and comments blanked out.
fn execution_sites(code: &str) -> Vec<Site> {
    let mut sites = Vec::new();
    let bytes = code.as_bytes();
    let is_ident = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut index = 0;
    while index < bytes.len() {
        if !is_ident(bytes[index]) || (index > 0 && is_ident(bytes[index - 1])) {
            index += 1;
            continue;
        }
        let end = index
            + code[index..]
                .bytes()
                .take_while(|byte| is_ident(*byte))
                .count();
        let word = &code[index..end];
        let before = code[..index].trim_end();
        let kind = if CONSTRUCTORS.contains(&word) && before.ends_with("sqlx::") {
            Some(SiteKind::Constructor)
        } else if word == "new" && follows_query_builder(before) {
            Some(SiteKind::Builder)
        } else if EXECUTORS.contains(&word) && before.ends_with('.') {
            Some(SiteKind::Executor)
        } else {
            None
        };
        if let Some(kind) = kind {
            let mut after = end;
            if code[after..].starts_with("::<") {
                let mut depth = 0_i32;
                for (offset, byte) in code[after + 2..].bytes().enumerate() {
                    match byte {
                        b'<' => depth += 1,
                        b'>' => {
                            depth -= 1;
                            if depth == 0 {
                                after += 2 + offset + 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
            let rest = &code[after..];
            if let Some(open) = rest.trim_start().strip_prefix('(') {
                sites.push(Site {
                    offset: index,
                    argument: code.len() - open.len(),
                    kind,
                });
            }
        }
        index = end;
    }
    sites
}

/// `/* project:<name> */` at the start of `text`: the name and what follows the comment.
fn marker_name(text: &str) -> Option<(String, &str)> {
    let rest = text.strip_prefix(MARKER_OPEN)?;
    let end = rest.find(MARKER_CLOSE)?;
    let name = &rest[..end];
    let valid = !name.is_empty()
        && !name.starts_with('.')
        && !name.ends_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "_.{}".contains(c));
    valid.then(|| (name.to_owned(), &rest[end + MARKER_CLOSE.len()..]))
}

fn collect(directory: &Path, rust: &mut Vec<PathBuf>, sql: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect(&path, rust, sql);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            rust.push(path);
        } else if path.extension().is_some_and(|extension| extension == "sql") {
            sql.push(path);
        }
    }
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap().display().to_string()
}

/// Files declared as `#[cfg(test)] mod x;`, and every file below such a module.
fn gated_files(files: &[PathBuf]) -> BTreeSet<PathBuf> {
    let mut gated = BTreeSet::new();
    for parent in files {
        let text = fs::read_to_string(parent).unwrap();
        let mut attributes: Vec<&str> = Vec::new();
        for line in text.lines().map(str::trim) {
            if line.starts_with("#[") {
                attributes.push(line);
                continue;
            }
            if line.is_empty() || line.starts_with("//") {
                continue;
            }
            let declaration = line
                .split_whitespace()
                .skip_while(|word| *word != "mod")
                .nth(1)
                .filter(|name| name.ends_with(';'));
            if let Some(name) = declaration
                && attributes.contains(&"#[cfg(test)]")
            {
                let name = name.trim_end_matches(';');
                let directory = parent.parent().unwrap();
                let explicit = attributes.iter().find_map(|attribute| {
                    attribute
                        .strip_prefix("#[path = \"")
                        .and_then(|path| path.strip_suffix("\"]"))
                });
                let module = match explicit {
                    Some(path) => directory.join(path),
                    None => {
                        let stem = parent.file_stem().unwrap();
                        let base = if stem == "lib" || stem == "mod" {
                            directory.to_owned()
                        } else {
                            directory.join(stem)
                        };
                        let file = base.join(format!("{name}.rs"));
                        if file.exists() {
                            file
                        } else {
                            base.join(name).join("mod.rs")
                        }
                    }
                };
                gated.insert(normalize(&module));
            }
            attributes.clear();
        }
    }
    for module in gated.clone() {
        let below = module.with_extension("");
        if below.is_dir() {
            let (mut rust, mut sql) = (Vec::new(), Vec::new());
            collect(&below, &mut rust, &mut sql);
            gated.extend(rust.into_iter().map(|path| normalize(&path)));
        }
    }
    files
        .iter()
        .filter(|file| gated.contains(&normalize(file)))
        .cloned()
        .collect()
}

fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::CurDir => {}
            other => normalized.push(other),
        }
    }
    normalized
}

struct Literal {
    start: usize,
    text: String,
}

/// A Rust file split into string literals and code with literals and comments blanked out.
struct Scanned {
    code: Vec<u8>,
    literals: Vec<Literal>,
}

impl Scanned {
    fn new(text: &str) -> Self {
        let bytes = text.as_bytes();
        let mut code = bytes.to_vec();
        let mut literals = Vec::new();
        let mut index = 0;
        let blank = |code: &mut Vec<u8>, from: usize, to: usize| {
            for byte in &mut code[from..to] {
                if *byte != b'\n' {
                    *byte = b' ';
                }
            }
        };
        while index < bytes.len() {
            let rest = &text[index..];
            let previous_is_ident =
                index > 0 && (bytes[index - 1].is_ascii_alphanumeric() || bytes[index - 1] == b'_');
            if rest.starts_with("//") {
                let end = rest.find('\n').map_or(bytes.len(), |offset| index + offset);
                blank(&mut code, index, end);
                index = end;
            } else if rest.starts_with("/*") {
                let end = rest
                    .find("*/")
                    .map_or(bytes.len(), |offset| index + offset + 2);
                blank(&mut code, index, end);
                index = end;
            } else if !previous_is_ident && let Some(hashes) = raw_string_hashes(rest) {
                let open = hashes.len() + 2;
                let close = format!("\"{hashes}");
                let body_end = index + open + rest[open..].find(&close).unwrap();
                literals.push(Literal {
                    start: index,
                    text: text[index + open..body_end].to_owned(),
                });
                let end = body_end + close.len();
                blank(&mut code, index, end);
                index = end;
            } else if bytes[index] == b'"' {
                let (decoded, end) = cooked_string(text, index);
                literals.push(Literal {
                    start: index,
                    text: decoded,
                });
                blank(&mut code, index, end);
                index = end;
            } else if bytes[index] == b'\'' {
                index += char_literal_length(rest).unwrap_or(1);
            } else {
                index += rest.chars().next().unwrap().len_utf8();
            }
        }
        Self { code, literals }
    }

    /// Byte ranges under `#[cfg(test)]`: the attributed item or statement, through its closing
    /// brace or semicolon.
    fn test_ranges(&self) -> Vec<(usize, usize)> {
        let code = String::from_utf8_lossy(&self.code);
        let mut ranges = Vec::new();
        let mut search = 0;
        while let Some(offset) = code[search..].find("#[cfg(test)]") {
            let start = search + offset;
            let mut index = start + "#[cfg(test)]".len();
            let bytes = code.as_bytes();
            while index < bytes.len() && bytes[index] != b'{' && bytes[index] != b';' {
                index += 1;
            }
            if index < bytes.len() && bytes[index] == b'{' {
                let mut depth = 0_i32;
                while index < bytes.len() {
                    match bytes[index] {
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    index += 1;
                }
            }
            ranges.push((start, index + 1));
            search = index + 1;
        }
        ranges
    }
}

fn raw_string_hashes(rest: &str) -> Option<&str> {
    let after_prefix = rest.strip_prefix("br").or_else(|| rest.strip_prefix('r'))?;
    let hashes = after_prefix.len() - after_prefix.trim_start_matches('#').len();
    after_prefix[hashes..]
        .starts_with('"')
        .then(|| &after_prefix[..hashes])
}

fn cooked_string(text: &str, start: usize) -> (String, usize) {
    let mut decoded = String::new();
    let mut chars = text[start + 1..].char_indices().peekable();
    while let Some((offset, character)) = chars.next() {
        match character {
            '"' => return (decoded, start + 1 + offset + 1),
            '\\' => match chars.next().map(|(_, escaped)| escaped) {
                Some('n') => decoded.push('\n'),
                Some('t') => decoded.push('\t'),
                Some('\n') => {
                    while chars.peek().is_some_and(|(_, next)| next.is_whitespace()) {
                        chars.next();
                    }
                }
                Some(other) => decoded.push(other),
                None => break,
            },
            other => decoded.push(other),
        }
    }
    panic!("unterminated string literal at byte {start}");
}

fn char_literal_length(rest: &str) -> Option<usize> {
    let mut chars = rest.char_indices().skip(1);
    let (_, first) = chars.next()?;
    let (end, _) = if first == '\\' {
        chars.find(|(_, character)| *character == '\'')?
    } else {
        chars.next().filter(|(_, character)| *character == '\'')?
    };
    Some(end + 1)
}
