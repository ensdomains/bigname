//! Every statement Project sends to PostgreSQL starts with a `/* project:<name> */` comment, so the
//! slow log, `pg_stat_activity` and `pg_stat_statements` name the statement that did the work.
//!
//! The guard reads the production sources of `crates/project/src` as text:
//!
//! - every `.sql` file: with `--` comments removed, each `;`-separated statement starts with an
//!   identifier, and the file's first line is one;
//! - every Rust string literal outside test code whose text starts with an SQL command keyword:
//!   the literal starts with an identifier. Fragments spliced into a larger statement carry one too;
//!   PostgreSQL treats the nested comment as whitespace.
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
    "EXPLAIN", "COPY", "CALL",
];

#[test]
fn every_production_statement_starts_with_an_identifier() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut rust_files = Vec::new();
    let mut sql_files = Vec::new();
    collect(&source_root, &mut rust_files, &mut sql_files);
    let gated = gated_files(&rust_files);

    let mut failures = Vec::new();
    let mut names: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut rust_sites = 0_usize;
    let mut sql_statements = 0_usize;
    let mut markers_in_text = 0_usize;

    for path in &sql_files {
        let text = fs::read_to_string(path).unwrap();
        let place = relative(&source_root, path);
        markers_in_text += text.matches(MARKER_OPEN).count();
        if marker_name(text.lines().next().unwrap_or_default())
            .is_none_or(|(_, rest)| !rest.trim().is_empty())
        {
            failures.push(format!("{place}: first line is not a statement identifier"));
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
            sql_statements += 1;
            match marker_name(statement) {
                Some((name, _)) => names.entry(name).or_default().push(place.clone()),
                None => failures.push(format!(
                    "{place}: statement {} does not start with an identifier",
                    index + 1
                )),
            }
        }
    }

    for path in rust_files.iter().filter(|path| !gated.contains(*path)) {
        let text = fs::read_to_string(path).unwrap();
        let place = relative(&source_root, path);
        let scanned = Scanned::new(&text);
        let test_ranges = scanned.test_ranges();
        for literal in &scanned.literals {
            if test_ranges
                .iter()
                .any(|range| range.0 <= literal.start && literal.start < range.1)
            {
                continue;
            }
            markers_in_text += literal.text.matches(MARKER_OPEN).count();
            let body = literal.text.trim_start();
            let is_statement = marker_name(body).is_some()
                || KEYWORDS.iter().any(|keyword| {
                    body.strip_prefix(keyword).is_some_and(|rest| {
                        rest.is_empty() || rest.starts_with(|c: char| !c.is_ascii_alphanumeric())
                    })
                });
            if !is_statement {
                continue;
            }
            rust_sites += 1;
            let line = text[..literal.start].matches('\n').count() + 1;
            match marker_name(&literal.text) {
                Some((name, _)) => names
                    .entry(name)
                    .or_default()
                    .push(format!("{place}:{line}")),
                None => failures.push(format!(
                    "{place}:{line}: statement does not start with an identifier: {}",
                    body.chars().take(60).collect::<String>().replace('\n', " ")
                )),
            }
        }
    }

    for (name, places) in &names {
        if places.len() > 1 {
            failures.push(format!(
                "identifier {name} is used by {}",
                places.join(", ")
            ));
        }
    }
    let identified = names.values().map(Vec::len).sum::<usize>();
    println!(
        "statement identifiers: {rust_sites} Rust statement sites, {sql_statements} statements in \
         {} .sql files, {identified} identified, {markers_in_text} identifiers in the text",
        sql_files.len()
    );
    assert!(
        failures.is_empty(),
        "{} statement identifier failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert_eq!(
        markers_in_text,
        rust_sites + sql_statements,
        "an identifier appears outside a statement start"
    );
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
