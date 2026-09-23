//! Every `.sql` file under `crates/project/src` is a hash input, whatever code loads it. SQL
//! that only tests load (fixtures, reference oracles) belongs in `crates/project/testdata/sql/`,
//! so that editing it does not rotate the interpreter content hash.
//!
//! # Contract
//!
//! The guard fails unless every `.sql` file under `crates/project/src` has at least one
//! production loader. A production loader is an `include_str!` call that meets all of these:
//!
//! - It is the built-in `include_str!`, spelled bare, as `std::include_str!` or
//!   `core::include_str!` (with or without a leading `::`, raw identifiers allowed), and its
//!   argument is a single string literal. The literal is resolved against the directory of the
//!   file that contains it.
//! - It sits in ordinary code, or inside the arguments of a macro on the audited list, matched
//!   by its full path: the listed `std`/`core` built-ins (bare or qualified) and the listed
//!   `sqlx`, `tracing`, `tokio`, `serde_json` and `anyhow` macros. Anything inside `stringify!`
//!   or a `macro_rules!` definition is text, not a call.
//! - No `#[cfg(test)]` gates it: not on any enclosing node (item, statement, expression, field,
//!   match arm, parameter and the like), not as an inner `#![cfg(test)]` on its file, and not
//!   on any module declaration along the route to its file.
//! - Its file is reached from `crates/project/src/lib.rs` through the module tree, where
//!   `mod x;` resolves to `x.rs` or `x/mod.rs` in the declaring module's directory and
//!   `#[path = "..."]` resolves against the declaring file's directory. A file reached by any
//!   ungated route is production, even if another route to it is gated.
//!
//! Loaders in files under `crates/project/tests`, or in files reached only through gated
//! routes, are test loaders. A `.rs` file that no module declaration reaches is scanned for
//! diagnostics only: its includes are named as "not reached from the crate root" and never
//! count as loader evidence.
//!
//! The guard also fails, before it reports missing loaders, on any Rust file that does not
//! parse, and on each production `include_str!` it cannot read: a computed path (anything but
//! one string literal, such as `concat!(...)`), or an include inside a macro that is not on the
//! audited list. Such spellings in test code are recorded but do not fail the guard.
//!
//! # Known limitations
//!
//! Both are tracked in Linear TYR-15.
//!
//! - Macro identity shadowing: a local `macro_rules!` named `stringify`, `assert`,
//!   `include_str` or another recognised name is still treated as the built-in.
//! - `#[path]` module-resolution context: rustc resolves the ordinary child modules of a file
//!   loaded through `#[path]` beside that file, while this guard looks for them under a
//!   directory named after the file's stem. In a non-`mod.rs` file, rustc applies an inline
//!   module's `#[path]` relative to the file's own directory, before the file-stem component;
//!   this guard applies it after. A file missed this way is treated as not reached, so its
//!   includes never count as production loaders.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use proc_macro2::{TokenStream, TokenTree};
use syn::{
    Arm, Attribute, BareFnArg, Block, ConstParam, Expr, ExprLit, Field, FieldPat, FieldValue,
    ForeignItem, ImplItem, Item, LifetimeParam, Lit, LitStr, Local, Macro, Meta, PatType, Receiver,
    StmtMacro, Token, TraitItem, TypeParam, Variadic, Variant,
    ext::IdentExt,
    parse::{ParseStream, Parser},
    punctuated::Punctuated,
    visit::{self, Visit},
};

use super::workspace_root;

const PROJECT_SOURCE_ROOT: &str = "crates/project/src";

#[derive(Default)]
struct References {
    production: BTreeSet<String>,
    test: BTreeSet<String>,
    /// Files no module declaration reaches from `lib.rs`: named in the failure, never counted.
    unreached: BTreeSet<String>,
}

/// How the Project crate's module graph reaches a Rust file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reach {
    /// At least one route from `lib.rs` passes no `#[cfg(test)]` gate.
    Production,
    /// Every route from `lib.rs` passes a `#[cfg(test)]` gate, or the file is an integration test
    /// under `crates/project/tests`.
    TestOnly,
    /// No module declaration leads to the file from `lib.rs`.
    NotReached,
}

#[test]
fn project_sql_under_the_hashed_root_is_loaded_by_production_code() {
    let root = workspace_root();
    let mut sql_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "sql", &mut sql_files);
    assert!(
        !sql_files.is_empty(),
        "expected production SQL under {PROJECT_SOURCE_ROOT}"
    );
    let failures = inventory_failures(&root);
    assert!(
        failures.is_empty(),
        "SQL under {PROJECT_SOURCE_ROOT} is a content-hash input; every file there needs a \
         production loader, and test-only SQL belongs in crates/project/testdata/sql/:\n{}",
        failures.join("\n")
    );
}

#[test]
fn include_scanner_separates_cfg_test_items_from_production_code() {
    let source = r#####"
        const A: &str = include_str!("production.sql");
        const RAW: &str = include_str!(r"raw_production.sql");
        const SPACED: &str = std::include_str! ( "spaced_production.sql" );
        fn build(x: bool) -> u8 {
            #[cfg(test)]
            if x {
                let _ = include_str!("gated_if.sql");
                let _ = include_str!(r#"raw_gated.sql"#);
                return 1;
            }
            let _ = "include_str!(\"in_string.sql\")";
            let _ = my_include_str!("other_macro.sql");
            #[cfg(test)]
            let y = if x { include_str!("gated_let.sql") } else { "}" };
            let _ = '{';
            let _ = r#"{ include_str!("in_raw_string.sql") "#;
            // include_str!("in_comment.sql") {
            let _: &'static str = include_str!(
                "after_lifetime.sql"
            );
            0
        }
        struct S {
            #[cfg(test)]
            gated: [u8; 2],
            open: u8,
        }
        #[cfg(test)]
        #[path = "x.rs"]
        mod tests;
        const B: &str = include_str!("after_module.sql");
    "#####;
    let found = include_sites(source);
    assert_eq!(
        found,
        vec![
            ("production.sql".to_owned(), false),
            ("raw_production.sql".to_owned(), false),
            ("spaced_production.sql".to_owned(), false),
            ("gated_if.sql".to_owned(), true),
            ("raw_gated.sql".to_owned(), true),
            ("gated_let.sql".to_owned(), true),
            ("after_lifetime.sql".to_owned(), false),
            ("after_module.sql".to_owned(), false),
        ]
    );
}

#[test]
fn include_scanner_accepts_every_literal_spelling() {
    let source = r#####"
        const C1: &str = include_str!(/* note */ "block_comment_production.sql");
        const C2: &str = include_str! // note
            ("line_comment_production.sql");
        const K: &str = include_str!["bracket_production.sql"];
        const BR: &str = include_str!{"brace_production.sql"};
        const H: &str = include_str!("hex_production\x2esql");
        const U: &str = include_str!("unicode_production\u{2e}sql");
        const N: &str = include_str!("continued_\
                                      production.sql");
        #[cfg(test)]
        fn gated_forms() {
            let _ = include_str!(/* note */ "block_comment_gated.sql");
            let _ = include_str!["bracket_gated.sql"];
            let _ = include_str!{"brace_gated.sql"};
            let _ = include_str!("hex_gated\x2esql");
            let _ = include_str!("unicode_gated\u{2e}sql");
            let _ = include_str!("continued_\
                                  gated.sql");
        }
        fn build() -> usize {
            #[cfg(test)]
            let a = include_str!{"brace_gate_first.sql"}.len() + include_str!("brace_gate_second.sql").len();
            let p = include_str!("after_brace_gate.sql");
            0
        }
    "#####;
    assert_eq!(
        include_sites(source),
        vec![
            ("block_comment_production.sql".to_owned(), false),
            ("line_comment_production.sql".to_owned(), false),
            ("bracket_production.sql".to_owned(), false),
            ("brace_production.sql".to_owned(), false),
            ("hex_production.sql".to_owned(), false),
            ("unicode_production.sql".to_owned(), false),
            ("continued_production.sql".to_owned(), false),
            ("block_comment_gated.sql".to_owned(), true),
            ("bracket_gated.sql".to_owned(), true),
            ("brace_gated.sql".to_owned(), true),
            ("hex_gated.sql".to_owned(), true),
            ("unicode_gated.sql".to_owned(), true),
            ("continued_gated.sql".to_owned(), true),
            ("brace_gate_first.sql".to_owned(), true),
            ("brace_gate_second.sql".to_owned(), true),
            ("after_brace_gate.sql".to_owned(), false),
        ]
    );
}

#[test]
fn include_scanner_reports_spellings_it_cannot_read() {
    let source = r#####"
        const A: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/computed.sql"));
        const D: &str = include_str!(PATH);
        const E: &str = include_str!("two" "literals.sql");
        const F: &str = include_str!(b"bytes.sql");
        const G: &str = std::include_str!(c"c_string.sql");
        #[cfg(test)]
        const H: &str = include_str!(concat!("gated", ".sql"));
        const I: &str = include_str!("still_read.sql");
        const J: bool = include_str != 0;
        const K: &str = my::include_str!(concat!("other", ".sql"));
    "#####;
    let scan = scan_includes(source).expect("fixture must parse as Rust");
    assert_eq!(scan.sites, vec![("still_read.sql".to_owned(), false)]);
    assert_eq!(
        scan.unsupported,
        vec![
            UnsupportedSite {
                line: 2,
                gated: false,
                kind: UnsupportedKind::Computed,
            },
            UnsupportedSite {
                line: 3,
                gated: false,
                kind: UnsupportedKind::Computed,
            },
            UnsupportedSite {
                line: 4,
                gated: false,
                kind: UnsupportedKind::Computed,
            },
            UnsupportedSite {
                line: 5,
                gated: false,
                kind: UnsupportedKind::Computed,
            },
            UnsupportedSite {
                line: 6,
                gated: false,
                kind: UnsupportedKind::Computed,
            },
            UnsupportedSite {
                line: 8,
                gated: true,
                kind: UnsupportedKind::Computed,
            },
        ]
    );
    let message = unsupported_message(
        "crates/project/src/scope/example.rs",
        2,
        UnsupportedKind::Computed,
    );
    for expected in [
        "crates/project/src/scope/example.rs:2:",
        "include_str!(\"path\")",
        "include_str![\"path\"]",
        "include_str!{\"path\"}",
        "concat!",
        "use a plain literal path",
    ] {
        assert!(message.contains(expected), "{message} lacks {expected}");
    }
}

#[test]
fn inventory_reports_an_unreadable_production_spelling_before_the_loader_it_hides() {
    let tree = super::SampleTree::empty();
    tree.write(
        "crates/project/src/scope/mirror.rs",
        "pub fn stage() -> &'static str {\n    include_str!(concat!(\"mirror\", \".sql\"))\n}\n",
    );
    tree.write("crates/project/src/scope/mirror.sql", "SELECT 1;\n");
    tree.write("crates/project/src/scope/broken.rs", "fn broken( {\n");
    tree.write("crates/project/src/lib.rs", "mod scope;\n");
    tree.write(
        "crates/project/src/scope/mod.rs",
        "mod broken;\nmod mirror;\n",
    );
    let failures = inventory_failures(tree.path());
    assert_eq!(failures.len(), 3, "{failures:#?}");
    assert!(
        failures[0].starts_with("crates/project/src/scope/broken.rs: does not parse as Rust"),
        "{failures:#?}"
    );
    assert_eq!(
        failures[1],
        unsupported_message(
            "crates/project/src/scope/mirror.rs",
            2,
            UnsupportedKind::Computed
        )
    );
    assert_eq!(
        failures[2],
        "crates/project/src/scope/mirror.sql (not loaded by any Rust source)"
    );
}

#[test]
fn current_tree_keeps_computed_includes_in_test_code() {
    let root = workspace_root();
    let inventory = project_sql_inventory(&root);
    assert!(
        inventory.unparsable.is_empty(),
        "{:#?}",
        inventory.unparsable
    );
    // Project tests load migrations through include_str!(concat!(env!(...), ...)), which the
    // guard cannot read; they must all be recognized as test code.
    assert!(
        !inventory.unsupported.is_empty(),
        "expected the computed migration includes in Project tests to be reported"
    );
    for (message, test) in &inventory.unsupported {
        eprintln!("unsupported (test code: {test}): {message}");
        assert!(test, "{message}");
    }
    let mut sql_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "sql", &mut sql_files);
    let loaded = sql_files
        .iter()
        .filter(|path| {
            inventory
                .references
                .get(&relative_key(&root, path))
                .is_some_and(|entry| !entry.production.is_empty())
        })
        .count();
    eprintln!(
        "production SQL files with a production loader: {loaded}/{}",
        sql_files.len()
    );
    assert_eq!(loaded, sql_files.len());
}

/// A scanner fixture: name, source, and the expected `(path, test-only)` sites.
type ScannerCase = (&'static str, String, Vec<(String, bool)>);

/// An inventory fixture with literal expected failures: name, `(path, contents)` files, and the expected failures.
type InventoryCase = (
    &'static str,
    Vec<(&'static str, &'static str)>,
    Vec<&'static str>,
);

/// An inventory fixture whose expected failures are built at run time.
type InventoryFailureCase = (&'static str, Vec<(&'static str, &'static str)>, Vec<String>);

#[test]
fn include_scanner_follows_rust_tokens_and_statement_boundaries() {
    let production = |path: &str| vec![(path.to_owned(), false)];
    let gated = |path: &str| vec![(path.to_owned(), true)];
    let cases: Vec<ScannerCase> = vec![
        (
            "gated brace-macro statement then a production include",
            "fn f() {\n    #[cfg(test)]\n    assert! { true }\n    execute(include_str!(\"mirror.sql\"));\n}\n"
                .to_owned(),
            production("mirror.sql"),
        ),
        (
            "gated let with stringify! and a comment before its braces",
            "fn f() {\n    #[cfg(test)]\n    let _ = stringify! /* c */ {x}.len()\n        + include_str!(\"orphan.sql\").len();\n}\n"
                .to_owned(),
            gated("orphan.sql"),
        ),
        (
            "raw C string holding macro-looking text, then a gated include",
            "const _: &std::ffi::CStr = cr#\"\" include_str!(\"orphan.sql\") \"\"#;\n#[cfg(test)]\nconst _: &str = include_str!(\"orphan.sql\");\n"
                .to_owned(),
            gated("orphan.sql"),
        ),
        (
            "identifier continuing through a combining mark",
            "fn f() { x\u{301}include_str!(\"y.sql\"); }\n".to_owned(),
            vec![],
        ),
        (
            "left-to-right mark between the name and the bang",
            "const M: &str = include_str\u{200e}!(\"mirror.sql\");\n".to_owned(),
            production("mirror.sql"),
        ),
        (
            "continuation followed by a no-break space",
            "const M: &str = include_str!(\"mir\\\n\u{a0}ror.sql\");\n".to_owned(),
            production("mir\u{a0}ror.sql"),
        ),
        (
            "literal containing CRLF",
            "const M: &str = include_str!(\"multi\r\nline.sql\");\r\n".to_owned(),
            production("multi\nline.sql"),
        ),
        (
            "gated function parameter whose type holds an include",
            "pub fn f(#[cfg(test)] _: [u8; include_str!(\"orphan.sql\").len()]) {}\n".to_owned(),
            gated("orphan.sql"),
        ),
        (
            "raw-identifier macro names, bare and std-qualified",
            "const A: &str = r#include_str!(\"mirror.sql\");\nconst B: &str = std::r#include_str!(\"mirror.sql\");\nconst C: &str = r#include_strs!(\"other.sql\");\n"
                .to_owned(),
            vec![("mirror.sql".to_owned(), false), ("mirror.sql".to_owned(), false)],
        ),
        (
            "gated macro statement whose parsed body holds an include",
            "fn f() {\n    #[cfg(test)]\n    assert!(include_str!(\"orphan.sql\").len() > 0);\n}\n".to_owned(),
            gated("orphan.sql"),
        ),
        (
            "quoted and uninvoked includes are not loaders",
            "const _: &str = stringify!(include_str!(\"orphan.sql\"));\nmacro_rules! m { () => { include_str!(\"orphan.sql\") }; }\nfn q() { let _ = quote!(include_str!(\"orphan.sql\")); }\n"
                .to_owned(),
            vec![],
        ),
        (
            "audited macros, qualified built-ins and crate paths, expand their arguments",
            "fn f() {\n    ::std::assert!(include_str!(\"a.sql\").len() > 0);\n    let _ = core::format_args!(\"{}\", include_str!(\"b.sql\"));\n    tracing::debug!(sql = include_str!(\"c.sql\"));\n}\n"
                .to_owned(),
            vec![
                ("a.sql".to_owned(), false),
                ("b.sql".to_owned(), false),
                ("c.sql".to_owned(), false),
            ],
        ),
        (
            "unaudited macros never count, even when their body parses",
            "use tracing::debug;\nfn f() {\n    debug!(sql = include_str!(\"bare.sql\"));\n    my::assert!(include_str!(\"custom.sql\").len() > 0);\n    ::assert!(include_str!(\"rooted.sql\").len() > 0);\n}\n"
                .to_owned(),
            vec![],
        ),
        (
            "concat! expands a nested include",
            "const B: &str = concat!(include_str!(\"build.sql\"), \"        \");\n".to_owned(),
            production("build.sql"),
        ),
        (
            "include inside a macro body that does not parse is not a loader",
            "fn f() { tokio::select! { x = include_str!(\"opaque.sql\") => {} } }\n".to_owned(),
            vec![],
        ),
    ];
    let failures = cases
        .into_iter()
        .filter_map(|(name, source, expected)| {
            let found = include_sites(&source);
            (found != expected).then(|| format!("{name}: found {found:?}, expected {expected:?}"))
        })
        .collect::<Vec<_>>();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn include_scanner_reports_an_include_inside_an_opaque_macro() {
    let source = "fn f() {\n    tokio::select! { x = include_str!(\"opaque.sql\") => {} }\n    #[cfg(test)]\n    tokio::select! { x = include_str!(\"gated.sql\") => {} }\n}\n";
    let scan = scan_includes(source).expect("fixture must parse as Rust");
    assert!(scan.sites.is_empty(), "{:?}", scan.sites);
    assert_eq!(
        scan.unsupported,
        vec![
            UnsupportedSite {
                line: 2,
                gated: false,
                kind: UnsupportedKind::Opaque,
            },
            UnsupportedSite {
                line: 4,
                gated: true,
                kind: UnsupportedKind::Opaque,
            },
        ]
    );
    let message = unsupported_message("crates/project/src/x.rs", 2, UnsupportedKind::Opaque);
    assert!(
        message.starts_with(
            "crates/project/src/x.rs:2: include inside an opaque macro; bind the path with a \
             direct include_str! literal outside the macro"
        ),
        "{message}"
    );
}

#[test]
fn raw_identifier_spelling_of_the_real_mirror_loader_still_counts() {
    let source = fs::read_to_string(workspace_root().join("crates/project/src/scope/mirror.rs"))
        .expect("mirror.rs must be readable");
    assert!(source.contains("include_str!(\"mirror.sql\")"));
    let mutated = source.replace(
        "include_str!(\"mirror.sql\")",
        "r#include_str!(\"mirror.sql\")",
    );
    assert!(
        include_sites(&mutated).contains(&("mirror.sql".to_owned(), false)),
        "r#include_str! must still load mirror.sql from production code"
    );
}

#[test]
fn inventory_takes_loader_evidence_only_from_expanded_includes() {
    let cases: Vec<InventoryCase> = vec![
        (
            "quoted include in production plus a gated real include",
            vec![
                ("crates/project/src/scope/mod.rs", "mod quoted;\n"),
                (
                    "crates/project/src/scope/quoted.rs",
                    "const _: &str = stringify!(include_str!(\"orphan.sql\"));\n#[cfg(test)]\nconst _: &str = include_str!(\"orphan.sql\");\n",
                ),
            ],
            vec![
                "crates/project/src/scope/orphan.sql (loaded only by test code: crates/project/src/scope/quoted.rs)",
            ],
        ),
        (
            "gate inside a parsed macro body",
            vec![
                ("crates/project/src/scope/mod.rs", "mod bodies;\n"),
                (
                    "crates/project/src/scope/bodies.rs",
                    "pub fn sample() {\n    let _ = vec![{\n        #[cfg(test)]\n        let _ = include_str!(\"orphan.sql\");\n        0\n    }];\n}\n",
                ),
            ],
            vec![
                "crates/project/src/scope/orphan.sql (loaded only by test code: crates/project/src/scope/bodies.rs)",
            ],
        ),
        (
            "quoted computed include is not a diagnostic",
            vec![
                (
                    "crates/project/src/scope/mod.rs",
                    "mod loader;\nmod quoted;\n",
                ),
                (
                    "crates/project/src/scope/quoted.rs",
                    "const _: &str = stringify!(include_str!(concat!(\"x\", \".sql\")));\n",
                ),
                (
                    "crates/project/src/scope/loader.rs",
                    "const _: &str = include_str!(\"orphan.sql\");\n",
                ),
            ],
            vec![],
        ),
        (
            "inner cfg(test) in a child file declared by an ordinary mod",
            vec![
                ("crates/project/src/lib.rs", "mod fixture;\n"),
                (
                    "crates/project/src/fixture.rs",
                    "#![cfg(test)]\nconst _: &str = include_str!(\"scope/orphan.sql\");\n",
                ),
            ],
            vec![
                "crates/project/src/scope/orphan.sql (loaded only by test code: crates/project/src/fixture.rs)",
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (name, files, expected) in cases {
        let tree = super::SampleTree::empty();
        tree.write("crates/project/src/scope/orphan.sql", "SELECT 1;\n");
        tree.write("crates/project/src/lib.rs", "mod scope;\n");
        for (path, contents) in files {
            tree.write(path, contents);
        }
        let found = inventory_failures(tree.path());
        if found != expected {
            failures.push(format!("{name}: found {found:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn inventory_admits_only_audited_macros_and_inherited_file_gates() {
    let opaque = |file: &str, line: usize| unsupported_message(file, line, UnsupportedKind::Opaque);
    let cases: Vec<InventoryFailureCase> = vec![
        (
            "a parseable argument to an unknown macro is not a loader",
            vec![
                ("crates/project/src/lib.rs", "mod discard;\n"),
                (
                    "crates/project/src/discard.rs",
                    "#[macro_export]\nmacro_rules! discard {\n    ($value:expr) => { \"\" };\n}\n\nconst _: &str = crate::discard!(include_str!(\"orphan.sql\"));\n\n#[cfg(test)]\nconst _: &str = include_str!(\"orphan.sql\");\n",
                ),
                ("crates/project/src/orphan.sql", "SELECT 1;\n"),
            ],
            vec![
                opaque("crates/project/src/discard.rs", 6),
                "crates/project/src/orphan.sql (loaded only by test code: crates/project/src/discard.rs)"
                    .to_owned(),
            ],
        ),
        (
            "a macro that adds a gate in its transcription is not a loader",
            vec![
                ("crates/project/src/lib.rs", "mod gate;\n"),
                (
                    "crates/project/src/gate.rs",
                    "macro_rules! gate {\n    ($value:expr) => { #[cfg(test)] const _: &str = $value; };\n}\n\ngate!(include_str!(\"orphan.sql\"));\n",
                ),
                ("crates/project/src/orphan.sql", "SELECT 1;\n"),
            ],
            vec![
                opaque("crates/project/src/gate.rs", 5),
                "crates/project/src/orphan.sql (not loaded by any Rust source)".to_owned(),
            ],
        ),
        (
            "an inner file gate reaches an external child module",
            vec![
                ("crates/project/src/lib.rs", "mod fixture;\n"),
                ("crates/project/src/fixture.rs", "#![cfg(test)]\nmod nested;\n"),
                (
                    "crates/project/src/fixture/nested.rs",
                    "const _: &str = include_str!(\"orphan.sql\");\n",
                ),
                ("crates/project/src/fixture/orphan.sql", "SELECT 1;\n"),
            ],
            vec![
                "crates/project/src/fixture/orphan.sql (loaded only by test code: crates/project/src/fixture/nested.rs)"
                    .to_owned(),
            ],
        ),
        (
            "a crate-qualified stringify! is not the built-in",
            vec![
                ("crates/project/src/lib.rs", "mod forward;\n"),
                (
                    "crates/project/src/forward.rs",
                    "const _: &str = crate::stringify!(include_str!(\"loaded.sql\"));\n",
                ),
                ("crates/project/src/loaded.sql", "SELECT 1;\n"),
            ],
            vec![
                opaque("crates/project/src/forward.rs", 1),
                "crates/project/src/loaded.sql (not loaded by any Rust source)".to_owned(),
            ],
        ),
        (
            "quote_spanned! evaluates its span expression",
            vec![
                ("crates/project/src/lib.rs", "mod spanned;\n"),
                (
                    "crates/project/src/spanned.rs",
                    "fn sample() {\n    let _ = quote::quote_spanned!(\n        {\n            let _ = include_str!(\"loaded.sql\");\n            proc_macro2::Span::call_site()\n        } => include_str!(\"quoted.sql\")\n    );\n}\n",
                ),
                ("crates/project/src/loaded.sql", "SELECT 1;\n"),
            ],
            vec![
                opaque("crates/project/src/spanned.rs", 4),
                opaque("crates/project/src/spanned.rs", 6),
                "crates/project/src/loaded.sql (not loaded by any Rust source)".to_owned(),
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (name, files, expected) in cases {
        let tree = super::SampleTree::empty();
        for (path, contents) in files {
            tree.write(path, contents);
        }
        let found = inventory_failures(tree.path());
        if found != expected {
            failures.push(format!(
                "{name}:\n  found    {found:?}\n  expected {expected:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn inventory_trusts_the_module_graph_for_reachability() {
    let cases: Vec<InventoryCase> = vec![
        (
            "a file reached by a production route and a gated route is production",
            vec![
                (
                    "crates/project/src/lib.rs",
                    "#[path = \"shared.rs\"]\nmod production;\n\n#[cfg(test)]\n#[path = \"shared.rs\"]\nmod tests;\n",
                ),
                (
                    "crates/project/src/shared.rs",
                    "pub const SQL: &str = include_str!(\"shared.sql\");\n",
                ),
                ("crates/project/src/shared.sql", "SELECT 1;\n"),
            ],
            vec![],
        ),
        (
            "a file no declaration reaches is not a loader",
            vec![
                (
                    "crates/project/src/lib.rs",
                    "#[cfg(test)]\nconst _: &str = include_str!(\"orphan.sql\");\n",
                ),
                (
                    "crates/project/src/unused.rs",
                    "const _: &str = include_str!(\"orphan.sql\");\n",
                ),
                ("crates/project/src/orphan.sql", "SELECT 1;\n"),
            ],
            vec![
                "crates/project/src/orphan.sql (loaded only by test code: crates/project/src/lib.rs; \
                 not reached from the crate root: crates/project/src/unused.rs)",
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (name, files, expected) in cases {
        let tree = super::SampleTree::empty();
        for (path, contents) in files {
            tree.write(path, contents);
        }
        let found = inventory_failures(tree.path());
        if found != expected {
            failures.push(format!(
                "{name}:\n  found    {found:?}\n  expected {expected:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// What the guard learned about one tree: each `.sql` path's loaders, every `include_str!` it
/// could not read (message, and whether it is in test code), and every Rust file that does not
/// parse.
#[derive(Default)]
struct Inventory {
    references: BTreeMap<String, References>,
    unsupported: Vec<(String, bool)>,
    unparsable: Vec<String>,
}

/// Every reason the tree under `root` breaks the rule, in one list: Rust files that do not
/// parse, then production `include_str!` spellings the guard cannot read, then SQL under the
/// hashed root without a production loader. Reporting them together keeps an unreadable
/// production spelling from being masked by the missing loader it causes.
fn inventory_failures(root: &Path) -> Vec<String> {
    let inventory = project_sql_inventory(root);
    let mut failures = inventory.unparsable;
    failures.extend(
        inventory
            .unsupported
            .into_iter()
            .filter(|(_, test)| !test)
            .map(|(message, _)| message),
    );
    let mut sql_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "sql", &mut sql_files);
    for path in sql_files {
        let key = relative_key(root, &path);
        let entry = inventory.references.get(&key);
        if entry.is_some_and(|entry| !entry.production.is_empty()) {
            continue;
        }
        let join = |files: &BTreeSet<String>| files.iter().cloned().collect::<Vec<_>>().join(", ");
        let loaders = entry.map(|entry| join(&entry.test)).unwrap_or_default();
        let unreached = entry
            .map(|entry| join(&entry.unreached))
            .unwrap_or_default();
        let mut failure = if loaders.is_empty() {
            format!("{key} (not loaded by any Rust source")
        } else {
            format!("{key} (loaded only by test code: {loaders}")
        };
        if !unreached.is_empty() {
            failure.push_str(&format!("; not reached from the crate root: {unreached}"));
        }
        failure.push(')');
        failures.push(failure);
    }
    failures
}

/// Maps each `.sql` file under the Project source root to the Rust files that load it with
/// `include_str!`, split into production loaders, test-only loaders, and files the module graph
/// does not reach. The module graph from `lib.rs` is authoritative: a loader is test-only when
/// every route to its file is gated (see [`module_reach`]), when its file is under
/// `crates/project/tests`, or when the `include_str!` sits inside an item, statement,
/// expression, or other node marked `#[cfg(test)]`. A file no declaration reaches is scanned
/// for diagnostics only; its includes are listed but never count as loader evidence.
fn project_sql_inventory(root: &Path) -> Inventory {
    let mut rust_files = Vec::new();
    collect_files(&root.join(PROJECT_SOURCE_ROOT), "rs", &mut rust_files);
    collect_files(&root.join("crates/project/tests"), "rs", &mut rust_files);

    let mut inventory = Inventory::default();
    let mut parsed = BTreeMap::new();
    for path in rust_files {
        let key = relative_key(root, &path);
        let source = fs::read_to_string(&path).expect("Project source must be readable");
        match parse_source(&source) {
            Ok(file) => {
                parsed.insert(normalize(&path), file);
            }
            Err(error) => inventory
                .unparsable
                .push(format!("{key}: does not parse as Rust: {error}")),
        }
    }
    let gates = module_reach(root, &parsed);

    for (path, file) in &parsed {
        let key = relative_key(root, path);
        let reach = if !key.starts_with(&format!("{PROJECT_SOURCE_ROOT}/")) {
            Reach::TestOnly
        } else {
            match gates.get(path) {
                Some(false) => Reach::Production,
                Some(true) => Reach::TestOnly,
                None => Reach::NotReached,
            }
        };
        let scan = scan_file(file, reach != Reach::Production);
        for site in scan.unsupported {
            inventory
                .unsupported
                .push((unsupported_message(&key, site.line, site.kind), site.gated));
        }
        for (literal, gated) in scan.sites {
            if !literal.ends_with(".sql") {
                continue;
            }
            let target = normalize(&path.parent().expect("file has a parent").join(&literal));
            let target_key = relative_key(root, &target);
            let entry = inventory.references.entry(target_key).or_default();
            if reach == Reach::NotReached {
                entry.unreached.insert(key.clone());
            } else if gated {
                entry.test.insert(key.clone());
            } else {
                entry.production.insert(key.clone());
            }
        }
    }
    inventory
}

/// Walks the Project crate's module tree from `lib.rs`, resolving each `mod x;` to `x.rs` or
/// `x/mod.rs` in the declaring module's directory (or to its `#[path]`), and returns, for every
/// file reached, whether it is test-only: every route to it passes an outer `#[cfg(test)]` on a
/// module or an inner `#![cfg(test)]` in a file. One ungated route makes the file production.
/// A file absent from the result is not reached from the crate root.
fn module_reach(root: &Path, parsed: &BTreeMap<PathBuf, syn::File>) -> BTreeMap<PathBuf, bool> {
    let mut gates: BTreeMap<PathBuf, bool> = BTreeMap::new();
    let crate_root = normalize(&root.join(PROJECT_SOURCE_ROOT).join("lib.rs"));
    let mut pending = vec![(crate_root, false)];
    while let Some((path, inherited)) = pending.pop() {
        let Some(file) = parsed.get(&path) else {
            continue;
        };
        match gates.get(&path) {
            // Already reached from production, or reached gated and this route is gated too.
            Some(false) => continue,
            Some(true) if inherited => continue,
            _ => {}
        }
        gates.insert(path.clone(), inherited);
        let gated = inherited || file.attrs.iter().any(is_cfg_test);
        let directory = path.parent().expect("file has a parent").to_owned();
        let mod_rs = path
            .file_name()
            .is_some_and(|name| name == "mod.rs" || name == "lib.rs" || name == "main.rs");
        let module_directory = if mod_rs {
            directory.clone()
        } else {
            directory.join(path.file_stem().expect("file has a stem"))
        };
        collect_module_files(
            &file.items,
            &directory,
            &module_directory,
            false,
            gated,
            &mut pending,
        );
    }
    gates
}

fn collect_module_files(
    items: &[Item],
    file_directory: &Path,
    module_directory: &Path,
    inline: bool,
    gated: bool,
    pending: &mut Vec<(PathBuf, bool)>,
) {
    for item in items {
        let Item::Mod(module) = item else {
            continue;
        };
        let gated = gated || module.attrs.iter().any(is_cfg_test);
        let path_attr = module.attrs.iter().find_map(path_attribute);
        let name = module.ident.unraw().to_string();
        match &module.content {
            Some((_, items)) => {
                let directory = module_directory.join(path_attr.as_deref().unwrap_or(&name));
                collect_module_files(items, file_directory, &directory, true, gated, pending);
            }
            None => {
                let candidates = match path_attr {
                    // A top-level #[path] is relative to the declaring file's directory; inside
                    // an inline module it is relative to that module's directory.
                    Some(explicit) if inline => vec![module_directory.join(explicit)],
                    Some(explicit) => vec![file_directory.join(explicit)],
                    None => vec![
                        module_directory.join(format!("{name}.rs")),
                        module_directory.join(&name).join("mod.rs"),
                    ],
                };
                for candidate in candidates {
                    pending.push((normalize(&candidate), gated));
                }
            }
        }
    }
}

fn path_attribute(attr: &Attribute) -> Option<String> {
    match &attr.meta {
        Meta::NameValue(value) if value.path.is_ident("path") => match &value.value {
            Expr::Lit(ExprLit {
                lit: Lit::Str(path),
                ..
            }) => Some(path.value()),
            _ => None,
        },
        _ => None,
    }
}

fn unsupported_message(file: &str, line: usize, kind: UnsupportedKind) -> String {
    match kind {
        UnsupportedKind::Computed => format!(
            "{file}:{line}: include_str! spelling this guard cannot read. Write \
             include_str!(\"path\"), include_str![\"path\"] or include_str!{{\"path\"}} with \
             one plain or raw string literal as the path (whitespace and // or /* */ comments \
             between the tokens are fine). concat! and other computed paths are not supported: \
             use a plain literal path."
        ),
        UnsupportedKind::Opaque => format!(
            "{file}:{line}: include inside an opaque macro; bind the path with a direct \
             include_str! literal outside the macro. This guard reads include_str! only where \
             it can parse the enclosing macro's body as Rust expressions or statements."
        ),
    }
}

#[derive(Debug, PartialEq)]
struct UnsupportedSite {
    line: usize,
    gated: bool,
    kind: UnsupportedKind,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum UnsupportedKind {
    /// An `include_str!` whose argument is not exactly one string literal.
    Computed,
    /// An `include_str!` inside another macro's body that does not parse as Rust.
    Opaque,
}

#[derive(Debug, Default)]
struct IncludeScan {
    sites: Vec<(String, bool)>,
    unsupported: Vec<UnsupportedSite>,
}

fn include_sites(source: &str) -> Vec<(String, bool)> {
    scan_includes(source)
        .expect("fixture must parse as Rust")
        .sites
}

/// Parses `source` as a Rust file and returns each `include_str!` path, decoded as the compiler
/// decodes it, with whether it sits under a `#[cfg(test)]` attribute, plus every `include_str!`
/// whose argument is not exactly one string literal. Includes nested in the bodies of other
/// macros are found at token level.
fn scan_includes(source: &str) -> syn::Result<IncludeScan> {
    Ok(scan_file(&parse_source(source)?, false))
}

fn parse_source(source: &str) -> syn::Result<syn::File> {
    // The compiler reads CRLF line endings as LF before it tokenizes.
    syn::parse_file(&source.replace("\r\n", "\n"))
}

/// Scans a parsed file; `gated` is true when the whole file is test code.
fn scan_file(file: &syn::File, gated: bool) -> IncludeScan {
    let mut visitor = IncludeVisitor {
        gated: usize::from(gated),
        ..IncludeVisitor::default()
    };
    visitor.visit_file(file);
    visitor.scan
}

#[derive(Default)]
struct IncludeVisitor {
    gated: usize,
    scan: IncludeScan,
}

impl IncludeVisitor {
    fn within(&mut self, attrs: &[Attribute], visit: impl FnOnce(&mut Self)) {
        let gated = attrs.iter().any(is_cfg_test);
        self.gated += usize::from(gated);
        visit(self);
        self.gated -= usize::from(gated);
    }

    fn record(&mut self, line: usize, tokens: TokenStream) {
        let gated = self.gated > 0;
        match include_path(tokens) {
            Some(path) => self.scan.sites.push((path, gated)),
            None => self.scan.unsupported.push(UnsupportedSite {
                line,
                gated,
                kind: UnsupportedKind::Computed,
            }),
        }
    }

    /// Visits the body of a macro other than `include_str!` when it parses as comma-separated
    /// expressions (`assert!`, `vec![a, b]`, `format!`, `sqlx::query!`) or as statements
    /// (`vec![x; n]`, item-defining macros), under the current gate. Returns false when it
    /// parses as neither.
    fn visit_macro_body(&mut self, tokens: TokenStream) -> bool {
        if let Ok(exprs) = Punctuated::<Expr, Token![,]>::parse_terminated.parse2(tokens.clone()) {
            for expr in &exprs {
                self.visit_expr(expr);
            }
            return true;
        }
        if let Ok(stmts) = Block::parse_within.parse2(tokens) {
            for stmt in &stmts {
                self.visit_stmt(stmt);
            }
            return true;
        }
        false
    }

    /// Reports each `include_str ! (…)` (optionally `std::`, `core::`, or `::`-qualified) inside
    /// a macro body the guard could not parse. It is never loader evidence: the guard cannot
    /// tell whether the macro expands it.
    fn report_opaque(&mut self, tokens: TokenStream) {
        let tokens = tokens.into_iter().collect::<Vec<_>>();
        for (index, token) in tokens.iter().enumerate() {
            match token {
                TokenTree::Ident(ident) if ident.unraw() == "include_str" => {
                    let bang = matches!(
                        tokens.get(index + 1),
                        Some(TokenTree::Punct(punct)) if punct.as_char() == '!'
                    );
                    if bang
                        && matches!(tokens.get(index + 2), Some(TokenTree::Group(_)))
                        && builtin_qualifier(&tokens[..index])
                    {
                        self.scan.unsupported.push(UnsupportedSite {
                            line: ident.span().start().line,
                            gated: self.gated > 0,
                            kind: UnsupportedKind::Opaque,
                        });
                    }
                }
                TokenTree::Group(group) => self.report_opaque(group.stream()),
                _ => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for IncludeVisitor {
    fn visit_item(&mut self, node: &'ast Item) {
        self.within(item_attrs(node), |this| visit::visit_item(this, node));
    }

    fn visit_impl_item(&mut self, node: &'ast ImplItem) {
        let attrs: &[Attribute] = match node {
            ImplItem::Const(item) => &item.attrs,
            ImplItem::Fn(item) => &item.attrs,
            ImplItem::Type(item) => &item.attrs,
            ImplItem::Macro(item) => &item.attrs,
            _ => &[],
        };
        self.within(attrs, |this| visit::visit_impl_item(this, node));
    }

    fn visit_trait_item(&mut self, node: &'ast TraitItem) {
        let attrs: &[Attribute] = match node {
            TraitItem::Const(item) => &item.attrs,
            TraitItem::Fn(item) => &item.attrs,
            TraitItem::Type(item) => &item.attrs,
            TraitItem::Macro(item) => &item.attrs,
            _ => &[],
        };
        self.within(attrs, |this| visit::visit_trait_item(this, node));
    }

    fn visit_local(&mut self, node: &'ast Local) {
        self.within(&node.attrs, |this| visit::visit_local(this, node));
    }

    fn visit_stmt_macro(&mut self, node: &'ast StmtMacro) {
        self.within(&node.attrs, |this| visit::visit_stmt_macro(this, node));
    }

    fn visit_expr(&mut self, node: &'ast Expr) {
        self.within(expr_attrs(node), |this| visit::visit_expr(this, node));
    }

    fn visit_arm(&mut self, node: &'ast Arm) {
        self.within(&node.attrs, |this| visit::visit_arm(this, node));
    }

    fn visit_field(&mut self, node: &'ast Field) {
        self.within(&node.attrs, |this| visit::visit_field(this, node));
    }

    fn visit_field_value(&mut self, node: &'ast FieldValue) {
        self.within(&node.attrs, |this| visit::visit_field_value(this, node));
    }

    fn visit_variant(&mut self, node: &'ast Variant) {
        self.within(&node.attrs, |this| visit::visit_variant(this, node));
    }

    fn visit_macro(&mut self, node: &'ast Macro) {
        match macro_kind(&node.path) {
            MacroKind::Include => {
                let line = node
                    .path
                    .segments
                    .last()
                    .map_or(0, |segment| segment.ident.span().start().line);
                self.record(line, node.tokens.clone());
            }
            MacroKind::Skip => {}
            MacroKind::Expands => {
                if !self.visit_macro_body(node.tokens.clone()) {
                    self.report_opaque(node.tokens.clone());
                }
            }
            MacroKind::Opaque => self.report_opaque(node.tokens.clone()),
        }
    }

    fn visit_file(&mut self, node: &'ast syn::File) {
        // An inner #![cfg(test)] gates the whole file, whatever `mod` declared it.
        self.within(&node.attrs, |this| visit::visit_file(this, node));
    }

    fn visit_pat_type(&mut self, node: &'ast PatType) {
        self.within(&node.attrs, |this| visit::visit_pat_type(this, node));
    }

    fn visit_field_pat(&mut self, node: &'ast FieldPat) {
        self.within(&node.attrs, |this| visit::visit_field_pat(this, node));
    }

    fn visit_receiver(&mut self, node: &'ast Receiver) {
        self.within(&node.attrs, |this| visit::visit_receiver(this, node));
    }

    fn visit_bare_fn_arg(&mut self, node: &'ast BareFnArg) {
        self.within(&node.attrs, |this| visit::visit_bare_fn_arg(this, node));
    }

    fn visit_variadic(&mut self, node: &'ast Variadic) {
        self.within(&node.attrs, |this| visit::visit_variadic(this, node));
    }

    fn visit_type_param(&mut self, node: &'ast TypeParam) {
        self.within(&node.attrs, |this| visit::visit_type_param(this, node));
    }

    fn visit_lifetime_param(&mut self, node: &'ast LifetimeParam) {
        self.within(&node.attrs, |this| visit::visit_lifetime_param(this, node));
    }

    fn visit_const_param(&mut self, node: &'ast ConstParam) {
        self.within(&node.attrs, |this| visit::visit_const_param(this, node));
    }

    fn visit_foreign_item(&mut self, node: &'ast ForeignItem) {
        let attrs: &[Attribute] = match node {
            ForeignItem::Fn(item) => &item.attrs,
            ForeignItem::Static(item) => &item.attrs,
            ForeignItem::Type(item) => &item.attrs,
            ForeignItem::Macro(item) => &item.attrs,
            _ => &[],
        };
        self.within(attrs, |this| visit::visit_foreign_item(this, node));
    }
}

/// How the guard treats a macro invocation, decided from its full path (after `unraw`), never
/// from its last segment alone.
#[derive(Clone, Copy, Debug, PartialEq)]
enum MacroKind {
    /// `include_str!`: its literal argument is loader evidence.
    Include,
    /// The built-in `stringify!` or a `macro_rules!` definition: its tokens are never expanded
    /// as code here, so they are neither loader evidence nor a diagnostic.
    Skip,
    /// An audited macro known to expand its arguments as code; its body is visited when it parses
    /// as expressions or statements.
    Expands,
    /// Any other macro. An `include_str!` in its body is reported, never counted, because the
    /// guard cannot tell whether or how the macro transcribes it.
    Opaque,
}

/// Built-in macros audited to expand their arguments as code. `concat!` expands a nested
/// `include_str!` eagerly; `crates/project/src/builders/name_current/query.rs` relies on it.
const EXPANDING_BUILTINS: &[&str] = &[
    "concat",
    "assert",
    "assert_eq",
    "assert_ne",
    "debug_assert",
    "debug_assert_eq",
    "debug_assert_ne",
    "format",
    "format_args",
    "write",
    "writeln",
    "print",
    "println",
    "eprint",
    "eprintln",
    "panic",
    "unreachable",
    "todo",
    "vec",
    "matches",
    "dbg",
];

/// Crate-qualified macros audited to expand their arguments as code.
const EXPANDING_CRATE_MACROS: &[(&str, &[&str])] = &[
    ("sqlx", &["query", "query_as", "query_scalar"]),
    ("tracing", &["trace", "debug", "info", "warn", "error"]),
    ("tokio", &["select"]),
    ("serde_json", &["json"]),
    ("anyhow", &["anyhow", "bail", "ensure"]),
];

fn macro_kind(path: &syn::Path) -> MacroKind {
    let segments = path
        .segments
        .iter()
        .map(|segment| segment.ident.unraw().to_string())
        .collect::<Vec<_>>();
    // A built-in: bare, or `std::`/`core::` with an optional leading `::`.
    let builtin = match segments.as_slice() {
        [name] if path.leading_colon.is_none() => Some(name.as_str()),
        [krate, name] if krate == "std" || krate == "core" => Some(name.as_str()),
        _ => None,
    };
    match builtin {
        Some("include_str") => return MacroKind::Include,
        Some("stringify") => return MacroKind::Skip,
        Some(name) if EXPANDING_BUILTINS.contains(&name) => return MacroKind::Expands,
        _ => {}
    }
    match segments.as_slice() {
        [name] if name == "macro_rules" && path.leading_colon.is_none() => MacroKind::Skip,
        [krate, name]
            if EXPANDING_CRATE_MACROS
                .iter()
                .any(|(known, names)| krate == known && names.contains(&name.as_str())) =>
        {
            MacroKind::Expands
        }
        _ => MacroKind::Opaque,
    }
}

/// Decodes a macro body that is exactly one string literal, optionally followed by a comma.
fn include_path(tokens: TokenStream) -> Option<String> {
    let parser = |input: ParseStream| {
        let path: LitStr = input.parse()?;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
        Ok(path)
    };
    parser.parse2(tokens).ok().map(|path| path.value())
}

/// True when the tokens before an `include_str` ident leave it unqualified or qualified by
/// `std::`/`core::` (with an optional leading `::`).
fn builtin_qualifier(before: &[TokenTree]) -> bool {
    let is_colon = |token: Option<&TokenTree>| matches!(token, Some(TokenTree::Punct(punct)) if punct.as_char() == ':');
    let count = before.len();
    if !(count >= 2 && is_colon(before.get(count - 1)) && is_colon(before.get(count - 2))) {
        return true;
    }
    matches!(
        count.checked_sub(3).and_then(|index| before.get(index)),
        Some(TokenTree::Ident(krate)) if krate.unraw() == "std" || krate.unraw() == "core"
    )
}

fn is_cfg_test(attr: &Attribute) -> bool {
    match &attr.meta {
        Meta::List(list) => list.path.is_ident("cfg") && list.tokens.to_string() == "test",
        _ => false,
    }
}

fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        _ => &[],
    }
}

fn expr_attrs(expr: &Expr) -> &[Attribute] {
    match expr {
        Expr::Array(expr) => &expr.attrs,
        Expr::Assign(expr) => &expr.attrs,
        Expr::Async(expr) => &expr.attrs,
        Expr::Await(expr) => &expr.attrs,
        Expr::Binary(expr) => &expr.attrs,
        Expr::Block(expr) => &expr.attrs,
        Expr::Break(expr) => &expr.attrs,
        Expr::Call(expr) => &expr.attrs,
        Expr::Cast(expr) => &expr.attrs,
        Expr::Closure(expr) => &expr.attrs,
        Expr::Const(expr) => &expr.attrs,
        Expr::Continue(expr) => &expr.attrs,
        Expr::Field(expr) => &expr.attrs,
        Expr::ForLoop(expr) => &expr.attrs,
        Expr::Group(expr) => &expr.attrs,
        Expr::If(expr) => &expr.attrs,
        Expr::Index(expr) => &expr.attrs,
        Expr::Infer(expr) => &expr.attrs,
        Expr::Let(expr) => &expr.attrs,
        Expr::Lit(expr) => &expr.attrs,
        Expr::Loop(expr) => &expr.attrs,
        Expr::Macro(expr) => &expr.attrs,
        Expr::Match(expr) => &expr.attrs,
        Expr::MethodCall(expr) => &expr.attrs,
        Expr::Paren(expr) => &expr.attrs,
        Expr::Path(expr) => &expr.attrs,
        Expr::Range(expr) => &expr.attrs,
        Expr::RawAddr(expr) => &expr.attrs,
        Expr::Reference(expr) => &expr.attrs,
        Expr::Repeat(expr) => &expr.attrs,
        Expr::Return(expr) => &expr.attrs,
        Expr::Struct(expr) => &expr.attrs,
        Expr::Try(expr) => &expr.attrs,
        Expr::TryBlock(expr) => &expr.attrs,
        Expr::Tuple(expr) => &expr.attrs,
        Expr::Unary(expr) => &expr.attrs,
        Expr::Unsafe(expr) => &expr.attrs,
        Expr::While(expr) => &expr.attrs,
        Expr::Yield(expr) => &expr.attrs,
        _ => &[],
    }
}

fn collect_files(directory: &Path, extension: &str, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    let mut entries = entries
        .collect::<Result<Vec<_>, _>>()
        .expect("directory entries must be readable");
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, extension, files);
        } else if path.extension().is_some_and(|found| found == extension) {
            files.push(path);
        }
    }
}

fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir if normalized.file_name().is_some_and(|name| name != "..") => {
                normalized.pop();
            }
            Component::CurDir => {}
            other => normalized.push(other),
        }
    }
    normalized
}

fn relative_key(workspace_root: &Path, path: &Path) -> String {
    let root = normalize(workspace_root);
    normalize(path)
        .strip_prefix(&root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}
