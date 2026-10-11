//! The interpret closure: every `crate::`, `self::`, `super::` or workspace-crate path that an
//! interpret- or both-class file names must resolve to an interpret- or both-class file. A path
//! that reaches a project-class file, or a file neither hash covers, could change what Interpret
//! persists while leaving the interpreter hash alone.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use syn::{Attribute, Item, ItemUse, UseTree, visit::Visit};

use super::{SampleTree, workspace_root};
use crate::{
    classes::Class,
    compute::{classed_inputs, module_keys},
};

/// Items of a file that interpret-side files name without them deciding an interpreted row.
struct Exemption {
    from: &'static str,
    target: &'static str,
    items: &'static [&'static str],
    reason: &'static str,
}

const EXEMPTIONS: &[Exemption] = &[
    Exemption {
        from: "crates/interpret/src/",
        target: "crates/interpret/src/error.rs",
        items: &["InterpretError", "ErrorKind", "Result"],
        reason: "error plumbing: an error aborts the write and never shapes a row",
    },
    Exemption {
        from: "crates/interpret/src/write/identity_names.rs",
        target: "crates/interpret/src/lib.rs",
        items: &["NORMALIZATION_STATE_REPAIR_REASON"],
        reason: "the repair instruction an aborted write's error message carries",
    },
    Exemption {
        from: "crates/storage/src/families/name/rendered.rs",
        target: "crates/storage/src/name_current/row.rs",
        items: &["NameCurrentRow"],
        reason: "only rendered_name_sql names it, and only Project calls that function",
    },
];

type Modules = BTreeMap<(String, Vec<String>), String>;

struct Closure {
    root: std::path::PathBuf,
    classes: BTreeMap<String, Class>,
    modules: Modules,
    /// Each file with the target root and module path that hold it, the shortest first.
    homes: BTreeMap<String, (String, Vec<String>)>,
    /// Each workspace crate's library name with its library target root.
    crates: BTreeMap<String, String>,
}

fn violations(root: &Path) -> Vec<String> {
    let classes = classed_inputs(root)
        .expect("tree must hash")
        .into_iter()
        .filter_map(|input| Some((input.key.strip_prefix("source:")?.to_owned(), input.class)))
        .collect::<BTreeMap<_, _>>();
    let modules = module_keys(root).expect("tree must walk");
    let mut homes = BTreeMap::<String, (String, Vec<String>)>::new();
    for ((target, path), file) in &modules {
        let home = homes
            .entry(file.clone())
            .or_insert_with(|| (target.clone(), path.clone()));
        if path.len() < home.1.len() {
            *home = (target.clone(), path.clone());
        }
    }
    let crates = modules
        .keys()
        .filter(|(target, path)| path.is_empty() && target.ends_with("/src/lib.rs"))
        .filter_map(|(target, _)| {
            let name = target
                .strip_prefix("crates/")?
                .strip_suffix("/src/lib.rs")?;
            Some((
                format!("bigname_{}", name.replace('-', "_")),
                target.clone(),
            ))
        })
        .collect();
    let closure = Closure {
        root: root.to_owned(),
        classes,
        modules,
        homes,
        crates,
    };
    let mut violations = BTreeSet::new();
    for (file, class) in &closure.classes {
        if *class == Class::Project || !file.ends_with(".rs") {
            continue;
        }
        let Some(home) = closure.homes.get(file) else {
            continue;
        };
        let source = fs::read_to_string(root.join(file)).expect("hashed file must read");
        let parsed = syn::parse_file(&source).expect("hashed file must parse");
        let mut paths = Paths::default();
        paths.visit_file(&parsed);
        for path in paths.found {
            let Some((target, item)) = closure.resolve(home, &path, 0) else {
                continue;
            };
            if closure
                .classes
                .get(&target)
                .is_some_and(|class| *class != Class::Project)
                || EXEMPTIONS.iter().any(|exemption| {
                    file.starts_with(exemption.from)
                        && target == exemption.target
                        && item
                            .as_deref()
                            .is_some_and(|item| exemption.items.contains(&item))
                })
            {
                continue;
            }
            let found = closure
                .classes
                .get(&target)
                .map_or("in neither hash", |class| class.as_str());
            violations.insert(format!(
                "{file} ({}) names {} in {target}, which is {found}",
                class.as_str(),
                path.join("::")
            ));
        }
    }
    violations.into_iter().collect()
}

impl Closure {
    /// The file holding `path` as named from the module `home`, following `use` re-exports, with
    /// the item named in it when the path goes past a module.
    fn resolve(
        &self,
        home: &(String, Vec<String>),
        path: &[String],
        depth: usize,
    ) -> Option<(String, Option<String>)> {
        let (target, mut module) = match path.first()?.as_str() {
            "crate" => (home.0.clone(), Vec::new()),
            "self" => home.clone(),
            "super" => {
                let mut module = home.1.clone();
                module.pop()?;
                (home.0.clone(), module)
            }
            name => (self.crates.get(name)?.clone(), Vec::new()),
        };
        let mut rest = &path[1..];
        while rest.first().is_some_and(|segment| segment == "super") {
            module.pop()?;
            rest = &rest[1..];
        }
        let mut depth_found = 0;
        for length in (0..=rest.len()).rev() {
            let mut candidate = module.clone();
            candidate.extend(rest[..length].iter().cloned());
            if self.modules.contains_key(&(target.clone(), candidate)) {
                depth_found = length;
                break;
            }
        }
        module.extend(rest[..depth_found].iter().cloned());
        let file = self.modules.get(&(target.clone(), module.clone()))?.clone();
        let Some(item) = rest.get(depth_found) else {
            return Some((file, None));
        };
        if depth < 8
            && let Some(reexport) = self.reexport(&file, item)
        {
            let mut followed = reexport;
            followed.extend(rest[depth_found + 1..].iter().cloned());
            if let Some(found) = self.resolve(&(target, module), &followed, depth + 1) {
                return Some(found);
            }
        }
        Some((file, Some(item.clone())))
    }

    /// The path a top-level `use` in `file` brings `item` in from, made relative to that module.
    fn reexport(&self, file: &str, item: &str) -> Option<Vec<String>> {
        let source = fs::read_to_string(self.root.join(file)).ok()?;
        let parsed = syn::parse_file(&source).ok()?;
        for top in &parsed.items {
            let Item::Use(declaration) = top else {
                continue;
            };
            if is_test_only(&declaration.attrs) {
                continue;
            }
            for (path, name) in flatten(&declaration.tree) {
                if name.as_deref() == Some(item) {
                    return Some(self.anchor(path));
                }
            }
        }
        None
    }

    /// A `use` path starting at a local name is relative to the module it sits in.
    fn anchor(&self, path: Vec<String>) -> Vec<String> {
        match path.first().map(String::as_str) {
            Some("crate" | "self" | "super") => path,
            Some(name) if self.crates.contains_key(name) => path,
            _ => std::iter::once("self".to_owned()).chain(path).collect(),
        }
    }
}

/// Every path a file names that starts at `crate`, `self`, `super` or a crate, outside test-only
/// items. A `use` contributes each leaf path, a glob its module. A macro call contributes the
/// paths written in its tokens, such as an argument of `format!`.
#[derive(Default)]
struct Paths {
    found: Vec<Vec<String>>,
}

impl<'a> Visit<'a> for Paths {
    fn visit_item(&mut self, item: &'a Item) {
        let attributes = match item {
            Item::Fn(item) => &item.attrs,
            Item::Mod(item) => &item.attrs,
            Item::Impl(item) => &item.attrs,
            Item::Use(item) => &item.attrs,
            Item::Const(item) => &item.attrs,
            Item::Static(item) => &item.attrs,
            Item::Struct(item) => &item.attrs,
            Item::Enum(item) => &item.attrs,
            Item::Trait(item) => &item.attrs,
            _ => return syn::visit::visit_item(self, item),
        };
        if !is_test_only(attributes) {
            syn::visit::visit_item(self, item);
        }
    }

    fn visit_visibility(&mut self, _: &'a syn::Visibility) {}

    fn visit_macro(&mut self, invocation: &'a syn::Macro) {
        syn::visit::visit_macro(self, invocation);
        token_paths(invocation.tokens.clone(), &mut self.found);
    }

    fn visit_item_use(&mut self, item: &'a ItemUse) {
        self.found
            .extend(flatten(&item.tree).into_iter().map(|(path, _)| path));
    }

    fn visit_path(&mut self, path: &'a syn::Path) {
        if path.leading_colon.is_none() {
            self.found.push(
                path.segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect(),
            );
        }
        syn::visit::visit_path(self, path);
    }
}

/// Each run `head::segment::…` in macro tokens whose head can start a path the closure resolves:
/// `crate`, `self`, `super` or a workspace crate. A head that follows `::` is part of a longer run.
fn token_paths(tokens: proc_macro2::TokenStream, found: &mut Vec<Vec<String>>) {
    use proc_macro2::{Spacing, TokenTree};
    let trees = tokens.into_iter().collect::<Vec<_>>();
    let path_separator = |index: usize| {
        matches!(
            (trees.get(index), trees.get(index + 1)),
            (Some(TokenTree::Punct(first)), Some(TokenTree::Punct(second)))
                if first.as_char() == ':' && first.spacing() == Spacing::Joint
                    && second.as_char() == ':'
        )
    };
    let mut index = 0;
    while index < trees.len() {
        match &trees[index] {
            TokenTree::Group(group) => token_paths(group.stream(), found),
            TokenTree::Ident(head) => {
                let head = head.to_string();
                let follows_separator = index >= 2 && path_separator(index - 2);
                if !follows_separator
                    && (["crate", "self", "super"].contains(&head.as_str())
                        || head.starts_with("bigname_"))
                {
                    let mut path = vec![head];
                    while path_separator(index + 1)
                        && let Some(TokenTree::Ident(segment)) = trees.get(index + 3)
                    {
                        path.push(segment.to_string());
                        index += 3;
                    }
                    if path.len() > 1 {
                        found.push(path);
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }
}

/// Each leaf of a `use` tree with the name it binds. A glob binds none, and `self` binds its
/// module's name.
fn flatten(tree: &UseTree) -> Vec<(Vec<String>, Option<String>)> {
    fn walk(
        tree: &UseTree,
        prefix: &mut Vec<String>,
        out: &mut Vec<(Vec<String>, Option<String>)>,
    ) {
        match tree {
            UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                walk(&path.tree, prefix, out);
                prefix.pop();
            }
            UseTree::Name(name) if name.ident == "self" => {
                out.push((prefix.clone(), prefix.last().cloned()));
            }
            UseTree::Name(name) => {
                let mut path = prefix.clone();
                path.push(name.ident.to_string());
                out.push((path, Some(name.ident.to_string())));
            }
            UseTree::Rename(rename) => {
                let mut path = prefix.clone();
                path.push(rename.ident.to_string());
                out.push((path, Some(rename.rename.to_string())));
            }
            UseTree::Glob(_) => out.push((prefix.clone(), None)),
            UseTree::Group(group) => {
                for tree in &group.items {
                    walk(tree, prefix, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(tree, &mut Vec::new(), &mut out);
    out
}

fn is_test_only(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("test")
            || (attribute.path().is_ident("cfg")
                && attribute
                    .parse_args::<syn::Ident>()
                    .is_ok_and(|ident| ident == "test"))
    })
}

#[test]
fn checked_in_interpret_side_files_name_only_interpret_side_files() {
    for exemption in EXEMPTIONS {
        assert!(!exemption.reason.trim().is_empty());
    }
    let found = violations(&workspace_root());
    assert!(
        found.is_empty(),
        "interpret-side files reach files outside the interpreter hash:\n{}",
        found.join("\n")
    );
}

#[test]
fn a_both_class_file_naming_a_project_composition_file_fails() {
    let tree = SampleTree::new();
    tree.write(
        "crates/storage/src/lib.rs",
        "pub mod identity_search;\npub mod families;\n",
    );
    tree.write(
        "crates/storage/src/identity_search.rs",
        "pub fn prepare() -> String { crate::families::name::summary::sql() }\n",
    );
    tree.write("crates/storage/src/families/mod.rs", "pub mod name;\n");
    tree.write(
        "crates/storage/src/families/name/mod.rs",
        "pub mod summary;\npub mod rendered;\n",
    );
    tree.write(
        "crates/storage/src/families/name/summary.rs",
        "pub fn sql() -> String { String::new() }\n",
    );
    let found = violations(tree.path());
    assert!(
        found.iter().any(|violation| violation.starts_with(
            "crates/storage/src/identity_search.rs (both) names crate::families::name::summary::sql \
             in crates/storage/src/families/name/summary.rs, which is project"
        )),
        "{found:?}"
    );
}

#[test]
fn a_both_class_file_naming_a_project_file_inside_a_macro_fails() {
    let tree = SampleTree::new();
    tree.write(
        "crates/storage/src/lib.rs",
        "pub mod identity_search;\npub mod families;\n",
    );
    tree.write(
        "crates/storage/src/identity_search.rs",
        "pub fn prepare() -> String { format!(\"{x}\", x = \
         crate::families::name::summary::sql()) }\n",
    );
    tree.write("crates/storage/src/families/mod.rs", "pub mod name;\n");
    tree.write(
        "crates/storage/src/families/name/mod.rs",
        "pub mod summary;\npub mod rendered;\n",
    );
    tree.write(
        "crates/storage/src/families/name/summary.rs",
        "pub fn sql() -> String { String::new() }\n",
    );
    let found = violations(tree.path());
    assert!(
        found.iter().any(|violation| violation.starts_with(
            "crates/storage/src/identity_search.rs (both) names crate::families::name::summary::sql \
             in crates/storage/src/families/name/summary.rs, which is project"
        )),
        "{found:?}"
    );
}

#[test]
fn a_both_class_file_reexporting_an_unhashed_file_fails() {
    let tree = SampleTree::new();
    tree.write("crates/domain/src/lib.rs", "pub mod unlisted;\n");
    tree.write("crates/domain/src/unlisted.rs", "pub enum Feature { A }\n");
    tree.write(
        "crates/manifests/src/lib.rs",
        "pub use bigname_domain::unlisted::Feature;\n",
    );
    let found = violations(tree.path());
    assert!(
        found.iter().any(|violation| violation.starts_with(
            "crates/manifests/src/lib.rs (both) names bigname_domain::unlisted::Feature in \
             crates/domain/src/unlisted.rs, which is in neither hash"
        )),
        "{found:?}"
    );
}
