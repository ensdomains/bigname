//! Syntax the module walk reads: module items, `macro_rules!` bodies, `include!` paths and the
//! cfg attributes on a declaration.

use std::{io, path::Path};

use syn::{
    Attribute, Block, Expr, ExprLit, Item, ItemMacro, ItemMod, ItemUse, Lit, LitStr, Macro, Meta,
    Token, UseTree,
    buffer::{Cursor, TokenBuffer},
    ext::IdentExt,
    parse::ParseStream,
    punctuated::Punctuated,
    visit::{self, Visit},
};

/// What one scope declares: its module items, each with whether it sits in a block, its
/// `macro_rules!` definitions, every other macro invocation, and its `use` items. Items nested in
/// blocks, such as a function body, count. The items of a nested module do not, as it is a scope
/// of its own.
#[derive(Default)]
pub(super) struct Found<'a> {
    pub(super) modules: Vec<(&'a ItemMod, bool)>,
    pub(super) macro_rules: Vec<(String, &'a Macro)>,
    pub(super) macros: Vec<&'a Macro>,
    uses: Vec<&'a ItemUse>,
}

#[derive(Default)]
pub(super) struct Collector<'a> {
    pub(super) found: Found<'a>,
    blocks: usize,
}

impl<'a> Visit<'a> for Collector<'a> {
    fn visit_item_mod(&mut self, declaration: &'a ItemMod) {
        self.found.modules.push((declaration, self.blocks > 0));
    }
    fn visit_item_macro(&mut self, item: &'a ItemMacro) {
        match &item.ident {
            Some(name) if named(&item.mac.path, "macro_rules") => {
                self.found.macro_rules.push((name.to_string(), &item.mac));
            }
            _ => self.found.macros.push(&item.mac),
        }
    }
    fn visit_macro(&mut self, invocation: &'a Macro) {
        self.found.macros.push(invocation);
    }
    fn visit_item_use(&mut self, item: &'a ItemUse) {
        self.found.uses.push(item);
    }
    fn visit_block(&mut self, block: &'a Block) {
        self.blocks += 1;
        visit::visit_block(self, block);
        self.blocks -= 1;
    }
}

pub(super) fn scope_items(items: &[Item]) -> Found<'_> {
    let mut collector = Collector::default();
    for item in items {
        collector.visit_item(item);
    }
    collector.found
}

/// The macros that read another file at a literal path: `include!` text is code the walk follows,
/// and `include_str!` or `include_bytes!` data only pins its file in the hash.
pub(super) const INCLUDE: &str = "include";
const INCLUDE_MACROS: [&str; 3] = [INCLUDE, "include_str", "include_bytes"];

/// Refuses each shape in one scope the walk cannot follow: a `macro_rules!` body or macro
/// invocation that may declare a module, a body that calls or names an include macro, an
/// invocation that names one without calling it, and a `use` that may alias one.
pub(super) fn refuse_unfollowable(file: &Path, found: &Found<'_>) -> io::Result<()> {
    for (name, definition) in &found.macro_rules {
        refuse_module_macro(file, name, definition)?;
    }
    for invocation in &found.macros {
        refuse_module_invocation(file, invocation)?;
    }
    for item in &found.uses {
        if let Some(include) = imported_include(&item.tree) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "use of {include} in {} may alias an include macro, which the content hash \
                     cannot follow",
                    file.display()
                ),
            ));
        }
    }
    Ok(())
}

/// The first include macro a `use` tree imports, renamed or not.
fn imported_include(tree: &UseTree) -> Option<&'static str> {
    match tree {
        UseTree::Path(path) => imported_include(&path.tree),
        UseTree::Name(name) => include_macro(&name.ident.unraw().to_string()),
        UseTree::Rename(rename) => include_macro(&rename.ident.unraw().to_string()),
        UseTree::Group(group) => group.items.iter().find_map(imported_include),
        UseTree::Glob(_) => None,
    }
}

/// Refuses a `macro_rules!` whose body holds the `mod` keyword or one of the include macros' names:
/// the walk cannot see where the macro is expanded, so it cannot prove which files it reaches.
fn refuse_module_macro(file: &Path, name: &str, definition: &Macro) -> io::Result<()> {
    let shape = if holds_mod(definition) {
        "holds `mod`, so it may declare a module".to_owned()
    } else if let Some(include) = include_token(definition, true) {
        format!("calls {include}!")
    } else if let Some(include) = include_token(definition, false) {
        format!("names {include}, so it may call it")
    } else {
        return Ok(());
    };
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "macro_rules! {name} in {} {shape}, which the content hash cannot follow",
            file.display()
        ),
    ))
}

/// Refuses a macro invocation whose tokens hold the `mod` keyword, or an include macro's name
/// other than as a direct `name!` call the walk follows: what the macro emits from them resolves
/// where it expands, which the walk cannot see.
fn refuse_module_invocation(file: &Path, invocation: &Macro) -> io::Result<()> {
    let shape = if holds_mod(invocation) {
        "holds `mod`, so it may declare a module".to_owned()
    } else if let Some(include) = include_token(invocation, false) {
        format!("passes {include} without calling it, so it may call it")
    } else {
        return Ok(());
    };
    let name = invocation
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>()
        .join("::");
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{name}! in {} {shape}, which the content hash cannot follow",
            file.display()
        ),
    ))
}

/// Whether the macro's tokens hold the `mod` keyword at any position. A declaration can be
/// assembled from a macro's arguments and its body, as in `emit!(mod, shared)`, so any `mod` token
/// counts, at the cost of refusing some macros that declare nothing.
fn holds_mod(invocation: &Macro) -> bool {
    let buffer = TokenBuffer::new2(invocation.tokens.clone());
    let mut holds = false;
    each_position(buffer.begin(), &mut |cursor| {
        holds |= cursor.ident().is_some_and(|(keyword, _)| keyword == "mod");
    });
    holds
}

/// The first include macro the tokens name as a `name!` call when `called`, or name any other way
/// when not, as in `emit!(include_str, "table.txt")`.
fn include_token(invocation: &Macro, called: bool) -> Option<&'static str> {
    let buffer = TokenBuffer::new2(invocation.tokens.clone());
    let mut found = None;
    each_position(buffer.begin(), &mut |cursor| {
        if found.is_none()
            && let Some((name, next)) = cursor.ident()
            && next.punct().is_some_and(|(bang, _)| bang.as_char() == '!') == called
        {
            found = include_macro(&name.unraw().to_string());
        }
    });
    found
}

fn include_macro(name: &str) -> Option<&'static str> {
    INCLUDE_MACROS.into_iter().find(|include| *include == name)
}

/// The include macro calls in an invocation, each with its path: the invocation itself, or calls
/// nested in its arguments. A path that is not one string literal is `Err`.
pub(super) fn include_paths(invocation: &Macro) -> Vec<(&'static str, Result<String, ()>)> {
    let own = invocation
        .path
        .segments
        .last()
        .and_then(|segment| include_macro(&segment.ident.unraw().to_string()));
    if let Some(include) = own {
        let path = invocation
            .parse_body_with(literal_path)
            .map(|path| path.value())
            .map_err(|_| ());
        return vec![(include, path)];
    }
    let buffer = TokenBuffer::new2(invocation.tokens.clone());
    let mut paths = Vec::new();
    each_position(buffer.begin(), &mut |cursor| {
        let Some((name, next)) = cursor.ident() else {
            return;
        };
        let Some((bang, next)) = next.punct() else {
            return;
        };
        let Some(include) = include_macro(&name.unraw().to_string()) else {
            return;
        };
        if bang.as_char() != '!' {
            return;
        }
        let Some((inside, ..)) = next.any_group() else {
            return;
        };
        let path = match inside.literal() {
            Some((literal, rest)) if trailing_comma_at_most(rest) => match Lit::new(literal) {
                Lit::Str(path) => Ok(path.value()),
                _ => Err(()),
            },
            _ => Err(()),
        };
        paths.push((include, path));
    });
    paths
}

/// One string literal with an optional trailing comma, as rustc accepts in an include macro.
fn literal_path(input: ParseStream<'_>) -> syn::Result<LitStr> {
    let path = input.parse()?;
    input.parse::<Option<Token![,]>>()?;
    Ok(path)
}

fn trailing_comma_at_most(rest: Cursor<'_>) -> bool {
    match rest.punct() {
        Some((comma, after)) => comma.as_char() == ',' && after.eof(),
        None => rest.eof(),
    }
}

/// Calls `visit` at every token position, inside groups too.
fn each_position<'a>(mut cursor: Cursor<'a>, visit: &mut dyn FnMut(Cursor<'a>)) {
    while !cursor.eof() {
        visit(cursor);
        if let Some((inside, _, _, after)) = cursor.any_group() {
            each_position(inside, visit);
            cursor = after;
        } else if let Some((_, after)) = cursor.token_tree() {
            cursor = after;
        } else {
            break;
        }
    }
}

/// Whether `path` is the single identifier `name`, in its plain or raw (`r#name`) spelling.
pub(super) fn named(path: &syn::Path, name: &str) -> bool {
    path.get_ident().is_some_and(|ident| ident.unraw() == name)
}

pub(super) fn is_cfg_test(attribute: &Attribute) -> bool {
    match &attribute.meta {
        Meta::List(list) => named(&list.path, "cfg") && list.tokens.to_string() == "test",
        _ => false,
    }
}

pub(super) fn path_value(meta: &Meta) -> Option<String> {
    match meta {
        Meta::NameValue(value) if named(&value.path, "path") => match &value.value {
            Expr::Lit(ExprLit {
                lit: Lit::Str(path),
                ..
            }) => Some(path.value()),
            _ => None,
        },
        _ => None,
    }
}

/// The paths a `#[cfg_attr(predicate, path = "…")]` selects, at any depth of nested `cfg_attr`,
/// each with whether a predicate on its chain is exactly `test`.
pub(super) fn cfg_attr_path(attribute: &Attribute) -> Vec<(bool, String)> {
    let mut paths = Vec::new();
    cfg_attr_paths(&attribute.meta, false, &mut paths);
    paths
}

fn cfg_attr_paths(meta: &Meta, test: bool, paths: &mut Vec<(bool, String)>) {
    let Meta::List(list) = meta else {
        return;
    };
    if !named(&list.path, "cfg_attr") {
        return;
    }
    let Ok(metas) = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) else {
        return;
    };
    let mut metas = metas.iter();
    let Some(predicate) = metas.next() else {
        return;
    };
    let test = test || matches!(predicate, Meta::Path(path) if named(path, "test"));
    for meta in metas {
        match path_value(meta) {
            Some(path) => paths.push((test, path)),
            None => cfg_attr_paths(meta, test, paths),
        }
    }
}
