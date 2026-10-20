//! Parsing the `topics!` tree. Errors are collected entry by entry: a wrong
//! entry is skipped up to the next `,` and its siblings are still parsed.

use std::collections::HashSet;

use proc_macro2::{TokenStream, TokenTree};
use syn::ext::IdentExt;
use syn::parse::discouraged::Speculative;
use syn::parse::{ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Error, Ident, Path, Result, Token, Type, Variant, braced, bracketed, parenthesized,
    token,
};

pub struct Input {
    /// Path of the nattes crate: `$crate`.
    pub krate: TokenTree,
    /// Attributes other than docs, applied to every `Topic` enum.
    pub attrs: Vec<Attribute>,
    /// Always a `Kind::Level`.
    pub root: Entry,
}

/// The group enum `[#![derive(Debug)] A, B(T)]`: one queue group per value.
pub struct GroupEnum {
    /// Inner attributes, applied to the enum.
    pub attrs: Vec<Attribute>,
    pub variants: Punctuated<Variant, Token![,]>,
}

/// One topic: `name(Param) [A, B]` and what follows.
pub struct Entry {
    pub docs: Vec<Attribute>,
    pub name: Ident,
    pub param: Option<Type>,
    pub groups: Option<GroupEnum>,
    pub kind: Kind,
}

pub enum Kind {
    /// `name { … }`, and `name.rest` for `name { rest }`
    Level(Vec<Entry>),
    /// `name: Message`
    Leaf(Type),
    /// `name = module`
    Mount(Path),
}

pub fn parse(tokens: TokenStream, errors: &mut Vec<Error>) -> Result<Input> {
    let parser = |input: ParseStream| {
        let krate = input.parse()?;
        let (docs, attrs) = Attribute::parse_outer(input)?
            .into_iter()
            .partition(|attr| attr.path().is_ident("doc"));
        let name = input.parse()?;
        let groups = group_enum(input)?;
        let content;
        braced!(content in input);
        let root = Entry {
            docs,
            name,
            param: None,
            groups,
            kind: Kind::Level(entries(&content, errors)),
        };
        Ok(Input { krate, attrs, root })
    };
    parser.parse2(tokens)
}

fn group_enum(input: ParseStream) -> Result<Option<GroupEnum>> {
    if !input.peek(token::Bracket) {
        return Ok(None);
    }
    let content;
    bracketed!(content in input);
    Ok(Some(GroupEnum {
        attrs: Attribute::parse_inner(&content)?,
        variants: Punctuated::parse_terminated(&content)?,
    }))
}

/// The entries of one level, up to the end of `input`.
fn entries(input: ParseStream, errors: &mut Vec<Error>) -> Vec<Entry> {
    let mut out = Vec::new();
    let mut names = HashSet::new();
    while !input.is_empty() {
        let fork = input.fork();
        match entry(&fork, errors) {
            Ok(entry) => {
                input.advance_to(&fork);
                if names.insert(entry.name.unraw().to_string()) {
                    out.push(entry);
                } else {
                    let msg = format!("`{}` is already a topic of this level", entry.name);
                    errors.push(Error::new(entry.name.span(), msg));
                }
                if !input.is_empty()
                    && let Err(error) = input.parse::<Token![,]>()
                {
                    errors.push(error);
                    skip_entry(input);
                }
            }
            Err(error) => {
                errors.push(error);
                skip_entry(input);
            }
        }
    }
    out
}

fn entry(input: ParseStream, errors: &mut Vec<Error>) -> Result<Entry> {
    let mut docs = Vec::new();
    for attr in Attribute::parse_outer(input)? {
        if attr.path().is_ident("doc") {
            docs.push(attr);
        } else {
            errors.push(Error::new_spanned(attr, "only doc comments go on a topic"));
        }
    }
    topic(input, docs, errors)
}

/// A topic after its docs, which go on the last topic of a chain `a.b.c`.
fn topic(input: ParseStream, docs: Vec<Attribute>, errors: &mut Vec<Error>) -> Result<Entry> {
    let name: Ident = input.parse()?;
    let param = if input.peek(token::Paren) {
        let content;
        parenthesized!(content in input);
        let param = content
            .parse()
            .map_err(|e| Error::new(e.span(), "expected the parameter type"))?;
        if !content.is_empty() {
            return Err(content.error("a topic has one parameter: expected `)`"));
        }
        Some(param)
    } else {
        None
    };
    let groups = group_enum(input)?;
    let (docs, kind) = if input.peek(Token![.]) {
        input.parse::<Token![.]>()?;
        (Vec::new(), Kind::Level(vec![topic(input, docs, errors)?]))
    } else if input.peek(token::Brace) {
        let content;
        braced!(content in input);
        (docs, Kind::Level(entries(&content, errors)))
    } else if input.peek(Token![:]) {
        input.parse::<Token![:]>()?;
        let msg = input
            .parse()
            .map_err(|e| Error::new(e.span(), "expected the message type"))?;
        (docs, Kind::Leaf(msg))
    } else if input.peek(Token![=]) && param.is_none() && groups.is_none() {
        input.parse::<Token![=]>()?;
        (docs, Kind::Mount(Path::parse_mod_style(input)?))
    } else {
        return Err(input.error(
            "expected `{ … }`, `: Message`, `.name` or, without parameter and groups, `= module`",
        ));
    };
    Ok(Entry {
        docs,
        name,
        param,
        groups,
        kind,
    })
}

/// Skips past the next `,` that isn't inside a generic argument list.
fn skip_entry(input: ParseStream) {
    let _ = input.step(|cursor| {
        let mut rest = *cursor;
        let mut depth = 0;
        let mut arrow = false;
        while let Some((tt, next)) = rest.token_tree() {
            if let TokenTree::Punct(punct) = &tt {
                match punct.as_char() {
                    ',' if depth <= 0 => return Ok(((), next)),
                    '<' => depth += 1,
                    '>' if !arrow => depth -= 1,
                    _ => {}
                }
            }
            arrow = matches!(&tt, TokenTree::Punct(p) if p.as_char() == '-');
            rest = next;
        }
        Ok(((), rest))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn parse_tree(body: TokenStream) -> (Vec<Entry>, Vec<String>) {
        let mut errors = Vec::new();
        let input = parse(quote!(krate ev { #body }), &mut errors).unwrap();
        let Kind::Level(entries) = input.root.kind else {
            unreachable!()
        };
        (entries, errors.iter().map(ToString::to_string).collect())
    }

    fn names(entries: &[Entry]) -> Vec<String> {
        entries.iter().map(|e| e.name.to_string()).collect()
    }

    fn children(entry: &Entry) -> &[Entry] {
        let Kind::Level(children) = &entry.kind else {
            panic!("not a level")
        };
        children
    }

    #[test]
    fn a_wrong_entry_keeps_its_siblings() {
        let (entries, errors) = parse_tree(quote! {
            a: Request<A, B>,
            b: ,
            c { d: , e: E },
            f g,
            h: H,
        });
        assert_eq!(names(&entries), ["a", "c", "h"]);
        assert_eq!(names(children(&entries[1])), ["e"]);
        assert_eq!(errors.len(), 3, "{errors:?}");
    }

    #[test]
    fn skipping_ignores_commas_in_generics() {
        let (entries, errors) = parse_tree(quote! {
            a(u32 u32): Request<A, B>,
            b: B,
        });
        assert_eq!(names(&entries), ["b"]);
        assert_eq!(errors.len(), 1, "{errors:?}");
    }

    #[test]
    fn duplicate_names_are_reported() {
        let (entries, errors) = parse_tree(quote!(a: A, r#a: B));
        assert_eq!(names(&entries), ["a"]);
        assert_eq!(errors, ["`r#a` is already a topic of this level"]);
    }

    #[test]
    fn docs_of_a_chain_go_to_its_end() {
        let (entries, errors) = parse_tree(quote! {
            /// deep
            a.b(u32).c: C,
        });
        assert!(errors.is_empty());
        let b = &children(&entries[0])[0];
        let c = &children(b)[0];
        assert_eq!(
            (entries[0].docs.len(), b.docs.len(), c.docs.len()),
            (0, 0, 1)
        );
    }

    #[test]
    fn only_docs_go_on_a_topic() {
        let (entries, errors) = parse_tree(quote!(#[cfg(test)] a: A));
        assert_eq!(names(&entries), ["a"]);
        assert_eq!(errors, ["only doc comments go on a topic"]);
    }
}
