//! Parsing the `topics!` tree. Errors are collected entry by entry: a wrong
//! entry is skipped up to the next `,` and its siblings are still parsed.

use std::collections::{HashMap, HashSet};

use proc_macro2::{TokenStream, TokenTree};
use syn::ext::IdentExt;
use syn::parse::discouraged::Speculative;
use syn::parse::{ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Error, Ident, LitInt, Path, Result, Token, Type, Variant, braced, bracketed,
    parenthesized, token,
};

pub struct Input {
    /// Path of the nattes crate: `$crate`.
    pub krate: TokenTree,
    /// Attributes other than docs, applied to every `TopicRef` enum.
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

/// The delivery defaults `#[buffer(n)]` and `#[on_full(Variant)]` of a topic,
/// for it and the topics below it that don't set their own.
#[derive(Clone, Default)]
pub struct Delivery {
    pub buffer: Option<LitInt>,
    /// A variant of `Overflow`.
    pub on_full: Option<Ident>,
}

impl Delivery {
    fn is_empty(&self) -> bool {
        self.buffer.is_none() && self.on_full.is_none()
    }

    /// Whether `attr` is one of these attributes.
    fn is(attr: &Attribute) -> bool {
        attr.path().is_ident("buffer") || attr.path().is_ident("on_full")
    }

    /// Reads `attr`, one of these attributes, into `self`.
    fn set(&mut self, attr: &Attribute) -> Result<()> {
        if attr.path().is_ident("buffer") {
            let size: LitInt = attr.parse_args()?;
            if size.base10_parse::<usize>()? == 0 {
                return Err(Error::new(size.span(), "buffer size must be at least 1"));
            }
            if self.buffer.replace(size).is_some() {
                return Err(Error::new_spanned(attr, "`buffer` is already set on this topic"));
            }
        } else {
            let overflow: Ident = attr.parse_args()?;
            if self.on_full.replace(overflow).is_some() {
                return Err(Error::new_spanned(attr, "`on_full` is already set on this topic"));
            }
        }
        Ok(())
    }

    /// These defaults, the unset ones taken from `outer`'s.
    pub fn or(&self, outer: &Delivery) -> Delivery {
        Delivery {
            buffer: self.buffer.clone().or_else(|| outer.buffer.clone()),
            on_full: self.on_full.clone().or_else(|| outer.on_full.clone()),
        }
    }
}

/// One topic: `name(Param) [A, B]` and what follows.
pub struct Entry {
    pub docs: Vec<Attribute>,
    pub delivery: Delivery,
    pub name: Ident,
    pub param: Option<Type>,
    pub groups: Option<GroupEnum>,
    pub kind: Kind,
}

// a few per tree, only while expanding: their size doesn't matter
#[allow(clippy::large_enum_variant)]
pub enum Kind {
    /// `name { … }`, and `name.rest` for `name { rest }`
    Level(Vec<Entry>),
    /// `name: Message`, or `name: Message => Reply` for a request leaf
    Leaf { msg: Type, reply: Option<Type> },
    /// `name = module`
    Mount(Path),
}

pub fn parse(tokens: TokenStream, errors: &mut Vec<Error>) -> Result<Input> {
    let parser = |input: ParseStream| {
        let krate = input.parse()?;
        let (docs, attrs): (Vec<_>, Vec<_>) = Attribute::parse_outer(input)?
            .into_iter()
            .partition(|attr| attr.path().is_ident("doc"));
        let (defaults, attrs): (Vec<_>, Vec<_>) = attrs.into_iter().partition(Delivery::is);
        let mut delivery = Delivery::default();
        for attr in &defaults {
            if let Err(error) = delivery.set(attr) {
                errors.push(error);
            }
        }
        let name = input.parse()?;
        let groups = group_enum(input)?;
        let content;
        braced!(content in input);
        let root = Entry {
            docs,
            delivery,
            name,
            param: None,
            groups,
            kind: Kind::Level(entries(&content, errors)),
        };
        Ok(Input { krate, attrs, root })
    };
    parser.parse2(tokens)
}

/// The group enum after a topic's name and parameter, if any.
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

/// Method names of `Channel`, and of the traits of the std prelude that it
/// implements: a topic with one of these names couldn't be stepped to.
const RESERVED: &[&str] = &[
    "subscribe",
    "respond",
    "publish",
    "publish_many",
    "request",
    "scatter",
    "close",
    "clone",
    "clone_from",
    "clone_into",
    "to_owned",
    "into",
    "try_into",
];

/// The segment id of a topic name: its FNV-1a hash. It identifies the topic
/// among its siblings, in the interest trie and on the wire, and doesn't
/// depend on the order of the siblings.
pub fn segment_id(name: &str) -> u32 {
    name.bytes().fold(0x811c_9dc5, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

/// The entries of one level, up to the end of `input`.
fn entries(input: ParseStream, errors: &mut Vec<Error>) -> Vec<Entry> {
    let mut out = Vec::new();
    let mut names = HashSet::new();
    let mut ids = HashMap::new();
    while !input.is_empty() {
        let fork = input.fork();
        match entry(&fork, errors) {
            Ok(entry) => {
                input.advance_to(&fork);
                let name = entry.name.unraw().to_string();
                if RESERVED.contains(&name.as_str()) {
                    let msg = format!(
                        "`{name}` can't be a topic name: channels have a method `{name}`"
                    );
                    errors.push(Error::new(entry.name.span(), msg));
                } else if !names.insert(name.clone()) {
                    let msg = format!("`{}` is already a topic of this level", entry.name);
                    errors.push(Error::new(entry.name.span(), msg));
                } else if let Some(other) = ids.insert(segment_id(&name), name.clone()) {
                    let msg = format!(
                        "`{name}` and `{other}` have the same segment id: rename one of them"
                    );
                    errors.push(Error::new(entry.name.span(), msg));
                } else {
                    out.push(entry);
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
    let mut delivery = Delivery::default();
    for attr in Attribute::parse_outer(input)? {
        if attr.path().is_ident("doc") {
            docs.push(attr);
        } else if Delivery::is(&attr) {
            if let Err(error) = delivery.set(&attr) {
                errors.push(error);
            }
        } else {
            errors.push(Error::new_spanned(
                attr,
                "only doc comments, `#[buffer(n)]` and `#[on_full(…)]` go on a topic",
            ));
        }
    }
    topic(input, docs, delivery, errors)
}

/// A topic after its docs and delivery defaults, which go on the last topic
/// of a chain `a.b.c`.
fn topic(
    input: ParseStream,
    docs: Vec<Attribute>,
    delivery: Delivery,
    errors: &mut Vec<Error>,
) -> Result<Entry> {
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
    let (docs, delivery, kind) = if input.peek(Token![.]) {
        input.parse::<Token![.]>()?;
        let last = topic(input, docs, delivery, errors)?;
        (Vec::new(), Delivery::default(), Kind::Level(vec![last]))
    } else if input.peek(token::Brace) {
        let content;
        braced!(content in input);
        (docs, delivery, Kind::Level(entries(&content, errors)))
    } else if input.peek(Token![:]) {
        input.parse::<Token![:]>()?;
        let msg = input
            .parse()
            .map_err(|e| Error::new(e.span(), "expected the message type"))?;
        let reply = if input.peek(Token![=>]) {
            input.parse::<Token![=>]>()?;
            let reply = input
                .parse()
                .map_err(|e| Error::new(e.span(), "expected the reply type"))?;
            Some(reply)
        } else {
            None
        };
        (docs, delivery, Kind::Leaf { msg, reply })
    } else if input.peek(Token![=]) && param.is_none() && groups.is_none() {
        input.parse::<Token![=]>()?;
        if !delivery.is_empty() {
            // every mount shares the mounted tree's types, and their defaults
            errors.push(Error::new(
                name.span(),
                "`#[buffer]` and `#[on_full]` don't go on a mount: set them in the mounted tree",
            ));
        }
        (
            docs,
            Delivery::default(),
            Kind::Mount(Path::parse_mod_style(input)?),
        )
    } else {
        return Err(input.error(
            "expected `{ … }`, `: Message`, `: Message => Reply`, `.name` or, without parameter and groups, `= module`",
        ));
    };
    Ok(Entry {
        docs,
        delivery,
        name,
        param,
        groups,
        kind,
    })
}

/// Skips past the next `,` that isn't inside a generic argument list. The `>`
/// of `->` and `=>` doesn't close one.
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
            arrow = matches!(&tt, TokenTree::Punct(p) if matches!(p.as_char(), '-' | '='));
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
    fn channel_methods_are_reserved() {
        let (entries, errors) = parse_tree(quote!(close: A, r#publish: B, c: C));
        assert_eq!(names(&entries), ["c"]);
        assert_eq!(
            errors,
            [
                "`close` can't be a topic name: channels have a method `close`",
                "`publish` can't be a topic name: channels have a method `publish`",
            ]
        );
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

    fn leaf(entry: &Entry) -> (String, Option<String>) {
        let Kind::Leaf { msg, reply } = &entry.kind else {
            panic!("not a leaf")
        };
        let text = |ty: &Type| quote!(#ty).to_string();
        (text(msg), reply.as_ref().map(text))
    }

    #[test]
    fn request_leaves_declare_a_reply() {
        let (entries, errors) = parse_tree(quote! {
            a: A => B,
            b(u32) [G]: A => Vec<B>,
            c.d: A => B,
            e: A,
        });
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(leaf(&entries[0]), ("A".into(), Some("B".into())));
        assert_eq!(leaf(&entries[1]), ("A".into(), Some("Vec < B >".into())));
        assert!(entries[1].param.is_some() && entries[1].groups.is_some());
        assert_eq!(leaf(&children(&entries[2])[0]).1.as_deref(), Some("B"));
        assert_eq!(leaf(&entries[3]), ("A".into(), None));
    }

    #[test]
    fn a_wrong_request_leaf_keeps_its_siblings() {
        let (entries, errors) = parse_tree(quote! {
            a: A =>,
            b: A B => Result<X, Y>,
            c: C => D,
        });
        assert_eq!(names(&entries), ["b", "c"]);
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors[0].contains("reply type"), "{errors:?}");
    }

    fn groups(entry: &Entry) -> Vec<String> {
        let groups = entry.groups.as_ref().expect("no groups");
        groups.variants.iter().map(|v| v.ident.to_string()).collect()
    }

    #[test]
    fn groups_go_on_every_kind_of_topic() {
        let mut errors = Vec::new();
        let body = quote! {
            krate ev [Root] {
                a [#![derive(Debug)] A, B(u32)] { x: X },
                b(u32) [C]: B,
                c [D]: C => R,
                d.e(u64) [E].f: F,
            }
        };
        let input = parse(body, &mut errors).unwrap();
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(groups(&input.root), ["Root"]);
        let entries = children(&input.root);
        assert_eq!(groups(&entries[0]), ["A", "B"]);
        assert_eq!(entries[0].groups.as_ref().unwrap().attrs.len(), 1);
        assert_eq!(groups(&entries[1]), ["C"]);
        assert_eq!(groups(&entries[2]), ["D"]);
        let e = &children(&entries[3])[0];
        assert!(entries[3].groups.is_none());
        assert_eq!(groups(e), ["E"]);
        assert!(children(e)[0].groups.is_none());
    }

    #[test]
    fn a_wrong_group_enum_keeps_its_siblings() {
        let (entries, errors) = parse_tree(quote! {
            a [A, 3]: A,
            b [B] = other,
            c [C, D]: C,
        });
        assert_eq!(names(&entries), ["c"]);
        assert_eq!(groups(&entries[0]), ["C", "D"]);
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(
            errors[1].contains("without parameter and groups"),
            "{errors:?}"
        );
    }

    #[test]
    fn only_docs_go_on_a_topic() {
        let (entries, errors) = parse_tree(quote!(#[cfg(test)] a: A));
        assert_eq!(names(&entries), ["a"]);
        assert_eq!(
            errors,
            ["only doc comments, `#[buffer(n)]` and `#[on_full(…)]` go on a topic"]
        );
    }

    fn delivery(entry: &Entry) -> (Option<String>, Option<String>) {
        let Delivery { buffer, on_full } = &entry.delivery;
        (
            buffer.as_ref().map(ToString::to_string),
            on_full.as_ref().map(ToString::to_string),
        )
    }

    #[test]
    fn delivery_defaults_go_on_levels_leaves_and_the_root() {
        let mut errors = Vec::new();
        let body = quote! {
            krate
            #[buffer(8)]
            #[derive(Debug)]
            ev {
                #[on_full(Wait)]
                a { #[buffer(2)] b: B, c: C },
                /// docs
                #[on_full(DropOldest)]
                d.e: E,
                f: F,
            }
        };
        let input = parse(body, &mut errors).unwrap();
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(delivery(&input.root), (Some("8".into()), None));
        assert_eq!(input.attrs.len(), 1, "other attributes go on `TopicRef`");
        let entries = children(&input.root);
        assert_eq!(delivery(&entries[0]), (None, Some("Wait".into())));
        let a = children(&entries[0]);
        assert_eq!(delivery(&a[0]), (Some("2".into()), None));
        assert_eq!(delivery(&a[1]), (None, None));
        assert_eq!(delivery(&entries[1]), (None, None));
        let e = &children(&entries[1])[0];
        assert_eq!(delivery(e), (None, Some("DropOldest".into())));
        assert_eq!(delivery(&entries[2]), (None, None));
    }

    #[test]
    fn wrong_delivery_defaults_are_reported() {
        let (entries, errors) = parse_tree(quote! {
            #[buffer(0)] a: A,
            #[on_full(Wait)] #[on_full(DropOldest)] b: B,
            #[buffer(4)] c = other,
            #[buffer(x)] d: D,
            e: E,
        });
        assert_eq!(names(&entries), ["a", "b", "c", "d", "e"]);
        assert_eq!(delivery(&entries[1]), (None, Some("DropOldest".into())));
        assert_eq!(delivery(&entries[2]), (None, None));
        assert_eq!(errors.len(), 4, "{errors:?}");
        assert_eq!(errors[0], "buffer size must be at least 1");
        assert_eq!(errors[1], "`on_full` is already set on this topic");
        assert!(errors[2].contains("don't go on a mount"), "{errors:?}");
    }
}
