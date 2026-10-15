//! Proc macros of [nattes](https://docs.rs/nattes). Use them through it:
//! `nattes::events!` is documented there.

use proc_macro::TokenStream;

mod expand;
mod parse;

/// Expands `nattes::events!`, which passes its `$crate` as the first token.
///
/// A wrong entry is reported on its own span and left out, so the rest of
/// the tree still expands: editors keep resolving it while it's being typed.
#[doc(hidden)]
#[proc_macro]
pub fn events(input: TokenStream) -> TokenStream {
    let mut errors = Vec::new();
    let mut out = match parse::parse(input.into(), &mut errors) {
        Ok(input) => expand::expand(&input),
        Err(error) => {
            errors.push(error);
            proc_macro2::TokenStream::new()
        }
    };
    out.extend(errors.iter().map(syn::Error::to_compile_error));
    out.into()
}
