---
name: macro-expand
description: Show the code topics! generates for a tree, with cargo-expand. Use when working on macros/src/expand.rs or parse.rs, debugging a topics! compile error, or checking the generated Steps, TopicRef, leaf types, group enums or Wire impls.
---

# Macro expand

`cargo-expand` is in the dev shell. It runs on stable (it sets `RUSTC_BOOTSTRAP` itself).

- **One existing tree**: expand the target that declares it and keep only its module. Examples:
  ```sh
  nix develop -c cargo expand --lib --tests tests    # the unit tests' trees (src/tests.rs, cfg(test))
  nix develop -c cargo expand --bench instructions   # the benches' tree (benches/common/mod.rs)
  nix develop -c cargo expand --features bridge --test bridge   # with Wire impls
  ```
  The output is long. Write it to a file under `target/` and read the module you need, instead of printing everything.
- **A new tree**: put it in a scratch example, `examples/expand_scratch.rs` (`use nattes::topics; topics! { … } fn main() {}`), then run `cargo expand --example expand_scratch`. Delete the file afterwards and don't commit it.
- Add `--features bridge` to see the `Wire<C>` impls, which are generated only with that feature.

When checking a change to `expand.rs`, compare the expansion of the same tree before and after. Generated code size affects compile time for users, which is why levels no longer get an owned `Topic` value.
