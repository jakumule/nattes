---
name: api-change
description: Checklist for changing nattes's public API or its compile-time guarantees (navigation, publishing, subscribing, request/reply, queue groups, the topics! syntax, bridges). Use when adding, removing or renaming a public item or method, changing a trait bound, or changing what topics! accepts or generates.
---

# API change

The crate is an unreleased preview: break the API freely, without deprecation shims or aliases. What has to stay consistent is the code, its compile-time checks and the docs. Go through every item. If one doesn't apply, say why.

1. **Compile-time checks.** Every guarantee that changed or was added gets a `compile_fail` doctest in `src/guide/compile_time.md`, under a sentence describing it and with the error quoted as a comment (the README quotes three of them at the top). Make sure each `compile_fail` test fails for the intended reason: write the passing version next to it, or check the error with `cargo test --doc -- --nocapture`.
2. **Error messages.** `QueueGroup`, `InScope` and similar traits have `on_unimplemented` messages that `src/guide/compile_time.md` quotes. Keep the two in sync.
3. **New `Channel` methods** can clash with topic names, which become `Steps` methods. Add the name to `RESERVED` in `macros/src/parse.rs`, along with a parser test.
4. **`topics!` syntax or output.** Update `macros/src/parse.rs` and `expand.rs` together. Keep the spans on names (go to definition lands in `topics!`) and the subject docs on variants and methods. Keep the `bridge` feature's `Wire` impls in step. The `macro-expand` skill shows the generated code.
5. **`Send + Sync`.** Every handle and future stays `Send + Sync`. Messages stay `Send + Sync + 'static`.
6. **Vocabulary.** Add new concepts to `doc/src/vocabulary.md`, and update renamed ones there and everywhere else (code, docs, comments). Don't introduce synonyms.
7. **Docs.** The guide (`src/guide/*.md`, the docs of `nattes::guide`) is the user-facing spec: update its chapters, whose examples are doctests (`cargo test --doc --all-features`). `README.md` is the pitch: update its quick start, tour or features only if the change shows there. Update the CLAUDE.md architecture section for the module concerned.
8. **Scope.** If the feature could be built outside nattes with a common library, at no speed cost, it belongs in a recipe of the guide (`src/guide/recipes.md`) instead.
9. **Verify.** Run the `verify` skill. If the change touches a hot path, also run `perf-iterate`.
