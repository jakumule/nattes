---
name: nattes-review
description: Review a nattes diff against the repo's own invariants (locks vs user code, logging, vocabulary, scope rule, Send + Sync, compile-time checks). Use when reviewing changes to this repo, alongside /code-review, which covers general correctness.
---

# nattes review

Review the current change (`jj diff`, or `git diff` against `main`) against the rules below. Report only real violations, each with `file:line`, the rule it breaks and a concrete fix. Reporting nothing is fine.

## Locks
- **No user value dropped while a lock is held.** That covers events, messages, filters, handlers, an old trie or `Interest` holding them, and reply tokens. Their `Drop` is user code and may publish or unsubscribe, so it deadlocks. The value must be moved out and dropped after unlocking (see `Buffer::try_push`/`close`, `Bus::unsubscribe`). Watch for implicit drops: a `MutexGuard` scope ending after an assignment that replaced a value, `Vec::clear`, `Option::take` whose result isn't bound outside the guard, `Arc::make_mut` on the trie.
- **No logging while a lock is held.** `trace!`/`debug!`/`warning!` (`src/log.rs`) call into a tracing subscriber, which is user code.
- **No user callback while a lock is held**, apart from the deliberate cases: a handler's `HandlerFn` runs in `try_push`, and matchers run in `visit`, with panics caught.
- If a new drop path is added, a matching `*_on_drop` test, run on a thread with a timeout, should come with it.

## Logging
- Format subjects and patterns lazily (`subject = %Subject(..)`, `pattern = %Pattern(..)`), never with `subject()`.

## API and types
- Handles and futures stay `Send + Sync`, and messages stay `Send + Sync + 'static`.
- A change to navigation or publishing comes with `compile_fail` doctests in `src/lib.rs` and an update to the README's "Compile-time checks" (see the `api-change` skill).
- No backward-compat shims, deprecated aliases or re-exports kept for old names.
- Generated names follow Rust conventions. Only clippy method-name lints are silenced.

## Words and scope
- Terms match `doc/src/vocabulary.md`: no new synonyms, and new concepts are added there.
- Scope rule: new code must need the trie, a buffer's lock or delivery counts, or avoid an extra buffer, wake or task. Otherwise it belongs in a README recipe. No async runtime in `[dependencies]`.

## Performance
- On hot paths (`bus.rs` delivery, `nav.rs`, `channel.rs` steps), ask whether the change was measured. CLAUDE.md records several innocent-looking refactors that cost 2–50%. If it wasn't measured, suggest `perf-iterate`.
