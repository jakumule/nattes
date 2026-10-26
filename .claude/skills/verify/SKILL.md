---
name: verify
description: Run nattes's full local check matrix (tests per feature, doctests, macro parser tests, clippy, the docs book) and report failures per step. Use after any change to src/, macros/, tests/, benches/ or doc/, and before saying a change is done.
---

# Verify

CI only runs `cargo test --workspace` and clippy. The `tracing` and `bridge` tests and the book need their own commands. Run every step, even after one fails, so the report is complete. Run each one through the dev shell (`nix develop -c …`), unless the environment already provides `cargo` (direnv).

| Step | Command |
|---|---|
| unit tests, doctests, macro parser tests | `cargo test --workspace` |
| tracing (own binary: tracing caches callsite interest globally) | `cargo test --features tracing --test tracing` |
| bridges, in memory and on a Unix socket | `cargo test --features bridge --test bridge` |
| all features together | `cargo test --workspace --all-features` |
| lint, as in CI | `cargo clippy --workspace --all-features --all-targets` |
| docs book | `mdbook build doc` |

The `*_on_drop`/`dropped_filter_*` tests run on a thread with a timeout. If one fails with a timeout, look for a deadlock: a user value dropped or a log emitted while a lock is held, not a slow machine.

Report one line per step (ok / failed). For each failure, give the first relevant error, with `file:line`. Clippy warnings don't block CI (the SARIF upload is non-blocking), but list any new ones.
