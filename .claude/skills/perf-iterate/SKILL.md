---
name: perf-iterate
description: Measure a performance change in nattes with exact instruction counts (the gungraun `instructions` bench, baseline before and after), optionally confirmed by the criterion `throughput` bench, and record the result in CLAUDE.md. Use when optimizing the bus, delivery, the trie, channels or the macro output, or when asked whether a change costs anything.
---

# Perf iterate

`instructions` counts are exact and repeatable, so they show small changes. `throughput` is wall-clock and noisy: use it only to confirm effects that a single thread can't show (wakes, contention, parking).

## Steps

1. **Baseline before touching code.** If the change is already in the working copy, stash it first (`jj new @-` for a sibling, or `git stash`), then:
   ```sh
   nix develop -c cargo bench --bench instructions -- --save-baseline=before
   ```
   This needs Valgrind and `gungraun-runner` at the version of the `gungraun` dev-dependency, which the dev shell provides. If the runner's version doesn't match, bump the `version` and hashes in `flake.nix` together with the dev-dependency.
2. **Apply the change**, then compare:
   ```sh
   nix develop -c cargo bench --bench instructions -- --baseline=before
   ```
   To run a subset, put a filter before `--`, the way the gungraun CLI takes it.
3. **Read the diff.**
   - Noise: `subscribe`/`unsubscribe` with 1000+ existing subscriptions move by up to ~8% between identical builds (the parameter map's random hash seed). Treat that as noise unless it shows up again on a rerun.
   - Everything else is real, even ±1%. Check the cases next to the target, not only the target: CLAUDE.md records regressions that showed up elsewhere (e.g. a shared helper costing `publish_many` +40%, fan-out +8%).
4. **Optional, for cross-thread effects**: run one throughput group before and after, e.g. `cargo bench --bench throughput -- fan_out`. Report it with its spread and treat <5% as inconclusive.
5. **Record it** in CLAUDE.md, in the existing style: what was tried, and the numbers (`449k → 12k instructions at 10k subscriptions`, `−10–17% throughput on routing`). Put an adopted change in the architecture paragraph of the module it touches. Put a rejected one under "Measured and not done", or next to the design it lost to, so it isn't tried again.

Shared bench setup (the tree, `Message`, `start()`) lives in `benches/common/mod.rs`. Add new cases there rather than copying the setup.
