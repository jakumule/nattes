# Nattes
## Design issues

Found in a design review, in priority order.

- [ ] **(Un)subscribe is O(n) in subscriptions on one trie node.**
  `Bus::subscribe` calls `Arc::make_mut` on `load_full()` while the `ArcSwap`
  still holds the trie, so every trie node on the path is cloned, including
  its whole `subs: Vec`. N subscriptions on one channel cost O(N²); the imbl
  map only fixed many parameter values. Fix: a persistent collection for
  `subs`/`groups`; add an `instructions` bench with 10k subscriptions on one
  channel.
- [ ] **`Overflow::Wait` pitfalls.** Undocumented self-deadlock: a task
  publishing to a subject it subscribes to with `Wait` and a full buffer. In a
  queue group where every member is full, `pick` applies the first accepting
  member's policy, so mixed policies drop or wait depending on the round
  robin, and the publisher waits on that member even if another drains first.
  Document head-of-line blocking next to the overflow table in the README.
- [ ] **No sync publish.** `publish` is `async` though only `Wait` buffers ever
  await. Add `try_publish` (doesn't wait; drops or reports full `Wait`
  buffers) for `Drop`, callbacks and non-async code.
- [ ] **Filters run on the publisher's thread**, inside `publish`: a slow
  filter slows every publisher, and with `panic = "abort"` a panicking filter
  aborts instead of counting as no match. Document ("keep filters cheap").
- [ ] **`scatter` has no timeout**, and `request_timeout` ties the crate to the
  tokio timer, against being executor-agnostic.
- [ ] **Ambiguous subjects**: a parameter whose `Display` contains `.` gives
  `user.a.b.delete`. Only logs and `subject()`; escape or document.
- [ ] **Document that messages must be `Sync`** (shared by every subscriber),
  which excludes `Cell`, `mpsc::Receiver`, …
- [ ] **Queue groups of a mounted tree are one group across its mounts**
  (`GroupKey` is type + value): delivery is right, but they share the round
  robin and member count.
