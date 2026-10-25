# Nattes
## Design issues

Found in a design review, in priority order.

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
