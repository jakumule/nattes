# Nattes
## Design issues

Found in a design review, in priority order.

- [ ] **Ambiguous subjects**: a parameter whose `Display` contains `.` gives
  `user.a.b.delete`. Only logs and `subject()`; escape or document.
- [ ] **Document that messages must be `Sync`** (shared by every subscriber),
  which excludes `Cell`, `mpsc::Receiver`, …
- [ ] **Queue groups of a mounted tree are one group across its mounts**
  (`GroupKey` is type + value): delivery is right, but they share the round
  robin and member count.
