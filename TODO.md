# Nattes
## Design issues

Found in a design review, in priority order.

- [ ] **Queue groups of a mounted tree are one group across its mounts**
  (`GroupKey` is type + value): delivery is right, but they share the round
  robin and member count.
