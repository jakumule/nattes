# Nattes

- Route cache: skip the trie walk on repeated publishes from one channel without
  `unsafe` or reference cycles (see CLAUDE.md, "Measured and not done").
- Bridges: request/reply across a bridge (reply tokens over the wire), and
  sending fixed parameter values in interest instead of widening them to `*`.
- `recv_many` on `Responder`.
