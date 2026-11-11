Tracing: what the `tracing` feature logs.

Enable the `tracing` feature to log what the bus does with
[`tracing`](https://docs.rs/tracing). Records keep their module as target
(`nattes::bus`, `nattes::bridge`), so `nattes=debug` enables them all and
`nattes::bridge=debug` only a bridge's:

```toml
nattes = { version = "0.4", features = ["tracing"] }
```

| Level   | Logged |
|---------|--------|
| `debug` | `subscribe` / `unsubscribe`, with the id, pattern (`user.id.*.>`) and queue group |
| `trace` | `publish`, with the subject and how many subscriptions got the message |
| `warn`  | a message dropped because a subscription's buffer is full (one record per subscription and `publish_many`, with the count), a filter or handler that panicked, a message a bridge couldn't encode or decode |

A [bridge](crate::guide::bridges) runs in a `debug` span named `bridge`, with
the bridged channel (`channel = user.>`), and logs at `debug` when it starts
and stops (with the error it returns, if any).

Subjects and patterns are only formatted when the log record is enabled.
Without the feature, nothing is compiled in. nattes installs no subscriber:
that's the application's choice. To get the records through
[`log`](https://docs.rs/log) instead, enable `tracing`'s `log` feature in your
own `Cargo.toml`:

```toml
tracing = { version = "0.1", features = ["log"] }
```
