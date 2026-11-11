Requirements: bounds on messages and parameters, and limits.

- **Messages must be `Send + Sync + 'static`.** One published message is shared
  by every subscriber that receives it, so it must be `Sync`: this rules out
  fields like `Cell`, `RefCell` or [`std::sync::mpsc::Receiver`]. Wrap such a
  field in a `Mutex`, or send something shareable instead (an `Arc`, a
  `futures::channel` sender, …).
  [`Msg::try_into_inner`](crate::Msg::try_into_inner) gives the message back by
  value once no other subscriber holds it.
- **Parameter types must implement `Clone + Eq + Hash + Display`**
  ([`ParamValue`](crate::ParamValue)). Messages are routed by comparing
  parameter values with `Eq`, and `Display` is only used for `subject()`,
  escaped as described in
  [subscribing](crate::guide::subscribing#messages). Floats aren't `Eq`, so
  they can't be parameters.
- **Fields of queue group variants** must implement
  `Clone + Eq + Hash + Send + Sync + 'static`.
- **A channel has at most 6 wildcard parameters.**
- **`topics!` must be used at module level,** because its modules find your
  types through `use super::*`.
- **Containing panicking filters and handlers relies on unwinding,** so it
  doesn't work with `panic = "abort"`.
- **Every handle and future is `Send + Sync`,** and nattes runs on any
  executor: it depends on none.
