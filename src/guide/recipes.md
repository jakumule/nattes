Recipes: what nattes leaves to other crates, and how to do it with them.

nattes leaves out what other crates already do at no extra cost. It depends on
no async runtime: timers and tasks are yours.

| To | Use |
|---|---|
| Receive or publish without `async` | `sub.next().now_or_never()`, `chan.publish(m).now_or_never()` (completes on the first poll unless a `Wait` buffer is full), or `futures::executor::block_on` |
| Feed a channel from a stream | `stream.for_each(\|m\| chan.publish(m))`, or `futures::sink::unfold` for a `Sink` |
| Timeouts and deadlines | your runtime's timer: `tokio::time::timeout`, `StreamExt::take_until` |
| Frame a byte stream for a bridge | `tokio_util::codec::LengthDelimitedCodec`, `asynchronous-codec` |
| Reconnect, encrypt, compress a bridge | your transport crate (rustls, quinn, …) |
| Serialize messages for a bridge | serde with bincode, postcard, …, behind your [`Codec`](https://docs.rs/nattes/latest/nattes/trait.Codec.html) |

Without `async`:

```
# use nattes::{topics, Channel};
# pub struct GitPushMsg { pub branch: String }
# topics! { topics { git { push: GitPushMsg } } }
# use topics::prelude::*;
use futures::{FutureExt, StreamExt};
use std::future::IntoFuture;

# fn main() {
let bus = Channel::<topics::Topic>::new();
let pushes = bus.git().push();
// a builder is ready at once
let mut sub = pushes.subscribe().into_future().now_or_never().unwrap();

let delivered = pushes.publish(GitPushMsg { branch: "main".into() }).now_or_never();
assert_eq!(delivered, Some(1));

let msg = sub.next().now_or_never().flatten().unwrap();
assert_eq!(msg.branch, "main");
assert!(sub.next().now_or_never().is_none()); // nothing buffered
# }
```

A timeout on a request, with tokio:

```
# use nattes::{topics, Channel};
# use std::time::Duration;
# pub struct GitPushMsg;
# pub struct PushAck;
# topics! { topics { git { push: GitPushMsg => PushAck } } }
# use topics::prelude::*;
# #[tokio::main]
# async fn main() {
# let bus = Channel::<topics::Topic>::new();
# let pushes = bus.git().push();
# let _responder = pushes.respond().await; // never answers
let request = pushes.request(GitPushMsg);
let reply = tokio::time::timeout(Duration::from_millis(10), request).await;
assert!(reply.is_err()); // elapsed
# }
```
