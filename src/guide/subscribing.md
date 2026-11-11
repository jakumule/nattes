Subscribing: subscriptions, filters, overflow policies, batches, handlers, closing.

[`subscribe()`](crate::Channel::subscribe) returns a
[builder](crate::SubscriptionBuilder); `.await` it to start receiving. The
[`Subscription`](crate::Subscription) is a `Stream` of messages:

```
# use futures::StreamExt;
# use nattes::{topics, Channel, Wild};
# use uuid::Uuid;
# pub struct UserUpdateMsg;
# pub struct UserDeleteMsg;
# topics! {
#     topics {
#         user { id(Uuid) [Oncall] { update: UserUpdateMsg, delete: UserDeleteMsg } },
#     }
# }
# use topics::prelude::*;
use nattes::Overflow;
use topics::user::id;

# async fn demo(bus: Channel<topics::Topic>, admin_id: Uuid) {
let any_user = bus.user().id(Wild); // user.id.*
let mut sub = any_user
    .subscribe()
    .filter(move |(id,), _view| *id != admin_id) // optional: filter on params and message
    .queue(id::Queue::Oncall)                    // optional: share messages with other members
    .buffer(256)                                 // optional: buffer size (default 1024, or the topic's)
    .on_full(Overflow::DropOldest)               // optional: what to do when it's full (default DropNewest, or the topic's)
    .await;

while let Some(msg) = sub.next().await {
    let (id,) = msg.params();
    match msg.view() {
        id::TopicRef::Update(update) => { /* … */ }
        id::TopicRef::Delete(delete) => { /* … */ }
    }
}
# }
# fn main() {}
```

# Messages

On a leaf, a [`Msg`](crate::Msg) derefs to the leaf's message type
(`msg.branch`). On a level, [`msg.view()`](crate::Msg::view) gives the level's
`TopicRef`, which tells which topic below it the message was published on. A
filter gets the same view, after the params.

**Owning a message.** A message is shared by every subscription it was
delivered to, so `msg` only gives references.
[`msg.try_into_inner()`](crate::Msg::try_into_inner) moves the message out of a
leaf's `msg`, with no `Clone` needed, once this `msg` is the last holder: no
other subscription still holds it, no clone of `msg` exists, and `publish` has
returned. That is always the case with a single subscriber or a queue group
member. Otherwise it gives `msg` back as the `Err`.

**Subjects.** [`msg.subject()`](crate::Msg::subject) gives a NATS-style subject
for logging, built from names and parameter values, e.g. `user.id.42.delete`.
`%`, `.`, `*`, `>` and whitespace in parameter values are escaped as `%XX`
(`user.id.a%2Eb.delete`), so each value stays one segment.

# Slow subscribers

Each subscription has its own buffer. When it's full, the
[`.on_full(...)`](crate::SubscriptionBuilder::on_full) policy applies:

| [`Overflow`](crate::Overflow) | Effect |
|---|---|
| `DropNewest` (default) | The new message is dropped, like NATS does for slow consumers. |
| `DropOldest` | The oldest buffered message makes room, so you always see the latest. |
| `Wait` | `publish` waits for room, then resumes once the buffer has drained to half. Use it for work queues that must not lose messages. |

The tree can change the default per topic with `#[on_full(…)]` and
`#[buffer(n)]` (see
[delivery defaults](crate::guide::declaring#delivery-defaults)).
[`sub.dropped()`](crate::Subscription::dropped) counts the messages lost so
far, and with the [`tracing` feature](crate::guide::tracing) each loss is
logged at `warn`.

`Wait` has two pitfalls:

- **Head-of-line blocking.** The other subscribers get the message before
  `publish` waits, but the publisher's next messages wait behind it: one slow
  `Wait` subscriber holds back what that publisher sends to everyone. Publish
  to `Wait` subscribers from a task of their own, or use a drop policy.
- **Self-deadlock.** A task that publishes to a subject it subscribes to with
  `Wait` never returns from `publish` once its own buffer is full, since only
  that task can make room.

# Filters

[Filters](crate::SubscriptionBuilder::filter) run inside `publish`, on the
publisher's thread (outside the bus's internal lock), so a slow filter slows
every publish on its subject. Keep them cheap and do heavy work after receiving
the message. A filter that panics counts as "no match" and doesn't affect other
subscribers or publishers (except with `panic = "abort"`, see
[requirements](crate::guide::requirements)).

# Batches

[`sub.recv_many(&mut buf, max).await`](crate::Subscription::recv_many) appends
up to `max` buffered messages to `buf`, taken under one lock, and waits only
while the buffer is empty. It returns how many it took, or 0 once the
subscription ended.
[`poll_recv_many`](crate::Subscription::poll_recv_many) is the same as a poll
function.

```
# use nattes::{topics, Channel};
# pub struct GitPushMsg { pub branch: String }
# topics! { topics { git { push: GitPushMsg } } }
# use topics::prelude::*;
# fn main() {
# futures::executor::block_on(async {
# let bus = Channel::<topics::Topic>::new();
let pushes = bus.git().push();
let mut sub = pushes.subscribe().await;
pushes
    .publish_many(["main", "dev"].map(|b| GitPushMsg { branch: b.into() }))
    .await;

let mut buf = Vec::new();
let taken = sub.recv_many(&mut buf, 16).await;
assert_eq!(taken, 2);
assert_eq!(buf[1].branch, "dev");
# });
# }
```

# Handlers

[`.handler(f)`](crate::SubscriptionBuilder::handler) instead of `.await` calls
`f` with each message, on the publisher's thread: no buffer, no task, no
wake-up. It returns a [`Handler`](crate::Handler), which unsubscribes on drop.
`f` must not block, may run on several publisher threads at once, and a panic
counts the message as dropped. Use it for counters, logs, or to forward into
another library's channel.

```
# use nattes::{topics, Channel};
# use std::sync::Arc;
# use std::sync::atomic::{AtomicUsize, Ordering};
# pub struct GitPushMsg { pub branch: String }
# topics! { topics { git { push: GitPushMsg } } }
# use topics::prelude::*;
# fn main() {
# futures::executor::block_on(async {
# let bus = Channel::<topics::Topic>::new();
let pushes = bus.git().push();
let count = Arc::new(AtomicUsize::new(0));
let counter = count.clone();
let _handler = pushes.subscribe().handler(move |_msg| {
    counter.fetch_add(1, Ordering::Relaxed);
});

pushes.publish(GitPushMsg { branch: "main".into() }).await;
assert_eq!(count.load(Ordering::Relaxed), 1); // already called
# });
# }
```

# Throughput

A subscriber that keeps up with its publisher finds its buffer empty after
nearly every message and parks, and waking it costs the publisher. For
high-rate fan-out to subscribers that do little per message, use handlers, or
`publish_many` and `recv_many`: in the `throughput` bench they move 2–4× as
many messages as one `publish` and `next()` each.

Subscriptions are indexed by pattern in an interest trie, like the NATS
sublist, so `publish` only looks at subscribers on its subject's path (with
wildcards). Its cost doesn't grow with subscribers on unrelated subjects.

# Streams

A subscription is a `FusedStream`, so every `StreamExt` adapter applies and it
goes straight into `futures::select!` without `.fuse()`.

# Closing and unsubscribing

**Unsubscribing.** Drop the subscription. Publishers waiting on it are
released.

**Closing.** Every channel of a bus, sub-channels included, keeps it open. Once
the last one is dropped, nothing can publish anymore: each subscription and
responder yields what its buffer holds, then ends, so
`while let Some(msg) = sub.next().await` returns. A task, a message or a filter
holding a channel keeps the bus open, like an `mpsc` sender.

[`close()`](crate::Channel::close) on any channel closes the bus whoever still
holds one, which is how to shut down workers that also publish: later publishes
reach nobody, requests fail with `NoResponders`, new subscriptions end at once
and bridges stop.

```
# use futures::StreamExt;
# use nattes::{topics, Channel};
# pub struct Job;
# topics! { topics { jobs: Job } }
# use topics::prelude::*;
# #[tokio::main]
# async fn main() -> Result<(), tokio::task::JoinError> {
# let bus = Channel::<topics::Topic>::new();
# let mut workers = Vec::new();
# for _ in 0..2 {
#     let mut sub = bus.jobs().subscribe().await;
#     let bus = bus.clone(); // a worker that could also publish
#     workers.push(tokio::spawn(async move {
#         while let Some(_job) = sub.next().await {}
#         drop(bus);
#     }));
# }
# bus.jobs().publish(Job).await;
bus.close();
for worker in workers {
    worker.await?; // each one reads what it was sent, then its loop ends
}
# Ok(())
# }
```
