# nattes

A typesafe, in-process message queue for Rust, inspired by [NATS](https://nats.io).

Instead of string subjects like `user.*.delete`, you declare your topics as a tree of
types. Then you walk down that tree to publish and subscribe. The compiler checks
every subject, parameter and message type, so a typo is a build error rather than a
subscription that silently never fires.

Terms used here are defined in the [vocabulary](doc/src/vocabulary.md).

```rust
let mut deletes = at!(bus, user.id().delete).subscribe().await; // user.*.delete
while let Some(msg) = deletes.next().await {
    let (user_id,) = msg.params();
    // msg derefs to &UserDeleteMsg
}
```

## Features

- **Typed subjects.** Topics, parameters and messages are Rust types. There's no
  string parsing and no serialization; subscribers get the published value through
  an `Arc`.
- **NATS-style subscriptions.** Subscribe to everything below a topic (`>`), use
  wildcard parameters (`*`) or fixed ones, join queue groups, and do request/reply
  or scatter-gather.
- **Sub-channels are roots.** A `Channel<topics::git::Topic>` behaves like a bus whose
  root is `git`. Hand it to a component, and that component works the same whether
  it's standalone or mounted inside a bigger tree.
- **Async-friendly.** Every handle and future is `Send + Sync`, and a slow
  subscriber never blocks publishers unless you ask for backpressure. Subscriptions
  are `futures::Stream`s that unsubscribe when dropped. It runs on any executor
  and depends on none: timeouts come from your runtime's timer.
- **Editor-friendly.** rust-analyzer completes `at!` paths, hovering a segment
  shows the topic's docs and subject, and going to the definition of a `Topic`
  variant or queue group lands on its line in `topics!`.

## Installation

```toml
[dependencies]
nattes = { git = "https://github.com/JacquesHumule/nattes" }
```

### Tracing

Enable the `tracing` feature to log what the bus does with
[`tracing`](https://docs.rs/tracing), under the `nattes` target:

```toml
nattes = { git = "https://github.com/JacquesHumule/nattes", features = ["tracing"] }
```

| Level   | Logged                                                                |
|---------|-----------------------------------------------------------------------|
| `debug` | `subscribe` / `unsubscribe`, with the id, pattern (`user.id.*.>`) and queue group |
| `trace` | `publish`, with the subject and how many subscriptions got the message |
| `debug` | a message dropped because a subscription's buffer is full            |
| `warn`  | a filter that panicked                                                |

Subjects and patterns are only formatted when the log record is enabled. Without
the feature, nothing is compiled in.

## Quick start

```rust
use futures::StreamExt;
use nattes::{at, topics, Channel};

#[derive(Debug)]
pub struct PushMsg {
    pub branch: String,
}

#[derive(Debug)]
pub struct DeleteMsg;

topics! {
    #[derive(Debug)]
    topics {
        git [Mailer] {
            push: PushMsg,
        },
        user {
            id(u32) {
                delete: DeleteMsg,
            },
        },
    }
}

#[tokio::main]
async fn main() {
    let bus = Channel::<topics::Topic>::new();

    // git.push
    let pushes = at!(bus, git.push);
    let mut sub = pushes.subscribe().await;

    pushes.publish(PushMsg { branch: "main".into() }).await;

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.branch, "main");
    assert_eq!(msg.subject(), "git.push");
}
```

## Declaring topics

`topics!` turns a tree into nested `pub` modules. The root is written like any
level, `topics { … }`, and each topic is one subject segment:

```rust
topics! {
    #[derive(Debug)]                    // applied to every `Topic` enum
    topics {
        git {                           // level
            /// Commits pushed.         // docs of this topic
            push: GitPushMsg,           // leaf message
            new(Uuid): GitNewMsg,       // parameter, then message
            fetch: GitFetchMsg => FetchAck, // request leaf: message, then reply
        },
        user {
            new: UserNewMsg,            // same name as in git: fine
            id(Uuid) [Oncall] {         // parameter, queue groups, then level
                update: UserUpdateMsg,
                delete: UserDeleteMsg,
            },
        },
        random.arrow.more: DeepMsg,     // `a.x` is `a { x }`
        jobs [Mailer] {                 // queue groups of jobs
            run [Shard(u32)]: JobMsg,   // queue groups of jobs.run
        },
        other = other,                  // mount the tree of another topics! module
    }
}
```

Every level is a module containing an enum named `Topic`. This example generates:

```text
topics::Topic                 { Git(git::Topic), User(user::Topic), Random(…), Jobs(jobs::Topic), Other(other::Topic) }
topics::git::Topic            { Push(GitPushMsg), New(Uuid, GitNewMsg), Fetch(GitFetchMsg) }
topics::git::FetchScope       {}
topics::user::Topic           { New(UserNewMsg), Id(Uuid, id::Topic) }
topics::user::id::Topic       { Update(UserUpdateMsg), Delete(UserDeleteMsg) }
topics::user::id::Queue       { Oncall }
topics::jobs::Topic           { Run(JobMsg) }
topics::jobs::Queue           { Mailer }
topics::jobs::RunQueue        { Shard(u32) }
```

So names only need to be unique among siblings: `git::new` and `user::new` don't
clash.

Names are written in snake case, like NATS subjects (`push`, `pull_request`).
They become the subject segments, the module and method names, and, in camel case,
the `Topic` variants: `pull_request` is `Topic::PullRequest`. Rust keywords such as
`type` or `match` are written as raw identifiers: `r#type` is the segment `type`
and `Topic::Type`.

| Syntax | Meaning | Variant in the parent's `Topic` |
|---|---|---|
| `name { … }` | level, in module `name` | `Name(name::Topic)` |
| `name: Msg` | leaf message | `Name(Msg)` |
| `name(Type): Msg` | parameter, then message | `Name(Type, Msg)` |
| `name: Msg => Reply` | request leaf (see [Request/reply](#requestreply)) | `Name(Msg)` |
| `name(Type) { … }` | parameter, then level, in module `name` | `Name(Type, name::Topic)` |
| `name.…` | chain: a level with a single child, no braces | same as `name { … }` |
| `name = path::to::module` | mount the tree of another `topics!` | `Name(path::to::module::Topic)` |
| `name [A, B(Type)] …` | queue groups, right after the name (and parameter); group enum `Queue` in a level's module, `NameQueue` for a leaf | unchanged |

Each module also gets a `Steps` type with one method per child, which is what
`at!` uses to navigate (see below).

Doc comments (`/// …`) on a topic document its `Topic` variant, its `Steps`
method and, for a level, its module, so your editor shows them when you hover
`push` in `at!(bus, git.push)`. On a chain `a.b.c: Msg` they document `c`.
Both also state the subject, such as `git.push` or `user.id.*.delete`. Doc comments before
the root's name document its module, while other attributes there are applied to
every `Topic` enum. A wrong topic is reported on its own and left out, so the rest
of the tree keeps working while you type.

Each generated module does `use super::*`, so message and parameter types are
resolved as in the code around `topics!`. For that reason, `topics!` must be used
at module level, not inside a function.

## Navigating the tree

`Channel::<topics::Topic>::new()` creates a bus rooted at the `topics` module.
Each call creates a separate bus, so create it once and clone it or go down from it.

`at!(channel, path)` gives a sub-channel further down the tree. The path is
relative to the channel and reads like the subject:

```rust
let bus = Channel::<topics::Topic>::new();

let git           = at!(bus, git);                    // git
let pushes        = at!(bus, git.push);               // git.push
let alice         = at!(bus, user.id(alice_id));      // user.<alice>
let alice_deletes = at!(bus, user.id(alice_id).delete);  // user.<alice>.delete
let any_user      = at!(bus, user.id());              // user.*
let all_deletes   = at!(bus, user.id().delete);       // user.*.delete
let any_delete    = at!(any_user, delete);            // user.*.delete, from any_user
```

- **The channel is borrowed, not consumed,** so you can keep using `bus` and take
  as many sub-channels from it as you like. They all share the same bus.
- **Every segment is one step, checked by the compiler.** A typo such as
  `user.push` fails with "no method named `push` found for struct `user::Steps`".
- **A parameter is fixed, `id(alice_id)`, or a wildcard, `id()`.** `id(_)` also
  works as a wildcard. Each wildcard adds its type to the channel: `all_deletes` is
  a `Channel<UserDeleteMsg, (Uuid,)>`. Messages received on it carry the actual
  values in `msg.params()`.

| NATS | nattes |
|---|---|
| `>` | `bus.subscribe()` |
| `git.>` | `at!(bus, git).subscribe()` |
| `git.push` | `at!(bus, git.push).subscribe()` |
| `user.<alice>.>` | `at!(bus, user.id(alice)).subscribe()` |
| `user.*.delete` | `at!(bus, user.id().delete).subscribe()` |
| queue group | `.subscribe().queue(jobs::Queue::Mailer)` |
| request/reply | `channel.request(msg)` / `channel.respond()`, then `req.respond(reply)` |
| scatter-gather | `channel.scatter(msg)` (a stream of replies) |

## Publishing

A channel without wildcards can publish its own type: a leaf message, or a whole
level value. Request leaves are the exception: they take requests only (see
[Request/reply](#requestreply)).

```rust
pushes.publish(GitPushMsg { branch: "main".into() }).await;
alice_deletes.publish(UserDeleteMsg).await;
git.publish(topics::git::Topic::Push(GitPushMsg { branch: "dev".into() })).await;
```

`publish` returns the number of subscriptions the message was delivered to. A channel
with a wildcard has no `publish` method, because there would be no value to fill
the `*` with.

`publish` only waits if a matching subscriber chose `Overflow::Wait` and its buffer
is full. Even then, every other subscriber has already received the message by the
time it waits. See [Delivery guarantees](#delivery-guarantees) for when a message
can be lost and in which order it arrives.

`try_publish` is the same without `async`, for `Drop`, callbacks and code that isn't
async. It never waits: a full `Overflow::Wait` buffer drops the message, as with
`DropNewest`, and counts it in `sub.dropped()`.

```rust
impl Drop for Session {
    fn drop(&mut self) {
        self.logouts.try_publish(LogoutMsg { id: self.id });
    }
}
```

## Subscribing

`subscribe()` returns a builder; `.await` it to start receiving. The subscription is
a `Stream` of messages that deref to the channel's type:

```rust
let mut sub = any_user
    .subscribe()
    .filter(move |(id,), _msg| *id != admin_id) // optional: filter on params and message
    .queue(id::Queue::Oncall)                   // optional: share messages with other members
    .buffer(256)                                // optional: buffer size (default 1024)
    .on_full(Overflow::DropOldest)              // optional: what to do when it's full
    .await;

while let Some(msg) = sub.next().await {
    let (id,) = msg.params();
    match &*msg {
        topics::user::id::Topic::Update(update) => { /* … */ }
        topics::user::id::Topic::Delete(delete) => { /* … */ }
    }
}
```

- **Queue groups.** Subscribers with the same `.queue(group)` on the same bus share
  the messages: each one goes to a single member. A member waiting in `next()` is
  picked first. If none is waiting, members with room in their buffer take turns
  (round robin). When every member's buffer is full, the group waits if any
  member has `Overflow::Wait`, and the message goes to the first of them to have
  room. Otherwise the overflow policy of the member whose turn it is applies. See
  [Queue groups](#queue-groups).
- **Owning a message.** A message is shared by every subscription it was
  delivered to, so `msg` only derefs to `&T`. `msg.try_into_inner()` moves the
  `T` out, with no `Clone` needed, once this `msg` is the last holder: no other
  subscription still holds it, no clone of `msg` exists, and `publish` has
  returned. That is always the case with a single subscriber or a queue group
  member. Otherwise it gives `msg` back as the `Err`.
- **Slow subscribers.** Each subscription has its own buffer. When it's full, the
  `.on_full(...)` policy applies:

  | `Overflow` | Effect |
  |---|---|
  | `DropNewest` (default) | The new message is dropped, like NATS does for slow consumers. |
  | `DropOldest` | The oldest buffered message makes room, so you always see the latest. |
  | `Wait` | `publish` waits for room, then resumes once the buffer has drained to half. Use it for work queues that must not lose messages. `try_publish` drops the new message instead. |

  `sub.dropped()` counts the messages lost so far.

  `Wait` has two pitfalls:

  - **Head-of-line blocking.** The other subscribers get the message before
    `publish` waits, but the publisher's next messages wait behind it: one slow
    `Wait` subscriber holds back what that publisher sends to everyone. Publish
    to `Wait` subscribers from a task of their own, or use a drop policy.
  - **Self-deadlock.** A task that publishes to a subject it subscribes to with
    `Wait` never returns from `publish` once its own buffer is full, since only
    that task can make room.
- **Batches.** `StreamExt::ready_chunks(max)` waits for a message, then returns it
  with the others already buffered (up to `max`), oldest first. What arrives while
  you handle a batch stacks up for the next one, e.g. to answer pending requests
  together:

  ```rust
  let mut batches = fetch_responder.ready_chunks(64);
  while let Some(batch) = batches.next().await {
      let acks = fetch_all(batch.iter().map(|req| &req.branch)).await;
      for (req, ack) in batch.into_iter().zip(acks) {
          req.respond(ack);
      }
  }
  ```

  The buffer size and `on_full` policy still apply while messages stack up.
- **Routing.** Subscriptions are indexed by pattern in an interest trie, like
  the NATS sublist, so `publish` only looks at subscribers on its subject's path
  (with wildcards). Its cost doesn't grow with subscribers on unrelated subjects.
- **Filters.** Filters run inside `publish`, on the publisher's thread (outside
  the bus's internal lock), so a slow filter slows every publish on its subject.
  Keep them cheap and do heavy work after receiving the message. A filter that
  panics counts as "no match" and doesn't affect other subscribers or publishers
  (except with `panic = "abort"`, see [Requirements](#requirements)).
- **Streams.** A subscription is a `FusedStream`, so every `StreamExt` adapter
  applies and it goes straight into `futures::select!` without `.fuse()`.
- **Closing.** Every channel of a bus, sub-channels included, keeps it open.
  Once the last one is dropped, nothing can publish anymore: each subscription
  and responder yields what its buffer holds, then ends, so
  `while let Some(msg) = sub.next().await` returns. A task, a message or a
  filter holding a channel keeps the bus open, like an `mpsc` sender.
- **Unsubscribing.** Drop the subscription. Publishers waiting on it are released.
- **Subjects.** `msg.subject()` gives a NATS-style subject for logging, built from
  names and parameter values, e.g. `user.id.42.delete`. `%`, `.`, `*`, `>` and
  whitespace in parameter values are escaped as `%XX` (`user.id.a%2Eb.delete`), so
  each value stays one segment.

## Queue groups

Queue groups are declared in `topics!` on the topic they belong to, with `[ … ]`
right after its name (after the parameter, if it has one), in chains too:
`user.id(Uuid) [Oncall].delete: Msg`. Like `Topic`, the generated enum has a
fixed name: `Queue` in the module of a level (and of the root, with
`topics [ … ] { … }`), and for a leaf its name followed by `Queue`, in its
parent's module (`push` gives `PushQueue`). Each of its values is one group:

```rust
topics! {
    topics {
        git [Mailer] {
            push [Indexer(u32), Audit]: GitPushMsg,
        },
    }
}

use topics::git::{PushQueue, Queue};

let pushes = at!(bus, git.push);
let a = pushes.subscribe().queue(PushQueue::Indexer(0)).await; // shard 0
let b = pushes.subscribe().queue(PushQueue::Indexer(0)).await; // shard 0
let c = pushes.subscribe().queue(PushQueue::Indexer(1)).await; // shard 1
let m = at!(bus, git).subscribe().queue(Queue::Mailer).await; // git.>

pushes.publish(GitPushMsg { branch: "main".into() }).await;
// one of a and b, then c, then m
```

- **A group is joined only on the topic that declares it.** `Queue` can be
  joined on `git` and `PushQueue` on `git.push`, not the other way around, and not
  on any other topic. A member still receives everything its subscription
  matches: `m`, on `git.>`, gets the pushes.
- **Each value is a group.** `Indexer(0)` and `Indexer(1)` are two groups, and
  two enums never share a group, even with the same variant names. Variant
  values must be `Eq + Hash`, like parameters.
- **Parameters don't change the topic.** A group declared on `user.id` can be
  joined on `user.id(alice)` and on `user.id()`; members on both form one group.
- **Mounted trees keep their groups.** Groups declared inside `other` are joined
  on `other`'s topics, wherever it's mounted.
- **Attributes** go inside the brackets, as inner attributes:
  `[#![derive(Debug)] Indexer(u32), Audit]`. The enum already
  derives `Clone`, `PartialEq`, `Eq` and `Hash`. Variants can have attributes
  of their own, such as doc comments.

A level or leaf declares at most one `[ … ]`: put several families of groups
in its variants. A leaf's group enum is also the scope of its channel, its third
type parameter: `at!(bus, git.push)` is a `Channel<GitPushMsg, (), topics::git::PushQueue>`.
Other channels keep the default (`Channel<T, P>` is `Channel<T, P, T>`).

## Request/reply

Make a leaf request-reply by declaring its reply type after the message,
`name: Msg => Reply`:

```rust
topics! {
    topics {
        git { push: GitPushMsg => PushAck },
    }
}

let pushes = at!(bus, git.push);

// responder: each `req` is an `Incoming`, which derefs to `GitPushMsg`
let mut responder = pushes.respond().await;
tokio::spawn(async move {
    while let Some(req) = responder.next().await {
        req.respond(PushAck { accepted: true });
    }
});

// requester
let ack: PushAck = pushes.request(GitPushMsg { branch: "main".into() }).await?;
// bounded by your runtime's timer
let request = pushes.request(GitPushMsg { branch: "main".into() });
let ack = tokio::time::timeout(Duration::from_secs(1), request).await??;
```

Only responders, made with `respond()`, answer requests. `respond()` returns
the same builder as `subscribe()` (`filter`, `queue`, `buffer`, `on_full`),
and awaiting it gives a `Responder`, a stream of `Incoming` requests. Each
responder gets its own reply token: `req.respond(reply)` takes `req` by value,
so a responder answers each request at most once, and dropping `req` without
answering counts as no reply from this responder. `req.is_pending()` tells
whether anyone still waits for a reply, to skip work nobody needs.
`req.try_into_inner()` takes the payload out, like `Msg::try_into_inner`, together
with a `ReplyToken` that answers with `respond(reply)` in the same way.

Subscribers of the leaf, or of anything above it (`bus.subscribe()` on `>`),
see the requests as plain messages, but they don't count as responders and
can't answer: a logger never delays or breaks a request. A request leaf takes
requests only: it has no `publish`. Responders and subscribers joining the same
queue group form two separate groups.

The first reply wins. Possible errors:

- `BusError::NoResponders`: no responder matches the request.
- `BusError::NoReply`: every responder dropped the request without answering,
  including when a full buffer dropped it.

`request` waits for as long as some responder still holds the request, so wrap
it in your runtime's timeout unless you know a responder will answer. The
timeout covers both delivery and the reply.

### Scatter-gather

`scatter` sends the request to every responder and streams their replies, one per
responder (a queue group counts as one responder). The stream ends once each of
them answered or dropped the request; `replies.remaining()` tells how many are
still expected. `Replies` is a `FusedStream`, so `StreamExt` adapters cover the
rest, e.g. a deadline with `take_until` (delivery, the `scatter(..)` future
itself, takes a timeout like `request`):

```rust
let mut replies = pushes.scatter(GitPushMsg { branch: "main".into() }).await?;
while let Some(ack) = replies.next().await {
    // one `PushAck` per responder
}

// What came back within a second
let acks: Vec<PushAck> = pushes
    .scatter(GitPushMsg { branch: "main".into() })
    .await?
    .take_until(tokio::time::sleep(Duration::from_secs(1)))
    .collect()
    .await;
```

Responders use the same `req.respond(reply)`. Both fail with
`BusError::NoResponders` when no responder matches; dropping the stream makes
`respond` return `false` and `is_pending` turn `false`.

## Delivery guarantees

nattes delivers **at most once**, in memory. A message only lives in subscriber
buffers: nothing is persisted, acknowledged or redelivered. "Delivered" means
pushed into a subscription's buffer, not handled.

These hold for every way of sending:

- **Nothing is sent until you await.** `publish`, `request` and `scatter` are lazy
  futures. `try_publish` sends when called.
- **Who gets it.** Every subscription that matches the subject and whose filters
  accept the message, but only one member per queue group. A subscription
  counts once its builder has been awaited. Messages sent before that are not
  replayed.
- **When it is lost.** A message is lost for a subscriber when:
  - its buffer is full with `DropNewest` (the new message) or `DropOldest` (the
    oldest one in the buffer), or with `Wait` on `try_publish`. In a queue
    group, this only happens when every member's buffer is full and none of
    them has `Overflow::Wait` (or on `try_publish`): the message goes to a
    member with room first;
  - one of its filters rejects the message or panics;
  - it drops the subscription before reading the message: buffered messages
    are discarded;
  - the sending future is dropped while it waits for an `Overflow::Wait`
    subscriber: the subscribers it waited for miss the message, the others
    already have it.
- **Order.** The messages of one publisher, awaiting each call before the next,
  reach each subscriber in the order they were sent, minus the ones lost.
  Concurrent publishers have no common order, so two subscribers can see their
  messages in different orders. Consecutive messages to a queue group can go to
  different members, which can handle them in any order.

Per method:

| Method | Returns | Waits for |
|---|---|---|
| `publish` | How many subscriptions the message was delivered to. Not on request leaves. | Room in full `Overflow::Wait` buffers. |
| `try_publish` | Same as `publish`, but full `Overflow::Wait` buffers drop the message. Not async. | Nothing. |
| `request` | The first reply. `NoResponders` if no responder matches it, `NoReply` once every responder dropped it without answering. | Delivery, then a reply, with no time limit. |
| `scatter` | A stream of replies, at most one per responder, or `NoResponders`. | Delivery. The stream ends once every responder answered or dropped the request. |

A request goes to every matching subscriber and responder, and only the
first reply counts. To have exactly one handler, respond through a queue group.
A request that is lost from a responder's buffer counts as dropped without a reply. Once
the requester gives up (drops the future, e.g. on a timeout), the request may still
sit in buffers and be handled, but its reply is discarded: `respond` returns `false`.

## Sub-channels as roots

A channel's type only mentions the part of the tree it points to. A component that
takes a `Channel<other::Topic>` doesn't know, and doesn't need to know, whether it's
connected to its own bus or mounted inside a bigger one:

```rust
topics! {
    #[derive(Debug)]
    other {
        text(String): StringEvent,
    }
}

async fn other_service(other: Channel<other::Topic>) {
    let mut texts = at!(other, text()).subscribe().await;
    // …
}

// standalone
other_service(Channel::<other::Topic>::new()).await;

// mounted with `other = other` in topics
other_service(at!(bus, other)).await;
```

## Passing channels around

To write a function that subscribes anywhere in the tree, take a `&Channel<T, P, S>`.
The root channel and any `at!` work the same:

```rust
async fn log_all<T: 'static, P: 'static, S>(ch: &Channel<T, P, S>) {
    let mut sub = ch.subscribe().await;
    while let Some(msg) = sub.next().await {
        println!("{}", msg.subject());
    }
}

log_all(&bus).await;                   // >
log_all(&at!(bus, git)).await;         // git.>
log_all(&at!(bus, user.id())).await;   // user.*.>
```

Fix `T` to accept only one part of the tree, for example
`&Channel<topics::git::Topic>`. Leave `P` generic to also accept wildcard channels,
or leave it out (`P = ()`) to require a channel that can publish. `S`, the scope,
only matters for leaves that declare queue groups (see
[Queue groups](#queue-groups)); leave it out unless you want to accept those.

Take the channel by value instead when the function keeps it, for example to move it
into `tokio::spawn`. Clones are cheap.

## Compile-time checks

Each of these is a build error:

```rust
// publishing on a wildcard channel
all_deletes.publish(UserDeleteMsg);
// error: no method named `publish` found for struct `Channel<UserDeleteMsg, (Uuid,)>`

// a path that isn't below the channel (alice is on user.<alice>)
at!(alice, git.push);
// error: no method named `git` found for struct `id::Steps<()>`

// wrong message for the topic
pushes.publish(UserDeleteMsg);
// error: expected `GitPushMsg`, found `UserDeleteMsg`

// typo in the path
at!(bus, user.push);
// error: no method named `push` found for struct `user::Steps<()>`

// wrong parameter type
at!(bus, user.id("alice").delete);
// error: `Is<&str>` can't select a `Uuid` parameter
//        expected `_` or a `Uuid` value

// two siblings with the same name
topics! { topics { git { push: GitPushMsg, push: GitPushMsg } } }
// error: `push` is already a topic of this level

// a parameter type that can't be compared exactly
topics! { sensors { reading(f64): Reading } }
// error: `f64` can't be a parameter
//        the trait `Eq` is not implemented for `f64`

// a queue group named with a string
pushes.subscribe().queue("workers");
// error: `&str` isn't a queue group

// a queue group declared on another topic (git::Queue is on git)
pushes.subscribe().queue(Queue::Mailer);
// error: queue group `Queue` isn't declared on this topic

// wrong value for a queue group
pushes.subscribe().queue(PushQueue::Indexer("0"));
// error: expected `u32`, found `&str`

// a request on a leaf declared without a reply (`push: GitPushMsg`)
pushes.request(GitPushMsg { branch: "main".into() });
// error: the method `request` exists for struct `Channel<GitPushMsg, (), PushQueue>`,
//        but its trait bounds were not satisfied
pushes.respond();
// error: `GitPushMsg` isn't the payload of a request leaf

// answering twice (`req` is an `Incoming`)
req.respond(ack);
req.respond(ack);
// error: use of moved value: `req`

// answering from a subscription (`msg` is a `Msg`)
msg.respond(ack);
// error: no method named `respond` found for struct `Msg`

// publishing on a request leaf (`fetches` is on `fetch: GitFetchMsg => FetchAck`)
fetches.publish(GitFetchMsg { branch: "main".into() });
// error: can't publish on a request leaf
```

The same holds for `try_publish`.

## Requirements

- Messages must be `Send + Sync + 'static`.
- Parameter types must implement `Clone + Eq + Hash + Display`. Messages are routed
  by comparing parameter values with `Eq`, and `Display` is only used for
  `subject()`, escaped as above. Floats aren't `Eq`, so they can't be parameters.
- Values in queue group variants must implement `Clone + Eq + Hash + Send + Sync +
  'static`.
- `topics!` must be used at module level, because its modules find your types
  through `use super::*`.
- Containing panicking filters relies on unwinding, so it doesn't work with
  `panic = "abort"`.

## License

MIT, see [LICENSE](LICENSE).
