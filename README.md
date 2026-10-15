# nattes

A typesafe, in-process message queue for Rust, inspired by [NATS](https://nats.io).

Instead of string subjects like `user.*.delete`, you declare your topics as a tree of
types. Then you walk down that tree to publish and subscribe. The compiler checks
every subject, parameter and message type, so a typo is a build error rather than a
subscription that silently never fires.

```rust
let mut deletes = topic!(bus, user.id().delete).subscribe().await; // user.*.delete
while let Some(msg) = deletes.next().await {
    let (user_id,) = msg.params();
    // msg derefs to &UserDeleteMsg
}
```

## Features

- **Typed subjects.** Topics, parameters and payloads are Rust types. There's no
  string parsing and no serialization; subscribers get the published value through
  an `Arc`.
- **NATS-style subscriptions.** Subscribe to everything below a topic (`>`), use
  wildcard parameters (`*`) or fixed ones, join queue groups, and do request/reply
  or scatter-gather.
- **Sub-channels are roots.** A `Channel<events::git::Topic>` behaves like a bus whose
  root is `git`. Hand it to a component, and that component works the same whether
  it's standalone or mounted inside a bigger tree.
- **Async-friendly.** Every handle and future is `Send + Sync`, and a slow
  subscriber never blocks publishers unless you ask for backpressure. Subscriptions
  are `futures::Stream`s that unsubscribe when dropped. It runs on any executor;
  only `request_timeout` needs tokio's timer.

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

| Level   | Event                                                                 |
|---------|-----------------------------------------------------------------------|
| `debug` | `subscribe` / `unsubscribe`, with the id, subject (`user.id.*.>`) and queue group |
| `trace` | `publish`, with the subject and how many subscribers got the message  |
| `debug` | a message dropped because a subscriber's buffer is full               |
| `warn`  | a filter that panicked                                                |

Subjects are only formatted when the event is enabled. Without the feature, nothing
is compiled in.

## Quick start

```rust
use futures::StreamExt;
use nattes::{channel, events, topic};

#[derive(Debug)]
pub struct PushMsg {
    pub branch: String,
}

#[derive(Debug)]
pub struct DeleteMsg;

events! {
    #[derive(Debug)]
    pub mod events {
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
    let bus = channel!(events);

    // git.push
    let pushes = topic!(bus, git.push);
    let mut sub = pushes.subscribe().await;

    pushes.publish(PushMsg { branch: "main".into() }).await;

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.branch, "main");
    assert_eq!(msg.subject(), "git.push");
}
```

## Declaring events

`events!` turns a tree into nested modules. Each entry is one subject segment:

```rust
events! {
    #[derive(Debug)]                    // applied to every `Topic` enum
    pub mod events {
        git {                           // subtree
            push: GitPushMsg,           // leaf message
            new(Uuid): GitNewMsg,       // parameter, then message
        },
        user {
            new: UserNewMsg,            // same name as in git: fine
            id(Uuid) [Oncall] {         // parameter, queue groups, then subtree
                update: UserUpdateMsg,
                delete: UserDeleteMsg,
            },
        },
        random.arrow.more: DeepMsg,     // `a.x` is `a { x }`
        jobs [Mailer] {                 // queue groups of jobs
            run [Shard(u32)]: JobMsg,   // queue groups of jobs.run
        },
        other = other,                  // mount the tree of another events! module
    }
}
```

Every level is a module containing an enum named `Topic`. This example generates:

```text
events::Topic                 { Git(git::Topic), User(user::Topic), Random(…), Jobs(jobs::Topic), Other(other::Topic) }
events::git::Topic            { Push(GitPushMsg), New(Uuid, GitNewMsg) }
events::user::Topic           { New(UserNewMsg), Id(Uuid, id::Topic) }
events::user::id::Topic       { Update(UserUpdateMsg), Delete(UserDeleteMsg) }
events::user::id::Queue       { Oncall }
events::jobs::Topic           { Run(JobMsg) }
events::jobs::Queue           { Mailer }
events::jobs::RunQueue        { Shard(u32) }
```

So names only need to be unique among siblings: `git::new` and `user::new` don't
clash.

Names are written in snake case, like NATS subjects (`push`, `pull_request`).
They become the subject segments, the module and method names, and, in camel case,
the `Topic` variants: `pull_request` is `Topic::PullRequest`. Rust keywords such as
`type` or `match` can't be used as names.

| Syntax | Meaning | Variant in the parent's `Topic` |
|---|---|---|
| `name { … }` | subtree, in module `name` | `Name(name::Topic)` |
| `name: Msg` | leaf message | `Name(Msg)` |
| `name(Type): Msg` | parameter, then message | `Name(Type, Msg)` |
| `name(Type) { … }` | parameter, then subtree, in module `name` | `Name(Type, name::Topic)` |
| `name.…` | subtree with a single child, no braces | same as `name { … }` |
| `name = path::to::module` | mount the tree of another `events!` | `Name(path::to::module::Topic)` |
| `name [A, B(Type)] …` | queue groups, right after the name (and parameter); enum `Queue` in a subtree's module, `NameQueue` for a leaf | unchanged |

Each module also gets a `Steps` type with one method per child, which is what
`topic!` uses to navigate (see below).

Each generated module does `use super::*`, so message and parameter types are
resolved as in the code around `events!`. For that reason, `events!` must be used
at module level, not inside a function.

## Navigating the tree

`channel!(events)` creates a bus rooted at the `events` module. It's short for
`Channel::<events::Topic>::new()`, and module paths work too:
`channel!(crate::events)`. Each call creates a separate bus, so create it once and
clone it or go down from it.

`topic!(channel, path)` gives a sub-channel further down the tree. The path is
relative to the channel and reads like the subject:

```rust
let bus = channel!(events);

let git           = topic!(bus, git);                       // git
let pushes        = topic!(bus, git.push);                  // git.push
let alice         = topic!(bus, user.id(alice_id));         // user.<alice>
let alice_deletes = topic!(bus, user.id(alice_id).delete);  // user.<alice>.delete
let any_user      = topic!(bus, user.id());                 // user.*
let all_deletes   = topic!(bus, user.id().delete);          // user.*.delete
let any_delete    = topic!(any_user, delete);               // user.*.delete, from any_user
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
| `git.>` | `topic!(bus, git).subscribe()` |
| `git.push` | `topic!(bus, git.push).subscribe()` |
| `user.<alice>.>` | `topic!(bus, user.id(alice)).subscribe()` |
| `user.*.delete` | `topic!(bus, user.id().delete).subscribe()` |
| queue group | `.subscribe().queue(jobs::Queue::Mailer)` |
| request/reply | `channel.request(msg)` / `msg.respond(reply)` |
| scatter-gather | `channel.scatter(msg)` / `channel.gather(msg, timeout)` |

## Publishing

A channel without wildcards can publish its own type: a leaf message, or a whole
subtree value.

```rust
pushes.publish(GitPushMsg { branch: "main".into() }).await;
alice_deletes.publish(UserDeleteMsg).await;
git.publish(events::git::Topic::Push(GitPushMsg { branch: "dev".into() })).await;
```

`publish` returns the number of subscribers that queued the message. A channel
with a wildcard has no `publish` method, because there would be no value to fill
the `*` with.

`publish` only waits if a matching subscriber chose `Overflow::Wait` and its buffer
is full. Even then, every other subscriber has already received the message by the
time it waits.

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
        events::user::id::Topic::Update(update) => { /* … */ }
        events::user::id::Topic::Delete(delete) => { /* … */ }
    }
}
```

- **Queue groups.** Subscribers with the same `.queue(group)` on the same bus share
  the messages: each one goes to a single member. A member waiting in `next()` is
  picked first. If none is waiting, members take turns (round robin). See
  [Queue groups](#queue-groups).
- **Slow subscribers.** Each subscription has its own buffer. When it's full, the
  `.on_full(...)` policy applies:

  | `Overflow` | Effect |
  |---|---|
  | `DropNewest` (default) | The new message is dropped, like NATS does for slow consumers. |
  | `DropOldest` | The oldest buffered message makes room, so you always see the latest. |
  | `Wait` | `publish` waits for room, then resumes once the buffer has drained to half. Use it for work queues that must not lose messages. |

  `sub.dropped()` counts the messages lost so far.
- **Routing.** Subscriptions are indexed by subject in an interest graph, like
  the NATS sublist, so `publish` only looks at subscribers on its subject's path
  (with wildcards). Its cost doesn't grow with subscribers on unrelated subjects.
- **Filters.** Filters run outside the bus's internal lock. A filter that panics
  counts as "no match" and doesn't affect other subscribers or publishers.
- **Unsubscribing.** Drop the subscription. Publishers waiting on it are released.
- **Subjects.** `msg.subject()` gives a NATS-style subject for logging, built from
  names and parameter values, e.g. `user.id.42.delete`. `msg.root()` gives the whole
  event.

## Queue groups

Queue groups are declared in `events!` on the topic they belong to, with `[ … ]`
right after its name (after the parameter, if it has one), in chains too:
`user.id(Uuid) [Oncall].delete: Msg`. Like `Topic`, the generated enum has a
fixed name: `Queue` in the module of a subtree (and of the root, with
`pub mod events [ … ] { … }`), and for a leaf its name followed by `Queue`, in its
parent's module (`push` gives `PushQueue`). Each of its values is one group:

```rust
events! {
    pub mod events {
        git [Mailer] {
            push [Indexer(u32), Audit]: GitPushMsg,
        },
    }
}

use events::git::{PushQueue, Queue};

let pushes = topic!(bus, git.push);
let a = pushes.subscribe().queue(PushQueue::Indexer(0)).await; // shard 0
let b = pushes.subscribe().queue(PushQueue::Indexer(0)).await; // shard 0
let c = pushes.subscribe().queue(PushQueue::Indexer(1)).await; // shard 1
let m = topic!(bus, git).subscribe().queue(Queue::Mailer).await; // git.>

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
  derives `Clone`, `PartialEq`, `Eq` and `Hash`.

A level or leaf declares at most one `[ … ]`: put several families of groups
in its variants. A leaf's enum is also the third type parameter of its channel:
`topic!(bus, git.push)` is a `Channel<GitPushMsg, (), events::git::PushQueue>`.
Other channels keep the default (`Channel<T, P>` is `Channel<T, P, T>`).

## Request/reply

Make a leaf request-reply by giving it the type `nattes::Request<Payload, Reply>`:

```rust
events! {
    pub mod events {
        git { push: Request<GitPushMsg, PushAck> },
    }
}

let pushes = topic!(bus, git.push);

// responder: `msg` derefs to the `Request`, which derefs to `GitPushMsg`
let mut requests = pushes.subscribe().await;
tokio::spawn(async move {
    while let Some(msg) = requests.next().await {
        msg.respond(PushAck { accepted: true });
    }
});

// requester
let ack: PushAck = pushes.request(GitPushMsg { branch: "main".into() }).await?;
let ack = pushes
    .request_timeout(GitPushMsg { branch: "main".into() }, Duration::from_secs(1))
    .await?;
```

Responders can check `msg.is_pending()` to skip work nobody is waiting for. Leaves
under `a.b.c` chains can't use a type with a top-level comma; use a type
alias there.

The first reply wins. Possible errors:

- `BusError::NoResponders`: nobody is subscribed.
- `BusError::NoReply`: every receiver dropped the request without answering.
- `BusError::Timeout`: `request_timeout` ran out of time.

`request` on its own waits for as long as some receiver still holds the request, so
prefer `request_timeout` unless you know a responder will answer.

### Scatter-gather

`scatter` sends the request to every subscriber and streams their replies, one per
responder (a queue group counts as one responder). The stream ends once each of
them answered or dropped the request; `replies.remaining()` tells how many are
still expected. `gather` collects the replies until they are all in or the timeout
runs out, and returns whatever came back:

```rust
let mut replies = pushes.scatter(GitPushMsg { branch: "main".into() }).await?;
while let Some(ack) = replies.next().await {
    // one `PushAck` per responder
}

let acks: Vec<PushAck> = pushes
    .gather(GitPushMsg { branch: "main".into() }, Duration::from_secs(1))
    .await?;
```

Responders use the same `msg.respond(reply)`, and should answer once: an extra
reply takes the place of another responder's. Both fail with
`BusError::NoResponders` when nobody is subscribed; dropping the stream makes
`respond` return `false` and `is_pending` turn `false`.

## Sub-channels as roots

A channel's type only mentions the part of the tree it points to. A component that
takes a `Channel<other::Topic>` doesn't know, and doesn't need to know, whether it's
connected to its own bus or mounted inside a bigger one:

```rust
events! {
    #[derive(Debug)]
    pub mod other {
        text(String): StringEvent,
    }
}

async fn other_service(other: Channel<other::Topic>) {
    let mut texts = topic!(other, text()).subscribe().await;
    // …
}

// standalone
other_service(channel!(other)).await;

// mounted with `other = other` in events
other_service(topic!(bus, other)).await;
```

## Passing channels around

To write a function that subscribes anywhere in the tree, take a `&Channel<T, P, S>`.
The root channel and any `topic!` work the same:

```rust
async fn log_all<T: 'static, P: 'static, S>(ch: &Channel<T, P, S>) {
    let mut sub = ch.subscribe().await;
    while let Some(msg) = sub.next().await {
        println!("{}", msg.subject());
    }
}

log_all(&bus).await;                      // >
log_all(&topic!(bus, git)).await;         // git.>
log_all(&topic!(bus, user.id())).await;   // user.*.>
```

Fix `T` to accept only one part of the tree, for example
`&Channel<events::git::Topic>`. Leave `P` generic to also accept wildcard channels,
or leave it out (`P = ()`) to require a channel that can publish. `S` only matters
for leaves that declare queue groups (see [Queue groups](#queue-groups)); leave it
out unless you want to accept those.

Take the channel by value instead when the function keeps it, for example to move it
into `tokio::spawn`. Clones are cheap.

## Compile-time checks

Each of these is a build error:

```rust
// publishing on a wildcard channel
all_deletes.publish(UserDeleteMsg);
// error: no method named `publish` found for struct `Channel<UserDeleteMsg, (Uuid,)>`

// a path that isn't below the channel (alice is on user.<alice>)
topic!(alice, git.push);
// error: no method named `git` found for struct `id::Steps<()>`

// wrong message for the topic
pushes.publish(UserDeleteMsg);
// error: expected `GitPushMsg`, found `UserDeleteMsg`

// typo in the path
topic!(bus, user.push);
// error: no method named `push` found for struct `user::Steps<()>`

// wrong parameter type
topic!(bus, user.id("alice").delete);
// error: `Is<&str>` can't select a `Uuid` parameter
//        expected `_` or a `Uuid` value

// a parameter type that can't be compared exactly
events! { pub mod sensors { reading(f64): Reading } }
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
```

## Requirements

- Messages must be `Send + Sync + 'static`.
- Parameter types must implement `Clone + Eq + Hash + Display`. Messages are routed
  by comparing parameter values with `Eq`, and `Display` is only used for
  `subject()`. Floats aren't `Eq`, so they can't be parameters.
- Values in queue group variants must implement `Clone + Eq + Hash + Send + Sync +
  'static`.
- `events!` must be used at module level, because its modules find your types
  through `use super::*`.
- `request_timeout` and `gather` need a tokio runtime with the timer enabled. Everything else
  runs on any executor.
- Containing panicking filters relies on unwinding, so it doesn't work with
  `panic = "abort"`.

## License

MIT, see [LICENSE](LICENSE).
