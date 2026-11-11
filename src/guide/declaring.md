Declaring topics: the `topics!` syntax and what it generates.

[`topics!`](crate::topics) turns a tree into nested `pub` modules. The root is
written like any level, `topics { … }`, and each topic is one subject segment:

```
# use nattes::topics;
# use uuid::Uuid;
# #[derive(Debug)] pub struct GitPushMsg;
# #[derive(Debug)] pub struct GitNewMsg;
# #[derive(Debug)] pub struct GitFetchMsg;
# #[derive(Debug)] pub struct FetchAck;
# #[derive(Debug)] pub struct UserNewMsg;
# #[derive(Debug)] pub struct UserUpdateMsg;
# #[derive(Debug)] pub struct UserDeleteMsg;
# #[derive(Debug)] pub struct DeepMsg;
# #[derive(Debug)] pub struct StringEvent;
# topics! { #[derive(Debug)] other { text(String): StringEvent } }
topics! {
    #[derive(Debug)]                    // applied to every `TopicRef` enum
    topics {
        git [Mailer] {                  // level, with its queue groups
            /// Commits pushed.
            push [Indexer(u32)]: GitPushMsg, // leaf message, with its queue groups
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
        other = other,                  // mount the tree of another topics! module
    }
}
# fn main() {}
```

Every level is a module with an empty [`Topic`](crate::Topic) enum, the `T` of
its channels, and a `TopicRef<'a>` enum, what a subscriber there sees: which
child a message was published on, with references to its parameters and
message. Every leaf gets a leaf type named after it, the `T` of its channels.
Queue groups become a group enum: `Queue` in a level's module, `NameQueue` next
to a leaf type. This example generates:

```text
topics::Topic, topics::git::Topic, topics::user::Topic, topics::user::id::Topic, …
topics::TopicRef<'a>     { Git(git::TopicRef<'a>), User(user::TopicRef<'a>), Random(…), Other(other::TopicRef<'a>) }
topics::git::TopicRef<'a> { Push(&'a GitPushMsg), New(&'a Uuid, &'a GitNewMsg), Fetch(&'a GitFetchMsg) }
topics::git::PushLeaf, NewLeaf, FetchLeaf
topics::git::Queue        { Mailer }
topics::git::PushQueue    { Indexer(u32) }
topics::user::TopicRef<'a> { New(&'a UserNewMsg), Id(&'a Uuid, id::TopicRef<'a>) }
topics::user::id::Queue   { Oncall }
topics::prelude           the `Steps` trait of every level
```

So names only need to be unique among siblings: `git::new` and `user::new` don't
clash.

# Names

Names are written in snake case, like NATS subjects (`push`, `pull_request`).
They become the subject segments, the module and method names, and, in camel
case, the `TopicRef` variants and leaf types: `pull_request` is
`TopicRef::PullRequest` and `PullRequestLeaf`. Rust keywords such as `type` or
`match` are written as raw identifiers: `r#type` is the segment `type` and
`TopicRef::Type`. A name can't be a method of [`Channel`](crate::Channel)
(`subscribe`, `publish`, `publish_many`, `respond`, `request`, `scatter`) or of
the std prelude traits it implements (`clone`, `into`, …).

# Syntax

| Syntax | Meaning | Variant in the parent's `TopicRef<'a>` |
|---|---|---|
| `name { … }` | level, in module `name` | `Name(name::TopicRef<'a>)` |
| `name: Msg` | leaf message, leaf type `NameLeaf` | `Name(&'a Msg)` |
| `name(Type): Msg` | parameter, then message | `Name(&'a Type, &'a Msg)` |
| `name: Msg => Reply` | request leaf (see [request/reply](crate::guide::request_reply)) | `Name(&'a Msg)` |
| `name(Type) { … }` | parameter, then level, in module `name` | `Name(&'a Type, name::TopicRef<'a>)` |
| `name(Type) [A, B(T)] …` | queue groups (see [queue groups](crate::guide::queue_groups)), after the name and parameter of a level, leaf, link of a chain or the root | unchanged |
| `name.…` | chain: a level with a single child, no braces | same as `name { … }` |
| `name = path::to::module` | mount the tree of another `topics!` | `Name(path::to::module::TopicRef<'a>)` |
| `#[on_full(Wait)]`, `#[buffer(64)]` before a topic | delivery defaults (see [below](#delivery-defaults)) | unchanged |

Each level also gets a `Steps` trait with one method per child, implemented on
its channels. `topics::prelude` brings every `Steps` trait of the tree into
scope, mounted trees included: `use topics::prelude::*;`.

# Docs and attributes

Doc comments (`/// …`) on a topic document its `TopicRef` variant, its `Steps`
method, and its module or leaf type, so your editor shows them when you hover
`push` in `bus.git().push()`. On a chain `a.b.c: Msg` they document `c`. Both
also state the subject, such as `git.push` or `user.id.*.delete`. Doc comments
before the root's name document its module, while other attributes there,
except [delivery defaults](#delivery-defaults), are applied to every `TopicRef`
enum. A wrong topic is reported on its own and left out, so the rest of the
tree keeps working while you type.

Each generated module does `use super::*`, so message and parameter types are
resolved as in the code around `topics!`. For that reason, `topics!` must be
used at module level, not inside a function.

# Delivery defaults

`#[on_full(…)]` and `#[buffer(n)]` before a topic set what its subscriptions
start with, instead of [`Overflow::DropNewest`](crate::Overflow) and
[1024](crate::DEFAULT_BUFFER) (see [subscribing](crate::guide::subscribing)).
Declare `Wait` on work topics, and nobody subscribes to them with a lossy
policy by mistake:

```
# use nattes::{topics, Overflow, Topic};
# pub struct Resize;
# pub struct Thumb;
# pub struct Progress;
# pub struct Metric;
topics! {
    app {
        #[on_full(Wait)]
        jobs {
            #[buffer(64)]
            resize: Resize,             // Wait, 64
            thumbnail: Thumb,           // Wait, 1024
            #[on_full(DropOldest)]
            progress: Progress,         // DropOldest, 1024
        },
        metrics: Metric,                // DropNewest, 1024
    }
}
# fn main() {
# use app::jobs::{ResizeLeaf, ThumbnailLeaf, ProgressLeaf};
# assert_eq!((<ResizeLeaf as Topic>::ON_FULL, <ResizeLeaf as Topic>::BUFFER), (Overflow::Wait, 64));
# assert_eq!((<ThumbnailLeaf as Topic>::ON_FULL, <ThumbnailLeaf as Topic>::BUFFER), (Overflow::Wait, 1024));
# assert_eq!((<ProgressLeaf as Topic>::ON_FULL, <ProgressLeaf as Topic>::BUFFER), (Overflow::DropOldest, 1024));
# assert_eq!((<app::jobs::Topic as Topic>::ON_FULL, <app::jobs::Topic as Topic>::BUFFER), (Overflow::Wait, 1024));
# assert_eq!((<app::MetricsLeaf as Topic>::ON_FULL, <app::MetricsLeaf as Topic>::BUFFER), (Overflow::DropNewest, 1024));
# }
```

- **Inherited.** A default applies to the topic and every topic below it that
  doesn't set its own. On the root, it applies to the whole tree; on a chain
  `a.b.c`, to `c`.
- **The topic you subscribe on.** `bus.jobs().resize().subscribe()` and
  `bus.jobs().subscribe()` start with `Wait`; `bus.subscribe()` starts with the
  root's defaults, even though it also gets the messages of `jobs`.
- **Only defaults.** `.on_full(…)` and `.buffer(n)` on a subscription, a
  responder or a [bridge](crate::guide::bridges) still win. Handlers don't
  buffer, so neither applies to them.
- **Mounts.** Every mount of a tree shares its types, so a mounted tree keeps
  the defaults declared in its own `topics!`, and a mount can't set any.
