# Vocabulary

One term per concept, used everywhere: in the README, the API docs, the code
and its comments. Don't introduce synonyms. When a concept is added or renamed,
update this page first.

The examples use this tree:

```rust
topics! {
    topics {
        git {
            push: GitPushMsg,
            fetch: GitFetchMsg => FetchAck,
        },
        user { id(Uuid) { delete: UserDeleteMsg } },
        random.arrow.more: DeepMsg,
        other = other,
    }
}
```

## Tree

| Term | Meaning | Don't say |
|---|---|---|
| **tree** | Everything declared in one `topics!`. A mounted tree is declared in another `topics!`. | — |
| **topic** | One named place in the tree: a level or a leaf. `git`, `git.push` and `user.id` are topics. The `T` of a channel implements the `Topic` trait. | node, entry (in user docs) |
| **level** | A topic with children. It becomes a module with an empty `Topic` enum (the `T` of its channels), a `TopicRef` enum (what its subscribers view) and a `Steps` trait. Levels subscribe but don't publish. `git` and `user.id` are levels. The top level, `topics`, is the **root**. | subtree, node |
| **leaf** | A topic carrying a message type: `push: GitPushMsg`. | — |
| **leaf type** | The empty enum generated for a leaf, named after it, the `T` of its channels: `git::PushLeaf`. It implements `Leaf`, which gives its message type. | marker |
| **request leaf** | A leaf declared with a reply type, `fetch: GitFetchMsg => FetchAck`. Its leaf type implements `Request`, which gives the reply type. It carries its message like any leaf, and its channel takes requests only: no `publish`. | — |
| **chain** | `a.b.c`, a shorthand for `a { b { c } }`: `random.arrow.more: DeepMsg`. | — |
| **mount** | `name = module`: the root of another tree, as a level of this one. `other = other`. | — |
| **name** | A topic's identifier as written in `topics!`: `push`, `r#type`. It gives the segment, the module and method names, and, in camel case, the `TopicRef` variant and leaf type. | — |
| **segment id** | A `u32` hash of a name, computed by `topics!`. It identifies a topic among its siblings, in the interest trie and on the wire, whatever their order. | index |
| **segment** | One part of a subject or pattern, separated by dots: a name or a parameter value. `user.id.42.delete` has four. `%`, `.`, `*`, `>` and whitespace in a parameter value are escaped as `%XX`, so it stays one segment. In the code, a `Seg` is one step of a path: a `SegInfo` (segment id and name) and the parameter value if fixed. | — |
| **parameter** | The typed slot on a topic: `id(Uuid)`. | param (in prose) |
| **fixed** | A parameter given a value in a path: `id(alice)`. | — |
| **wildcard** | A parameter left open in a path: `id(Wild)`, shown as `*`. | any, `Any` |
| **params** | The tuple `P` of wildcard values collected on a channel: `bus.user().id(Wild).delete()` has `P = (Uuid,)`, read with `msg.params()`. | — |
| **prelude** | The module `topics::prelude`, which brings every `Steps` trait of a tree into scope. | — |

## Messages

| Term | Meaning | Don't say |
|---|---|---|
| **event** | What a publish delivers: the message and the path it was published on, which gives its subject. Held as an `Event`, an `Arc<dyn Envelope>`. | — |
| **message** | What a publisher passes to `publish`: a leaf's message type. A subscriber reads it through a `Msg`. | item |
| **view** | A message as a subscriber's topic shows it, from `msg.view()` and in filters: `&GitPushMsg` on `git.push`, a `git::TopicRef` on `git`. | — |
| **payload** | Only the message of a request leaf, as sent by a requester: the `GitFetchMsg` of `fetch`. | — |
| **subject** | The string of a concrete event: `git.push`, `user.id.42.delete`. Built by `msg.subject()`. | — |
| **pattern** | What a channel or subscription covers, with `*` for wildcards and `>` for everything below: `user.id.*.>`. | subject (for wildcards) |

## Channels

| Term | Meaning | Don't say |
|---|---|---|
| **bus** | The shared state behind every channel that comes from one `Channel::new()`. Two `Channel::new()` are two buses. | core |
| **channel** | A typed view `Channel<T, P>` on part of a tree, which publishes and subscribes relative to it. | — |
| **closed** | A bus whose last channel was dropped, or that `Channel::close` closed. Its subscriptions end once they have read their buffers. | shut down, dead |
| **sub-channel** | A channel taken from another one with steps: `bus.git()`. | — |
| **path** | The steps from the root to a channel, `user().id(alice).delete()`: a `Node` (its `Seg`s, flat, and which are wildcards), and the keys it stands for in the interest trie (`[Key]`). An event's subject is the `Node` of the channel it was published on. | route |
| **step** | One move down one level: a method of a `Steps` trait, which adds one `Seg`. `bus.git().push()` takes two steps. | hop |

## Subscriptions

| Term | Meaning | Don't say |
|---|---|---|
| **subscription** | The registration made by awaiting `subscribe()`, and its handle `Subscription`, a `Stream` of messages. Dropping it unsubscribes. A responder and a handler are subscriptions too. | — |
| **subscriber** | The code that reads a subscription. | consumer, receiver |
| **handler** | A subscription made with `.handler(f)`: `f` gets each message on the publisher's thread, without a buffer. Its handle is a `Handler`. | callback |
| **buffer** | A subscription's bounded storage for messages it hasn't read yet: `.buffer(256)`. | queue (for buffers) |
| **deliver** | Push an event into a buffer, or pass it to a handler. `publish` and `publish_many` return how many deliveries were made. | queued (as the outcome) |
| **batch** | Several messages moved under one lock: `publish_many`, `recv_many`. | chunk |
| **filter** | A user predicate on params and view: `.filter(\|(id,), _\| *id != admin)`. | — |
| **overflow policy** | What a full buffer does with a new message: `Overflow`, set with `.on_full(…)`. | — |
| **delivery defaults** | The buffer size and overflow policy a topic's subscriptions start with, declared in the tree with `#[buffer(n)]` and `#[on_full(…)]` and inherited by the topics below: `Topic::BUFFER`, `Topic::ON_FULL`. | default policy, QoS |

## Queue groups

| Term | Meaning | Don't say |
|---|---|---|
| **queue group** | One value of a group enum, passed to `.queue(…)`: `PushQueue::Indexer(0)`. Each message goes to one member per queue group. | — |
| **group enum** | The enum generated from `[ … ]` after a topic's name and parameter: `Queue` in a level's module, `NameQueue` for a leaf (`git::PushQueue`). It implements `QueueGroup`, and `InScope<T>` for the `T` of the topic's channels, so it is joined only there. | scope |
| **member** | A subscription in a queue group. | — |

## Request/reply

| Term | Meaning | Don't say |
|---|---|---|
| **publisher** | Who publishes: calls `publish` or `publish_many` on a channel. | sender |
| **requester** | Who sends a request: calls `request` or `scatter`. | sender |
| **responder** | A subscription made with `respond()` on a request leaf, and its handle `Responder`, a `Stream` of incoming requests. Only responders count for and answer requests. | receiver |
| **incoming request** | What a responder receives: `Incoming`, which derefs to the payload and carries the responder's own **reply token**, used once by `respond(reply)`. `try_into_inner` splits it into the payload and the token, a `ReplyToken`. | — |

## Bridges

| Term | Meaning | Don't say |
|---|---|---|
| **bridge** | A `Bridge`: a channel of a bus extended over a byte transport to the same channel of another bus. Behind the `bridge` feature. | link, route (for bridges) |
| **codec** | The user's `Codec<M>` implementation, which encodes messages and parameter values. A topic `T` whose messages a codec `C` handles implements `Wire<C>`. | serializer |
| **frame** | One unit sent over a bridge's transport: a tag and a body (hello, interest, message). | packet |
| **frame version** | `FRAME_VERSION`, the format of frames, exchanged in the hello. The only mismatch that refuses a connection. | protocol version |
| **interest** | What a side's subscriptions cover, sent to the other side as patterns of segment ids, fixed parameters widened to `*`. | — |
| **export** | The one subscription a bridge makes on its channel, matched by the other side's interest. | — |
| **imported** | An event received from a bridge. No bridge sends it on. | — |

## Internals

| Term | Meaning | Don't say |
|---|---|---|
| **key** | A step as stored in the interest trie: `Key` is a segment id, a name (for patterns) and a `KeyParam` (`None`, `Wild` or `Is(ParamKey)`). | edge |
| **interest trie** | `Interest`, the index of subscriptions by path, like the NATS sublist. Its nodes are **trie nodes**. | graph, interest graph, sublist |
| **matcher** | The internal boxed closure that combines a subscription's filters (or a bridge's interest). Subscriptions without filters have none. | — |
| **watcher** | A bridge's view of the bus's subscriptions: told each subscription and unsubscription (`Change`), except of responders and exports. | — |

The internal types, by term:

| Term | Type |
|---|---|
| bus | `Bus` (`bus.rs`), held by channels through a `Handle` (`channel.rs`) that closes it on drop (or `Channel::close`) |
| buffer | `Buffer`, or for a handler a `Buffer` with a `HandlerFn` |
| subscription, in the trie | `Sub` |
| queue group, at compile time | `QueueGroup`, `InScope<T>` (`group.rs`) |
| queue group, in the trie | `Members`, sharing a `RoundRobin` position, keyed by `GroupId` (the `GroupKey` and the segment ids of the topic's path) |
| interest trie | `Interest`, children in `Edges { plain, wild, fixed }` |
| key | `Key`, `KeyParam`, `ParamKey` |
| matcher | `Matcher` |
| event | `Event` (`Arc<dyn Envelope>`), `Env<M>` for a published message |
| path, segment | `Node`, `Path`, `Seg`, `SegInfo` (`nav.rs`); `Shape`, what a message keeps of its channel's path |
| topic | `Topic`, `Level`, `Leaf`, `Publish`, `Request` (`tree.rs`) |
| responder, in the bus | `Answering<R>` (the event of one responder: the request's event and its reply token), `Reply` (the token, public as `ReplyToken`) |
| step | `Steps` (generated), `Param` (`Wild` or a value), `Channel::__plain` / `__param` |
| bridge | `Bridge`, `Export`, `Patterns` (the other side's interest) in `bridge.rs`; `Codec`, `Wire` in `wire.rs` |
| watcher | `Change`, `Bus::watch` |
