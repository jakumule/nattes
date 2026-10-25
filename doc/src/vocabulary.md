# Vocabulary

One term per concept, used everywhere: in the README, the API docs, the code
and its comments. Don't introduce synonyms. When a concept is added or renamed,
update this page first.

The examples use this tree:

```rust
topics! {
    topics {
        git [Mailer] {
            push [Indexer(u32)]: GitPushMsg,
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
| **topic** | One named place in the tree: a level or a leaf. `git`, `git.push` and `user.id` are topics. | node, entry (in user docs) |
| **level** | A topic with children. It becomes a module with a `Topic` enum and a `Steps` type, plus a group enum `Queue` if it declares queue groups. `git` and `user.id` are levels. The top level, `topics`, is the **root**. | subtree, node |
| **leaf** | A topic carrying a message type: `push: GitPushMsg`. | — |
| **request leaf** | A leaf declared with a reply type, `fetch: GitFetchMsg => FetchAck`. It holds its message like any leaf, and its channel takes requests only: no `publish`. | — |
| **chain** | `a.b.c`, a shorthand for `a { b { c } }`: `random.arrow.more: DeepMsg`. | — |
| **mount** | `name = module`: the root of another tree, as a level of this one. `other = other`. | — |
| **name** | A topic's identifier as written in `topics!`: `push`, `r#type`. It gives the segment, the module and method names, and, in camel case, the `Topic` variant. | — |
| **segment** | One part of a subject or pattern, separated by dots: a name or a parameter value. `user.id.42.delete` has four. | — |
| **parameter** | The typed slot on a topic: `id(Uuid)`. | param (in prose) |
| **fixed** | A parameter given a value in a path: `id(alice)`, selected with `Is`. | — |
| **wildcard** | A parameter left open in a path: `id()` or `id(_)`, selected with `Wild`, shown as `*`. | any, `Any` |
| **params** | The tuple `P` of wildcard values collected on a channel: `at!(bus, user.id().delete)` has `P = (Uuid,)`, read with `msg.params()`. | — |

## Messages

| Term | Meaning | Don't say |
|---|---|---|
| **event** | The root value that a publish delivers: a `topics::Topic`, held as an `Arc<dyn Level>`. Publishing on `git.push` builds `Topic::Git(git::Topic::Push(msg))`. | — |
| **message** | The part of an event that a channel sees: its `T`. On `at!(bus, git)` the message is a `git::Topic`; on `at!(bus, git.push)` a `GitPushMsg`. | item, payload |
| **payload** | Only the message of a request leaf, as sent by a requester: the `GitFetchMsg` of `fetch`. | — |
| **subject** | The string of a concrete event: `git.push`, `user.id.42.delete`. Built by `subject()` and `msg.subject()`. | — |
| **pattern** | What a channel or subscription covers, with `*` for wildcards and `>` for everything below: `user.id.*.>`. | subject (for wildcards) |

## Channels

| Term | Meaning | Don't say |
|---|---|---|
| **bus** | The shared state behind every channel that comes from one `Channel::new()`. Two `Channel::new()` are two buses. | core |
| **channel** | A typed view `Channel<T, P, S>` on part of a tree, which publishes and subscribes relative to it. | — |
| **closed** | A bus whose last channel was dropped. Its subscriptions end once they have read their buffers. | shut down, dead |
| **sub-channel** | A channel taken from another one with `at!`: `at!(bus, git)`. | — |
| **path** | The argument of `at!`, `user.id(alice).delete`, and the keys it stands for in the interest trie (`Vec<Key>`). | route (for keys) |
| **route** | The typed chain of `Root` and `Hop`s behind a channel, which projects an event to its message and injects a message into an event. | — |
| **step** | One move down one level: a `Prism`, a `Step` of a `Level`, a method of `Steps`. `at!(bus, git.push)` takes two steps. | hop (except the struct `Hop`) |

## Subscriptions

| Term | Meaning | Don't say |
|---|---|---|
| **subscription** | The registration made by awaiting `subscribe()`, and its handle `Subscription`, a `Stream` of messages. Dropping it unsubscribes. A responder is a subscription too, made with `respond()`. | — |
| **subscriber** | The code that reads a subscription. | consumer, receiver |
| **buffer** | A subscription's bounded storage for messages it hasn't read yet: `.buffer(256)`. | queue (for buffers) |
| **deliver** | Push an event into a buffer. `publish` and `try_publish` return how many deliveries were made. | queued (as the outcome) |
| **filter** | A user predicate on params and message: `.filter(\|(id,), _\| *id != admin)`. | — |
| **overflow policy** | What a full buffer does with a new message: `Overflow`, set with `.on_full(…)`. | — |

## Queue groups

| Term | Meaning | Don't say |
|---|---|---|
| **queue group** | One value of a group enum: `PushQueue::Indexer(0)`. Each message goes to one member per queue group. | — |
| **group enum** | The enum generated for `[ … ]`: `Queue` in a level's module (`git::Queue`), `<Leaf>Queue` for a leaf (`git::PushQueue`). | — |
| **member** | A subscription in a queue group, joined with `.queue(…)`. | — |
| **scope** | The type `S` of a channel, which tells which group enums it can join: the level's `Topic` enum, or a leaf's group enum. A request leaf without groups gets an empty enum `<Leaf>Scope` (`git::FetchScope`). On a request leaf, the scope implements `Answer`, which gives the reply type; on any other topic, `Publish`. `at!(bus, git.push)` has the scope `git::PushQueue`. | topic (for `S`) |

## Request/reply

| Term | Meaning | Don't say |
|---|---|---|
| **publisher** | Who publishes: calls `publish` (or `try_publish`) on a channel. | sender |
| **requester** | Who sends a request: calls `request` or `scatter`. | sender |
| **responder** | A subscription made with `respond()` on a request leaf, and its handle `Responder`, a `Stream` of incoming requests. Only responders count for and answer requests. | receiver |
| **incoming request** | What a responder receives: `Incoming`, which derefs to the payload and carries the responder's own **reply token**, used once by `respond(reply)`. `try_into_inner` splits it into the payload and the token, a `ReplyToken`. | — |

## Internals

| Term | Meaning | Don't say |
|---|---|---|
| **key** | A step as stored in the interest trie: `Key` is a name and a `KeyParam` (`None`, `Wild` or `Is(ParamKey)`). | edge |
| **interest trie** | `Interest`, the index of subscriptions by path, like the NATS sublist. Its nodes are **trie nodes**. | graph, interest graph, sublist |
| **matcher** | The internal boxed closure that combines a subscription's filters. Subscriptions without filters have none. | — |

The internal types, by term:

| Term | Type |
|---|---|
| bus | `Bus` (`bus.rs`), held by channels through a `Handle` (`channel.rs`) that closes it on drop |
| buffer | `Buffer` |
| subscription, in the trie | `Sub` |
| queue group, in the trie | `Members`, sharing a `RoundRobin` position, keyed by `GroupKey` |
| interest trie | `Interest`, children in `Edges { plain, wild, fixed }` |
| key | `Key`, `KeyParam`, `ParamKey` |
| matcher | `Matcher` |
| event | `Event` (`Arc<dyn Level>`) |
| level | `Level` (the trait every `Topic` enum implements) |
| route | `Route`, `Root`, `Hop` |
| responder, in the bus | `Answering<R>` (the event of one responder: the request's event and its reply token), `Reply` (the token, public as `ReplyToken`) |
| step | `Prism` (`Plain`, `ParamStep`), `Step`, `Steps` |
| scope | `S`, `InScope<S>`, `Answer<M>`, `Publish<T>` |
