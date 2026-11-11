Queue groups: each message goes to one member of the group.

Queue groups are declared on a topic in [`topics!`](crate::topics), with
`[ … ]` right after its name and parameter: `git [Mailer] { … }`,
`push [Indexer(u32)]: GitPushMsg`, `fetch [Pool]: GitFetchMsg => FetchAck`,
`repo.id(u64) [Workers].delete: Msg`, or on the root, `topics [Workers] { … }`.
They become a group enum, `Queue` in a level's module and `PushQueue` for the
leaf `push`, deriving `Clone`, `PartialEq`, `Eq` and `Hash`. Inner attributes go
on the enum (`[#![derive(Debug)] Mailer]`), and doc comments on a variant
document it. A mount takes no groups: its tree declares its own.

Each value of a group enum is one queue group, so `Indexer(0)` and `Indexer(1)`
are two groups. Subscribers that join the same group, with
[`.queue(group)`](crate::SubscriptionBuilder::queue), share its messages: each
one goes to a single member.

```
# use nattes::{topics, Channel};
# pub struct GitPushMsg { pub branch: String }
topics! {
    topics {
        git [Mailer] {
            push [Indexer(u32)]: GitPushMsg,
        },
    }
}
use topics::git::{PushQueue, Queue};
use topics::prelude::*;

# fn main() {
# futures::executor::block_on(async {
let bus = Channel::<topics::Topic>::new();
let pushes = bus.git().push();
let a = pushes.subscribe().queue(PushQueue::Indexer(0)).await; // shard 0
let b = pushes.subscribe().queue(PushQueue::Indexer(0)).await; // shard 0
let c = pushes.subscribe().queue(PushQueue::Indexer(1)).await; // shard 1
let m = bus.git().subscribe().queue(Queue::Mailer).await;      // git.>

// one of a and b, then c, then m
let delivered = pushes.publish(GitPushMsg { branch: "main".into() }).await;
assert_eq!(delivered, 3);
# drop((a, b, c, m));
# });
# }
```

- **Who gets it.** A member waiting in `next()` is picked first. If none is
  waiting, members with room in their buffer take turns (round robin). When
  every member's buffer is full, the group waits if any member has
  [`Overflow::Wait`](crate::Overflow::Wait), and the message goes to the first
  of them to have room. Otherwise the overflow policy of the member whose turn
  it is applies.
- **A group belongs to its topic.** It is joined only on the topic that
  declares it, not above or below (checked at
  [compile time](crate::guide::compile_time#queue-groups)), and each mount of a
  tree has its own groups (with `a = other` and `b = other`, members on `a.x`
  and on `b.x` don't share turns).
- **Parameters don't change the topic.** Members on `user.id(alice)` and on
  `user.id(Wild)` joining the same value form one group.
- **Responders and subscribers** joining the same group form two separate
  groups.
- **Fields of a group variant** must implement
  `Clone + Eq + Hash + Send + Sync + 'static`.
