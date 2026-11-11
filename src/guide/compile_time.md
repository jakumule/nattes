Compile-time checks: every mistake here is a build error.

Each example below fails to compile, with the error quoted under it. They are
tested: these guarantees are part of the API.

# Paths

A typo in the path:

```compile_fail,E0599
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git { push: PushMsg }, user { new: PushMsg } } }
# use topics::prelude::*;
# fn main() {
# let bus = Channel::<topics::Topic>::new();
bus.user().push();
// error: no method named `push` found for struct `Channel<T, P>`
# }
```

A path that isn't below the channel:

```compile_fail,E0599
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git { push: PushMsg }, user { new: PushMsg } } }
# use topics::prelude::*;
# fn main() {
# let bus = Channel::<topics::Topic>::new();
let users = bus.user();
users.git();
// error: no method named `git` found for struct `Channel<T, P>`
# }
```

A parameter takes a value of its type, or [`Wild`](crate::Wild):

```compile_fail,E0277
# use nattes::{topics, Channel};
# pub struct DeleteMsg;
# topics! { topics { user { id(u32) { delete: DeleteMsg } } } }
# use topics::prelude::*;
# fn main() {
# let bus = Channel::<topics::Topic>::new();
bus.user().id("alice");
// error: `&str` can't select a `u32` parameter
//        expected `Wild` or a `u32` value
# }
```

# Publishing

The wrong message for the topic:

```compile_fail,E0308
# use nattes::{topics, Channel};
# pub struct PushMsg;
# pub struct NewMsg;
# topics! { topics { git { push: PushMsg }, user { new: NewMsg } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().push().publish(NewMsg).await;
// error: expected `PushMsg`, found `NewMsg`
# }
# fn main() {}
```

Publishing on a wildcard channel, which has no value for the `*`:

```compile_fail,E0599
# use nattes::{topics, Channel, Wild};
# pub struct DeleteMsg;
# topics! { topics { user { id(u32) { delete: DeleteMsg } } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.user().id(Wild).delete().publish(DeleteMsg).await;
// error: no method named `publish` found for struct `Channel<DeleteLeaf, (u32,)>`
# }
# fn main() {}
```

Publishing on a level, which only subscribes:

```compile_fail,E0277
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git { push: PushMsg } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().publish(PushMsg).await;
// error: can't publish on `git::Topic`
# }
# fn main() {}
```

Publishing on a request leaf, which takes requests only:

```compile_fail,E0277
# use nattes::{topics, Channel};
# pub struct FetchMsg;
# pub struct FetchAck;
# topics! { topics { git { fetch: FetchMsg => FetchAck } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().fetch().publish(FetchMsg).await;
// error: can't publish on `FetchLeaf`
# }
# fn main() {}
```

# Declaring the tree

Two siblings with the same name:

```compile_fail
# use nattes::topics;
# pub struct PushMsg;
topics! { topics { git { push: PushMsg, push: PushMsg } } }
// error: `push` is already a topic of this level
# fn main() {}
```

A name that is a method of [`Channel`](crate::Channel):

```compile_fail
# use nattes::topics;
# pub struct Msg;
topics! { topics { subscribe: Msg } }
// error: `subscribe` can't be a topic name: channels have a method `subscribe`
# fn main() {}
```

A parameter type that can't be compared exactly (parameters are matched by
value, so their type must be `Eq + Hash`):

```compile_fail,E0277
# use nattes::topics;
# pub struct Reading;
topics! { sensors { reading(f64): Reading } }
// error: `f64` can't be a parameter
# fn main() {}
```

A delivery default that isn't an [`Overflow`](crate::Overflow) variant:

```compile_fail,E0599
# use nattes::topics;
# pub struct Job;
topics! { app { #[on_full(Wiat)] jobs { run: Job } } }
// error: no variant, associated function, or constant named `Wiat` found for enum `Overflow`
# fn main() {}
```

or an empty buffer:

```compile_fail
# use nattes::topics;
# pub struct Job;
topics! { app { #[buffer(0)] jobs { run: Job } } }
// error: buffer size must be at least 1
# fn main() {}
```

# Queue groups

A queue group named with a string instead of declared:

```compile_fail,E0277
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git { push: PushMsg } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().push().subscribe().queue("indexers").await;
// error: `&str` isn't a queue group
//        declare queue groups in `topics!` with `[ … ]` after a topic's name
# }
# fn main() {}
```

A queue group of another topic (`git [Mailer]`, joined on `git.push`):

```compile_fail,E0277
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git [Mailer] { push: PushMsg } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().push().subscribe().queue(topics::git::Queue::Mailer).await;
// error: queue group `Queue` isn't declared on `PushLeaf`
//        a queue group is joined on the topic that declares it
# }
# fn main() {}
```

A wrong value in a queue group (`push [Indexer(u32)]`):

```compile_fail,E0308
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git { push [Indexer(u32)]: PushMsg } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().push().subscribe().queue(topics::git::PushQueue::Indexer("0")).await;
// error: mismatched types: expected `u32`, found `&str`
# }
# fn main() {}
```

# Request/reply

A request on a leaf declared without a reply (`push: PushMsg`):

```compile_fail,E0599
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git { push: PushMsg } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().push().request(PushMsg).await;
// error: the method `request` exists for struct `Channel<PushLeaf>`,
//        but its trait bounds were not satisfied
# }
# fn main() {}
```

or a responder on it:

```compile_fail,E0277
# use nattes::{topics, Channel};
# pub struct PushMsg;
# topics! { topics { git { push: PushMsg } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
bus.git().push().respond().await;
// error: `PushLeaf` isn't a request leaf
# }
# fn main() {}
```

Answering twice (`request` is an [`Incoming`](crate::Incoming)):

```compile_fail,E0382
# use futures::StreamExt;
# use nattes::{topics, Channel};
# pub struct FetchMsg;
# pub struct FetchAck;
# topics! { topics { git { fetch: FetchMsg => FetchAck } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
let mut responder = bus.git().fetch().respond().await;
let request = responder.next().await.unwrap();
request.respond(FetchAck);
request.respond(FetchAck);
// error: use of moved value: `request`
# }
# fn main() {}
```

Answering from a subscription, which sees requests but isn't a responder:

```compile_fail,E0599
# use futures::StreamExt;
# use nattes::{topics, Channel};
# pub struct FetchMsg;
# pub struct FetchAck;
# topics! { topics { git { fetch: FetchMsg => FetchAck } } }
# use topics::prelude::*;
# async fn f() {
# let bus = Channel::<topics::Topic>::new();
let mut sub = bus.git().fetch().subscribe().await;
sub.next().await.unwrap().respond(FetchAck);
// error: no method named `respond` found for struct `Msg`
# }
# fn main() {}
```

A handler on a request leaf: responders answer from their stream.

```compile_fail,E0599
# use nattes::{topics, Channel};
# pub struct FetchMsg;
# pub struct FetchAck;
# topics! { topics { git { fetch: FetchMsg => FetchAck } } }
# use topics::prelude::*;
# fn main() {
# let bus = Channel::<topics::Topic>::new();
let _handler = bus.git().fetch().respond().handler(|_| {});
// error: no method named `handler` found for struct `SubscriptionBuilder<FetchLeaf, (), Respond>`
# }
```

# Bridges

A codec that misses a message or parameter type of the bridged tree: see
[bridges](crate::guide::bridges).
