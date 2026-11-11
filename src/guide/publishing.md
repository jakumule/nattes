Publishing: `publish` and `publish_many`.

A leaf channel without wildcards can publish its message. Levels only
subscribe, and request leaves take requests only (see
[request/reply](crate::guide::request_reply)).

```
# use nattes::{topics, Channel};
# pub struct GitPushMsg { pub branch: String }
# pub struct UserDeleteMsg;
# topics! {
#     topics {
#         git { push: GitPushMsg },
#         user { id(u32) { delete: UserDeleteMsg } },
#     }
# }
# use topics::prelude::*;
# fn main() {
# futures::executor::block_on(async {
# let bus = Channel::<topics::Topic>::new();
# let _all = bus.subscribe().await;
let pushes = bus.git().push();                // git.push
let alice_deletes = bus.user().id(7).delete(); // user.id.7.delete

pushes.publish(GitPushMsg { branch: "main".into() }).await;
let delivered = alice_deletes.publish(UserDeleteMsg).await;
# assert_eq!(delivered, 1);
# });
# }
```

[`publish`](crate::Channel::publish) returns the number of subscriptions the
message was delivered to (a bridge counts as one). A channel with a wildcard has
no `publish` method, because there would be no value to fill the `*` with.

`publish` only waits if a matching subscriber chose
[`Overflow::Wait`](crate::Overflow::Wait) and its buffer is full. Even then,
every other subscriber has already received the message by the time it waits.
See [delivery guarantees](crate::guide::delivery) for when a message can be
lost and in which order it arrives.

# Batches

[`publish_many`](crate::Channel::publish_many) publishes a batch, in order, for
less than one `publish` per message: on a leaf, the subscriptions are looked up
once and each buffer takes the whole batch under one lock.

```
# use nattes::{topics, Channel};
# pub struct GitPushMsg { pub branch: String }
# topics! { topics { git { push: GitPushMsg } } }
# use topics::prelude::*;
# fn main() {
# futures::executor::block_on(async {
# let bus = Channel::<topics::Topic>::new();
# let pushes = bus.git().push();
# let _sub = pushes.subscribe().await;
let branches = vec!["main".to_string(), "dev".to_string()];
let delivered = pushes
    .publish_many(branches.into_iter().map(|branch| GitPushMsg { branch }))
    .await;
# assert_eq!(delivered, 2);
# });
# }
```

To publish without `async`, see the [recipes](crate::guide::recipes).
