Navigating the tree: channels, steps and wildcards.

[`Channel::<topics::Topic>::new()`](crate::Channel::new) creates a bus rooted
at the `topics` module. Each call creates a separate bus, so create it once and
clone it or go down from it.

Each step is a method, and a path reads like the subject:

```
# use nattes::{topics, Channel};
# use uuid::Uuid;
# pub struct GitPushMsg;
# pub struct UserUpdateMsg;
# pub struct UserDeleteMsg;
# topics! {
#     topics {
#         git { push: GitPushMsg },
#         user { id(Uuid) { update: UserUpdateMsg, delete: UserDeleteMsg } },
#     }
# }
use nattes::Wild;
use topics::prelude::*;

# fn main() {
# let alice_id = Uuid::now_v7();
let bus = Channel::<topics::Topic>::new();

let git           = bus.git();                          // git
let pushes        = bus.git().push();                   // git.push
let alice         = bus.user().id(alice_id);            // user.id.<alice>
let alice_deletes = bus.user().id(alice_id).delete();   // user.id.<alice>.delete
let any_user      = bus.user().id(Wild);                // user.id.*
let all_deletes   = bus.user().id(Wild).delete();       // user.id.*.delete
let any_delete    = any_user.delete();                  // user.id.*.delete, from any_user
# let _: &Channel<topics::user::id::DeleteLeaf, (Uuid,)> = &all_deletes;
# let _: &Channel<topics::user::id::DeleteLeaf, (Uuid,)> = &any_delete;
# let _: (&Channel<topics::git::Topic>, &Channel<topics::git::PushLeaf>) = (&git, &pushes);
# let _: (&Channel<topics::user::id::Topic>, &Channel<topics::user::id::DeleteLeaf>) = (&alice, &alice_deletes);
# }
```

- **The channel is borrowed, not consumed,** so you can keep using `bus` and
  take as many sub-channels from it as you like. They all share the same bus.
- **Every step is checked by the compiler.** A typo such as `bus.user().push()`
  fails with "no method named `push` found for struct `Channel<T, P>`" (see
  [compile-time checks](crate::guide::compile_time)).
- **A parameter is fixed, `id(alice_id)`, or a wildcard,
  [`id(Wild)`](crate::Wild).** Each wildcard adds its type to the channel:
  `all_deletes` is a `Channel<topics::user::id::DeleteLeaf, (Uuid,)>`. Messages
  received on it carry the actual values in
  [`msg.params()`](crate::Msg::params).

# Coming from NATS

| NATS | nattes |
|---|---|
| `>` | `bus.subscribe()` |
| `git.>` | `bus.git().subscribe()` |
| `git.push` | `bus.git().push().subscribe()` |
| `user.id.<alice>.>` | `bus.user().id(alice).subscribe()` |
| `user.id.*.delete` | `bus.user().id(Wild).delete().subscribe()` |
| queue group | `.subscribe().queue(git::Queue::Mailer)` |
| request/reply | `channel.request(msg)` / `channel.respond()`, then `req.respond(reply)` |
| scatter-gather | `channel.scatter(msg)` (a stream of replies) |

# Sub-channels as roots

A channel's type only mentions the part of the tree it points to. A component
that takes a `Channel<other::Topic>` doesn't know, and doesn't need to know,
whether it's connected to its own bus or mounted inside a bigger one:

```
# use nattes::{topics, Channel, Wild};
# #[derive(Debug)] pub struct StringEvent;
topics! {
    #[derive(Debug)]
    other {
        text(String): StringEvent,
    }
}
# topics! { topics { other = other } }
# use topics::prelude::*;

async fn other_service(other: Channel<other::Topic>) {
    use other::prelude::*;
    let mut texts = other.text(Wild).subscribe().await;
    // …
#   let _ = &mut texts;
}

# fn main() {
# futures::executor::block_on(async {
# let bus = Channel::<topics::Topic>::new();
// standalone
other_service(Channel::<other::Topic>::new()).await;

// mounted with `other = other` in topics
other_service(bus.other()).await;
# });
# }
```

# Passing channels around

To write a function that subscribes anywhere in the tree, take a
`&Channel<T, P>`. The root channel and any sub-channel work the same:

```
# use futures::StreamExt;
# use nattes::{topics, Channel, Wild};
# pub struct GitPushMsg;
# pub struct UserDeleteMsg;
# topics! {
#     topics {
#         git { push: GitPushMsg },
#         user { id(u32) { delete: UserDeleteMsg } },
#     }
# }
# use topics::prelude::*;
use nattes::Topic;

async fn log_all<T: Topic, P: 'static>(ch: &Channel<T, P>) {
    let mut sub = ch.subscribe().await;
    while let Some(msg) = sub.next().await {
        println!("{}", msg.subject());
    }
}

# async fn demo(bus: Channel<topics::Topic>) {
log_all(&bus).await;                     // >
log_all(&bus.git()).await;               // git.>
log_all(&bus.user().id(Wild)).await;     // user.id.*.>
# }
# fn main() {}
```

Fix `T` to accept only one part of the tree, for example
`&Channel<topics::git::Topic>` or `&Channel<topics::git::PushLeaf>`. Leave `P`
generic to also accept wildcard channels (add [`P: Params`](crate::Params) to
read `msg.params()`), or leave it out (`P = ()`) to require a channel that can
publish.

Take the channel by value instead when the function keeps it, for example to
move it into a spawned task. Clones are cheap.
