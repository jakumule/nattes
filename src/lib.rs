//! Typesafe subject-based message queue inspired by NATS.
//!
//! Declare the tree with [`events!`](crate::events), then walk down it with
//! [`topic!`](crate::topic) to get a [`Channel`] on any part of it. A channel
//! is a bus rooted at that part: hand a `Channel<events::git::Topic>` to a
//! component and it publishes and subscribes relative to `git`.
//!
//! ```
//! use futures::StreamExt;
//! use nattes::channel;
//! use nattes::{events, topic};
//!
//! pub struct PushMsg;
//! pub struct DeleteMsg;
//!
//! events! {
//!     pub mod events {
//!         git { push: PushMsg },
//!         user { id(u32) { delete: DeleteMsg } },
//!     }
//! }
//!
//! # fn main() {
//! # futures::executor::block_on(async {
//! let bus = channel!(events);
//! let git = topic!(bus, git);                       // git
//! let pushes = topic!(bus, git.push);               // git.push
//! let alice_deletes = topic!(bus, user.id(7).delete); // user.7.delete
//!
//! let mut git_sub = git.subscribe().await;                     // git.>
//! let mut deletes = topic!(bus, user.id().delete).subscribe().await; // user.*.delete
//!
//! pushes.publish(PushMsg).await;
//! alice_deletes.publish(DeleteMsg).await;
//!
//! assert!(matches!(*git_sub.next().await.unwrap(), events::git::Topic::Push(_)));
//! assert_eq!(deletes.next().await.unwrap().params(), (7,));
//! # });
//! # }
//! ```
//!
//! A channel with a wildcard can't publish:
//!
//! ```compile_fail,E0599
//! # use nattes::channel;
//! # use nattes::{events, topic};
//! # pub struct DeleteMsg;
//! # events! { pub mod events { user { id(u32) { delete: DeleteMsg } } } }
//! # async fn f() {
//! let bus = channel!(events);
//! topic!(bus, user.id().delete).publish(DeleteMsg).await;
//! # }
//! # fn main() {}
//! ```
//!
//! and each path must start where the channel is:
//!
//! ```compile_fail,E0599
//! # use nattes::channel;
//! # use nattes::{events, topic};
//! # pub struct PushMsg;
//! # events! { pub mod events { git { push: PushMsg }, user { new: PushMsg } } }
//! # fn main() {
//! let users = topic!(channel!(events), user);
//! topic!(users, git.push);
//! # }
//! ```
//!
//! Parameters are routed by value, so their type must be `Eq + Hash`; floats
//! can't be parameters:
//!
//! ```compile_fail,E0277
//! # use nattes::events;
//! # pub struct Reading;
//! events! { pub mod sensors { reading(f64): Reading } }
//! # fn main() {}
//! ```
//!
//! Queue groups are declared on a topic, with `queue Name { … }` in a subtree
//! or `where queue Name { … }` after a leaf. Each value is a group, so
//! `Indexer(0)` and `Indexer(1)` are two groups:
//!
//! ```
//! # use nattes::{channel, events, topic};
//! # pub struct PushMsg;
//! events! {
//!     pub mod events {
//!         git {
//!             push: PushMsg where queue Pushers { Indexer(u32), Audit },
//!             queue Workers { Mailer },
//!         },
//!     }
//! }
//! use events::git::{Pushers, Workers};
//!
//! # fn main() {
//! # futures::executor::block_on(async {
//! let bus = channel!(events);
//! let pushes = topic!(bus, git.push);
//! let _a = pushes.subscribe().queue(Pushers::Indexer(0)).await;
//! let _b = pushes.subscribe().queue(Pushers::Indexer(0)).await;
//! let _c = pushes.subscribe().queue(Pushers::Indexer(1)).await;
//! let _m = topic!(bus, git).subscribe().queue(Workers::Mailer).await; // git.>
//!
//! // one of _a and _b, _c, and _m
//! assert_eq!(pushes.publish(PushMsg).await, 3);
//! # });
//! # }
//! ```
//!
//! A group can only be joined on the topic that declares it, not above or
//! below:
//!
//! ```compile_fail,E0277
//! # use nattes::{channel, events, topic};
//! # pub struct PushMsg;
//! # events! { pub mod events { git { push: PushMsg, queue Workers { Mailer } } } }
//! # async fn f() {
//! let bus = channel!(events);
//! topic!(bus, git.push).subscribe().queue(events::git::Workers::Mailer).await;
//! # }
//! # fn main() {}
//! ```
//!
//! and groups must be declared, not named with a string:
//!
//! ```compile_fail,E0277
//! # use nattes::{channel, events, topic};
//! # pub struct PushMsg;
//! # events! { pub mod events { git { push: PushMsg } } }
//! # async fn f() {
//! let bus = channel!(events);
//! topic!(bus, git.push).subscribe().queue("workers").await;
//! # }
//! # fn main() {}
//! ```
//!
//! Their values are typed like parameters:
//!
//! ```compile_fail,E0308
//! # use nattes::{channel, events, topic};
//! # pub struct PushMsg;
//! # events! { pub mod events { git { push: PushMsg where queue Pushers { Indexer(u32) } } } }
//! # async fn f() {
//! let bus = channel!(events);
//! topic!(bus, git.push).subscribe().queue(events::git::Pushers::Indexer("0")).await;
//! # }
//! # fn main() {}
//! ```

mod bus;
mod channel;
mod group;
mod log;
mod macros;
mod nav;
mod request;
mod subscription;
#[cfg(test)]
mod tests;
mod tree;

pub use bus::{Overflow, DEFAULT_BUFFER};
pub use channel::Channel;
pub use group::{InScope, QueueGroup};
#[doc(hidden)]
pub use nav::{Append, AppendParams, ParamStep, Plain, Prism};
pub use nav::{IntoChannel, Is, Navigable, Param, Wild};
pub use request::{BusError, Replies, Request};
pub use subscription::{Msg, Subscription, SubscriptionBuilder};
#[doc(hidden)]
pub use tree::Step;
pub use tree::{subject, Node, ParamValue};

#[doc(hidden)]
pub use pastey as __pastey;
