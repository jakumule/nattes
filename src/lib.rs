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
//!     events {
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
//! # events! { events { user { id(u32) { delete: DeleteMsg } } } }
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
//! # events! { events { git { push: PushMsg }, user { new: PushMsg } } }
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
//! events! { sensors { reading(f64): Reading } }
//! # fn main() {}
//! ```
//!
//! Names are unique among siblings:
//!
//! ```compile_fail
//! # use nattes::events;
//! # pub struct PushMsg;
//! events! { events { git { push: PushMsg, push: PushMsg } } }
//! # fn main() {}
//! ```
//!
//! Queue groups are declared on a topic, with `[ … ]` right after its name:
//! `git [Mailer] { … }`, `push [Indexer(u32)]: Msg`. Each value is a group, so
//! `Indexer(0)` and `Indexer(1)` are two groups:
//!
//! ```
//! # use nattes::{channel, events, topic};
//! # pub struct PushMsg;
//! events! {
//!     events {
//!         git [Mailer] {
//!             push [Indexer(u32), Audit]: PushMsg,
//!         },
//!     }
//! }
//! use events::git::{PushQueue, Queue};
//!
//! # fn main() {
//! # futures::executor::block_on(async {
//! let bus = channel!(events);
//! let pushes = topic!(bus, git.push);
//! let _a = pushes.subscribe().queue(PushQueue::Indexer(0)).await;
//! let _b = pushes.subscribe().queue(PushQueue::Indexer(0)).await;
//! let _c = pushes.subscribe().queue(PushQueue::Indexer(1)).await;
//! let _m = topic!(bus, git).subscribe().queue(Queue::Mailer).await; // git.>
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
//! # events! { events { git [Mailer] { push: PushMsg } } }
//! # async fn f() {
//! let bus = channel!(events);
//! topic!(bus, git.push).subscribe().queue(events::git::Queue::Mailer).await;
//! # }
//! # fn main() {}
//! ```
//!
//! and groups must be declared, not named with a string:
//!
//! ```compile_fail,E0277
//! # use nattes::{channel, events, topic};
//! # pub struct PushMsg;
//! # events! { events { git { push: PushMsg } } }
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
//! # events! { events { git { push [Indexer(u32)]: PushMsg } } }
//! # async fn f() {
//! let bus = channel!(events);
//! topic!(bus, git.push).subscribe().queue(events::git::PushQueue::Indexer("0")).await;
//! # }
//! # fn main() {}
//! ```
//!
//! # Delivery guarantees
//!
//! Delivery is **at most once**, in memory. A message only lives in subscriber
//! buffers: nothing is persisted, acknowledged or redelivered. "Delivered" means
//! queued in a subscription's buffer, not handled. This holds for
//! [`publish`](Channel::publish), [`request`](Channel::request),
//! [`request_timeout`](Channel::request_timeout) and [`scatter`](Channel::scatter):
//!
//! - Nothing is sent until the returned future is polled.
//! - The message goes to every subscription that matches the subject and whose
//!   filters accept it, and to one member per queue group. A subscription counts
//!   once its [`SubscriptionBuilder`] has been awaited. Earlier messages are
//!   not replayed.
//! - A message is lost for a subscriber when:
//!   - its buffer is full with [`Overflow::DropNewest`] or [`Overflow::DropOldest`].
//!     In a queue group, only when every member's buffer is full: the message
//!     goes to a member with room first;
//!   - one of its filters rejects the message or panics;
//!   - it drops the [`Subscription`] before reading the message;
//!   - the sending future is dropped while it waits for an [`Overflow::Wait`]
//!     subscriber, which then misses the message.
//! - The messages of one publisher, awaiting each call before the next, reach
//!   each subscriber in the order they were sent, minus the ones lost.
//!   Concurrent publishers have no common order, and members of a queue group
//!   can handle consecutive messages in any order.

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
pub use nattes_macros::events as __events;
