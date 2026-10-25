//! Typesafe subject-based message queue inspired by NATS.
//!
//! Declare the tree with [`topics!`](crate::topics), then walk down it with
//! [`at!`](crate::at) to get a [`Channel`] on any part of it. A channel
//! is a bus rooted at that part: hand a `Channel<topics::git::Topic>` to a
//! component and it publishes and subscribes relative to `git`.
//!
//! ```
//! use futures::StreamExt;
//! use nattes::{at, topics, Channel};
//!
//! pub struct PushMsg;
//! pub struct DeleteMsg;
//!
//! topics! {
//!     topics {
//!         git { push: PushMsg },
//!         user { id(u32) { delete: DeleteMsg } },
//!     }
//! }
//!
//! # fn main() {
//! # futures::executor::block_on(async {
//! let bus = Channel::<topics::Topic>::new();
//! let git = at!(bus, git);                    // git
//! let pushes = at!(bus, git.push);            // git.push
//! let alice_deletes = at!(bus, user.id(7).delete); // user.7.delete
//!
//! let mut git_sub = git.subscribe().await;                  // git.>
//! let mut deletes = at!(bus, user.id().delete).subscribe().await; // user.*.delete
//!
//! pushes.publish(PushMsg).await;
//! alice_deletes.publish(DeleteMsg).await;
//!
//! assert!(matches!(*git_sub.next().await.unwrap(), topics::git::Topic::Push(_)));
//! assert_eq!(deletes.next().await.unwrap().params(), (7,));
//! # });
//! # }
//! ```
//!
//! A channel with a wildcard can't publish:
//!
//! ```compile_fail,E0599
//! # use nattes::{at, topics, Channel};
//! # pub struct DeleteMsg;
//! # topics! { topics { user { id(u32) { delete: DeleteMsg } } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, user.id().delete).publish(DeleteMsg).await;
//! # }
//! # fn main() {}
//! ```
//!
//! not even without waiting:
//!
//! ```compile_fail,E0599
//! # use nattes::{at, topics, Channel};
//! # pub struct DeleteMsg;
//! # topics! { topics { user { id(u32) { delete: DeleteMsg } } } }
//! # fn main() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, user.id().delete).try_publish(DeleteMsg);
//! # }
//! ```
//!
//! and each path must start where the channel is:
//!
//! ```compile_fail,E0599
//! # use nattes::{at, topics, Channel};
//! # pub struct PushMsg;
//! # topics! { topics { git { push: PushMsg }, user { new: PushMsg } } }
//! # fn main() {
//! let users = at!(Channel::<topics::Topic>::new(), user);
//! at!(users, git.push);
//! # }
//! ```
//!
//! Parameters are matched by value, so their type must be `Eq + Hash`; floats
//! can't be parameters:
//!
//! ```compile_fail,E0277
//! # use nattes::topics;
//! # pub struct Reading;
//! topics! { sensors { reading(f64): Reading } }
//! # fn main() {}
//! ```
//!
//! Names are unique among siblings:
//!
//! ```compile_fail
//! # use nattes::topics;
//! # pub struct PushMsg;
//! topics! { topics { git { push: PushMsg, push: PushMsg } } }
//! # fn main() {}
//! ```
//!
//! Queue groups are declared on a topic, with `[ … ]` right after its name:
//! `git [Mailer] { … }`, `push [Indexer(u32)]: Msg`. Each value is a group, so
//! `Indexer(0)` and `Indexer(1)` are two groups:
//!
//! ```
//! # use nattes::{at, topics, Channel};
//! # pub struct PushMsg;
//! topics! {
//!     topics {
//!         git [Mailer] {
//!             push [Indexer(u32), Audit]: PushMsg,
//!         },
//!     }
//! }
//! use topics::git::{PushQueue, Queue};
//!
//! # fn main() {
//! # futures::executor::block_on(async {
//! let bus = Channel::<topics::Topic>::new();
//! let pushes = at!(bus, git.push);
//! let _a = pushes.subscribe().queue(PushQueue::Indexer(0)).await;
//! let _b = pushes.subscribe().queue(PushQueue::Indexer(0)).await;
//! let _c = pushes.subscribe().queue(PushQueue::Indexer(1)).await;
//! let _m = at!(bus, git).subscribe().queue(Queue::Mailer).await; // git.>
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
//! # use nattes::{at, topics, Channel};
//! # pub struct PushMsg;
//! # topics! { topics { git [Mailer] { push: PushMsg } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, git.push).subscribe().queue(topics::git::Queue::Mailer).await;
//! # }
//! # fn main() {}
//! ```
//!
//! and groups must be declared, not named with a string:
//!
//! ```compile_fail,E0277
//! # use nattes::{at, topics, Channel};
//! # pub struct PushMsg;
//! # topics! { topics { git { push: PushMsg } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, git.push).subscribe().queue("workers").await;
//! # }
//! # fn main() {}
//! ```
//!
//! Their values are typed like parameters:
//!
//! ```compile_fail,E0308
//! # use nattes::{at, topics, Channel};
//! # pub struct PushMsg;
//! # topics! { topics { git { push [Indexer(u32)]: PushMsg } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, git.push).subscribe().queue(topics::git::PushQueue::Indexer("0")).await;
//! # }
//! # fn main() {}
//! ```
//!
//! A request leaf, `name: Message => Reply`, takes requests: responders made
//! with [`respond`](Channel::respond) answer them, each one at most once.
//!
//! ```
//! # use futures::StreamExt;
//! # use nattes::{at, topics, Channel};
//! pub struct FetchMsg(pub String);
//! pub struct FetchAck(pub usize);
//!
//! topics! { topics { git { fetch: FetchMsg => FetchAck } } }
//!
//! # fn main() {
//! # futures::executor::block_on(async {
//! let bus = Channel::<topics::Topic>::new();
//! let fetches = at!(bus, git.fetch);
//! let mut responder = fetches.respond().await;
//! let answer = async {
//!     let request = responder.next().await.unwrap(); // derefs to `FetchMsg`
//!     let len = request.0.len();
//!     request.respond(FetchAck(len));
//! };
//! let (ack, ()) = futures::join!(fetches.request(FetchMsg("main".into())), answer);
//! assert_eq!(ack.unwrap().0, 4);
//! # });
//! # }
//! ```
//!
//! Other leaves take no requests:
//!
//! ```compile_fail,E0599
//! # use nattes::{at, topics, Channel};
//! # pub struct PushMsg;
//! # topics! { topics { git { push: PushMsg } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, git.push).request(PushMsg).await;
//! # }
//! # fn main() {}
//! ```
//!
//! and have no responders:
//!
//! ```compile_fail,E0277
//! # use nattes::{at, topics, Channel};
//! # pub struct PushMsg;
//! # topics! { topics { git { push: PushMsg } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, git.push).respond().await;
//! # }
//! # fn main() {}
//! ```
//!
//! A request leaf takes requests only, so it can't publish:
//!
//! ```compile_fail,E0277
//! # use nattes::{at, topics, Channel};
//! # pub struct FetchMsg;
//! # pub struct FetchAck;
//! # topics! { topics { git { fetch: FetchMsg => FetchAck } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, git.fetch).publish(FetchMsg).await;
//! # }
//! # fn main() {}
//! ```
//!
//! nor `try_publish`:
//!
//! ```compile_fail,E0277
//! # use nattes::{at, topics, Channel};
//! # pub struct FetchMsg;
//! # pub struct FetchAck;
//! # topics! { topics { git { fetch: FetchMsg => FetchAck } } }
//! # fn main() {
//! let bus = Channel::<topics::Topic>::new();
//! at!(bus, git.fetch).try_publish(FetchMsg);
//! # }
//! ```
//!
//! A responder answers a request once:
//!
//! ```compile_fail,E0382
//! # use futures::StreamExt;
//! # use nattes::{at, topics, Channel};
//! # pub struct FetchMsg;
//! # pub struct FetchAck;
//! # topics! { topics { git { fetch: FetchMsg => FetchAck } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! let mut responder = at!(bus, git.fetch).respond().await;
//! let request = responder.next().await.unwrap();
//! request.respond(FetchAck);
//! request.respond(FetchAck);
//! # }
//! # fn main() {}
//! ```
//!
//! and a subscriber sees requests, but can't answer them:
//!
//! ```compile_fail,E0599
//! # use futures::StreamExt;
//! # use nattes::{at, topics, Channel};
//! # pub struct FetchMsg;
//! # pub struct FetchAck;
//! # topics! { topics { git { fetch: FetchMsg => FetchAck } } }
//! # async fn f() {
//! let bus = Channel::<topics::Topic>::new();
//! let mut sub = at!(bus, git.fetch).subscribe().await;
//! sub.next().await.unwrap().respond(FetchAck);
//! # }
//! # fn main() {}
//! ```
//!
//! # Messages are shared
//!
//! Messages must be `Send + Sync + 'static`. One published message is shared by
//! every subscriber that receives it, so it must be `Sync`: this rules out
//! fields like `Cell`, `RefCell` or [`std::sync::mpsc::Receiver`]. Wrap such a
//! field in a `Mutex`, or send something shareable instead (an `Arc`, a
//! `futures::channel` sender, …). [`Msg::try_into_inner`] gives the message
//! back by value once no other subscriber holds it.
//!
//! # Delivery guarantees
//!
//! Delivery is **at most once**, in memory. A message only lives in subscriber
//! buffers: nothing is persisted, acknowledged or redelivered. "Delivered" means
//! pushed into a subscription's buffer, not handled. This holds for
//! [`publish`](Channel::publish), [`try_publish`](Channel::try_publish),
//! [`request`](Channel::request) and [`scatter`](Channel::scatter):
//!
//! - Nothing is sent until the returned future is polled; `try_publish` sends
//!   when called.
//! - The message goes to every subscription that matches the subject and whose
//!   filters accept it, and to one member per queue group. A subscription counts
//!   once its [`SubscriptionBuilder`] has been awaited. Earlier messages are
//!   not replayed. Responders ([`Responder`]) are subscriptions that only get
//!   requests, with their own reply token.
//! - A message is lost for a subscriber when:
//!   - its buffer is full with [`Overflow::DropNewest`] or [`Overflow::DropOldest`],
//!     or with [`Overflow::Wait`] on `try_publish`. In a queue group, only when
//!     every member's buffer is full and none of them has [`Overflow::Wait`] (or
//!     on `try_publish`): the message goes to a member with room first;
//!   - one of its filters rejects the message or panics;
//!   - it drops the [`Subscription`] or [`Responder`] before reading the
//!     message;
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

pub use bus::{DEFAULT_BUFFER, Overflow};
pub use channel::{Channel, Publish};
pub use group::{InScope, QueueGroup};
#[doc(hidden)]
pub use nav::{Append, AppendParams, ParamStep, Plain, Prism};
pub use nav::{IntoChannel, Is, Navigable, Param, Wild};
pub use request::{Answer, BusError, Incoming, Replies, ReplyToken};
pub use subscription::{Msg, Respond, Responder, Subscribe, Subscription, SubscriptionBuilder};
#[doc(hidden)]
pub use tree::Step;
pub use tree::{Level, ParamValue, subject};

#[doc(hidden)]
pub use nattes_macros::topics as __topics;
