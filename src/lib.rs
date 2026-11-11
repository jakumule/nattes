//! Typesafe subject-based message queue inspired by NATS.
//!
//! Declare the tree with [`topics!`](crate::topics), then walk down it with
//! the methods it generates to get a [`Channel`] on any part of it. A channel
//! is a bus rooted at that part: hand a `Channel<topics::git::Topic>` to a
//! component and it publishes and subscribes relative to `git`.
//!
//! ```
//! use futures::StreamExt;
//! use nattes::{topics, Channel, Wild};
//! use topics::prelude::*;
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
//! let git = bus.git();                            // git
//! let pushes = bus.git().push();                  // git.push
//! let alice_deletes = bus.user().id(7).delete();  // user.id.7.delete
//!
//! let mut git_sub = git.subscribe().await;                           // git.>
//! let mut deletes = bus.user().id(Wild).delete().subscribe().await;  // user.id.*.delete
//!
//! pushes.publish(PushMsg).await;
//! alice_deletes.publish(DeleteMsg).await;
//!
//! let msg = git_sub.next().await.unwrap();
//! assert!(matches!(msg.view(), topics::git::TopicRef::Push(_)));
//! assert_eq!(deletes.next().await.unwrap().params(), (7,));
//! # });
//! # }
//! ```
//!
//! # Guide
//!
//! The [`guide`] explains each part, with tested examples:
//!
//! - [Declaring topics](guide::declaring): the `topics!` syntax, what it
//!   generates, delivery defaults.
//! - [Navigating the tree](guide::navigating): steps, wildcards, sub-channels
//!   as roots.
//! - [Publishing](guide::publishing) and [subscribing](guide::subscribing):
//!   filters, overflow policies, batches, handlers, closing.
//! - [Queue groups](guide::queue_groups) and
//!   [request/reply](guide::request_reply), with scatter-gather.
//! - [Bridges](guide::bridges): a bus over a byte transport (feature `bridge`).
//! - [Delivery guarantees](guide::delivery): at most once, when a message is
//!   lost, ordering.
//! - [Compile-time checks](guide::compile_time): every mistake that is a
//!   build error.
//! - [Tracing](guide::tracing), [recipes](guide::recipes) and
//!   [requirements](guide::requirements).

#[cfg(feature = "bridge")]
mod bridge;
mod bus;
mod channel;
mod event;
mod group;
pub mod guide;
mod hamt;
mod log;
mod macros;
mod nav;
mod request;
mod subscription;
#[cfg(test)]
mod tests;
mod tree;
#[cfg(feature = "bridge")]
mod wire;

#[cfg(feature = "bridge")]
pub use bridge::{Bridge, BridgeError, FRAME_VERSION};
pub use bus::{DEFAULT_BUFFER, Overflow};
pub use channel::Channel;
#[doc(hidden)]
pub use event::{Envelope, Event};
pub use group::{InScope, QueueGroup};
#[doc(hidden)]
pub use nav::{Append, AppendParams, Node, Path, Seg, SegInfo};
pub use nav::{Param, Params, Wild};
pub use request::{BusError, Incoming, Replies, ReplyToken};
pub use subscription::{
    Handler, Msg, Respond, Responder, Subscribe, Subscription, SubscriptionBuilder,
};
#[doc(hidden)]
pub use tree::{DynValue, __param, __payload};
pub use tree::{Leaf, Level, ParamValue, Publish, Request, Topic};
#[cfg(feature = "bridge")]
pub use wire::{Codec, CodecError, Wire};
#[cfg(feature = "bridge")]
#[doc(hidden)]
pub use wire::{WireError, __decode_leaf, __decode_param, __encode_leaf, __encode_param};

#[doc(hidden)]
pub use nattes_macros::topics as __topics;

/// The README's examples, as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct Readme;
