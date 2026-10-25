//! [`Channel`]: a typed view on part of the tree.

use crate::Level;
use crate::bus::{Bus, Event};
use crate::nav::{Append, AppendParams, Key, Navigable, Pattern, Prism};
use crate::request::Answer;
use crate::subscription::{Respond, SubscriptionBuilder};
use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

/// Route from the root event down to `T`, with the wildcard values `P` met on
/// the way. One link per step: walking down allocates one per [`Prism`].
pub(crate) trait Route<T, P>: Send + Sync {
    /// The `T` inside a root event, with the wildcard values: matches
    /// subscriptions and backs [`Msg`](crate::Msg).
    fn project<'a>(&self, root: &'a dyn Any) -> Option<(P, &'a T)>;
    /// The `T` moved out of a root event that nobody else holds; gives the
    /// event back otherwise. Backs `try_into_inner`.
    fn project_owned(&self, event: Event) -> Result<T, Event>;
    /// The root event holding `t`, for `publish`. `None` below a wildcard.
    fn inject(&self, t: T) -> Option<Event>;
    /// Where subscriptions sit in the interest trie. Built on subscribe only.
    fn path(&self, keys: &mut Vec<Key>);
}

/// Start of every route: the root event itself.
struct Root<R>(PhantomData<fn() -> R>);

impl<R: Level> Route<R, ()> for Root<R> {
    fn project<'a>(&self, root: &'a dyn Any) -> Option<((), &'a R)> {
        root.downcast_ref::<R>().map(|r| ((), r))
    }
    fn project_owned(&self, event: Event) -> Result<R, Event> {
        let any: Arc<dyn Any + Send + Sync> = event;
        let r = any.downcast::<R>().expect("events are of the root type");
        Arc::try_unwrap(r).map_err(|r| r as Event)
    }
    fn inject(&self, r: R) -> Option<Event> {
        Some(Arc::new(r))
    }
    fn path(&self, _: &mut Vec<Key>) {}
}

/// One more step after the route to `A`.
struct Hop<A, P, S> {
    parent: Arc<dyn Route<A, P>>,
    step: S,
}

impl<A, P, S> Route<S::B, Append<P, S::Q>> for Hop<A, P, S>
where
    A: 'static,
    P: AppendParams<S::Q> + 'static,
    S: Prism<A>,
{
    fn project<'a>(&self, root: &'a dyn Any) -> Option<(Append<P, S::Q>, &'a S::B)> {
        let (p, a) = self.parent.project(root)?;
        let (q, b) = self.step.project(a)?;
        Some((p.append_params(q), b))
    }
    fn project_owned(&self, event: Event) -> Result<S::B, Event> {
        let a = self.parent.project_owned(event)?;
        Ok((self.step.project_owned(a)).expect("delivered only if it matched"))
    }
    fn inject(&self, b: S::B) -> Option<Event> {
        self.parent.inject(self.step.inject(b)?)
    }
    fn path(&self, keys: &mut Vec<Key>) {
        self.parent.path(keys);
        keys.push(self.step.key());
    }
}

/// The bus as the channels hold it: subscriptions hold the [`Bus`] itself, so
/// the bus closes when the last channel drops this.
pub(crate) struct Handle(pub(crate) Arc<Bus>);

impl Deref for Handle {
    type Target = Bus;

    fn deref(&self) -> &Bus {
        &self.0
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Typed view on the part `T` of a tree; behaves like a bus rooted at `T`.
///
/// `P` holds the parameters left as wildcards on the way down. Only fully fixed
/// channels (`P = ()`) can publish.
///
/// `S` is the scope of the channel, which tells the queue groups it can join: the
/// level's `Topic` enum (so `S = T`), or the group enum of a leaf that
/// declares one.
///
/// Every channel of a bus, sub-channels included, keeps it open. Once the last
/// one is dropped, nothing can publish anymore: each subscription yields its
/// buffered messages, then ends.
///
/// A function taking `&Channel<T, P, S>` accepts any channel, root or not, with
/// or without wildcards:
///
/// ```
/// # use nattes::{at, topics, Channel};
/// # pub struct DeleteMsg;
/// # topics! { topics { user { id(u32) { delete: DeleteMsg } } } }
/// async fn watch<T: 'static, P: 'static, S>(ch: &Channel<T, P, S>) {
///     let _sub = ch.subscribe().await;
///     // …
/// }
///
/// # fn main() {
/// # futures::executor::block_on(async {
/// let bus = Channel::<topics::Topic>::new();
/// watch(&bus).await;                           // >
/// watch(&at!(bus, user)).await;                // user.>
/// watch(&at!(bus, user.id().delete)).await;    // user.*.delete
/// # });
/// # }
/// ```
pub struct Channel<T, P = (), S = T> {
    pub(crate) bus: Arc<Handle>,
    // The path from the root is a chain of trait objects, not a type:
    // `at!(bus, user.id(alice))` and `at!(bus, user.id(bob))` are both
    // `Channel<user::id::Topic>`, and a mounted tree has the same channel type
    // as a standalone one.
    pub(crate) route: Arc<dyn Route<T, P>>,
    pub(crate) scope: PhantomData<fn() -> S>,
}

impl<T, P, S> Clone for Channel<T, P, S> {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            route: self.route.clone(),
            scope: PhantomData,
        }
    }
}

impl<T, P, S> fmt::Debug for Channel<T, P, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut path = Vec::new();
        self.route.path(&mut path);
        f.debug_tuple("Channel")
            .field(&format_args!("{}", Pattern(&path)))
            .finish()
    }
}

impl<R: Level> Channel<R> {
    /// New bus with `R` as its root.
    pub fn new() -> Self {
        Self {
            bus: Arc::new(Handle(Arc::default())),
            route: Arc::new(Root(PhantomData)),
            scope: PhantomData,
        }
    }
}

impl<R: Level> Default for Channel<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Navigable, P: 'static> Channel<T, P> {
    /// Navigation from this channel; used by [`at!`](crate::at).
    #[doc(hidden)]
    pub fn steps(&self) -> T::Steps<P> {
        T::steps(self.clone())
    }
}

impl<T: 'static, P: 'static, S> Channel<T, P, S> {
    /// One step down; used by the code generated by [`topics!`](crate::topics).
    #[doc(hidden)]
    pub fn step<X: Prism<T>>(&self, step: X) -> Channel<X::B, Append<P, X::Q>>
    where
        P: AppendParams<X::Q>,
    {
        Channel {
            bus: self.bus.clone(),
            route: Arc::new(Hop {
                parent: self.route.clone(),
                step,
            }),
            scope: PhantomData,
        }
    }

    /// The same channel in the scope `X`; used by [`topics!`](crate::topics)
    /// for leaves that declare queue groups.
    #[doc(hidden)]
    pub fn with_scope<X>(self) -> Channel<T, P, X> {
        Channel {
            bus: self.bus,
            route: self.route,
            scope: PhantomData,
        }
    }

    /// Where subscriptions on this channel sit in the interest trie.
    pub(crate) fn path(&self) -> Vec<Key> {
        let mut keys = Vec::new();
        self.route.path(&mut keys);
        keys
    }

    /// Everything at or below this channel (NATS `subject.>`).
    pub fn subscribe(&self) -> SubscriptionBuilder<T, P, S> {
        SubscriptionBuilder::new(self.clone())
    }

    /// Answer the requests sent to this request leaf: awaited, it gives a
    /// [`Responder`](crate::Responder), a stream of
    /// [`Incoming`](crate::Incoming) requests.
    ///
    /// Only responders count for [`request`](Self::request) and
    /// [`scatter`](Self::scatter), and each one gets its own reply token.
    /// Subscribers see the requests too, but can't answer them.
    pub fn respond(&self) -> SubscriptionBuilder<T, P, S, Respond>
    where
        S: Answer<T>,
    {
        SubscriptionBuilder::new(self.clone())
    }
}

/// A channel with the scope `Self` can publish a `T`: every level, and every
/// leaf but request leaves, which take requests only.
///
/// Implemented for every type with itself as `T`, which covers levels
/// (`S = Topic`) and leaves without groups (`S` is the message type), and by
/// [`topics!`](crate::topics) on the group enum of a leaf with groups.
#[diagnostic::on_unimplemented(
    message = "can't publish on a request leaf",
    label = "takes requests only",
    note = "send with `request` or `scatter`"
)]
pub trait Publish<T> {}

impl<T> Publish<T> for T {}

impl<T: 'static, S> Channel<T, (), S> {
    /// Publish `msg` here. Returns how many subscriptions it was delivered to.
    ///
    /// The message is first delivered to every subscription with room in its
    /// buffer. Only then does `publish` wait for room in the full buffers of
    /// [`Overflow::Wait`](crate::Overflow::Wait) subscribers. If this future is dropped during that
    /// wait, those subscribers miss the message. Delivery is at most once:
    /// see [delivery guarantees](crate#delivery-guarantees).
    ///
    /// Not on a request leaf, which takes [`request`](Self::request)s only.
    pub async fn publish(&self, msg: T) -> usize
    where
        S: Publish<T>,
    {
        let event = self.to_event(msg);
        self.bus.deliver(event).await
    }

    /// [`publish`](Self::publish) without waiting, for `Drop`, callbacks and
    /// code that isn't async. Returns how many subscriptions it was delivered to.
    ///
    /// A full [`Overflow::Wait`](crate::Overflow::Wait) buffer drops the
    /// message, as with `DropNewest`, and counts it in `dropped()`.
    pub fn try_publish(&self, msg: T) -> usize
    where
        S: Publish<T>,
    {
        self.bus.deliver_now(self.to_event(msg))
    }

    /// Wrap `msg` into the root event to deliver.
    pub(crate) fn to_event(&self, msg: T) -> Event {
        // Only wildcard steps can't inject, and they add to `P`
        self.route
            .inject(msg)
            .expect("channels without wildcards can inject")
    }
}
