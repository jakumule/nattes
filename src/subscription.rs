//! Subscriptions: the builder, the [`Stream`]s of messages and incoming
//! requests, and [`Msg`].

use crate::bus::{Buffer, Bus, Matcher};
use crate::event::{take, Envelope, Event};
use crate::group::{GroupKey, InScope, QueueGroup};
use crate::nav::{Key, Params, Pattern, Seg, Shape};
use crate::request::Incoming;
use crate::tree::{Leaf, Request, Subject, Topic};
use crate::{Channel, Overflow};
use futures::Stream;
use futures::stream::FusedStream;
use std::fmt;
use std::future::{IntoFuture, Ready, ready};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

/// A filter, typed on the way in: it reads the params and view of an event out of
/// its envelope and subject.
type Filter = Box<dyn Fn(&dyn Envelope, &[Seg]) -> bool + Send + Sync>;

/// Mode of a [`SubscriptionBuilder`] made by [`Channel::subscribe`]: it
/// starts a [`Subscription`].
pub struct Subscribe;

/// Mode of a [`SubscriptionBuilder`] made by [`Channel::respond`]: it starts
/// a [`Responder`].
pub struct Respond;

/// Configure a subscription, then `.await` it to start receiving.
///
/// `K` is [`Subscribe`] or [`Respond`], which decides what it starts.
pub struct SubscriptionBuilder<T: Topic, P, K = Subscribe> {
    pub(crate) channel: Channel<T, P>,
    pub(crate) filters: Vec<Filter>,
    pub(crate) group: Option<GroupKey>,
    pub(crate) buffer: usize,
    pub(crate) overflow: Overflow,
    pub(crate) mode: PhantomData<fn() -> K>,
}

impl<T: Topic, P, K> SubscriptionBuilder<T, P, K> {
    pub(crate) fn new(channel: Channel<T, P>) -> Self {
        Self {
            channel,
            filters: Vec::new(),
            group: None,
            buffer: T::BUFFER,
            overflow: T::ON_FULL,
            mode: PhantomData,
        }
    }

    /// Only messages for which `f(params, view)` is true, `view` being what
    /// [`Msg::view`] gives. It runs inside `publish`, on the publisher's
    /// thread, so keep it cheap. A panicking filter counts as "no match"
    /// (unless built with `panic = "abort"`).
    pub fn filter(
        mut self,
        f: impl for<'a> Fn(&P, T::View<'a>) -> bool + Send + Sync + 'static,
    ) -> Self
    where
        P: Params,
    {
        let shape = self.channel.path.shape();
        self.filters.push(Box::new(move |event, segs| {
            let params = shape.params::<P>(segs);
            f(&params, T::view(&segs[shape.depth()..], event.payload()))
        }));
        self
    }

    /// Join a queue group: each message goes to one member of the group.
    ///
    /// A group is a value of the group enum declared on this topic with
    /// `[ … ]` in [`topics!`](crate::topics). Members join it with or without
    /// parameters: `user.id(alice)` and `user.id(*)` share the groups of
    /// `user.id`, while another mount of this tree has its own. Responders and
    /// subscribers joining the same group form two separate groups.
    pub fn queue<G: QueueGroup + InScope<T>>(mut self, group: G) -> Self {
        self.group = Some(GroupKey::new(group));
        self
    }

    /// How many messages to buffer before applying [`on_full`](Self::on_full)
    /// (default [`Topic::BUFFER`]: [`DEFAULT_BUFFER`](crate::DEFAULT_BUFFER)
    /// unless the tree sets `#[buffer(n)]` on this topic or above).
    pub fn buffer(mut self, size: usize) -> Self {
        assert!(size > 0, "buffer size must be at least 1");
        self.buffer = size;
        self
    }

    /// What to do when the buffer is full (default [`Topic::ON_FULL`]:
    /// [`Overflow::DropNewest`] unless the tree sets `#[on_full(…)]` on this
    /// topic or above).
    pub fn on_full(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// Deliver to `f`, called with each message on the publisher's thread,
    /// instead of buffering: the cheapest way to receive, and one that needs no
    /// task. Returns a [`Handler`], which unsubscribes on drop.
    ///
    /// `f` must not block: it delays the publisher. Publishers on several
    /// threads may call it at the same time. A panic in `f` counts the message
    /// as dropped. [`buffer`](Self::buffer) and [`on_full`](Self::on_full) don't
    /// apply: a handler takes every message. A publish that started before the
    /// [`Handler`] is dropped may still call `f`.
    pub fn handler(self, f: impl Fn(Msg<T, P>) + Send + Sync + 'static) -> Handler<T, P> {
        let shape = self.channel.path.shape();
        let handler = move |event| {
            f(Msg {
                event,
                shape,
                topic: PhantomData,
            })
        };
        let buffer = Arc::new(Buffer::handler(Box::new(handler)));
        Handler {
            active: self.start_with(buffer, false),
            topic: PhantomData,
        }
    }

    fn start(self, responder: bool) -> Active {
        let buffer = Arc::new(Buffer::new(self.buffer, self.overflow));
        self.start_with(buffer, responder)
    }

    fn start_with(self, buffer: Arc<Buffer>, responder: bool) -> Active {
        let path = self.channel.path;
        let keys: Arc<[Key]> = path.keys().into();
        // The trie matches the subject exactly: only filters need a matcher
        let matcher: Option<Matcher> = (!self.filters.is_empty()).then(|| {
            let filters = self.filters;
            Box::new(move |event: &dyn Envelope, segs: &[Seg]| {
                filters.iter().all(|f| f(event, segs))
            }) as Matcher
        });

        let bus = self.channel.bus.0.clone();
        let id = bus.subscribe(
            &keys,
            matcher,
            self.group.as_ref(),
            responder,
            !responder,
            buffer.clone(),
        );
        Active {
            id,
            bus,
            buffer,
            shape: path.shape(),
            keys,
            group: self.group,
            responder,
        }
    }
}

impl<T: Topic, P> IntoFuture for SubscriptionBuilder<T, P, Subscribe> {
    type Output = Subscription<T, P>;
    type IntoFuture = Ready<Subscription<T, P>>;

    fn into_future(self) -> Self::IntoFuture {
        ready(Subscription {
            active: self.start(false),
            topic: PhantomData,
        })
    }
}

impl<L: Request, P> IntoFuture for SubscriptionBuilder<L, P, Respond> {
    type Output = Responder<L, P>;
    type IntoFuture = Ready<Responder<L, P>>;

    fn into_future(self) -> Self::IntoFuture {
        ready(Responder {
            active: self.start(true),
            leaf: PhantomData,
        })
    }
}

/// What [`Subscription`] and [`Responder`] share: the registration in the
/// bus, removed on drop, and the buffer.
pub(crate) struct Active {
    id: u64,
    bus: Arc<Bus>,
    pub(crate) buffer: Arc<Buffer>,
    pub(crate) shape: Shape,
    keys: Arc<[Key]>,
    group: Option<GroupKey>,
    responder: bool,
}

impl Active {
    fn dropped(&self) -> u64 {
        self.buffer.dropped.load(Ordering::Relaxed)
    }

    fn debug(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        f.debug_struct(name)
            .field("id", &self.id)
            .field("subject", &format_args!("{}", Pattern(&self.keys)))
            .field("dropped", &self.dropped())
            .finish()
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        self.bus.unsubscribe(
            &self.keys,
            self.id,
            self.group.as_ref(),
            self.responder,
            !self.responder,
        );
        self.buffer.close();
    }
}

/// Active subscription, a [`Stream`] of [`Msg`]. Unsubscribes on drop. Ends
/// once the bus is [closed](crate::Channel::close) and the buffer read.
pub struct Subscription<T, P> {
    active: Active,
    topic: PhantomData<fn() -> (T, P)>,
}

impl<T, P> Subscription<T, P> {
    /// Messages lost because the buffer was full.
    pub fn dropped(&self) -> u64 {
        self.active.dropped()
    }
}

impl<T, P> Stream for Subscription<T, P> {
    type Item = Msg<T, P>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        (self.active.buffer.poll_next(cx)).map(|event| {
            event.map(|event| Msg {
                event,
                shape: self.active.shape,
                topic: PhantomData,
            })
        })
    }
}

impl<T, P> Subscription<T, P> {
    /// Up to `max` buffered messages, appended to `buf`, taken under one lock:
    /// how many, or 0 once the subscription ended (or with `max == 0`). Waits
    /// while the buffer is empty. Cheaper than one `next()` per message.
    pub async fn recv_many(&mut self, buf: &mut Vec<Msg<T, P>>, max: usize) -> usize {
        std::future::poll_fn(|cx| self.poll_recv_many(cx, buf, max)).await
    }

    /// [`recv_many`](Self::recv_many) as a poll function.
    pub fn poll_recv_many(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut Vec<Msg<T, P>>,
        max: usize,
    ) -> Poll<usize> {
        let shape = self.active.shape;
        self.active.buffer.poll_many(cx, max, |event| {
            buf.push(Msg {
                event,
                shape,
                topic: PhantomData,
            })
        })
    }
}

/// Terminated once the bus is closed and the buffer read; goes straight into
/// `futures::select!`.
impl<T, P> FusedStream for Subscription<T, P> {
    fn is_terminated(&self) -> bool {
        self.active.buffer.is_terminated()
    }
}

impl<T, P> fmt::Debug for Subscription<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.active.debug(f, "Subscription")
    }
}

/// Active handler, made by [`SubscriptionBuilder::handler`]: its function gets
/// every message. Unsubscribes on drop.
pub struct Handler<T, P> {
    active: Active,
    topic: PhantomData<fn() -> (T, P)>,
}

impl<T, P> Handler<T, P> {
    /// Messages lost because the function panicked.
    pub fn dropped(&self) -> u64 {
        self.active.dropped()
    }
}

impl<T, P> fmt::Debug for Handler<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.active.debug(f, "Handler")
    }
}

/// Active responder on a request leaf, a [`Stream`] of [`Incoming`]
/// requests. Unsubscribes on drop, which drops the requests it didn't read.
/// Ends once the bus is [closed](crate::Channel::close) and the buffer read.
pub struct Responder<L, P> {
    active: Active,
    leaf: PhantomData<fn() -> (L, P)>,
}

impl<L, P> Responder<L, P> {
    /// Requests lost because the buffer was full.
    pub fn dropped(&self) -> u64 {
        self.active.dropped()
    }
}

impl<L: Request, P> Stream for Responder<L, P> {
    type Item = Incoming<L, P>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        (self.active.buffer.poll_next(cx))
            .map(|event| event.map(|event| Incoming::new(event, self.active.shape)))
    }
}

/// Terminated once the bus is closed and the buffer read; goes straight into
/// `futures::select!`.
impl<L: Request, P> FusedStream for Responder<L, P> {
    fn is_terminated(&self) -> bool {
        self.active.buffer.is_terminated()
    }
}

impl<L, P> fmt::Debug for Responder<L, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.active.debug(f, "Responder")
    }
}

/// Received message. On a leaf it derefs to the message; on a level,
/// [`view`](Self::view) gives the level's `TopicRef`, which tells which topic
/// below it the message was published on.
pub struct Msg<T, P> {
    pub(crate) event: Event,
    pub(crate) shape: Shape,
    topic: PhantomData<fn() -> (T, P)>,
}

impl<T, P> Msg<T, P> {
    /// Values of the wildcard parameters on the way to this channel.
    pub fn params(&self) -> P
    where
        P: Params,
    {
        self.shape.params(self.event.path())
    }

    /// NATS-style subject of the message, e.g. `user.id.42.delete`.
    pub fn subject(&self) -> String {
        Subject(self.event.path()).to_string()
    }
}

impl<T: Topic, P> Msg<T, P> {
    /// The message as this channel's topic shows it: `&PushMsg` on a leaf,
    /// the `TopicRef` of a level on a level.
    pub fn view(&self) -> T::View<'_> {
        view::<T>(&*self.event, self.shape)
    }
}

impl<L: Leaf, P> Msg<L, P> {
    /// Take the message out, which needs no `Clone`. Fails, giving `self` back, unless
    /// this `Msg` holds the last reference to the event: other subscriptions
    /// that received it have dropped their `Msg`, no clone of this one is
    /// alive, and `publish` returned (while it waits on an [`Overflow::Wait`]
    /// buffer it still holds the event). A message with one subscriber, or
    /// picked for one member of a queue group, is in that case once `publish`
    /// returns.
    ///
    /// Read [`params`](Self::params) first if needed.
    pub fn try_into_inner(self) -> Result<L::Msg, Self> {
        take(self.event).map_err(|event| Self {
            event,
            shape: self.shape,
            topic: PhantomData,
        })
    }
}

/// The view of `event` for a channel of the topic `T` of the shape `shape`.
pub(crate) fn view<'a, T: Topic>(event: &'a dyn Envelope, shape: Shape) -> T::View<'a> {
    T::view(&event.path()[shape.depth()..], event.payload())
}

impl<L: Leaf, P> Deref for Msg<L, P> {
    type Target = L::Msg;

    fn deref(&self) -> &L::Msg {
        crate::tree::__payload(self.event.payload())
    }
}

/// Shares the event.
impl<T, P> Clone for Msg<T, P> {
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            shape: self.shape,
            topic: PhantomData,
        }
    }
}

impl<T: Topic, P> fmt::Debug for Msg<T, P>
where
    for<'a> T::View<'a>: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Msg")
            .field("subject", &format_args!("{}", Subject(self.event.path())))
            .field("msg", &self.view())
            .finish()
    }
}
